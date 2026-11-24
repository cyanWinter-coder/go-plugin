// Copyright IBM Corp. 2016, 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/hashicorp/yamux"
)

// RPCClient connects to an RPCServer over net/rpc to dispense plugin types.
type RPCClient struct {
	broker  *MuxBroker
	control *rpc.Client
	plugins map[string]Plugin

	// These are the streams used for the various stdout/err overrides
	stdout, stderr net.Conn

	// handshakeConfig is non-nil only when the in-band handshake should be
	// performed over the raw connection before yamux is created.
	handshakeConfig *rpcHandshakeConfig

	// handshakeOnce makes sure the handshake is performed at most once, even
	// if Handshake is called concurrently or repeatedly.
	handshakeOnce sync.Once

	// The result of a successful handshake. Cached so repeated calls do not
	// touch the network.
	negotiatedName    string
	negotiatedVersion uint32
	handshakeErr      error
}

// rpcHandshakeConfig carries the parameters the client presents during the
// in-band handshake.
type rpcHandshakeConfig struct {
	// Name is the plugin name the client expects. Empty disables the name
	// check.
	Name string

	// ProtocolVersion is the go-plugin protocol version the client was
	// configured for.
	ProtocolVersion uint32

	// Timeout bounds the handshake. A zero value means
	// defaultHandshakeTimeout.
	Timeout time.Duration
}

// newRPCClient creates a new RPCClient. The Client argument is expected
// to be successfully started already with a lock held.
func newRPCClient(c *Client) (*RPCClient, error) {
	// Connect to the client
	conn, err := net.Dial(c.address.Network(), c.address.String())
	if err != nil {
		return nil, err
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		// Make sure to set keep alive so that the connection doesn't die
		_ = tcpConn.SetKeepAlive(true)
	}

	if c.config.TLSConfig != nil {
		conn = tls.Client(conn, c.config.TLSConfig)
	}

	// Create the actual RPC client. The in-band handshake is enabled when
	// the client was configured with a plugin name; this is the path used by
	// the subprocess-managed Client.
	var handshakeConfig *rpcHandshakeConfig
	if c.config.Name != "" {
		version := c.negotiatedVersion
		if version == 0 {
			// Reattached clients do not always record the negotiated
			// version; fall back to the configured one.
			version = int(c.config.ProtocolVersion)
		}
		handshakeConfig = &rpcHandshakeConfig{
			Name:            c.config.Name,
			ProtocolVersion: uint32(version),
			Timeout:         c.config.StartTimeout,
		}
	}

	result, err := newRPCClientWithHandshake(conn, c.config.Plugins, handshakeConfig)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	// Begin the stream syncing so that stdin, out, err work properly
	err = result.SyncStreams(
		c.config.SyncStdout,
		c.config.SyncStderr)
	if err != nil {
		_ = result.Close()
		return nil, err
	}

	return result, nil
}

// NewRPCClient creates a client from an already-open connection-like value.
// Dial is typically used instead.
//
// The connection is assumed to already carry multiplexed traffic; no
// in-band handshake is performed. This preserves the historical contract,
// including the yamux stream open order, for callers constructing clients
// directly.
func NewRPCClient(conn io.ReadWriteCloser, plugins map[string]Plugin) (*RPCClient, error) {
	return newRPCClientWithHandshake(conn, plugins, nil)
}

// newRPCClientWithHandshake creates a client and, when config is non-nil,
// performs the in-band version and plugin-name handshake on the raw
// connection before the yamux session is created. On negotiation failure a
// typed sentinel error (see ErrProtocolMismatch, ErrPluginNameMismatch,
// ErrHandshakeTimeout and ErrHandshakeInvalidResponse) is returned and the
// connection is closed.
func newRPCClientWithHandshake(conn io.ReadWriteCloser, plugins map[string]Plugin, config *rpcHandshakeConfig) (*RPCClient, error) {
	if config != nil {
		nextConn, resp, err := clientHandshake(conn, handshakeRequest{
			ProtocolVersion: config.ProtocolVersion,
			Name:            config.Name,
		}, config.Timeout)
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		conn = nextConn

		client, err := newRPCClientFromConn(conn, plugins)
		if err != nil {
			return nil, err
		}
		client.handshakeConfig = config
		client.negotiatedName = resp.Name
		client.negotiatedVersion = resp.ProtocolVersion
		// Mark the once as done so Handshake returns the cached result.
		client.handshakeOnce.Do(func() {})
		return client, nil
	}

	return newRPCClientFromConn(conn, plugins)
}

// newRPCClientFromConn builds the yamux session and RPC control client. The
// yamux stream open order (control first, then the two stdio streams) is
// unchanged from the original implementation.
func newRPCClientFromConn(conn io.ReadWriteCloser, plugins map[string]Plugin) (*RPCClient, error) {
	// Create the yamux client so we can multiplex
	mux, err := yamux.Client(conn, nil)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	// Connect to the control stream.
	control, err := mux.Open()
	if err != nil {
		_ = mux.Close()
		return nil, err
	}

	// Connect stdout, stderr streams
	stdstream := make([]net.Conn, 2)
	for i := range stdstream {
		stdstream[i], err = mux.Open()
		if err != nil {
			_ = mux.Close()
			return nil, err
		}
	}

	// Create the broker and start it up
	broker := newMuxBroker(mux)
	go broker.Run()

	// Build the client using our broker and control channel.
	return &RPCClient{
		broker:  broker,
		control: rpc.NewClient(control),
		plugins: plugins,
		stdout:  stdstream[0],
		stderr:  stdstream[1],
	}, nil
}

// Handshake exchanges protocol version and plugin name with the plugin
// server and caches the negotiated result.
//
// When the client was created through a Client that configured a handshake,
// the handshake already ran during construction and this call returns the
// cached result without any network traffic. Clients created with
// NewRPCClient have no handshake configuration and Handshake returns
// ErrHandshakeInvalidResponse. It is safe to call concurrently and any
// number of times: only the first call can perform I/O.
func (c *RPCClient) Handshake() (name string, protocolVersion uint32, err error) {
	c.handshakeOnce.Do(func() {
		if c.handshakeConfig == nil {
			c.handshakeErr = errors.New("handshake was not configured for this client")
			return
		}
		// This branch is only reachable for clients whose handshake was not
		// performed at construction time. The yamux session already owns the
		// raw connection here, so a second in-band exchange is not possible;
		// construction is the only path that sets a config today.
		c.handshakeErr = fmt.Errorf("%w: handshake not performed during construction", ErrHandshakeInvalidResponse)
	})

	return c.negotiatedName, c.negotiatedVersion, c.handshakeErr
}

// NegotiatedName returns the plugin name returned by the server during the
// handshake. It is empty if no handshake was performed.
func (c *RPCClient) NegotiatedName() string {
	return c.negotiatedName
}

// NegotiatedProtocolVersion returns the protocol version agreed on during
// the handshake. It is zero if no handshake was performed.
func (c *RPCClient) NegotiatedProtocolVersion() uint32 {
	return c.negotiatedVersion
}

// SyncStreams should be called to enable syncing of stdout,
// stderr with the plugin.
//
// This will return immediately and the syncing will continue to happen
// in the background. You do not need to launch this in a goroutine itself.
//
// This should never be called multiple times.
func (c *RPCClient) SyncStreams(stdout io.Writer, stderr io.Writer) error {
	go copyStream("stdout", stdout, c.stdout)
	go copyStream("stderr", stderr, c.stderr)
	return nil
}

// Close closes the connection. The client is no longer usable after this
// is called.
func (c *RPCClient) Close() error {
	// Call the control channel and ask it to gracefully exit. If this
	// errors, then we save it so that we always return an error but we
	// want to try to close the other channels anyways.
	var empty struct{}
	returnErr := c.control.Call("Control.Quit", true, &empty)

	// Close the other streams we have
	if err := c.control.Close(); err != nil {
		return err
	}
	if err := c.stdout.Close(); err != nil {
		return err
	}
	if err := c.stderr.Close(); err != nil {
		return err
	}
	if err := c.broker.Close(); err != nil {
		return err
	}

	// Return back the error we got from Control.Quit. This is very important
	// since we MUST return non-nil error if this fails so that Client.Kill
	// will properly try a process.Kill.
	return returnErr
}

func (c *RPCClient) Dispense(name string) (interface{}, error) {
	p, ok := c.plugins[name]
	if !ok {
		return nil, fmt.Errorf("unknown plugin type: %s", name)
	}

	var id uint32
	if err := c.control.Call(
		"Dispenser.Dispense", name, &id); err != nil {
		return nil, err
	}

	conn, err := c.broker.Dial(id)
	if err != nil {
		return nil, err
	}

	return p.Client(c.broker, rpc.NewClient(conn))
}

// Ping pings the connection to ensure it is still alive.
//
// The error from the RPC call is returned exactly if you want to inspect
// it for further error analysis. Any error returned from here would indicate
// that the connection to the plugin is not healthy.
func (c *RPCClient) Ping() error {
	var empty struct{}
	return c.control.Call("Control.Ping", true, &empty)
}
