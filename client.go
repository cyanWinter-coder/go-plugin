// Copyright IBM Corp. 2016, 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"bufio"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin/internal/cmdrunner"
	"github.com/hashicorp/go-plugin/internal/grpcmux"
	"github.com/hashicorp/go-plugin/runner"
	"google.golang.org/grpc"
)

// If this is 1, then we've called CleanupClients. This can be used
// by plugin RPC implementations to change error behavior since you
// can expected network connection errors at this point. This should be
// read by using sync/atomic.
var Killed uint32 = 0

// This is a slice of the "managed" clients which are cleaned up when
// calling Cleanup
var managedClients = make([]*Client, 0, 5)
var managedClientsLock sync.Mutex

// Error types
var (
	// ErrProcessNotFound is returned when a client is instantiated to
	// reattach to an existing process and it isn't found.
	ErrProcessNotFound = cmdrunner.ErrProcessNotFound

	// ErrChecksumsDoNotMatch is returned when binary's checksum doesn't match
	// the one provided in the SecureConfig.
	ErrChecksumsDoNotMatch = errors.New("checksums did not match")

	// ErrSecureNoChecksum is returned when an empty checksum is provided to the
	// SecureConfig.
	ErrSecureConfigNoChecksum = errors.New("no checksum provided")

	// ErrSecureNoHash is returned when a nil Hash object is provided to the
	// SecureConfig.
	ErrSecureConfigNoHash = errors.New("no hash implementation provided")

	// ErrSecureConfigAndReattach is returned when both Reattach and
	// SecureConfig are set.
	ErrSecureConfigAndReattach = errors.New("only one of Reattach or SecureConfig can be set")

	// ErrGRPCBrokerMuxNotSupported is returned when the client requests
	// multiplexing over the gRPC broker, but the plugin does not support the
	// feature. In most cases, this should be resolvable by updating and
	// rebuilding the plugin, or restarting the plugin with
	// ClientConfig.GRPCBrokerMultiplex set to false.
	ErrGRPCBrokerMuxNotSupported = errors.New("client requested gRPC broker multiplexing but plugin does not support the feature")
)

// defaultPluginLogBufferSize is the default size of the buffer used to read from stderr for plugin log lines.
const defaultPluginLogBufferSize = 64 * 1024

// defaultExitGracefulTimeout is how long Kill waits for a plugin to shut
// itself down after being notified before forcefully terminating it.
const defaultExitGracefulTimeout = 2 * time.Second

// handshakeState tracks the progress of the plugin's initial handshake.
type handshakeState int

const (
	// handshakeNotStarted is the state before Start has launched a subprocess.
	handshakeNotStarted handshakeState = iota
	// handshakeInProgress means the subprocess is running but has not yet sent
	// a valid handshake line.
	handshakeInProgress
	// handshakeReady means the plugin completed the handshake and the client
	// has a usable RPC address.
	handshakeReady
	// handshakeFailed means the handshake was abandoned because of a timeout,
	// an invalid response, or the process exiting early.
	handshakeFailed
)

func (s handshakeState) String() string {
	switch s {
	case handshakeNotStarted:
		return "not started"
	case handshakeInProgress:
		return "in progress"
	case handshakeReady:
		return "ready"
	case handshakeFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Client handles the lifecycle of a plugin application. It launches
// plugins, connects to them, dispenses interface implementations, and handles
// killing the process.
//
// Plugin hosts should use one Client for each plugin executable. To
// dispense a plugin type, use the `Client.Client` function, and then
// call `Dispense`. This awkward API is mostly historical but is used to split
// the client that deals with subprocess management and the client that
// does RPC management.
//
// See NewClient and ClientConfig for using a Client.
type Client struct {
	config            *ClientConfig
	exited            bool
	l                 sync.Mutex
	killMu            sync.Mutex
	address           net.Addr
	runner            runner.AttachedRunner
	client            ClientProtocol
	protocol          Protocol
	logger            hclog.Logger
	doneCtx           context.Context
	ctxCancel         context.CancelFunc
	negotiatedVersion int

	// closed records that Kill has been invoked. Once closed, Start and Client
	// return ErrClientClosed instead of silently reusing stale state. It is
	// guarded by l.
	closed bool

	// stateMu guards exited and exitErr. It is kept separate from l so that
	// the process reaper goroutine can record the result without holding (or
	// contending on) l, which Start holds throughout the handshake.
	stateMu sync.Mutex

	// handshakeState records how far the subprocess got with its initial
	// protocol handshake. It is guarded by l, which Start holds for its whole
	// duration.
	handshakeState handshakeState

	// stderrTail retains the most recent stderr output of the plugin so that
	// startup and crash errors can include it.
	stderrTail *stderrTail

	// exitCh receives the ExitError computed by the goroutine that waits on
	// the plugin process, exactly once. It is buffered and may be nil for
	// reattached clients.
	exitCh chan *ExitError

	// exitErr records how the plugin process ended. It is nil until the
	// process has been reaped and is guarded by l.
	exitErr *ExitError

	// clientWaitGroup is used to manage the lifecycle of the plugin management
	// goroutines.
	clientWaitGroup sync.WaitGroup

	// pipesWaitGroup is used to prevent the command's Wait() function from
	// being called before we've finished reading from the stdout and stderr pipe.
	pipesWaitGroup sync.WaitGroup

	// processKilled is used for testing only, to flag when the process was
	// forcefully killed.
	processKilled bool

	unixSocketCfg UnixSocketConfig

	grpcMuxerOnce sync.Once
	grpcMuxer     *grpcmux.GRPCClientMuxer
}

// NegotiatedVersion returns the protocol version negotiated with the server.
// This is only valid after Start() is called.
func (c *Client) NegotiatedVersion() int {
	return c.negotiatedVersion
}

// ID returns a unique ID for the running plugin. By default this is the process
// ID (pid), but it could take other forms if RunnerFunc was provided.
func (c *Client) ID() string {
	c.l.Lock()
	defer c.l.Unlock()

	if c.runner != nil {
		return c.runner.ID()
	}

	return ""
}

// ClientConfig is the configuration used to initialize a new
// plugin client. After being used to initialize a plugin client,
// that configuration must not be modified again.
type ClientConfig struct {
	// HandshakeConfig is the configuration that must match servers.
	HandshakeConfig

	// Plugins are the plugins that can be consumed.
	// The implied version of this PluginSet is the Handshake.ProtocolVersion.
	Plugins PluginSet

	// VersionedPlugins is a map of PluginSets for specific protocol versions.
	// These can be used to negotiate a compatible version between client and
	// server. If this is set, Handshake.ProtocolVersion is not required.
	VersionedPlugins map[int]PluginSet

	// One of the following must be set, but not both.
	//
	// Cmd is the unstarted subprocess for starting the plugin. If this is
	// set, then the Client starts the plugin process on its own and connects
	// to it.
	//
	// Reattach is configuration for reattaching to an existing plugin process
	// that is already running. This isn't common.
	Cmd      *exec.Cmd
	Reattach *ReattachConfig

	// RunnerFunc allows consumers to provide their own implementation of
	// runner.Runner and control the context within which a plugin is executed.
	// The cmd argument will have been copied from the config and populated with
	// environment variables that a go-plugin server expects to read such as
	// AutoMTLS certs and the magic cookie key.
	RunnerFunc func(l hclog.Logger, cmd *exec.Cmd, tmpDir string) (runner.Runner, error)

	// SecureConfig is configuration for verifying the integrity of the
	// executable. It can not be used with Reattach.
	SecureConfig *SecureConfig

	// TLSConfig is used to enable TLS on the RPC client.
	TLSConfig *tls.Config

	// Managed represents if the client should be managed by the
	// plugin package or not. If true, then by calling CleanupClients,
	// it will automatically be cleaned up. Otherwise, the client
	// user is fully responsible for making sure to Kill all plugin
	// clients. By default the client is _not_ managed.
	Managed bool

	// The minimum and maximum port to use for communicating with
	// the subprocess. If not set, this defaults to 10,000 and 25,000
	// respectively.
	MinPort, MaxPort uint

	// StartTimeout is the timeout to wait for the plugin to say it
	// has started successfully.
	StartTimeout time.Duration

	// ExitGracefulTimeout is the maximum time Kill waits for the plugin to
	// terminate itself after being asked to shut down gracefully (via the RPC
	// control connection) before forcefully killing its process. If zero, a
	// default of two seconds is used.
	//
	// This only affects plugins started by this client (Cmd or RunnerFunc);
	// reattached processes are handled by their runner.
	ExitGracefulTimeout time.Duration

	// If non-nil, then the stderr of the client will be written to here
	// (as well as the log). This is the original os.Stderr of the subprocess.
	// This isn't the output of synced stderr.
	Stderr io.Writer

	// SyncStdout, SyncStderr can be set to override the
	// respective os.Std* values in the plugin. Care should be taken to
	// avoid races here. If these are nil, then this will be set to
	// io.Discard.
	SyncStdout io.Writer
	SyncStderr io.Writer

	// AllowedProtocols is a list of allowed protocols. If this isn't set,
	// then only netrpc is allowed. This is so that older go-plugin systems
	// can show friendly errors if they see a plugin with an unknown
	// protocol.
	//
	// By setting this, you can cause an error immediately on plugin start
	// if an unsupported protocol is used with a good error message.
	//
	// If this isn't set at all (nil value), then only net/rpc is accepted.
	// This is done for legacy reasons. You must explicitly opt-in to
	// new protocols.
	AllowedProtocols []Protocol

	// Logger is the logger that the client will used. If none is provided,
	// it will default to hclog's default logger.
	Logger hclog.Logger

	// PluginLogBufferSize is the buffer size(bytes) to read from stderr for plugin log lines.
	// If this is 0, then the default of 64KB is used.
	PluginLogBufferSize int

	// AutoMTLS has the client and server automatically negotiate mTLS for
	// transport authentication. This ensures that only the original client will
	// be allowed to connect to the server, and all other connections will be
	// rejected. The client will also refuse to connect to any server that isn't
	// the original instance started by the client.
	//
	// In this mode of operation, the client generates a one-time use tls
	// certificate, sends the public x.509 certificate to the new server, and
	// the server generates a one-time use tls certificate, and sends the public
	// x.509 certificate back to the client. These are used to authenticate all
	// rpc connections between the client and server.
	//
	// Setting AutoMTLS to true implies that the server must support the
	// protocol, and correctly negotiate the tls certificates, or a connection
	// failure will result.
	//
	// The client should not set TLSConfig, nor should the server set a
	// TLSProvider, because AutoMTLS implies that a new certificate and tls
	// configuration will be generated at startup.
	//
	// You cannot Reattach to a server with this option enabled.
	AutoMTLS bool

	// GRPCDialOptions allows plugin users to pass custom grpc.DialOption
	// to create gRPC connections. This only affects plugins using the gRPC
	// protocol.
	GRPCDialOptions []grpc.DialOption

	// GRPCBrokerMultiplex turns on multiplexing for the gRPC broker. The gRPC
	// broker will multiplex all brokered gRPC servers over the plugin's original
	// listener socket instead of making a new listener for each server. The
	// go-plugin library currently only includes a Go implementation for the
	// server (i.e. plugin) side of gRPC broker multiplexing.
	//
	// Does not support reattaching.
	//
	// Multiplexed gRPC streams MUST be established sequentially, i.e. after
	// calling AcceptAndServe from one side, wait for the other side to Dial
	// before calling AcceptAndServe again.
	GRPCBrokerMultiplex bool

	// SkipHostEnv allows plugins to run without inheriting the parent process'
	// environment variables.
	SkipHostEnv bool

	// UnixSocketConfig configures additional options for any Unix sockets
	// that are created. Not normally required. Not supported on Windows.
	UnixSocketConfig *UnixSocketConfig
}

type UnixSocketConfig struct {
	// If set, go-plugin will change the owner of any Unix sockets created to
	// this group, and set them as group-writable. Can be a name or gid. The
	// client process must be a member of this group or chown will fail.
	Group string

	// TempDir specifies the base directory to use when creating a plugin-specific
	// temporary directory. It is expected to already exist and be writable. If
	// not set, defaults to the directory chosen by os.MkdirTemp.
	TempDir string

	// The directory to create Unix sockets in. Internally created and managed
	// by go-plugin and deleted when the plugin is killed. Will be created
	// inside TempDir if specified.
	socketDir string
}

// ReattachConfig is used to configure a client to reattach to an
// already-running plugin process. You can retrieve this information by
// calling ReattachConfig on Client.
type ReattachConfig struct {
	Protocol        Protocol
	ProtocolVersion int
	Addr            net.Addr
	Pid             int

	// ReattachFunc allows consumers to provide their own implementation of
	// runner.AttachedRunner and attach to something other than a plain process.
	// At least one of Pid or ReattachFunc must be set.
	ReattachFunc runner.ReattachFunc

	// Test is set to true if this is reattaching to to a plugin in "test mode"
	// (see ServeConfig.Test). In this mode, client.Kill will NOT kill the
	// process and instead will rely on the plugin to terminate itself. This
	// should not be used in non-test environments.
	Test bool
}

// SecureConfig is used to configure a client to verify the integrity of an
// executable before running. It does this by verifying the checksum is
// expected. Hash is used to specify the hashing method to use when checksumming
// the file.  The configuration is verified by the client by calling the
// SecureConfig.Check() function.
//
// The host process should ensure the checksum was provided by a trusted and
// authoritative source. The binary should be installed in such a way that it
// can not be modified by an unauthorized user between the time of this check
// and the time of execution.
type SecureConfig struct {
	Checksum []byte
	Hash     hash.Hash
}

// Check takes the filepath to an executable and returns true if the checksum of
// the file matches the checksum provided in the SecureConfig.
func (s *SecureConfig) Check(filePath string) (bool, error) {
	if len(s.Checksum) == 0 {
		return false, ErrSecureConfigNoChecksum
	}

	if s.Hash == nil {
		return false, ErrSecureConfigNoHash
	}

	file, err := os.Open(filePath)
	if err != nil {
		return false, err
	}
	defer func() { _ = file.Close() }()

	_, err = io.Copy(s.Hash, file)
	if err != nil {
		return false, err
	}

	sum := s.Hash.Sum(nil)

	return subtle.ConstantTimeCompare(sum, s.Checksum) == 1, nil
}

// This makes sure all the managed subprocesses are killed and properly
// logged. This should be called before the parent process running the
// plugins exits.
//
// This must only be called _once_.
func CleanupClients() {
	// Set the killed to true so that we don't get unexpected panics
	atomic.StoreUint32(&Killed, 1)

	// Kill all the managed clients in parallel and use a WaitGroup
	// to wait for them all to finish up.
	var wg sync.WaitGroup
	managedClientsLock.Lock()
	for _, client := range managedClients {
		wg.Add(1)

		go func(client *Client) {
			client.Kill()
			wg.Done()
		}(client)
	}
	managedClientsLock.Unlock()

	wg.Wait()
}

// NewClient creates a new plugin client which manages the lifecycle of an external
// plugin and gets the address for the RPC connection.
//
// The client must be cleaned up at some point by calling Kill(). If
// the client is a managed client (created with ClientConfig.Managed) you
// can just call CleanupClients at the end of your program and they will
// be properly cleaned.
func NewClient(config *ClientConfig) (c *Client) {
	if config.MinPort == 0 && config.MaxPort == 0 {
		config.MinPort = 10000
		config.MaxPort = 25000
	}

	if config.StartTimeout == 0 {
		config.StartTimeout = 1 * time.Minute
	}

	if config.ExitGracefulTimeout <= 0 {
		config.ExitGracefulTimeout = defaultExitGracefulTimeout
	}

	if config.Stderr == nil {
		config.Stderr = io.Discard
	}

	if config.SyncStdout == nil {
		config.SyncStdout = io.Discard
	}
	if config.SyncStderr == nil {
		config.SyncStderr = io.Discard
	}

	if config.AllowedProtocols == nil {
		config.AllowedProtocols = []Protocol{ProtocolNetRPC}
	}

	if config.Logger == nil {
		config.Logger = hclog.New(&hclog.LoggerOptions{
			Output: hclog.DefaultOutput,
			Level:  hclog.Trace,
			Name:   "plugin",
		})
	}

	if config.PluginLogBufferSize == 0 {
		config.PluginLogBufferSize = defaultPluginLogBufferSize
	}

	c = &Client{
		config: config,
		logger: config.Logger,
	}
	if config.Managed {
		managedClientsLock.Lock()
		managedClients = append(managedClients, c)
		managedClientsLock.Unlock()
	}

	return
}

// Client returns the protocol client for this connection.
//
// Subsequent calls to this will return the same client.
func (c *Client) Client() (ClientProtocol, error) {
	_, err := c.Start()
	if err != nil {
		return nil, err
	}

	c.l.Lock()
	defer c.l.Unlock()

	if c.client != nil {
		return c.client, nil
	}

	switch c.protocol {
	case ProtocolNetRPC:
		c.client, err = newRPCClient(c)

	case ProtocolGRPC:
		c.client, err = newGRPCClient(c.doneCtx, c)

	default:
		return nil, fmt.Errorf("unknown server protocol: %s", c.protocol)
	}

	if err != nil {
		c.client = nil
		return nil, err
	}

	return c.client, nil
}

// Tells whether or not the underlying process has exited.
func (c *Client) Exited() bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.exited
}

// HandshakeState reports the current state of the plugin's initial handshake.
// This is primarily intended for diagnostics and testing.
func (c *Client) HandshakeState() string {
	c.l.Lock()
	defer c.l.Unlock()
	return c.handshakeState.String()
}

// ExitError returns information about how the plugin process ended, including
// its exit code, any terminating signal, and the tail of its stderr output. It
// returns nil if the process has not yet exited.
func (c *Client) ExitError() *ExitError {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.exitErr
}

// killed is used in tests to check if a process failed to exit gracefully, and
// needed to be killed.
func (c *Client) killed() bool {
	c.l.Lock()
	defer c.l.Unlock()
	return c.processKilled
}

// End the executing subprocess (if it is running) and perform any cleanup
// tasks necessary such as capturing any remaining logs and so on.
//
// This method blocks until the process successfully exits.
//
// This method can safely be called multiple times, including concurrently:
// after the first call has shut the client down, subsequent calls are
// no-ops. Once Kill has been called, Start and Client return ErrClientClosed
// instead of reusing the dead process.
func (c *Client) Kill() {
	// Serialize concurrent Kill calls so the graceful then forced shutdown
	// sequence runs exactly once.
	c.killMu.Lock()
	defer c.killMu.Unlock()

	// Grab a lock to read some private fields and snapshot the closed flag.
	c.l.Lock()
	runner := c.runner
	addr := c.address
	hostSocketDir := c.unixSocketCfg.socketDir
	alreadyClosed := c.closed
	protocolClient := c.client
	c.closed = true
	c.l.Unlock()

	// A second (or later) call has nothing to do. This also covers clients
	// that were never started, or whose startup failed before a runner existed.
	if alreadyClosed || runner == nil || runner.ID() == "" {
		return
	}

	defer func() {
		// Wait for all client goroutines (the process waiter, stdout scanner
		// and stderr logger) to finish, so the process is fully reaped and no
		// zombie or open pipe remains before Kill returns.
		c.clientWaitGroup.Wait()

		if hostSocketDir != "" {
			_ = os.RemoveAll(hostSocketDir)
		}

		// Make sure there is no reference to the old process after it has been
		// killed.
		c.l.Lock()
		c.runner = nil
		c.l.Unlock()
	}()

	// We need to check for address here. It is possible that the plugin
	// started (process != nil) but has no address (addr == nil) if the
	// plugin failed at startup. If we do have an address, we need to close
	// the plugin net connections.
	graceful := false
	if addr != nil {
		// Reuse the already-negotiated protocol client if there is one.
		// Calling c.Client() here would try to (re)start the closed client.
		client := protocolClient
		if client == nil {
			built, err := c.newProtocolClient()
			if err != nil {
				// On error newProtocolClient may return a typed nil wrapped in
				// the interface; drop it so the nil check below is accurate.
				c.logger.Error("client", "error", err)
				client = nil
			} else {
				client = built
			}
		}

		// Guard against a typed nil wrapped in the interface.
		if !isNilClient(client) {
			// Close the client to cleanly exit the process.
			err := client.Close()

			// If there is no error, then we attempt to wait for a graceful
			// exit. If there was an error, we assume that graceful cleanup
			// won't happen and just force kill.
			graceful = err == nil
			if err != nil {
				// If there was an error just log it. We're going to force
				// kill in a moment anyways.
				c.logger.Warn("error closing client during Kill", "err", err)
			}
		}
	}

	// If we're attempting a graceful exit, then we wait for a short period
	// of time to allow that to happen. To wait for this we just wait on the
	// doneCh which would be closed if the process exits.
	if graceful {
		select {
		case <-c.doneCtx.Done():
			c.logger.Debug("plugin exited")
			return
		case <-time.After(c.config.ExitGracefulTimeout):
		}
	}

	// If graceful exiting failed, just kill it
	c.logger.Warn("plugin failed to exit gracefully")
	if err := runner.Kill(context.Background()); err != nil {
		c.logger.Debug("error killing plugin", "error", err)
	}

	c.l.Lock()
	c.processKilled = true
	c.l.Unlock()
}

// newProtocolClient builds the ClientProtocol implementation for a running
// plugin without mutating the cached c.client field. It is used by Kill when
// the protocol client was never created (e.g. Start succeeded but Client() was
// never called). The caller must not hold c.l. On failure it always returns a
// nil interface so callers can use a plain nil comparison.
func (c *Client) newProtocolClient() (ClientProtocol, error) {
	switch c.protocol {
	case ProtocolNetRPC:
		client, err := newRPCClient(c)
		if err != nil {
			return nil, err
		}
		return client, nil

	case ProtocolGRPC:
		client, err := newGRPCClient(c.doneCtx, c)
		if err != nil {
			return nil, err
		}
		return client, nil

	default:
		return nil, fmt.Errorf("unknown server protocol: %s", c.protocol)
	}
}

// isNilClient reports whether the protocol client is unusable, covering both
// the untyped nil case and a typed nil pointer boxed into the interface.
func isNilClient(client ClientProtocol) bool {
	if client == nil {
		return true
	}
	v := reflect.ValueOf(client)
	switch v.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface:
		return v.IsNil()
	default:
		return false
	}
}

// Start the underlying subprocess, communicating with it to negotiate
// a port for RPC connections, and returning the address to connect via RPC.
//
// This method is safe to call multiple times while the client is alive:
// subsequent calls return the negotiated address without restarting the
// process. Once a client has been closed with Kill it cannot be started
// again; this returns ErrClientClosed. Create a new client in that case.
func (c *Client) Start() (addr net.Addr, err error) {
	c.l.Lock()
	defer c.l.Unlock()

	if c.closed {
		return nil, ErrClientClosed
	}

	if c.address != nil {
		return c.address, nil
	}

	// If one of cmd or reattach isn't set, then it is an error. We wrap
	// this in a {} for scoping reasons, and hopeful that the escape
	// analysis will pop the stack here.
	{
		var mutuallyExclusiveOptions int
		if c.config.Cmd != nil {
			mutuallyExclusiveOptions += 1
		}
		if c.config.Reattach != nil {
			mutuallyExclusiveOptions += 1
		}
		if c.config.RunnerFunc != nil {
			mutuallyExclusiveOptions += 1
		}
		if mutuallyExclusiveOptions != 1 {
			return nil, fmt.Errorf("exactly one of Cmd, or Reattach, or RunnerFunc must be set")
		}

		if c.config.SecureConfig != nil && c.config.Reattach != nil {
			return nil, ErrSecureConfigAndReattach
		}

		if c.config.GRPCBrokerMultiplex && c.config.Reattach != nil {
			return nil, fmt.Errorf("gRPC broker multiplexing is not supported with Reattach config")
		}
	}

	if c.config.Reattach != nil {
		return c.reattach()
	}

	if c.config.VersionedPlugins == nil {
		c.config.VersionedPlugins = make(map[int]PluginSet)
	}

	// handle all plugins as versioned, using the handshake config as the default.
	version := int(c.config.ProtocolVersion)

	// Make sure we're not overwriting a real version 0. If ProtocolVersion was
	// non-zero, then we have to just assume the user made sure that
	// VersionedPlugins doesn't conflict.
	if _, ok := c.config.VersionedPlugins[version]; !ok && c.config.Plugins != nil {
		c.config.VersionedPlugins[version] = c.config.Plugins
	}

	var versionStrings []string
	for v := range c.config.VersionedPlugins {
		versionStrings = append(versionStrings, strconv.Itoa(v))
	}

	env := []string{
		fmt.Sprintf("%s=%s", c.config.MagicCookieKey, c.config.MagicCookieValue),
		fmt.Sprintf("PLUGIN_MIN_PORT=%d", c.config.MinPort),
		fmt.Sprintf("PLUGIN_MAX_PORT=%d", c.config.MaxPort),
		fmt.Sprintf("PLUGIN_PROTOCOL_VERSIONS=%s", strings.Join(versionStrings, ",")),
	}
	if c.config.GRPCBrokerMultiplex {
		env = append(env, fmt.Sprintf("%s=true", envMultiplexGRPC))
	}

	cmd := c.config.Cmd
	if cmd == nil {
		// It's only possible to get here if RunnerFunc is non-nil, but we'll
		// still use cmd as a spec to populate metadata for the external
		// implementation to consume.
		cmd = exec.Command("")
	}
	if !c.config.SkipHostEnv {
		cmd.Env = append(cmd.Env, os.Environ()...)
	}
	cmd.Env = append(cmd.Env, env...)
	cmd.Stdin = os.Stdin

	if c.config.SecureConfig != nil {
		if ok, err := c.config.SecureConfig.Check(cmd.Path); err != nil {
			return nil, fmt.Errorf("error verifying checksum: %s", err)
		} else if !ok {
			return nil, ErrChecksumsDoNotMatch
		}
	}

	// Setup a temporary certificate for client/server mtls, and send the public
	// certificate to the plugin.
	if c.config.AutoMTLS {
		c.logger.Debug("configuring client automatic mTLS")
		certPEM, keyPEM, err := generateCert()
		if err != nil {
			c.logger.Error("failed to generate client certificate", "error", err)
			return nil, err
		}
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			c.logger.Error("failed to parse client certificate", "error", err)
			return nil, err
		}

		cmd.Env = append(cmd.Env, fmt.Sprintf("PLUGIN_CLIENT_CERT=%s", certPEM))

		c.config.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			MinVersion:   tls.VersionTLS12,
			ServerName:   "localhost",
		}
	}

	if c.config.UnixSocketConfig != nil {
		c.unixSocketCfg = *c.config.UnixSocketConfig
	}

	if c.unixSocketCfg.Group != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", EnvUnixSocketGroup, c.unixSocketCfg.Group))
	}

	var runner runner.Runner
	switch {
	case c.config.RunnerFunc != nil:
		c.unixSocketCfg.socketDir, err = os.MkdirTemp(c.unixSocketCfg.TempDir, "plugin-dir")
		if err != nil {
			return nil, err
		}
		// os.MkdirTemp creates folders with 0o700, so if we have a group
		// configured we need to make it group-writable.
		if c.unixSocketCfg.Group != "" {
			err = setGroupWritable(c.unixSocketCfg.socketDir, c.unixSocketCfg.Group, 0o770)
			if err != nil {
				return nil, err
			}
		}
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", EnvUnixSocketDir, c.unixSocketCfg.socketDir))
		c.logger.Trace("created temporary directory for unix sockets", "dir", c.unixSocketCfg.socketDir)

		runner, err = c.config.RunnerFunc(c.logger, cmd, c.unixSocketCfg.socketDir)
		if err != nil {
			return nil, err
		}
	default:
		runner, err = cmdrunner.NewCmdRunner(c.logger, cmd)
		if err != nil {
			return nil, err
		}

	}

	// Record the lifecycle state of this handshake attempt.
	c.stderrTail = newStderrTail(defaultPluginStderrTailSize)
	c.exitCh = make(chan *ExitError, 1)
	c.handshakeState = handshakeInProgress
	defer func() {
		if err != nil {
			c.handshakeState = handshakeFailed
			// A failed handshake leaves a dead process behind; mark the client
			// closed so a subsequent Start/Client returns ErrClientClosed rather
			// than silently reusing stale state.
			c.closed = true
		}
	}()

	c.runner = runner
	startCtx, startCtxCancel := context.WithTimeout(context.Background(), c.config.StartTimeout)
	defer startCtxCancel()
	err = runner.Start(startCtx)
	if err != nil {
		return nil, err
	}

	// Make sure the command is properly cleaned up if there is an error
	reaped := false
	forceKilled := false
	defer func() {
		rErr := recover()

		if err != nil || rErr != nil {
			if !reaped {
				_ = runner.Kill(context.Background())
				forceKilled = true
				// Reap the process synchronously so that by the time Start
				// returns the subprocess is gone (no zombie, no lingering temp
				// directory) and c.exited/c.exitErr have been recorded.
				if c.exitCh != nil {
					select {
					case <-c.exitCh:
						reaped = true
					case <-time.After(c.config.ExitGracefulTimeout):
					}
				}
			}
			if forceKilled {
				c.processKilled = true
			}
			// The client is marked closed, so a later Kill will short-circuit;
			// remove the socket directory here rather than relying on Kill.
			if c.unixSocketCfg.socketDir != "" {
				_ = os.RemoveAll(c.unixSocketCfg.socketDir)
			}
		}

		if rErr != nil {
			panic(rErr)
		}
	}()

	// Create a context for when we kill
	c.doneCtx, c.ctxCancel = context.WithCancel(context.Background())

	// Add two to pipesWaitGroup: one for logStderr, one for the goroutine
	// below that consumes Stdout.  We mustn't continue to Add once we might Wait.
	c.pipesWaitGroup.Add(2)

	// Start goroutine that logs the stderr
	c.clientWaitGroup.Add(1)
	// logStderr calls c.pipesWaitGroup.Done()
	go c.logStderr(runner.Name(), runner.Stderr(), c.stderrTail)

	c.clientWaitGroup.Add(1)
	go func() {
		// ensure the context is cancelled when we're done
		defer c.ctxCancel()

		defer c.clientWaitGroup.Done()

		// wait to finish reading from stdout/stderr since the stdout/stderr pipe readers
		// will be closed by the subsequent call to cmd.Wait().
		c.pipesWaitGroup.Wait()

		// Wait for the command to end.
		waitErr := runner.Wait(context.Background())
		exitErr := newExitError(waitErr, c.stderrTail)
		if waitErr != nil {
			c.logger.Error("plugin process exited", "plugin", runner.Name(), "id", runner.ID(), "error", waitErr.Error())
		} else {
			// Log and make sure to flush the logs right away
			c.logger.Info("plugin process exited", "plugin", runner.Name(), "id", runner.ID())
		}

		_ = os.Stderr.Sync()

		// Record how the process ended and publish it. We use stateMu rather
		// than l because Start holds l throughout the handshake and may be
		// waiting on exitCh below; taking l here would deadlock.
		c.stateMu.Lock()
		c.exitErr = exitErr
		c.exited = true
		c.stateMu.Unlock()

		// Publish after recording. The channel is buffered, so this never
		// blocks and a Start waiting on it will observe the stored state too.
		c.exitCh <- exitErr
	}()

	// Start a goroutine that is going to be reading the lines
	// out of stdout
	linesCh := make(chan string)
	c.clientWaitGroup.Add(1)
	go func() {
		defer c.clientWaitGroup.Done()
		defer c.pipesWaitGroup.Done()
		defer close(linesCh)

		scanner := bufio.NewScanner(runner.Stdout())
		for scanner.Scan() {
			linesCh <- scanner.Text()
		}
		if scanner.Err() != nil {
			c.logger.Error("error encountered while scanning stdout", "error", scanner.Err())
		}
	}()

	// Make sure after we exit we read the lines from stdout forever
	// so they don't block since it is a pipe.
	// The scanner goroutine above will close this, but track it with a wait
	// group for completeness.
	c.clientWaitGroup.Add(1)
	defer func() {
		go func() {
			defer c.clientWaitGroup.Done()
			for range linesCh {
			}
		}()
	}()

	// Some channels for the next step
	timeout := time.After(c.config.StartTimeout)

	// Start looking for the address
	c.logger.Debug("waiting for RPC address", "plugin", runner.Name())
	select {
	case <-timeout:
		// The plugin did not finish the handshake in time. Stop it and wait
		// for the process to be reaped so no zombie or lingering process (or
		// its temporary directory) survives the failed start.
		if killErr := runner.Kill(context.Background()); killErr != nil {
			c.logger.Debug("error killing plugin after start timeout", "error", killErr)
		}
		forceKilled = true
		exitErr := c.waitForExitError(c.config.ExitGracefulTimeout)
		reaped = true
		err = fmt.Errorf("%w: %s", ErrPluginStartTimeout, exitSummary(exitErr, c.stderrTail))
	case <-c.doneCtx.Done():
		// The process ended before producing a handshake. Wait for the reaper
		// to record the exit details so the error can distinguish a clean exit
		// from a crash and include the stderr tail.
		exitErr := c.waitForExitError(c.config.ExitGracefulTimeout)
		reaped = true
		switch {
		case exitErr != nil && exitErr.Crashed():
			err = fmt.Errorf("%w: %w", ErrPluginExited, exitErr)
		default:
			err = fmt.Errorf("%w: %s", ErrPluginExited, exitSummary(exitErr, c.stderrTail))
		}
	case line, ok := <-linesCh:
		// If the stdout pipe closed without a handshake line, the process has
		// gone away. Wait briefly for the reaper so we report the real exit
		// cause (exit code/signal plus stderr tail) instead of a generic
		// "unrecognized message" error.
		if !ok {
			exitErr := c.waitForExitError(c.config.ExitGracefulTimeout)
			reaped = true
			switch {
			case exitErr != nil && exitErr.Crashed():
				err = fmt.Errorf("%w: %w", ErrPluginExited, exitErr)
			default:
				err = fmt.Errorf("%w: %s", ErrPluginExited, exitSummary(exitErr, c.stderrTail))
			}
			return
		}

		// Trim the line and split by "|" in order to get the parts of
		// the output.
		line = strings.TrimSpace(line)
		parts := strings.Split(line, "|")
		if len(parts) < 4 {
			errText := fmt.Sprintf("Unrecognized remote plugin message: %s", line)
			if !ok {
				errText += "\n" + "Failed to read any lines from plugin's stdout"
			}
			additionalNotes := runner.Diagnose(context.Background())
			if additionalNotes != "" {
				errText += "\n" + additionalNotes
			}
			err = errors.New(errText)
			return
		}

		// Check the core protocol. Wrapped in a {} for scoping.
		{
			var coreProtocol int
			coreProtocol, err = strconv.Atoi(parts[0])
			if err != nil {
				err = fmt.Errorf("error parsing core protocol version: %s", err)
				return
			}

			if coreProtocol != CoreProtocolVersion {
				err = fmt.Errorf("incompatible core API version with plugin. "+
					"Plugin version: %s, Core version: %d\n\n"+
					"To fix this, the plugin usually only needs to be recompiled.\n"+
					"Please report this to the plugin author", parts[0], CoreProtocolVersion)
				return
			}
		}

		// Test the API version
		version, pluginSet, err := c.checkProtoVersion(parts[1])
		if err != nil {
			return addr, err
		}

		// set the Plugins value to the compatible set, so the version
		// doesn't need to be passed through to the ClientProtocol
		// implementation.
		c.config.Plugins = pluginSet
		c.negotiatedVersion = version
		c.logger.Debug("using plugin", "version", version)

		network, address, err := runner.PluginToHost(parts[2], parts[3])
		if err != nil {
			return addr, err
		}

		switch network {
		case "tcp":
			addr, err = net.ResolveTCPAddr("tcp", address)
			if err != nil {
				return nil, err
			}
		case "unix":
			addr, err = net.ResolveUnixAddr("unix", address)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unknown address type: %s", address)
		}

		// If we have a server type, then record that. We default to net/rpc
		// for backwards compatibility.
		c.protocol = ProtocolNetRPC
		if len(parts) >= 5 {
			c.protocol = Protocol(parts[4])
		}

		found := false
		for _, p := range c.config.AllowedProtocols {
			if p == c.protocol {
				found = true
				break
			}
		}
		if !found {
			err = fmt.Errorf("unsupported plugin protocol %q. Supported: %v",
				c.protocol, c.config.AllowedProtocols)
			return addr, err
		}

		// See if we have a TLS certificate from the server.
		// Checking if the length is > 50 rules out catching the unused "extra"
		// data returned from some older implementations.
		if len(parts) >= 6 && len(parts[5]) > 50 {
			err := c.loadServerCert(parts[5])
			if err != nil {
				return nil, fmt.Errorf("error parsing server cert: %s", err)
			}
		}

		if c.config.GRPCBrokerMultiplex && c.protocol == ProtocolGRPC {
			if len(parts) <= 6 {
				return nil, fmt.Errorf("%w; for Go plugins, you will need to update the "+
					"github.com/hashicorp/go-plugin dependency and recompile", ErrGRPCBrokerMuxNotSupported)
			}
			if muxSupported, err := strconv.ParseBool(parts[6]); err != nil {
				return nil, fmt.Errorf("error parsing %q as a boolean for gRPC broker multiplexing support", parts[6])
			} else if !muxSupported {
				return nil, ErrGRPCBrokerMuxNotSupported
			}
		}
	}

	c.address = addr
	c.handshakeState = handshakeReady
	return
}

// waitForExitError blocks until the process waiter goroutine publishes the
// ExitError, or until wait elapses. On timeout it snapshots whatever stderr has
// been captured so the caller still gets a useful error. It must be called
// while holding c.l (Start holds it for its whole duration). The value is
// consumed from the channel; the waiter goroutine also stores it on the client,
// which is what Kill (after waiting for the goroutines) reads.
func (c *Client) waitForExitError(wait time.Duration) *ExitError {
	if c.exitCh == nil {
		return nil
	}

	select {
	case exitErr := <-c.exitCh:
		return exitErr
	case <-time.After(wait):
		return &ExitError{ExitCode: -1, Stderr: tailString(c.stderrTail)}
	}
}

// exitSummary renders an ExitError for inclusion in a startup error, falling
// back to the retained stderr when the process has not been reaped yet.
func exitSummary(exitErr *ExitError, stderr *stderrTail) string {
	if exitErr != nil {
		return exitErr.Error()
	}
	if s := tailString(stderr); s != "" {
		return s
	}
	return "no exit status available"
}

// loadServerCert is used by AutoMTLS to read an x.509 cert returned by the
// server, and load it as the RootCA and ClientCA for the client TLSConfig.
func (c *Client) loadServerCert(cert string) error {
	certPool := x509.NewCertPool()

	asn1, err := base64.RawStdEncoding.DecodeString(cert)
	if err != nil {
		return err
	}

	x509Cert, err := x509.ParseCertificate([]byte(asn1))
	if err != nil {
		return err
	}

	certPool.AddCert(x509Cert)

	c.config.TLSConfig.RootCAs = certPool
	c.config.TLSConfig.ClientCAs = certPool
	return nil
}

func (c *Client) reattach() (net.Addr, error) {
	reattachFunc := c.config.Reattach.ReattachFunc
	// For backwards compatibility default to cmdrunner.ReattachFunc
	if reattachFunc == nil {
		reattachFunc = cmdrunner.ReattachFunc(c.config.Reattach.Pid, c.config.Reattach.Addr)
	}

	r, err := reattachFunc()
	if err != nil {
		return nil, err
	}

	// Create a context for when we kill
	c.doneCtx, c.ctxCancel = context.WithCancel(context.Background())

	c.clientWaitGroup.Add(1)
	// Goroutine to mark exit status
	go func(r runner.AttachedRunner) {
		defer c.clientWaitGroup.Done()

		// ensure the context is cancelled when we're done
		defer c.ctxCancel()

		// Wait for the process to die
		waitErr := r.Wait(context.Background())

		// Log so we can see it
		c.logger.Debug("reattached plugin process exited")

		// Mark it. stateMu is used rather than l so this can never deadlock
		// with Start, which holds l.
		c.stateMu.Lock()
		c.exitErr = newExitError(waitErr, nil)
		c.exited = true
		c.stateMu.Unlock()
	}(r)

	// Set the address and protocol
	c.address = c.config.Reattach.Addr
	c.protocol = c.config.Reattach.Protocol
	if c.protocol == "" {
		// Default the protocol to net/rpc for backwards compatibility
		c.protocol = ProtocolNetRPC
	}

	// A reattached process has already completed its handshake elsewhere.
	c.handshakeState = handshakeReady

	if c.config.Reattach.Test {
		c.negotiatedVersion = c.config.Reattach.ProtocolVersion
	} else {
		// If we're in test mode, we do NOT set the runner. This avoids the
		// runner being killed (the only purpose we have for setting c.runner
		// when reattaching), since in test mode the process is responsible for
		// exiting on its own.
		c.runner = r
	}

	return c.address, nil
}

// checkProtoVersion returns the negotiated version and PluginSet.
// This returns an error if the server returned an incompatible protocol
// version, or an invalid handshake response.
func (c *Client) checkProtoVersion(protoVersion string) (int, PluginSet, error) {
	serverVersion, err := strconv.Atoi(protoVersion)
	if err != nil {
		return 0, nil, fmt.Errorf("Error parsing protocol version %q: %s", protoVersion, err)
	}

	// record these for the error message
	var clientVersions []int

	// all versions, including the legacy ProtocolVersion have been added to
	// the versions set
	for version, plugins := range c.config.VersionedPlugins {
		clientVersions = append(clientVersions, version)

		if serverVersion != version {
			continue
		}
		return version, plugins, nil
	}

	return 0, nil, fmt.Errorf("incompatible API version with plugin. "+
		"Plugin version: %d, Client versions: %d", serverVersion, clientVersions)
}

// ReattachConfig returns the information that must be provided to NewClient
// to reattach to the plugin process that this client started. This is
// useful for plugins that detach from their parent process.
//
// If this returns nil then the process hasn't been started yet. Please
// call Start or Client before calling this.
//
// Clients who specified a RunnerFunc will need to populate their own
// ReattachFunc in the returned ReattachConfig before it can be used.
func (c *Client) ReattachConfig() *ReattachConfig {
	c.l.Lock()
	defer c.l.Unlock()

	if c.address == nil {
		return nil
	}

	if c.config.Cmd != nil && c.config.Cmd.Process == nil {
		return nil
	}

	// If we connected via reattach, just return the information as-is
	if c.config.Reattach != nil {
		return c.config.Reattach
	}

	reattach := &ReattachConfig{
		Protocol: c.protocol,
		Addr:     c.address,
	}

	if c.config.Cmd != nil && c.config.Cmd.Process != nil {
		reattach.Pid = c.config.Cmd.Process.Pid
	}

	return reattach
}

// Protocol returns the protocol of server on the remote end. This will
// start the plugin process if it isn't already started. Errors from
// starting the plugin are surpressed and ProtocolInvalid is returned. It
// is recommended you call Start explicitly before calling Protocol to ensure
// no errors occur.
func (c *Client) Protocol() Protocol {
	_, err := c.Start()
	if err != nil {
		return ProtocolInvalid
	}

	return c.protocol
}

func netAddrDialer(addr net.Addr) func(context.Context, string) (net.Conn, error) {
	return func(context.Context, string) (net.Conn, error) {
		// Connect to the client
		conn, err := net.Dial(addr.Network(), addr.String())
		if err != nil {
			return nil, err
		}
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			// Make sure to set keep alive so that the connection doesn't die
			_ = tcpConn.SetKeepAlive(true)
		}

		return conn, nil
	}
}

// dialer is compatible with grpc.WithDialer and creates the connection
// to the plugin.
func (c *Client) dialer(ctx context.Context, _ string) (net.Conn, error) {
	muxer, err := c.getGRPCMuxer(c.address)
	if err != nil {
		return nil, err
	}

	var conn net.Conn
	if muxer.Enabled() {
		conn, err = muxer.Dial()
		if err != nil {
			return nil, err
		}
	} else {
		conn, err = netAddrDialer(c.address)(ctx, "")
		if err != nil {
			return nil, err
		}
	}

	// If we have a TLS config we wrap our connection. We only do this
	// for net/rpc since gRPC uses its own mechanism for TLS.
	if c.protocol == ProtocolNetRPC && c.config.TLSConfig != nil {
		conn = tls.Client(conn, c.config.TLSConfig)
	}

	return conn, nil
}

func (c *Client) getGRPCMuxer(addr net.Addr) (*grpcmux.GRPCClientMuxer, error) {
	if c.protocol != ProtocolGRPC || !c.config.GRPCBrokerMultiplex {
		return nil, nil
	}

	var err error
	c.grpcMuxerOnce.Do(func() {
		c.grpcMuxer, err = grpcmux.NewGRPCClientMuxer(c.logger, addr)
	})
	if err != nil {
		return nil, err
	}

	return c.grpcMuxer, nil
}

func (c *Client) logStderr(name string, r io.Reader, tail *stderrTail) {
	defer c.clientWaitGroup.Done()
	defer c.pipesWaitGroup.Done()

	l := c.logger.Named(filepath.Base(name))
	loggerLevel := l.GetLevel()
	loggerDisabled := loggerLevel == hclog.Off

	reader := bufio.NewReaderSize(r, c.config.PluginLogBufferSize)
	// continuation indicates the previous line was a prefix
	continuation := false

	// inPanic indicates we saw the start of a stack trace and should divert all
	// remaining untagged lines to stderr
	var inPanic bool

	// writeStderr forwards plugin output both to the user-configured writer and
	// the bounded ring buffer used for crash diagnostics.
	writeStderr := func(p []byte) {
		_, _ = c.config.Stderr.Write(p)
		if tail != nil {
			_, _ = tail.Write(p)
		}
	}

	for {

		line, isPrefix, err := reader.ReadLine()
		switch {
		case err == io.EOF:
			return
		case err != nil:
			l.Error("reading plugin stderr", "error", err)
			return
		}

		writeStderr(line)

		// The line was longer than our max token size, so it's likely
		// incomplete and won't unmarshal.
		if isPrefix || continuation {
			l.Debug(string(line))

			// if we're finishing a continued line, add the newline back in
			if !isPrefix {
				writeStderr([]byte{'\n'})
			}

			continuation = isPrefix
			continue
		}

		writeStderr([]byte{'\n'})

		//
		// Any side-effects other than writing to the hclog logger must be
		// above this point!
		//

		if loggerDisabled {
			// If the logger we'd be writing to is completely disabled then
			// we can skip all of the parsing work to decide what log level
			// we'd use to write this line.
			continue
		}

		entry, err := parseJSON(line)
		// If output is not JSON format, print directly to Debug
		if err != nil {
			// Attempt to infer the desired log level from the commonly used
			// string prefixes
			switch line := string(line); {
			case strings.HasPrefix(line, "[TRACE]"):
				l.Trace(line)
			case strings.HasPrefix(line, "[DEBUG]"):
				l.Debug(line)
			case strings.HasPrefix(line, "[INFO]"):
				l.Info(line)
			case strings.HasPrefix(line, "[WARN]"):
				l.Warn(line)
			case strings.HasPrefix(line, "[ERROR]"):
				l.Error(line)
			case strings.HasPrefix(line, "panic: ") || strings.HasPrefix(line, "fatal error: "):
				inPanic = true
				fallthrough
			case inPanic:
				l.Error(line)
			default:
				l.Debug(line)
			}
		} else {
			logLevel := hclog.LevelFromString(entry.Level)
			if logLevel != hclog.NoLevel && logLevel < loggerLevel {
				// The logger will ignore this log entry anyway, so we
				// won't spend any more time preparing it.
				continue
			}

			out := flattenKVPairs(entry.KVPairs)
			out = append(out, "timestamp", entry.Timestamp.Format(hclog.TimeFormat))
			switch logLevel {
			case hclog.Trace:
				l.Trace(entry.Message, out...)
			case hclog.Debug:
				l.Debug(entry.Message, out...)
			case hclog.Info:
				l.Info(entry.Message, out...)
			case hclog.Warn:
				l.Warn(entry.Message, out...)
			case hclog.Error:
				l.Error(entry.Message, out...)
			default:
				// if there was no log level, it's likely this is unexpected
				// json from something other than hclog, and we should output
				// it verbatim.
				l.Debug(string(line))
			}
		}
	}
}
