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

// cleanupGracePeriod is the minimum amount of time allowed for terminating a
// plugin process and letting its goroutines exit after a failed startup.
// Without it, a startup that consumed its entire StartTimeout would have no
// budget left for cleanup and could block indefinitely on an unresponsive
// process.
const cleanupGracePeriod = 2 * time.Second

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
	address           net.Addr
	runner            runner.AttachedRunner
	client            ClientProtocol
	protocol          Protocol
	logger            hclog.Logger
	doneCtx           context.Context
	ctxCancel         context.CancelFunc
	negotiatedVersion int

	// clientWaitGroup is used to manage the lifecycle of the plugin management
	// goroutines.
	clientWaitGroup sync.WaitGroup

	// pipesWaitGroup is used to prevent the command's Wait() function from
	// being called before we've finished reading from the stdout and stderr pipe.
	pipesWaitGroup sync.WaitGroup

	// processKilled is used for testing only, to flag when the process was
	// forcefully killed.
	processKilled bool

	// cleanedUp is set once releaseResources has run. It is protected by c.l.
	cleanedUp bool

	unixSocketCfg UnixSocketConfig

	grpcMuxerMu     sync.Mutex
	grpcMuxer       *grpcmux.GRPCClientMuxer
	grpcMuxerClosed bool

	// startMu serializes Start calls. Unlike c.l, it is held for the whole
	// duration of startup, which allows the failure path to release c.l before
	// running cleanup.
	startMu sync.Mutex

	// killMu serializes Kill calls.
	killMu sync.Mutex

	// clientMu serializes creation and shutdown of the protocol client so a
	// concurrent Kill can't close it while Client is building it.
	clientMu sync.Mutex

	// cleanupOnce ensures that the resources acquired while starting the
	// plugin are released exactly once. Startup failures and Kill can race to
	// tear the client down.
	cleanupOnce sync.Once

	// startErr records the error from the first failed Start. A client that
	// failed to start cannot be started again, so subsequent calls return the
	// same error instead of reusing a subprocess that has already been torn
	// down. It is protected by c.l.
	startErr error
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
	if _, err := c.Start(); err != nil {
		return nil, err
	}

	// Serialize client creation with Kill, which may simultaneously be
	// shutting the protocol client down.
	c.clientMu.Lock()
	defer c.clientMu.Unlock()

	client, err := c.newProtocolClient()
	if err != nil {
		// The plugin process is running but we couldn't establish the
		// protocol connection (for example, the stdio stream failed to
		// initialize). Tear the process and any partially created resources
		// down through the same path used for startup failures, and remember
		// the error so later calls don't retry against a torn-down process.
		c.l.Lock()
		if c.startErr == nil {
			c.startErr = err
		}
		c.l.Unlock()

		deadline := time.Now().Add(c.config.StartTimeout)
		c.cleanupFailedStart(deadline)
		return nil, err
	}

	return client, nil
}

// newProtocolClient returns the cached protocol client, creating it if
// necessary. The address and protocol must already have been negotiated by
// Start.
func (c *Client) newProtocolClient() (ClientProtocol, error) {
	c.l.Lock()
	defer c.l.Unlock()

	if c.client != nil {
		return c.client, nil
	}

	if c.cleanedUp || c.startErr != nil {
		// The client has already been torn down; don't start a new protocol
		// client against a process that is no longer running.
		if c.startErr != nil {
			return nil, c.startErr
		}
		return nil, fmt.Errorf("plugin client has been closed")
	}

	var err error
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

// takeProtocolClient atomically returns the current protocol client and clears
// the reference, so that it is only closed once.
func (c *Client) takeProtocolClient() ClientProtocol {
	c.l.Lock()
	defer c.l.Unlock()

	client := c.client
	c.client = nil
	return client
}

// Exited tells whether or not the underlying process has exited.
func (c *Client) Exited() bool {
	c.l.Lock()
	defer c.l.Unlock()
	return c.exited
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
// This method can safely be called multiple times.
func (c *Client) Kill() {
	// Serialize Kill calls so a concurrent Kill and process exit don't both
	// attempt a graceful shutdown. releaseResources is also guarded by a
	// sync.Once, so cleanup happens exactly once either way.
	c.killMu.Lock()
	defer c.killMu.Unlock()

	// Grab a lock to read some private fields.
	c.l.Lock()
	runner := c.runner
	addr := c.address
	cleanedUp := c.cleanedUp
	c.l.Unlock()

	// If the client has already been cleaned up, or there is no runner with an
	// ID, there is nothing left to kill. Resources acquired by a failed
	// startup are handled by the startup path itself, and test-mode reattach
	// clients deliberately leave the plugin running and usable.
	if cleanedUp || runner == nil || runner.ID() == "" {
		if cleanedUp {
			// Cleanup already ran; give its goroutines a bounded chance to
			// finish without blocking on a process that never exits.
			c.waitForGoroutines(time.Now().Add(cleanupGracePeriod))
		}
		return
	}

	// We need to check for address here. It is possible that the plugin
	// started (process != nil) but has no address (addr == nil) if the
	// plugin failed at startup. If we do have an address, we need to close
	// the plugin net connections.
	graceful := false
	if addr != nil {
		// Close the client to cleanly exit the process. The client is taken
		// out of the Client first so the cleanup path can't close it a second
		// time.
		client, err := c.takeClientForShutdown()
		if err == nil && client != nil {
			err = client.Close()

			// If there is no error, then we attempt to wait for a graceful
			// exit. If there was an error, we assume that graceful cleanup
			// won't happen and just force kill.
			graceful = err == nil
			if err != nil {
				// If there was an error just log it. We're going to force
				// kill in a moment anyways.
				c.logger.Warn("error closing client during Kill", "err", err)
			}
		} else if err != nil {
			c.logger.Error("client", "error", err)
		}
	}

	// If we're attempting a graceful exit, then we wait for a short period
	// of time to allow that to happen. To wait for this we just wait on the
	// doneCh which would be closed if the process exits.
	if graceful {
		select {
		case <-c.doneCtx.Done():
			c.logger.Debug("plugin exited")
			// The process exited on its own, so release the remaining
			// resources without killing it.
			c.releaseResources(false, time.Time{})
			c.waitForGoroutines(time.Time{})
			return
		case <-time.After(2 * time.Second):
		}
	}

	// If graceful exiting failed, just kill it
	c.logger.Warn("plugin failed to exit gracefully")
	c.releaseResources(true, time.Time{})
	c.waitForGoroutines(time.Time{})
}

// takeClientForShutdown returns the protocol client that should be shut down
// gracefully before the plugin process is killed, creating it if necessary. It
// removes the client from the Client so that releaseResources won't close it
// again.
func (c *Client) takeClientForShutdown() (ClientProtocol, error) {
	c.clientMu.Lock()
	defer c.clientMu.Unlock()

	if client := c.takeProtocolClient(); client != nil {
		return client, nil
	}

	if _, err := c.newProtocolClient(); err != nil {
		return nil, err
	}

	return c.takeProtocolClient(), nil
}

// releaseResources releases the process and protocol resources acquired by
// Start and Client. It is safe to call concurrently from the startup failure
// path and Kill: the work is performed exactly once.
//
// If kill is true, the runner is asked to stop the plugin. The deadline bounds
// how long the kill call may take; a zero deadline means no bound. It must not
// be called while holding c.l.
func (c *Client) releaseResources(kill bool, deadline time.Time) {
	c.cleanupOnce.Do(func() {
		c.l.Lock()
		c.cleanedUp = true
		client := c.client
		c.client = nil
		runner := c.runner
		c.runner = nil
		socketDir := c.unixSocketCfg.socketDir
		c.l.Unlock()

		// Cancel the done context first: it unblocks Start (if it is waiting
		// for the plugin), gRPC shutdown calls, and the broker goroutines.
		if c.ctxCancel != nil {
			c.ctxCancel()
		}

		// Close the protocol connection so the broker and stdio goroutines
		// stop and any sockets are released.
		c.closeProtocolClient(client)
		c.closeGRPCMuxer()

		if kill && runner != nil {
			killCtx := context.Background()
			if !deadline.IsZero() {
				var cancel context.CancelFunc
				killCtx, cancel = context.WithDeadline(killCtx, deadline)
				defer cancel()
			}

			if err := runner.Kill(killCtx); err != nil {
				c.logger.Debug("error killing plugin", "error", err)
			}

			c.l.Lock()
			c.processKilled = true
			c.l.Unlock()
		}

		if socketDir != "" {
			_ = os.RemoveAll(socketDir)
		}
	})
}

// closeProtocolClient closes the protocol client, if it hasn't been closed
// already.
func (c *Client) closeProtocolClient(client ClientProtocol) {
	if client == nil {
		return
	}

	if err := client.Close(); err != nil {
		c.logger.Debug("error closing protocol client", "error", err)
	}
}

// closeGRPCMuxer closes the gRPC broker muxer if it was created. Once closed,
// the muxer is never recreated.
func (c *Client) closeGRPCMuxer() {
	c.grpcMuxerMu.Lock()
	muxer := c.grpcMuxer
	c.grpcMuxer = nil
	c.grpcMuxerClosed = true
	c.grpcMuxerMu.Unlock()

	if muxer != nil {
		_ = muxer.Close()
	}
}

// waitForGoroutines waits for the client goroutines to exit. A zero deadline
// waits indefinitely; otherwise the wait is bounded, which keeps startup
// failures from blocking forever on a process that refuses to exit.
func (c *Client) waitForGoroutines(deadline time.Time) {
	if deadline.IsZero() {
		c.clientWaitGroup.Wait()
		return
	}

	doneCh := make(chan struct{})
	go func() {
		c.clientWaitGroup.Wait()
		close(doneCh)
	}()

	remaining := time.Until(deadline)
	if remaining <= 0 {
		return
	}

	timer := time.NewTimer(remaining)
	defer timer.Stop()

	select {
	case <-doneCh:
	case <-timer.C:
	}
}

// Start the underlying subprocess, communicating with it to negotiate
// a port for RPC connections, and returning the address to connect via RPC.
//
// This method is safe to call multiple times. Subsequent calls have no effect.
// Once a client has been started once, it cannot be started again, even if
// it was killed.
func (c *Client) Start() (addr net.Addr, err error) {
	// Serialize startup attempts. This is separate from c.l so that the
	// failure path can release c.l before tearing the client down.
	c.startMu.Lock()
	defer c.startMu.Unlock()

	// All startup errors, including panics, go through the same cleanup path.
	// Cleanup is bounded by the start deadline so a plugin that refuses to
	// exit can't keep Start from returning.
	startDeadline := time.Now().Add(c.config.StartTimeout)
	locked := false
	failed := false
	defer func() {
		rErr := recover()

		if locked {
			c.l.Unlock()
		}

		if failed || rErr != nil {
			c.cleanupFailedStart(startDeadline)
		}

		if rErr != nil {
			panic(rErr)
		}
	}()

	c.l.Lock()
	locked = true

	if c.address != nil {
		return c.address, nil
	}

	if c.startErr != nil {
		// A previous Start failed and already tore down the subprocess.
		// Starting again would reuse a client that is no longer viable.
		return nil, c.startErr
	}

	addr, err = c.startLocked()
	if err != nil {
		failed = true
		if c.startErr == nil {
			c.startErr = err
		}
		return nil, err
	}

	locked = false
	c.l.Unlock()
	return addr, nil
}

// cleanupFailedStart releases every resource acquired by a failed startup and
// terminates the subprocess. It must not be called while holding c.l.
//
// Start waits for cleanup only until the startup deadline, so a plugin that
// refuses to exit can never keep the caller blocked past StartTimeout. The
// cleanup itself keeps running in the background with its own short grace
// period, so the process is still terminated even when startup consumed the
// entire budget.
func (c *Client) cleanupFailedStart(deadline time.Time) {
	killDeadline := deadline
	if minDeadline := time.Now().Add(cleanupGracePeriod); killDeadline.Before(minDeadline) {
		killDeadline = minDeadline
	}

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		c.releaseResources(true, killDeadline)
		c.waitForGoroutines(killDeadline)
	}()

	remaining := time.Until(deadline)
	if remaining <= 0 {
		c.logger.Debug("startup deadline exceeded, cleaning up plugin in background")
		return
	}

	timer := time.NewTimer(remaining)
	defer timer.Stop()

	select {
	case <-doneCh:
	case <-timer.C:
		c.logger.Debug("timed out waiting for plugin cleanup after failed startup")
	}
}

// startLocked performs the actual startup sequence. The caller must hold c.l.
func (c *Client) startLocked() (addr net.Addr, err error) {
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

	c.runner = runner

	// Create the context used to signal shutdown before the process is
	// started, so that a concurrent Kill or startup failure can always cancel
	// it and unblock any goroutines waiting on the plugin.
	c.doneCtx, c.ctxCancel = context.WithCancel(context.Background())

	startCtx, startCtxCancel := context.WithTimeout(context.Background(), c.config.StartTimeout)
	defer startCtxCancel()
	if err = runner.Start(startCtx); err != nil {
		return nil, err
	}

	// From here on, any error (or panic) is handled by Start's deferred
	// cleanup, which terminates the runner and releases everything Start and
	// Client may have created.

	// Add two to pipesWaitGroup: one for logStderr, one for the goroutine
	// below that consumes Stdout.  We mustn't continue to Add once we might Wait.
	c.pipesWaitGroup.Add(2)

	// Start goroutine that logs the stderr
	c.clientWaitGroup.Add(1)
	// logStderr calls c.pipesWaitGroup.Done()
	go c.logStderr(runner.Name(), runner.Stderr())

	c.clientWaitGroup.Add(1)
	go func() {
		// ensure the context is cancelled when we're done
		defer c.ctxCancel()

		defer c.clientWaitGroup.Done()

		// wait to finish reading from stdout/stderr since the stdout/stderr pipe readers
		// will be closed by the subsequent call to cmd.Wait().
		c.pipesWaitGroup.Wait()

		// Wait for the command to end.
		err := runner.Wait(context.Background())
		if err != nil {
			c.logger.Error("plugin process exited", "plugin", runner.Name(), "id", runner.ID(), "error", err.Error())
		} else {
			// Log and make sure to flush the logs right away
			c.logger.Info("plugin process exited", "plugin", runner.Name(), "id", runner.ID())
		}

		_ = os.Stderr.Sync()

		// Set that we exited, which takes a lock
		c.l.Lock()
		c.exited = true
		c.l.Unlock()
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
		err = errors.New("timeout while waiting for plugin to start")
	case <-c.doneCtx.Done():
		err = errors.New("plugin exited before we could connect")
	case line, ok := <-linesCh:
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
	return
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
		_ = r.Wait(context.Background())

		// Log so we can see it
		c.logger.Debug("reattached plugin process exited")

		// Mark it
		c.l.Lock()
		defer c.l.Unlock()
		c.exited = true
	}(r)

	// Set the address and protocol
	c.address = c.config.Reattach.Addr
	c.protocol = c.config.Reattach.Protocol
	if c.protocol == "" {
		// Default the protocol to net/rpc for backwards compatibility
		c.protocol = ProtocolNetRPC
	}

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

	c.grpcMuxerMu.Lock()
	defer c.grpcMuxerMu.Unlock()

	if c.grpcMuxerClosed {
		// The client has been torn down; don't recreate a muxer that would
		// otherwise leak its connection and goroutines.
		return nil, fmt.Errorf("gRPC broker muxer is closed")
	}

	if c.grpcMuxer != nil {
		return c.grpcMuxer, nil
	}

	muxer, err := grpcmux.NewGRPCClientMuxer(c.logger, addr)
	if err != nil {
		return nil, err
	}

	c.grpcMuxer = muxer
	return muxer, nil
}

func (c *Client) logStderr(name string, r io.Reader) {
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

	for {

		line, isPrefix, err := reader.ReadLine()
		switch {
		case err == io.EOF:
			return
		case err != nil:
			l.Error("reading plugin stderr", "error", err)
			return
		}

		_, _ = c.config.Stderr.Write(line)

		// The line was longer than our max token size, so it's likely
		// incomplete and won't unmarshal.
		if isPrefix || continuation {
			l.Debug(string(line))

			// if we're finishing a continued line, add the newline back in
			if !isPrefix {
				_, _ = c.config.Stderr.Write([]byte{'\n'})
			}

			continuation = isPrefix
			continue
		}

		_, _ = c.config.Stderr.Write([]byte{'\n'})

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
