// Copyright IBM Corp. 2016, 2025
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin/internal/grpcmux"
	"github.com/hashicorp/go-plugin/internal/plugin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// GRPCServiceName is the name of the service that the health check should
// return as passing.
const GRPCServiceName = "plugin"

// grpcGracefulStopGrace is the time GRPCServer.Shutdown gives the gRPC
// server to drain in-flight RPCs after plugins have been notified before
// it forces a stop. It is deliberately short: the client-side graceful
// shutdown window is only a couple of seconds and long-lived streams such
// as the stdio stream keep GracefulStop from ever returning on its own.
const grpcGracefulStopGrace = 250 * time.Millisecond

// DefaultGRPCServer can be used with the "GRPCServer" field for Server
// as a default factory method to create a gRPC server with no extra options.
func DefaultGRPCServer(opts []grpc.ServerOption) *grpc.Server {
	return grpc.NewServer(opts...)
}

// GRPCServer is a ServerType implementation that serves plugins over
// gRPC. This allows plugins to easily be written for other languages.
//
// The GRPCServer outputs a custom configuration as a base64-encoded
// JSON structure represented by the GRPCServerConfig config structure.
type GRPCServer struct {
	// Plugins are the list of plugins to serve.
	Plugins map[string]Plugin

	// Registry is the validated plugin registry used for name-based
	// routing and lifecycle management. When nil, Init builds one from
	// Plugins and rejects duplicate names. When set, it takes precedence
	// over Plugins.
	Registry *PluginRegistry

	// Server is the actual server that will accept connections. This
	// will be used for plugin registration as well.
	Server func([]grpc.ServerOption) *grpc.Server

	// TLS should be the TLS configuration if available. If this is nil,
	// the connection will not have transport security.
	TLS *tls.Config

	// DoneCh is the channel that is closed when this server has exited.
	DoneCh chan struct{}

	// Stdout/StderrLis are the readers for stdout/stderr that will be copied
	// to the stdout/stderr connection that is output.
	Stdout io.Reader
	Stderr io.Reader

	// ShutdownTimeout bounds how long Shutdown waits for plugins that
	// implement io.Closer to drain before forcibly stopping the gRPC
	// server. A zero value uses the package default.
	ShutdownTimeout time.Duration

	config      GRPCServerConfig
	server      *grpc.Server
	broker      *GRPCBroker
	stdioServer *grpcStdioServer

	logger hclog.Logger

	muxer *grpcmux.GRPCServerMuxer

	// shutdownOnce serializes Shutdown so a controller-triggered shutdown
	// and a context-canceled shutdown cannot run concurrently and race on
	// the broker/server fields.
	shutdownOnce sync.Once
	shutdownDone chan struct{}
	shutdownErr  error
}

// ServerProtocol impl.
func (s *GRPCServer) Init() error {
	// Build the registry from the plugin set so duplicate names fail with
	// an explicit error rather than one registration silently replacing
	// the other.
	if s.Registry == nil {
		registry, err := NewPluginRegistry(s.Plugins)
		if err != nil {
			return err
		}
		s.Registry = registry
	}

	// Create our server
	var opts []grpc.ServerOption
	if s.TLS != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(s.TLS)))
	}
	s.server = s.Server(opts)

	// Register the health service
	healthCheck := health.NewServer()
	healthCheck.SetServingStatus(
		GRPCServiceName, grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(s.server, healthCheck)

	// Register the reflection service
	reflection.Register(s.server)

	// Register the broker service
	brokerServer := newGRPCBrokerServer()
	plugin.RegisterGRPCBrokerServer(s.server, brokerServer)
	s.broker = newGRPCBroker(brokerServer, s.TLS, unixSocketConfigFromEnv(), nil, s.muxer)
	go s.broker.Run()

	// Register the controller
	controllerServer := &grpcControllerServer{server: s}
	plugin.RegisterGRPCControllerServer(s.server, controllerServer)

	// Register the stdio service
	s.stdioServer = newGRPCStdioServer(s.logger, s.Stdout, s.Stderr)
	plugin.RegisterGRPCStdioServer(s.server, s.stdioServer)

	// Register all our plugins onto the gRPC server. Iterating the
	// registry (rather than the raw map) guarantees both unique names and
	// deterministic registration order.
	for _, info := range s.Registry.List() {
		raw, err := s.Registry.Lookup(info.Name)
		if err != nil {
			return err
		}

		p, ok := raw.(GRPCPlugin)
		if !ok {
			return fmt.Errorf("%q is not a GRPC-compatible plugin", info.Name)
		}

		if err := p.GRPCServer(s.broker, s.server); err != nil {
			return fmt.Errorf("error registering %q: %s", info.Name, err)
		}
	}

	s.Registry.markServing()
	return nil
}

// Stop calls Stop on the underlying grpc.Server and Close on the underlying
// grpc.Broker if present.
func (s *GRPCServer) Stop() {
	s.server.Stop()
	s.closeBroker()
}

// closeBroker closes the broker exactly once. It is safe for concurrent
// use with Shutdown.
func (s *GRPCServer) closeBroker() {
	if s.broker != nil {
		_ = s.broker.Close()
		s.broker = nil
	}
}

// GracefulStop calls GracefulStop on the underlying grpc.Server and Close on
// the underlying grpc.Broker if present.
func (s *GRPCServer) GracefulStop() {
	s.server.GracefulStop()
	s.closeBroker()
}

// Shutdown performs an orderly server shutdown. It first notifies every
// registered plugin (plugins implementing io.Closer have Close called on
// them concurrently) and waits for them to exit, bounded by
// ShutdownTimeout. The gRPC server is then given a short grace period to
// drain in-flight RPCs, after which it is forcibly stopped so a
// long-lived stream or unresponsive connection cannot hang the process.
//
// Shutdown is idempotent: concurrent or repeated calls block until the
// first shutdown completes and return the same error.
func (s *GRPCServer) Shutdown() error {
	s.shutdownOnce.Do(func() {
		// Created here rather than in Init so Shutdown is also safe on a
		// server driven without the full Init/Serve lifecycle. Reads of
		// shutdownDone below happen after Do returns, which establishes a
		// happens-before edge with this write.
		s.shutdownDone = make(chan struct{})
		defer close(s.shutdownDone)
		s.shutdownErr = s.doShutdown()
	})

	<-s.shutdownDone
	return s.shutdownErr
}

func (s *GRPCServer) doShutdown() error {
	timeout := s.ShutdownTimeout
	if timeout <= 0 {
		timeout = defaultPluginShutdownTimeout
	}

	// Notify plugins and wait for them to drain. The registry itself
	// enforces the timeout and marks every plugin stopped afterwards.
	err := s.Registry.Shutdown(timeout)

	// GracefulStop blocks until all outstanding RPCs finish. Run it
	// concurrently with a hard Stop so an unresponsive connection cannot
	// extend the shutdown past a short, fixed grace period.
	timer := time.NewTimer(grpcGracefulStopGrace)
	defer timer.Stop()

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		s.server.GracefulStop()
	}()

	select {
	case <-stopped:
	case <-timer.C:
		s.logger.Warn("graceful stop timed out, forcing server stop",
			"grace", grpcGracefulStopGrace)
		s.Stop()
		<-stopped
	}

	s.closeBroker()
	return err
}

// Config is the GRPCServerConfig encoded as JSON then base64.
func (s *GRPCServer) Config() string {
	// Create a buffer that will contain our final contents
	var buf bytes.Buffer

	// Wrap the base64 encoding with JSON encoding.
	if err := json.NewEncoder(&buf).Encode(s.config); err != nil {
		// We panic since ths shouldn't happen under any scenario. We
		// carefully control the structure being encoded here and it should
		// always be successful.
		panic(err)
	}

	return buf.String()
}

func (s *GRPCServer) Serve(lis net.Listener) {
	defer close(s.DoneCh)
	err := s.server.Serve(lis)
	if err != nil {
		s.logger.Error("grpc server", "error", err)
	}
}

// GRPCServerConfig is the extra configuration passed along for consumers
// to facilitate using GRPC plugins.
type GRPCServerConfig struct {
	StdoutAddr string `json:"stdout_addr"`
	StderrAddr string `json:"stderr_addr"`
}
