// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"bytes"
	"context"
	"errors"
	"net/rpc"
	"sync"
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	grpctest "github.com/hashicorp/go-plugin/test/grpc"
	"google.golang.org/grpc"
)

// siblingGRPCPlugin registers a different gRPC service (PingPong) so two
// plugins with a shared name prefix can coexist in one server.
type siblingGRPCPlugin struct{}

func (siblingGRPCPlugin) Server(*MuxBroker) (interface{}, error) {
	return nil, errors.New("net/rpc plugin protocol not supported")
}

func (siblingGRPCPlugin) Client(*MuxBroker, *rpc.Client) (interface{}, error) {
	return nil, errors.New("net/rpc plugin protocol not supported")
}

func (siblingGRPCPlugin) GRPCServer(_ *GRPCBroker, s *grpc.Server) error {
	grpctest.RegisterPingPongServer(s, &pingPongServer{})
	return nil
}

func (siblingGRPCPlugin) GRPCClient(_ context.Context, _ *GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return grpctest.NewPingPongClient(c), nil
}

// startTestGRPCServer boots Serve in test mode and returns the client plus
// cleanup helpers.
func startTestGRPCServer(t *testing.T, plugins map[string]Plugin, opts ...func(*ServeConfig)) (ClientProtocol, *ServeConfig, context.CancelFunc, <-chan struct{}) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	reattachCh := make(chan *ReattachConfig, 1)
	closeCh := make(chan struct{})

	cfg := &ServeConfig{
		HandshakeConfig: testHandshake,
		Plugins:         plugins,
		GRPCServer:      DefaultGRPCServer,
		Logger:          hclog.NewNullLogger(),
		Test: &ServeTestConfig{
			Context:          ctx,
			ReattachConfigCh: reattachCh,
			CloseCh:          closeCh,
		},
	}
	for _, opt := range opts {
		opt(cfg)
	}

	go Serve(cfg)

	var reattach *ReattachConfig
	select {
	case reattach = <-reattachCh:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("did not receive reattach config")
	}

	c := NewClient(&ClientConfig{
		Cmd:              nil,
		HandshakeConfig:  testHandshake,
		Plugins:          plugins,
		Reattach:         reattach,
		AllowedProtocols: []Protocol{ProtocolGRPC},
	})
	client, err := c.Client()
	if err != nil {
		cancel()
		t.Fatalf("client connection failed: %s", err)
	}
	return client, cfg, cancel, closeCh
}

func TestServer_RegistryServesSiblingNames(t *testing.T) {
	plugins := map[string]Plugin{
		"team-a/foo": new(testGRPCInterfacePlugin),
		"team-a/bar": siblingGRPCPlugin{},
	}

	client, _, cancel, closeCh := startTestGRPCServer(t, plugins)
	defer func() {
		_ = client.Close()
		cancel()
		<-closeCh
	}()

	// Each name must route to its own implementation despite the shared
	// "team-a/" prefix.
	raw, err := client.Dispense("team-a/foo")
	if err != nil {
		t.Fatalf("Dispense(team-a/foo) failed: %s", err)
	}
	if _, ok := raw.(testInterface); !ok {
		t.Fatalf("team-a/foo returned wrong type: %#v", raw)
	}

	if _, err := client.Dispense("team-a/bar"); err != nil {
		t.Fatalf("Dispense(team-a/bar) failed: %s", err)
	}
}

func TestServer_RegistryUnknownNameError(t *testing.T) {
	plugins := map[string]Plugin{
		"team-a/foo": new(testGRPCInterfacePlugin),
		"team-a/bar": siblingGRPCPlugin{},
	}

	client, _, cancel, closeCh := startTestGRPCServer(t, plugins)
	defer func() {
		_ = client.Close()
		cancel()
		<-closeCh
	}()

	// Prefix collisions and off-by-one names must not resolve.
	for _, name := range []string{"team-a/fo", "team-a/foo ", "team-a/foobar", ""} {
		_, err := client.Dispense(name)
		if !errors.Is(err, ErrPluginNotFound) {
			t.Fatalf("Dispense(%q): expected ErrPluginNotFound, got: %v", name, err)
		}
	}
}

func TestServer_DuplicateRegistrationRejected(t *testing.T) {
	// A pre-populated registry conflicting with the ServeConfig PluginSet
	// must fail loudly instead of being overwritten.
	registry, err := NewPluginRegistry(map[string]Plugin{
		"test": new(testGRPCInterfacePlugin),
	})
	if err != nil {
		t.Fatalf("registry construction failed: %s", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	closeCh := make(chan struct{})
	reattachCh := make(chan *ReattachConfig, 1)

	go Serve(&ServeConfig{
		HandshakeConfig: testHandshake,
		Plugins:         testGRPCPluginMap,
		Registry:        registry,
		GRPCServer:      DefaultGRPCServer,
		Logger:          hclog.NewNullLogger(),
		Test: &ServeTestConfig{
			Context:          ctx,
			ReattachConfigCh: reattachCh,
			CloseCh:          closeCh,
		},
	})

	// Serve must exit without ever emitting a reattach config.
	select {
	case cfg := <-reattachCh:
		cancel()
		t.Fatalf("expected startup failure, got reattach config: %+v", cfg)
	case <-closeCh:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("Serve did not exit on duplicate registration")
	}

	// The original entry must survive the rejected registration attempt.
	if _, err := registry.Lookup("test"); err != nil {
		t.Fatalf("original registry entry not preserved: %v", err)
	}
}

func TestServer_GRPCInitBuildsRegistry(t *testing.T) {
	// With no caller-supplied registry, Init builds one from the PluginSet
	// and starts serving from it.
	server := &GRPCServer{
		Plugins: map[string]Plugin{
			"a": new(testGRPCInterfacePlugin),
		},
		Server: DefaultGRPCServer,
		DoneCh: make(chan struct{}),
		Stdout: new(bytes.Buffer),
		Stderr: new(bytes.Buffer),
		logger: hclog.NewNullLogger(),
	}
	if err := server.Init(); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer server.Stop()

	if server.Registry == nil {
		t.Fatal("Init should have created a registry")
	}
	info, ok := server.Registry.Get("a")
	if !ok || info.Status != PluginStatusServing {
		t.Fatalf("unexpected registry state: %+v ok=%v", info, ok)
	}

	// The registry remains the enforcement point for duplicate names.
	err := server.Registry.Register("a", siblingGRPCPlugin{})
	if !errors.Is(err, ErrPluginAlreadyRegistered) {
		t.Fatalf("expected ErrPluginAlreadyRegistered, got: %v", err)
	}
}

func TestServer_RPCInitRejectsDuplicateNames(t *testing.T) {
	// The net/rpc server builds its registry from the PluginSet when none
	// is supplied, and duplicate names (including empty and nil entries)
	// must be rejected at construction.
	server := &RPCServer{
		Plugins: map[string]Plugin{
			"test": new(testInterfacePlugin),
		},
	}
	if err := server.Init(); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	err := server.Registry.Register("test", new(testInterfacePlugin))
	if !errors.Is(err, ErrPluginAlreadyRegistered) {
		t.Fatalf("expected ErrPluginAlreadyRegistered, got: %v", err)
	}

	original, _ := server.Registry.Lookup("test")
	if original == nil {
		t.Fatal("original registration was lost")
	}
}

func TestBuildServerRegistry_Conflict(t *testing.T) {
	existing, err := NewPluginRegistry(map[string]Plugin{
		"test": new(testGRPCInterfacePlugin),
	})
	if err != nil {
		t.Fatalf("registry construction failed: %s", err)
	}
	original, _ := existing.Lookup("test")

	_, err = buildServerRegistry(existing, PluginSet{
		"test": new(testGRPCInterfacePlugin),
	})
	if !errors.Is(err, ErrPluginAlreadyRegistered) {
		t.Fatalf("expected ErrPluginAlreadyRegistered, got: %v", err)
	}

	stillThere, err := existing.Lookup("test")
	if err != nil || stillThere != original {
		t.Fatal("original registration was not preserved")
	}
}

func TestRPCServer_DispenseRoutingAndUnknownName(t *testing.T) {
	client, server := TestPluginRPCConn(t, map[string]Plugin{
		"team-a/foo": new(testInterfacePlugin),
	}, nil)
	defer func() { _ = client.Close() }()

	if _, err := client.Dispense("team-a/foo"); err != nil {
		t.Fatalf("Dispense failed: %s", err)
	}

	// Unknown names must be identifiable as ErrPluginNotFound even after
	// the net/rpc transport strips the error type.
	_, err := client.Dispense("team-a/foobar")
	if !errors.Is(err, ErrPluginNotFound) {
		t.Fatalf("expected ErrPluginNotFound, got: %v", err)
	}

	// The server-side registry tracks the dispensed plugin as serving.
	info, ok := server.Registry.Get("team-a/foo")
	if !ok || info.Status != PluginStatusServing {
		t.Fatalf("unexpected registry state: %+v ok=%v", info, ok)
	}
}

func TestRPCServer_ShutdownNotifiesPlugins(t *testing.T) {
	release := make(chan struct{})
	closer := &rpcCloserPlugin{
		testInterfacePlugin: new(testInterfacePlugin),
		closeCalled:         make(chan struct{}),
		release:             release,
	}

	client, server := TestPluginRPCConn(t, map[string]Plugin{
		"closer": closer,
	}, nil)
	_ = client.Close()
	server.ShutdownTimeout = time.Second

	done := make(chan error, 1)
	go func() { done <- server.Shutdown() }()

	select {
	case <-closer.closeCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("net/rpc plugin was not notified of shutdown")
	}

	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected shutdown error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("net/rpc Shutdown did not return")
	}

	if info, _ := server.Registry.Get("closer"); info.Status != PluginStatusStopped {
		t.Fatalf("expected stopped status, got %s", info.Status)
	}

	// Idempotency: a second call returns immediately with the same result.
	if err := server.Shutdown(); err != nil {
		t.Fatalf("second Shutdown failed: %v", err)
	}
}

// rpcCloserPlugin is a net/rpc plugin implementing io.Closer.
type rpcCloserPlugin struct {
	*testInterfacePlugin
	closeCalled chan struct{}
	release     chan struct{}
	once        sync.Once
}

func (p *rpcCloserPlugin) Close() error {
	p.once.Do(func() { close(p.closeCalled) })
	if p.release != nil {
		<-p.release
	}
	return nil
}

func TestServer_ShutdownNotifiesPlugins(t *testing.T) {
	release := make(chan struct{})
	closer := newCloserPlugin(release, nil)

	plugins := map[string]Plugin{
		"closer": closer,
	}

	_, _, cancel, closeCh := startTestGRPCServer(t, plugins,
		func(c *ServeConfig) { c.ShutdownTimeout = time.Second })

	// Cancelling triggers the graceful shutdown path.
	cancel()

	select {
	case <-closer.closeCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("plugin was not notified during server shutdown")
	}

	// While the plugin is still draining, Serve must not have returned.
	select {
	case <-closeCh:
		t.Fatal("server exited before the plugin finished closing")
	default:
	}

	close(release)
	select {
	case <-closeCh:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not exit after plugins closed")
	}
}

func TestServer_ShutdownTimeoutForcesStop(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	closer := newCloserPlugin(release, nil)

	plugins := map[string]Plugin{
		"hung": closer,
	}

	_, _, cancel, closeCh := startTestGRPCServer(t, plugins,
		func(c *ServeConfig) { c.ShutdownTimeout = 50 * time.Millisecond })

	start := time.Now()
	cancel()

	select {
	case <-closeCh:
	case <-time.After(3 * time.Second):
		t.Fatal("server hung instead of force-stopping after timeout")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("forced stop took too long: %s", elapsed)
	}
}

func TestServer_RegistryStatusQueryable(t *testing.T) {
	plugins := map[string]Plugin{
		"team-a/foo": new(testGRPCInterfacePlugin),
	}

	client, cfg, cancel, closeCh := startTestGRPCServer(t, plugins)
	defer func() {
		_ = client.Close()
		cancel()
		<-closeCh
	}()

	if cfg.Registry == nil {
		t.Fatal("Serve should expose its registry on the config")
	}
	info, ok := cfg.Registry.Get("team-a/foo")
	if !ok {
		t.Fatal("registered plugin missing from query")
	}
	if info.Status != PluginStatusServing {
		t.Fatalf("expected serving status, got %s", info.Status)
	}

	// Use the connection so routing is exercised end to end.
	raw, err := client.Dispense("team-a/foo")
	if err != nil {
		t.Fatalf("Dispense failed: %s", err)
	}
	if impl, ok := raw.(testInterface); !ok || impl.Double(3) != 6 {
		t.Fatalf("bad implementation: %#v", raw)
	}

	cancel()
	<-closeCh

	info, ok = cfg.Registry.Get("team-a/foo")
	if !ok {
		t.Fatal("plugin entry vanished after shutdown")
	}
	if info.Status != PluginStatusStopped {
		t.Fatalf("expected stopped status after shutdown, got %s", info.Status)
	}
}

func TestServer_RestartLeavesNoStaleEntries(t *testing.T) {
	plugins := map[string]Plugin{
		"test": new(testGRPCInterfacePlugin),
	}

	// First generation.
	client1, cfg1, cancel1, closeCh1 := startTestGRPCServer(t, plugins)
	_ = client1.Close()
	cancel1()
	<-closeCh1

	if infos := cfg1.Registry.List(); len(infos) != 1 || infos[0].Status != PluginStatusStopped {
		t.Fatalf("unexpected first-generation state: %+v", infos)
	}

	// Second generation gets a fresh registry; nothing from the previous
	// run may leak into it.
	client2, cfg2, cancel2, closeCh2 := startTestGRPCServer(t, plugins)
	defer func() {
		_ = client2.Close()
		cancel2()
		<-closeCh2
	}()

	if cfg2.Registry == cfg1.Registry {
		t.Fatal("restarted server reused the previous registry instance")
	}
	if infos := cfg2.Registry.List(); len(infos) != 1 || infos[0].Status != PluginStatusServing {
		t.Fatalf("unexpected restarted state: %+v", infos)
	}
}
