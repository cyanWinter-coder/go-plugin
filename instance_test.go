// Copyright IBM Corp. 2016, 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
)

// fakeConnection is a ClientProtocol used by reconnect tests. It records how
// it was closed so tests can distinguish a release (rollback) from a graceful
// shutdown.
type fakeConnection struct {
	name     string
	pingErr  error
	released atomic.Int32
	closed   atomic.Int32
}

func (f *fakeConnection) Dispense(string) (interface{}, error) { return nil, nil }

func (f *fakeConnection) Ping() error { return f.pingErr }

func (f *fakeConnection) Close() error {
	f.closed.Add(1)
	return nil
}

func (f *fakeConnection) ReleaseConnection() error {
	f.released.Add(1)
	return nil
}

func (f *fakeConnection) releaseCount() int32 { return f.released.Load() }
func (f *fakeConnection) closeCount() int32   { return f.closed.Load() }

// newReconnectTestClient builds a client that is already bound to a fake live
// instance, without starting a process.
func newReconnectTestClient(t *testing.T, protocol Protocol, version int) *Client {
	t.Helper()

	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}

	c := &Client{
		config: &ClientConfig{
			HandshakeConfig: testHandshake,
			Plugins:         testPluginMap,
		},
		logger: hclog.NewNullLogger(),
	}
	c.address = addr
	c.protocol = protocol
	c.negotiatedVersion = version
	c.doneCtx, c.ctxCancel = context.WithCancel(context.Background())
	c.publishInstanceLocked("1234")
	// Start from the state a started and connected client would be in.
	c.publishConnectedLocked()
	return c
}

func TestInstanceView_PublishedAtomically(t *testing.T) {
	process := helperProcess("test-interface")
	c := NewClient(&ClientConfig{
		Cmd:             process,
		HandshakeConfig: testHandshake,
		Plugins:         testPluginMap,
	})
	defer c.Kill()

	if _, ok := c.InstanceView(); ok {
		t.Fatal("instance view should not exist before Start")
	}

	addr, err := c.Start()
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	view, ok := c.InstanceView()
	if !ok {
		t.Fatal("instance view should exist after Start")
	}
	if view.ID == "" {
		t.Fatal("instance view should record the process identity")
	}
	if !sameAddr(view.Address, addr) {
		t.Fatalf("instance address %s does not match Start address %s", view.Address, addr)
	}
	if view.Protocol != ProtocolNetRPC {
		t.Fatalf("bad protocol: %s", view.Protocol)
	}
	if view.ProtocolVersion != int(testHandshake.ProtocolVersion) {
		t.Fatalf("bad version: %d", view.ProtocolVersion)
	}
	if view.State != InstanceStateActive {
		t.Fatalf("bad state: %s", view.State)
	}
	if view.Connected {
		t.Fatal("no control connection should be published before Client")
	}

	// Starting again must return the exact same published view; Start cannot
	// overwrite it with a second handshake.
	if _, err := c.Start(); err != nil {
		t.Fatalf("err: %s", err)
	}
	again, _ := c.InstanceView()
	if !view.SameInstance(again) {
		t.Fatalf("Start overwrote the published instance view: %+v vs %+v", view, again)
	}

	if _, err := c.Client(); err != nil {
		t.Fatalf("err: %s", err)
	}
	connected, _ := c.InstanceView()
	if !connected.Connected {
		t.Fatal("instance view should report a published connection")
	}
	if connected.ConnectionGeneration != view.ConnectionGeneration+1 {
		t.Fatalf("connection generation should advance once, got %d", connected.ConnectionGeneration)
	}
	if !connected.SameInstance(view) {
		t.Fatal("publishing a connection must not change the instance identity")
	}
}

func TestInstanceView_SameInstance(t *testing.T) {
	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:9999")
	base := InstanceView{
		ID: "1", Address: addr, Protocol: ProtocolGRPC, ProtocolVersion: 2, CoreProtocolVersion: 1,
	}

	same := base
	same.ConnectionGeneration = 7
	same.Connected = true
	if !base.SameInstance(same) {
		t.Fatal("generation and connectivity must not affect identity")
	}

	cases := map[string]InstanceView{
		"id":      {ID: "2", Address: addr, Protocol: ProtocolGRPC, ProtocolVersion: 2, CoreProtocolVersion: 1},
		"addr":    {ID: "1", Address: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9998}, Protocol: ProtocolGRPC, ProtocolVersion: 2, CoreProtocolVersion: 1},
		"proto":   {ID: "1", Address: addr, Protocol: ProtocolNetRPC, ProtocolVersion: 2, CoreProtocolVersion: 1},
		"version": {ID: "1", Address: addr, Protocol: ProtocolGRPC, ProtocolVersion: 3, CoreProtocolVersion: 1},
		"core":    {ID: "1", Address: addr, Protocol: ProtocolGRPC, ProtocolVersion: 2, CoreProtocolVersion: 2},
	}
	for name, other := range cases {
		if base.SameInstance(other) {
			t.Errorf("%s: views must not be considered the same instance", name)
		}
	}
}

func TestClient_Reconnect_NotStarted(t *testing.T) {
	c := NewClient(&ClientConfig{
		Cmd:             helperProcess("test-interface"),
		HandshakeConfig: testHandshake,
		Plugins:         testPluginMap,
	})

	_, err := c.Reconnect(context.Background())
	if err == nil {
		t.Fatal("expected an error")
	}
	if !errors.Is(err, ErrNoLiveInstance) {
		t.Fatalf("expected ErrNoLiveInstance, got %s", err)
	}
	var rErr *ReconnectError
	if !errors.As(err, &rErr) || rErr.Reason != ReconnectReasonNotRunning {
		t.Fatalf("expected not-running reconnect error, got %#v", err)
	}
	if c.LastReconnectError() == nil {
		t.Fatal("failure should be retained for diagnosis")
	}
}

func TestClient_Reconnect_AfterKill(t *testing.T) {
	process := helperProcess("test-interface")
	c := NewClient(&ClientConfig{
		Cmd:             process,
		HandshakeConfig: testHandshake,
		Plugins:         testPluginMap,
	})
	defer c.Kill()

	if _, err := c.Client(); err != nil {
		t.Fatalf("err: %s", err)
	}
	c.Kill()

	if _, err := c.Reconnect(context.Background()); !errors.Is(err, ErrNoLiveInstance) {
		t.Fatalf("expected ErrNoLiveInstance after Kill, got %s", err)
	}
}

func TestClient_Reconnect_NetRPC(t *testing.T) {
	testReconnectReplacesConnection(t, "test-interface", ProtocolNetRPC, testPluginMap)
}

func TestClient_Reconnect_GRPC(t *testing.T) {
	testReconnectReplacesConnection(t, "test-grpc", ProtocolGRPC, testGRPCPluginMap)
}

// testReconnectReplacesConnection drops the control connection of a real
// plugin process and verifies that Reconnect restores it without starting a
// second instance.
func testReconnectReplacesConnection(t *testing.T, helper string, protocol Protocol, plugins PluginSet) {
	t.Helper()

	c := NewClient(&ClientConfig{
		Cmd:              helperProcess(helper),
		HandshakeConfig:  testHandshake,
		Plugins:          plugins,
		AllowedProtocols: []Protocol{protocol},
	})
	defer c.Kill()

	first, err := c.Client()
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	before, ok := c.InstanceView()
	if !ok {
		t.Fatal("instance view missing")
	}

	// Drop the control connection the way a vanished host would, without
	// asking the plugin to quit.
	dropControlConnection(t, first)

	// The process itself is still alive: reconnecting must reuse it.
	second, err := c.Reconnect(context.Background())
	if err != nil {
		t.Fatalf("reconnect failed: %s", err)
	}
	if second == first {
		t.Fatal("reconnect should publish a new connection")
	}
	if err := second.Ping(); err != nil {
		t.Fatalf("new connection should be healthy: %s", err)
	}

	after, ok := c.InstanceView()
	if !ok {
		t.Fatal("instance view missing after reconnect")
	}
	if !before.SameInstance(after) {
		t.Fatalf("reconnect must not change the instance identity: %+v vs %+v", before, after)
	}
	if after.ConnectionGeneration <= before.ConnectionGeneration {
		t.Fatalf("connection generation should advance: %d -> %d",
			before.ConnectionGeneration, after.ConnectionGeneration)
	}
	if !after.Connected {
		t.Fatal("reconnected view should be connected")
	}

	// The client now serves the new connection.
	current, err := c.Client()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if current != second {
		t.Fatal("Client() should return the reconnected connection")
	}

	// The plugin process was not restarted: its identity is unchanged and the
	// original runner is still the live one.
	if got := c.ID(); got != before.ID {
		t.Fatalf("plugin process changed: %s -> %s", before.ID, got)
	}
	if c.Exited() {
		t.Fatal("plugin should still be running")
	}
	if c.LastReconnectError() != nil {
		t.Fatalf("successful reconnect should clear the last error, got %s", c.LastReconnectError())
	}
}

// dropControlConnection tears down the transport of a live client without
// triggering a graceful plugin shutdown.
func dropControlConnection(t *testing.T, client ClientProtocol) {
	t.Helper()

	switch c := client.(type) {
	case *RPCClient:
		if err := c.broker.Close(); err != nil {
			t.Fatalf("closing rpc transport: %s", err)
		}
	case *GRPCClient:
		if err := c.Conn.Close(); err != nil {
			t.Fatalf("closing grpc transport: %s", err)
		}
	default:
		t.Fatalf("unexpected client type %T", client)
	}
}

func TestClient_Reconnect_IdentityMismatch(t *testing.T) {
	tests := map[string]func(*ReattachConfig){
		"address": func(cfg *ReattachConfig) {
			cfg.Addr = &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 65000}
		},
		"pid": func(cfg *ReattachConfig) {
			cfg.Pid = 999999
		},
		"protocol": func(cfg *ReattachConfig) {
			cfg.Protocol = Protocol("bogus")
		},
		"version": func(cfg *ReattachConfig) {
			cfg.ProtocolVersion = 99
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			process := helperProcess("test-interface")
			c := NewClient(&ClientConfig{
				Cmd:             process,
				HandshakeConfig: testHandshake,
				Plugins:         testPluginMap,
			})
			defer c.Kill()

			if _, err := c.Client(); err != nil {
				t.Fatalf("err: %s", err)
			}

			view, _ := c.InstanceView()
			cfg := &ReattachConfig{
				Addr:            view.Address,
				Pid:             1234,
				Protocol:        view.Protocol,
				ProtocolVersion: view.ProtocolVersion,
			}
			mutate(cfg)

			c.l.Lock()
			c.config.Reattach = cfg
			c.l.Unlock()

			_, err := c.Reconnect(context.Background())
			if !errors.Is(err, ErrInstanceIdentityMismatch) {
				t.Fatalf("expected ErrInstanceIdentityMismatch, got %s", err)
			}
			var rErr *ReconnectError
			if !errors.As(err, &rErr) || rErr.Reason != ReconnectReasonIdentity {
				t.Fatalf("expected identity reconnect error, got %#v", err)
			}

			// The previously published connection must be left untouched.
			after, _ := c.InstanceView()
			if after.ConnectionGeneration != view.ConnectionGeneration || !after.Connected {
				t.Fatalf("failed reconnect mutated the instance view: %+v", after)
			}
		})
	}
}

func TestClient_Reconnect_RunnerIdentityMismatch(t *testing.T) {
	c := newReconnectTestClient(t, ProtocolNetRPC, 1)

	// A runner that reports a different identity must be refused.
	c.runner = &identityRunner{id: "9999"}
	c.client = &fakeConnection{name: "old"}

	_, err := c.Reconnect(context.Background())
	if !errors.Is(err, ErrInstanceIdentityMismatch) {
		t.Fatalf("expected ErrInstanceIdentityMismatch, got %s", err)
	}
}

func TestClient_Reconnect_Canceled(t *testing.T) {
	c := newReconnectTestClient(t, ProtocolNetRPC, 1)
	old := &fakeConnection{name: "old"}
	c.client = old
	before, _ := c.InstanceView()

	started := make(chan struct{})
	c.dialInstance = func(ctx context.Context, _ InstanceView) (ClientProtocol, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Reconnect(ctx)
		errCh <- err
	}()

	<-started
	cancel()

	err := <-errCh
	if !errors.Is(err, ErrReconnectCanceled) {
		t.Fatalf("expected ErrReconnectCanceled, got %s", err)
	}
	var rErr *ReconnectError
	if !errors.As(err, &rErr) || rErr.Reason != ReconnectReasonCanceled {
		t.Fatalf("expected canceled reconnect error, got %#v", err)
	}

	after, _ := c.InstanceView()
	if after.ConnectionGeneration != before.ConnectionGeneration || !after.Connected {
		t.Fatalf("canceled reconnect mutated the view: %+v", after)
	}
	if old.releaseCount() != 0 || old.closeCount() != 0 {
		t.Fatal("the previously published connection must not be released")
	}
}

func TestClient_Reconnect_Timeout(t *testing.T) {
	c := newReconnectTestClient(t, ProtocolNetRPC, 1)
	c.config.ReconnectTimeout = 20 * time.Millisecond
	c.client = &fakeConnection{name: "old"}

	c.dialInstance = func(ctx context.Context, _ InstanceView) (ClientProtocol, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	_, err := c.Reconnect(context.Background())
	if !errors.Is(err, ErrReconnectTimeout) {
		t.Fatalf("expected ErrReconnectTimeout, got %s", err)
	}
	var rErr *ReconnectError
	if !errors.As(err, &rErr) || rErr.Reason != ReconnectReasonTimeout {
		t.Fatalf("expected timeout reconnect error, got %#v", err)
	}
}

func TestClient_Reconnect_ProcessExited(t *testing.T) {
	c := newReconnectTestClient(t, ProtocolNetRPC, 1)
	c.client = &fakeConnection{name: "old"}

	// Simulate the process exiting after the snapshot but before publication.
	c.dialInstance = func(context.Context, InstanceView) (ClientProtocol, error) {
		c.l.Lock()
		c.markExitedLocked()
		c.l.Unlock()
		return &fakeConnection{name: "candidate"}, nil
	}

	_, err := c.Reconnect(context.Background())
	if !errors.Is(err, ErrReconnectProcessExited) {
		t.Fatalf("expected ErrReconnectProcessExited, got %s", err)
	}
}

func TestClient_Reconnect_HandshakeFailureReleasesCandidate(t *testing.T) {
	c := newReconnectTestClient(t, ProtocolNetRPC, 1)
	old := &fakeConnection{name: "old"}
	c.client = old

	// A dial or health-check failure must leave the old connection published
	// and must not release it.
	c.dialInstance = func(context.Context, InstanceView) (ClientProtocol, error) {
		return nil, ErrReconnectHandshake
	}

	_, err := c.Reconnect(context.Background())
	if !errors.Is(err, ErrReconnectHandshake) {
		t.Fatalf("expected ErrReconnectHandshake, got %s", err)
	}
	if old.releaseCount() != 0 || old.closeCount() != 0 {
		t.Fatal("the live connection must not be released")
	}

	after, _ := c.InstanceView()
	if !after.Connected {
		t.Fatal("the previously published connection must remain current")
	}
}

func TestClient_Reconnect_ConcurrentOnlyOneWins(t *testing.T) {
	c := newReconnectTestClient(t, ProtocolNetRPC, 1)
	c.client = &fakeConnection{name: "old"}

	const attempts = 8
	var mu sync.Mutex
	candidates := make([]*fakeConnection, 0, attempts)
	ready := make(chan struct{})

	// All attempts dial concurrently and block until every one of them has
	// taken its snapshot, so their installation genuinely races.
	c.dialInstance = func(context.Context, InstanceView) (ClientProtocol, error) {
		conn := &fakeConnection{name: "candidate"}
		mu.Lock()
		candidates = append(candidates, conn)
		if len(candidates) == attempts {
			close(ready)
		}
		mu.Unlock()
		<-ready
		return conn, nil
	}

	var wg sync.WaitGroup
	results := make([]ClientProtocol, attempts)
	errs := make([]error, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = c.Reconnect(context.Background())
		}(i)
	}
	wg.Wait()

	var winner ClientProtocol
	published := 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("attempt %d failed: %s", i, err)
		}
		if winner == nil {
			winner = results[i]
		}
		if results[i] != winner {
			t.Fatalf("attempt %d returned a different connection than the winner", i)
		}
	}
	for _, conn := range candidates {
		if conn == winner {
			published++
		}
	}
	if published != 1 {
		t.Fatalf("exactly one candidate should be published, got %d", published)
	}

	current, err := c.Client()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if current != winner {
		t.Fatal("Client() should return the winning connection")
	}

	// Every losing candidate must have been released exactly once, without a
	// graceful shutdown of the plugin.
	for _, conn := range candidates {
		if conn == winner {
			if conn.releaseCount() != 0 || conn.closeCount() != 0 {
				t.Fatal("winner must stay live")
			}
			continue
		}
		if conn.releaseCount() != 1 || conn.closeCount() != 0 {
			t.Fatalf("loser release=%d close=%d, want 1/0",
				conn.releaseCount(), conn.closeCount())
		}
	}

	view, _ := c.InstanceView()
	if view.ConnectionGeneration != 2 {
		t.Fatalf("generation should advance exactly once, got %d", view.ConnectionGeneration)
	}
}

// TestClient_Reconnect_LateCloseKeepsNewConnection verifies that closing a
// superseded connection after a newer one was published cannot clear the newer
// connection.
func TestClient_Reconnect_LateCloseKeepsNewConnection(t *testing.T) {
	c := newReconnectTestClient(t, ProtocolNetRPC, 1)

	oldConn := &fakeConnection{name: "old"}
	candidate := &fakeConnection{name: "new"}

	// Publish the first connection, capturing its generation.
	c.l.Lock()
	c.client = oldConn
	c.publishConnectedLocked()
	c.l.Unlock()
	first, _ := c.InstanceView()

	// Publish a newer connection.
	c.l.Lock()
	c.client = candidate
	c.publishConnectedLocked()
	c.l.Unlock()
	second, _ := c.InstanceView()

	if second.ConnectionGeneration != first.ConnectionGeneration+1 {
		t.Fatalf("generation should advance: %d -> %d",
			first.ConnectionGeneration, second.ConnectionGeneration)
	}

	// The late close arrives for the old generation.
	if err := c.closeConnection(oldConn, first.ConnectionGeneration); err != nil {
		t.Fatalf("err: %s", err)
	}

	after, _ := c.InstanceView()
	if !after.Connected {
		t.Fatal("late close cleared the newly published connection")
	}
	if after.ConnectionGeneration != second.ConnectionGeneration {
		t.Fatal("late close mutated the connection generation")
	}
	if after.SameInstance(second) == false {
		t.Fatal("late close mutated the instance identity")
	}

	current, err := c.Client()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if current != candidate {
		t.Fatal("Client() should still return the newer connection")
	}

	// Releasing the superseded connection must not ask the plugin to quit.
	if oldConn.releaseCount() != 1 || oldConn.closeCount() != 0 {
		t.Fatalf("superseded connection release=%d close=%d, want 1/0",
			oldConn.releaseCount(), oldConn.closeCount())
	}
}

// TestClient_Reconnect_SupersededConnectionReleased verifies that a successful
// reconnect releases the previous connection via ReleaseConnection rather than
// Close, so the live plugin is not asked to shut down.
func TestClient_Reconnect_SupersededConnectionReleased(t *testing.T) {
	c := newReconnectTestClient(t, ProtocolNetRPC, 1)
	old := &fakeConnection{name: "old"}
	c.client = old
	c.l.Lock()
	c.publishConnectedLocked()
	c.l.Unlock()

	candidate := &fakeConnection{name: "new"}
	c.dialInstance = func(context.Context, InstanceView) (ClientProtocol, error) {
		return candidate, nil
	}

	if _, err := c.Reconnect(context.Background()); err != nil {
		t.Fatalf("err: %s", err)
	}

	if old.releaseCount() != 1 || old.closeCount() != 0 {
		t.Fatalf("old connection release=%d close=%d, want 1/0",
			old.releaseCount(), old.closeCount())
	}
	if candidate.releaseCount() != 0 || candidate.closeCount() != 0 {
		t.Fatal("the published connection must stay live")
	}
}

func TestClient_Reconnect_GRPCBrokerMultiplexRefused(t *testing.T) {
	c := newReconnectTestClient(t, ProtocolGRPC, 1)
	c.config.GRPCBrokerMultiplex = true

	_, err := c.Reconnect(context.Background())
	if !errors.Is(err, ErrReconnectHandshake) {
		t.Fatalf("expected ErrReconnectHandshake, got %s", err)
	}
}

// hashIdentityRunner is a minimal AttachedRunner that reports a fixed identity
// and liveness, used to exercise the identity checks.
type identityRunner struct {
	id    string
	alive bool
}

func (r *identityRunner) Wait(context.Context) error { return nil }
func (r *identityRunner) Kill(context.Context) error { return nil }
func (r *identityRunner) ID() string                 { return r.id }
func (r *identityRunner) Alive() bool                { return r.alive }
func (r *identityRunner) PluginToHost(network, addr string) (string, string, error) {
	return network, addr, nil
}
func (r *identityRunner) HostToPlugin(network, addr string) (string, string, error) {
	return network, addr, nil
}

func TestClient_Reconnect_ProcessNotAlive(t *testing.T) {
	c := newReconnectTestClient(t, ProtocolNetRPC, 1)
	c.runner = &identityRunner{id: "1234", alive: false}

	_, err := c.Reconnect(context.Background())
	if !errors.Is(err, ErrReconnectProcessExited) {
		t.Fatalf("expected ErrReconnectProcessExited, got %s", err)
	}
}

// TestClient_Reconnect_ProcessExitDetected verifies a real process that has
// exited is reported as such, and that the live instance's identity is used to
// refuse the attempt before any dial happens.
func TestClient_Reconnect_ProcessExitDetected(t *testing.T) {
	process := helperProcess("test-interface")
	c := NewClient(&ClientConfig{
		Cmd:              process,
		HandshakeConfig:  testHandshake,
		Plugins:          testPluginMap,
		AllowedProtocols: []Protocol{ProtocolNetRPC},
	})
	defer c.Kill()

	if _, err := c.Client(); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Kill the process out of band, the way a crash would, and wait for the
	// client to observe the exit.
	c.l.Lock()
	r := c.runner
	c.l.Unlock()
	if err := r.Kill(context.Background()); err != nil {
		t.Fatalf("err: %s", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for !c.Exited() {
		if time.Now().After(deadline) {
			t.Fatal("client never observed the process exit")
		}
		time.Sleep(10 * time.Millisecond)
	}

	_, err := c.Reconnect(context.Background())
	if !errors.Is(err, ErrReconnectProcessExited) {
		t.Fatalf("expected ErrReconnectProcessExited, got %s", err)
	}
}

// TestClient_Start_FailureIsTerminal verifies that a failed Start is recorded
// and never retried against the torn-down process, so a second call cannot
// produce a second instance.
func TestClient_Start_FailureIsTerminal(t *testing.T) {
	td := t.TempDir()
	path := filepath.Join(td, "booted")

	c := NewClient(&ClientConfig{
		Cmd:             helperProcess("bad-version", path),
		HandshakeConfig: testHandshake,
		Plugins:         testPluginMap,
		StartTimeout:    5 * time.Second,
	})
	defer c.Kill()

	_, firstErr := c.Start()
	if firstErr == nil {
		t.Fatal("expected a startup error")
	}

	// The helper writes the marker file when it starts. Remove it and verify a
	// second Start does not start another process.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}

	_, secondErr := c.Start()
	if secondErr == nil {
		t.Fatal("expected the recorded startup error")
	}
	if !errors.Is(secondErr, firstErr) && secondErr.Error() != firstErr.Error() {
		t.Fatalf("second Start returned a different error: %s vs %s", secondErr, firstErr)
	}
	if _, err := os.Stat(path); err == nil || !os.IsNotExist(err) {
		t.Fatalf("a second Start must not launch another process (marker: %v)", err)
	}

	if _, err := c.Client(); err == nil {
		t.Fatal("Client must not build a connection for a failed instance")
	}
	if _, ok := c.InstanceView(); ok {
		t.Fatal("a failed Start must not publish an instance")
	}
}

// TestClient_Reconnect_ReattachIdentityBound verifies that a reattached client
// binds reconnect to the discovery record it was configured with, and refuses
// a record that describes a different instance.
func TestClient_Reconnect_ReattachIdentityBound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *ReattachConfig, 1)
	closeCh := make(chan struct{})
	go Serve(&ServeConfig{
		HandshakeConfig: testHandshake,
		Plugins:         testGRPCPluginMap,
		GRPCServer:      DefaultGRPCServer,
		Test: &ServeTestConfig{
			Context:          ctx,
			ReattachConfigCh: ch,
			CloseCh:          closeCh,
		},
	})

	var config *ReattachConfig
	select {
	case config = <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("should've received reattach")
	}

	c := NewClient(&ClientConfig{
		HandshakeConfig:  testHandshake,
		Plugins:          testGRPCPluginMap,
		Reattach:         config,
		AllowedProtocols: []Protocol{ProtocolGRPC},
	})
	if _, err := c.Client(); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Overwrite the discovery record with one that points elsewhere. Reconnect
	// must refuse rather than bind to whatever answers there.
	c.l.Lock()
	c.config.Reattach = &ReattachConfig{
		Protocol: config.Protocol,
		Addr:     &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 65001},
		Pid:      config.Pid,
		Test:     true,
	}
	c.l.Unlock()

	_, err := c.Reconnect(context.Background())
	if !errors.Is(err, ErrInstanceIdentityMismatch) {
		t.Fatalf("expected ErrInstanceIdentityMismatch, got %s", err)
	}

	cancel()
	<-closeCh
}

// TestClient_Reconnect_DiscoveryRecordReusedAddr verifies that a record that
// reuses the address but names a different process is refused: the address
// alone is not sufficient evidence.
func TestClient_Reconnect_DiscoveryRecordReusedAddr(t *testing.T) {
	c := newReconnectTestClient(t, ProtocolNetRPC, 1)

	view, _ := c.InstanceView()
	c.config.Reattach = &ReattachConfig{
		Addr:            view.Address,
		Pid:             4242, // not the bound process
		Protocol:        view.Protocol,
		ProtocolVersion: view.ProtocolVersion,
	}

	_, err := c.Reconnect(context.Background())
	if !errors.Is(err, ErrInstanceIdentityMismatch) {
		t.Fatalf("expected ErrInstanceIdentityMismatch, got %s", err)
	}
}

// TestClient_Reconnect_TestModeReattachSurvivesKill verifies the reattach test
// mode contract: Kill does not stop the plugin, so the instance remains
// reconnectable.
func TestClient_Reconnect_TestModeReattachSurvivesKill(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *ReattachConfig, 1)
	closeCh := make(chan struct{})
	go Serve(&ServeConfig{
		HandshakeConfig: testHandshake,
		Plugins:         testGRPCPluginMap,
		GRPCServer:      DefaultGRPCServer,
		Test: &ServeTestConfig{
			Context:          ctx,
			ReattachConfigCh: ch,
			CloseCh:          closeCh,
		},
	})

	var config *ReattachConfig
	select {
	case config = <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("should've received reattach")
	}

	c := NewClient(&ClientConfig{
		HandshakeConfig:  testHandshake,
		Plugins:          testGRPCPluginMap,
		Reattach:         config,
		AllowedProtocols: []Protocol{ProtocolGRPC},
	})

	client, err := c.Client()
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	view, ok := c.InstanceView()
	if !ok {
		t.Fatal("instance view missing")
	}
	if view.ID == "" {
		t.Fatal("test-mode reattach should still publish an identity")
	}

	// Kill is a no-op in test mode and must not mark the instance exited.
	c.Kill()
	afterKill, _ := c.InstanceView()
	if afterKill.State != InstanceStateActive {
		t.Fatalf("Kill must leave a test-mode instance active, got %s", afterKill.State)
	}

	// Reconnect still works against the same instance.
	reconnected, err := c.Reconnect(context.Background())
	if err != nil {
		t.Fatalf("reconnect failed: %s", err)
	}
	if err := reconnected.Ping(); err != nil {
		t.Fatalf("reconnected client should be healthy: %s", err)
	}
	if err := client.Ping(); err == nil {
		t.Fatal("the superseded connection should no longer be usable")
	}

	cancel()
	<-closeCh
}
