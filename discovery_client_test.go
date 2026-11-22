// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// newTestRegistryForClient returns a registry wired for client integration
// tests, with short cleanup bounds.
func newTestRegistryForClient(t *testing.T) *DiscoveryRegistry {
	t.Helper()
	registry, err := NewRegistry(context.Background(), RegistryConfig{
		Directory:            filepath.Join(t.TempDir(), "discovery"),
		RecordTTL:            time.Hour,
		CleanupTimeout:       time.Second,
		CleanupRetryInterval: 25 * time.Millisecond,
		MaxCleanupAttempts:   3,
		Now:                  time.Now,
	})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	return registry
}

func TestClient_DiscoveryLifecycleEndToEnd(t *testing.T) {
	registry := newTestRegistryForClient(t)
	identity, err := NewPluginIdentity(helperProcess("test-grpc").Path, "", int(testHandshake.ProtocolVersion), ProtocolGRPC)
	if err != nil {
		t.Fatalf("NewPluginIdentity: %v", err)
	}

	// First generation: start, full handshake, then reattach through the
	// registry instead of PID/port bookkeeping.
	c1 := NewClient(&ClientConfig{
		Cmd:               helperProcess("test-grpc"),
		HandshakeConfig:   testHandshake,
		Plugins:           testGRPCPluginMap,
		AllowedProtocols:  []Protocol{ProtocolGRPC},
		DiscoveryRegistry: registry,
		DiscoveryIdentity: identity,
	})
	client1, err := c1.Client()
	if err != nil {
		t.Fatalf("first client: %v", err)
	}

	current, ok := registry.Current(identity)
	if !ok {
		t.Fatal("no current record after handshake")
	}
	if current.State != RecordStateActive || current.Generation != 1 {
		t.Fatalf("current record = %+v, want active gen 1", current)
	}

	// The record carries the negotiated address and transport, not just a PID.
	// The helper may negotiate either a TCP or Unix listener.
	reattachAddr := current.Address
	if reattachAddr == "" || (current.Network != "tcp" && current.Network != "unix") {
		t.Fatalf("record missing negotiated address: %+v", current)
	}
	if current.RunnerID == "" || current.RunnerID == "0" {
		t.Fatalf("record missing the real runner ID: %+v", current)
	}

	// Kill the first client through the normal path. Its record must leave the
	// active state immediately and eventually be cleaned up.
	c1.Kill()
	if !c1.Exited() {
		t.Fatal("first plugin should have exited")
	}
	waitForTerminalState(t, registry, identity, 1)

	// Reattach is impossible once the record is gone or tombstoned.
	if _, err := registry.Reattach(context.Background(), identity); !errors.Is(err, ErrRecordNotFound) && !errors.Is(err, ErrRecordNotReattachable) {
		t.Fatalf("reattach after kill err = %v, want terminal-state error", err)
	}
	_ = client1
}

func TestClient_DiscoveryUpgradeReplacesAfterHandshake(t *testing.T) {
	registry := newTestRegistryForClient(t)
	identity, err := NewPluginIdentity(helperProcess("test-grpc").Path, "", int(testHandshake.ProtocolVersion), ProtocolGRPC)
	if err != nil {
		t.Fatalf("NewPluginIdentity: %v", err)
	}

	// Old instance.
	old := NewClient(&ClientConfig{
		Cmd:               helperProcess("test-grpc"),
		HandshakeConfig:   testHandshake,
		Plugins:           testGRPCPluginMap,
		AllowedProtocols:  []Protocol{ProtocolGRPC},
		DiscoveryRegistry: registry,
		DiscoveryIdentity: identity,
	})
	if _, err := old.Client(); err != nil {
		t.Fatalf("old client: %v", err)
	}
	oldRec, ok := registry.Current(identity)
	if !ok {
		t.Fatal("old record missing")
	}

	// New instance of the same identity. Until its handshake commits, the old
	// record stays current. After commit, it becomes the only reattachable
	// generation.
	next := NewClient(&ClientConfig{
		Cmd:               helperProcess("test-grpc"),
		HandshakeConfig:   testHandshake,
		Plugins:           testGRPCPluginMap,
		AllowedProtocols:  []Protocol{ProtocolGRPC},
		DiscoveryRegistry: registry,
		DiscoveryIdentity: identity,
	})
	if _, err := next.Client(); err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(next.Kill)

	newRec, ok := registry.Current(identity)
	if !ok {
		t.Fatal("new record missing")
	}
	if newRec.Generation <= oldRec.Generation {
		t.Fatalf("new generation %d not greater than old %d", newRec.Generation, oldRec.Generation)
	}
	if newRec.Address == oldRec.Address {
		t.Fatal("upgraded record kept the old listener address")
	}

	// The old generation can never be reattached, even with the generation
	// pinned explicitly. Bounded cleanup may already have deleted its record,
	// in which case the result is ErrRecordNotFound; both are terminal states.
	if _, err := registry.ReattachGeneration(context.Background(), identity, oldRec.Generation); !errors.Is(err, ErrRecordNotReattachable) && !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("superseded reattach err = %v, want a terminal-state error", err)
	}

	// And a fresh reattach resolves to the new generation, then works end to
	// end through NewClient.
	reattachCfg, err := ReattachConfigFromRecord(context.Background(), registry, identity)
	if err != nil {
		t.Fatalf("ReattachConfigFromRecord: %v", err)
	}
	reattached := NewClient(&ClientConfig{
		Reattach:         reattachCfg,
		HandshakeConfig:  testHandshake,
		Plugins:          testGRPCPluginMap,
		AllowedProtocols: []Protocol{ProtocolGRPC},
	})
	proto := reattached.Protocol()
	if proto != ProtocolGRPC {
		t.Fatalf("reattached protocol = %q, want grpc", proto)
	}
	reattached.Kill()

	// Old client's Kill must not disturb the new record: it tombstones its own
	// (already superseded) generation only.
	old.Kill()
	current, ok := registry.Current(identity)
	if !ok {
		t.Fatal("new record must survive the old client's Kill")
	}
	if current.Generation != newRec.Generation {
		t.Fatalf("current generation = %d, want %d", current.Generation, newRec.Generation)
	}
}

func TestClient_DiscoveryFailedStartTombstonesPending(t *testing.T) {
	registry := newTestRegistryForClient(t)
	identity, err := NewPluginIdentity(helperProcess("bad-version").Path, "", int(testHandshake.ProtocolVersion), ProtocolGRPC)
	if err != nil {
		t.Fatalf("NewPluginIdentity: %v", err)
	}

	c := NewClient(&ClientConfig{
		Cmd:               helperProcess("bad-version"),
		HandshakeConfig:   testHandshake,
		Plugins:           testGRPCPluginMap,
		AllowedProtocols:  []Protocol{ProtocolGRPC},
		StartTimeout:      5 * time.Second,
		DiscoveryRegistry: registry,
		DiscoveryIdentity: identity,
	})
	if _, err := c.Client(); err == nil {
		t.Fatal("expected handshake failure for bad-version helper")
	}

	// The claimed record must have been tombstoned rather than left pending,
	// and the identity has no reattachable record.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		current, ok := registry.Current(identity)
		if !ok {
			break
		}
		if current.State == RecordStateTombstoned || current.State == RecordStateCleaned {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if current, ok := registry.Current(identity); ok && current.State == RecordStatePending {
		t.Fatalf("failed start left a pending, un-reattachable record: %+v", current)
	}
	if _, err := registry.Reattach(context.Background(), identity); err == nil {
		t.Fatal("failed-start identity must not be reattachable")
	}
}

// waitForTerminalState polls until the given generation no longer has an
// active record, giving bounded asynchronous cleanup time to finish.
func waitForTerminalState(t *testing.T, registry *DiscoveryRegistry, identity PluginIdentity, generation uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rec, err := registry.ReattachGeneration(context.Background(), identity, generation)
		if errors.Is(err, ErrRecordNotFound) || errors.Is(err, ErrRecordNotReattachable) {
			return
		}
		if err == nil && rec.State != RecordStateActive {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("generation %d never reached a terminal state", generation)
}
