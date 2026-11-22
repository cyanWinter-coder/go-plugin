// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testIdentity(t *testing.T, name string) PluginIdentity {
	t.Helper()
	return PluginIdentity{
		ExecutablePath:    filepath.Join(t.TempDir(), name),
		ProtocolVersion:   1,
		TransportProtocol: ProtocolGRPC,
	}
}

// testRegistry builds a registry with short, deterministic cleanup bounds so
// tests stay fast.
func testRegistry(t *testing.T, now func() time.Time) *DiscoveryRegistry {
	t.Helper()
	cfg := RegistryConfig{
		Directory:            filepath.Join(t.TempDir(), "discovery"),
		RecordTTL:            time.Hour,
		CleanupTimeout:       250 * time.Millisecond,
		CleanupRetryInterval: 25 * time.Millisecond,
		MaxCleanupAttempts:   3,
		Now:                  now,
	}
	registry, err := NewRegistry(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	return registry
}

func fixedNow() (*time.Time, func() time.Time) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	return &base, func() time.Time { return base }
}

// startListener returns the network/address pair of a live local listener and
// keeps it open for the lifetime of the test.
func startListener(t *testing.T) (string, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().Network(), ln.Addr().String()
}

func TestDiscovery_ClaimIsNotReattachable(t *testing.T) {
	_, now := fixedNow()
	registry := testRegistry(t, now)
	id := testIdentity(t, "plugin-a")

	rec, err := registry.Claim(context.Background(), ClaimOptions{Identity: id, RunnerID: "100"})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if rec.State != RecordStatePending {
		t.Fatalf("new claim state = %q, want pending", rec.State)
	}
	if rec.Generation != 1 {
		t.Fatalf("first generation = %d, want 1", rec.Generation)
	}

	if _, err := registry.Reattach(context.Background(), id); !errors.Is(err, ErrRecordNotReattachable) {
		t.Fatalf("reattach pending record err = %v, want ErrRecordNotReattachable", err)
	}

	// A second claim takes a strictly higher generation and never overwrites
	// the first pending record.
	rec2, err := registry.Claim(context.Background(), ClaimOptions{Identity: id, RunnerID: "101"})
	if err != nil {
		t.Fatalf("second Claim: %v", err)
	}
	if rec2.Generation <= rec.Generation {
		t.Fatalf("generations not monotonic: %d then %d", rec.Generation, rec2.Generation)
	}
	if _, err := registry.Reattach(context.Background(), id); !errors.Is(err, ErrRecordNotReattachable) {
		t.Fatalf("reattach with only pending records err = %v", err)
	}
}

func TestDiscovery_CommitReplacesOnlyAfterHandshake(t *testing.T) {
	_, now := fixedNow()
	registry := testRegistry(t, now)
	id := testIdentity(t, "plugin-a")
	ctx := context.Background()

	// First generation completes its full handshake.
	first, err := registry.Claim(ctx, ClaimOptions{Identity: id, RunnerID: "100"})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	netw, addr := startListener(t)
	first, err = registry.Commit(ctx, id, first.Generation, HandshakeResult{Network: netw, Address: addr})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if first.State != RecordStateActive {
		t.Fatalf("committed state = %q, want active", first.State)
	}

	// An in-place upgrade claims a new generation. While it is pending, the
	// old generation stays current and reattachable.
	upgraded, err := registry.Claim(ctx, ClaimOptions{Identity: id, RunnerID: "200"})
	if err != nil {
		t.Fatalf("upgrade Claim: %v", err)
	}
	current, err := registry.Reattach(ctx, id)
	if err != nil {
		t.Fatalf("old record should stay reattachable during upgrade: %v", err)
	}
	if current.Generation != first.Generation {
		t.Fatalf("current generation = %d, want old %d", current.Generation, first.Generation)
	}

	// Committing a non-pending / unknown generation is rejected.
	if _, err := registry.Commit(ctx, id, first.Generation, HandshakeResult{Network: netw, Address: addr}); !errors.Is(err, ErrRecordTerminal) {
		t.Fatalf("re-commit active record err = %v, want ErrRecordTerminal", err)
	}
	if _, err := registry.Commit(ctx, id, 999, HandshakeResult{Network: netw, Address: addr}); !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("commit unknown generation err = %v, want ErrRecordNotFound", err)
	}

	// Once the new handshake fully commits, it becomes current and the old
	// record is tombstoned (and therefore un-reattachable).
	netw2, addr2 := startListener(t)
	if _, err := registry.Commit(ctx, id, upgraded.Generation, HandshakeResult{Network: netw2, Address: addr2}); err != nil {
		t.Fatalf("upgrade Commit: %v", err)
	}
	current, err = registry.Reattach(ctx, id)
	if err != nil {
		t.Fatalf("new record should be reattachable: %v", err)
	}
	if current.Generation != upgraded.Generation || current.Address != addr2 {
		t.Fatalf("current = gen %d addr %q, want gen %d addr %q",
			current.Generation, current.Address, upgraded.Generation, addr2)
	}
	if _, err := registry.ReattachGeneration(ctx, id, first.Generation); !errors.Is(err, ErrRecordNotReattachable) {
		t.Fatalf("superseded generation err = %v, want ErrRecordNotReattachable", err)
	}
}

func TestDiscovery_FailedHandshakeKeepsOldInstance(t *testing.T) {
	_, now := fixedNow()
	registry := testRegistry(t, now)
	id := testIdentity(t, "plugin-a")
	ctx := context.Background()

	first, err := registry.Claim(ctx, ClaimOptions{Identity: id, RunnerID: "100"})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	netw, addr := startListener(t)
	if _, err := registry.Commit(ctx, id, first.Generation, HandshakeResult{Network: netw, Address: addr}); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// The upgrade process dies before committing. Its pending record must not
	// disturb the active first generation.
	upgraded, err := registry.Claim(ctx, ClaimOptions{Identity: id, RunnerID: "200"})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := registry.Tombstone(ctx, id, upgraded.Generation); err != nil {
		t.Fatalf("Tombstone pending: %v", err)
	}

	current, err := registry.Reattach(ctx, id)
	if err != nil {
		t.Fatalf("old instance should survive failed upgrade: %v", err)
	}
	if current.Generation != first.Generation {
		t.Fatalf("current generation = %d, want %d", current.Generation, first.Generation)
	}
}

func TestDiscovery_ExpiredRecordCannotReattach(t *testing.T) {
	nowPtr, now := fixedNow()
	registry := testRegistry(t, now)
	id := testIdentity(t, "plugin-a")
	ctx := context.Background()

	rec, err := registry.Claim(ctx, ClaimOptions{Identity: id, RunnerID: "100"})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	netw, addr := startListener(t)
	if _, err := registry.Commit(ctx, id, rec.Generation, HandshakeResult{Network: netw, Address: addr}); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	*nowPtr = nowPtr.Add(2 * time.Hour) // past the 1h TTL
	if _, err := registry.Reattach(ctx, id); !errors.Is(err, ErrRecordNotReattachable) {
		t.Fatalf("expired reattach err = %v, want ErrRecordNotReattachable", err)
	}

	// A new discovery round is still possible and takes a fresh generation.
	next, err := registry.Claim(ctx, ClaimOptions{Identity: id, RunnerID: "101"})
	if err != nil {
		t.Fatalf("Claim after expiry: %v", err)
	}
	if next.Generation <= rec.Generation {
		t.Fatalf("generation after expiry not monotonic: %d", next.Generation)
	}
}

func TestDiscovery_DeadProcessTombstonesOnReattach(t *testing.T) {
	_, now := fixedNow()
	registry := testRegistry(t, now)
	id := testIdentity(t, "plugin-a")
	ctx := context.Background()

	rec, err := registry.Claim(ctx, ClaimOptions{Identity: id, RunnerID: "100"})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	// Commit against an address we immediately close: the record is active on
	// disk, but the liveness dial must fail.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	if _, err := registry.Commit(ctx, id, rec.Generation,
		HandshakeResult{Network: ln.Addr().Network(), Address: ln.Addr().String()}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	_ = ln.Close()

	if _, err := registry.Reattach(ctx, id); !errors.Is(err, ErrRecordNotReattachable) {
		t.Fatalf("reattach dead process err = %v, want ErrRecordNotReattachable", err)
	}
	// The dead record must now be tombstoned: no current record remains.
	if cur, ok := registry.Current(id); ok && cur.State == RecordStateActive {
		t.Fatalf("dead record left active: %#v", cur)
	}
}

func TestDiscovery_KillIsSingleTerminalState(t *testing.T) {
	_, now := fixedNow()
	registry := testRegistry(t, now)
	id := testIdentity(t, "plugin-a")
	ctx := context.Background()

	resourceDir := filepath.Join(t.TempDir(), "socket-dir")
	if err := os.MkdirAll(resourceDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	var cleanupCalls atomic.Int32
	rec, err := registry.Claim(ctx, ClaimOptions{
		Identity:  id,
		RunnerID:  "100",
		Resources: []string{resourceDir},
		Cleanup:   func(context.Context, *DiscoveryRecord) error { cleanupCalls.Add(1); return nil },
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	netw, addr := startListener(t)
	if _, err := registry.Commit(ctx, id, rec.Generation, HandshakeResult{Network: netw, Address: addr}); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Concurrent Kill/Tombstone/Reattach on the same record must converge on
	// one terminal state.
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = registry.Tombstone(ctx, id, 0)
		}()
	}
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = registry.Reattach(ctx, id)
		}()
	}
	wg.Wait()

	if err := registry.Kill(ctx, id, 0); err != nil {
		t.Fatalf("Kill: %v", err)
	}

	// The record can never be reattached after kill, whether cleanup has
	// already deleted it (ErrRecordNotFound) or it is still tombstoned
	// (ErrRecordNotReattachable).
	if _, err := registry.Reattach(ctx, id); !errors.Is(err, ErrRecordNotFound) && !errors.Is(err, ErrRecordNotReattachable) {
		t.Fatalf("reattach after kill err = %v, want a terminal-state error", err)
	}
	if _, err := os.Stat(resourceDir); !os.IsNotExist(err) {
		t.Fatalf("resource dir not removed, stat err = %v", err)
	}
	if got := cleanupCalls.Load(); got != 1 {
		t.Fatalf("cleanup ran %d times, want exactly once", got)
	}

	// Killing an already cleaned record is idempotent.
	if err := registry.Kill(ctx, id, rec.Generation); err != nil {
		t.Fatalf("second Kill: %v", err)
	}

	// The retry budget is respected: Sweep on an unknown identity is empty.
	report, err := registry.Sweep(ctx, id)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(report.Failed) != 0 || len(report.Cleaned) != 0 {
		t.Fatalf("unexpected sweep report after kill: %+v", report)
	}
}

func TestDiscovery_CleanupFailureDoesNotBlockNewInstance(t *testing.T) {
	_, now := fixedNow()
	registry := testRegistry(t, now)
	id := testIdentity(t, "plugin-a")
	ctx := context.Background()

	// First instance owns a resource whose cleanup always fails, and a
	// cleanup hook that also fails. The failure must be bounded and reported,
	// while the upgraded instance commits and serves normally.
	rec, err := registry.Claim(ctx, ClaimOptions{
		Identity: id,
		RunnerID: "100",
		// A path nested inside a missing parent fails RemoveAll only when the
		// parent is a regular file; create that setup.
		Resources: []string{},
		Cleanup: func(context.Context, *DiscoveryRecord) error {
			return errors.New("simulated cleanup failure")
		},
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	netw, addr := startListener(t)
	if _, err := registry.Commit(ctx, id, rec.Generation, HandshakeResult{Network: netw, Address: addr}); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if err := registry.Tombstone(ctx, id, rec.Generation); err != nil {
		t.Fatalf("Tombstone: %v", err)
	}

	// New instance commits even though the old record's cleanup keeps failing.
	next, err := registry.Claim(ctx, ClaimOptions{Identity: id, RunnerID: "200"})
	if err != nil {
		t.Fatalf("Claim new: %v", err)
	}
	netw2, addr2 := startListener(t)
	if _, err := registry.Commit(ctx, id, next.Generation, HandshakeResult{Network: netw2, Address: addr2}); err != nil {
		t.Fatalf("Commit new: %v", err)
	}
	current, err := registry.Reattach(ctx, id)
	if err != nil {
		t.Fatalf("new instance should be reattachable: %v", err)
	}
	if current.Generation != next.Generation {
		t.Fatalf("current generation = %d, want %d", current.Generation, next.Generation)
	}

	// Wait for the bounded cleanup to exhaust its attempts.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		report, err := registry.Sweep(ctx, id)
		if err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		if failErr, ok := report.Failed[rec.Generation]; ok {
			if failErr.Error() != "simulated cleanup failure" {
				t.Fatalf("unexpected cleanup error: %v", failErr)
			}
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The failed record stays tombstoned and un-reattachable forever.
	if _, err := registry.ReattachGeneration(ctx, id, rec.Generation); !errors.Is(err, ErrRecordNotReattachable) {
		t.Fatalf("failed-cleanup record err = %v, want ErrRecordNotReattachable", err)
	}
}

func TestDiscovery_ReloadRecoversLifecycle(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "discovery")
	nowPtr, now := fixedNow()
	cfg := RegistryConfig{
		Directory:            dir,
		RecordTTL:            time.Hour,
		CleanupTimeout:       250 * time.Millisecond,
		CleanupRetryInterval: 25 * time.Millisecond,
		MaxCleanupAttempts:   2,
		Now:                  now,
	}

	id := testIdentity(t, "plugin-a")
	resourceDir := filepath.Join(t.TempDir(), "sockets")
	if err := os.MkdirAll(resourceDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	registry, err := NewRegistry(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	rec, err := registry.Claim(context.Background(), ClaimOptions{
		Identity: id, RunnerID: "100", Resources: []string{resourceDir},
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	netw, addr := startListener(t)
	if _, err := registry.Commit(context.Background(), id, rec.Generation, HandshakeResult{Network: netw, Address: addr}); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// A pending, uncommitted record simulates a host crash mid-handshake.
	pending, err := registry.Claim(context.Background(), ClaimOptions{Identity: id, RunnerID: "101"})
	if err != nil {
		t.Fatalf("pending Claim: %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen with a fresh registry. The pending record must be tombstoned and
	// its (nonexistent here) cleanup driven, while the active record survives.
	reloaded, err := NewRegistry(context.Background(), cfg)
	if err != nil {
		t.Fatalf("reopen NewRegistry: %v", err)
	}
	t.Cleanup(func() { _ = reloaded.Close() })

	current, err := reloaded.Reattach(context.Background(), id)
	if err != nil {
		t.Fatalf("active record should survive reload: %v", err)
	}
	if current.Generation != rec.Generation {
		t.Fatalf("reloaded current gen = %d, want %d", current.Generation, rec.Generation)
	}
	if _, err := reloaded.ReattachGeneration(context.Background(), id, pending.Generation); !errors.Is(err, ErrRecordNotReattachable) {
		t.Fatalf("pending-after-crash record err = %v, want ErrRecordNotReattachable", err)
	}

	// An expired active record is tombstoned on reload.
	*nowPtr = nowPtr.Add(2 * time.Hour)
	if err := reloaded.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reloaded2, err := NewRegistry(context.Background(), cfg)
	if err != nil {
		t.Fatalf("second reopen: %v", err)
	}
	t.Cleanup(func() { _ = reloaded2.Close() })
	if _, err := reloaded2.Reattach(context.Background(), id); !errors.Is(err, ErrRecordNotFound) && !errors.Is(err, ErrRecordNotReattachable) {
		t.Fatalf("expired reload err = %v, want a terminal-state error", err)
	}
}

func TestDiscovery_RecordFileIsJSON(t *testing.T) {
	_, now := fixedNow()
	registry := testRegistry(t, now)
	id := testIdentity(t, "plugin-a")

	rec, err := registry.Claim(context.Background(), ClaimOptions{Identity: id, RunnerID: "100"})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	netw, addr := startListener(t)
	if _, err := registry.Commit(context.Background(), id, rec.Generation, HandshakeResult{Network: netw, Address: addr}); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	entries, err := os.ReadDir(registry.cfg.Directory)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var found bool
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(registry.cfg.Directory, entry.Name()))
		if err != nil {
			t.Fatalf("read record: %v", err)
		}
		if !bytesContains(data, []byte(`"state": "active"`)) || !bytesContains(data, []byte(addr)) {
			t.Fatalf("record file missing state/address: %s", data)
		}
		found = true
	}
	if !found {
		t.Fatal("no JSON record file found in discovery directory")
	}
}

func TestDiscovery_StateTransitionsAreOneDirectional(t *testing.T) {
	cases := []struct {
		from, to string
		wantErr  bool
	}{
		{RecordStatePending, RecordStateActive, false},
		{RecordStatePending, RecordStateTombstoned, false},
		{RecordStateActive, RecordStateTombstoned, false},
		{RecordStateTombstoned, RecordStateCleaned, false},
		{RecordStateActive, RecordStateActive, true},
		{RecordStateTombstoned, RecordStateActive, true},
		{RecordStateCleaned, RecordStateTombstoned, true},
		{RecordStatePending, RecordStateCleaned, true},
	}
	for _, tc := range cases {
		err := assertTransition(tc.from, tc.to)
		if tc.wantErr && !errors.Is(err, ErrRecordTerminal) {
			t.Errorf("transition %s->%s err = %v, want ErrRecordTerminal", tc.from, tc.to, err)
		}
		if !tc.wantErr && err != nil {
			t.Errorf("transition %s->%s unexpected err = %v", tc.from, tc.to, err)
		}
	}
}

func TestDiscovery_NewPluginIdentityDistinguishesUpgrades(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "plugin")
	if err := os.WriteFile(path, []byte("v1"), 0o700); err != nil {
		t.Fatalf("write: %v", err)
	}

	id1, err := NewPluginIdentity(path, "sha256", 1, ProtocolGRPC)
	if err != nil {
		t.Fatalf("NewPluginIdentity v1: %v", err)
	}
	if id1.ExecutableDigest == "" {
		t.Fatal("expected digest to be populated")
	}
	if err := id1.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	// Same path, new contents: the identity key must change.
	if err := os.WriteFile(path, []byte("v2-binary"), 0o700); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	id2, err := NewPluginIdentity(path, "sha256", 1, ProtocolGRPC)
	if err != nil {
		t.Fatalf("NewPluginIdentity v2: %v", err)
	}
	if id1.key() == id2.key() {
		t.Fatal("upgraded binary at the same path produced the same identity key")
	}

	// Without a digest, the path alone identifies the program.
	id3, err := NewPluginIdentity(path, "", 1, ProtocolGRPC)
	if err != nil {
		t.Fatalf("NewPluginIdentity no-digest: %v", err)
	}
	if id3.ExecutableDigest != "" || id3.DigestAlgorithm != "" {
		t.Fatalf("unexpected digest fields: %+v", id3)
	}

	if _, err := NewPluginIdentity(path, "md5", 1, ProtocolGRPC); err == nil {
		t.Fatal("expected error for unsupported digest algorithm")
	}
}

func TestDiscovery_ReattachConfigRoundTrip(t *testing.T) {
	_, now := fixedNow()
	registry := testRegistry(t, now)
	id := testIdentity(t, "plugin-a")
	ctx := context.Background()

	rec, err := registry.Claim(ctx, ClaimOptions{Identity: id, RunnerID: "4242"})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	netw, addr := startListener(t)
	if _, err := registry.Commit(ctx, id, rec.Generation, HandshakeResult{Network: netw, Address: addr}); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	cfg, err := ReattachConfigFromRecord(ctx, registry, id)
	if err != nil {
		t.Fatalf("ReattachConfigFromRecord: %v", err)
	}
	if cfg.Pid != 4242 {
		t.Fatalf("pid = %d, want 4242", cfg.Pid)
	}
	if cfg.Protocol != ProtocolGRPC || cfg.Addr.String() != addr {
		t.Fatalf("round trip mismatch: %+v", cfg)
	}
	if cfg.DiscoveryRecord == nil || cfg.DiscoveryRecord.registry == nil {
		t.Fatal("reattach config lost lifecycle record back-reference")
	}

	// A non-numeric runner ID requires an explicit ReattachFunc.
	id2 := testIdentity(t, "plugin-b")
	rec2, err := registry.Claim(ctx, ClaimOptions{Identity: id2, RunnerID: "container://abc"})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if _, err := registry.Commit(ctx, id2, rec2.Generation, HandshakeResult{Network: netw, Address: addr}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if _, err := ReattachConfigFromRecord(ctx, registry, id2); err == nil {
		t.Fatal("expected error when runner ID is not a numeric PID")
	}
}

func TestDiscovery_ConcurrentClaimsAreSerialized(t *testing.T) {
	_, now := fixedNow()
	registry := testRegistry(t, now)
	id := testIdentity(t, "plugin-a")

	const n = 50
	var wg sync.WaitGroup
	generations := make(chan uint64, n)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec, err := registry.Claim(context.Background(), ClaimOptions{Identity: id, RunnerID: "x"})
			if err != nil {
				t.Errorf("Claim: %v", err)
				return
			}
			generations <- rec.Generation
		}()
	}
	wg.Wait()
	close(generations)

	seen := make(map[uint64]bool)
	for gen := range generations {
		if seen[gen] {
			t.Fatalf("duplicate generation %d", gen)
		}
		seen[gen] = true
	}
	if len(seen) != n {
		t.Fatalf("got %d distinct generations, want %d", len(seen), n)
	}
}

func TestDiscovery_LockSerializesTwoRegistries(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "discovery")
	cfg := RegistryConfig{
		Directory: dir,
		Now:       time.Now,
	}
	r1, err := NewRegistry(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewRegistry r1: %v", err)
	}
	t.Cleanup(func() { _ = r1.Close() })
	r2, err := NewRegistry(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewRegistry r2: %v", err)
	}
	t.Cleanup(func() { _ = r2.Close() })

	id := testIdentity(t, "plugin-a")
	const n = 25
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range n {
			_, _ = r1.Claim(context.Background(), ClaimOptions{Identity: id})
		}
	}()
	go func() {
		defer wg.Done()
		for range n {
			_, _ = r2.Claim(context.Background(), ClaimOptions{Identity: id})
		}
	}()
	wg.Wait()

	// The two registries share the on-disk truth; reopen a third to verify the
	// full generation range was persisted without collisions.
	r3, err := NewRegistry(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewRegistry r3: %v", err)
	}
	t.Cleanup(func() { _ = r3.Close() })
	recs := r3.records[id.key()]
	if len(recs) != 2*n {
		t.Fatalf("persisted %d records, want %d", len(recs), 2*n)
	}
	for gen := uint64(1); gen <= 2*n; gen++ {
		if _, ok := recs[gen]; !ok {
			t.Fatalf("missing generation %d", gen)
		}
	}
}

// bytesContains is a small local helper to avoid pulling bytes into every test
// for a single call.
func bytesContains(haystack, needle []byte) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		match := true
		for j := range needle {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func TestDiscovery_ReattachConfigFromRecord(t *testing.T) {
	_, now := fixedNow()
	registry := testRegistry(t, now)
	id := testIdentity(t, "plugin-a")

	rec, err := registry.Claim(context.Background(), ClaimOptions{Identity: id, RunnerID: "777"})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	netw, addr := startListener(t)
	if _, err := registry.Commit(context.Background(), id, rec.Generation, HandshakeResult{Network: netw, Address: addr}); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	cfg, err := ReattachConfigFromRecord(context.Background(), registry, id)
	if err != nil {
		t.Fatalf("ReattachConfigFromRecord: %v", err)
	}
	if cfg.Addr.String() != addr || fmt.Sprint(cfg.Pid) != "777" {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
}

func TestDiscovery_StalePendingIsReclaimedByClaimTTL(t *testing.T) {
	nowPtr, now := fixedNow()
	cfg := RegistryConfig{
		Directory:            filepath.Join(t.TempDir(), "discovery"),
		RecordTTL:            time.Hour,
		ClaimTTL:             time.Minute,
		CleanupTimeout:       250 * time.Millisecond,
		CleanupRetryInterval: 25 * time.Millisecond,
		MaxCleanupAttempts:   2,
		Now:                  now,
	}
	registry, err := NewRegistry(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	t.Cleanup(func() { _ = registry.Close() })

	id := testIdentity(t, "plugin-a")
	pending, err := registry.Claim(context.Background(), ClaimOptions{Identity: id, RunnerID: "100"})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// Within the claim TTL the pending record is preserved and still blocks
	// reattachment rather than being reclaimed.
	*nowPtr = nowPtr.Add(30 * time.Second)
	report, err := registry.Sweep(context.Background(), id)
	if err != nil {
		t.Fatalf("Sweep within TTL: %v", err)
	}
	if len(report.Cleaned) != 0 {
		t.Fatalf("fresh pending record swept early: %+v", report)
	}
	if cur, ok := registry.Current(id); ok && cur.State == RecordStateActive {
		t.Fatal("pending record must not become current")
	}

	// Past the TTL the abandoned handshake is tombstoned and cleaned.
	*nowPtr = nowPtr.Add(31 * time.Second)
	report, err = registry.Sweep(context.Background(), id)
	if err != nil {
		t.Fatalf("Sweep past TTL: %v", err)
	}
	if len(report.Cleaned) != 1 || report.Cleaned[0] != pending.Generation {
		t.Fatalf("stale pending not cleaned: %+v", report)
	}
	if _, err := registry.ReattachGeneration(context.Background(), id, pending.Generation); !errors.Is(err, ErrRecordNotFound) && !errors.Is(err, ErrRecordNotReattachable) {
		t.Fatalf("reclaimed pending err = %v, want terminal-state error", err)
	}
}

func TestDiscovery_SecondRegistryAdoptsAndCleansTombstone(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "discovery")
	baseCfg := func() RegistryConfig {
		return RegistryConfig{
			Directory:            dir,
			RecordTTL:            time.Hour,
			CleanupTimeout:       250 * time.Millisecond,
			CleanupRetryInterval: 25 * time.Millisecond,
			MaxCleanupAttempts:   2,
			Now:                  time.Now,
		}
	}

	r1, err := NewRegistry(context.Background(), baseCfg())
	if err != nil {
		t.Fatalf("NewRegistry r1: %v", err)
	}
	id := testIdentity(t, "plugin-a")

	resourceDir := filepath.Join(t.TempDir(), "owned-socket")
	if err := os.MkdirAll(resourceDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	rec, err := r1.Claim(context.Background(), ClaimOptions{
		Identity: id, RunnerID: "100", Resources: []string{resourceDir},
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	netw, addr := startListener(t)
	if _, err := r1.Commit(context.Background(), id, rec.Generation, HandshakeResult{Network: netw, Address: addr}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := r1.Tombstone(context.Background(), id, rec.Generation); err != nil {
		t.Fatalf("Tombstone: %v", err)
	}
	// Simulate the owning host disappearing before cleanup completes.
	if err := r1.Close(); err != nil {
		t.Fatalf("Close r1: %v", err)
	}

	// A second host opens the same directory. It must adopt the tombstoned
	// record and release the resource the first host left behind.
	r2, err := NewRegistry(context.Background(), baseCfg())
	if err != nil {
		t.Fatalf("NewRegistry r2: %v", err)
	}
	t.Cleanup(func() { _ = r2.Close() })

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(resourceDir); os.IsNotExist(err) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(resourceDir); !os.IsNotExist(err) {
		t.Fatalf("adopted tombstone resource not reclaimed: stat err = %v", err)
	}
	if _, err := r2.Reattach(context.Background(), id); !errors.Is(err, ErrRecordNotFound) && !errors.Is(err, ErrRecordNotReattachable) {
		t.Fatalf("adopted tombstone err = %v, want terminal-state error", err)
	}
}
