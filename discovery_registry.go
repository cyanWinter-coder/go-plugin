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
	"time"

	"github.com/hashicorp/go-hclog"
)

const (
	// lockFileName is the advisory lock file used to serialize discovery
	// operations across host processes.
	lockFileName = ".discovery.lock"

	// lockRetryInterval is how long a lock attempt waits before retrying.
	lockRetryInterval = 50 * time.Millisecond

	// defaultRecordTTL is how long after its handshake a record remains
	// reattachable unless the registry is configured with another TTL.
	defaultRecordTTL = 24 * time.Hour

	// defaultClaimTTL is how long a pending record may wait for its handshake
	// before it is treated as abandoned.
	defaultClaimTTL = 5 * time.Minute

	// defaultCleanupTimeout bounds how long a single cleanup attempt may take.
	defaultCleanupTimeout = 10 * time.Second

	// defaultCleanupRetryInterval bounds how long a failed cleanup is retried
	// asynchronously before Sweep reports it back to the caller.
	defaultCleanupRetryInterval = time.Minute

	// defaultMaxCleanupAttempts is the number of attempts a record's cleanup
	// receives before the failure is surfaced through Sweep.
	defaultMaxCleanupAttempts = 3
)

// LivenessChecker confirms that the process backing a record is still alive
// before reattachment. Implementations must not block forever: they run under
// the registry's liveness timeout. The default checker dials the record's
// negotiated address; hosts with custom runners can add their own check here.
type LivenessChecker func(ctx context.Context, record *DiscoveryRecord) error

// CleanupFunc releases in-process resources owned by a record. It runs under
// the registry's bounded cleanup machinery and must respect the passed
// context. Filesystem resources should be recorded in DiscoveryRecord.Resources
// instead so they survive host restarts.
type CleanupFunc func(ctx context.Context, record *DiscoveryRecord) error

// RegistryConfig configures a DiscoveryRegistry. The zero value is not valid;
// use NewRegistry which applies defaults.
type RegistryConfig struct {
	// Directory is the discovery directory in which records are persisted. It
	// is created if it does not exist. Existing callers that use Discover on a
	// plugin directory can keep using a separate directory; the two mechanisms
	// do not share files.
	Directory string

	// Liveness is consulted before every reattachment. When nil, the default
	// checker dials the record's negotiated net.Addr.
	Liveness LivenessChecker

	// RecordTTL bounds how long an active record stays reattachable after its
	// handshake. Zero means the default; a negative value disables expiry.
	RecordTTL time.Duration

	// ClaimTTL bounds how long a pending record may wait for its handshake. A
	// pending record older than this is considered abandoned (for example its
	// host crashed mid-handshake) and is tombstoned on registry load and during
	// sweeps. Zero means the default; a negative value disables the timeout.
	ClaimTTL time.Duration

	// CleanupTimeout bounds a single cleanup attempt. Zero means the default.
	CleanupTimeout time.Duration

	// CleanupRetryInterval is the delay between asynchronous cleanup retries.
	// Zero means the default.
	CleanupRetryInterval time.Duration

	// MaxCleanupAttempts is the number of cleanup attempts before a failure is
	// reported by Sweep. Zero means the default.
	MaxCleanupAttempts int

	// Now is used for all time decisions. It is overridable in tests.
	Now func() time.Time

	// Logger receives registry diagnostics. When nil, logs are discarded.
	Logger hclog.Logger
}

// DiscoveryRegistry owns the versioned lifecycle of plugin discovery records.
//
// A record is keyed by the full plugin identity (executable, digest, protocol
// version and transport), never by PID or port alone. Within one identity the
// start generation is strictly monotonic: a new process replaces the old
// discovery record only after its full handshake commits; records that are
// pending, tombstoned or expired can never be reattached.
//
// All mutating operations take an inter-process advisory lock on the discovery
// directory, and an in-process mutex on top, so concurrent discovery, Kill and
// cleanup of the same record converge on a single terminal state.
type DiscoveryRegistry struct {
	cfg RegistryConfig

	// mu protects the in-memory maps below. Every public operation that
	// touches them also holds the inter-process file lock.
	mu sync.Mutex

	// records holds every record known to this registry, keyed by
	// identityKey -> generation -> record.
	records map[string]map[uint64]*DiscoveryRecord

	// current holds the generation of the Active record for each identity, or
	// zero when the identity has no active record.
	current map[string]uint64

	// cleanupMu guards cleanupJobs.
	cleanupMu   sync.Mutex
	cleanupJobs map[string]chan error
	cleanupWG   sync.WaitGroup

	closeOnce sync.Once
	closeCh   chan struct{}
	closed    bool
}

// ClaimOptions carries the information available as soon as a plugin process
// has been started, before the handshake completes.
type ClaimOptions struct {
	// Identity is the program identity being claimed.
	Identity PluginIdentity

	// RunnerID is the runner-reported process identifier (PID, container ID...).
	RunnerID string

	// Resources are filesystem paths owned exclusively by the new instance.
	Resources []string

	// Cleanup, when set, runs after Resources are released during bounded
	// cleanup.
	Cleanup CleanupFunc
}

// HandshakeResult is the evidence that a full protocol handshake completed.
// Commit accepts it and is the only operation that can make a record active.
type HandshakeResult struct {
	// Network and Address are the negotiated listener, in net.Addr wire form.
	Network string
	Address string

	// Liveness overrides the registry-wide checker for this record when set.
	Liveness LivenessChecker
}

// NewRegistry opens (or creates) the discovery directory and loads any records
// left behind by previous host processes. Tombstoned and pending records found
// on disk are re-scheduled for bounded cleanup; expired active records are
// tombstoned. Load failures for individual corrupt records are skipped.
func NewRegistry(ctx context.Context, cfg RegistryConfig) (*DiscoveryRegistry, error) {
	if cfg.Directory == "" {
		return nil, errors.New("registry config: Directory is required")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.RecordTTL == 0 {
		cfg.RecordTTL = defaultRecordTTL
	}
	if cfg.ClaimTTL == 0 {
		cfg.ClaimTTL = defaultClaimTTL
	}
	if cfg.CleanupTimeout == 0 {
		cfg.CleanupTimeout = defaultCleanupTimeout
	}
	if cfg.CleanupRetryInterval == 0 {
		cfg.CleanupRetryInterval = defaultCleanupRetryInterval
	}
	if cfg.MaxCleanupAttempts == 0 {
		cfg.MaxCleanupAttempts = defaultMaxCleanupAttempts
	}
	if cfg.Logger == nil {
		cfg.Logger = hclog.NewNullLogger()
	}

	if err := os.MkdirAll(cfg.Directory, 0o700); err != nil {
		return nil, fmt.Errorf("creating discovery directory: %w", err)
	}

	r := &DiscoveryRegistry{
		cfg:         cfg,
		records:     make(map[string]map[uint64]*DiscoveryRecord),
		current:     make(map[string]uint64),
		cleanupJobs: make(map[string]chan error),
		closeCh:     make(chan struct{}),
	}

	// The initial load takes the file lock and the in-process mutex like any
	// other operation, so it cannot interleave with another registry's writes
	// on the same directory or with this registry's own cleanup goroutines.
	if err := r.withLock(ctx, func() error { return r.loadLocked(ctx) }); err != nil {
		return nil, err
	}
	return r, nil
}

// syncFromDiskLocked rebuilds the in-memory index from the on-disk records.
// It reconciles the two directions of drift between registries sharing a
// directory: records another process wrote are adopted, and records another
// process cleaned up (deleted) are dropped. In-memory-only records that are
// being cleaned up by this process cannot exist on this path because every
// state change is persisted before the index changes.
//
// The caller must hold the registry locks.
func (r *DiscoveryRegistry) syncFromDiskLocked() error {
	onDisk, err := loadRecords(r.cfg.Directory)
	if err != nil {
		return err
	}

	now := r.cfg.Now()
	seen := make(map[string]map[uint64]bool)
	for _, rec := range onDisk {
		key := rec.Identity.key()
		if seen[key] == nil {
			seen[key] = make(map[uint64]bool)
		}
		seen[key][rec.Generation] = true

		existing := r.records[key][rec.Generation]
		if existing != nil && existing.State == rec.State {
			// Keep the in-memory record: it carries this registry's
			// non-persisted Liveness/Cleanup hooks and is identical in state.
			r.reconcileCurrentLocked(rec, now)
			continue
		}

		// A pending record past its claim TTL is an abandoned handshake: it can
		// never become reattachable, so tombstone it and reclaim its resources.
		if rec.State == RecordStatePending && r.isPendingStale(rec, now) {
			rec.State = RecordStateTombstoned
			rec.TombstonedAt = now
			if writeErr := writeRecordAtomic(r.cfg.Directory, rec); writeErr != nil {
				r.cfg.Logger.Warn("persisting stale pending tombstone", "generation", rec.Generation, "error", writeErr)
			}
		}

		r.putRecord(rec)
		r.reconcileCurrentLocked(rec, now)

		// A tombstoned record adopted from another process still owns
		// resources on disk; make sure this registry drives their cleanup.
		if rec.State == RecordStateTombstoned {
			r.scheduleCleanup(rec)
		}
	}

	for key, gens := range r.records {
		for gen := range gens {
			if !seen[key][gen] {
				// The record file is gone: another registry finished cleanup.
				// Drop it and clear any dangling current pointer.
				delete(r.records[key], gen)
				if r.current[key] == gen {
					r.current[key] = 0
				}
			}
		}
		if len(r.records[key]) == 0 {
			delete(r.records, key)
			delete(r.current, key)
		}
	}
	return nil
}

// reconcileCurrentLocked keeps the current-generation pointer consistent with
// an on-disk record: active records can hold the pointer, non-active records
// cannot.
func (r *DiscoveryRegistry) reconcileCurrentLocked(rec *DiscoveryRecord, now time.Time) {
	key := rec.Identity.key()
	switch rec.State {
	case RecordStateActive:
		if r.current[key] == rec.Generation {
			return
		}
		// Only an unexpired active record may be current.
		if rec.ExpiresAt.IsZero() || !now.After(rec.ExpiresAt) {
			if gen := r.current[key]; rec.Generation > gen {
				r.current[key] = rec.Generation
			}
		}
	default:
		if r.current[key] == rec.Generation {
			r.current[key] = 0
		}
	}
}

// isPendingStale reports whether a pending record has outlived its claim TTL.
func (r *DiscoveryRegistry) isPendingStale(rec *DiscoveryRecord, now time.Time) bool {
	if r.cfg.ClaimTTL < 0 {
		return false
	}
	return now.Sub(rec.CreatedAt) > r.cfg.ClaimTTL
}

// withLock runs fn while holding the inter-process discovery lock and the
// in-process mutex. The file lock is released even if fn panics or errors.
func (r *DiscoveryRegistry) withLock(ctx context.Context, fn func() error) error {
	lock, err := acquireLock(ctx, filepath.Join(r.cfg.Directory, lockFileName))
	if err != nil {
		return err
	}
	defer func() { _ = lock.release() }()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrRegistryClosed
	}
	return fn()
}

// loadLocked reads the directory and rebuilds the in-memory index, re-driving
// cleanup for anything that was not active and clean.
func (r *DiscoveryRegistry) loadLocked(ctx context.Context) error {
	loaded, err := loadRecords(r.cfg.Directory)
	if err != nil {
		return err
	}

	now := r.cfg.Now()
	for _, rec := range loaded {
		key := rec.Identity.key()

		switch rec.State {
		case RecordStateCleaned:
			// A record marked cleaned whose file survived a crash: finish the
			// job by deleting the file.
			_ = removeRecord(r.cfg.Directory, rec)
			continue

		case RecordStateTombstoned:
			r.putRecord(rec)
			r.scheduleCleanup(rec)
			continue

		case RecordStatePending:
			// Only an abandoned handshake (a pending record past its claim TTL)
			// is reclaimed on load. A fresh pending record may belong to another
			// host process that is still negotiating, so it is left alone.
			if !r.isPendingStale(rec, now) {
				r.putRecord(rec)
				continue
			}
			rec.State = RecordStateTombstoned
			rec.TombstonedAt = now
			if err := writeRecordAtomic(r.cfg.Directory, rec); err != nil {
				r.cfg.Logger.Warn("persisting stale pending tombstone", "error", err)
			}
			r.putRecord(rec)
			r.scheduleCleanup(rec)
			continue

		case RecordStateActive:
			// An expired record can never be reattached, even if its process
			// still answers. Tombstone it but leave cleanup for Sweep/Kill.
			if !rec.ExpiresAt.IsZero() && now.After(rec.ExpiresAt) {
				rec.State = RecordStateTombstoned
				rec.TombstonedAt = now
				if err := writeRecordAtomic(r.cfg.Directory, rec); err != nil {
					r.cfg.Logger.Warn("persisting expired record tombstone", "error", err)
				}
				r.putRecord(rec)
				r.scheduleCleanup(rec)
				continue
			}

			r.putRecord(rec)
			if gen := r.current[key]; rec.Generation > gen {
				r.current[key] = rec.Generation
			}
		}
	}
	return nil
}

// Claim reserves the next start generation for an identity. The returned
// record is pending: it is persisted so a crash before the handshake cannot be
// mistaken for a running plugin, but it cannot be reattached. Claim does not
// touch the identity's current active record, so an in-place upgrade keeps
// serving the old instance until the new one has fully shaken hands.
func (r *DiscoveryRegistry) Claim(ctx context.Context, opts ClaimOptions) (*DiscoveryRecord, error) {
	if err := opts.Identity.validate(); err != nil {
		return nil, err
	}

	var out *DiscoveryRecord
	err := r.withLock(ctx, func() error {
		// Adopt records written by other host processes before deciding the
		// next generation, so generations stay unique registry-wide.
		if err := r.syncFromDiskLocked(); err != nil {
			return err
		}
		gen := r.highestGeneration(opts.Identity.key()) + 1

		rec := &DiscoveryRecord{
			Identity:   opts.Identity,
			Generation: gen,
			State:      RecordStatePending,
			RunnerID:   opts.RunnerID,
			Resources:  append([]string(nil), opts.Resources...),
			Cleanup:    opts.Cleanup,
			CreatedAt:  r.cfg.Now(),
		}
		if err := writeRecordAtomic(r.cfg.Directory, rec); err != nil {
			return err
		}
		r.putRecord(rec)
		out = rec.clone()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Commit marks a claimed record active after its full handshake. It is the
// only operation that can replace the identity's current record: the previous
// active generation is tombstoned (and its cleanup scheduled) only once the
// new generation has committed successfully. A failed handshake leaves the
// pending record in place and never disturbs the old instance.
func (r *DiscoveryRegistry) Commit(ctx context.Context, identity PluginIdentity, generation uint64, hs HandshakeResult) (*DiscoveryRecord, error) {
	if err := identity.validate(); err != nil {
		return nil, err
	}
	if hs.Network == "" || hs.Address == "" {
		return nil, errors.New("commit: handshake network and address are required")
	}

	var out *DiscoveryRecord
	err := r.withLock(ctx, func() error {
		if err := r.syncFromDiskLocked(); err != nil {
			return err
		}
		rec, err := r.requireRecord(identity, generation)
		if err != nil {
			return err
		}
		if rec.State != RecordStatePending {
			return fmt.Errorf("%w: cannot commit record in state %q", ErrRecordTerminal, rec.State)
		}
		if hs.Liveness != nil {
			rec.Liveness = hs.Liveness
		}

		rec.State = RecordStateActive
		rec.Network = hs.Network
		rec.Address = hs.Address
		rec.CommittedAt = r.cfg.Now()
		if r.cfg.RecordTTL > 0 {
			rec.ExpiresAt = rec.CommittedAt.Add(r.cfg.RecordTTL)
		}
		if err := writeRecordAtomic(r.cfg.Directory, rec); err != nil {
			return err
		}

		// Supersede the previously active generation, if any. Cleanup failure
		// is recorded and retried asynchronously; it must not block the new
		// instance from becoming current.
		key := identity.key()
		prevGen := r.current[key]
		if prevGen != 0 && prevGen != generation {
			if prev, ok := r.records[key][prevGen]; ok && prev.State == RecordStateActive {
				if err := r.tombstoneLocked(prev); err != nil {
					r.cfg.Logger.Warn("tombstoning superseded record", "generation", prevGen, "error", err)
				}
			}
		}
		r.current[key] = generation

		out = rec.clone()
		out.registry = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Reattach returns the current, reattachable record for an identity after
// re-validating its lifecycle state, expiry and liveness. It never returns a
// pending, tombstoned or expired record. If the backing process is found to be
// dead, the record is tombstoned automatically.
func (r *DiscoveryRegistry) Reattach(ctx context.Context, identity PluginIdentity) (*DiscoveryRecord, error) {
	return r.reattachGeneration(ctx, identity, 0)
}

// ReattachGeneration is like Reattach but also pins the generation: callers
// holding an older record can never be silently redirected to a newer or
// tombstoned instance.
func (r *DiscoveryRegistry) ReattachGeneration(ctx context.Context, identity PluginIdentity, generation uint64) (*DiscoveryRecord, error) {
	if generation == 0 {
		return nil, errors.New("reattach: generation must be non-zero")
	}
	return r.reattachGeneration(ctx, identity, generation)
}

func (r *DiscoveryRegistry) reattachGeneration(ctx context.Context, identity PluginIdentity, generation uint64) (*DiscoveryRecord, error) {
	if err := identity.validate(); err != nil {
		return nil, err
	}

	var out *DiscoveryRecord
	err := r.withLock(ctx, func() error {
		if err := r.syncFromDiskLocked(); err != nil {
			return err
		}
		key := identity.key()
		gen := generation
		if gen == 0 {
			gen = r.current[key]
			if gen == 0 {
				// The identity may have pending/tombstoned records but no
				// active one; distinguish that from a completely unknown
				// identity so callers get a precise non-reattachable error.
				if _, known := r.records[key]; known {
					return ErrRecordNotReattachable
				}
				return fmt.Errorf("%w: no active record for identity", ErrRecordNotFound)
			}
		}

		rec, err := r.requireRecord(identity, gen)
		if err != nil {
			return err
		}
		if rec.State != RecordStateActive {
			return fmt.Errorf("%w: record state is %q", ErrRecordNotReattachable, rec.State)
		}
		if r.current[key] != gen {
			// The record was active once but a newer generation has since
			// committed. Reattaching here would bind to a superseded version.
			return fmt.Errorf("%w: generation %d was superseded by %d", ErrRecordNotReattachable, gen, r.current[key])
		}
		if !rec.ExpiresAt.IsZero() && r.cfg.Now().After(rec.ExpiresAt) {
			// Expired records can never be reattached. Move it out of the way
			// and reclaim its resources asynchronously.
			if err := r.tombstoneLocked(rec); err != nil {
				r.cfg.Logger.Warn("tombstoning expired record during reattach", "error", err)
			}
			return fmt.Errorf("%w: record expired at %s", ErrRecordNotReattachable, rec.ExpiresAt.Format(time.RFC3339))
		}

		if err := r.checkLiveness(ctx, rec); err != nil {
			if err := r.tombstoneLocked(rec); err != nil {
				r.cfg.Logger.Warn("tombstoning dead record during reattach", "error", err)
			}
			return fmt.Errorf("%w: liveness check failed: %v", ErrRecordNotReattachable, err)
		}

		out = rec.clone()
		out.registry = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// checkLiveness dials the negotiated address and, when configured, runs the
// record's or registry's liveness checker.
func (r *DiscoveryRegistry) checkLiveness(ctx context.Context, rec *DiscoveryRecord) error {
	addr, err := rec.Addr()
	if err != nil {
		return err
	}

	dialer := &net.Dialer{Timeout: r.cfg.CleanupTimeout}
	conn, err := dialer.DialContext(ctx, addr.Network(), addr.String())
	if err != nil {
		return err
	}
	_ = conn.Close()

	check := rec.Liveness
	if check == nil {
		check = r.cfg.Liveness
	}
	if check != nil {
		return check(ctx, rec)
	}
	return nil
}

// Tombstone moves the identity's current record (or a specific generation) to
// the tombstoned state and schedules bounded cleanup. It is idempotent:
// tombstoning an already tombstoned record is not an error. A tombstoned
// record can never be reattached.
func (r *DiscoveryRegistry) Tombstone(ctx context.Context, identity PluginIdentity, generation uint64) error {
	return r.withLock(ctx, func() error {
		if err := r.syncFromDiskLocked(); err != nil {
			return err
		}
		gen := generation
		if gen == 0 {
			gen = r.current[identity.key()]
			if gen == 0 {
				return nil
			}
		}
		rec, err := r.requireRecord(identity, gen)
		if err != nil {
			return err
		}
		if rec.State == RecordStateCleaned {
			return nil
		}
		if rec.State == RecordStateTombstoned {
			return nil
		}
		return r.tombstoneLocked(rec)
	})
}

// Kill tombstones the identity's current record (or a specific generation),
// waits for its bounded cleanup to finish, and returns any cleanup error. The
// record is un-reattachable the moment Kill enters, before cleanup runs, so a
// concurrent discovery can never bind to it while resources are being released.
func (r *DiscoveryRegistry) Kill(ctx context.Context, identity PluginIdentity, generation uint64) error {
	var job chan error
	err := r.withLock(ctx, func() error {
		gen := generation
		if gen == 0 {
			gen = r.current[identity.key()]
			if gen == 0 {
				// No active record: a previous Kill/Sweep already reached the
				// terminal state, or the identity never had one. Idempotent.
				return nil
			}
		}
		rec, err := r.requireRecord(identity, gen)
		if errors.Is(err, ErrRecordNotFound) {
			// A concurrent Kill/Sweep may already have reached the terminal
			// state and deleted the record. Kill is idempotent, so this is
			// success: there is nothing reattachable left to clean up.
			return nil
		}
		if err != nil {
			return err
		}

		if rec.State == RecordStateCleaned {
			return nil
		}
		if rec.State != RecordStateTombstoned {
			if err := r.tombstoneLocked(rec); err != nil {
				return err
			}
		}
		job = r.cleanupJob(recordJobKey(rec))
		return nil
	})
	if err != nil {
		return err
	}
	if job == nil {
		return nil
	}

	select {
	case cleanupErr := <-job:
		return cleanupErr
	case <-ctx.Done():
		return ctx.Err()
	case <-r.closeCh:
		return ErrRegistryClosed
	}
}

// SweepReport summarizes a Sweep run.
type SweepReport struct {
	// Cleaned are generations whose resources were fully released and whose
	// records were deleted.
	Cleaned []uint64
	// Failed are generations whose cleanup exhausted every attempt without
	// succeeding. Their records remain on disk, tombstoned and un-reattachable.
	Failed map[uint64]error
}

// Sweep re-drives bounded cleanup for every tombstoned record under an
// identity and returns the outcome. Expired active records are tombstoned
// first. Sweep never returns an error for individual cleanup failures; they
// are reported instead so one stuck resource cannot block the others.
func (r *DiscoveryRegistry) Sweep(ctx context.Context, identity PluginIdentity) (SweepReport, error) {
	report := SweepReport{Failed: make(map[uint64]error)}
	err := r.withLock(ctx, func() error {
		if err := r.syncFromDiskLocked(); err != nil {
			return err
		}
		key := identity.key()
		now := r.cfg.Now()

		for gen, rec := range r.records[key] {
			switch rec.State {
			case RecordStatePending:
				// Reclaim an abandoned handshake within the same sweep.
				if !r.isPendingStale(rec, now) {
					continue
				}
				if err := r.tombstoneLocked(rec); err != nil {
					r.cfg.Logger.Warn("tombstoning stale pending record during sweep", "generation", gen, "error", err)
					continue
				}
				if err := r.cleanupLocked(ctx, rec); err != nil {
					report.Failed[gen] = err
					continue
				}
				report.Cleaned = append(report.Cleaned, gen)
			case RecordStateActive:
				if !rec.ExpiresAt.IsZero() && now.After(rec.ExpiresAt) {
					if err := r.tombstoneLocked(rec); err != nil {
						r.cfg.Logger.Warn("tombstoning expired record during sweep", "generation", gen, "error", err)
					}
				}
			case RecordStateTombstoned:
				if err := r.cleanupLocked(ctx, rec); err != nil {
					report.Failed[gen] = err
					continue
				}
				report.Cleaned = append(report.Cleaned, gen)
			case RecordStateCleaned:
				report.Cleaned = append(report.Cleaned, gen)
			}
		}
		return nil
	})
	return report, err
}

// Current returns the current record for an identity without performing a
// liveness check. It is intended for diagnostics; reattachment must go through
// Reattach or ReattachGeneration.
func (r *DiscoveryRegistry) Current(identity PluginIdentity) (*DiscoveryRecord, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := identity.key()
	gen := r.current[key]
	if gen == 0 {
		return nil, false
	}
	rec, ok := r.records[key][gen]
	if !ok {
		return nil, false
	}
	return rec.clone(), true
}

// Close stops background cleanup goroutines. Records on disk are untouched:
// reopening the registry re-drives their cleanup.
func (r *DiscoveryRegistry) Close() error {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		close(r.closeCh)
		r.mu.Unlock()
	})
	r.cleanupWG.Wait()
	return nil
}

// tombstoned transitions a record to the tombstoned state, persists it, drops
// it from the current index and schedules cleanup. The caller must hold both
// locks (i.e. be inside withLock).
func (r *DiscoveryRegistry) tombstoneLocked(rec *DiscoveryRecord) error {
	if rec.State == RecordStateTombstoned || rec.State == RecordStateCleaned {
		r.scheduleCleanup(rec)
		return nil
	}
	if err := assertTransition(rec.State, RecordStateTombstoned); err != nil {
		return err
	}

	rec.State = RecordStateTombstoned
	if rec.TombstonedAt.IsZero() {
		rec.TombstonedAt = r.cfg.Now()
	}
	if err := writeRecordAtomic(r.cfg.Directory, rec); err != nil {
		return err
	}

	key := rec.Identity.key()
	if r.current[key] == rec.Generation {
		r.current[key] = 0
	}
	r.scheduleCleanup(rec)
	return nil
}

// cleanupJob returns the completion channel for a record's cleanup job, or nil
// if no job is running (cleanup already finished or never needed).
func (r *DiscoveryRegistry) cleanupJob(jobKey string) chan error {
	r.cleanupMu.Lock()
	defer r.cleanupMu.Unlock()
	return r.cleanupJobs[jobKey]
}

// scheduleCleanup starts a single bounded cleanup goroutine per record.
// Repeated calls for the same record reuse the existing job channel, so
// concurrent Kill and cleanup cannot release the same resources twice.
func (r *DiscoveryRegistry) scheduleCleanup(rec *DiscoveryRecord) {
	jobKey := recordJobKey(rec)

	r.cleanupMu.Lock()
	defer r.cleanupMu.Unlock()

	if _, ok := r.cleanupJobs[jobKey]; ok {
		return
	}
	ch := make(chan error, 1)
	r.cleanupJobs[jobKey] = ch
	r.cleanupWG.Add(1)

	go func() {
		defer r.cleanupWG.Done()
		ch <- r.cleanupLoop(rec.Identity, rec.Generation)
	}()
}

// cleanupLoop runs bounded cleanup attempts with backoff until one succeeds,
// the attempt budget is exhausted, or the registry closes. Every attempt
// re-acquires the file lock and re-reads the record, so the state machine
// stays authoritative across processes.
func (r *DiscoveryRegistry) cleanupLoop(identity PluginIdentity, generation uint64) error {
	jobKey := recordJobKeyFrom(identity, generation)
	defer func() {
		r.cleanupMu.Lock()
		delete(r.cleanupJobs, jobKey)
		r.cleanupMu.Unlock()
	}()

	var lastErr error
	for attempt := 1; attempt <= r.cfg.MaxCleanupAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), r.cfg.CleanupTimeout)
		err := r.runCleanupOnce(ctx, mustRecord(identity, generation))
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		r.cfg.Logger.Warn("bounded plugin cleanup attempt failed",
			"identity", identity.key(), "generation", generation, "attempt", attempt, "error", err)

		if attempt == r.cfg.MaxCleanupAttempts {
			break
		}
		select {
		case <-r.closeCh:
			return lastErr
		case <-time.After(r.cfg.CleanupRetryInterval):
		}
	}
	return lastErr
}

// runCleanupOnce performs a single cleanup attempt under the registry locks
// and, on success, marks the record cleaned and deletes its file. A missing
// record counts as success: the terminal state has already been reached.
func (r *DiscoveryRegistry) runCleanupOnce(ctx context.Context, rec *DiscoveryRecord) error {
	return r.withLock(ctx, func() error {
		return r.cleanupLocked(ctx, rec)
	})
}

// cleanupLocked is the lock-free inner cleanup. The caller must already hold
// the registry locks (via withLock), which lets Sweep clean several records in
// one critical section without re-entrant locking.
func (r *DiscoveryRegistry) cleanupLocked(ctx context.Context, rec *DiscoveryRecord) error {
	current, err := r.requireRecord(rec.Identity, rec.Generation)
	if errors.Is(err, ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if current.State == RecordStateCleaned {
		return nil
	}

	cleanupCtx, cancel := context.WithTimeout(ctx, r.cfg.CleanupTimeout)
	defer cancel()

	// Filesystem resources are released first and are owned exclusively by
	// this generation.
	for _, path := range current.Resources {
		if err := os.RemoveAll(path); err != nil {
			r.recordCleanupFailure(current, err)
			return err
		}
	}
	if current.Cleanup != nil {
		if err := current.Cleanup(cleanupCtx, current); err != nil {
			r.recordCleanupFailure(current, err)
			return err
		}
	}

	if err := assertTransition(current.State, RecordStateCleaned); err != nil {
		return err
	}
	current.State = RecordStateCleaned
	if err := removeRecord(r.cfg.Directory, current); err != nil {
		r.recordCleanupFailure(current, err)
		return err
	}

	key := current.Identity.key()
	delete(r.records[key], current.Generation)
	if len(r.records[key]) == 0 {
		delete(r.records, key)
	}
	if r.current[key] == current.Generation {
		r.current[key] = 0
	}
	return nil
}

// recordCleanupFailure persists the error on the record for Sweep diagnostics.
func (r *DiscoveryRegistry) recordCleanupFailure(rec *DiscoveryRecord, cleanupErr error) {
	rec.CleanupAttempts++
	rec.LastCleanupError = cleanupErr.Error()
	if err := writeRecordAtomic(r.cfg.Directory, rec); err != nil {
		r.cfg.Logger.Warn("persisting cleanup failure", "error", err)
	}
}

// putRecord indexes a record, creating the generation map as needed.
func (r *DiscoveryRegistry) putRecord(rec *DiscoveryRecord) {
	key := rec.Identity.key()
	if r.records[key] == nil {
		r.records[key] = make(map[uint64]*DiscoveryRecord)
	}
	r.records[key][rec.Generation] = rec
}

// requireRecord returns the in-memory record for an identity/generation.
func (r *DiscoveryRegistry) requireRecord(identity PluginIdentity, generation uint64) (*DiscoveryRecord, error) {
	recs, ok := r.records[identity.key()]
	if !ok {
		return nil, fmt.Errorf("%w: identity is unknown", ErrRecordNotFound)
	}
	rec, ok := recs[generation]
	if !ok {
		return nil, fmt.Errorf("%w: generation %d", ErrRecordNotFound, generation)
	}
	return rec, nil
}

// highestGeneration returns the largest generation stored for an identity.
func (r *DiscoveryRegistry) highestGeneration(key string) uint64 {
	var highest uint64
	for gen := range r.records[key] {
		if gen > highest {
			highest = gen
		}
	}
	return highest
}

// assertTransition encodes the single-direction state machine.
func assertTransition(from, to string) error {
	allowed := map[string]map[string]bool{
		RecordStatePending: {
			RecordStateActive:     true,
			RecordStateTombstoned: true,
		},
		RecordStateActive: {
			RecordStateTombstoned: true,
		},
		RecordStateTombstoned: {
			RecordStateCleaned: true,
		},
		RecordStateCleaned: {},
	}
	if next, ok := allowed[from]; ok && next[to] {
		return nil
	}
	return fmt.Errorf("%w: illegal transition %q -> %q", ErrRecordTerminal, from, to)
}

func recordJobKey(rec *DiscoveryRecord) string {
	return recordJobKeyFrom(rec.Identity, rec.Generation)
}

func recordJobKeyFrom(identity PluginIdentity, generation uint64) string {
	return fmt.Sprintf("%s#%d", identity.key(), generation)
}

// mustRecord returns a minimal record handle for cleanup goroutines. The
// goroutine re-reads authoritative state under the lock; this only carries the
// identity and generation through runCleanupOnce's signature.
func mustRecord(identity PluginIdentity, generation uint64) *DiscoveryRecord {
	return &DiscoveryRecord{Identity: identity, Generation: generation}
}
