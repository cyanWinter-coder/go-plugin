// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"time"
)

// Discovery lifecycle states. A record moves through these states in exactly
// one direction: once it leaves Active it can never be reattached, and once it
// reaches Cleaned it is removed from the registry entirely.
const (
	// RecordStatePending is the state of a record whose process has been
	// claimed but has not yet completed the full protocol handshake. Pending
	// records are never reattachable.
	RecordStatePending = "pending"

	// RecordStateActive is the state of a record whose process has completed
	// the full handshake and is the current record for its plugin identity.
	// Only Active records can be reattached.
	RecordStateActive = "active"

	// RecordStateTombstoned is the state of a record that has been killed or
	// superseded by a new generation. Tombstoned records can never be
	// reattached; their cleanup runs asynchronously and is bounded.
	RecordStateTombstoned = "tombstoned"

	// RecordStateCleaned is the terminal state of a record whose resources
	// have been fully released. Cleaned records are deleted from disk.
	RecordStateCleaned = "cleaned"
)

var (
	// ErrRecordNotFound is returned when no record exists for the given
	// identity or generation.
	ErrRecordNotFound = errors.New("plugin discovery record not found")

	// ErrRecordNotReattachable is returned when a record exists but its state
	// does not permit reattachment, for example because it is still pending
	// the handshake, has been tombstoned, or has expired.
	ErrRecordNotReattachable = errors.New("plugin discovery record is not reattachable")

	// ErrRecordTerminal is returned when an operation attempts to transition
	// a record that has already reached a terminal state.
	ErrRecordTerminal = errors.New("plugin discovery record has reached a terminal state")

	// ErrGenerationMismatch is returned when an operation targets a record
	// generation that no longer matches the record on disk, for example
	// because another caller already committed a newer generation.
	ErrGenerationMismatch = errors.New("plugin discovery record generation mismatch")

	// ErrRegistryClosed is returned when the registry has been closed.
	ErrRegistryClosed = errors.New("plugin discovery registry is closed")
)

// PluginIdentity identifies the plugin program itself, independently of which
// process happens to be running it or which port it happens to be listening
// on. Every field is validated during discovery so that a reused PID or port
// can never cause one version of a plugin to be mistaken for another.
type PluginIdentity struct {
	// ExecutablePath is the absolute path to the plugin executable.
	ExecutablePath string

	// ExecutableDigest is the hex-encoded digest of the executable at the
	// time the record was created, for example a SHA-256 checksum. It may be
	// empty if the host does not verify executable integrity, in which case
	// ExecutablePath alone identifies the program.
	ExecutableDigest string

	// DigestAlgorithm names the algorithm used for ExecutableDigest, e.g.
	// "sha256". It must be set when ExecutableDigest is set.
	DigestAlgorithm string

	// ProtocolVersion is the go-plugin handshake protocol version negotiated
	// with the plugin (HandshakeConfig.ProtocolVersion).
	ProtocolVersion int

	// TransportProtocol is the transport negotiated during the handshake,
	// e.g. ProtocolGRPC or ProtocolNetRPC.
	TransportProtocol Protocol
}

// key returns the stable on-disk key for an identity. Records belonging to
// different versions of the same executable path use different keys, so an
// upgrade never overwrites the record of the version it replaces.
func (id PluginIdentity) key() string {
	return fmt.Sprintf("%s|%s|%s|%d|%s",
		id.ExecutablePath,
		id.DigestAlgorithm,
		id.ExecutableDigest,
		id.ProtocolVersion,
		id.TransportProtocol,
	)
}

// validate checks that an identity is well-formed.
func (id PluginIdentity) validate() error {
	if id.ExecutablePath == "" {
		return errors.New("plugin identity: ExecutablePath is required")
	}
	if (id.ExecutableDigest == "") != (id.DigestAlgorithm == "") {
		return errors.New("plugin identity: ExecutableDigest and DigestAlgorithm must be set together")
	}
	if id.TransportProtocol == ProtocolInvalid {
		return errors.New("plugin identity: TransportProtocol is required")
	}
	return nil
}

// DiscoveryRecord is the versioned lifecycle record of one running plugin
// instance. It is persisted atomically by the registry and is the unit of
// ownership for all resources associated with the instance.
//
// A record must never be mutated after it is returned from the registry; the
// registry copies records before handing them out.
type DiscoveryRecord struct {
	// Identity is the program identity of the plugin.
	Identity PluginIdentity

	// Generation is the monotonic start generation of this instance. Each
	// successful handshake produces a new, strictly higher generation for the
	// same identity. It is used instead of PID or port to tell instances apart.
	Generation uint64

	// State is the current lifecycle state of the record.
	State string

	// RunnerID is the runner-reported identifier of the running process, e.g.
	// a PID or container ID. It is informational and never used on its own to
	// decide reattachability.
	RunnerID string

	// Network and Address record the negotiated listener in wire form so the
	// record can be persisted as JSON. They are converted back to a net.Addr
	// by Addr.
	Network string
	Address string

	// Liveness, when non-nil, is used to confirm that the process backing the
	// record is still alive and accepting connections before reattachment. It
	// is not persisted; the registry config supplies it on load.
	Liveness LivenessChecker `json:"-"`

	// Resources are filesystem paths owned exclusively by this instance, for
	// example its Unix socket directory. They are removed during bounded
	// cleanup and are persisted so cleanup still works if the host restarts.
	Resources []string `json:"resources,omitempty"`

	// Cleanup, when non-nil, releases in-process resources owned by this
	// instance. It is invoked at most once, under the bounded cleanup
	// machinery, after Resources have been removed. It is not persisted.
	Cleanup CleanupFunc `json:"-"`

	// registry is a non-persisted back-reference to the registry that handed
	// out this record, so a client built by reattaching through a registry can
	// tombstone the same record on Kill. It is nil for records created outside
	// a registry (including legacy tests).
	registry *DiscoveryRegistry `json:"-"`

	// CreatedAt is when the record was first claimed.
	CreatedAt time.Time

	// CommittedAt is when the full handshake completed and the record became
	// active. It is zero for pending records.
	CommittedAt time.Time

	// ExpiresAt is when an active record is considered stale and may no longer
	// be reattached. A zero value means the record does not expire on time.
	ExpiresAt time.Time

	// TombstonedAt is when the record left the Active state. It is zero until
	// then.
	TombstonedAt time.Time

	// CleanupAttempts counts how many times bounded cleanup has run for this
	// record.
	CleanupAttempts int

	// LastCleanupError holds the error from the most recent failed cleanup
	// attempt. It is cleared on success.
	LastCleanupError string
}

// Addr reconstructs the net.Addr captured during the handshake.
func (r *DiscoveryRecord) Addr() (net.Addr, error) {
	switch r.Network {
	case "tcp":
		return net.ResolveTCPAddr("tcp", r.Address)
	case "unix":
		return net.ResolveUnixAddr("unix", r.Address)
	case "":
		return nil, errors.New("record has no negotiated address")
	default:
		return nil, fmt.Errorf("unknown network in record: %q", r.Network)
	}
}

// ReattachConfig builds a ReattachConfig from the record's negotiated
// handshake information. Reattachability has already been decided by the
// registry (identity, generation, state, expiry and liveness); the PID here is
// only the attach handle, never the basis of the decision. When the record's
// RunnerID is not a numeric PID, the returned config has no PID and the caller
// must supply a ReattachFunc before starting the client.
func (r *DiscoveryRecord) ReattachConfig() (*ReattachConfig, error) {
	addr, err := r.Addr()
	if err != nil {
		return nil, err
	}

	cfg := &ReattachConfig{
		Protocol:        r.Identity.TransportProtocol,
		ProtocolVersion: r.Identity.ProtocolVersion,
		Addr:            addr,
		Test:            false,
	}
	if pid, convErr := strconv.Atoi(r.RunnerID); convErr == nil {
		cfg.Pid = pid
	}
	return cfg, nil
}

// NewPluginIdentity builds a PluginIdentity for executablePath, hashing the
// file with digestAlgorithm (e.g. "sha256") so that an upgraded binary has a
// distinct identity even if its path is unchanged. The digest can be omitted
// by passing an empty digestAlgorithm, in which case the path alone identifies
// the program.
func NewPluginIdentity(executablePath string, digestAlgorithm string, protocolVersion int, transport Protocol) (PluginIdentity, error) {
	abs, err := filepath.Abs(executablePath)
	if err != nil {
		return PluginIdentity{}, err
	}

	id := PluginIdentity{
		ExecutablePath:    abs,
		ProtocolVersion:   protocolVersion,
		TransportProtocol: transport,
	}
	if digestAlgorithm == "" {
		return id, id.validate()
	}

	hash, ok := digestHashes[digestAlgorithm]
	if !ok {
		return PluginIdentity{}, fmt.Errorf("unsupported digest algorithm %q", digestAlgorithm)
	}
	sum, err := hashFile(abs, hash())
	if err != nil {
		return PluginIdentity{}, err
	}
	id.DigestAlgorithm = digestAlgorithm
	id.ExecutableDigest = sum
	return id, id.validate()
}

// clone returns a deep-enough copy of the record for handing to callers. The
// function fields are copied by reference; they are stateless closures.
func (r *DiscoveryRecord) clone() *DiscoveryRecord {
	cp := *r
	return &cp
}
