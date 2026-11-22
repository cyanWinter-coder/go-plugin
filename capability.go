// Copyright IBM Corp. 2016, 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Capability is a named, optional protocol feature that a client or server
// may support. Capabilities are exchanged during the handshake so that both
// sides agree on a common feature set before any RPC connection is made.
type Capability string

const (
	// CapabilityNetRPC indicates support for the net/rpc protocol.
	CapabilityNetRPC Capability = "netrpc"

	// CapabilityGRPC indicates support for the gRPC protocol.
	CapabilityGRPC Capability = "grpc"

	// CapabilityMTLS indicates support for automatic mutual TLS transport
	// authentication (see ClientConfig.AutoMTLS).
	CapabilityMTLS Capability = "mtls"

	// CapabilityStdioSync indicates support for streaming the plugin's
	// stdout/stderr back to the client (see ClientConfig.SyncStdout and
	// ClientConfig.SyncStderr).
	CapabilityStdioSync Capability = "stdio-sync"

	// CapabilityGRPCBrokerMultiplex indicates support for multiplexing
	// brokered gRPC servers over the plugin's listener.
	CapabilityGRPCBrokerMultiplex Capability = "grpc-broker-multiplex"
)

const (
	// envClientCapabilities carries the client's declared capability set to
	// the server as a comma-separated list.
	envClientCapabilities = "PLUGIN_CLIENT_CAPABILITIES"

	// envClientRequiredCapabilities carries the capabilities the client
	// refuses to start without to the server as a comma-separated list.
	envClientRequiredCapabilities = "PLUGIN_CLIENT_REQUIRED_CAPABILITIES"

	// envClientInstanceID carries the client's instance identity to the
	// server. The server answers with its own instance identity in the
	// handshake response.
	envClientInstanceID = "PLUGIN_CLIENT_INSTANCE_ID"
)

// ErrCapabilityNegotiation is returned (wrapped) when the client and server
// cannot agree on a capability set that satisfies the client's required
// capabilities.
var ErrCapabilityNegotiation = errors.New("plugin capability negotiation failed")

// DefaultCapabilities returns the capability set assumed for peers that do
// not exchange capability fields in the handshake (plugins and hosts built
// against older versions of this library). These match the features that
// have long been part of the core protocol.
func DefaultCapabilities() CapabilitySet {
	return NewCapabilitySet(
		CapabilityNetRPC,
		CapabilityGRPC,
		CapabilityMTLS,
		CapabilityStdioSync,
	)
}

// CapabilitySet is a set of capabilities.
type CapabilitySet map[Capability]struct{}

// NewCapabilitySet returns a CapabilitySet containing the given capabilities.
func NewCapabilitySet(caps ...Capability) CapabilitySet {
	s := make(CapabilitySet, len(caps))
	for _, c := range caps {
		s.Add(c)
	}
	return s
}

// maxCapabilityLen bounds a single capability name.
const maxCapabilityLen = 64

// capabilityPattern restricts capability names to a conservative alphabet so
// they can safely travel in environment variables and the handshake line.
var capabilityPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

// validCapability reports whether c is a well-formed capability name.
func validCapability(c Capability) bool {
	return len(c) > 0 && len(c) <= maxCapabilityLen && capabilityPattern.MatchString(string(c))
}

// ParseCapabilitySet parses a comma-separated capability list. An empty
// string yields an empty set. It is an error for any entry to be empty or
// malformed.
func ParseCapabilitySet(s string) (CapabilitySet, error) {
	set := NewCapabilitySet()
	if s == "" {
		return set, nil
	}
	for _, part := range strings.Split(s, ",") {
		c := Capability(part)
		if !validCapability(c) {
			return nil, fmt.Errorf("invalid capability %q", part)
		}
		set.Add(c)
	}
	return set, nil
}

// capabilitiesFromStrings converts a configured list of capability names,
// validating each entry.
func capabilitiesFromStrings(caps []string) (CapabilitySet, error) {
	set := NewCapabilitySet()
	for _, s := range caps {
		c := Capability(s)
		if !validCapability(c) {
			return nil, fmt.Errorf("invalid capability %q", s)
		}
		set.Add(c)
	}
	return set, nil
}

// Add inserts c into the set.
func (s CapabilitySet) Add(c Capability) {
	s[c] = struct{}{}
}

// Contains reports whether c is in the set.
func (s CapabilitySet) Contains(c Capability) bool {
	_, ok := s[c]
	return ok
}

// Intersect returns the capabilities present in both sets. This is the
// minimal common capability set used when negotiating with a peer.
func (s CapabilitySet) Intersect(o CapabilitySet) CapabilitySet {
	out := NewCapabilitySet()
	for c := range s {
		if o.Contains(c) {
			out.Add(c)
		}
	}
	return out
}

// Difference returns the capabilities in s that are not in o.
func (s CapabilitySet) Difference(o CapabilitySet) CapabilitySet {
	out := NewCapabilitySet()
	for c := range s {
		if !o.Contains(c) {
			out.Add(c)
		}
	}
	return out
}

// Sorted returns the capabilities in sorted order.
func (s CapabilitySet) Sorted() []Capability {
	out := make([]Capability, 0, len(s))
	for c := range s {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// String returns the canonical comma-separated form of the set.
func (s CapabilitySet) String() string {
	parts := s.Sorted()
	strs := make([]string, len(parts))
	for i, c := range parts {
		strs[i] = string(c)
	}
	return strings.Join(strs, ",")
}

// negotiateCapabilities computes the minimal common capability set between
// client and server and verifies that every required capability survives the
// intersection. Required capabilities are never silently dropped: if any is
// missing from the common set, negotiation fails.
func negotiateCapabilities(client, server, required CapabilitySet) (CapabilitySet, error) {
	negotiated := client.Intersect(server)
	if missing := required.Difference(negotiated); len(missing) > 0 {
		return nil, fmt.Errorf("%w: peer does not support required capabilities: %s",
			ErrCapabilityNegotiation, missing.String())
	}
	return negotiated, nil
}

// maxInstanceIDLen bounds handshake instance identities.
const maxInstanceIDLen = 128

// instanceIDPattern restricts instance identities to a conservative alphabet
// so they can safely travel in environment variables and the handshake line.
var instanceIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// validInstanceID reports whether id is a well-formed instance identity.
func validInstanceID(id string) bool {
	return len(id) > 0 && len(id) <= maxInstanceIDLen && instanceIDPattern.MatchString(id)
}

// newInstanceID generates a random instance identity for one side of a
// handshake.
func newInstanceID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err == nil {
		return hex.EncodeToString(b[:])
	}
	// crypto/rand should never fail; fall back to something unique enough
	// for correlation purposes rather than failing the handshake.
	return fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
}

// clientHandshake is the parsed form of the handshake request a client sends
// to the server through the environment.
type clientHandshake struct {
	// capable is true if the client sent capability fields at all. A legacy
	// client sends none and is served with the default capability set.
	capable bool

	// capabilities is the client's declared capability set.
	capabilities CapabilitySet

	// required is the set of capabilities the client refuses to run without.
	required CapabilitySet

	// instanceID is the client's instance identity.
	instanceID string
}

// parseClientHandshake reads the client's handshake request from the
// environment. A request that carries capability fields is only valid if it
// also carries a well-formed instance identity; a partial request is
// incompatible and rejected.
func parseClientHandshake(getenv func(string) string) (*clientHandshake, error) {
	capsEnv := getenv(envClientCapabilities)
	if capsEnv == "" {
		// Legacy client: no capability fields. Handled with the default
		// capability set.
		return &clientHandshake{capabilities: DefaultCapabilities()}, nil
	}

	caps, err := ParseCapabilitySet(capsEnv)
	if err != nil {
		return nil, fmt.Errorf("invalid client capabilities: %w", err)
	}

	required, err := ParseCapabilitySet(getenv(envClientRequiredCapabilities))
	if err != nil {
		return nil, fmt.Errorf("invalid client required capabilities: %w", err)
	}

	id := getenv(envClientInstanceID)
	if !validInstanceID(id) {
		return nil, errors.New("capability-aware client handshake is missing a valid instance ID")
	}

	return &clientHandshake{
		capable:      true,
		capabilities: caps,
		required:     required,
		instanceID:   id,
	}, nil
}

// serverHandshake is the parsed form of the capability fields of the
// server's handshake response.
type serverHandshake struct {
	// capable is true if the server sent capability fields at all. A legacy
	// server sends none and is assumed to support the default capabilities.
	capable bool

	// capabilities is the server's declared capability set.
	capabilities CapabilitySet

	// instanceID is the server's instance identity.
	instanceID string
}

// parseServerHandshake extracts the capability fields from the server's
// handshake response parts. A capability-aware response must carry both the
// capability set and the instance identity; a partial response is
// incompatible and rejected.
func parseServerHandshake(parts []string) (*serverHandshake, error) {
	if len(parts) <= 7 {
		// Legacy response: no capability fields. Handled with the default
		// capability set.
		return &serverHandshake{capabilities: DefaultCapabilities()}, nil
	}

	if len(parts) < 9 || parts[7] == "" || parts[8] == "" {
		return nil, errors.New("incompatible plugin handshake: capability-aware response " +
			"must include both a capability set and an instance ID")
	}

	caps, err := ParseCapabilitySet(parts[7])
	if err != nil {
		return nil, fmt.Errorf("invalid server capabilities: %w", err)
	}

	if !validInstanceID(parts[8]) {
		return nil, fmt.Errorf("invalid server instance ID %q", parts[8])
	}

	return &serverHandshake{
		capable:      true,
		capabilities: caps,
		instanceID:   parts[8],
	}, nil
}
