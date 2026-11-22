// Copyright IBM Corp. 2016, 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestParseCapabilitySet(t *testing.T) {
	t.Run("empty string yields empty set", func(t *testing.T) {
		s, err := ParseCapabilitySet("")
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if len(s) != 0 {
			t.Fatalf("bad: %v", s)
		}
	})

	t.Run("round trip is canonical", func(t *testing.T) {
		s, err := ParseCapabilitySet("stdio-sync,netrpc,mtls")
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if got := s.String(); got != "mtls,netrpc,stdio-sync" {
			t.Fatalf("bad: %s", got)
		}
	})

	t.Run("invalid entries are rejected", func(t *testing.T) {
		for _, in := range []string{
			",",
			"mtls,,grpc",
			"MTLS",
			"mtls|grpc",
			"mt ls",
			"-mtls",
		} {
			if _, err := ParseCapabilitySet(in); err == nil {
				t.Fatalf("expected error for %q", in)
			}
		}
	})
}

func TestCapabilitySet_ops(t *testing.T) {
	a := NewCapabilitySet("netrpc", "grpc", "mtls")
	b := NewCapabilitySet("grpc", "mtls", "stdio-sync")

	if got := a.Intersect(b).String(); got != "grpc,mtls" {
		t.Fatalf("bad intersect: %s", got)
	}
	if got := a.Difference(b).String(); got != "netrpc" {
		t.Fatalf("bad difference: %s", got)
	}
	if !a.Contains("netrpc") || a.Contains("stdio-sync") {
		t.Fatalf("bad contains: %v", a)
	}
}

func TestNegotiateCapabilities(t *testing.T) {
	client := NewCapabilitySet("netrpc", "mtls", "stdio-sync")
	server := NewCapabilitySet("grpc", "mtls", "stdio-sync")

	t.Run("negotiated set is the minimal common set", func(t *testing.T) {
		negotiated, err := negotiateCapabilities(client, server, nil)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if got := negotiated.String(); got != "mtls,stdio-sync" {
			t.Fatalf("bad: %s", got)
		}
	})

	t.Run("satisfied required capabilities survive", func(t *testing.T) {
		_, err := negotiateCapabilities(client, server, NewCapabilitySet("mtls"))
		if err != nil {
			t.Fatalf("err: %s", err)
		}
	})

	t.Run("required capabilities are never silently dropped", func(t *testing.T) {
		_, err := negotiateCapabilities(client, server, NewCapabilitySet("mtls", "netrpc"))
		if err == nil {
			t.Fatal("expected error")
		}
		if !errors.Is(err, ErrCapabilityNegotiation) {
			t.Fatalf("bad: %s", err)
		}
	})
}

func TestParseClientHandshake(t *testing.T) {
	getter := func(env map[string]string) func(string) string {
		return func(k string) string { return env[k] }
	}

	t.Run("legacy client without capability fields gets defaults", func(t *testing.T) {
		hs, err := parseClientHandshake(getter(nil))
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if hs.capable {
			t.Fatal("should not be capability-aware")
		}
		if got := hs.capabilities.String(); got != DefaultCapabilities().String() {
			t.Fatalf("bad: %s", got)
		}
	})

	t.Run("capability-aware client", func(t *testing.T) {
		hs, err := parseClientHandshake(getter(map[string]string{
			envClientCapabilities:         "mtls,netrpc",
			envClientRequiredCapabilities: "mtls",
			envClientInstanceID:           "client-1",
		}))
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if !hs.capable {
			t.Fatal("should be capability-aware")
		}
		if got := hs.capabilities.String(); got != "mtls,netrpc" {
			t.Fatalf("bad: %s", got)
		}
		if got := hs.required.String(); got != "mtls" {
			t.Fatalf("bad: %s", got)
		}
		if hs.instanceID != "client-1" {
			t.Fatalf("bad: %s", hs.instanceID)
		}
	})

	t.Run("missing instance ID is incompatible", func(t *testing.T) {
		_, err := parseClientHandshake(getter(map[string]string{
			envClientCapabilities: "mtls",
		}))
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("malformed capabilities are incompatible", func(t *testing.T) {
		_, err := parseClientHandshake(getter(map[string]string{
			envClientCapabilities: "mt ls",
			envClientInstanceID:   "client-1",
		}))
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestParseServerHandshake(t *testing.T) {
	t.Run("legacy response without capability fields gets defaults", func(t *testing.T) {
		for _, parts := range [][]string{
			{"1", "1", "tcp", ":1234"},
			{"1", "1", "tcp", ":1234", "netrpc", "", "true"},
		} {
			hs, err := parseServerHandshake(parts)
			if err != nil {
				t.Fatalf("err: %s", err)
			}
			if hs.capable {
				t.Fatal("should not be capability-aware")
			}
			if got := hs.capabilities.String(); got != DefaultCapabilities().String() {
				t.Fatalf("bad: %s", got)
			}
		}
	})

	t.Run("capability-aware response", func(t *testing.T) {
		hs, err := parseServerHandshake([]string{
			"1", "1", "tcp", ":1234", "netrpc", "", "false", "mtls,netrpc", "server-1",
		})
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if !hs.capable {
			t.Fatal("should be capability-aware")
		}
		if got := hs.capabilities.String(); got != "mtls,netrpc" {
			t.Fatalf("bad: %s", got)
		}
		if hs.instanceID != "server-1" {
			t.Fatalf("bad: %s", hs.instanceID)
		}
	})

	t.Run("partial capability response is incompatible", func(t *testing.T) {
		for _, parts := range [][]string{
			// capability set without instance ID
			{"1", "1", "tcp", ":1234", "netrpc", "", "false", "mtls"},
			// empty capability set
			{"1", "1", "tcp", ":1234", "netrpc", "", "false", "", "server-1"},
			// empty instance ID
			{"1", "1", "tcp", ":1234", "netrpc", "", "false", "mtls", ""},
			// invalid instance ID
			{"1", "1", "tcp", ":1234", "netrpc", "", "false", "mtls", "bad|id"},
		} {
			if _, err := parseServerHandshake(parts); err == nil {
				t.Fatalf("expected error for %v", parts)
			}
		}
	})
}

func TestClientConfig_capabilitySet(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		caps, err := (&ClientConfig{}).capabilitySet()
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if got := caps.String(); got != DefaultCapabilities().String() {
			t.Fatalf("bad: %s", got)
		}
	})

	t.Run("implied capabilities are added", func(t *testing.T) {
		caps, err := (&ClientConfig{
			HandshakeConfig:     HandshakeConfig{Capabilities: []string{"netrpc"}},
			AutoMTLS:            true,
			GRPCBrokerMultiplex: true,
		}).capabilitySet()
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if got := caps.String(); got != "grpc-broker-multiplex,mtls,netrpc" {
			t.Fatalf("bad: %s", got)
		}
	})

	t.Run("AutoMTLS implies required mTLS", func(t *testing.T) {
		required, err := (&ClientConfig{AutoMTLS: true}).requiredCapabilitySet()
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if !required.Contains(CapabilityMTLS) {
			t.Fatalf("bad: %v", required)
		}
	})

	t.Run("invalid configured capability", func(t *testing.T) {
		cfg := &ClientConfig{HandshakeConfig: HandshakeConfig{Capabilities: []string{"bad cap"}}}
		if _, err := cfg.capabilitySet(); err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestServerCapabilities(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		caps, err := serverCapabilities(&ServeConfig{})
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if got := caps.String(); got != DefaultCapabilities().String() {
			t.Fatalf("bad: %s", got)
		}
	})

	t.Run("implied capabilities are added", func(t *testing.T) {
		caps, err := serverCapabilities(&ServeConfig{
			HandshakeConfig: HandshakeConfig{Capabilities: []string{"netrpc"}},
			GRPCServer:      DefaultGRPCServer,
		})
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if got := caps.String(); got != "grpc,netrpc" {
			t.Fatalf("bad: %s", got)
		}
	})
}

// testCapabilityClientHandshake is a client-side handshake config that
// declares (and requires) the test-only capability also declared by
// testCapabilityHandshake on the server side.
var testCapabilityClientHandshake = HandshakeConfig{
	ProtocolVersion:      1,
	MagicCookieKey:       "TEST_MAGIC_COOKIE",
	MagicCookieValue:     "test",
	Capabilities:         []string{"grpc", "mtls", "netrpc", "stdio-sync", "test-cap"},
	RequiredCapabilities: []string{"test-cap"},
}

// TestClient_capabilityNegotiation verifies a full capability-aware
// handshake: both sides carry capabilities and instance identity, and the
// negotiated set is the intersection of both declarations.
func TestClient_capabilityNegotiation(t *testing.T) {
	process := helperProcess("test-interface-caps")
	c := NewClient(&ClientConfig{
		Cmd:             process,
		HandshakeConfig: testCapabilityClientHandshake,
		Plugins:         testPluginMap,
	})
	defer c.Kill()

	if _, err := c.Start(); err != nil {
		t.Fatalf("err: %s", err)
	}

	// The negotiated set is the intersection of the client defaults and the
	// server's declared set.
	if got, want := c.NegotiatedCapabilities(), []string{"netrpc", "stdio-sync", "test-cap"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("bad: %v, want %v", got, want)
	}

	// A capability-aware server identifies itself.
	if id := c.PeerInstanceID(); id == "" {
		t.Fatal("expected a server instance ID")
	}

	// The plugin is fully functional after negotiation.
	client, err := c.Client()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	raw, err := client.Dispense("test")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	impl, ok := raw.(testInterface)
	if !ok {
		t.Fatalf("bad: %#v", raw)
	}
	if got := impl.Double(21); got != 42 {
		t.Fatalf("bad: %d", got)
	}
}

// TestClient_capabilityNegotiation_missingRequired verifies that requiring a
// capability the server does not declare fails the handshake up front, and
// that the failure is a single terminal state: the subprocess is terminated
// and a subsequent Start returns the same error instead of resurrecting the
// instance.
func TestClient_capabilityNegotiation_missingRequired(t *testing.T) {
	process := helperProcess("test-interface-caps")
	c := NewClient(&ClientConfig{
		Cmd: process,
		HandshakeConfig: HandshakeConfig{
			ProtocolVersion:      1,
			MagicCookieKey:       "TEST_MAGIC_COOKIE",
			MagicCookieValue:     "test",
			Capabilities:         []string{"netrpc", "missing-cap"},
			RequiredCapabilities: []string{"missing-cap"},
		},
		Plugins: testPluginMap,
	})
	defer c.Kill()

	_, err := c.Start()
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrCapabilityNegotiation) {
		t.Fatalf("bad: %s", err)
	}

	// The subprocess must be terminated before the error is returned to the
	// caller; wait for the exit to be observed.
	deadline := time.Now().Add(10 * time.Second)
	for !c.Exited() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !c.Exited() {
		t.Fatal("plugin process was not terminated after negotiation failure")
	}

	// A late response must not resurrect the instance: Start returns the
	// same terminal error.
	if _, err2 := c.Start(); err2 == nil || err2.Error() != err.Error() {
		t.Fatalf("expected terminal error %q, got %v", err, err2)
	}

	// No RPC client can be created for the failed instance.
	if _, err3 := c.Client(); err3 == nil {
		t.Fatal("expected error creating a client for a failed negotiation")
	}
}

// TestClient_capabilityNegotiation_legacyServer verifies that a legacy
// plugin whose handshake carries no capability fields is handled with the
// default capability set.
func TestClient_capabilityNegotiation_legacyServer(t *testing.T) {
	process := helperProcess("mock")
	c := NewClient(&ClientConfig{
		Cmd:             process,
		HandshakeConfig: testHandshake,
		Plugins:         testPluginMap,
	})
	defer c.Kill()

	if _, err := c.Start(); err != nil {
		t.Fatalf("err: %s", err)
	}

	var want []string
	for _, cap := range DefaultCapabilities().Sorted() {
		want = append(want, string(cap))
	}
	if got := c.NegotiatedCapabilities(); !reflect.DeepEqual(got, want) {
		t.Fatalf("bad: %v, want %v", got, want)
	}

	// Legacy servers have no instance identity.
	if id := c.PeerInstanceID(); id != "" {
		t.Fatalf("bad: %s", id)
	}
}

// TestClient_capabilityNegotiation_legacyRequiredMissing verifies that a
// required capability absent from the default set fails negotiation with a
// legacy plugin.
func TestClient_capabilityNegotiation_legacyRequiredMissing(t *testing.T) {
	process := helperProcess("mock")
	c := NewClient(&ClientConfig{
		Cmd: process,
		HandshakeConfig: HandshakeConfig{
			ProtocolVersion:      1,
			MagicCookieKey:       "TEST_MAGIC_COOKIE",
			MagicCookieValue:     "test",
			Capabilities:         []string{"netrpc", "test-cap"},
			RequiredCapabilities: []string{"test-cap"},
		},
		Plugins: testPluginMap,
	})
	defer c.Kill()

	_, err := c.Start()
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrCapabilityNegotiation) {
		t.Fatalf("bad: %s", err)
	}
}

// TestClient_capabilityNegotiation_versioned verifies that version
// negotiation composes with capability negotiation: downgrading the protocol
// version still negotiates the minimal common capability set and never
// silently drops required capabilities.
func TestClient_capabilityNegotiation_versioned(t *testing.T) {
	process := helperProcess("test-interface-caps")
	c := NewClient(&ClientConfig{
		Cmd: process,
		HandshakeConfig: HandshakeConfig{
			MagicCookieKey:       "TEST_MAGIC_COOKIE",
			MagicCookieValue:     "test",
			Capabilities:         []string{"grpc", "mtls", "netrpc", "stdio-sync", "test-cap"},
			RequiredCapabilities: []string{"test-cap"},
		},
		VersionedPlugins: map[int]PluginSet{
			1: testPluginMap,
		},
	})
	defer c.Kill()

	if _, err := c.Start(); err != nil {
		t.Fatalf("err: %s", err)
	}

	if got := c.NegotiatedVersion(); got != 1 {
		t.Fatalf("bad: %d", got)
	}
	if got, want := c.NegotiatedCapabilities(), []string{"netrpc", "stdio-sync", "test-cap"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("bad: %v, want %v", got, want)
	}
}

// TestClient_startTimeout_terminalState verifies that a timeout during the
// handshake produces exactly one terminal state: the subprocess is
// terminated and a subsequent Start returns the same error rather than
// waiting for a late response.
func TestClient_startTimeout_terminalState(t *testing.T) {
	process := helperProcess("start-timeout")
	c := NewClient(&ClientConfig{
		Cmd:             process,
		HandshakeConfig: testHandshake,
		Plugins:         testPluginMap,
		StartTimeout:    50 * time.Millisecond,
	})
	defer c.Kill()

	_, err := c.Start()
	if err == nil {
		t.Fatal("expected error")
	}

	deadline := time.Now().Add(10 * time.Second)
	for !c.Exited() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !c.Exited() {
		t.Fatal("plugin process was not terminated after handshake timeout")
	}

	start := time.Now()
	if _, err2 := c.Start(); err2 == nil || err2.Error() != err.Error() {
		t.Fatalf("expected terminal error %q, got %v", err, err2)
	}
	if elapsed := time.Since(start); elapsed > 40*time.Millisecond {
		t.Fatalf("second Start re-attempted the handshake, took %s", elapsed)
	}
}
