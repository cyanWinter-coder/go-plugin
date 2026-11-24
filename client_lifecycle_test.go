// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// waitForExited polls until the client reports the plugin exited or the test
// deadline elapses.
func waitForExited(t *testing.T, c *Client) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c.Exited() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("client never reported the plugin as exited")
}

func TestClientLifecycle_HandshakeStates(t *testing.T) {
	c := NewClient(&ClientConfig{
		Cmd:             helperProcess("mock"),
		HandshakeConfig: testHandshake,
		Plugins:         testPluginMap,
	})
	defer c.Kill()

	if got := c.HandshakeState(); got != (handshakeNotStarted).String() {
		t.Fatalf("initial state = %q, want %q", got, handshakeNotStarted)
	}

	if _, err := c.Start(); err != nil {
		t.Fatalf("Start: %s", err)
	}
	if got := c.HandshakeState(); got != (handshakeReady).String() {
		t.Fatalf("after start state = %q, want %q", got, handshakeReady)
	}

	// A second Start is a no-op that returns the same address.
	addr2, err := c.Start()
	if err != nil {
		t.Fatalf("second Start: %s", err)
	}
	if addr2 == nil {
		t.Fatal("second Start returned nil address")
	}
}

func TestClientLifecycle_StartTimeoutKillsAndReaps(t *testing.T) {
	c := NewClient(&ClientConfig{
		Cmd:             helperProcess("start-timeout"),
		StartTimeout:    50 * time.Millisecond,
		HandshakeConfig: testHandshake,
		Plugins:         testPluginMap,
	})

	_, err := c.Start()
	if !errors.Is(err, ErrPluginStartTimeout) {
		t.Fatalf("err = %v, want ErrPluginStartTimeout", err)
	}
	if got := c.HandshakeState(); got != (handshakeFailed).String() {
		t.Fatalf("state = %q, want %q", got, handshakeFailed)
	}
	if !c.killed() {
		t.Fatal("the timed-out plugin should have been forcefully killed")
	}

	// Start must return synchronously with the process already reaped, so no
	// zombie remains and the client knows it has exited.
	if !c.Exited() {
		t.Fatal("timed-out plugin should be marked as exited after Start returns")
	}

	// The failed client cannot be restarted.
	if _, err := c.Start(); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("restart err = %v, want ErrClientClosed", err)
	}
}

func TestClientLifecycle_CrashExitCodeCarriesStderr(t *testing.T) {
	c := NewClient(&ClientConfig{
		Cmd:             helperProcess("crash-exit-code"),
		HandshakeConfig: testHandshake,
		Plugins:         testPluginMap,
	})
	defer c.Kill()

	if _, err := c.Start(); err != nil {
		t.Fatalf("Start: %s", err)
	}

	waitForExited(t, c)
	exitErr := c.ExitError()
	if exitErr == nil {
		t.Fatal("ExitError should be populated after crash")
	}
	if !exitErr.Crashed() {
		t.Fatal("non-zero exit should be reported as a crash")
	}
	if exitErr.ExitCode != 23 {
		t.Fatalf("exit code = %d, want 23", exitErr.ExitCode)
	}
	if exitErr.Signal != nil {
		t.Fatalf("signal = %v, want nil for an exit-code crash", exitErr.Signal)
	}
	if !strings.Contains(exitErr.Stderr, "boom: deliberate failure for testing") {
		t.Fatalf("stderr tail missing, got %q", exitErr.Stderr)
	}
	if !strings.Contains(exitErr.Error(), "error code 23") {
		t.Fatalf("error message should mention exit code, got %q", exitErr.Error())
	}
}

func TestClientLifecycle_CrashBeforeHandshake(t *testing.T) {
	c := NewClient(&ClientConfig{
		Cmd:             helperProcess("crash-before-handshake"),
		StartTimeout:    5 * time.Second,
		HandshakeConfig: testHandshake,
		Plugins:         testPluginMap,
	})
	defer c.Kill()

	_, err := c.Start()
	if !errors.Is(err, ErrPluginExited) {
		t.Fatalf("err = %v, want ErrPluginExited", err)
	}

	// The wrapped error must explain the failure and carry the stderr tail.
	if !strings.Contains(err.Error(), "startup failure detail line") {
		t.Fatalf("start error should include plugin stderr, got %q", err.Error())
	}

	exitErr := c.ExitError()
	if exitErr == nil {
		t.Fatal("ExitError should be recorded even when the handshake never completed")
	}
	if exitErr.ExitCode != 42 {
		t.Fatalf("exit code = %d, want 42", exitErr.ExitCode)
	}
}

func TestClientLifecycle_KillIdempotentAndClosesClient(t *testing.T) {
	c := NewClient(&ClientConfig{
		Cmd:             helperProcess("mock"),
		HandshakeConfig: testHandshake,
		Plugins:         testPluginMap,
	})

	if _, err := c.Start(); err != nil {
		t.Fatalf("Start: %s", err)
	}

	// Repeated Kill calls must be safe no-ops.
	c.Kill()
	c.Kill()
	c.Kill()

	waitForExited(t, c)

	if _, err := c.Start(); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("Start after Kill err = %v, want ErrClientClosed", err)
	}
	if _, err := c.Client(); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("Client after Kill err = %v, want ErrClientClosed", err)
	}
}

func TestClientLifecycle_KillNeverStartedIsNoop(t *testing.T) {
	c := NewClient(&ClientConfig{
		Cmd:             helperProcess("mock"),
		HandshakeConfig: testHandshake,
		Plugins:         testPluginMap,
	})

	// Killing before Start must not panic or block.
	c.Kill()
	c.Kill()

	if _, err := c.Start(); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("Start after pre-start Kill err = %v, want ErrClientClosed", err)
	}
}

func TestStderrTail_RetainsMostRecent(t *testing.T) {
	tail := newStderrTail(8)

	tail.Write([]byte("abc"))
	if got := tail.String(); got != "abc" {
		t.Fatalf("got %q, want %q", got, "abc")
	}

	// Writing past capacity keeps only the last 8 bytes, across Write calls.
	tail.Write([]byte("defghijkl"))
	got := tail.String()
	if len(got) > 8 {
		t.Fatalf("tail grew beyond capacity: %q", got)
	}
	if want := "defghijkl"[len("defghijkl")-8:]; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	// Concurrent writers must not race or corrupt the ring (run with -race).
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			tail.Write([]byte("xyz123\n"))
		}
	}()
	for i := 0; i < 200; i++ {
		_ = tail.Bytes()
	}
	<-done
}
