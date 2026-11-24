// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

//go:build !windows

package plugin

import (
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestClientLifecycle_CrashBySignal(t *testing.T) {
	c := NewClient(&ClientConfig{
		Cmd:             helperProcess("crash-signal"),
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
		t.Fatal("ExitError should be populated after signal crash")
	}
	if exitErr.Signal == nil {
		t.Fatalf("expected a terminating signal, got exit code %d", exitErr.ExitCode)
	}
	if exitErr.Signal.Signal() != int(syscall.SIGTERM) {
		t.Fatalf("signal = %d, want SIGTERM(%d)", exitErr.Signal.Signal(), syscall.SIGTERM)
	}
	if !exitErr.Crashed() {
		t.Fatal("signal termination should be reported as a crash")
	}
	if !strings.Contains(exitErr.Stderr, "going down by signal") {
		t.Fatalf("stderr tail missing, got %q", exitErr.Stderr)
	}
	if !strings.Contains(exitErr.Error(), "terminated by signal") {
		t.Fatalf("error message should mention signal termination, got %q", exitErr.Error())
	}
}

func TestClientLifecycle_ForceKillAfterGracefulTimeout(t *testing.T) {
	c := NewClient(&ClientConfig{
		Cmd:                 helperProcess("ignore-sigterm"),
		HandshakeConfig:     testHandshake,
		Plugins:             testPluginMap,
		ExitGracefulTimeout: 100 * time.Millisecond,
	})
	defer c.Kill()

	if _, err := c.Start(); err != nil {
		t.Fatalf("Start: %s", err)
	}

	// Kill must block until the unresponsive process is forcefully reaped.
	done := make(chan struct{})
	go func() {
		c.Kill()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Kill did not return after forcefully killing an unresponsive plugin")
	}

	if !c.killed() {
		t.Fatal("process should have been forcefully killed")
	}
	if !c.Exited() {
		t.Fatal("forcefully killed process should be marked exited")
	}
}

func TestClientLifecycle_ConcurrentKill(t *testing.T) {
	c := NewClient(&ClientConfig{
		Cmd:                 helperProcess("ignore-sigterm"),
		HandshakeConfig:     testHandshake,
		Plugins:             testPluginMap,
		ExitGracefulTimeout: 50 * time.Millisecond,
	})

	if _, err := c.Start(); err != nil {
		t.Fatalf("Start: %s", err)
	}

	// Many concurrent Kill calls must serialize through killMu and all return
	// without panicking or leaving the process behind.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Kill()
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("concurrent Kill calls did not all return")
	}

	if !c.Exited() {
		t.Fatal("plugin should be fully reaped after concurrent Kill")
	}
}
