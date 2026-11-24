// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

//go:build !windows

package plugin

import (
	"os"
	"os/signal"
	"syscall"
)

// terminateSelfBySignal sends SIGTERM to the current process so that the client
// observes a signal-terminated plugin rather than an exit code.
func terminateSelfBySignal() {
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
	// Block until the signal is delivered; SIGTERM's default action terminates
	// the process, so this sleep only runs if the signal was handled.
	select {}
}

// ignoreTerminationSignals makes the helper process ignore SIGTERM (and
// SIGINT) so that graceful shutdown cannot complete and the host is forced to
// kill the process.
func ignoreTerminationSignals() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		for range sigs {
		}
	}()
}
