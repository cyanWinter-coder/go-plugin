// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

//go:build !windows

package plugin

import (
	"os/exec"
	"syscall"
)

// unixSignal wraps a syscall.Signal so that it satisfies the Signal interface.
type unixSignal struct {
	sig syscall.Signal
}

func (s unixSignal) String() string {
	return s.sig.String()
}

func (s unixSignal) Signal() int {
	return int(s.sig)
}

// processSignal returns the signal that terminated the process, or nil if the
// process exited normally with an exit code.
func processSignal(e *exec.ExitError) Signal {
	status, ok := e.Sys().(syscall.WaitStatus)
	if !ok {
		return nil
	}

	if status.Signaled() {
		return unixSignal{sig: status.Signal()}
	}

	return nil
}
