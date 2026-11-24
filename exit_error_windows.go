// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

//go:build windows

package plugin

import "os/exec"

// processSignal always returns nil on Windows, where processes are not
// terminated by Unix-style signals.
func processSignal(_ *exec.ExitError) Signal {
	return nil
}
