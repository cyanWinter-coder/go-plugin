// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

//go:build windows

package plugin

// terminateSelfBySignal has no Unix signals to send on Windows. The caller
// falls back to exiting with a non-zero status, which exercises the exit-code
// path instead of the signal path.
func terminateSelfBySignal() {}

// ignoreTerminationSignals has no Unix signals to ignore on Windows.
func ignoreTerminationSignals() {}
