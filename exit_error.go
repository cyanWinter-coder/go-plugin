// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

// Error types describing how a plugin process ended. They are wrapped around
// an *ExitError so callers can use errors.Is to distinguish the cases while
// still inspecting the exit code or signal.
var (
	// ErrPluginCrashed is returned (or wrapped) when the plugin process
	// terminated on its own with a non-zero exit code or because of a signal.
	ErrPluginCrashed = errors.New("plugin crashed")

	// ErrPluginStartTimeout is returned (or wrapped) when the plugin did not
	// complete its handshake within ClientConfig.StartTimeout.
	ErrPluginStartTimeout = errors.New("timeout while waiting for plugin to start")

	// ErrPluginExited is returned (or wrapped) when the plugin process exited
	// before the handshake completed.
	ErrPluginExited = errors.New("plugin exited before we could connect")

	// ErrClientClosed is returned by Start (and Client) when called on a client
	// that has already been closed with Kill. A closed client cannot be
	// restarted; create a new client instead.
	ErrClientClosed = errors.New("plugin client has already been closed")
)

// defaultPluginStderrTailSize is how many bytes of the plugin's stderr are
// retained so that failure errors can include the last output it produced.
const defaultPluginStderrTailSize = 4 * 1024

// ExitError describes why a plugin process exited. It distinguishes a clean
// exit from a non-zero exit code and from termination by a signal, and carries
// the tail of the plugin's stderr output for diagnostics.
//
// ExitError values are produced by the client and exposed via Client.ExitError;
// they are also wrapped into the errors returned from Start when the plugin
// fails before completing its handshake.
type ExitError struct {
	// ExitCode is the process exit code. It is set when the process exited on
	// its own (including with code 0). When the process was terminated by a
	// signal it is -1.
	ExitCode int

	// Signal is the signal that terminated the process, or nil if the process
	// exited normally with an exit code. It is always nil on Windows.
	Signal Signal

	// Stderr holds the last portion of the plugin's stderr output. It may be
	// empty if the plugin produced no output.
	Stderr string
}

// Signal describes an OS signal that terminated a plugin process. The concrete
// value is platform specific; on Unix it wraps a syscall.Signal.
type Signal interface {
	// String returns a human-readable description of the signal.
	String() string

	// Signal returns the numeric signal number.
	Signal() int
}

// newExitError converts the error returned by waiting on a plugin process into
// an ExitError, attaching the retained stderr output. A nil wait error
// describes a clean exit (exit code 0).
func newExitError(waitErr error, stderr *stderrTail) *ExitError {
	e := &ExitError{
		ExitCode: 0,
		Stderr:   tailString(stderr),
	}

	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		e.ExitCode = exitErr.ExitCode()
		e.Signal = processSignal(exitErr)
	} else if waitErr != nil {
		// The runner reported an error that is not an *exec.ExitError (for
		// example an external runner implementation). Surface it as a crash
		// with an unknown exit code rather than misreporting success.
		e.ExitCode = -1
		e.Stderr = strings.TrimSpace(e.Stderr)
		if e.Stderr != "" {
			e.Stderr = e.Stderr + "\n" + waitErr.Error()
		} else {
			e.Stderr = waitErr.Error()
		}
		return e
	}

	return e
}

func tailString(stderr *stderrTail) string {
	if stderr == nil {
		return ""
	}
	return strings.TrimRight(string(stderr.Bytes()), "\r\n")
}

// Crashed reports whether the process ended abnormally: a non-zero exit code
// or termination by a signal.
func (e *ExitError) Crashed() bool {
	return e.Signal != nil || e.ExitCode != 0
}

func (e *ExitError) Error() string {
	var b strings.Builder

	switch {
	case e.Signal != nil:
		fmt.Fprintf(&b, "plugin was terminated by signal %s", e.Signal.String())
	case e.ExitedClean():
		b.WriteString("plugin exited cleanly")
	default:
		fmt.Fprintf(&b, "plugin exited with error code %d", e.ExitCode)
	}

	if strings.TrimSpace(e.Stderr) != "" {
		b.WriteString(":\n")
		b.WriteString(e.Stderr)
	}

	return b.String()
}

// ExitedClean reports whether the process exited with status 0 and no signal.
func (e *ExitError) ExitedClean() bool {
	return e.Signal == nil && e.ExitCode == 0
}
