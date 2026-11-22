// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

//go:build windows

package plugin

import (
	"context"
	"errors"
	"os"
	"syscall"
	"time"
	"unsafe"
)

const (
	lockfileExclusiveLock                 = 0x00000002
	lockfileFailImmediately               = 0x00000001
	errLockViolation        syscall.Errno = 33
)

var (
	modKernel32      = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = modKernel32.NewProc("LockFileEx")
	procUnlockFileEx = modKernel32.NewProc("UnlockFileEx")
)

// fileLock serializes access to the discovery directory between goroutines of
// this process and any other host process on the machine.
type fileLock struct {
	file       *os.File
	overlapped syscall.Overlapped
}

// acquireLock takes an exclusive lock on the lock file at path. The lock is
// retried non-blockingly until it is acquired or ctx is done, so callers never
// block indefinitely on a lock held by a crashed process.
func acquireLock(ctx context.Context, path string) (*fileLock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}

	timer := time.NewTimer(lockRetryInterval)
	defer timer.Stop()

	for {
		l := &fileLock{file: f}
		err := lockFileEx(f, lockfileExclusiveLock|lockfileFailImmediately, &l.overlapped)
		if err == nil {
			return l, nil
		} else if !errors.Is(err, errLockViolation) {
			_ = f.Close()
			return nil, err
		}

		select {
		case <-ctx.Done():
			_ = f.Close()
			return nil, ctx.Err()
		case <-timer.C:
			timer.Reset(lockRetryInterval)
		}
	}
}

// lockFileEx locks the entire 4 GiB region of the file. UnlockFileEx requires
// the same region length and overlapped structure.
func lockFileEx(f *os.File, flags uint32, overlapped *syscall.Overlapped) error {
	r1, _, e1 := procLockFileEx.Call(
		f.Fd(),
		uintptr(flags),
		0,
		uintptr(1<<31),
		uintptr(0),
		uintptr(unsafe.Pointer(overlapped)),
	)
	if r1 == 0 {
		return e1
	}
	return nil
}

// release unlocks and closes the lock file. The file itself is intentionally
// retained on disk: removing it would race with another process that has the
// same path open and would break the locking protocol.
func (l *fileLock) release() error {
	if l == nil || l.file == nil {
		return nil
	}

	r1, _, e1 := procUnlockFileEx.Call(
		l.file.Fd(),
		0,
		uintptr(1<<31),
		uintptr(0),
		uintptr(unsafe.Pointer(&l.overlapped)),
	)
	var unlockErr error
	if r1 == 0 {
		unlockErr = e1
	}

	closeErr := l.file.Close()
	l.file = nil
	if unlockErr != nil {
		return unlockErr
	}
	return closeErr
}
