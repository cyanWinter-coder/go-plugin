// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

//go:build !windows

package plugin

import (
	"context"
	"errors"
	"os"
	"syscall"
	"time"
)

// fileLock serializes access to the discovery directory between goroutines of
// this process and any other host process on the machine.
type fileLock struct {
	file *os.File
}

// acquireLock takes an exclusive advisory lock on the lock file at path. The
// lock is retried non-blockingly until it is acquired or ctx is done, so
// callers never block indefinitely on a lock held by a crashed process.
func acquireLock(ctx context.Context, path string) (*fileLock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}

	timer := time.NewTimer(lockRetryInterval)
	defer timer.Stop()

	for {
		// LOCK_NB keeps the acquisition interruptible via ctx.
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
			return &fileLock{file: f}, nil
		} else if !errors.Is(err, syscall.EWOULDBLOCK) {
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

// release unlocks and closes the lock file. The file itself is intentionally
// retained on disk: removing it would race with another process that has the
// same path open and would break the locking protocol.
func (l *fileLock) release() error {
	if l == nil || l.file == nil {
		return nil
	}
	unlockErr := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	closeErr := l.file.Close()
	l.file = nil
	if unlockErr != nil {
		return unlockErr
	}
	return closeErr
}
