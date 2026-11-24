// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import "sync"

// stderrTail captures the last portion of a plugin's stderr output. It is safe
// for concurrent use: the goroutine draining stderr writes to it while the
// client may read a snapshot when reporting why a plugin failed to start or
// exited unexpectedly.
type stderrTail struct {
	mu sync.Mutex

	// buf holds the most recent bytes in a circular buffer. The valid data is
	// the slice buf[head:] followed by buf[:head], and its length is always
	// min(total written, cap(buf)).
	buf  []byte
	head int
	full bool
}

// newStderrTail allocates a ring buffer retaining up to size bytes of the
// most recent stderr output.
func newStderrTail(size int) *stderrTail {
	return &stderrTail{buf: make([]byte, 0, size)}
}

// Write appends p to the ring buffer, overwriting the oldest bytes once the
// buffer reaches capacity. It always succeeds and never returns an error.
func (t *stderrTail) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	capacity := cap(t.buf)
	if capacity == 0 {
		return len(p), nil
	}

	// Only the last capacity bytes can ever be retained.
	if len(p) > capacity {
		p = p[len(p)-capacity:]
	}

	for len(p) > 0 {
		// Grow into the spare capacity first; once full, wrap around.
		if !t.full && len(t.buf) < capacity {
			room := capacity - len(t.buf)
			n := min(room, len(p))
			t.buf = append(t.buf, p[:n]...)
			p = p[n:]
			continue
		}

		t.full = true
		room := capacity - t.head
		n := min(room, len(p))
		copy(t.buf[t.head:t.head+n], p[:n])
		t.head = (t.head + n) % capacity
		p = p[n:]
	}

	return len(p), nil
}

// Bytes returns a copy of the retained stderr output, oldest bytes first.
func (t *stderrTail) Bytes() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.full {
		out := make([]byte, len(t.buf))
		copy(out, t.buf)
		return out
	}

	out := make([]byte, 0, len(t.buf))
	out = append(out, t.buf[t.head:]...)
	out = append(out, t.buf[:t.head]...)
	return out
}

// String returns the retained stderr output as a string, oldest bytes first.
func (t *stderrTail) String() string {
	return string(t.Bytes())
}
