// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"bytes"
	"io"
	"strconv"
	"strings"
	"sync"
)

const (
	sourceStdout = "stdout"
	sourceStderr = "stderr"

	// defaultOutputBufferSize bounds the number of bytes retained from each
	// plugin's combined stdout/stderr output.
	defaultOutputBufferSize = 64 * 1024

	// outputTruncationMarker is inserted between the retained head and tail.
	outputTruncationMarker = "... output truncated ..."
)

// OutputBuffer is a concurrency-safe byte buffer that retains its head and
// tail once maxBytes has been exceeded.
type OutputBuffer struct {
	mu sync.Mutex

	maxBytes int

	// buf is used until the buffer first exceeds maxBytes. Keeping one slice
	// for the common case avoids artificially splitting output that still
	// fits within the limit.
	buf []byte

	// head and tail are used after truncation. They never overlap.
	head []byte
	tail []byte

	truncated bool
	dropped   int64
}

// NewOutputBuffer creates an OutputBuffer capped at maxBytes. A non-positive
// maxBytes selects the default limit.
func NewOutputBuffer(maxBytes int) *OutputBuffer {
	if maxBytes <= 0 {
		maxBytes = defaultOutputBufferSize
	}
	return &OutputBuffer{maxBytes: maxBytes}
}

// newOutputBuffer keeps package-internal callers and tests concise. The
// variadic form also allows a zero-argument default in focused tests.
func newOutputBuffer(maxBytes ...int) *OutputBuffer {
	size := 0
	if len(maxBytes) > 0 {
		size = maxBytes[0]
	}
	return NewOutputBuffer(size)
}

// Write implements io.Writer. It always consumes all of p; output beyond the
// cap is discarded from the middle while preserving the head and tail.
func (b *OutputBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	n := len(p)
	if !b.truncated {
		b.buf = append(b.buf, p...)
		if len(b.buf) <= b.maxBytes {
			return n, nil
		}

		headSize := b.maxBytes / 2
		tailSize := b.maxBytes - headSize
		if headSize > len(b.buf) {
			headSize = len(b.buf)
		}
		if tailSize > len(b.buf)-headSize {
			tailSize = len(b.buf) - headSize
		}

		b.head = append([]byte(nil), b.buf[:headSize]...)
		b.tail = append([]byte(nil), b.buf[len(b.buf)-tailSize:]...)
		b.dropped = int64(len(b.buf) - headSize - tailSize)
		b.buf = nil
		b.truncated = true
		return n, nil
	}

	tailLimit := b.maxBytes - b.maxBytes/2
	b.tail = append(b.tail, p...)
	if len(b.tail) > tailLimit {
		excess := len(b.tail) - tailLimit
		b.dropped += int64(excess)
		remaining := b.tail[excess:]
		b.tail = append(make([]byte, 0, tailLimit), remaining...)
	}

	return n, nil
}

// WriteString is the string form of Write.
func (b *OutputBuffer) WriteString(s string) (int, error) {
	return b.Write([]byte(s))
}

// Bytes returns the retained head/truncation-marker/tail representation.
func (b *OutputBuffer) Bytes() []byte {
	return []byte(b.String())
}

// Len returns the length of the string representation.
func (b *OutputBuffer) Len() int {
	return len(b.String())
}

// String returns the retained head, a truncation marker, and the retained
// tail. If no truncation occurred, it returns all output.
func (b *OutputBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.truncated {
		return string(b.buf)
	}

	var out strings.Builder
	out.Write(b.head)
	out.WriteByte('\n')
	out.WriteString(outputTruncationMarker)
	if b.dropped > 0 {
		out.WriteString(" (")
		out.WriteString(strconv.FormatInt(b.dropped, 10))
		out.WriteString(" bytes omitted)")
	}
	out.WriteByte('\n')
	out.Write(b.tail)
	return out.String()
}

// Tail returns the most recent retained output. Before truncation this is all
// output; afterwards it is only the retained tail.
func (b *OutputBuffer) Tail() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	var tail []byte
	if b.truncated {
		tail = b.tail
	} else {
		tail = b.buf
	}
	return append([]byte(nil), tail...)
}

// TailString is Tail as a string.
func (b *OutputBuffer) TailString() string {
	return string(b.Tail())
}

// TailLines returns up to n complete lines from the end of the retained tail.
// A non-positive n returns the complete retained tail, with a partial first
// line removed after truncation.
func (b *OutputBuffer) TailLines(n int) string {
	tail := b.TailString()

	b.mu.Lock()
	truncated := b.truncated
	b.mu.Unlock()

	if truncated {
		// Byte-based truncation can begin in the middle of a line. If there
		// is a later complete line, drop that fragment; otherwise retain the
		// fragment because it may be the only tail content available.
		firstNewline := strings.IndexByte(tail, '\n')
		lastNewline := strings.LastIndexByte(tail, '\n')
		if firstNewline >= 0 && firstNewline != lastNewline {
			tail = tail[firstNewline+1:]
		}
	}

	lines := strings.Split(tail, "\n")
	// A trailing newline produces a final empty element.
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if n > 0 && len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// Truncated reports whether output has exceeded the buffer cap.
func (b *OutputBuffer) Truncated() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.truncated
}

// Reset discards all buffered output.
func (b *OutputBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = nil
	b.head = nil
	b.tail = nil
	b.truncated = false
	b.dropped = 0
}

// prefixLineWriter frames writes as newline-terminated lines and adds a
// source prefix to each complete line before writing it to dst.
type prefixLineWriter struct {
	mu     sync.Mutex
	dst    io.Writer
	prefix string

	pending    []byte
	maxPending int
}

// newPrefixLineWriter creates a line writer. The source is rendered as
// "[source]". If source already includes brackets it is used verbatim.
func newPrefixLineWriter(dst io.Writer, source string) *prefixLineWriter {
	prefix := source
	if prefix != "" {
		if !strings.HasPrefix(prefix, "[") {
			prefix = "[" + prefix + "]"
		}
		prefix += " "
	}
	return &prefixLineWriter{
		dst:        dst,
		prefix:     prefix,
		maxPending: defaultOutputBufferSize,
	}
}

// Write buffers partial lines and forwards complete lines with their prefix.
func (w *prefixLineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	consumed := len(p)
	for len(p) > 0 {
		i := bytes.IndexByte(p, '\n')
		if i < 0 {
			w.appendPending(p)
			break
		}

		w.pending = append(w.pending, p[:i]...)
		line := w.pending
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		if err := w.writeLine(line); err != nil {
			w.pending = nil
			return consumed, err
		}
		w.pending = nil
		p = p[i+1:]
	}

	return consumed, nil
}

// Flush forwards a final line that was not terminated by a newline. It is
// safe to call multiple times; only the first call flushes the pending line.
func (w *prefixLineWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.pending) == 0 {
		return nil
	}
	line := w.pending
	w.pending = nil
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	return w.writeLine(line)
}

func (w *prefixLineWriter) appendPending(p []byte) {
	if w.maxPending <= 0 {
		w.pending = append(w.pending, p...)
		return
	}
	if len(w.pending)+len(p) <= w.maxPending {
		w.pending = append(w.pending, p...)
		return
	}

	const marker = "... [line truncated] "
	combined := append(append([]byte(nil), w.pending...), p...)
	keep := w.maxPending - len(marker)
	if keep < 0 {
		keep = w.maxPending
	}
	if len(combined) > keep {
		combined = combined[len(combined)-keep:]
	}
	w.pending = append([]byte(marker), combined...)
}

func (w *prefixLineWriter) writeLine(line []byte) error {
	if w.dst == nil {
		return nil
	}

	var formatted []byte
	formatted = append(formatted, w.prefix...)
	formatted = append(formatted, line...)
	formatted = append(formatted, '\n')
	_, err := w.dst.Write(formatted)
	return err
}
