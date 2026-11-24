// Copyright IBM Corp. 2016, 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

// handshakeMagic is the fixed prefix written by a client to identify a
// connection that speaks the in-band RPC handshake. A server peeks the first
// bytes of every incoming connection: if this prefix is absent the connection
// is assumed to belong to a legacy client and the pre-handshake protocol path
// is used unchanged.
var handshakeMagic = []byte("GOPLGH")

const (
	// handshakeFrameHandshake is the only request frame type. It carries the
	// client's protocol version and plugin name.
	handshakeFrameHandshake byte = 1

	// handshakeResponseHeaderSize is the size of a response frame without the
	// optional error message: magic(6) + type(1) + status(1) + version(4) +
	// name length(2).
	handshakeResponseHeaderSize = 6 + 1 + 1 + 4 + 2

	// handshakeRequestHeaderSize is the size of a request frame without the
	// plugin name: magic(6) + type(1) + version(4) + name length(2).
	handshakeRequestHeaderSize = 6 + 1 + 4 + 2

	// handshakeMaxNameLength bounds the plugin name length accepted on the
	// wire so a corrupt length field cannot make the peer allocate an
	// unbounded buffer.
	handshakeMaxNameLength = 256

	// handshakeMaxMessageLength bounds the optional response error message.
	handshakeMaxMessageLength = 4096

	// defaultHandshakeTimeout is the maximum time a client waits for the
	// server's handshake response.
	defaultHandshakeTimeout = 30 * time.Second
)

// Handshake response status codes.
const (
	handshakeStatusOK               byte = 0
	handshakeStatusProtocolMismatch byte = 1
	handshakeStatusNameMismatch     byte = 2
	handshakeStatusInvalid          byte = 3
)

// Handshake failure modes. These sentinels are wrapped (never returned
// bare), so callers should inspect them with errors.Is rather than by direct
// comparison.
var (
	// ErrHandshakeTimeout is returned when the peer does not complete the
	// handshake within the configured timeout. This is distinct from a
	// connection failure: the transport was established but the peer failed to
	// answer in time.
	ErrHandshakeTimeout = errors.New("handshake timed out waiting for plugin response")

	// ErrHandshakeInvalidResponse is returned when the peer sends bytes that
	// cannot be parsed as a handshake frame. This is the typical error when an
	// older plugin binary that does not implement the handshake is reached.
	ErrHandshakeInvalidResponse = errors.New("invalid handshake response from plugin")

	// ErrProtocolMismatch is returned when the plugin's protocol version does
	// not match any version supported by the client.
	ErrProtocolMismatch = errors.New("plugin protocol version mismatch")

	// ErrPluginNameMismatch is returned when the plugin name reported by the
	// peer does not match the name the client was configured to connect to.
	ErrPluginNameMismatch = errors.New("plugin name mismatch")
)

// handshakeRequest is the frame sent by the client immediately after the
// magic prefix.
type handshakeRequest struct {
	// ProtocolVersion is the go-plugin protocol version the client wants to
	// use.
	ProtocolVersion uint32

	// Name is the plugin name the client expects. It may be empty, in which
	// case the server does not enforce a name check.
	Name string
}

// handshakeResponse is the frame returned by the server.
type handshakeResponse struct {
	Status          byte
	ProtocolVersion uint32
	Name            string
	Message         string
}

// writeHandshakeRequest writes the magic prefix followed by the request
// frame. All multi-byte integers use big-endian byte order.
func writeHandshakeRequest(w io.Writer, req handshakeRequest) error {
	if len(req.Name) > handshakeMaxNameLength {
		return fmt.Errorf("handshake plugin name exceeds maximum length of %d bytes", handshakeMaxNameLength)
	}

	buf := make([]byte, 0, handshakeRequestHeaderSize+len(req.Name))
	buf = append(buf, handshakeMagic...)
	buf = append(buf, handshakeFrameHandshake)
	buf = binary.BigEndian.AppendUint32(buf, req.ProtocolVersion)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(req.Name)))
	buf = append(buf, req.Name...)

	_, err := w.Write(buf)
	return err
}

// readHandshakeResponse reads and validates a response frame.
func readHandshakeResponse(r io.Reader) (*handshakeResponse, error) {
	header := make([]byte, handshakeResponseHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	pos := 0
	if !bytes.Equal(header[pos:pos+len(handshakeMagic)], handshakeMagic) {
		return nil, fmt.Errorf("%w: expected magic prefix %q", ErrHandshakeInvalidResponse, handshakeMagic)
	}
	pos += len(handshakeMagic)

	if frameType := header[pos]; frameType != handshakeFrameHandshake {
		return nil, fmt.Errorf("%w: unexpected frame type %d", ErrHandshakeInvalidResponse, frameType)
	}
	pos++

	resp := &handshakeResponse{Status: header[pos]}
	pos++

	resp.ProtocolVersion = binary.BigEndian.Uint32(header[pos : pos+4])
	pos += 4

	nameLen := int(binary.BigEndian.Uint16(header[pos : pos+2]))
	if nameLen > handshakeMaxNameLength {
		return nil, fmt.Errorf("%w: plugin name length %d exceeds maximum of %d", ErrHandshakeInvalidResponse, nameLen, handshakeMaxNameLength)
	}

	if nameLen > 0 {
		name := make([]byte, nameLen)
		if _, err := io.ReadFull(r, name); err != nil {
			return nil, err
		}
		resp.Name = string(name)
	}

	// A trailing error message is only present on failure statuses.
	if resp.Status != handshakeStatusOK {
		msgLenBytes := make([]byte, 2)
		if _, err := io.ReadFull(r, msgLenBytes); err != nil {
			return nil, err
		}
		msgLen := int(binary.BigEndian.Uint16(msgLenBytes))
		if msgLen > handshakeMaxMessageLength {
			return nil, fmt.Errorf("%w: error message length %d exceeds maximum of %d", ErrHandshakeInvalidResponse, msgLen, handshakeMaxMessageLength)
		}
		if msgLen > 0 {
			msg := make([]byte, msgLen)
			if _, err := io.ReadFull(r, msg); err != nil {
				return nil, err
			}
			resp.Message = string(msg)
		}
	}

	return resp, nil
}

// writeHandshakeResponse writes a response frame.
func writeHandshakeResponse(w io.Writer, resp handshakeResponse) error {
	if len(resp.Name) > handshakeMaxNameLength {
		return fmt.Errorf("handshake plugin name exceeds maximum length of %d bytes", handshakeMaxNameLength)
	}
	if len(resp.Message) > handshakeMaxMessageLength {
		return fmt.Errorf("handshake error message exceeds maximum length of %d bytes", handshakeMaxMessageLength)
	}

	buf := make([]byte, 0, handshakeResponseHeaderSize+len(resp.Name)+2+len(resp.Message))
	buf = append(buf, handshakeMagic...)
	buf = append(buf, handshakeFrameHandshake)
	buf = append(buf, resp.Status)
	buf = binary.BigEndian.AppendUint32(buf, resp.ProtocolVersion)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(resp.Name)))
	buf = append(buf, resp.Name...)
	if resp.Status != handshakeStatusOK {
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(resp.Message)))
		buf = append(buf, resp.Message...)
	}

	_, err := w.Write(buf)
	return err
}

// clientHandshake performs the client side of the in-band handshake over a
// raw connection, before yamux is created.
//
// On success the returned io.ReadWriteCloser wraps the original connection
// with a buffered reader that preserves any bytes the peek consumed; it must
// be used for all subsequent traffic. On failure a wrapped sentinel error is
// returned so callers can distinguish timeouts, invalid responses and
// negotiation mismatches.
func clientHandshake(conn io.ReadWriteCloser, req handshakeRequest, timeout time.Duration) (io.ReadWriteCloser, *handshakeResponse, error) {
	if timeout <= 0 {
		timeout = defaultHandshakeTimeout
	}

	// Only net.Conn implementations support deadlines. io.ReadWriteCloser
	// values used in tests (such as net.Pipe) do as well.
	if deadlineConn, ok := conn.(net.Conn); ok {
		_ = deadlineConn.SetDeadline(time.Now().Add(timeout))
		defer func() { _ = deadlineConn.SetDeadline(time.Time{}) }()
	}

	if err := writeHandshakeRequest(conn, req); err != nil {
		if isDeadlineExceeded(err) {
			return nil, nil, fmt.Errorf("%w: %s", ErrHandshakeTimeout, err)
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
			return nil, nil, fmt.Errorf("%w: %s", ErrHandshakeInvalidResponse, err)
		}
		return nil, nil, err
	}

	// Peek the magic prefix ourselves so that a peer that does not implement
	// the handshake (an older plugin binary) and answers with arbitrary bytes
	// is reported as an invalid handshake rather than silently corrupting the
	// yamux session.
	reader := bufio.NewReader(conn)
	peeked, err := reader.Peek(len(handshakeMagic))
	if err != nil {
		if isDeadlineExceeded(err) {
			return nil, nil, fmt.Errorf("%w: %s", ErrHandshakeTimeout, err)
		}
		// A closed peer, EOF or broken pipe means no valid handshake
		// answer arrived. These are protocol errors rather than dial
		// failures (the transport connected) or timeouts.
		return nil, nil, fmt.Errorf("%w: %s", ErrHandshakeInvalidResponse, err)
	}

	if !bytes.Equal(peeked, handshakeMagic) {
		return nil, nil, fmt.Errorf("%w: expected magic prefix %q but the peer sent non-handshake bytes", ErrHandshakeInvalidResponse, handshakeMagic)
	}

	resp, err := readHandshakeResponse(reader)
	if err != nil {
		if isDeadlineExceeded(err) {
			return nil, nil, fmt.Errorf("%w: %s", ErrHandshakeTimeout, err)
		}
		return nil, nil, fmt.Errorf("%w: %s", ErrHandshakeInvalidResponse, err)
	}

	// Preserve any bytes buffered by the peek for the yamux layer.
	nextConn := newBufferedReadWriteCloser(conn, reader)
	if err := handshakeResponseError(req, resp); err != nil {
		return nextConn, resp, err
	}

	return nextConn, resp, nil
}

// handshakeResponseError translates a non-OK response status into the
// matching sentinel error.
func handshakeResponseError(req handshakeRequest, resp *handshakeResponse) error {
	switch resp.Status {
	case handshakeStatusOK:
		return nil
	case handshakeStatusProtocolMismatch:
		return fmt.Errorf("%w: plugin supports protocol version %d, client requested version %d%s",
			ErrProtocolMismatch, resp.ProtocolVersion, req.ProtocolVersion, messageSuffix(resp.Message))
	case handshakeStatusNameMismatch:
		return fmt.Errorf("%w: plugin reported name %q%s", ErrPluginNameMismatch, resp.Name, messageSuffix(resp.Message))
	default:
		return fmt.Errorf("%w: plugin rejected the handshake (status %d)%s",
			ErrHandshakeInvalidResponse, resp.Status, messageSuffix(resp.Message))
	}
}

func messageSuffix(message string) string {
	if message == "" {
		return ""
	}
	return ": " + message
}

// serverHandshake performs the server side of the in-band handshake. The
// returned io.ReadWriteCloser is always safe to hand to yamux: it is a
// buffered wrapper when a handshake was consumed, or the untouched original
// connection for legacy clients.
//
// A nil response with a nil error means a legacy client connected and no
// handshake frame was present.
func serverHandshake(conn io.ReadWriteCloser, wantName string, supportedVersions map[int]struct{}) (io.ReadWriteCloser, *handshakeResponse, error) {
	reader := bufio.NewReader(conn)
	peeked, err := reader.Peek(len(handshakeMagic))
	if err != nil {
		return nil, nil, err
	}

	// Legacy clients speak yamux directly. Do not consume a single byte.
	if !bytes.Equal(peeked, handshakeMagic) {
		return newBufferedReadWriteCloser(conn, reader), nil, nil
	}

	// Consume the magic prefix before parsing the request frame.
	magic := make([]byte, len(handshakeMagic))
	if _, err := io.ReadFull(reader, magic); err != nil {
		return nil, nil, err
	}

	header := make([]byte, handshakeRequestHeaderSize-len(handshakeMagic))
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, nil, err
	}

	pos := 0
	if frameType := header[pos]; frameType != handshakeFrameHandshake {
		resp := &handshakeResponse{Status: handshakeStatusInvalid, Message: fmt.Sprintf("unexpected frame type %d", frameType)}
		_ = writeHandshakeResponse(conn, *resp)
		return newBufferedReadWriteCloser(conn, reader), resp, nil
	}
	pos++

	req := handshakeRequest{
		ProtocolVersion: binary.BigEndian.Uint32(header[pos : pos+4]),
	}
	pos += 4

	nameLen := int(binary.BigEndian.Uint16(header[pos : pos+2]))
	if nameLen > handshakeMaxNameLength {
		resp := &handshakeResponse{Status: handshakeStatusInvalid, Message: "plugin name too long"}
		_ = writeHandshakeResponse(conn, *resp)
		return newBufferedReadWriteCloser(conn, reader), resp, nil
	}
	if nameLen > 0 {
		name := make([]byte, nameLen)
		if _, err := io.ReadFull(reader, name); err != nil {
			return nil, nil, err
		}
		req.Name = string(name)
	}

	resp := negotiateHandshake(req, wantName, supportedVersions)
	if err := writeHandshakeResponse(conn, *resp); err != nil {
		return nil, nil, err
	}

	return newBufferedReadWriteCloser(conn, reader), resp, nil
}

// negotiateHandshake contains the pure decision logic for the server side so
// it can be unit tested without any I/O.
func negotiateHandshake(req handshakeRequest, wantName string, supportedVersions map[int]struct{}) *handshakeResponse {
	// An empty configured name disables the name check; likewise a client
	// that sends no name is accepted (legacy handshake configuration).
	if wantName != "" && req.Name != "" && wantName != req.Name {
		return &handshakeResponse{
			Status:  handshakeStatusNameMismatch,
			Name:    wantName,
			Message: fmt.Sprintf("client requested plugin %q but this plugin is %q", req.Name, wantName),
		}
	}

	if len(supportedVersions) > 0 {
		if _, ok := supportedVersions[int(req.ProtocolVersion)]; !ok {
			// Report the highest version we support to make the client error
			// message actionable.
			var highest uint32
			for v := range supportedVersions {
				if uint32(v) > highest {
					highest = uint32(v)
				}
			}
			return &handshakeResponse{
				Status:          handshakeStatusProtocolMismatch,
				ProtocolVersion: highest,
				Name:            wantName,
				Message:         fmt.Sprintf("client requested protocol version %d", req.ProtocolVersion),
			}
		}
	}

	return &handshakeResponse{
		Status:          handshakeStatusOK,
		ProtocolVersion: req.ProtocolVersion,
		Name:            wantName,
	}
}

// isDeadlineExceeded reports whether err wraps a timeout. bufio can surface
// the raw os deadline error without the net.Error interface, so check both.
func isDeadlineExceeded(err error) bool {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// bufferedReadWriteCloser wraps a connection whose reads must go through a
// bufio.Reader (because Peek may have buffered handshake bytes) while writes
// continue to go directly to the underlying connection.
type bufferedReadWriteCloser struct {
	io.Reader
	raw io.ReadWriteCloser
}

func newBufferedReadWriteCloser(raw io.ReadWriteCloser, reader *bufio.Reader) io.ReadWriteCloser {
	return bufferedReadWriteCloser{Reader: reader, raw: raw}
}

func (c bufferedReadWriteCloser) Write(p []byte) (int, error) {
	return c.raw.Write(p)
}

func (c bufferedReadWriteCloser) Close() error {
	return c.raw.Close()
}
