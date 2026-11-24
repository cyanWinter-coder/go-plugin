// Copyright IBM Corp. 2016, 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/yamux"
)

func TestWriteReadHandshakeRequest(t *testing.T) {
	t.Parallel()

	req := handshakeRequest{ProtocolVersion: 7, Name: "widget"}

	var buf bytes.Buffer
	if err := writeHandshakeRequest(&buf, req); err != nil {
		t.Fatalf("write: %s", err)
	}

	// Verify the exact wire layout and big-endian byte order so the
	// handshake byte order cannot change accidentally.
	raw := buf.Bytes()
	want := []byte("GOPLGH")
	want = append(want, handshakeFrameHandshake)
	want = binary.BigEndian.AppendUint32(want, 7)
	want = binary.BigEndian.AppendUint16(want, uint16(len("widget")))
	want = append(want, "widget"...)

	if !bytes.Equal(raw, want) {
		t.Fatalf("wire bytes mismatch\n got: %v\nwant: %v", raw, want)
	}

	// Round trip through a pipe.
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	go func() { _ = writeHandshakeRequest(a, req) }()

	got, err := readHandshakeRequest(b)
	if err != nil {
		t.Fatalf("read: %s", err)
	}
	if got != req {
		t.Fatalf("round trip mismatch: got %+v want %+v", got, req)
	}
}

func readHandshakeRequest(r io.Reader) (handshakeRequest, error) {
	magic := make([]byte, len(handshakeMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return handshakeRequest{}, err
	}
	if !bytes.Equal(magic, handshakeMagic) {
		return handshakeRequest{}, fmt.Errorf("%w: bad magic", ErrHandshakeInvalidResponse)
	}

	header := make([]byte, handshakeRequestHeaderSize-len(handshakeMagic))
	if _, err := io.ReadFull(r, header); err != nil {
		return handshakeRequest{}, err
	}
	if header[0] != handshakeFrameHandshake {
		return handshakeRequest{}, fmt.Errorf("%w: bad frame type", ErrHandshakeInvalidResponse)
	}
	req := handshakeRequest{ProtocolVersion: binary.BigEndian.Uint32(header[1:5])}
	nameLen := int(binary.BigEndian.Uint16(header[5:7]))
	if nameLen > 0 {
		name := make([]byte, nameLen)
		if _, err := io.ReadFull(r, name); err != nil {
			return handshakeRequest{}, err
		}
		req.Name = string(name)
	}
	return req, nil
}

func TestWriteReadHandshakeResponse(t *testing.T) {
	t.Parallel()

	cases := []handshakeResponse{
		{Status: handshakeStatusOK, ProtocolVersion: 3, Name: "widget"},
		{Status: handshakeStatusProtocolMismatch, ProtocolVersion: 2, Name: "widget", Message: "nope"},
		{Status: handshakeStatusNameMismatch, Name: "gadget", Message: "wrong name"},
	}

	for _, want := range cases {
		var buf bytes.Buffer
		if err := writeHandshakeResponse(&buf, want); err != nil {
			t.Fatalf("write: %s", err)
		}
		got, err := readHandshakeResponse(&buf)
		if err != nil {
			t.Fatalf("read: %s", err)
		}
		if *got != want {
			t.Fatalf("round trip mismatch: got %+v want %+v", got, want)
		}
	}
}

func TestNegotiateHandshake(t *testing.T) {
	t.Parallel()

	supported := map[int]struct{}{1: {}, 2: {}}

	cases := []struct {
		name       string
		req        handshakeRequest
		wantName   string
		wantStatus byte
	}{
		{
			name:       "matching name and version",
			req:        handshakeRequest{ProtocolVersion: 2, Name: "widget"},
			wantName:   "widget",
			wantStatus: handshakeStatusOK,
		},
		{
			name:       "version 1 supported",
			req:        handshakeRequest{ProtocolVersion: 1, Name: "widget"},
			wantName:   "widget",
			wantStatus: handshakeStatusOK,
		},
		{
			name:       "name mismatch",
			req:        handshakeRequest{ProtocolVersion: 2, Name: "gadget"},
			wantName:   "widget",
			wantStatus: handshakeStatusNameMismatch,
		},
		{
			name:       "protocol mismatch",
			req:        handshakeRequest{ProtocolVersion: 99, Name: "widget"},
			wantName:   "widget",
			wantStatus: handshakeStatusProtocolMismatch,
		},
		{
			name:       "empty configured name skips name check",
			req:        handshakeRequest{ProtocolVersion: 2, Name: "anything"},
			wantName:   "",
			wantStatus: handshakeStatusOK,
		},
		{
			name:       "empty client name skips name check",
			req:        handshakeRequest{ProtocolVersion: 2, Name: ""},
			wantName:   "widget",
			wantStatus: handshakeStatusOK,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := negotiateHandshake(tc.req, tc.wantName, supported)
			if resp.Status != tc.wantStatus {
				t.Fatalf("status: got %d want %d (message: %s)", resp.Status, tc.wantStatus, resp.Message)
			}
		})
	}
}

func TestNegotiateHandshake_ReportsHighestSupportedVersion(t *testing.T) {
	t.Parallel()

	resp := negotiateHandshake(
		handshakeRequest{ProtocolVersion: 99, Name: "widget"},
		"widget",
		map[int]struct{}{1: {}, 2: {}},
	)
	if resp.Status != handshakeStatusProtocolMismatch {
		t.Fatalf("status: got %d", resp.Status)
	}
	if resp.ProtocolVersion != 2 {
		t.Fatalf("highest supported version: got %d want 2", resp.ProtocolVersion)
	}
}

// startTestHandshakeServer runs the full server side of the handshake on one
// end of a pipe and returns the client end plus the server's result. The
// server side keeps the connection open after the handshake so callers can
// exchange post-handshake data.
func startTestHandshakeServer(t *testing.T, wantName string, versions map[int]struct{}) (net.Conn, chan *handshakeResponse) {
	t.Helper()

	clientConn, serverConn := net.Pipe()
	resultCh := make(chan *handshakeResponse, 1)
	go func() {
		_, resp, err := serverHandshake(serverConn, wantName, versions)
		if err != nil && !errors.Is(err, io.ErrClosedPipe) {
			t.Errorf("server handshake: %s", err)
		}
		resultCh <- resp
	}()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	return clientConn, resultCh
}

func TestClientHandshake_Success(t *testing.T) {
	t.Parallel()

	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})
	resultCh := make(chan *handshakeResponse, 1)
	go func() {
		_, resp, err := serverHandshake(serverConn, "widget", map[int]struct{}{2: {}})
		if err != nil && !errors.Is(err, io.ErrClosedPipe) {
			t.Errorf("server handshake: %s", err)
		}
		resultCh <- resp
	}()

	nextConn, resp, err := clientHandshake(clientConn, handshakeRequest{ProtocolVersion: 2, Name: "widget"}, time.Second)
	if err != nil {
		t.Fatalf("handshake: %s", err)
	}
	if resp.Status != handshakeStatusOK || resp.Name != "widget" || resp.ProtocolVersion != 2 {
		t.Fatalf("unexpected response: %+v", resp)
	}

	// The returned connection must remain usable after the handshake. The
	// server side echoes the bytes the client writes.
	go func() {
		buf := make([]byte, 5)
		if _, err := io.ReadFull(serverConn, buf); err != nil {
			return
		}
		_, _ = serverConn.Write(buf)
	}()
	go func() { _, _ = nextConn.Write([]byte("after")) }()
	buf := make([]byte, 5)
	if _, err := io.ReadFull(nextConn, buf); err != nil {
		t.Fatalf("post-handshake read: %s", err)
	}
	if string(buf) != "after" {
		t.Fatalf("post-handshake data: %q", buf)
	}
	_ = clientConn.Close()

	if serverResp := <-resultCh; serverResp == nil || serverResp.Status != handshakeStatusOK {
		t.Fatalf("server side result: %+v", serverResp)
	}
}

func TestClientHandshake_ProtocolMismatch(t *testing.T) {
	t.Parallel()

	conn, _ := startTestHandshakeServer(t, "widget", map[int]struct{}{1: {}})
	defer conn.Close()

	_, _, err := clientHandshake(conn, handshakeRequest{ProtocolVersion: 99, Name: "widget"}, time.Second)
	if !errors.Is(err, ErrProtocolMismatch) {
		t.Fatalf("expected ErrProtocolMismatch, got: %v", err)
	}
}

func TestClientHandshake_NameMismatch(t *testing.T) {
	t.Parallel()

	conn, _ := startTestHandshakeServer(t, "widget", map[int]struct{}{1: {}})
	defer conn.Close()

	_, _, err := clientHandshake(conn, handshakeRequest{ProtocolVersion: 1, Name: "gadget"}, time.Second)
	if !errors.Is(err, ErrPluginNameMismatch) {
		t.Fatalf("expected ErrPluginNameMismatch, got: %v", err)
	}
}

func TestClientHandshake_Timeout(t *testing.T) {
	t.Parallel()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	// The peer accepts the connection but never answers: this must be a
	// timeout, not a generic connection failure.
	start := time.Now()
	_, _, err := clientHandshake(clientConn, handshakeRequest{ProtocolVersion: 1, Name: "widget"}, 50*time.Millisecond)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrHandshakeTimeout) {
		t.Fatalf("expected ErrHandshakeTimeout, got: %v", err)
	}
	if errors.Is(err, ErrHandshakeInvalidResponse) {
		t.Fatalf("timeout must not be classified as an invalid response: %v", err)
	}
	if elapsed > time.Second {
		t.Fatalf("timeout took too long: %s", elapsed)
	}
}

func TestClientHandshake_InvalidBytes(t *testing.T) {
	t.Parallel()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	// Simulate a peer that does not speak the handshake protocol and emits
	// arbitrary bytes instead. It must first drain the client's full request
	// frame so both sides are unblocked, then close.
	go func() {
		header := make([]byte, handshakeRequestHeaderSize)
		if _, err := io.ReadFull(serverConn, header); err != nil {
			return
		}
		nameLen := int(binary.BigEndian.Uint16(header[len(header)-2:]))
		if nameLen > 0 {
			if _, err := io.CopyN(io.Discard, serverConn, int64(nameLen)); err != nil {
				return
			}
		}
		_, _ = serverConn.Write([]byte{0xff, 0xfe, 0xfd, 0xfc, 0xfb, 0xfa, 0x09})
		_ = serverConn.Close()
	}()

	_, _, err := clientHandshake(clientConn, handshakeRequest{ProtocolVersion: 1, Name: "widget"}, time.Second)
	if !errors.Is(err, ErrHandshakeInvalidResponse) {
		t.Fatalf("expected ErrHandshakeInvalidResponse, got: %v", err)
	}
}

func TestClientHandshake_PeerClosed(t *testing.T) {
	t.Parallel()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	// A peer that reads the request and closes without answering is an
	// invalid response, not a timeout and not a dial failure.
	go func() {
		// Drain the complete request frame so the client's write completes,
		// then vanish.
		header := make([]byte, handshakeRequestHeaderSize)
		if _, err := io.ReadFull(serverConn, header); err != nil {
			return
		}
		nameLen := int(binary.BigEndian.Uint16(header[len(header)-2:]))
		if nameLen > 0 {
			_, _ = io.CopyN(io.Discard, serverConn, int64(nameLen))
		}
		_ = serverConn.Close()
	}()

	_, _, err := clientHandshake(clientConn, handshakeRequest{ProtocolVersion: 1, Name: "widget"}, time.Second)
	if !errors.Is(err, ErrHandshakeInvalidResponse) {
		t.Fatalf("expected ErrHandshakeInvalidResponse, got: %v", err)
	}
}

func TestServerHandshake_LegacyClient(t *testing.T) {
	t.Parallel()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	// A legacy client speaks yamux directly: no magic prefix, raw bytes.
	// Start the reader first so the peer write cannot block on a full pipe.
	readErr := make(chan error, 1)
	go func() {
		next, resp, err := serverHandshake(serverConn, "widget", map[int]struct{}{1: {}})
		if err != nil {
			readErr <- err
			return
		}
		if resp != nil {
			readErr <- fmt.Errorf("legacy connection must not produce a handshake response: %+v", resp)
			return
		}
		buf := make([]byte, len("legacy-bytes"))
		if _, err := io.ReadFull(next, buf); err != nil {
			readErr <- err
			return
		}
		if string(buf) != "legacy-bytes" {
			readErr <- fmt.Errorf("legacy bytes: %q", buf)
			return
		}
		readErr <- nil
	}()

	_, _ = clientConn.Write([]byte("legacy-bytes"))
	if err := <-readErr; err != nil {
		t.Fatal(err)
	}
}

// countingConn records bytes written after construction so tests can prove
// the cached handshake performs no I/O.
type countingConn struct {
	net.Conn
	mu         sync.Mutex
	writeCount int
}

func (c *countingConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writeCount += len(p)
	c.mu.Unlock()
	return c.Conn.Write(p)
}

func (c *countingConn) writes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeCount
}

// newHandshakeRPCClientPair builds a connected, handshake-configured
// RPCClient/RPCServer pair over real TCP conns.
func newHandshakeRPCClientPair(t *testing.T, clientName, serverName string, version uint32) (*RPCClient, *RPCServer) {
	t.Helper()

	rawClientConn, serverConn := TestConn(t)
	clientConn := &countingConn{Conn: rawClientConn}

	server := &RPCServer{
		Plugins: map[string]Plugin{
			"test": &testInterfacePlugin{Impl: &testInterfaceImpl{}},
		},
		Stdout:           new(bytes.Buffer),
		Stderr:           new(bytes.Buffer),
		Name:             serverName,
		ProtocolVersions: map[int]struct{}{int(version): {}},
	}
	go server.ServeConn(serverConn)

	client, err := newRPCClientWithHandshake(clientConn, server.Plugins, &rpcHandshakeConfig{
		Name:            clientName,
		ProtocolVersion: version,
		Timeout:         5 * time.Second,
	})
	if err != nil {
		t.Fatalf("newRPCClientWithHandshake: %s", err)
	}

	t.Cleanup(func() { _ = client.Close() })
	return client, server
}

func TestRPCClient_HandshakeEndToEnd(t *testing.T) {
	t.Parallel()

	client, _ := newHandshakeRPCClientPair(t, "widget", "widget", 1)

	name, version, err := client.Handshake()
	if err != nil {
		t.Fatalf("Handshake: %s", err)
	}
	if name != "widget" || version != 1 {
		t.Fatalf("handshake result: name=%q version=%d", name, version)
	}
	if client.NegotiatedName() != "widget" || client.NegotiatedProtocolVersion() != 1 {
		t.Fatalf("negotiated accessors: %q %d", client.NegotiatedName(), client.NegotiatedProtocolVersion())
	}

	// The negotiated connection must serve RPCs normally.
	if err := client.Ping(); err != nil {
		t.Fatalf("Ping: %s", err)
	}
	raw, err := client.Dispense("test")
	if err != nil {
		t.Fatalf("Dispense: %s", err)
	}
	if impl, ok := raw.(testInterface); !ok || impl.Double(21) != 42 {
		t.Fatalf("dispensed plugin mismatch: %#v", raw)
	}
}

func TestRPCClient_HandshakeCaching(t *testing.T) {
	t.Parallel()

	rawClientConn, serverConn := TestConn(t)
	clientConn := &countingConn{Conn: rawClientConn}

	server := &RPCServer{
		Plugins: map[string]Plugin{
			"test": &testInterfacePlugin{Impl: &testInterfaceImpl{}},
		},
		Stdout:           new(bytes.Buffer),
		Stderr:           new(bytes.Buffer),
		Name:             "widget",
		ProtocolVersions: map[int]struct{}{1: {}},
	}
	go server.ServeConn(serverConn)

	client, err := newRPCClientWithHandshake(clientConn, server.Plugins, &rpcHandshakeConfig{
		Name:            "widget",
		ProtocolVersion: 1,
		Timeout:         5 * time.Second,
	})
	if err != nil {
		t.Fatalf("newRPCClientWithHandshake: %s", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	writesAfterConstruction := clientConn.writes()

	// Repeated and concurrent calls must return the cached result without
	// writing a single byte to the wire again.
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name, version, err := client.Handshake()
			if err != nil || name != "widget" || version != 1 {
				t.Errorf("cached handshake: %q %d %v", name, version, err)
			}
		}()
	}
	wg.Wait()

	if clientConn.writes() != writesAfterConstruction {
		t.Fatalf("cached handshake performed I/O: %d extra bytes", clientConn.writes()-writesAfterConstruction)
	}
}

func TestRPCClient_HandshakeNotConfigured(t *testing.T) {
	t.Parallel()

	client, _ := TestPluginRPCConn(t, map[string]Plugin{}, nil)
	defer func() { _ = client.Close() }()

	if _, _, err := client.Handshake(); err == nil {
		t.Fatal("expected error from unconfigured handshake")
	}
}

func TestRPCClient_HandshakeNameMismatch(t *testing.T) {
	t.Parallel()

	rawClientConn, serverConn := TestConn(t)
	defer serverConn.Close()
	defer rawClientConn.Close()

	server := &RPCServer{
		Plugins:          map[string]Plugin{},
		Stdout:           new(bytes.Buffer),
		Stderr:           new(bytes.Buffer),
		Name:             "widget",
		ProtocolVersions: map[int]struct{}{1: {}},
	}
	go server.ServeConn(serverConn)

	_, err := newRPCClientWithHandshake(rawClientConn, server.Plugins, &rpcHandshakeConfig{
		Name:            "gadget",
		ProtocolVersion: 1,
		Timeout:         5 * time.Second,
	})
	if !errors.Is(err, ErrPluginNameMismatch) {
		t.Fatalf("expected ErrPluginNameMismatch, got: %v", err)
	}
}

func TestRPCClient_HandshakeProtocolMismatch(t *testing.T) {
	t.Parallel()

	rawClientConn, serverConn := TestConn(t)
	defer serverConn.Close()
	defer rawClientConn.Close()

	server := &RPCServer{
		Plugins:          map[string]Plugin{},
		Stdout:           new(bytes.Buffer),
		Stderr:           new(bytes.Buffer),
		Name:             "widget",
		ProtocolVersions: map[int]struct{}{1: {}},
	}
	go server.ServeConn(serverConn)

	_, err := newRPCClientWithHandshake(rawClientConn, server.Plugins, &rpcHandshakeConfig{
		Name:            "widget",
		ProtocolVersion: 99,
		Timeout:         5 * time.Second,
	})
	if !errors.Is(err, ErrProtocolMismatch) {
		t.Fatalf("expected ErrProtocolMismatch, got: %v", err)
	}
}

// TestRPCClient_LegacyPluginBinary reproduces the upgraded-host scenario:
// the peer runs a plain yamux server with no handshake awareness.
func TestRPCClient_LegacyPluginBinary(t *testing.T) {
	t.Parallel()

	rawClientConn, serverConn := TestConn(t)
	defer serverConn.Close()
	defer rawClientConn.Close()

	mux, err := yamux.Server(serverConn, nil)
	if err != nil {
		t.Fatalf("yamux server: %s", err)
	}
	go func() {
		for {
			stream, err := mux.Accept()
			if err != nil {
				return
			}
			_ = stream.Close()
		}
	}()

	_, err = newRPCClientWithHandshake(rawClientConn, map[string]Plugin{}, &rpcHandshakeConfig{
		Name:            "widget",
		ProtocolVersion: 1,
		Timeout:         5 * time.Second,
	})
	if !errors.Is(err, ErrHandshakeInvalidResponse) {
		t.Fatalf("expected ErrHandshakeInvalidResponse for a legacy plugin, got: %v", err)
	}
}

// TestRPCClient_LegacyClientCompatibility verifies that clients constructed
// through the unchanged NewRPCClient API still work against a handshake-aware
// server: the server peeks, sees no magic, and takes the legacy path.
func TestRPCClient_LegacyClientCompatibility(t *testing.T) {
	t.Parallel()

	rawClientConn, serverConn := TestConn(t)

	server := &RPCServer{
		Plugins: map[string]Plugin{
			"test": &testInterfacePlugin{Impl: &testInterfaceImpl{}},
		},
		Stdout:           new(bytes.Buffer),
		Stderr:           new(bytes.Buffer),
		Name:             "widget",
		ProtocolVersions: map[int]struct{}{1: {}},
	}
	go server.ServeConn(serverConn)

	// Historical constructor, no handshake configuration.
	client, err := NewRPCClient(rawClientConn, server.Plugins)
	if err != nil {
		t.Fatalf("NewRPCClient: %s", err)
	}
	defer func() { _ = client.Close() }()

	if err := client.Ping(); err != nil {
		t.Fatalf("Ping over legacy path: %s", err)
	}
}

func TestClient_ValidateHandshakePreflight(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		config  *ClientConfig
		wantErr error
	}{
		{
			name: "named handshake without cookie",
			config: &ClientConfig{
				HandshakeConfig: HandshakeConfig{Name: "widget"},
				Plugins:         testPluginMap,
			},
			wantErr: ErrPluginNameMismatch,
		},
		{
			name: "named handshake with cookie",
			config: &ClientConfig{
				HandshakeConfig: HandshakeConfig{
					Name:             "widget",
					MagicCookieKey:   "COOKIE",
					MagicCookieValue: "value",
				},
				Plugins: testPluginMap,
			},
			wantErr: nil,
		},
		{
			name: "unnamed legacy configuration",
			config: &ClientConfig{
				HandshakeConfig: testHandshake,
				Plugins:         testPluginMap,
			},
			wantErr: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClient(tc.config)
			err := c.validateHandshake()
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected %v, got %v", tc.wantErr, err)
			}
		})
	}
}

// TestClient_PreflightStartsNoProcess verifies the requirement that a
// preflight rejection leaves no subprocess and no log files behind.
func TestClient_PreflightStartsNoProcess(t *testing.T) {
	t.Parallel()

	logFile := filepath.Join(t.TempDir(), "plugin.log")
	f, err := os.Create(logFile)
	if err != nil {
		t.Fatalf("create log file: %s", err)
	}
	defer f.Close()

	stderrBuf := new(bytes.Buffer)
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "preflight",
		Level:  hclog.Trace,
		Output: f,
	})

	process := helperProcess("mock")
	c := NewClient(&ClientConfig{
		Cmd: process,
		HandshakeConfig: HandshakeConfig{
			Name: "widget",
			// No magic cookie: preflight must reject this.
		},
		Plugins: testPluginMap,
		Logger:  logger,
		Stderr:  stderrBuf,
	})

	if _, err := c.Start(); !errors.Is(err, ErrPluginNameMismatch) {
		t.Fatalf("expected ErrPluginNameMismatch, got: %v", err)
	}

	// Kill must be a no-op because no runner was ever created.
	c.Kill()
	if c.ID() != "" {
		t.Fatalf("no process should have been started, got ID %q", c.ID())
	}
	if process.ProcessState != nil {
		t.Fatal("helper process was unexpectedly started")
	}

	// Nothing may have been logged or captured.
	if stderrBuf.Len() != 0 {
		t.Fatalf("stderr received output despite preflight failure: %q", stderrBuf.String())
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("stat log file: %s", err)
	}
	if info.Size() != 0 {
		t.Fatalf("log file should be empty, got %d bytes", info.Size())
	}
}

// TestClient_NamedHandshakeSubprocess drives the full subprocess lifecycle
// with a named handshake: matching names work, a mismatching name fails with
// the typed error after the process is cleaned up.
func TestClient_NamedHandshakeSubprocess(t *testing.T) {
	t.Parallel()

	const pluginName = "named-test-plugin"

	t.Run("matching name", func(t *testing.T) {
		process := helperProcess("test-interface-named", pluginName)
		c := NewClient(&ClientConfig{
			Cmd:     process,
			Plugins: testPluginMap,
			HandshakeConfig: HandshakeConfig{
				ProtocolVersion:  testHandshake.ProtocolVersion,
				Name:             pluginName,
				MagicCookieKey:   testHandshake.MagicCookieKey,
				MagicCookieValue: testHandshake.MagicCookieValue,
			},
			Logger: hclog.NewNullLogger(),
		})
		defer c.Kill()

		proto, err := c.Client()
		if err != nil {
			t.Fatalf("Client: %s", err)
		}
		if err := proto.Ping(); err != nil {
			t.Fatalf("Ping: %s", err)
		}

		rpc, ok := proto.(*RPCClient)
		if !ok {
			t.Fatalf("expected *RPCClient, got %T", proto)
		}
		name, version, err := rpc.Handshake()
		if err != nil {
			t.Fatalf("Handshake: %s", err)
		}
		if name != pluginName || int(version) != int(testHandshake.ProtocolVersion) {
			t.Fatalf("handshake: name=%q version=%d", name, version)
		}
	})

	t.Run("mismatching name", func(t *testing.T) {
		process := helperProcess("test-interface-named", pluginName)
		c := NewClient(&ClientConfig{
			Cmd:     process,
			Plugins: testPluginMap,
			HandshakeConfig: HandshakeConfig{
				ProtocolVersion:  testHandshake.ProtocolVersion,
				Name:             "a-completely-different-plugin",
				MagicCookieKey:   testHandshake.MagicCookieKey,
				MagicCookieValue: testHandshake.MagicCookieValue,
			},
			Logger: hclog.NewNullLogger(),
		})
		defer c.Kill()

		_, err := c.Client()
		if !errors.Is(err, ErrPluginNameMismatch) {
			t.Fatalf("expected ErrPluginNameMismatch, got: %v", err)
		}
	})
}
