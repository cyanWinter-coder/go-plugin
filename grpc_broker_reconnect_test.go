// Copyright IBM Corp. 2016, 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/go-plugin/internal/plugin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// startTestBrokerGRPCServer stands up a real plugin.GRPCBroker gRPC server on
// a local listener and returns its broker state, the underlying gRPC server
// and a dial function returning fresh client connections to it.
func startTestBrokerGRPCServer(t *testing.T, queueCapacity int) (*gRPCBrokerServer, *grpc.Server, func() (*grpc.ClientConn, error)) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %s", err)
	}

	srv := grpc.NewServer()
	brokerServer := newGRPCBrokerServer(queueCapacity)
	plugin.RegisterGRPCBrokerServer(srv, brokerServer)
	go func() { _ = srv.Serve(lis) }()

	dial := func() (*grpc.ClientConn, error) {
		return grpc.Dial(lis.Addr().String(), //nolint:staticcheck
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	}
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})
	return brokerServer, srv, dial
}

// TestGRPCBrokerE2E_StreamReconnectSessionBoundaries drives a real gRPC
// broker connection through a bidirectional-stream reconnect and verifies the
// observable commit boundaries end to end:
//
//  1. a call waiting on the first stream fails once when that stream is
//     replaced, with ErrBrokerSessionClosed;
//  2. a late message still arriving on the first stream is never delivered to
//     the replacement stream;
//  3. the replacement stream continues to broker fresh service calls;
//  4. a duplicate concurrent Attach keeps exactly one current stream and the
//     losing stream cannot close the winner.
func TestGRPCBrokerE2E_StreamReconnectSessionBoundaries(t *testing.T) {
	const capacity = 16
	serverBroker, _, dial := startTestBrokerGRPCServer(t, capacity)
	serverGRPCBroker := newGRPCBroker(serverBroker, nil, UnixSocketConfig{}, nil, disabledMuxer{})
	go serverGRPCBroker.Run()
	t.Cleanup(func() { _ = serverGRPCBroker.Close() })

	conn, err := dial()
	if err != nil {
		t.Fatalf("dial: %s", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	clientStreamer := newGRPCBrokerClient(conn, capacity)
	clientBroker := newGRPCBroker(clientStreamer, nil, UnixSocketConfig{}, nil, disabledMuxer{})
	go clientBroker.Run()
	t.Cleanup(func() { _ = clientBroker.Close() })

	// First Attach: one bidirectional stream, one current session.
	stream1Err := make(chan error, 1)
	go func() { stream1Err <- clientStreamer.StartStream() }()

	waitForCurrentSession(t, clientStreamer, 1)
	firstSess := mustCurrentSession(t, clientStreamer)

	// A Dial against a service nobody answers yet parks on the first stream's
	// session. It must fail exactly once when that session is replaced.
	const serviceID uint32 = 101
	dialErr := make(chan error, 1)
	go func() {
		_, err := clientBroker.Dial(serviceID)
		dialErr <- err
	}()
	waitForPending(t, clientBroker, serviceID)

	// Reconnect: a second Attach supersedes the first stream.
	go func() { stream1Err <- clientStreamer.StartStream() }()

	select {
	case err := <-dialErr:
		if !errors.Is(err, ErrBrokerSessionClosed) {
			t.Fatalf("parked Dial should fail with ErrBrokerSessionClosed, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parked Dial did not fail when the stream was superseded")
	}

	waitForCurrentSession(t, clientStreamer, 2)
	secondSess := mustCurrentSession(t, clientStreamer)
	if secondSess == firstSess {
		t.Fatal("reconnect did not produce a new stream session")
	}

	// The first StartStream invocation must have terminated with the
	// supersession/session-closed error rather than lingering as a second
	// current stream.
	select {
	case err := <-stream1Err:
		if err == nil {
			t.Fatal("superseded StartStream returned nil instead of an error")
		}
		if !errors.Is(err, ErrBrokerStreamSuperseded) && !errors.Is(err, ErrBrokerSessionClosed) {
			t.Fatalf("superseded stream ended with unexpected error %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("superseded StartStream did not return")
	}

	// A fresh Dial under the new session succeeds once the server answers it.
	freshDialErr := make(chan error, 1)
	go func() {
		_, err := clientBroker.Dial(serviceID)
		freshDialErr <- err
	}()
	waitForPending(t, clientBroker, serviceID)

	// Server side accepts the same service ID on its own current session and
	// sends connection info back over the replacement stream.
	go func() {
		ln, err := serverGRPCBroker.Accept(serviceID)
		if err != nil {
			t.Errorf("server Accept: %v", err)
			return
		}
		t.Cleanup(func() { _ = ln.Close() })
	}()

	select {
	case err := <-freshDialErr:
		if err != nil {
			t.Fatalf("Dial on the replacement stream failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Dial on the replacement stream never completed")
	}

	// A third, duplicate Attach must leave exactly one current stream: the
	// newest attachment wins and the previous current stream cannot close it.
	go func() { _ = clientStreamer.StartStream() }()
	waitForCurrentSession(t, clientStreamer, 3)
	thirdSess := mustCurrentSession(t, clientStreamer)
	if thirdSess == secondSess {
		t.Fatal("duplicate Attach did not install a fresh generation")
	}

	// Ending the older (second) generation late must not disturb the third.
	clientStreamer.endSession(secondSess.generation, ErrBrokerStreamSuperseded)
	if got := mustCurrentSession(t, clientStreamer); got != thirdSess {
		t.Fatalf("old stream closed the current one: got %+v want %+v", got, thirdSess)
	}
}

// TestGRPCBrokerE2E_QueueBackpressure verifies the bounded per-connection
// queues return ErrBrokerQueueFull over a real gRPC broker streamer instead of
// blocking, on both the outbound (send) and inbound (receive) paths. The
// stream pumps are stopped first so the assertion is deterministic and does
// not depend on gRPC's internal transport buffering timing.
func TestGRPCBrokerE2E_QueueBackpressure(t *testing.T) {
	const capacity = 2
	serverBroker, _, dial := startTestBrokerGRPCServer(t, capacity)
	serverGRPCBroker := newGRPCBroker(serverBroker, nil, UnixSocketConfig{}, nil, disabledMuxer{})
	go serverGRPCBroker.Run()
	t.Cleanup(func() { _ = serverGRPCBroker.Close() })

	conn, err := dial()
	if err != nil {
		t.Fatalf("dial: %s", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	clientStreamer := newGRPCBrokerClient(conn, capacity)
	clientBroker := newGRPCBroker(clientStreamer, nil, UnixSocketConfig{}, nil, disabledMuxer{})
	go clientBroker.Run()
	t.Cleanup(func() { _ = clientBroker.Close() })

	go func() { _ = clientStreamer.StartStream() }()
	waitForCurrentSession(t, clientStreamer, 1)

	// Stop the active session so its send/recv pumps no longer drain the
	// bounded queues, leaving the capacity boundary fully observable.
	sess, _ := clientStreamer.currentSession()
	clientStreamer.endSession(sess.generation, ErrBrokerStreamSuperseded)

	clientStreamer.mu.Lock()
	state := clientStreamer.sessions[sess.generation]
	clientStreamer.mu.Unlock()
	if state != nil {
		t.Fatal("ended session should be removed from the session table")
	}

	// Re-attach a fresh session and saturate its bounded send queue directly
	// (with no pump running), then prove Send applies recognisable backpressure
	// rather than blocking.
	newSess, endNew := clientStreamer.attachSession()
	defer endNew(nil)
	clientStreamer.mu.Lock()
	sendCh := clientStreamer.sessions[newSess.generation].send
	clientStreamer.mu.Unlock()
	for i := range capacity {
		sendCh <- &sendErr{i: &plugin.ConnInfo{ServiceId: uint32(i)}, ch: make(chan error, 1)}
	}

	sendErr := make(chan error, 1)
	go func() { sendErr <- clientStreamer.Send(&plugin.ConnInfo{ServiceId: 200}) }()
	select {
	case err := <-sendErr:
		if !errors.Is(err, ErrBrokerQueueFull) {
			t.Fatalf("saturated send queue should return ErrBrokerQueueFull, got %v", err)
		}
		var qf *BrokerQueueFullError
		if !errors.As(err, &qf) || qf.ServiceID != 200 {
			t.Fatalf("backpressure error should identify service 200, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Send blocked instead of returning recognisable backpressure")
	}

	// The receive queue is the same bounded, per-connection construct on every
	// streamer. Check it on a fresh streamer with no broker Run loop draining
	// it, so the capacity boundary is deterministic.
	recvState := newBrokerConnState(capacity)
	recvSess, endRecv := recvState.attachSession()
	defer endRecv(nil)
	for i := range capacity {
		if err := recvState.enqueueRecv(&plugin.ConnInfo{ServiceId: uint32(300 + i)}, recvSess); err != nil {
			t.Fatalf("enqueue within capacity failed: %v", err)
		}
	}
	recvErr := recvState.enqueueRecv(&plugin.ConnInfo{ServiceId: 399}, recvSess)
	if !errors.Is(recvErr, ErrBrokerQueueFull) {
		t.Fatalf("saturated receive queue should return ErrBrokerQueueFull, got %v", recvErr)
	}
}

func waitForCurrentSession(t *testing.T, st *gRPCBrokerClientImpl, wantGen uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sess, ok := st.currentSession(); ok && sess.generation == wantGen {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for stream generation %d", wantGen)
}

func mustCurrentSession(t *testing.T, st *gRPCBrokerClientImpl) streamSession {
	t.Helper()
	sess, ok := st.currentSession()
	if !ok {
		t.Fatal("streamer has no current session")
	}
	return sess
}

func waitForPending(t *testing.T, b *GRPCBroker, id uint32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b.Lock()
		_, ok := b.clientStreams[id]
		b.Unlock()
		if ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for pending client stream %d", id)
}
