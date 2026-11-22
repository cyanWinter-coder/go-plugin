// Copyright IBM Corp. 2016, 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-plugin/internal/grpcmux"
	"github.com/hashicorp/go-plugin/internal/plugin"
)

// fakeGRPCStream is an in-memory stand-in for the generated bidirectional
// gRPC stream used by StartStream. It lets tests drive Recv and observe Send
// without any network.
type fakeGRPCStream struct {
	recv chan *plugin.ConnInfo
	send chan *plugin.ConnInfo

	ctxDone chan struct{}
	closeMu sync.Mutex
	closed  bool

	recvErrOnce sync.Once
	recvErr     error
}

func newFakeGRPCStream(buffer int) *fakeGRPCStream {
	return &fakeGRPCStream{
		recv:    make(chan *plugin.ConnInfo, buffer),
		send:    make(chan *plugin.ConnInfo, buffer),
		ctxDone: make(chan struct{}),
	}
}

func (f *fakeGRPCStream) Send(i *plugin.ConnInfo) error {
	f.closeMu.Lock()
	defer f.closeMu.Unlock()
	if f.closed {
		return errors.New("stream closed")
	}
	f.send <- i
	return nil
}

func (f *fakeGRPCStream) Recv() (*plugin.ConnInfo, error) {
	select {
	case <-f.ctxDone:
		return nil, f.recvErr
	case i := <-f.recv:
		if i == nil {
			return nil, f.recvErr
		}
		return i, nil
	}
}

func (f *fakeGRPCStream) closeWith(err error) {
	f.recvErrOnce.Do(func() {
		f.recvErr = err
		f.closeMu.Lock()
		f.closed = true
		f.closeMu.Unlock()
		close(f.ctxDone)
	})
}

func newSessionTestBroker(capacity int) (*GRPCBroker, *brokerConnState) {
	state := newBrokerConnState(capacity)
	broker := newGRPCBroker(state, nil, UnixSocketConfig{}, nil, disabledMuxer{})
	return broker, state
}

// disabledMuxer is a GRPCMuxer whose Enabled always reports false, for unit
// tests that exercise only the non-multiplexed broker paths.
type disabledMuxer struct{}

var _ grpcmux.GRPCMuxer = disabledMuxer{}

func (disabledMuxer) Enabled() bool                                          { return false }
func (disabledMuxer) Listener(uint32, <-chan struct{}) (net.Listener, error) { return nil, nil }
func (disabledMuxer) AcceptKnock(uint32) error                               { return nil }
func (disabledMuxer) Dial() (net.Conn, error)                                { return nil, nil }
func (disabledMuxer) Close() error                                           { return nil }

// TestBrokerConnState_ConnectionAndStreamSessions verifies requirement 1:
// every connection gets a distinct connection identity and every StartStream
// gets a distinct, monotonically increasing stream generation.
func TestBrokerConnState_ConnectionAndStreamSessions(t *testing.T) {
	first := newBrokerConnState(4)
	second := newBrokerConnState(4)

	if first.connectionID == 0 || second.connectionID == 0 {
		t.Fatalf("connection IDs must be assigned, got %d and %d", first.connectionID, second.connectionID)
	}
	if first.connectionID == second.connectionID {
		t.Fatalf("two broker connections share connection ID %d", first.connectionID)
	}

	sess1, cancel1 := first.attachSession()
	sess2, cancel2 := first.attachSession()
	defer cancel2(nil)

	if sess1.connectionID != first.connectionID || sess2.connectionID != first.connectionID {
		t.Fatalf("stream sessions must inherit their connection ID: %+v %+v", sess1, sess2)
	}
	if sess1.generation == 0 || sess2.generation <= sess1.generation {
		t.Fatalf("stream generations must be monotonic, got %d then %d", sess1.generation, sess2.generation)
	}

	current, ok := first.currentSession()
	if !ok || current != sess2 {
		t.Fatalf("current session should be the newest attachment, got %+v", current)
	}

	// Ending the older generation after a newer one attached must be a no-op:
	// an old stream cannot close the new stream (requirement 4).
	cancel1(nil)
	current, ok = first.currentSession()
	if !ok || current != sess2 {
		t.Fatalf("ending an old stream generation must not close the current one, got %+v", current)
	}
}

// TestBrokerConnState_SessionEndFailsCallsOnce verifies requirement 2: when a
// stream session closes, each service call attached to it fails exactly once
// with ErrBrokerSessionClosed.
func TestBrokerConnState_SessionEndFailsCallsOnce(t *testing.T) {
	state := newBrokerConnState(4)
	sess, end := state.attachSession()
	defer end(nil)

	failures := make(chan error, 4)
	for range 3 {
		attachedSess, ok := state.onSessionEnd(7, func(err error) {
			failures <- err
		})
		if !ok || attachedSess != sess {
			t.Fatalf("call should attach to the current session")
		}
	}

	state.endSession(sess.generation, &BrokerSessionClosedError{ServiceID: 7})

	for i := range 3 {
		select {
		case err := <-failures:
			if !errors.Is(err, ErrBrokerSessionClosed) {
				t.Fatalf("call %d: expected ErrBrokerSessionClosed, got %v", i, err)
			}
			var sc *BrokerSessionClosedError
			if !errors.As(err, &sc) || sc.ServiceID != 7 {
				t.Fatalf("call %d: expected service ID 7 in error, got %v", i, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("call %d was not failed when the session ended", i)
		}
	}

	// Ending the same session again must not deliver a second failure.
	state.endSession(sess.generation, &BrokerSessionClosedError{ServiceID: 7})
	select {
	case err := <-failures:
		t.Fatalf("duplicate session end delivered a second failure: %v", err)
	default:
	}
}

// TestBrokerConnState_SupersedeFailsOldCallsNotNewOnes verifies requirement 4
// at the connection layer: attaching a new stream fails only the old
// generation's calls; calls attached afterwards stay healthy.
func TestBrokerConnState_SupersedeFailsOldCallsNotNewOnes(t *testing.T) {
	state := newBrokerConnState(4)
	oldSess, _ := state.attachSession()

	oldFailed := make(chan error, 1)
	state.onSessionEnd(1, func(err error) { oldFailed <- err })

	newSess, endNew := state.attachSession()
	defer endNew(nil)

	select {
	case err := <-oldFailed:
		if !errors.Is(err, ErrBrokerSessionClosed) {
			t.Fatalf("old session call should fail with ErrBrokerSessionClosed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("old session call was not failed when superseded")
	}

	newFailed := make(chan error, 1)
	attached, ok := state.onSessionEnd(2, func(err error) { newFailed <- err })
	if !ok || attached != newSess {
		t.Fatalf("new call must attach to the new session %+v, got %+v ok=%v", newSess, attached, ok)
	}

	// The old session ending late must not affect the new call.
	state.endSession(oldSess.generation, &BrokerSessionClosedError{})
	select {
	case err := <-newFailed:
		t.Fatalf("new session call must not be failed by the old stream ending: %v", err)
	default:
	}
}

// TestBrokerConnState_RecvQueueBackpressure verifies requirement 3 for the
// receive path: the bounded per-connection queue returns ErrBrokerQueueFull
// instead of blocking, and the queue is isolated to its connection.
func TestBrokerConnState_RecvQueueBackpressure(t *testing.T) {
	const capacity = 2
	state := newBrokerConnState(capacity)
	sess, end := state.attachSession()
	defer end(nil)

	for i := range capacity {
		err := state.enqueueRecv(&plugin.ConnInfo{ServiceId: uint32(i + 1)}, sess)
		if err != nil {
			t.Fatalf("enqueue %d should succeed within capacity: %v", i, err)
		}
	}

	err := state.enqueueRecv(&plugin.ConnInfo{ServiceId: 99}, sess)
	if !errors.Is(err, ErrBrokerQueueFull) {
		t.Fatalf("full receive queue should return ErrBrokerQueueFull, got %v", err)
	}
	var qf *BrokerQueueFullError
	if !errors.As(err, &qf) || qf.ServiceID != 99 {
		t.Fatalf("backpressure error should identify service 99, got %v", err)
	}

	// Draining one slot makes room again.
	if _, _, err := state.Recv(); err != nil {
		t.Fatalf("Recv failed: %v", err)
	}
	if err := state.enqueueRecv(&plugin.ConnInfo{ServiceId: 99}, sess); err != nil {
		t.Fatalf("queue should accept again after draining: %v", err)
	}

	// A separate connection has its own independent queue and is unaffected.
	other := newBrokerConnState(capacity)
	otherSess, endOther := other.attachSession()
	defer endOther(nil)
	if err := other.enqueueRecv(&plugin.ConnInfo{ServiceId: 1}, otherSess); err != nil {
		t.Fatalf("per-connection queue isolation broken: %v", err)
	}
}

// TestBrokerConnState_SendQueueBackpressure verifies requirement 3 for the
// send path: Send on a saturated per-session queue fails fast with
// ErrBrokerQueueFull and never blocks the caller. No send pump is running, so
// the bounded queue fills deterministically.
func TestBrokerConnState_SendQueueBackpressure(t *testing.T) {
	const capacity = 2
	state := newBrokerConnState(capacity)
	sess, end := state.attachSession()
	defer end(nil)

	state.mu.Lock()
	sendCh := state.sessions[sess.generation].send
	state.mu.Unlock()
	replyChs := make([]chan error, capacity)
	for i := range capacity {
		replyChs[i] = make(chan error, 1)
		sendCh <- &sendErr{i: &plugin.ConnInfo{ServiceId: uint32(i)}, ch: replyChs[i]}
	}

	doneErr := make(chan error, 1)
	go func() {
		doneErr <- state.Send(&plugin.ConnInfo{ServiceId: 42})
	}()
	select {
	case err := <-doneErr:
		if !errors.Is(err, ErrBrokerQueueFull) {
			t.Fatalf("full send queue should return ErrBrokerQueueFull, got %v", err)
		}
		var qf *BrokerQueueFullError
		if !errors.As(err, &qf) || qf.ServiceID != 42 {
			t.Fatalf("backpressure error should identify service 42, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Send blocked instead of returning recognisable backpressure")
	}

	// Ending the session drains the saturated queue and fails every queued
	// caller deterministically instead of leaking a blocked goroutine.
	state.endSession(sess.generation, &BrokerSessionClosedError{})
	for i, reply := range replyChs {
		select {
		case err := <-reply:
			if !errors.Is(err, ErrBrokerSessionClosed) {
				t.Fatalf("queued sender %d should fail when the session ends, got %v", i, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("queued sender %d not resolved on session end", i)
		}
	}
}

// TestBrokerConnState_SendAfterSessionEndFails verifies that Send never
// blocks when the current session ends around the enqueue: it returns a
// session-closed (or backpressure) error deterministically.
func TestBrokerConnState_SendAfterSessionEndFails(t *testing.T) {
	state := newBrokerConnState(1)
	sess, _ := state.attachSession()

	// Fill the single-slot queue so the next Send either sees backpressure or,
	// if the session ends first, a session-closed error, but never blocks.
	state.mu.Lock()
	sendCh := state.sessions[sess.generation].send
	state.mu.Unlock()
	sendCh <- &sendErr{i: &plugin.ConnInfo{ServiceId: 1}, ch: make(chan error, 1)}

	done := make(chan error, 1)
	go func() { done <- state.Send(&plugin.ConnInfo{ServiceId: 2}) }()

	// End the session concurrently with the saturated Send.
	state.endSession(sess.generation, &BrokerSessionClosedError{})

	select {
	case err := <-done:
		if !errors.Is(err, ErrBrokerQueueFull) &&
			!errors.Is(err, ErrBrokerSessionClosed) &&
			!errors.Is(err, ErrBrokerClosed) {
			t.Fatalf("Send during session teardown should fail recognizably, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Send blocked across a session end")
	}
}

// TestBrokerConnState_RunSendExitsOnSupersession verifies the outbound pump
// stops promptly once its stream session is replaced, so an old stream never
// sends messages committed onto the new session.
func TestBrokerConnState_RunSendExitsOnSupersession(t *testing.T) {
	state := newBrokerConnState(8)
	sess, _ := state.attachSession()
	stream := newFakeGRPCStream(8)
	done := make(chan struct{})
	go func() {
		state.runSend(stream, sess, stream.ctxDone)
		close(done)
	}()

	newSess, endNew := state.attachSession()
	defer endNew(nil)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("old session send pump did not exit after supersession")
	}

	// A message committed onto the new session's send queue must never reach
	// the old stream (its pump has exited). Enqueue directly because no pump is
	// running for the new session; a blocking Send would correctly wait for a
	// reply that never comes, which is tested separately.
	state.mu.Lock()
	newSend := state.sessions[newSess.generation].send
	state.mu.Unlock()
	newSend <- &sendErr{i: &plugin.ConnInfo{ServiceId: 77}, ch: make(chan error, 1)}
	select {
	case msg := <-stream.send:
		t.Fatalf("new-session message leaked onto the old stream: %+v", msg)
	default:
	}
	if !state.isCurrent(newSess) {
		t.Fatal("new session should remain current")
	}
}

// TestBrokerConnState_RecvDropsMessagesFromSupersededStream verifies the
// headline fix: messages still arriving on an old stream after a reconnect are
// never committed onto the new session.
func TestBrokerConnState_RecvDropsMessagesFromSupersededStream(t *testing.T) {
	state := newBrokerConnState(8)
	oldSess, _ := state.attachSession()
	newSess, endNew := state.attachSession()
	defer endNew(nil)

	// A message tagged with the old session must not be enqueued as current
	// work: runRecv refuses it before enqueue.
	oldStream := newFakeGRPCStream(2)
	oldStream.recv <- &plugin.ConnInfo{ServiceId: 5}
	go func() { _ = state.runRecv(oldStream, oldSess, oldStream.ctxDone) }()

	select {
	case item := <-state.recv:
		t.Fatalf("old-stream message leaked into the connection recv queue: %+v", item)
	case <-time.After(50 * time.Millisecond):
	}

	// The new session still delivers normally.
	newStream := newFakeGRPCStream(2)
	newStream.recv <- &plugin.ConnInfo{ServiceId: 6}
	go func() { _ = state.runRecv(newStream, newSess, newStream.ctxDone) }()
	select {
	case item := <-state.recv:
		if item.info.ServiceId != 6 || item.session != newSess {
			t.Fatalf("new session message mis-routed: %+v", item)
		}
	case <-time.After(time.Second):
		t.Fatal("new session message was not forwarded")
	}
}

// TestGRPCBroker_PendingCallFailsOnSessionEnd verifies requirement 2 end to
// end through the broker: a Dial waiting on a stream session fails once with
// ErrBrokerSessionClosed when that session closes.
func TestGRPCBroker_PendingCallFailsOnSessionEnd(t *testing.T) {
	broker, state := newSessionTestBroker(8)
	go broker.Run()
	defer broker.Close()

	sess, end := state.attachSession()

	const id uint32 = 11
	p, err := broker.getClientStream(id)
	if err != nil {
		t.Fatal(err)
	}
	if p.session != sess {
		t.Fatalf("pending entry should bind to session %+v, got %+v", sess, p.session)
	}

	dialErr := make(chan error, 1)
	go func() {
		_, err := broker.Dial(id)
		dialErr <- err
	}()

	// Give the dialer time to block on the pending entry.
	time.Sleep(50 * time.Millisecond)
	end(&BrokerSessionClosedError{ServiceID: id})

	select {
	case err := <-dialErr:
		if !errors.Is(err, ErrBrokerSessionClosed) {
			t.Fatalf("Dial should fail with ErrBrokerSessionClosed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Dial did not fail when its stream session ended")
	}

	// The stale entry was removed: the same service ID can be re-attached on a
	// fresh session.
	newSess, endNew := state.attachSession()
	defer endNew(nil)
	p2, err := broker.getClientStream(id)
	if err != nil {
		t.Fatal(err)
	}
	if p2 == p || p2.session != newSess {
		t.Fatalf("service ID %d should re-attach to the new session %+v, got %+v", id, newSess, p2.session)
	}
}

// TestGRPCBroker_LateMessageFromOldSessionIsDropped verifies the full broker
// routing path: a message that arrives tagged with an old stream generation is
// dropped, while the same service ID on the new session is delivered.
func TestGRPCBroker_LateMessageFromOldSessionIsDropped(t *testing.T) {
	broker, state := newSessionTestBroker(8)
	go broker.Run()
	defer broker.Close()

	oldSess, _ := state.attachSession()
	newSess, endNew := state.attachSession()
	defer endNew(nil)

	const id uint32 = 23

	// Late message from the replaced stream: it must never reach a waiter.
	state.recv <- &queuedConnInfo{
		info:    &plugin.ConnInfo{ServiceId: id, Address: "stale"},
		session: oldSess,
	}
	// Current-session message for the same service.
	state.recv <- &queuedConnInfo{
		info:    &plugin.ConnInfo{ServiceId: id, Address: "fresh"},
		session: newSess,
	}

	p, err := broker.getClientStream(id)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-p.ch:
		if msg.Address != "fresh" {
			t.Fatalf("stale message from old session was committed: %+v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("current-session message was not delivered")
	}
}

// TestGRPCBroker_BrokerCloseFailsCalls verifies that permanent shutdown fails
// attached calls with ErrBrokerClosed.
func TestGRPCBroker_BrokerCloseFailsCalls(t *testing.T) {
	broker, state := newSessionTestBroker(8)
	go broker.Run()

	sess, _ := state.attachSession()
	const id uint32 = 31
	p, err := broker.getClientStream(id)
	if err != nil {
		t.Fatal(err)
	}
	if p.session != sess {
		t.Fatalf("pending entry bound to wrong session")
	}

	if err := broker.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-p.doneCh:
	case <-time.After(time.Second):
		t.Fatal("pending call was not failed when the broker closed")
	}

	if err := state.Send(&plugin.ConnInfo{ServiceId: id}); !errors.Is(err, ErrBrokerClosed) {
		t.Fatalf("Send after close should return ErrBrokerClosed, got %v", err)
	}
	if _, _, err := state.Recv(); !errors.Is(err, ErrBrokerClosed) {
		t.Fatalf("Recv after close should return ErrBrokerClosed, got %v", err)
	}
}
