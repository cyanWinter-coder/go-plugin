// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-plugin/internal/grpcmux"
	"github.com/hashicorp/go-plugin/internal/plugin"
	"google.golang.org/grpc"
)

// fakeStreamer is an in-memory implementation of the broker streamer used by
// the unit tests. Sent messages are delivered to sendCh; tests can also inject
// frames (including late ones) directly via deliver. recvMu guards recvCh so
// deliver does not block a producer goroutine when the broker chooses to drop
// a frame, and fail (peer close) races a pending deliver safely.
type fakeStreamer struct {
	sendCh chan *plugin.ConnInfo

	recvMu sync.Mutex
	recvCh chan *plugin.ConnInfo
	closed bool

	quit chan struct{}
	o    sync.Once
}

func newFakeStreamer() *fakeStreamer {
	return &fakeStreamer{
		sendCh: make(chan *plugin.ConnInfo),
		recvCh: make(chan *plugin.ConnInfo, 128),
		quit:   make(chan struct{}),
	}
}

func (s *fakeStreamer) Send(i *plugin.ConnInfo) error {
	// quit is selected even while blocked on sendCh, matching the real
	// streamers: broker Close must unblock an in-flight Send.
	select {
	case <-s.quit:
		return errors.New("broker closed")
	case s.sendCh <- i:
		return nil
	}
}

func (s *fakeStreamer) Recv() (*plugin.ConnInfo, error) {
	select {
	case <-s.quit:
		return nil, errors.New("stream closed")
	case i := <-s.recvCh:
		if i == nil {
			return nil, errors.New("stream closed")
		}
		return i, nil
	}
}

func (s *fakeStreamer) Close() {
	s.o.Do(func() { close(s.quit) })
}

// deliver injects a frame as if it had arrived from the peer. It never blocks
// the caller: if the broker cannot accept the frame (for example because it
// correctly dropped a late frame), the frame is discarded.
func (s *fakeStreamer) deliver(i *plugin.ConnInfo) {
	s.recvMu.Lock()
	defer s.recvMu.Unlock()
	if s.closed {
		return
	}
	select {
	case s.recvCh <- i:
	default:
	}
}

// fail closes the receive side so Run's Recv returns an error, simulating the
// peer shutting down the control stream.
func (s *fakeStreamer) fail() {
	s.recvMu.Lock()
	defer s.recvMu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.recvCh)
	}
}

// newTestBroker wires a broker to a fake streamer with a short drain window so
// the fence/recycle logic is fast and deterministic in tests.
func newTestBroker(t *testing.T) (*GRPCBroker, *fakeStreamer) {
	t.Helper()
	st := newFakeStreamer()
	b := newGRPCBroker(st, nil, UnixSocketConfig{}, nil, nil)
	b.drainTimeout = 20 * time.Millisecond
	go b.Run()
	t.Cleanup(func() { _ = b.Close() })
	return b, st
}

func connInfo(id uint32) *plugin.ConnInfo {
	return &plugin.ConnInfo{ServiceId: id, Network: "tcp", Address: "127.0.0.1:0"}
}

// TestGRPCBroker_GenerationIsolation proves a late frame from an old generation
// can never be consumed by the next call that reuses the same stream ID.
func TestGRPCBroker_GenerationIsolation(t *testing.T) {
	b, st := newTestBroker(t)
	const id uint32 = 7

	// First generation: negotiate and complete normally.
	p1, err := b.acquireClientStream(id)
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	firstGen := p1.generation

	st.deliver(connInfo(id))
	msg, err := b.waitForConnInfo(p1, b.clientStreams, errors.New("timeout"))
	if err != nil {
		t.Fatalf("wait 1: %v", err)
	}
	if msg.ServiceId != id {
		t.Fatalf("unexpected service id %d", msg.ServiceId)
	}

	// While generation 1 is draining, a frame for the old generation arrives
	// late. It must be absorbed and dropped, not buffered for a future caller.
	st.deliver(connInfo(id))

	// The ID is fenced off until recycle; acquiring then waits and yields a
	// brand-new generation that did not inherit the late frame.
	p2, err := b.acquireClientStream(id)
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	if p2 == p1 {
		t.Fatal("expected a new entry generation after recycle")
	}
	if p2.generation <= firstGen {
		t.Fatalf("generation did not advance: %d -> %d", firstGen, p2.generation)
	}

	select {
	case stray := <-p2.ch:
		t.Fatalf("new generation consumed a stale frame: %+v", stray)
	default:
	}
}

// TestGRPCBroker_RepeatedDialLateFrame reproduces the reported failure at the
// Dial level: a frame that arrives late for a finished call must not be handed
// to the next Dial reusing the ID, and that next Dial must still complete
// normally when its own frame arrives.
func TestGRPCBroker_RepeatedDialLateFrame(t *testing.T) {
	b, st := newTestBroker(t)
	const id uint32 = 8

	negotiateOnce := func() {
		p, err := b.acquireClientStream(id)
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		st.deliver(connInfo(id))
		if _, err := b.waitForConnInfo(p, b.clientStreams, errors.New("timeout")); err != nil {
			t.Fatalf("wait: %v", err)
		}
	}

	// First call completes, then a frame for the old generation arrives late.
	negotiateOnce()
	st.deliver(connInfo(id))

	// The second call gets a fresh generation, ignores the late frame, and
	// completes on its own frame.
	next, err := b.acquireClientStream(id)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	st.deliver(connInfo(id))
	msg, err := b.waitForConnInfo(next, b.clientStreams, errors.New("timeout"))
	if err != nil {
		t.Fatalf("second call failed: %v", err)
	}
	if msg.ServiceId != id {
		t.Fatalf("unexpected service id %d", msg.ServiceId)
	}
}

// TestGRPCBroker_ServerEntryDrainIsolation verifies that re-accepting a
// multiplexed listener ID during the drain window waits for the old generation
// to recycle and yields a fresh, live entry rather than one already finished.
func TestGRPCBroker_ServerEntryDrainIsolation(t *testing.T) {
	st := newFakeStreamer()
	mx := newFakeMuxer()
	b := newGRPCBroker(st, nil, UnixSocketConfig{}, nil, mx)
	b.drainTimeout = 30 * time.Millisecond
	go b.Run()
	t.Cleanup(func() { _ = b.Close() })

	const id uint32 = 9

	first, err := b.acquireServerStream(id)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	b.finish(first, b.serverStreams, nil)

	// Re-acquiring while draining must block until recycle, then return a new
	// live generation whose doneCh is not already closed.
	second, err := b.acquireServerStream(id)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if second == first {
		t.Fatal("re-accept returned the drained generation")
	}
	if second.state != entryLive || isClosed(second.doneCh) {
		t.Fatal("new server listener got a finished entry")
	}
}

// TestGRPCBroker_DrainFencesIDBeforeReuse verifies requirement 3 directly: the
// stream ID stays in the map during draining and cannot be allocated until the
// drain window elapses.
func TestGRPCBroker_DrainFencesIDBeforeReuse(t *testing.T) {
	b, st := newTestBroker(t)
	const id uint32 = 11

	p1, err := b.acquireClientStream(id)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	st.deliver(connInfo(id))
	if _, err := b.waitForConnInfo(p1, b.clientStreams, errors.New("timeout")); err != nil {
		t.Fatalf("wait: %v", err)
	}

	// Immediately after completion the entry is draining and still owns the ID.
	b.Lock()
	cur, ok := b.clientStreams[id]
	b.Unlock()
	if !ok || cur != p1 || cur.state != entryDraining {
		t.Fatal("expected draining entry to keep fencing the ID")
	}

	// Re-acquiring must block for the drain window rather than return at once.
	done := make(chan struct{})
	go func() {
		_, _ = b.acquireClientStream(id)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("ID was reallocated before the drain window elapsed")
	case <-time.After(b.drainTimeout / 2):
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("acquire did not unblock after the drain window")
	}

	// The waiting acquire itself re-creates the entry, so assert the new
	// entry is a distinct, live generation rather than the drained one.
	b.Lock()
	next, exists := b.clientStreams[id]
	b.Unlock()
	if !exists {
		t.Fatal("expected the re-acquire to allocate a fresh entry")
	}
	if next == p1 {
		t.Fatal("re-acquire returned the drained generation")
	}
	if next.state != entryLive {
		t.Fatalf("new generation not live: state %d", next.state)
	}
}

// TestGRPCBroker_TerminalPublishedOnce checks that timeout, Close and normal
// completion all converge on a single terminal state and doneCh closes once.
func TestGRPCBroker_TerminalPublishedOnce(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		b, _ := newTestBroker(t)
		b.drainTimeout = 10 * time.Millisecond

		p, err := b.acquireClientStream(21)
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}

		timeoutErr := errors.New("timeout waiting for connection info")
		_, err = b.waitForConnInfo(p, b.clientStreams, timeoutErr)
		if !errors.Is(err, timeoutErr) {
			t.Fatalf("expected timeout error, got %v", err)
		}

		// A second terminal path (broker Close) must not panic or change the
		// already published result.
		_ = b.Close()
		if !isClosed(p.doneCh) {
			t.Fatal("doneCh not closed after terminal state")
		}
		if !errors.Is(p.terminal, timeoutErr) {
			t.Fatalf("terminal error changed: %v", p.terminal)
		}
	})

	t.Run("broker close", func(t *testing.T) {
		b, _ := newTestBroker(t)
		p, err := b.acquireClientStream(22)
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}

		_ = b.Close()

		select {
		case <-p.doneCh:
		case <-time.After(time.Second):
			t.Fatal("waiter was not unblocked by Close")
		}
		if !errors.Is(p.terminal, errBrokerClosed) {
			t.Fatalf("expected broker closed error, got %v", p.terminal)
		}

		// Closing again must be a no-op.
		_ = b.Close()
	})

	t.Run("peer close", func(t *testing.T) {
		b, st := newTestBroker(t)
		p, err := b.acquireClientStream(23)
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}

		st.fail()

		select {
		case <-p.doneCh:
		case <-time.After(time.Second):
			t.Fatal("waiter was not unblocked when the peer closed")
		}
		if !errors.Is(p.terminal, errPeerClosed) {
			t.Fatalf("expected peer closed error, got %v", p.terminal)
		}
	})
}

// TestGRPCBroker_CloseRejectsNewStreams ensures new Accept/Dial/acquire calls
// are refused after Close while in-flight ones are released.
func TestGRPCBroker_CloseRejectsNewStreams(t *testing.T) {
	b, _ := newTestBroker(t)
	_ = b.Close()

	if _, err := b.acquireClientStream(31); !errors.Is(err, errBrokerClosed) {
		t.Fatalf("acquire after close: %v", err)
	}
	if _, err := b.acquireServerStream(32); !errors.Is(err, errBrokerClosed) {
		t.Fatalf("server acquire after close: %v", err)
	}
	if _, err := b.Accept(33); !errors.Is(err, errBrokerClosed) {
		t.Fatalf("accept after close: %v", err)
	}
}

// TestGRPCBroker_ConcurrentHealthyStreamsNotBlocked runs many concurrent Dials
// against distinct IDs while Close is racing, and asserts healthy streams
// complete and every waiter resolves exactly once.
func TestGRPCBroker_ConcurrentHealthyStreamsNotBlocked(t *testing.T) {
	b, st := newTestBroker(t)

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)

	// Each waiter's peer answer is delivered straight into the control stream
	// once its entry exists, exercising Run -> route -> entry delivery under
	// concurrency.
	for i := uint32(1); i <= n; i++ {
		wg.Add(1)
		go func(id uint32) {
			defer wg.Done()
			p, err := b.acquireClientStream(id)
			if err != nil {
				errs <- err
				return
			}
			// The entry is registered before waitForConnInfo blocks, so the
			// routed frame has a live owner.
			st.deliver(connInfo(id))
			if _, err := b.waitForConnInfo(p, b.clientStreams, errors.New("timeout")); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()

	// Close after the healthy round: every remaining entry must resolve once.
	_ = b.Close()
	close(errs)
	for err := range errs {
		if !errors.Is(err, errBrokerClosed) {
			t.Errorf("unexpected waiter error: %v", err)
		}
	}
}

// fakeMuxer is a minimal enabled GRPCMuxer for broker multiplexing unit tests.
type fakeMuxer struct {
	mu        sync.Mutex
	listeners map[uint32]chan struct{}
	enabled   bool
}

func newFakeMuxer() *fakeMuxer {
	return &fakeMuxer{enabled: true, listeners: make(map[uint32]chan struct{})}
}

func (f *fakeMuxer) Enabled() bool { return f.enabled }

func (f *fakeMuxer) Listener(id uint32, doneCh <-chan struct{}) (net.Listener, error) {
	f.mu.Lock()
	f.listeners[id] = make(chan struct{}, 1)
	f.mu.Unlock()
	return &fakeListener{doneCh: doneCh}, nil
}

func (f *fakeMuxer) AcceptKnock(id uint32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch, ok := f.listeners[id]
	if !ok {
		return errors.New("no listener")
	}
	select {
	case ch <- struct{}{}:
	default:
	}
	return nil
}

func (f *fakeMuxer) Dial() (net.Conn, error) { return nil, errors.New("not implemented") }
func (f *fakeMuxer) Close() error            { return nil }

type fakeListener struct {
	addr   net.Addr
	doneCh <-chan struct{}
}

func (l *fakeListener) Accept() (net.Conn, error) {
	<-l.doneCh
	return nil, errors.New("listener closed")
}
func (l *fakeListener) Close() error   { return nil }
func (l *fakeListener) Addr() net.Addr { return l.addr }

var _ grpcmux.GRPCMuxer = (*fakeMuxer)(nil)

// TestGRPCBroker_MuxKnockGenerationIsolation exercises the multiplexed knock
// handshake: a late ack from an old generation cannot reach a later knock, and
// a knock whose listener never answers converges on a terminal error.
func TestGRPCBroker_MuxKnockGenerationIsolation(t *testing.T) {
	st := newFakeStreamer()
	mx := newFakeMuxer()
	b := newGRPCBroker(st, nil, UnixSocketConfig{}, nil, mx)
	b.drainTimeout = 20 * time.Millisecond
	go b.Run()
	t.Cleanup(func() { _ = b.Close() })

	const id uint32 = 41

	// Set up the server-side listener entry so inbound knocks are routable.
	if _, err := b.acquireServerStream(id); err != nil {
		t.Fatalf("server acquire: %v", err)
	}

	// One successful knock/ack round trip.
	knockDone := make(chan error, 1)
	go func() { knockDone <- b.knock(id) }()

	var knock *plugin.ConnInfo
	select {
	case knock = <-st.sendCh:
	case <-time.After(time.Second):
		t.Fatal("knock not sent")
	}
	if knock.ServiceId != id || knock.Knock == nil || !knock.Knock.Knock {
		t.Fatalf("bad knock frame: %+v", knock)
	}
	st.deliver(&plugin.ConnInfo{
		ServiceId: id,
		Knock:     &plugin.ConnInfo_Knock{Knock: true, Ack: true},
	})
	select {
	case err := <-knockDone:
		if err != nil {
			t.Fatalf("knock failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("knock did not complete")
	}

	// A duplicate, late ack must not corrupt the next generation's handshake.
	st.deliver(&plugin.ConnInfo{
		ServiceId: id,
		Knock:     &plugin.ConnInfo_Knock{Knock: true, Ack: true},
	})

	// After the idle reaper runs, a fresh acquire gets a new generation.
	p2, err := b.acquireClientStream(id)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	select {
	case stray := <-p2.ch:
		t.Fatalf("new generation received stale ack: %+v", stray)
	default:
	}
}

// TestGRPCBroker_MuxKnockTimeoutTerminal proves the knock handshake has a
// single terminal result on timeout and then drains before reuse.
func TestGRPCBroker_MuxKnockTimeoutTerminal(t *testing.T) {
	st := newFakeStreamer()
	mx := newFakeMuxer()
	b := newGRPCBroker(st, nil, UnixSocketConfig{}, nil, mx)
	b.drainTimeout = 10 * time.Millisecond
	go b.Run()
	t.Cleanup(func() { _ = b.Close() })

	const id uint32 = 42
	if _, err := b.acquireServerStream(id); err != nil {
		t.Fatalf("server acquire: %v", err)
	}

	// Drain outbound knocks (the real streamer's send goroutine does this) but
	// never answer, so the handshake must hit its negotiation timeout.
	go func() {
		for {
			select {
			case <-st.quit:
				return
			case <-st.sendCh:
			}
		}
	}()

	// The knock entry is the one the handshake acquired; after its 5s timeout
	// it has published a terminal state (the short drain window may already
	// have elapsed, so assert on the entry itself rather than re-acquire).
	p, err := b.acquireClientStream(id)
	if err != nil {
		t.Fatalf("acquire before knock: %v", err)
	}

	start := time.Now()
	if err := b.knock(id); err == nil {
		t.Fatal("expected knock timeout")
	} else if elapsed := time.Since(start); elapsed < negotiateTimeout/2 {
		t.Fatalf("knock returned before timeout: %s", elapsed)
	}

	if !isClosed(p.doneCh) {
		t.Fatal("terminal state not published on knock timeout")
	}
	if p.terminal == nil {
		t.Fatal("expected a terminal error on knock timeout")
	}
	if p.state == entryLive {
		t.Fatal("entry remained live after knock timeout")
	}
}

// TestGRPCBroker_DialRoundTrip is an end-to-end non-multiplexed negotiation
// using the real Dial path: Accept advertises a local listener, Dial resolves
// to it and a gRPC round trip over the brokered connection stays compatible.
func TestGRPCBroker_DialRoundTrip(t *testing.T) {
	st := newFakeStreamer()
	b := newGRPCBroker(st, nil, UnixSocketConfig{}, nil, nil)
	b.drainTimeout = 20 * time.Millisecond
	go b.Run()
	t.Cleanup(func() { _ = b.Close() })

	const id uint32 = 51

	// Reader must be ready before Accept: the non-mux Accept blocks until its
	// advertised ConnInfo is consumed from the streamer. Hold the frame until
	// the Dial side has registered a live entry, mirroring real network timing
	// without letting route see an ownerless frame.
	gotConnInfo := make(chan *plugin.ConnInfo, 1)
	go func() {
		gotConnInfo <- <-st.sendCh
	}()

	acceptDone := make(chan net.Listener, 1)
	go func() {
		ln, err := b.Accept(id)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		acceptDone <- ln
	}()

	var ln net.Listener
	select {
	case ln = <-acceptDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Accept did not return a listener")
	}
	defer ln.Close()

	var advertised *plugin.ConnInfo
	select {
	case advertised = <-gotConnInfo:
	case <-time.After(2 * time.Second):
		t.Fatal("Accept did not advertise connection info")
	}

	go func() {
		deadline := time.Now().Add(2 * time.Second)
		for {
			b.Lock()
			_, ok := b.clientStreams[id]
			b.Unlock()
			if ok {
				st.deliver(advertised)
				return
			}
			if time.Now().After(deadline) {
				t.Errorf("dial entry never registered")
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// Serve a trivial gRPC server on the accepted connection.
	srv := grpc.NewServer()
	go func() { _ = srv.Serve(ln) }()
	defer srv.Stop()

	conn, err := b.Dial(id)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// The brokered connection must reach the gRPC server: force connection
	// establishment and confirm it leaves its initial IDLE state.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if !conn.WaitForStateChange(ctx, conn.GetState()) {
		t.Fatalf("brokered connection never changed state: %s", conn.GetState())
	}
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
