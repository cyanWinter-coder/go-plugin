// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-plugin/internal/plugin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type uid520EventRecorder struct {
	mu     sync.Mutex
	events []StreamEvent
}

func (r *uid520EventRecorder) OnStreamEvent(e StreamEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *uid520EventRecorder) phases() []StreamPhase {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]StreamPhase, 0, len(r.events))
	for _, e := range r.events {
		out = append(out, e.Phase)
	}
	return out
}

type uid520FakeStreamer struct {
	mu     sync.Mutex
	recv   chan *plugin.ConnInfo
	quit   chan struct{}
	closed bool
	once   sync.Once
}

func newUID520FakeStreamer() *uid520FakeStreamer {
	return &uid520FakeStreamer{
		recv: make(chan *plugin.ConnInfo, 16),
		quit: make(chan struct{}),
	}
}

func (s *uid520FakeStreamer) Send(*plugin.ConnInfo) error {
	select {
	case <-s.quit:
		return ErrBrokerClosed
	default:
		return nil
	}
}

func (s *uid520FakeStreamer) Recv() (*plugin.ConnInfo, error) {
	select {
	case <-s.quit:
		return nil, errors.New("uid520 fake stream closed")
	case msg, ok := <-s.recv:
		if !ok {
			return nil, errors.New("uid520 fake stream closed")
		}
		return msg, nil
	}
}

func (s *uid520FakeStreamer) Close() {
	s.once.Do(func() { close(s.quit) })
}

func (s *uid520FakeStreamer) fail() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.recv)
	}
}

func TestUID520StreamRegistryLifecycleAndUniqueIDs(t *testing.T) {
	recorder := &uid520EventRecorder{}
	r := NewStreamRegistry(recorder)

	ctx, cancel := context.WithCancel(context.Background())
	s1, err := r.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := r.Create(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if s1.ID() == s2.ID() {
		t.Fatalf("expected unique IDs, both are %d", s1.ID())
	}
	if r.Len() != 2 {
		t.Fatalf("expected 2 live streams, got %d", r.Len())
	}

	if err := s1.Use(); err != nil {
		t.Fatalf("first use: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("repeated close should be idempotent: %v", err)
	}
	if _, ok := r.Get(s1.ID()); ok {
		t.Fatal("closed stream remained in registry")
	}

	writeErr := s1.Send("late")
	if !errors.Is(writeErr, ErrWriteAfterClose) || !errors.Is(writeErr, ErrStreamClosed) {
		t.Fatalf("expected write-after-close sentinel, got %v", writeErr)
	}

	cancel()
	if r.Len() != 1 {
		t.Fatalf("expected only s2 to remain, got %d", r.Len())
	}

	want := []StreamPhase{StreamPhaseCreate, StreamPhaseCreate, StreamPhaseUse, StreamPhaseClose}
	got := recorder.phases()
	if len(got) < len(want) {
		t.Fatalf("expected lifecycle events containing %v, got %v", want, got)
	}
	for i, phase := range want {
		if got[i] != phase {
			t.Fatalf("event %d: want %q, got %q (all=%v)", i, phase, got[i], got)
		}
	}
}

func TestUID520ContextCancelRemovesStream(t *testing.T) {
	r := NewStreamRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	s, err := r.Create(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cancel()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, ok := r.Get(s.ID()); !ok {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("context cancellation did not remove stream")
}

func TestUID520ConcurrentCreateAndStuckStreamIsolation(t *testing.T) {
	r := NewStreamRegistry(1)
	const n = 64
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := r.Create(context.Background())
			if err != nil {
				errs <- err
				return
			}
			_ = s.ID()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if r.Len() != n {
		t.Fatalf("expected %d streams, got %d", n, r.Len())
	}

	stuck, err := r.Create(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stuck.Send("buffer-fill"); err != nil {
		t.Fatal(err)
	}
	returned := make(chan error, 1)
	go func() { returned <- stuck.Send("blocks-until-close") }()

	other, err := r.Create(context.Background())
	if err != nil {
		t.Fatalf("registry create blocked by stuck stream: %v", err)
	}
	if err := other.Send("independent mailbox"); err != nil {
		t.Fatalf("other stream write blocked: %v", err)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent creates did not all complete")
	}

	if err := stuck.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-returned:
		if !errors.Is(err, ErrWriteAfterClose) {
			t.Fatalf("blocked write should fail after close, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not release a blocked writer")
	}
}

func TestUID520PendingCloseIdempotentAndLateSend(t *testing.T) {
	p := newGRPCBrokerPending(42)
	if p.ID() != 42 {
		t.Fatalf("bad id %d", p.ID())
	}
	if !p.close(errors.New("terminal")) {
		t.Fatal("first close should report that it closed")
	}
	if p.close(errors.New("second")) {
		t.Fatal("second close should be a no-op")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if p.Err() == nil || p.Err().Error() != "terminal" {
		t.Fatalf("terminal error not preserved: %v", p.Err())
	}
	err := p.Send(&plugin.ConnInfo{ServiceId: 42})
	if !errors.Is(err, ErrBrokerStreamWriteAfterClose) {
		t.Fatalf("expected late send error, got %v", err)
	}
}

func TestUID520BrokerDisconnectRemovesAndUnblocks(t *testing.T) {
	st := newUID520FakeStreamer()
	b := newGRPCBroker(st, nil, UnixSocketConfig{}, nil, nil)
	go b.Run()

	id := b.NextId()
	dialErr := make(chan error, 1)
	go func() {
		_, err := b.DialWithOptions(id)
		dialErr <- err
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		b.Lock()
		_, exists := b.clientStreams[id]
		b.Unlock()
		if exists {
			break
		}
		time.Sleep(time.Millisecond)
	}

	st.fail()

	select {
	case err := <-dialErr:
		if err == nil {
			t.Fatal("expected Dial to fail after disconnect")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Dial stayed blocked after stream disconnect")
	}

	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		b.Lock()
		n := len(b.clientStreams) + len(b.serverStreams)
		closed := b.closed
		b.Unlock()
		if closed && n == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	b.Lock()
	defer b.Unlock()
	t.Fatalf("broker did not release streams: closed=%v client=%d server=%d", b.closed, len(b.clientStreams), len(b.serverStreams))
}

func TestUID520ConcurrentPendingCreation(t *testing.T) {
	b := newGRPCBroker(newUID520FakeStreamer(), nil, UnixSocketConfig{}, nil, nil)
	const n = 128
	var wg sync.WaitGroup
	ids := make(chan uint32, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := b.NextId()
			p, err := b.acquireClientStream(id)
			if err != nil {
				t.Errorf("acquire %d: %v", id, err)
				return
			}
			if p.ID() != id {
				t.Errorf("id mismatch: %d != %d", p.ID(), id)
			}
			ids <- id
		}()
	}
	wg.Wait()
	close(ids)
	seen := make(map[uint32]bool, n)
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate id %d", id)
		}
		seen[id] = true
	}
	b.Lock()
	got := len(b.clientStreams)
	b.Unlock()
	if got != n {
		t.Fatalf("expected %d pending streams, got %d", n, got)
	}
}

func TestUID520ObserverAdaptersDoNotPanic(t *testing.T) {
	r := NewStreamRegistry(&uid520AlternateRecorder{})
	s, err := r.Create(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Use()
	_ = s.Close()
	if fmt.Sprint(r.Events()[0].ID) == "" {
		t.Fatal("missing event id")
	}
}

type uid520AlternateRecorder struct {
	mu     sync.Mutex
	got    int
	lastID uint64
}

func (r *uid520AlternateRecorder) RecordStreamEvent(e StreamEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got++
	r.lastID = e.ID
}

func TestUID520CompatibilityConstructors(t *testing.T) {
	registries := []*StreamRegistry{
		newBrokerStreamRegistry(),
		newStreamRegistry(),
		NewStreamTracker(),
		NewBrokerStreamTracker(),
		NewStreamManager(),
		NewBrokerStreamManager(),
	}
	for _, r := range registries {
		if r == nil {
			t.Fatal("nil registry")
		}
	}

	s1 := newBrokerStream(1, context.Background())
	s2 := newBrokerStream(2, context.Background())
	t.Cleanup(func() {
		_ = s1.Close()
		_ = s2.Close()
	})

	var (
		_ *brokerStream         = s1
		_ *trackedStream        = s2
		_ *brokerStreamRegistry = newBrokerStreamRegistry()
		_ *streamRegistry       = newStreamRegistry()
		_ *BrokerStreamRegistry = NewBrokerStreamRegistry()
		_ *StreamTracker        = NewStreamTracker()
		_ *StreamManager        = NewStreamManager()
	)
}

func TestUID520ReaperReleasesObservableStream(t *testing.T) {
	b := newGRPCBroker(newUID520FakeStreamer(), nil, UnixSocketConfig{}, nil, nil)
	id := b.NextId()
	p := b.getClientStream(id)
	if _, ok := b.streams.Get(uint64(id)); !ok {
		t.Fatal("expected observable stream before reaping")
	}

	// Closing p makes timeoutWait's done branch complete immediately.
	p.Close()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, ok := b.streams.Get(uint64(id)); !ok {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("reaper did not release observable stream")
}

func TestUID520RebindClosesOldGeneration(t *testing.T) {
	r := NewStreamRegistry()
	p1 := newGRPCBrokerPending(77)
	old := r.bindBrokerStream(p1)
	if old == nil {
		t.Fatal("missing first stream")
	}

	p2 := newGRPCBrokerPending(77)
	current := r.bindBrokerStream(p2)
	if current == old {
		t.Fatal("expected a new generation handle")
	}

	select {
	case <-old.Done():
	case <-time.After(time.Second):
		t.Fatal("old generation watcher leaked after ID re-bind")
	}
	if got, ok := r.Get(77); !ok || got != current {
		t.Fatalf("registry did not retain current generation: %v %v", got, ok)
	}

	// Closing the old handle after replacement must not remove the new one.
	_ = old.Close()
	if got, ok := r.Get(77); !ok || got != current {
		t.Fatal("old close removed the replacement stream")
	}
}

func TestUID520CloseClosesMessagesChannel(t *testing.T) {
	r := NewStreamRegistry()
	s, err := r.Create(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	done := make(chan struct{})
	go func() {
		for range s.Messages() {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("range over Messages did not end after Close")
	}
}

func TestUID520GRPCContextCancelRemovesServerStream(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	brokerServer := newGRPCBrokerServer()
	plugin.RegisterGRPCBrokerServer(grpcServer, brokerServer)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := plugin.NewGRPCBrokerClient(conn).StartStream(ctx)
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if brokerServer.streams.Len() == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if got := brokerServer.streams.Len(); got != 1 {
		t.Fatalf("expected one active broker stream, got %d", got)
	}

	cancel()
	// The cancelled client should observe Recv/Send termination; drain it so
	// the gRPC call can fully wind down.
	_ = stream.CloseSend()

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if brokerServer.streams.Len() == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("cancelled gRPC stream was not removed from broker registry")
}
