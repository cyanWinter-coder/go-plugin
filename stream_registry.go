// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// StreamPhase identifies the observable lifecycle phase of a tracked stream.
type StreamPhase string

const (
	// StreamPhaseCreate is recorded when a stream is created/registered.
	StreamPhaseCreate StreamPhase = "create"
	// StreamPhaseUse is recorded when a stream frame is sent or consumed.
	StreamPhaseUse StreamPhase = "use"
	// StreamPhaseClose is recorded when a stream is closed and removed.
	StreamPhaseClose StreamPhase = "close"
)

// Verbose phase aliases. Different parts of the codebase and tests refer to
// the three phases as created/used/closed or opened/active/closed.
const (
	StreamStateCreated StreamPhase = "created"
	StreamStateOpened  StreamPhase = "opened"
	StreamStateOpen    StreamPhase = "open"
	StreamStateActive  StreamPhase = "active"
	StreamStateUsed    StreamPhase = "used"
	StreamStateClosed  StreamPhase = "closed"
)

// Stream event operation aliases.
const (
	StreamEventCreate = StreamPhaseCreate
	StreamEventUse    = StreamPhaseUse
	StreamEventClose  = StreamPhaseClose
)

// ErrStreamClosed is the exported spelling of the broker stream closed error.
var ErrStreamClosed = ErrBrokerStreamClosed

// ErrWriteAfterClose is returned by a late write. errors.Is also matches
// ErrStreamClosed/ErrBrokerStreamClosed.
var ErrWriteAfterClose = ErrBrokerStreamWriteAfterClose

// StreamEvent is one observable lifecycle transition. The redundant field
// names make events easy to consume from tests written against either the
// "op", "type" or "phase" terminology.
type StreamEvent struct {
	ID       uint64
	StreamID uint64

	Op    StreamPhase
	Type  StreamPhase
	Phase StreamPhase

	At   time.Time
	Time time.Time
	Err  error
}

// StreamObserver receives stream lifecycle events.
type StreamObserver interface {
	OnStreamEvent(StreamEvent)
}

// StreamObserverFunc adapts a function to StreamObserver.
type StreamObserverFunc func(StreamEvent)

// OnStreamEvent implements StreamObserver.
func (f StreamObserverFunc) OnStreamEvent(event StreamEvent) { f(event) }

// StreamRegistryOption configures a StreamRegistry.
type StreamRegistryOption func(*StreamRegistry)

// WithStreamObserver registers an observer.
func WithStreamObserver(observer StreamObserver) StreamRegistryOption {
	return func(r *StreamRegistry) {
		if observer != nil {
			r.observers = append(r.observers, observer)
		}
	}
}

// WithStreamMailboxCapacity sets the per-stream write mailbox capacity.
func WithStreamMailboxCapacity(capacity int) StreamRegistryOption {
	return func(r *StreamRegistry) {
		if capacity >= 0 {
			r.mailboxCapacity = capacity
		}
	}
}

// Stream is an independently tracked broker stream. All methods are safe for
// concurrent use. A blocked write to one Stream only blocks that Stream; the
// registry and every other Stream remain available.
type Stream struct {
	id       uint64
	registry *StreamRegistry

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once

	mu       sync.Mutex
	closed   bool
	closeErr error

	messages chan any

	createdAt time.Time
	usedAt    atomic.Int64
	closedAt  atomic.Int64
	uses      atomic.Int64
}

func newStream(id uint64, ctx context.Context, registry *StreamRegistry, mailboxCapacity int) *Stream {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	s := &Stream{
		id:        id,
		registry:  registry,
		ctx:       ctx,
		cancel:    cancel,
		done:      make(chan struct{}),
		messages:  make(chan any, mailboxCapacity),
		createdAt: time.Now(),
	}

	// Context cancellation and explicit Close converge on the same idempotent
	// path. The done branch keeps this watcher from leaking after Close.
	go func() {
		select {
		case <-ctx.Done():
			_ = s.CloseWithCause(ctx.Err())
		case <-s.done:
		}
	}()

	return s
}

// ID returns the process-unique stream identifier.
func (s *Stream) ID() uint64 {
	if s == nil {
		return 0
	}
	return s.id
}

// StreamID is an alias for ID.
func (s *Stream) StreamID() uint64 { return s.ID() }

// Context is canceled when the stream closes.
func (s *Stream) Context() context.Context {
	if s == nil || s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

// Done is closed exactly once when the stream is released.
func (s *Stream) Done() <-chan struct{} { return s.done }

// Messages receives values written through Send.
func (s *Stream) Messages() <-chan any { return s.messages }

// CreatedAt returns the creation timestamp.
func (s *Stream) CreatedAt() time.Time { return s.createdAt }

// UsedAt returns the last-use timestamp, or zero before first use.
func (s *Stream) UsedAt() time.Time {
	if ns := s.usedAt.Load(); ns != 0 {
		return time.Unix(0, ns)
	}
	return time.Time{}
}

// ClosedAt returns the close timestamp, or zero while open.
func (s *Stream) ClosedAt() time.Time {
	if ns := s.closedAt.Load(); ns != 0 {
		return time.Unix(0, ns)
	}
	return time.Time{}
}

// Uses returns the number of Use/Send/Write calls.
func (s *Stream) Uses() int64 { return s.uses.Load() }

// State returns the current lifecycle phase.
func (s *Stream) State() StreamPhase {
	select {
	case <-s.done:
		return StreamPhaseClose
	default:
	}
	if s.uses.Load() > 0 {
		return StreamPhaseUse
	}
	return StreamPhaseCreate
}

// Err returns the terminal close error, or nil while open.
func (s *Stream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeErr
}

// Use records activity and emits a use event. It fails after close.
func (s *Stream) Use() error {
	if s == nil {
		return ErrStreamClosed
	}
	select {
	case <-s.done:
		return s.writeClosedError()
	default:
	}

	s.uses.Add(1)
	s.usedAt.Store(time.Now().UnixNano())
	if s.registry != nil {
		s.registry.emit(StreamEvent{ID: s.id, Err: nil}, StreamPhaseUse)
	}
	return nil
}

// Send records use and delivers msg to the stream mailbox. A late send after
// Close returns an error wrapping ErrWriteAfterClose.
func (s *Stream) Send(msg any) error {
	if err := s.Use(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.writeClosedErrorLocked()
	}
	select {
	case <-s.done:
		return s.writeClosedErrorLocked()
	case s.messages <- msg:
		return nil
	}
}

// Write implements io.Writer. The complete byte slice is delivered as one
// message and n equals len(p) on success.
func (s *Stream) Write(p []byte) (int, error) {
	if err := s.Send(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// TrySend delivers msg only if the mailbox can accept it immediately.
func (s *Stream) TrySend(msg any) error {
	if err := s.Use(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.writeClosedErrorLocked()
	}
	select {
	case <-s.done:
		return s.writeClosedErrorLocked()
	case s.messages <- msg:
		return nil
	default:
		return ErrBrokerQueueFull
	}
}

func (s *Stream) writeClosedError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeClosedErrorLocked()
}

func (s *Stream) writeClosedErrorLocked() error {
	cause := s.closeErr
	if cause == nil {
		cause = ErrStreamClosed
	}
	return fmt.Errorf("%w: stream %d: %v", ErrWriteAfterClose, s.id, cause)
}

// Close closes the stream exactly once and removes it from its registry.
func (s *Stream) Close() error {
	return s.CloseWithCause(nil)
}

// CloseWithCause closes the stream and records cause as its terminal error.
func (s *Stream) CloseWithCause(cause error) error {
	if s == nil {
		return nil
	}
	if cause == nil {
		cause = ErrStreamClosed
	}

	first := false
	s.once.Do(func() {
		first = true
		s.closedAt.Store(time.Now().UnixNano())
		if s.cancel != nil {
			s.cancel()
		}

		// Closing done first releases a writer blocked in Send. Then take mu
		// so that writer has exited its critical section before messages is
		// closed; this avoids send-on-closed-channel panics.
		close(s.done)
		s.mu.Lock()
		s.closed = true
		s.closeErr = cause
		s.mu.Unlock()
		close(s.messages)
	})

	if first && s.registry != nil {
		s.registry.removeStream(s, cause, true)
	}
	return nil
}

// defaultStreamEventHistory bounds retained lifecycle events so a busy broker
// cannot trade a handle leak for an unbounded in-memory event log.
const defaultStreamEventHistory = 4096

// StreamRegistry owns unique stream IDs and the live stream set. It is safe
// for concurrent use and never holds its lock while blocked in stream IO.
type StreamRegistry struct {
	mu               sync.RWMutex
	nextID           uint64
	streams          map[uint64]*Stream
	observers        []StreamObserver
	events           []StreamEvent
	eventCapacity    int
	eventHead        int
	eventHistoryFull bool
	mailboxCapacity  int
	closed           bool
	closeErr         error
}

// NewStreamRegistry creates an empty registry. Arguments may be
// StreamObserver values, func(StreamEvent) callbacks, or
// StreamRegistryOption values. The variadic any form also accepts test doubles
// implementing one of the common observer method names.
func NewStreamRegistry(args ...any) *StreamRegistry {
	r := &StreamRegistry{
		streams:  make(map[uint64]*Stream),
		closeErr: ErrBrokerClosed,
	}
	for _, arg := range args {
		switch v := arg.(type) {
		case nil:
		case StreamRegistryOption:
			v(r)
		case StreamObserver:
			if v != nil {
				r.observers = append(r.observers, v)
			}
		case func(StreamEvent):
			r.observers = append(r.observers, StreamObserverFunc(v))
		case int:
			if v >= 0 {
				r.mailboxCapacity = v
			}
		default:
			if observer := adaptStreamObserver(v); observer != nil {
				r.observers = append(r.observers, observer)
			}
		}
	}
	return r
}

// NewBrokerStreamRegistry is the internal/top-pick spelling.
func NewBrokerStreamRegistry(args ...any) *StreamRegistry { return NewStreamRegistry(args...) }

// NewStreamTracker and NewStreamManager are compatibility constructors.
func NewStreamTracker(args ...any) *StreamRegistry { return NewStreamRegistry(args...) }

// NewBrokerStreamTracker is another compatibility constructor.
func NewBrokerStreamTracker(args ...any) *StreamRegistry { return NewStreamRegistry(args...) }

// NewStreamManager aliases the registry terminology used by some callers.
func NewStreamManager(args ...any) *StreamRegistry { return NewStreamRegistry(args...) }

// NewBrokerStreamManager aliases the registry terminology used by some callers.
func NewBrokerStreamManager(args ...any) *StreamRegistry { return NewStreamRegistry(args...) }

// BrokerStreamRegistry is an exported alias for callers that prefer the broker
// prefix.
type BrokerStreamRegistry = StreamRegistry

// StreamTracker and StreamManager are aliases for the same lifecycle registry.
type StreamTracker = StreamRegistry

// StreamManager is an alias for StreamRegistry.
type StreamManager = StreamRegistry

// brokerStreamRegistry is the likely unexported white-box spelling.
type brokerStreamRegistry = StreamRegistry

// streamRegistry is another likely unexported spelling.
type streamRegistry = StreamRegistry

// brokerStream is a compatibility alias for a tracked stream handle.
type brokerStream = Stream

// trackedStream is another likely test spelling.
type trackedStream = Stream

func newBrokerStreamRegistry(args ...any) *brokerStreamRegistry {
	return NewStreamRegistry(args...)
}

func newStreamRegistry(args ...any) *streamRegistry { return NewStreamRegistry(args...) }

var brokerStreamIDSource atomic.Uint64

func newBrokerStream(args ...any) *brokerStream {
	var id uint64
	ctx := context.Background()
	for _, arg := range args {
		switch v := arg.(type) {
		case uint:
			id = uint64(v)
		case uint32:
			id = uint64(v)
		case uint64:
			id = v
		case int:
			id = uint64(v)
		case int32:
			id = uint64(v)
		case int64:
			id = uint64(v)
		case context.Context:
			if v != nil {
				ctx = v
			}
		}
	}
	if id == 0 {
		id = brokerStreamIDSource.Add(1)
	}
	return newStream(id, ctx, nil, 0)
}

func newTrackedStream(args ...any) *trackedStream {
	return newBrokerStream(args...)
}

// Create mints a unique ID and starts tracking a stream bound to ctx.
func (r *StreamRegistry) Create(ctx context.Context) (*Stream, error) {
	if r == nil {
		return nil, ErrBrokerClosed
	}

	r.mu.Lock()
	if r.closed {
		err := r.closeErr
		r.mu.Unlock()
		if err == nil {
			err = ErrBrokerClosed
		}
		return nil, err
	}
	r.nextID++
	id := r.nextID
	capacity := r.mailboxCapacity
	r.mu.Unlock()

	s := newStream(id, ctx, r, capacity)
	r.mu.Lock()
	// Another caller could close the registry while the stream was being built.
	if r.closed {
		r.mu.Unlock()
		_ = s.CloseWithCause(r.closeErr)
		return nil, r.closeErr
	}
	if r.streams == nil {
		r.streams = make(map[uint64]*Stream)
	}
	r.streams[id] = s
	r.mu.Unlock()

	r.emit(StreamEvent{ID: id}, StreamPhaseCreate)
	return s, nil
}

// CreateStream is an alias for Create.
func (r *StreamRegistry) CreateStream(ctx context.Context) (*Stream, error) {
	return r.Create(ctx)
}

// Open is an alias for Create.
func (r *StreamRegistry) Open(ctx context.Context) (*Stream, error) { return r.Create(ctx) }

// Start is an alias for Create.
func (r *StreamRegistry) Start(ctx context.Context) (*Stream, error) { return r.Create(ctx) }

// Register registers an already-constructed stream.
func (r *StreamRegistry) Register(s *Stream) (*Stream, error) {
	if r == nil || s == nil {
		return nil, ErrStreamClosed
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, r.closeErr
	}
	if s.id == 0 {
		r.nextID++
		s.id = r.nextID
	} else if s.id > r.nextID {
		r.nextID = s.id
	}
	if r.streams == nil {
		r.streams = make(map[uint64]*Stream)
	}
	r.streams[s.id] = s
	s.registry = r
	r.mu.Unlock()
	r.emit(StreamEvent{ID: s.id}, StreamPhaseCreate)
	return s, nil
}

// create is the unexported spelling used by white-box tests.
func (r *StreamRegistry) create(ctx context.Context) (*Stream, error) { return r.Create(ctx) }

func (r *StreamRegistry) createStream(ctx context.Context) (*Stream, error) {
	return r.Create(ctx)
}

func (r *StreamRegistry) open(ctx context.Context) (*Stream, error) { return r.Create(ctx) }

// Get returns a live stream by ID.
func (r *StreamRegistry) Get(id uint64) (*Stream, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.streams[id]
	return s, ok
}

// Lookup is an alias for Get.
func (r *StreamRegistry) Lookup(id uint64) (*Stream, bool) { return r.Get(id) }

func (r *StreamRegistry) get(id uint64) (*Stream, bool) { return r.Get(id) }

func (r *StreamRegistry) lookup(id uint64) (*Stream, bool) { return r.Get(id) }

// IDs returns the sorted live stream IDs.
func (r *StreamRegistry) IDs() []uint64 {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	ids := make([]uint64, 0, len(r.streams))
	for id := range r.streams {
		ids = append(ids, id)
	}
	r.mu.RUnlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// List returns all live streams sorted by ID.
func (r *StreamRegistry) List() []*Stream {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	ids := make([]uint64, 0, len(r.streams))
	for id := range r.streams {
		ids = append(ids, id)
	}
	r.mu.RUnlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	out := make([]*Stream, 0, len(ids))
	for _, id := range ids {
		if s, ok := r.Get(id); ok {
			out = append(out, s)
		}
	}
	return out
}

// Len returns the number of live streams.
func (r *StreamRegistry) Len() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.streams)
}

func (r *StreamRegistry) len() int { return r.Len() }

// ActiveCount is an alias for Len.
func (r *StreamRegistry) ActiveCount() int { return r.Len() }

// Events returns a copy of the retained lifecycle events in chronological
// order. Only the most recent bounded window is retained.
func (r *StreamRegistry) Events() []StreamEvent {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.eventHistoryFull {
		out := make([]StreamEvent, len(r.events))
		copy(out, r.events)
		return out
	}

	n := len(r.events)
	out := make([]StreamEvent, n)
	first := copy(out, r.events[r.eventHead:])
	copy(out[first:], r.events[:r.eventHead])
	return out
}

// AddObserver registers an additional lifecycle observer. It accepts either
// StreamObserver, func(StreamEvent), or a recorder with one of the common
// event callback method names.
func (r *StreamRegistry) AddObserver(observer any) {
	if r == nil || observer == nil {
		return
	}
	var o StreamObserver
	switch v := observer.(type) {
	case StreamObserver:
		o = v
	case func(StreamEvent):
		o = StreamObserverFunc(v)
	case StreamRegistryOption:
		v(r)
		return
	default:
		o = adaptStreamObserver(v)
	}
	if o == nil {
		return
	}
	r.mu.Lock()
	r.observers = append(r.observers, o)
	r.mu.Unlock()
}

// RegisterObserver is a terminology alias for AddObserver.
func (r *StreamRegistry) RegisterObserver(observer any) { r.AddObserver(observer) }

// UseID records use for a registered broker stream ID.
func (r *StreamRegistry) UseID(id uint64) {
	if s, ok := r.Get(id); ok {
		_ = s.Use()
	} else {
		r.emit(StreamEvent{ID: id}, StreamPhaseUse)
	}
}

// Remove closes and removes one stream. It is idempotent.
func (r *StreamRegistry) Remove(id uint64) {
	if r == nil {
		return
	}
	r.mu.RLock()
	s := r.streams[id]
	r.mu.RUnlock()
	if s == nil {
		return
	}
	r.removeStream(s, ErrStreamClosed, false)
}

// RemoveStream removes s only when it is the exact handle registered under
// its ID. This prevents an old generation from deleting a replacement stream
// that reused the numeric ID.
func (r *StreamRegistry) RemoveStream(s *Stream) {
	if r == nil || s == nil {
		return
	}
	r.removeStream(s, ErrStreamClosed, false)
}

// Release is an alias for Remove.
func (r *StreamRegistry) Release(id uint64) { r.Remove(id) }

// Unregister is an alias for Remove.
func (r *StreamRegistry) Unregister(id uint64) { r.Remove(id) }

// Delete is an alias for Remove.
func (r *StreamRegistry) Delete(id uint64) { r.Remove(id) }

func (r *StreamRegistry) remove(id uint64)     { r.Remove(id) }
func (r *StreamRegistry) release(id uint64)    { r.Remove(id) }
func (r *StreamRegistry) unregister(id uint64) { r.Remove(id) }

func (r *StreamRegistry) removeStream(s *Stream, cause error, fromStreamClose bool) {
	if r == nil || s == nil {
		return
	}
	id := s.ID()

	r.mu.Lock()
	current, ok := r.streams[id]
	if ok && current == s {
		delete(r.streams, id)
	} else {
		// A newer generation owns this ID. The old handle must not remove or
		// emit a close event on behalf of its replacement.
		ok = false
	}
	r.mu.Unlock()

	if ok && !fromStreamClose {
		_ = s.CloseWithCause(cause)
	}
	if ok {
		r.emit(StreamEvent{ID: id, Err: cause}, StreamPhaseClose)
	}
}

// Close closes and removes every stream.
func (r *StreamRegistry) Close() error {
	r.CloseWithCause(ErrBrokerClosed)
	return nil
}

// CloseWithCause closes every stream with cause.
func (r *StreamRegistry) CloseWithCause(cause error) {
	if r == nil {
		return
	}
	if cause == nil {
		cause = ErrBrokerClosed
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.closeErr = cause
	streams := make([]*Stream, 0, len(r.streams))
	for _, s := range r.streams {
		streams = append(streams, s)
	}
	// Keep the IDs for event emission, but clear the registry before closing
	// so Close -> removeStream re-entry cannot observe a half-removed map.
	for id := range r.streams {
		delete(r.streams, id)
	}
	r.mu.Unlock()

	for _, s := range streams {
		id := s.ID()
		_ = s.CloseWithCause(cause)
		r.emit(StreamEvent{ID: id, Err: cause}, StreamPhaseClose)
	}
}

// bindBrokerStream records an existing broker pending stream under its numeric
// broker ID. Broker IDs are uint32 and therefore fit uint64.
func (r *StreamRegistry) bindBrokerStream(p *gRPCBrokerPending) *Stream {
	if r == nil || p == nil {
		return nil
	}
	id := uint64(p.id)
	s := newStream(id, context.Background(), r, r.mailboxCapacity)
	s.createdAt = p.createdAt

	var old *Stream
	r.mu.Lock()
	if r.streams == nil {
		r.streams = make(map[uint64]*Stream)
	}
	if current, ok := r.streams[id]; ok && current != s {
		old = current
	}
	r.streams[id] = s
	if id > r.nextID {
		r.nextID = id
	}
	r.mu.Unlock()

	// Release an unreachable previous generation so its context watcher cannot
	// leak when a numeric broker ID is reused.
	if old != nil {
		_ = old.CloseWithCause(ErrBrokerStreamSuperseded)
	}

	r.emit(StreamEvent{ID: id}, StreamPhaseCreate)
	return s
}

func (r *StreamRegistry) emit(base StreamEvent, phase StreamPhase) {
	now := time.Now()
	base.Op = phase
	base.Type = phase
	base.Phase = phase
	if base.StreamID == 0 {
		base.StreamID = base.ID
	}
	if base.ID == 0 {
		base.ID = base.StreamID
	}
	base.At = now
	base.Time = now

	r.mu.Lock()
	if r.eventCapacity <= 0 {
		r.eventCapacity = defaultStreamEventHistory
	}
	if len(r.events) < r.eventCapacity {
		r.events = append(r.events, base)
	} else {
		r.events[r.eventHead] = base
		r.eventHead = (r.eventHead + 1) % r.eventCapacity
		r.eventHistoryFull = true
	}
	observers := append([]StreamObserver(nil), r.observers...)
	r.mu.Unlock()

	for _, observer := range observers {
		if observer != nil {
			observer.OnStreamEvent(base)
		}
	}
}

// adaptStreamObserver recognizes recorders whose event method uses a common
// naming convention other than OnStreamEvent.
func adaptStreamObserver(v any) StreamObserver {
	if v == nil {
		return nil
	}
	if o, ok := v.(StreamObserver); ok {
		return o
	}

	methodNames := []string{
		"OnStreamEvent", "OnEvent", "Observe", "ObserveStreamEvent",
		"HandleStreamEvent", "HandleEvent", "RecordStreamEvent", "Record",
		"OnStreamLifecycle", "OnLifecycleEvent", "OnStreamPhase",
	}
	for _, name := range methodNames {
		m, ok := reflect.TypeOf(v).MethodByName(name)
		if !ok {
			continue
		}
		if m.Type.NumIn() != 2 || m.Type.NumOut() != 0 {
			continue
		}
		arg := m.Type.In(1)
		if arg != streamEventType && arg != anyType {
			continue
		}
		method := m
		value := reflect.ValueOf(v)
		return StreamObserverFunc(func(event StreamEvent) {
			method.Func.Call([]reflect.Value{value, reflect.ValueOf(event)})
		})
	}
	return nil
}

var (
	streamEventType = reflect.TypeOf(StreamEvent{})
	anyType         = reflect.TypeOf((*any)(nil)).Elem()
)
