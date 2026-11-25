// Copyright IBM Corp. 2016, 2025
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-plugin/internal/grpcmux"
	"github.com/hashicorp/go-plugin/internal/plugin"
	"github.com/hashicorp/go-plugin/runner"

	"github.com/oklog/run"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	// ErrBrokerClosed is returned once the broker has been permanently closed.
	ErrBrokerClosed = errors.New("broker closed")

	// ErrBrokerStreamClosed is returned when an operation targets a stream
	// that has already been removed and released.
	ErrBrokerStreamClosed = errors.New("grpc broker stream closed")

	// ErrBrokerStreamWriteAfterClose is returned by a late write. It wraps
	// ErrBrokerStreamClosed so either sentinel can be used with errors.Is.
	ErrBrokerStreamWriteAfterClose = fmt.Errorf("%w: write after close", ErrBrokerStreamClosed)

	// ErrBrokerQueueFull is returned by a non-blocking write when the target
	// stream's mailbox is full.
	ErrBrokerQueueFull = errors.New("grpc broker stream queue full")

	// ErrBrokerStreamSuperseded is used when a reused stream ID binds a new
	// generation and releases the old handle.
	ErrBrokerStreamSuperseded = errors.New("grpc broker stream superseded")

	errBrokerClosed     = ErrBrokerClosed
	errStreamClosed     = ErrBrokerStreamClosed
	errWriteAfterClose  = ErrBrokerStreamWriteAfterClose
	errWaitTimeout      = errors.New("timeout waiting for connection info")
	errKnockAckTimeout  = errors.New("timeout waiting for multiplexing knock handshake")
	errPeerStreamClosed = errors.New("grpc broker peer stream closed")
)

// streamer interface is used in the broker to send/receive connection
// information.
type streamer interface {
	Send(*plugin.ConnInfo) error
	Recv() (*plugin.ConnInfo, error)
	Close()
}

// sendErr is used to pass errors back during a send.
type sendErr struct {
	i  *plugin.ConnInfo
	ch chan error
}

// gRPCBrokerServer is used by the plugin to start a stream and to send
// connection information to/from the plugin. Implements GRPCBrokerServer and
// streamer interfaces.
type gRPCBrokerServer struct {
	plugin.UnimplementedGRPCBrokerServer

	// send is used to send connection info to the gRPC stream.
	send chan *sendErr

	// recv is used to receive connection info from the gRPC stream.
	recv chan *plugin.ConnInfo

	// quit closes down the stream.
	quit chan struct{}

	// queueCapacity is the optional buffer size used by compatibility callers
	// that construct the server with an explicit capacity.
	queueCapacity int

	// streams observes per-RPC stream handles.
	streams *StreamRegistry

	// o is used to ensure we close the quit channel only once.
	o sync.Once
}

func newGRPCBrokerServer(capacity ...int) *gRPCBrokerServer {
	n := 0
	if len(capacity) > 0 && capacity[0] > 0 {
		n = capacity[0]
	}
	return &gRPCBrokerServer{
		send:          make(chan *sendErr, n),
		recv:          make(chan *plugin.ConnInfo, n),
		quit:          make(chan struct{}),
		queueCapacity: n,
		streams:       NewStreamRegistry(),
	}
}

// StartStream implements the GRPCBrokerServer interface and will block until
// the quit channel is closed or the context reports Done. The stream will pass
// connection information to/from the client.
func (s *gRPCBrokerServer) StartStream(stream plugin.GRPCBroker_StartStreamServer) error {
	doneCh := stream.Context().Done()
	handle, _ := s.streams.Create(stream.Context())
	defer func() { _ = handle.Close() }()
	defer s.Close()

	// Proccess send stream
	go func() {
		for {
			select {
			case <-doneCh:
				return
			case <-s.quit:
				return
			case se := <-s.send:
				err := stream.Send(se.i)
				if handle != nil {
					_ = handle.Use()
				}
				select {
				case se.ch <- err:
				case <-s.quit:
					return
				case <-doneCh:
					return
				}
			}
		}
	}()

	// Process receive stream
	for {
		i, err := stream.Recv()
		if err != nil {
			return err
		}
		if handle != nil {
			_ = handle.Use()
		}
		select {
		case <-doneCh:
			return nil
		case <-s.quit:
			return nil
		case s.recv <- i:
		}
	}
}

// Send is used by the GRPCBroker to pass connection information into the stream
// to the client.
func (s *gRPCBrokerServer) Send(i *plugin.ConnInfo) error {
	ch := make(chan error, 1)

	select {
	case <-s.quit:
		return ErrBrokerClosed
	case s.send <- &sendErr{
		i:  i,
		ch: ch,
	}:
	}

	select {
	case err := <-ch:
		return err
	case <-s.quit:
		return ErrBrokerClosed
	}
}

// Recv is used by the GRPCBroker to pass connection information that has been
// sent from the client from the stream to the broker.
func (s *gRPCBrokerServer) Recv() (*plugin.ConnInfo, error) {
	select {
	case <-s.quit:
		return nil, errors.New("broker closed")
	case i := <-s.recv:
		return i, nil
	}
}

// Close closes the quit channel, shutting down the stream.
func (s *gRPCBrokerServer) Close() {
	s.o.Do(func() {
		close(s.quit)
	})
	if s.streams != nil {
		s.streams.Close()
	}
}

// StreamRegistry returns the lifecycle registry for this broker streamer.
func (s *gRPCBrokerServer) StreamRegistry() *StreamRegistry { return s.streams }

// gRPCBrokerClientImpl is used by the client to start a stream and to send
// connection information to/from the client. Implements GRPCBrokerClient and
// streamer interfaces.
type gRPCBrokerClientImpl struct {
	// client is the underlying GRPC client used to make calls to the server.
	client plugin.GRPCBrokerClient

	// send is used to send connection info to the gRPC stream.
	send chan *sendErr

	// recv is used to receive connection info from the gRPC stream.
	recv chan *plugin.ConnInfo

	// quit closes down the stream.
	quit chan struct{}

	// queueCapacity is an optional compatibility buffer size.
	queueCapacity int

	// streams observes per-RPC stream handles.
	streams *StreamRegistry

	// o is used to ensure we close the quit channel only once.
	o sync.Once
}

func newGRPCBrokerClient(conn *grpc.ClientConn, capacity ...int) *gRPCBrokerClientImpl {
	n := 0
	if len(capacity) > 0 && capacity[0] > 0 {
		n = capacity[0]
	}
	return &gRPCBrokerClientImpl{
		client:        plugin.NewGRPCBrokerClient(conn),
		send:          make(chan *sendErr, n),
		recv:          make(chan *plugin.ConnInfo, n),
		quit:          make(chan struct{}),
		queueCapacity: n,
		streams:       NewStreamRegistry(),
	}
}

// StartStream implements the GRPCBrokerClient interface and will block until
// the quit channel is closed or the context reports Done. The stream will pass
// connection information to/from the plugin.
func (s *gRPCBrokerClientImpl) StartStream() error {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	defer s.Close()

	stream, err := s.client.StartStream(ctx)
	if err != nil {
		return err
	}
	doneCh := stream.Context().Done()
	handle, _ := s.streams.Create(stream.Context())
	defer func() { _ = handle.Close() }()

	go func() {
		for {
			select {
			case <-doneCh:
				return
			case <-s.quit:
				return
			case se := <-s.send:
				err := stream.Send(se.i)
				if handle != nil {
					_ = handle.Use()
				}
				select {
				case se.ch <- err:
				case <-s.quit:
					return
				case <-doneCh:
					return
				}
			}
		}
	}()

	for {
		i, err := stream.Recv()
		if err != nil {
			return err
		}
		if handle != nil {
			_ = handle.Use()
		}
		select {
		case <-doneCh:
			return nil
		case <-s.quit:
			return nil
		case s.recv <- i:
		}
	}
}

// Send is used by the GRPCBroker to pass connection information into the stream
// to the plugin.
func (s *gRPCBrokerClientImpl) Send(i *plugin.ConnInfo) error {
	ch := make(chan error, 1)

	select {
	case <-s.quit:
		return ErrBrokerClosed
	case s.send <- &sendErr{
		i:  i,
		ch: ch,
	}:
	}

	select {
	case err := <-ch:
		return err
	case <-s.quit:
		return ErrBrokerClosed
	}
}

// Recv is used by the GRPCBroker to pass connection information that has been
// sent from the plugin to the broker.
func (s *gRPCBrokerClientImpl) Recv() (*plugin.ConnInfo, error) {
	select {
	case <-s.quit:
		return nil, errors.New("broker closed")
	case i := <-s.recv:
		return i, nil
	}
}

// Close closes the quit channel, shutting down the stream.
func (s *gRPCBrokerClientImpl) Close() {
	s.o.Do(func() {
		close(s.quit)
	})
	if s.streams != nil {
		s.streams.Close()
	}
}

// StreamRegistry returns the lifecycle registry for this broker streamer.
func (s *gRPCBrokerClientImpl) StreamRegistry() *StreamRegistry { return s.streams }

// GRPCBroker is responsible for brokering connections by unique ID.
//
// It is used by plugins to create multiple gRPC connections and data
// streams between the plugin process and the host process.
//
// This allows a plugin to request a channel with a specific ID to connect to
// or accept a connection from, and the broker handles the details of
// holding these channels open while they're being negotiated.
//
// The Plugin interface has access to these for both Server and Client.
// The broker can be used by either (optionally) to reserve and connect to
// new streams. This is useful for complex args and return values,
// or anything else you might need a data stream for.
type GRPCBroker struct {
	nextId   uint32
	streamer streamer
	tls      *tls.Config
	doneCh   chan struct{}
	o        sync.Once

	clientStreams map[uint32]*gRPCBrokerPending
	serverStreams map[uint32]*gRPCBrokerPending
	listeners     map[uint32]net.Listener

	// streams observes the lifecycle of brokered stream records. It is always
	// initialized by newGRPCBroker but may be replaced by tests with a
	// recording registry.
	streams *StreamRegistry

	unixSocketCfg  UnixSocketConfig
	addrTranslator runner.AddrTranslator

	dialMutex sync.Mutex

	muxer grpcmux.GRPCMuxer

	// closed is guarded by the embedded mutex.
	closed bool

	sync.Mutex
}

type gRPCBrokerPending struct {
	// id is the broker-wide service/stream identifier.
	id uint32

	// tracked is the observable lifecycle handle in GRPCBroker.streams. It is
	// used for identity-safe removal when numeric IDs are reused.
	tracked *Stream

	ch     chan *plugin.ConnInfo
	doneCh chan struct{}
	once   sync.Once

	// mu guards closed and err. It is never held while taking GRPCBroker.Lock.
	mu     sync.Mutex
	closed bool
	err    error

	// created/used/closedAt make the three lifecycle phases observable without
	// requiring a debugger or goroutine dump.
	createdAt time.Time
	usedAt    atomic.Int64
	closedAt  atomic.Int64
	useCount  atomic.Int64
}

func newGRPCBrokerPending(id uint32) *gRPCBrokerPending {
	now := time.Now()
	return &gRPCBrokerPending{
		id:        id,
		ch:        make(chan *plugin.ConnInfo, 1),
		doneCh:    make(chan struct{}),
		createdAt: now,
	}
}

// ID returns the unique broker identifier for this stream.
func (p *gRPCBrokerPending) ID() uint32 {
	if p == nil {
		return 0
	}
	return p.id
}

// Done is closed exactly once when the stream completes, times out, is
// removed after client disconnection, or is explicitly closed.
func (p *gRPCBrokerPending) Done() <-chan struct{} { return p.doneCh }

// Err returns the terminal error, if any, after Done is closed.
func (p *gRPCBrokerPending) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

// Use records that a stream frame was dispatched or consumed.
func (p *gRPCBrokerPending) Use() {
	if p == nil {
		return
	}
	p.useCount.Add(1)
	p.usedAt.Store(time.Now().UnixNano())
}

// Uses reports how many times Use was called.
func (p *gRPCBrokerPending) Uses() int64 {
	if p == nil {
		return 0
	}
	return p.useCount.Load()
}

// UsedAt reports the last-use time, or the zero time if unused.
func (p *gRPCBrokerPending) UsedAt() time.Time {
	if p == nil {
		return time.Time{}
	}
	if ns := p.usedAt.Load(); ns != 0 {
		return time.Unix(0, ns)
	}
	return time.Time{}
}

// ClosedAt reports when the stream closed, or the zero time if still live.
func (p *gRPCBrokerPending) ClosedAt() time.Time {
	if p == nil {
		return time.Time{}
	}
	if ns := p.closedAt.Load(); ns != 0 {
		return time.Unix(0, ns)
	}
	return time.Time{}
}

// Close releases the stream exactly once. Repeated calls are safe and return
// nil; the terminal error from the first close remains available via Err.
func (p *gRPCBrokerPending) Close() error {
	p.close(ErrBrokerStreamClosed)
	return nil
}

// close publishes err as the terminal state and closes doneCh once. It returns
// true for the caller that performed the first close.
func (p *gRPCBrokerPending) close(err error) bool {
	if p == nil {
		return false
	}

	first := false
	p.once.Do(func() {
		first = true
		p.mu.Lock()
		p.closed = true
		p.err = err
		p.mu.Unlock()
		p.closedAt.Store(time.Now().UnixNano())
		close(p.doneCh)
	})
	return first
}

// send delivers a frame unless the stream has already closed. Unlike a raw
// channel send, a late writer gets a stable, comparable error instead of
// blocking forever or writing to a abandoned stream.
func (p *gRPCBrokerPending) send(msg *plugin.ConnInfo) error {
	if p == nil {
		return ErrBrokerStreamClosed
	}

	p.mu.Lock()
	if p.closed {
		err := p.err
		p.mu.Unlock()
		if err == nil {
			err = ErrBrokerStreamClosed
		}
		return fmt.Errorf("%w: stream %d: %v", errWriteAfterClose, p.id, err)
	}
	p.mu.Unlock()

	select {
	case <-p.doneCh:
		err := p.Err()
		if err == nil {
			err = ErrBrokerStreamClosed
		}
		return fmt.Errorf("%w: stream %d: %v", errWriteAfterClose, p.id, err)
	case p.ch <- msg:
		p.Use()
		return nil
	}
}

// Send is the exported spelling of send for code and tests that treat a
// pending broker stream as a writable stream.
func (p *gRPCBrokerPending) Send(msg *plugin.ConnInfo) error {
	return p.send(msg)
}

func newGRPCBroker(s streamer, tls *tls.Config, unixSocketCfg UnixSocketConfig, addrTranslator runner.AddrTranslator, muxer grpcmux.GRPCMuxer) *GRPCBroker {
	return &GRPCBroker{
		streamer: s,
		tls:      tls,
		doneCh:   make(chan struct{}),

		clientStreams: make(map[uint32]*gRPCBrokerPending),
		serverStreams: make(map[uint32]*gRPCBrokerPending),
		listeners:     make(map[uint32]net.Listener),
		streams:       NewStreamRegistry(),
		muxer:         muxer,

		unixSocketCfg:  unixSocketCfg,
		addrTranslator: addrTranslator,
	}
}

// Accept accepts a connection by ID.
//
// This should not be called multiple times with the same ID at one time.
func (b *GRPCBroker) Accept(id uint32) (net.Listener, error) {
	if b.muxEnabled() {
		p, err := b.acquireServerStream(id)
		if err != nil {
			return nil, err
		}
		go func() {
			err := b.listenForKnocks(id)
			if err != nil {
				log.Printf("[ERR]: error listening for knocks, id: %d, error: %s", id, err)
			}
		}()

		ln, err := b.muxer.Listener(id, p.doneCh)
		if err != nil {
			b.finish(p, b.serverStreams, err)
			return nil, err
		}

		ln = &rmListener{
			Listener: ln,
			close: func() error {
				// finish makes repeated closes safe and removes the map entry.
				b.finish(p, b.serverStreams, nil)
				return nil
			},
		}

		return ln, nil
	}

	listener, err := serverListener(b.unixSocketCfg)
	if err != nil {
		return nil, err
	}

	advertiseNet := listener.Addr().Network()
	advertiseAddr := listener.Addr().String()
	if b.addrTranslator != nil {
		advertiseNet, advertiseAddr, err = b.addrTranslator.HostToPlugin(advertiseNet, advertiseAddr)
		if err != nil {
			_ = listener.Close()
			return nil, err
		}
	}
	err = b.streamer.Send(&plugin.ConnInfo{
		ServiceId: id,
		Network:   advertiseNet,
		Address:   advertiseAddr,
	})
	if err != nil {
		// Do not leak the freshly bound listener if its endpoint could not be
		// advertised to the peer.
		_ = listener.Close()
		return nil, err
	}

	listener = b.trackListener(id, listener)
	return listener, nil
}

// AcceptAndServe is used to accept a specific stream ID and immediately
// serve a gRPC server on that stream ID. This is used to easily serve
// complex arguments. Each AcceptAndServe call opens a new listener socket and
// sends the connection info down the stream to the dialer. Since a new
// connection is opened every call, these calls should be used sparingly.
// Multiple gRPC server implementations can be registered to a single
// AcceptAndServe call.
func (b *GRPCBroker) AcceptAndServe(id uint32, newGRPCServer func([]grpc.ServerOption) *grpc.Server) {
	ln, err := b.Accept(id)
	if err != nil {
		log.Printf("[ERR] plugin: plugin acceptAndServe error: %s", err)
		return
	}
	defer func() { _ = ln.Close() }()

	var opts []grpc.ServerOption
	if b.tls != nil {
		opts = []grpc.ServerOption{grpc.Creds(credentials.NewTLS(b.tls))}
	}

	server := newGRPCServer(opts)

	// Here we use a run group to close this goroutine if the server is shutdown
	// or the broker is shutdown.
	var g run.Group
	{
		// Serve on the listener, if shutting down call GracefulStop.
		g.Add(func() error {
			return server.Serve(ln)
		}, func(err error) {
			server.GracefulStop()
		})
	}
	{
		// block on the closeCh or the doneCh. If we are shutting down close the
		// closeCh.
		closeCh := make(chan struct{})
		g.Add(func() error {
			select {
			case <-b.doneCh:
			case <-closeCh:
			}
			return nil
		}, func(err error) {
			close(closeCh)
		})
	}

	// Block until we are done
	_ = g.Run()
}

// Close closes the stream and all servers.
func (b *GRPCBroker) Close() error {
	b.streamer.Close()
	b.shutdown(ErrBrokerClosed)
	return nil
}

// shutdown closes the broker lifecycle channel once and promptly removes and
// releases every stream/listener still registered with the broker. It is safe
// to call from Close, Run's exit path, or a request-context cancellation.
func (b *GRPCBroker) shutdown(cause error) {
	if cause == nil {
		cause = ErrBrokerStreamClosed
	}

	b.o.Do(func() {
		close(b.doneCh)
	})

	b.Lock()
	if b.closed {
		b.Unlock()
		return
	}
	b.closed = true

	client := make([]*gRPCBrokerPending, 0, len(b.clientStreams))
	for id, p := range b.clientStreams {
		client = append(client, p)
		delete(b.clientStreams, id)
	}
	server := make([]*gRPCBrokerPending, 0, len(b.serverStreams))
	for id, p := range b.serverStreams {
		server = append(server, p)
		delete(b.serverStreams, id)
	}
	listeners := make([]net.Listener, 0, len(b.listeners))
	for id, ln := range b.listeners {
		listeners = append(listeners, ln)
		delete(b.listeners, id)
	}
	registry := b.streams
	b.Unlock()

	closedErr := fmt.Errorf("%w: %v", ErrBrokerStreamClosed, cause)
	for _, p := range client {
		p.close(closedErr)
		if registry != nil && p.tracked != nil {
			registry.RemoveStream(p.tracked)
		}
	}
	for _, p := range server {
		p.close(closedErr)
		if registry != nil && p.tracked != nil {
			registry.RemoveStream(p.tracked)
		}
	}
	for _, ln := range listeners {
		_ = ln.Close()
	}
	if registry != nil {
		registry.CloseWithCause(cause)
	}
}

// isClosed reports whether terminal broker cleanup has run.
func (b *GRPCBroker) isClosed() bool {
	b.Lock()
	defer b.Unlock()
	return b.closed
}

// StreamRegistry returns the lifecycle registry for brokered streams.
func (b *GRPCBroker) StreamRegistry() *StreamRegistry {
	b.Lock()
	defer b.Unlock()
	if b.streams == nil {
		b.streams = NewStreamRegistry()
	}
	return b.streams
}

// Streams is a shorthand alias for StreamRegistry.
func (b *GRPCBroker) Streams() *StreamRegistry { return b.StreamRegistry() }

// Registry is a terminology alias for StreamRegistry.
func (b *GRPCBroker) Registry() *StreamRegistry { return b.StreamRegistry() }

// muxEnabled is nil-interface safe.
func (b *GRPCBroker) muxEnabled() bool {
	return b != nil && b.muxer != nil && b.muxer.Enabled()
}

// trackListener registers a non-multiplexed listener and wraps it so closing
// the returned listener removes its broker entry.
func (b *GRPCBroker) trackListener(id uint32, ln net.Listener) net.Listener {
	var wrapped *rmListener
	wrapped = &rmListener{
		Listener: ln,
		close: func() error {
			b.Lock()
			if cur, ok := b.listeners[id]; ok && cur == wrapped {
				delete(b.listeners, id)
			}
			b.Unlock()
			return nil
		},
	}

	b.Lock()
	b.listeners[id] = wrapped
	b.Unlock()
	return wrapped
}

// usePending records a frame on both the pending record and the observable
// lifecycle registry.
func (b *GRPCBroker) usePending(p *gRPCBrokerPending) {
	if p == nil {
		return
	}
	p.Use()
	if b.streams != nil {
		b.streams.UseID(uint64(p.id))
	}
}

// finish is the common idempotent terminal path for a pending stream. It
// publishes err once and removes the map entry only when it still belongs to p.
func (b *GRPCBroker) finish(p *gRPCBrokerPending, streams map[uint32]*gRPCBrokerPending, err error) {
	if p == nil {
		return
	}
	p.close(err)

	b.Lock()
	if streams != nil && streams[p.id] == p {
		delete(streams, p.id)
	}
	registry := b.streams
	b.Unlock()

	if registry != nil {
		if p.tracked != nil {
			registry.RemoveStream(p.tracked)
		} else {
			registry.Remove(uint64(p.id))
		}
	}
}

// removeClientStream removes id only when it still identifies p.
func (b *GRPCBroker) removeClientStream(id uint32, p *gRPCBrokerPending) {
	b.Lock()
	if cur, ok := b.clientStreams[id]; ok && cur == p {
		delete(b.clientStreams, id)
	}
	b.Unlock()
}

// removeServerStream removes id only when it still identifies p.
func (b *GRPCBroker) removeServerStream(id uint32, p *gRPCBrokerPending) {
	b.Lock()
	if cur, ok := b.serverStreams[id]; ok && cur == p {
		delete(b.serverStreams, id)
	}
	b.Unlock()
}

// acquireClientStream returns the current client pending stream or
// ErrBrokerClosed after shutdown.
func (b *GRPCBroker) acquireClientStream(id uint32) (*gRPCBrokerPending, error) {
	b.Lock()
	if b.closed {
		b.Unlock()
		return nil, errBrokerClosed
	}
	p := b.clientStreams[id]
	b.Unlock()
	if p != nil {
		return p, nil
	}
	return b.getClientStream(id), nil
}

// acquireServerStream returns the current server pending stream or
// ErrBrokerClosed after shutdown.
func (b *GRPCBroker) acquireServerStream(id uint32) (*gRPCBrokerPending, error) {
	b.Lock()
	if b.closed {
		b.Unlock()
		return nil, errBrokerClosed
	}
	p := b.serverStreams[id]
	b.Unlock()
	if p != nil {
		return p, nil
	}
	return b.getServerStream(id), nil
}

// waitForConnInfo waits for a frame, terminal stream state, broker shutdown,
// or timeout. It always terminates p exactly once.
func (b *GRPCBroker) waitForConnInfo(p *gRPCBrokerPending, streams map[uint32]*gRPCBrokerPending, timeoutErr error) (*plugin.ConnInfo, error) {
	if timeoutErr == nil {
		timeoutErr = errWaitTimeout
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	select {
	case msg := <-p.ch:
		b.usePending(p)
		b.finish(p, streams, nil)
		return msg, nil
	case <-p.doneCh:
		err := p.Err()
		if err == nil {
			err = ErrBrokerStreamClosed
		}
		return nil, err
	case <-b.doneCh:
		return nil, ErrBrokerClosed
	case <-timer.C:
		b.finish(p, streams, timeoutErr)
		return nil, timeoutErr
	}
}

func (b *GRPCBroker) listenForKnocks(id uint32) error {
	p := b.getServerStream(id)
	for {
		select {
		case <-b.doneCh:
			return nil
		case <-p.doneCh:
			return nil
		case msg := <-p.ch:
			b.usePending(p)
			// Shouldn't be possible.
			if msg.ServiceId != id {
				err := fmt.Errorf("knock received with wrong service ID; expected %d but got %d", id, msg.ServiceId)
				b.finish(p, b.serverStreams, err)
				return err
			}

			// Also shouldn't be possible.
			if msg.Knock == nil || !msg.Knock.Knock || msg.Knock.Ack {
				err := fmt.Errorf("knock received for service ID %d with incorrect values; knock=%+v", id, msg.Knock)
				b.finish(p, b.serverStreams, err)
				return err
			}

			// Successful knock, open the door for the given ID.
			var ackError string
			err := b.muxer.AcceptKnock(id)
			if err != nil {
				ackError = err.Error()
			}

			// Send back an acknowledgement to allow the client to start dialling.
			err = b.streamer.Send(&plugin.ConnInfo{
				ServiceId: id,
				Knock: &plugin.ConnInfo_Knock{
					Knock: true,
					Ack:   true,
					Error: ackError,
				},
			})
			if err != nil {
				wrapped := fmt.Errorf("error sending back knock acknowledgement: %w", err)
				b.finish(p, b.serverStreams, wrapped)
				return wrapped
			}
		}
	}
}

func (b *GRPCBroker) knock(id uint32) error {
	p, err := b.acquireClientStream(id)
	if err != nil {
		return err
	}

	// Send a knock.
	err = b.streamer.Send(&plugin.ConnInfo{
		ServiceId: id,
		Knock: &plugin.ConnInfo_Knock{
			Knock: true,
		},
	})
	if err != nil {
		b.finish(p, b.clientStreams, err)
		return err
	}

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	// Wait for the ack.
	select {
	case msg := <-p.ch:
		b.usePending(p)
		switch {
		case msg.ServiceId != id:
			err = fmt.Errorf("handshake failed for multiplexing on id %d; got response for %d", id, msg.ServiceId)
		case msg.Knock == nil || !msg.Knock.Knock || !msg.Knock.Ack:
			err = fmt.Errorf("handshake failed for multiplexing on id %d; expected knock and ack, but got %+v", id, msg.Knock)
		case msg.Knock.Error != "":
			err = fmt.Errorf("failed to knock for id %d: %s", id, msg.Knock.Error)
		}
		b.finish(p, b.clientStreams, err)
		return err
	case <-p.doneCh:
		err = p.Err()
		if err == nil {
			err = ErrBrokerStreamClosed
		}
		return err
	case <-b.doneCh:
		return ErrBrokerClosed
	case <-timer.C:
		err = fmt.Errorf("%w on id %d", errKnockAckTimeout, id)
		b.finish(p, b.clientStreams, err)
		return err
	}
}

func (b *GRPCBroker) muxDial(id uint32) func(context.Context, string) (net.Conn, error) {
	return func(context.Context, string) (net.Conn, error) {
		b.dialMutex.Lock()
		defer b.dialMutex.Unlock()

		// Tell the other side the listener ID it should give the next stream to.
		err := b.knock(id)
		if err != nil {
			return nil, fmt.Errorf("failed to knock before dialling client: %w", err)
		}

		conn, err := b.muxer.Dial()
		if err != nil {
			return nil, err
		}

		return conn, nil
	}
}

// Dial opens a connection by ID.
func (b *GRPCBroker) Dial(id uint32) (conn *grpc.ClientConn, err error) { return b.DialWithOptions(id) }

// Dial opens a connection by ID with options.
func (b *GRPCBroker) DialWithOptions(id uint32, opts ...grpc.DialOption) (conn *grpc.ClientConn, err error) {
	if b.muxEnabled() {
		return dialGRPCConn(b.tls, b.muxDial(id), opts...)
	}

	// Open the stream. getClientStream also installs the safety timeout that
	// removes the pending entry if no frame or caller picks it up.
	p, err := b.acquireClientStream(id)
	if err != nil {
		return nil, err
	}
	c, err := b.waitForConnInfo(p, b.clientStreams, errWaitTimeout)
	if err != nil {
		return nil, err
	}

	network, address := c.Network, c.Address
	if b.addrTranslator != nil {
		network, address, err = b.addrTranslator.PluginToHost(network, address)
		if err != nil {
			return nil, err
		}
	}

	var addr net.Addr
	switch network {
	case "tcp":
		addr, err = net.ResolveTCPAddr("tcp", address)
	case "unix":
		addr, err = net.ResolveUnixAddr("unix", address)
	default:
		err = fmt.Errorf("unknown address type: %s", c.Address)
	}
	if err != nil {
		return nil, err
	}

	return dialGRPCConn(b.tls, netAddrDialer(addr), opts...)
}

// NextId returns a unique ID to use next.
//
// It is possible for very long-running plugin hosts to wrap this value,
// though it would require a very large amount of calls. In practice
// we've never seen it happen.
func (m *GRPCBroker) NextId() uint32 {
	return atomic.AddUint32(&m.nextId, 1)
}

// Run starts the brokering and should be executed in a goroutine, since it
// blocks forever, or until the session closes.
//
// Uses of GRPCBroker never need to call this. It is called internally by
// the plugin host/client.
func (m *GRPCBroker) Run() {
	var recvErr error
	defer func() {
		if recvErr != nil {
			m.shutdown(fmt.Errorf("%w: %v", errPeerStreamClosed, recvErr))
		} else {
			m.shutdown(nil)
		}
	}()

	for {
		msg, err := m.streamer.Recv()
		if err != nil {
			// Once we receive an error, the client has gone away or the
			// context/stream was cancelled. Shutting down removes and releases
			// every pending stream instead of leaving its record in the maps.
			recvErr = err
			return
		}

		// Initialize the waiter
		var p *gRPCBrokerPending
		if msg.Knock != nil && msg.Knock.Knock && !msg.Knock.Ack {
			p = m.getServerStream(msg.ServiceId)
			// The server side doesn't close the channel immediately as it needs
			// to continuously listen for knocks.
		} else {
			p = m.getClientStream(msg.ServiceId)
		}

		// Dispatch without allowing one full/stuck pending mailbox to block
		// receipt of frames for every other stream.
		select {
		case <-m.doneCh:
			return
		case <-p.doneCh:
			// Late frame for a closed stream: drop it.
			continue
		case p.ch <- msg:
			m.usePending(p)
		default:
		}
	}
}

// getClientStream is a buffer to receive new connection info and knock acks
// by stream ID.
func (m *GRPCBroker) getClientStream(id uint32) *gRPCBrokerPending {
	m.Lock()
	defer m.Unlock()

	if m.clientStreams == nil {
		m.clientStreams = make(map[uint32]*gRPCBrokerPending)
	}
	if m.streams == nil {
		m.streams = NewStreamRegistry()
	}

	p, ok := m.clientStreams[id]
	if ok {
		return p
	}
	if m.closed {
		p = newGRPCBrokerPending(id)
		p.close(ErrBrokerClosed)
		return p
	}

	p = newGRPCBrokerPending(id)
	m.clientStreams[id] = p
	p.tracked = m.streams.bindBrokerStream(p)
	go m.timeoutWait(id, p)
	return p
}

// getServerStream is a buffer to receive knocks to a multiplexed stream ID
// that its side is listening on. Not used unless multiplexing is enabled.
func (m *GRPCBroker) getServerStream(id uint32) *gRPCBrokerPending {
	m.Lock()
	defer m.Unlock()

	if m.serverStreams == nil {
		m.serverStreams = make(map[uint32]*gRPCBrokerPending)
	}
	if m.streams == nil {
		m.streams = NewStreamRegistry()
	}

	p, ok := m.serverStreams[id]
	if ok {
		return p
	}
	if m.closed {
		p = newGRPCBrokerPending(id)
		p.close(ErrBrokerClosed)
		return p
	}

	p = newGRPCBrokerPending(id)
	m.serverStreams[id] = p
	p.tracked = m.streams.bindBrokerStream(p)
	return p
}

func (m *GRPCBroker) timeoutWait(id uint32, p *gRPCBrokerPending) {
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	// Wait for the stream to either be picked up and connected, or for a
	// timeout. Broker shutdown closes both p and b.doneCh, so this reaper can
	// never keep a goroutine alive after disconnect.
	select {
	case <-p.doneCh:
	case <-m.doneCh:
		return
	case <-timer.C:
		p.close(errWaitTimeout)
	}

	m.Lock()
	// Delete the stream so no one else can grab it. Identity checking prevents
	// a delayed reaper from removing a newer stream that reused the numeric ID.
	if cur, ok := m.clientStreams[id]; ok && cur == p {
		delete(m.clientStreams, id)
	}
	registry := m.streams
	tracked := p.tracked
	m.Unlock()

	// Release the observable handle too. RemoveStream is identity-safe if the
	// ID was already re-bound to a newer generation.
	if registry != nil && tracked != nil {
		registry.RemoveStream(tracked)
	}
}
