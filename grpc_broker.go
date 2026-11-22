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

	// o is used to ensure we close the quit channel only once.
	o sync.Once
}

func newGRPCBrokerServer() *gRPCBrokerServer {
	return &gRPCBrokerServer{
		send: make(chan *sendErr),
		recv: make(chan *plugin.ConnInfo),
		quit: make(chan struct{}),
	}
}

// StartStream implements the GRPCBrokerServer interface and will block until
// the quit channel is closed or the context reports Done. The stream will pass
// connection information to/from the client.
func (s *gRPCBrokerServer) StartStream(stream plugin.GRPCBroker_StartStreamServer) error {
	doneCh := stream.Context().Done()
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
				se.ch <- err
			}
		}
	}()

	// Process receive stream
	for {
		i, err := stream.Recv()
		if err != nil {
			return err
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
	ch := make(chan error)
	defer close(ch)

	select {
	case <-s.quit:
		return errors.New("broker closed")
	case s.send <- &sendErr{
		i:  i,
		ch: ch,
	}:
	}

	return <-ch
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
}

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

	// o is used to ensure we close the quit channel only once.
	o sync.Once
}

func newGRPCBrokerClient(conn *grpc.ClientConn) *gRPCBrokerClientImpl {
	return &gRPCBrokerClientImpl{
		client: plugin.NewGRPCBrokerClient(conn),
		send:   make(chan *sendErr),
		recv:   make(chan *plugin.ConnInfo),
		quit:   make(chan struct{}),
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

	go func() {
		for {
			select {
			case <-doneCh:
				return
			case <-s.quit:
				return
			case se := <-s.send:
				err := stream.Send(se.i)
				se.ch <- err
			}
		}
	}()

	for {
		i, err := stream.Recv()
		if err != nil {
			return err
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
	ch := make(chan error)
	defer close(ch)

	select {
	case <-s.quit:
		return errors.New("broker closed")
	case s.send <- &sendErr{
		i:  i,
		ch: ch,
	}:
	}

	return <-ch
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
}

// negotiateTimeout is how long a Dial or knock handshake waits for the peer to
// answer before giving up.
const negotiateTimeout = 5 * time.Second

// defaultDrainTimeout bounds how long a finished broker stream keeps its
// stream ID fenced off after its terminal state has been published. During the
// drain window any late frames belonging to the old generation are absorbed and
// dropped, so the ID cannot be handed to a new call until the old frame can no
// longer reach a live waiter. The value is stored on GRPCBroker.drainTimeout so
// tests can shorten it.
const defaultDrainTimeout = time.Second

// errBrokerClosed is published to waiters once the broker has been shut down.
var errBrokerClosed = errors.New("broker closed")

// errPeerClosed is published to waiters when the broker's control stream ends
// because the peer went away.
var errPeerClosed = errors.New("broker stream closed by peer")

type GRPCBroker struct {
	nextId   uint32
	streamer streamer
	tls      *tls.Config
	doneCh   chan struct{}
	o        sync.Once

	// clientStreams and serverStreams hold the entry that currently owns each
	// stream ID. An ID stays in the map for the whole life of its entry,
	// including the drain window, so a late frame from an old generation can
	// never be routed to a new call reusing the ID.
	clientStreams map[uint32]*gRPCBrokerPending
	serverStreams map[uint32]*gRPCBrokerPending

	// nextGeneration is bumped every time a new entry takes ownership of a
	// stream ID. Together with the map identity checks it gives every
	// generation unique ownership: frames are only delivered to the entry that
	// owned the ID while they were negotiated.
	nextGeneration uint64

	// closed is set under the broker lock before doneCh is closed, so paths
	// that reject new streams cannot race Close.
	closed bool

	// drainTimeout bounds how long a finished entry keeps its ID fenced off
	// while late frames drain away. It is a field (rather than a constant) so
	// tests can shorten it.
	drainTimeout time.Duration

	unixSocketCfg  UnixSocketConfig
	addrTranslator runner.AddrTranslator

	dialMutex sync.Mutex

	muxer grpcmux.GRPCMuxer

	sync.Mutex
}

// entryState is the lifecycle phase of a gRPCBrokerPending entry.
type entryState int

const (
	// entryLive means the entry owns its stream ID and is serving waiters.
	entryLive entryState = iota
	// entryDraining means the entry has published its terminal state exactly
	// once; its ID is kept fenced off for drainTimeout while late frames from
	// the old generation are absorbed and dropped.
	entryDraining
)

// gRPCBrokerPending is the broker record for one generation of a stream ID.
// Each Dial/knock/Accept gets an entry with a unique generation, and Run only
// delivers frames to the entry whose generation owns the ID. Every terminal
// path (normal completion, negotiation timeout, peer close, broker Close)
// converges on finish, which publishes one result and then keeps the ID fenced
// through its drain window before it can be recycled.
type gRPCBrokerPending struct {
	// id and generation identify this exact ownership of a stream ID.
	id         uint32
	generation uint64

	// ch carries frames from Run to the entry's waiters. It is buffered, so a
	// frame arriving just as the entry finishes is absorbed and dropped by Run
	// instead of blocking the broker receive loop.
	ch chan *plugin.ConnInfo

	// doneCh is closed once, by finish, when the terminal state is published.
	doneCh chan struct{}

	// recycledCh is closed once, after the drain window, when the entry has
	// removed itself from the map and the ID is available for a new generation.
	recycledCh chan struct{}

	// terminal is the single result published to waiters. Set under the broker
	// lock before doneCh is closed, so at most one terminal state exists.
	terminal error

	state entryState

	// waiters counts in-flight knocks sharing this (client-side, multiplexed)
	// entry. Such entries span many handshakes and are reaped only once idle,
	// so a late ack can never be handed to the next generation.
	waiters int

	// reapSeq is bumped whenever a new handshake starts on a shared multiplexed
	// entry. A scheduled reaper captures the current value; if it changed before
	// the timer fires, a newer handshake owns the entry and the stale reaper
	// must not delete it.
	reapSeq uint64

	// reapOnce guarantees the entry removes itself from its map and signals
	// recycle exactly once, regardless of which lifecycle path gets there
	// first (finish drain timer or an idle-reap timer).
	reapOnce sync.Once

	once sync.Once
}

func newGRPCBroker(s streamer, tls *tls.Config, unixSocketCfg UnixSocketConfig, addrTranslator runner.AddrTranslator, muxer grpcmux.GRPCMuxer) *GRPCBroker {
	return &GRPCBroker{
		streamer: s,
		tls:      tls,
		doneCh:   make(chan struct{}),

		clientStreams: make(map[uint32]*gRPCBrokerPending),
		serverStreams: make(map[uint32]*gRPCBrokerPending),

		drainTimeout: defaultDrainTimeout,

		muxer: muxer,

		unixSocketCfg:  unixSocketCfg,
		addrTranslator: addrTranslator,
	}
}

// newPendingLocked mints an entry owned by a new, never-reused generation. The
// caller must hold b.Lock.
func (b *GRPCBroker) newPendingLocked(id uint32) *gRPCBrokerPending {
	b.nextGeneration++
	return &gRPCBrokerPending{
		id:         id,
		generation: b.nextGeneration,
		ch:         make(chan *plugin.ConnInfo, 1),
		doneCh:     make(chan struct{}),
		recycledCh: make(chan struct{}),
		state:      entryLive,
	}
}

// acquireClientStream returns the live entry that currently owns stream id on
// the client (dialing) side. If the previous generation is still draining, it
// blocks on that entry's recycledCh until the ID is fenced back into the pool;
// a fresh entry with a new generation is then created. It fails once the broker
// has been closed.
func (b *GRPCBroker) acquireClientStream(id uint32) (*gRPCBrokerPending, error) {
	for {
		b.Lock()
		if b.closed {
			b.Unlock()
			return nil, errBrokerClosed
		}

		p, ok := b.clientStreams[id]
		if !ok {
			p = b.newPendingLocked(id)
			b.clientStreams[id] = p
			b.Unlock()
			return p, nil
		}

		if p.state == entryLive {
			b.Unlock()
			return p, nil
		}

		// The previous generation is draining: its ID is not reusable until it
		// has absorbed its late frames.
		recycledCh := p.recycledCh
		b.Unlock()
		select {
		case <-recycledCh:
			// Loop and re-check under the lock; another acquirer may have won.
		case <-b.doneCh:
			return nil, errBrokerClosed
		}
	}
}

// acquireServerStream returns the live entry for a server-side (listening)
// stream id, creating one with a fresh generation if none exists. Server
// entries outlive individual knocks and are only recycled when the listener
// closes. If the previous generation is still draining it waits for recycle
// rather than handing a new listener an entry whose doneCh is already closed.
// It fails once the broker has been closed.
func (b *GRPCBroker) acquireServerStream(id uint32) (*gRPCBrokerPending, error) {
	for {
		b.Lock()
		if b.closed {
			b.Unlock()
			return nil, errBrokerClosed
		}

		p, ok := b.serverStreams[id]
		if !ok {
			p = b.newPendingLocked(id)
			b.serverStreams[id] = p
			b.Unlock()
			return p, nil
		}

		if p.state == entryLive {
			b.Unlock()
			return p, nil
		}

		// The listener for the previous generation is draining; wait for its
		// ID to be recycled before minting a new generation.
		recycledCh := p.recycledCh
		b.Unlock()
		select {
		case <-recycledCh:
		case <-b.doneCh:
			return nil, errBrokerClosed
		}
	}
}

// addWaiter marks a new in-flight handshake on a shared multiplexed client
// entry and returns the entry's new handshake sequence. The returned value
// lets a previously scheduled reaper tell that a newer handshake now owns the
// entry. removeWaiter decrements the count once the handshake completes.
func (b *GRPCBroker) addWaiter(p *gRPCBrokerPending) uint64 {
	b.Lock()
	p.waiters++
	p.reapSeq++
	seq := p.reapSeq
	b.Unlock()
	return seq
}

func (b *GRPCBroker) removeWaiter(p *gRPCBrokerPending) {
	b.Lock()
	if p.waiters > 0 {
		p.waiters--
	}
	b.Unlock()
}

// recycleLocked removes the entry from fromMap by identity and signals that
// its stream ID is available for a new generation. Identity checking means a
// newer generation that already holds the ID is never deleted by an older
// entry's cleanup; reapOnce makes the recycle idempotent across the finish
// drain timer and the idle-reap timer. The caller must hold b.Lock.
func (b *GRPCBroker) recycleLocked(p *gRPCBrokerPending, fromMap map[uint32]*gRPCBrokerPending) {
	p.reapOnce.Do(func() {
		if cur := fromMap[p.id]; cur == p {
			delete(fromMap, p.id)
		}
		close(p.recycledCh)
	})
}

// finish converges every terminal path for a one-shot entry (a non-multiplexed
// Dial, a multiplexed knock handshake, a server listener, peer shutdown, broker
// Close). The terminal error is published to waiters exactly once; the entry
// then drains for drainTimeout, absorbing late frames from its generation,
// before recycling the ID.
func (b *GRPCBroker) finish(p *gRPCBrokerPending, fromMap map[uint32]*gRPCBrokerPending, err error) {
	p.once.Do(func() {
		b.Lock()
		if p.state != entryLive {
			b.Unlock()
			return
		}
		p.terminal = err
		p.state = entryDraining
		b.Unlock()

		// Publish the single terminal state to all waiters.
		close(p.doneCh)

		// Keep the ID fenced off while late frames from this generation drain,
		// then recycle it.
		time.AfterFunc(b.drainTimeout, func() {
			b.Lock()
			b.recycleLocked(p, fromMap)
			b.Unlock()
		})
	})
}

// scheduleIdleReap marks a multiplexed client entry for recycling once no
// further handshake has started and the drain window has elapsed. seq is the
// handshake sequence captured by the knock that scheduled the reap: if another
// knock starts before the timer fires, its newer sequence invalidates this
// reaper. This keeps the ID fenced through a drain window without letting a
// stale timer delete an entry a new handshake is actively using.
func (b *GRPCBroker) scheduleIdleReap(p *gRPCBrokerPending, seq uint64) {
	time.AfterFunc(b.drainTimeout, func() {
		b.Lock()
		defer b.Unlock()

		if p.state != entryLive || p.waiters > 0 || p.reapSeq != seq {
			return
		}
		b.recycleLocked(p, b.clientStreams)
	})
}

// waitForConnInfo waits for the peer to deliver a frame to a one-shot entry. It
// returns the frame on normal completion, or the entry's terminal error on
// timeout/close/peer shutdown, and guarantees the entry is finished exactly
// once either way.
func (b *GRPCBroker) waitForConnInfo(p *gRPCBrokerPending, fromMap map[uint32]*gRPCBrokerPending, timeoutErr error) (*plugin.ConnInfo, error) {
	timer := time.NewTimer(negotiateTimeout)
	defer timer.Stop()

	select {
	case msg := <-p.ch:
		// This generation consumed the frame it was waiting for; finish and
		// drain so any further frames for the ID cannot reach a new caller.
		b.finish(p, fromMap, nil)
		return msg, nil
	case <-p.doneCh:
		return nil, p.terminal
	case <-timer.C:
		b.finish(p, fromMap, timeoutErr)
		return nil, timeoutErr
	}
}

// muxEnabled reports whether broker multiplexing is in use. It is nil-interface
// safe: brokers constructed without a muxer hold a nil GRPCMuxer.
func (b *GRPCBroker) muxEnabled() bool {
	return b.muxer != nil && b.muxer.Enabled()
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
			if err := b.listenForKnocks(id); err != nil {
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
				// finish's sync.Once makes repeated closes (multiple listeners
				// may wrap the same ID) safe and recycles the entry once.
				b.finish(p, b.serverStreams, nil)
				return nil
			},
		}

		return ln, nil
	}

	b.Lock()
	closed := b.closed
	b.Unlock()
	if closed {
		return nil, errBrokerClosed
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
			return nil, err
		}
	}
	err = b.streamer.Send(&plugin.ConnInfo{
		ServiceId: id,
		Network:   advertiseNet,
		Address:   advertiseAddr,
	})
	if err != nil {
		return nil, err
	}

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

// Close closes the stream and all servers. It is the single shutdown path:
// once it runs, attempts to acquire new streams fail with errBrokerClosed, and
// every in-flight waiter is published the same terminal error exactly once.
// The lock is only held over map bookkeeping, never while waiting on a frame or
// goroutine, so healthy streams cannot be blocked by the shutdown.
func (b *GRPCBroker) Close() error {
	b.streamer.Close()

	b.o.Do(func() {
		b.Lock()
		b.closed = true
		clientEntries := make([]*gRPCBrokerPending, 0, len(b.clientStreams))
		for _, p := range b.clientStreams {
			clientEntries = append(clientEntries, p)
		}
		serverEntries := make([]*gRPCBrokerPending, 0, len(b.serverStreams))
		for _, p := range b.serverStreams {
			serverEntries = append(serverEntries, p)
		}
		b.Unlock()

		close(b.doneCh)

		for _, p := range clientEntries {
			b.finish(p, b.clientStreams, errBrokerClosed)
		}
		for _, p := range serverEntries {
			b.finish(p, b.serverStreams, errBrokerClosed)
		}
	})
	return nil
}

// shutdown is the peer-closed terminal path. Run invokes it when the control
// stream returns an error. It converges with broker Close on finish, so callers
// still observe exactly one terminal result; the error distinguishes a peer
// shutdown from a local Close.
func (b *GRPCBroker) shutdown(err error) {
	b.Lock()
	if b.closed {
		b.Unlock()
		return
	}
	clientEntries := make([]*gRPCBrokerPending, 0, len(b.clientStreams))
	for _, p := range b.clientStreams {
		clientEntries = append(clientEntries, p)
	}
	serverEntries := make([]*gRPCBrokerPending, 0, len(b.serverStreams))
	for _, p := range b.serverStreams {
		serverEntries = append(serverEntries, p)
	}
	b.Unlock()

	for _, p := range clientEntries {
		b.finish(p, b.clientStreams, err)
	}
	for _, p := range serverEntries {
		b.finish(p, b.serverStreams, err)
	}
}

func (b *GRPCBroker) listenForKnocks(id uint32) error {
	p, err := b.acquireServerStream(id)
	if err != nil {
		return err
	}
	for {
		select {
		case msg := <-p.ch:
			// Shouldn't be possible.
			if msg.ServiceId != id {
				return fmt.Errorf("knock received with wrong service ID; expected %d but got %d", id, msg.ServiceId)
			}

			// Also shouldn't be possible.
			if msg.Knock == nil || !msg.Knock.Knock || msg.Knock.Ack {
				return fmt.Errorf("knock received for service ID %d with incorrect values; knock=%+v", id, msg.Knock)
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
				return fmt.Errorf("error sending back knock acknowledgement: %w", err)
			}
		case <-p.doneCh:
			return p.terminal
		case <-b.doneCh:
			return nil
		}
	}
}

// knock performs one multiplexing handshake for id. The entry that owns id is
// shared across successive knocks (a listener answers many of them), so a knock
// only registers itself as a waiter rather than finishing the entry; once the
// handshake completes the entry is scheduled for idle reaping, which keeps the
// ID fenced through the drain window before it may be reused.
func (b *GRPCBroker) knock(id uint32) error {
	p, err := b.acquireClientStream(id)
	if err != nil {
		return err
	}
	seq := b.addWaiter(p)
	defer b.removeWaiter(p)

	// Send a knock.
	if err := b.streamer.Send(&plugin.ConnInfo{
		ServiceId: id,
		Knock: &plugin.ConnInfo_Knock{
			Knock: true,
		},
	}); err != nil {
		b.finish(p, b.clientStreams, err)
		return err
	}

	// Wait for the ack.
	timer := time.NewTimer(negotiateTimeout)
	defer timer.Stop()
	select {
	case msg := <-p.ch:
		if msg.ServiceId != id {
			err := fmt.Errorf("handshake failed for multiplexing on id %d; got response for %d", id, msg.ServiceId)
			b.finish(p, b.clientStreams, err)
			return err
		}
		if msg.Knock == nil || !msg.Knock.Knock || !msg.Knock.Ack {
			err := fmt.Errorf("handshake failed for multiplexing on id %d; expected knock and ack, but got %+v", id, msg.Knock)
			b.finish(p, b.clientStreams, err)
			return err
		}
		if msg.Knock.Error != "" {
			err := fmt.Errorf("failed to knock for id %d: %s", id, msg.Knock.Error)
			b.finish(p, b.clientStreams, err)
			return err
		}
	case <-p.doneCh:
		return p.terminal
	case <-timer.C:
		err := fmt.Errorf("timeout waiting for multiplexing knock handshake on id %d", id)
		b.finish(p, b.clientStreams, err)
		return err
	}

	// Success: keep the entry alive briefly to absorb a duplicate/late ack,
	// then reap it once no further handshake is using it.
	b.scheduleIdleReap(p, seq)
	return nil
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

	// Acquire the unique generation that owns this ID for this call. If a
	// previous generation is still draining, block until its ID is recycled.
	p, err := b.acquireClientStream(id)
	if err != nil {
		return nil, err
	}

	c, err := b.waitForConnInfo(p, b.clientStreams, errors.New("timeout waiting for connection info"))
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
func (b *GRPCBroker) Run() {
	for {
		msg, err := b.streamer.Recv()
		if err != nil {
			// The control stream is gone: converge every waiter on the same
			// terminal state instead of leaving them to time out individually.
			b.shutdown(errPeerClosed)
			return
		}

		b.route(msg)
	}
}

// route delivers one control-stream frame to the entry that currently owns its
// service ID. Frames for an ID with no live entry, or for an entry already in
// its drain window, are late frames from an old generation and are dropped: they
// must never create or reach a waiter belonging to a new call.
func (b *GRPCBroker) route(msg *plugin.ConnInfo) {
	b.Lock()
	if b.closed {
		b.Unlock()
		return
	}

	var p *gRPCBrokerPending
	if msg.Knock != nil && msg.Knock.Knock && !msg.Knock.Ack {
		// An inbound knock is only valid for a live server listener. Knocks
		// that arrive before Accept or after the listener closed are dropped
		// rather than allocating an entry that could poison a later Accept.
		p = b.serverStreams[msg.ServiceId]
	} else {
		p = b.clientStreams[msg.ServiceId]
	}

	if p == nil || p.state != entryLive {
		b.Unlock()
		return
	}
	b.Unlock()

	// Buffered channel: if no waiter is ready (the handshake already timed
	// out or completed), the frame is absorbed and dropped on the next read
	// instead of stalling the receive loop.
	select {
	case p.ch <- msg:
	default:
	}
}
