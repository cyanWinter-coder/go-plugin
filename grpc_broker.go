// Copyright IBM Corp. 2016, 2025
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"context"
	"crypto/tls"
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
// information. Recv additionally reports the bidirectional stream session the
// message arrived on, so the broker can discard messages from streams that
// have been superseded by a reconnect.
type streamer interface {
	Send(*plugin.ConnInfo) error
	Recv() (*plugin.ConnInfo, streamSession, error)
	Close()
}

// streamSession identifies one bidirectional StartStream RPC within a
// broker connection. Every message received on the stream, and every local
// service call waiting on one, carries the session it belongs to so that a
// message from a replaced ("old") stream can never be committed onto the
// stream that replaced it.
type streamSession struct {
	// connectionID identifies the broker connection (streamer) the stream
	// belongs to. It is unique within the process for the lifetime of the
	// connection.
	connectionID uint64

	// generation identifies one StartStream invocation within the connection.
	// Generations are allocated monotonically; only the highest generation is
	// ever the current session.
	generation uint64
}

func (s streamSession) String() string {
	return fmt.Sprintf("broker-stream-session(conn=%d,gen=%d)", s.connectionID, s.generation)
}

// sessionStreamer is implemented by the streamer implementations that manage
// reconnectable bidirectional streams. It exposes the session a received
// message arrived on and the session currently allowed to commit work.
type sessionStreamer interface {
	// currentSession returns the connection/stream session that is currently
	// allowed to forward messages. The boolean is false once the streamer has
	// been closed permanently.
	currentSession() (streamSession, bool)
}

// queuedConnInfo pairs a broker control message with the stream session it
// was received on. The broker only commits a message when its session is
// still the current one.
type queuedConnInfo struct {
	info    *plugin.ConnInfo
	session streamSession
}

// sendErr is used to pass errors back during a send.
type sendErr struct {
	i  *plugin.ConnInfo
	ch chan error
}

// brokerConnState holds the session bookkeeping shared by the client and
// server stream implementations. A single instance backs one broker
// connection across however many bidirectional streams it attaches.
//
// Invariants:
//   - Only the stream holding the highest generation is "current".
//   - endStream fails exactly the calls still attached to its generation;
//     an older stream can never end a newer one.
//   - The send and receive queues are bounded and per connection.
type brokerConnState struct {
	// connectionID is the process-unique identity of this broker connection.
	connectionID uint64

	// queueCapacity bounds both the send and receive forwarding queues.
	queueCapacity int

	// recv is used to hand connection info received from gRPC streams to the
	// broker. Entries retain the session they arrived on. The queue belongs to
	// the whole connection, so backpressure is isolated per connection.
	recv chan *queuedConnInfo

	// quit closes down the whole connection permanently.
	quit chan struct{}

	// o is used to ensure we close the quit channel only once.
	o sync.Once

	// mu guards generation, currentGeneration and closed.
	mu                sync.Mutex
	generation        uint64
	currentGeneration uint64
	closed            bool

	// sessions holds, per generation, the hooks used to fail every call
	// attached to that generation when the stream ends.
	sessions map[uint64]*streamSessionState
}

// streamSessionState tracks the local service calls attached to one
// bidirectional stream session. When the session ends, onFail is invoked once
// for each still-pending call so that each call fails exactly once.
type streamSessionState struct {
	session streamSession

	// send is the bounded queue of outbound control messages for this stream
	// session. It is per session (not shared across reconnects) so a superseded
	// stream's runSend goroutine can never steal messages committed onto the
	// replacement stream.
	send chan *sendErr

	// done is closed when the session ends (supersession or connection close).
	// It unblocks the session's Recv/Send pumps even while the underlying gRPC
	// HTTP/2 stream has not yet reported an error, so a replaced StartStream
	// returns promptly and can never touch the replacement session.
	done chan struct{}

	// doneOnce guards closing done.
	doneOnce sync.Once

	// mu guards onFail and ended.
	mu     sync.Mutex
	onFail []func(error)
	ended  bool
}

// signal closes the session's done channel exactly once.
func (s *streamSessionState) signal() {
	s.doneOnce.Do(func() { close(s.done) })
}

// endSession marks the generation finished and fails every call still
// attached to it. It is a no-op for any generation that is no longer (or was
// never) current, which is what prevents an old stream from closing the state
// of a newer one.
func (st *brokerConnState) endSession(generation uint64, err error) {
	st.mu.Lock()
	if st.closed || generation != st.currentGeneration {
		st.mu.Unlock()
		return
	}
	sess := st.sessions[generation]
	delete(st.sessions, generation)
	st.mu.Unlock()

	if sess == nil {
		return
	}

	st.failSession(sess, err)
}

// failSession marks a session ended, fails its attached calls and drains its
// bounded send queue so any caller blocked behind a saturated queue gets a
// deterministic failure rather than a goroutine leak.
func (st *brokerConnState) failSession(sess *streamSessionState, err error) {
	sess.signal()

	sess.mu.Lock()
	sess.ended = true
	hooks := sess.onFail
	sess.onFail = nil
	sess.mu.Unlock()

	for _, fail := range hooks {
		fail(err)
	}

	for {
		select {
		case se := <-sess.send:
			se.ch <- err
		default:
			return
		}
	}
}

// attachSession starts a new stream generation and makes it the current one.
// It returns the new session and a cancel function that ends only this
// generation. If a previous generation was current its pending calls are
// failed once.
func (st *brokerConnState) attachSession() (streamSession, func(error)) {
	st.mu.Lock()
	if st.closed {
		st.mu.Unlock()
		return streamSession{connectionID: st.connectionID}, func(error) {}
	}

	st.generation++
	gen := st.generation
	previous := st.sessions[st.currentGeneration]
	delete(st.sessions, st.currentGeneration)

	sess := &streamSessionState{
		session: streamSession{connectionID: st.connectionID, generation: gen},
		send:    make(chan *sendErr, st.queueCapacity),
		done:    make(chan struct{}),
	}
	st.sessions[gen] = sess
	st.currentGeneration = gen
	st.mu.Unlock()

	if previous != nil {
		st.failSession(previous, &BrokerSessionClosedError{})
	}

	cancel := func(err error) {
		if err == nil {
			err = &BrokerSessionClosedError{}
		}
		st.endSession(gen, err)
	}
	return sess.session, cancel
}

// onSessionEnd registers a fail hook for the current generation. If the
// session has already ended (or the whole connection is closed), the hook
// fires immediately with the supplied failure. The returned session identifies
// the generation the caller is now attached to.
func (st *brokerConnState) onSessionEnd(id uint32, fail func(error)) (streamSession, bool) {
	st.mu.Lock()
	if st.closed {
		st.mu.Unlock()
		fail(&BrokerSessionClosedError{ServiceID: id})
		return streamSession{connectionID: st.connectionID}, false
	}
	sess := st.sessions[st.currentGeneration]
	st.mu.Unlock()

	if sess == nil {
		fail(&BrokerSessionClosedError{ServiceID: id})
		return streamSession{connectionID: st.connectionID}, false
	}

	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.ended {
		fail(&BrokerSessionClosedError{ServiceID: id})
		return sess.session, false
	}
	sess.onFail = append(sess.onFail, fail)
	return sess.session, true
}

func (st *brokerConnState) currentSession() (streamSession, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return streamSession{}, false
	}
	return streamSession{connectionID: st.connectionID, generation: st.currentGeneration}, true
}

// close permanently shuts the connection down, failing any calls still
// attached to its current session. Subsequent Send/Recv calls return
// ErrBrokerClosed.
func (st *brokerConnState) close() {
	st.o.Do(func() {
		st.mu.Lock()
		st.closed = true
		sess := st.sessions[st.currentGeneration]
		delete(st.sessions, st.currentGeneration)
		st.mu.Unlock()

		close(st.quit)

		if sess != nil {
			st.failSession(sess, ErrBrokerClosed)
		}
	})
}

// enqueueRecv hands a message received on the given session to the broker
// unless the bounded receive queue is full, in which case it returns a
// [BrokerQueueFullError] and the caller drops the message instead of blocking
// the stream's Recv loop.
func (st *brokerConnState) enqueueRecv(msg *plugin.ConnInfo, sess streamSession) error {
	item := &queuedConnInfo{info: msg, session: sess}
	select {
	case <-st.quit:
		return ErrBrokerClosed
	case st.recv <- item:
		return nil
	default:
		return &BrokerQueueFullError{ServiceID: msg.ServiceId}
	}
}

// connIDSource hands out process-unique broker connection identifiers.
var connIDSource atomic.Uint64

func newBrokerConnState(queueCapacity int) *brokerConnState {
	if queueCapacity <= 0 {
		queueCapacity = DefaultGRPCBrokerQueueCapacity
	}
	return &brokerConnState{
		connectionID:  connIDSource.Add(1),
		queueCapacity: queueCapacity,
		recv:          make(chan *queuedConnInfo, queueCapacity),
		quit:          make(chan struct{}),
		sessions:      make(map[uint64]*streamSessionState),
	}
}

// gRPCBrokerServer is used by the plugin to start a stream and to send
// connection information to/from the plugin. Implements GRPCBrokerServer and
// streamer interfaces.
type gRPCBrokerServer struct {
	plugin.UnimplementedGRPCBrokerServer

	*brokerConnState
}

func newGRPCBrokerServer(queueCapacity int) *gRPCBrokerServer {
	return &gRPCBrokerServer{
		brokerConnState: newBrokerConnState(queueCapacity),
	}
}

// StartStream implements the GRPCBrokerServer interface and will block until
// the quit channel is closed or the context reports Done. The stream will pass
// connection information to/from the client.
//
// A duplicate or concurrent StartStream attaches a new, higher-generation
// session and supersedes the previous stream rather than racing with it: the
// previous stream's pending calls fail once and its late messages are
// discarded.
func (s *gRPCBrokerServer) StartStream(stream plugin.GRPCBroker_StartStreamServer) (retErr error) {
	sess, endSession := s.attachSession()
	if sess.generation == 0 {
		// The broker connection was closed permanently before this stream
		// attached: there is no session to run.
		return ErrBrokerClosed
	}
	doneCh := stream.Context().Done()
	defer func() {
		// Propagate the real cause (supersession or backpressure) to pending
		// calls when this session ends; default to the session-closed error.
		if retErr != nil {
			endSession(retErr)
		} else {
			endSession(&BrokerSessionClosedError{})
		}
	}()
	defer s.endIfCurrent(sess)

	// Process send stream.
	go s.runSend(stream, sess, doneCh)

	// Process receive stream.
	return s.runRecv(stream, sess, doneCh)
}

// endIfCurrent ends the session only while it is still the current one, so a
// late return from an old stream can never shut down the stream that replaced
// it.
func (st *brokerConnState) endIfCurrent(sess streamSession) {
	st.mu.Lock()
	isCurrent := !st.closed && st.currentGeneration == sess.generation
	st.mu.Unlock()
	if isCurrent {
		st.endSession(sess.generation, &BrokerSessionClosedError{})
	}
}

// runSend services outbound control messages for one stream session. It reads
// only from that session's bounded send queue, so a superseded stream's pump
// can never steal a message committed onto the replacement stream. The pump
// exits as soon as the session ends (its done channel closes) even if the
// underlying gRPC stream has not reported an error yet.
func (st *brokerConnState) runSend(stream grpcStreamSender, sess streamSession, doneCh <-chan struct{}) {
	sendCh, sessionDone := st.sessionChannels(sess.generation)
	if sendCh == nil {
		return
	}
	for {
		select {
		case <-doneCh:
			return
		case <-sessionDone:
			return
		case <-st.quit:
			return
		case se := <-sendCh:
			if !st.isCurrent(sess) {
				se.ch <- &BrokerSessionClosedError{ServiceID: se.i.ServiceId}
				return
			}
			err := stream.Send(se.i)
			se.ch <- err
			if err != nil {
				return
			}
		}
	}
}

// sessionChannels returns the bounded send queue and done channel for a
// generation, or nil channels if that generation is no longer attached.
func (st *brokerConnState) sessionChannels(generation uint64) (chan *sendErr, <-chan struct{}) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if sess, ok := st.sessions[generation]; ok {
		return sess.send, sess.done
	}
	return nil, nil
}

// grpcStreamSender is the subset of the generated gRPC stream interfaces used
// to send ConnInfo messages.
type grpcStreamSender interface {
	Send(*plugin.ConnInfo) error
}

func (st *brokerConnState) isCurrent(sess streamSession) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return !st.closed && st.currentGeneration == sess.generation
}

// runRecv pumps inbound control messages for one stream session. Every
// message is tagged with its session; the broker's Run loop commits it only
// while the session is still current. If the connection's bounded receive
// queue saturates, this stream session ends with ErrBrokerQueueFull so its
// in-flight calls fail once and a reconnect can attach a fresh session,
// instead of stalling silently. A superseded stream also stops reading
// immediately: its session done channel interrupts even a Recv blocked inside
// gRPC.
func (st *brokerConnState) runRecv(stream grpcStreamReceiver, sess streamSession, doneCh <-chan struct{}) error {
	_, sessionDone := st.sessionChannels(sess.generation)
	if sessionDone == nil {
		// The session was already removed (superseded or closed) before the
		// pump started: nothing to do, and it must not touch a newer session.
		return ErrBrokerStreamSuperseded
	}

	type recvResult struct {
		info *plugin.ConnInfo
		err  error
	}
	recvCh := make(chan recvResult, 1)
	go func() {
		for {
			info, err := stream.Recv()
			select {
			case recvCh <- recvResult{info, err}:
			case <-sessionDone:
				return
			case <-st.quit:
				return
			case <-doneCh:
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-doneCh:
			return nil
		case <-sessionDone:
			// Superseded or closed: stop committing anything from this stream.
			return ErrBrokerStreamSuperseded
		case <-st.quit:
			return nil
		case r := <-recvCh:
			if r.err != nil {
				return r.err
			}
			i := r.info

			if !st.isCurrent(sess) {
				// The stream was replaced: drop anything still arriving on it so
				// a late message can never land on the new session.
				return ErrBrokerStreamSuperseded
			}

			if qErr := st.enqueueRecv(i, sess); qErr != nil {
				// The connection's bounded receive queue is saturated. End this
				// session with the recognisable backpressure error so its calls
				// fail deterministically; StartStream's defer reports qErr and a
				// subsequent reconnect attaches a fresh session.
				st.endSession(sess.generation, qErr)
				return qErr
			}
		}
	}
}

// grpcStreamReceiver is the subset of the generated gRPC stream interfaces
// used to receive ConnInfo messages.
type grpcStreamReceiver interface {
	Recv() (*plugin.ConnInfo, error)
}

// Send is used by the GRPCBroker to pass connection information into the
// current stream to the peer. It fails fast with ErrBrokerClosed once the
// connection is shut down, and with ErrBrokerQueueFull when the current
// stream session's bounded send queue is saturated: the caller observes
// recognisable per-connection backpressure instead of blocking indefinitely.
func (st *brokerConnState) Send(i *plugin.ConnInfo) error {
	st.mu.Lock()
	if st.closed {
		st.mu.Unlock()
		return ErrBrokerClosed
	}
	sess := st.sessions[st.currentGeneration]
	st.mu.Unlock()
	if sess == nil {
		return ErrBrokerClosed
	}

	ch := make(chan error, 1)
	select {
	case <-st.quit:
		return ErrBrokerClosed
	case sess.send <- &sendErr{i: i, ch: ch}:
	default:
		return &BrokerQueueFullError{ServiceID: i.ServiceId}
	}

	select {
	case <-st.quit:
		return ErrBrokerClosed
	case <-sess.done:
		// The session ended right after enqueueing; failSession drains the
		// queue, but watch done as well so a racing end never leaves this call
		// blocked on an un-serviced reply channel.
		return &BrokerSessionClosedError{ServiceID: i.ServiceId}
	case err := <-ch:
		return err
	}
}

// Recv is used by the GRPCBroker to pull connection information that has been
// sent from the peer, alongside the stream session it arrived on.
func (st *brokerConnState) Recv() (*plugin.ConnInfo, streamSession, error) {
	select {
	case <-st.quit:
		return nil, streamSession{}, ErrBrokerClosed
	case item := <-st.recv:
		return item.info, item.session, nil
	}
}

// Close closes the quit channel, shutting down every stream on the connection
// permanently.
func (st *brokerConnState) Close() { st.close() }

// gRPCBrokerClientImpl is used by the client to start a stream and to send
// connection information to/from the client. Implements GRPCBrokerClient and
// streamer interfaces.
type gRPCBrokerClientImpl struct {
	// client is the underlying GRPC client used to make calls to the server.
	client plugin.GRPCBrokerClient

	*brokerConnState
}

func newGRPCBrokerClient(conn *grpc.ClientConn, queueCapacity int) *gRPCBrokerClientImpl {
	return &gRPCBrokerClientImpl{
		client:          plugin.NewGRPCBrokerClient(conn),
		brokerConnState: newBrokerConnState(queueCapacity),
	}
}

// StartStream implements the GRPCBrokerClient interface and will block until
// the quit channel is closed or the context reports Done. The stream will pass
// connection information to/from the plugin.
//
// Calling StartStream again (an explicit reconnect/reattach) installs a new
// stream generation; the previous generation's pending calls fail once and
// any messages still arriving on the old stream are discarded.
func (s *gRPCBrokerClientImpl) StartStream() (retErr error) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	sess, endSession := s.attachSession()
	if sess.generation == 0 {
		// The broker connection was closed permanently before this stream
		// attached: there is no session to run.
		return ErrBrokerClosed
	}
	defer func() {
		// Propagate the real cause (supersession or backpressure) to pending
		// calls when this session ends; default to the session-closed error.
		if retErr != nil {
			endSession(retErr)
		} else {
			endSession(&BrokerSessionClosedError{})
		}
	}()
	defer s.endIfCurrent(sess)

	stream, err := s.client.StartStream(ctx)
	if err != nil {
		return err
	}
	doneCh := stream.Context().Done()

	go s.runSend(stream, sess, doneCh)

	return s.runRecv(stream, sess, doneCh)
}

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

	unixSocketCfg  UnixSocketConfig
	addrTranslator runner.AddrTranslator

	dialMutex sync.Mutex

	muxer grpcmux.GRPCMuxer

	sync.Mutex
}

type gRPCBrokerPending struct {
	id     uint32
	ch     chan *plugin.ConnInfo
	doneCh chan struct{}
	once   sync.Once

	// session is the bidirectional stream session this service call attached
	// under. Messages are only delivered here while that session is current.
	session streamSession

	// failOnce ensures the call observes a single failure even if both the
	// session end hook and the broker's timeout fire.
	failOnce sync.Once
}

// finish closes the pending call's done channel exactly once.
func (p *gRPCBrokerPending) finish() {
	p.once.Do(func() {
		close(p.doneCh)
	})
}

// fail completes the pending call with err exactly once. It is safe to invoke
// from the session-end hook and from timeout paths.
func (p *gRPCBrokerPending) fail(err error) {
	p.failOnce.Do(func() {
		p.finish()
	})
}

func newGRPCBroker(s streamer, tls *tls.Config, unixSocketCfg UnixSocketConfig, addrTranslator runner.AddrTranslator, muxer grpcmux.GRPCMuxer) *GRPCBroker {
	return &GRPCBroker{
		streamer: s,
		tls:      tls,
		doneCh:   make(chan struct{}),

		clientStreams: make(map[uint32]*gRPCBrokerPending),
		serverStreams: make(map[uint32]*gRPCBrokerPending),
		muxer:         muxer,

		unixSocketCfg:  unixSocketCfg,
		addrTranslator: addrTranslator,
	}
}

// Accept accepts a connection by ID.
//
// This should not be called multiple times with the same ID at one time.
func (b *GRPCBroker) Accept(id uint32) (net.Listener, error) {
	if b.muxer.Enabled() {
		p, err := b.getServerStream(id)
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
			return nil, err
		}

		ln = &rmListener{
			Listener: ln,
			close: func() error {
				// We could have multiple listeners on the same ID, so use sync.Once
				// for closing doneCh to ensure we don't get a panic.
				p.finish()

				b.Lock()
				defer b.Unlock()

				// No longer need to listen for knocks once the listener is closed.
				delete(b.serverStreams, id)

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

// Close closes the stream and all servers.
func (b *GRPCBroker) Close() error {
	b.streamer.Close()
	b.o.Do(func() {
		close(b.doneCh)
	})
	return nil
}

func (b *GRPCBroker) listenForKnocks(id uint32) error {
	p, err := b.getServerStream(id)
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
			return nil
		}
	}
}

func (b *GRPCBroker) knock(id uint32) error {
	// Send a knock.
	err := b.streamer.Send(&plugin.ConnInfo{
		ServiceId: id,
		Knock: &plugin.ConnInfo_Knock{
			Knock: true,
		},
	})
	if err != nil {
		return err
	}

	// Wait for the ack.
	p, err := b.getClientStream(id)
	if err != nil {
		return err
	}
	select {
	case msg := <-p.ch:
		if msg.ServiceId != id {
			return fmt.Errorf("handshake failed for multiplexing on id %d; got response for %d", id, msg.ServiceId)
		}
		if msg.Knock == nil || !msg.Knock.Knock || !msg.Knock.Ack {
			return fmt.Errorf("handshake failed for multiplexing on id %d; expected knock and ack, but got %+v", id, msg.Knock)
		}
		if msg.Knock.Error != "" {
			return fmt.Errorf("failed to knock for id %d: %s", id, msg.Knock.Error)
		}
	case <-p.doneCh:
		return &BrokerSessionClosedError{ServiceID: id}
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout waiting for multiplexing knock handshake on id %d", id)
	}

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
	if b.muxer.Enabled() {
		return dialGRPCConn(b.tls, b.muxDial(id), opts...)
	}

	var c *plugin.ConnInfo

	// Open the stream
	p, err := b.getClientStream(id)
	if err != nil {
		return nil, err
	}
	select {
	case c = <-p.ch:
		p.finish()
	case <-p.doneCh:
		return nil, &BrokerSessionClosedError{ServiceID: id}
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("timeout waiting for connection info")
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
	for {
		msg, sess, err := m.streamer.Recv()
		if err != nil {
			// Once we receive an error, just exit
			break
		}

		// Session boundary: only commit messages from the stream session that
		// is still current. Late messages arriving on a replaced stream are
		// dropped here rather than delivered onto the new session.
		if !m.isCurrentSession(sess) {
			continue
		}

		// Resolve (or create) the waiter for this service ID. The lookup is
		// session-aware: entries left over from a replaced stream are removed
		// when their session ends, so a message on the new session attaches a
		// fresh waiter instead of being silently dropped onto the stale one.
		var p *gRPCBrokerPending
		if msg.Knock != nil && msg.Knock.Knock && !msg.Knock.Ack {
			// The server side doesn't close the channel immediately as it needs
			// to continuously listen for knocks.
			p, err = m.getServerStream(msg.ServiceId)
		} else {
			p, err = m.getClientStream(msg.ServiceId)
			if err != nil {
				continue
			}
			go m.timeoutWait(msg.ServiceId, p)
		}
		if err != nil {
			continue
		}

		// The waiter belongs to the current session at attach time; only commit
		// the message if it arrived on that same session. A mismatch means the
		// entry was created while another stream was current, so the message is
		// stale and must be dropped rather than committed onto the new stream.
		if p.session != sess {
			continue
		}

		select {
		case p.ch <- msg:
		default:
			// The pending entry's single-slot buffer is full (a duplicate or a
			// message with no active waiter). Dropping here preserves the
			// capacity boundary instead of blocking the broker loop.
		}
	}
}

// isCurrentSession reports whether sess is still allowed to commit work. A
// zero session (streamer without session tracking) is always treated as
// current so plain streamer doubles keep working.
func (m *GRPCBroker) isCurrentSession(sess streamSession) bool {
	if sess == (streamSession{}) {
		return true
	}
	sr, ok := m.streamer.(sessionStreamer)
	if !ok {
		return true
	}
	current, ok := sr.currentSession()
	if !ok {
		return false
	}
	return current == sess
}

// getClientStream is a buffer to receive new connection info and knock acks
// by stream ID. An existing entry is reused only while it is still attached
// to the current bidirectional stream session; an entry left over from a
// replaced stream is discarded and recreated.
func (m *GRPCBroker) getClientStream(id uint32) (*gRPCBrokerPending, error) {
	m.Lock()
	defer m.Unlock()

	if p, ok := m.clientStreams[id]; ok && m.pendingIsCurrent(p) {
		return p, nil
	}

	p := m.newPendingLocked(id)
	m.clientStreams[id] = p
	return p, nil
}

// getServerStream is a buffer to receive knocks to a multiplexed stream ID
// that its side is listening on. Not used unless multiplexing is enabled.
func (m *GRPCBroker) getServerStream(id uint32) (*gRPCBrokerPending, error) {
	m.Lock()
	defer m.Unlock()

	if p, ok := m.serverStreams[id]; ok && m.pendingIsCurrent(p) {
		return p, nil
	}

	p := m.newPendingLocked(id)
	m.serverStreams[id] = p
	return p, nil
}

// pendingIsCurrent reports whether a pending entry is still attached to the
// stream session allowed to commit work. Callers must hold the broker lock.
func (m *GRPCBroker) pendingIsCurrent(p *gRPCBrokerPending) bool {
	ss, ok := m.streamer.(sessionStreamer)
	if !ok {
		return true
	}
	current, ok := ss.currentSession()
	if !ok {
		return false
	}
	return p.session == current
}

// newPendingLocked creates a pending service-call entry bound to the current
// stream session and registers a session-end hook that fails the call exactly
// once and removes it from the pending maps, so late messages from the dead
// stream can never be routed onto it and the same service ID can be re-attached
// cleanly on the replacement stream. Callers must hold the broker lock.
func (m *GRPCBroker) newPendingLocked(id uint32) *gRPCBrokerPending {
	p := &gRPCBrokerPending{
		id:     id,
		ch:     make(chan *plugin.ConnInfo, 1),
		doneCh: make(chan struct{}),
	}

	if ss, ok := m.streamer.(sessionStreamer); ok {
		sess, _ := ss.currentSession()
		p.session = sess

		type sessionHooker interface {
			onSessionEnd(id uint32, fail func(error)) (streamSession, bool)
		}
		sessionEnded := make(chan struct{})
		if hooker, ok := m.streamer.(sessionHooker); ok {
			// onSessionEnd closes no channel itself; it either registers the
			// hook or invokes it inline when the session is already gone. The
			// map cleanup must run without holding the broker lock (this method
			// holds it), so both paths funnel through the goroutine below.
			hooker.onSessionEnd(id, func(error) {
				close(sessionEnded)
			})
		}
		go func() {
			<-sessionEnded
			p.fail(&BrokerSessionClosedError{ServiceID: id})

			m.Lock()
			defer m.Unlock()
			// Only delete entries that are still this one; a replacement
			// session may already have reattached the same service ID.
			if cur, ok := m.clientStreams[id]; ok && cur == p {
				delete(m.clientStreams, id)
			}
			if cur, ok := m.serverStreams[id]; ok && cur == p {
				delete(m.serverStreams, id)
			}
		}()
	}

	return p
}

func (m *GRPCBroker) timeoutWait(id uint32, p *gRPCBrokerPending) {
	// Wait for the stream to either be picked up and connected, or
	// for a timeout.
	select {
	case <-p.doneCh:
	case <-time.After(5 * time.Second):
	}

	m.Lock()
	defer m.Unlock()

	cur, ok := m.clientStreams[id]
	// Only remove the entry if it is still the same pending call; a session
	// boundary may have replaced it with a fresh attach under the same ID.
	if ok && cur == p {
		delete(m.clientStreams, id)
	}
}
