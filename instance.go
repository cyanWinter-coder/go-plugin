// Copyright IBM Corp. 2016, 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"
)

// InstanceState describes the lifecycle state of a plugin instance view.
type InstanceState string

const (
	// InstanceStateActive means the plugin process is known to be running and
	// the view is eligible for reconnection.
	InstanceStateActive InstanceState = "active"

	// InstanceStateExited means the plugin process has exited, or the client
	// was killed. Such a view can never be reconnected to and a fresh
	// instance must be started instead.
	InstanceStateExited InstanceState = "exited"
)

// ReconnectFailureReason identifies why a reconnect attempt was rolled back.
// It is stable enough to be switched on by callers.
type ReconnectFailureReason string

const (
	// ReconnectReasonNotRunning means there is no live instance to reconnect
	// to (it was never started, has already exited, or was killed).
	ReconnectReasonNotRunning ReconnectFailureReason = "not-running"

	// ReconnectReasonProcessExited means the process exited while the new
	// connection was being established.
	ReconnectReasonProcessExited ReconnectFailureReason = "process-exited"

	// ReconnectReasonIdentity means the discovery record, process identity or
	// handshake endpoint no longer match the instance this client is bound to.
	ReconnectReasonIdentity ReconnectFailureReason = "identity-mismatch"

	// ReconnectReasonCanceled means the caller's context was canceled before
	// the new connection was published.
	ReconnectReasonCanceled ReconnectFailureReason = "canceled"

	// ReconnectReasonTimeout means the reconnect handshake did not complete
	// before the deadline.
	ReconnectReasonTimeout ReconnectFailureReason = "timeout"

	// ReconnectReasonHandshake means the endpoint answered but the control
	// connection could not be established or did not pass its health check.
	ReconnectReasonHandshake ReconnectFailureReason = "handshake-failed"
)

// Reconnect errors. Every failure returned by Client.Reconnect wraps exactly
// one of these, and is additionally reported as a *ReconnectError carrying the
// precise ReconnectFailureReason.
var (
	// ErrNoLiveInstance is returned when a reconnect is attempted without a
	// running plugin instance bound to the client.
	ErrNoLiveInstance = errors.New("no live plugin instance to reconnect to")

	// ErrReconnectProcessExited is returned when the plugin process exited
	// while the control connection was being re-established.
	ErrReconnectProcessExited = errors.New("plugin process exited during reconnect")

	// ErrInstanceIdentityMismatch is returned when the evidence used to
	// reattach (discovery record, process identity or handshake endpoint) no
	// longer describes the instance the client is bound to. The reconnect is
	// refused rather than silently binding to a different instance.
	ErrInstanceIdentityMismatch = errors.New("plugin instance identity mismatch")

	// ErrReconnectCanceled is returned when the caller cancels the reconnect.
	ErrReconnectCanceled = errors.New("plugin reconnect canceled")

	// ErrReconnectTimeout is returned when the reconnect handshake times out.
	ErrReconnectTimeout = errors.New("plugin reconnect timed out")

	// ErrReconnectHandshake is returned when the plugin endpoint answers, but
	// the new control connection cannot be established or fails its health
	// check.
	ErrReconnectHandshake = errors.New("plugin reconnect handshake failed")
)

// ReconnectError is the error type returned by Client.Reconnect for every
// failed attempt. It records the reason the attempt was rolled back, and wraps
// the underlying cause (which is also matchable through errors.Is).
type ReconnectError struct {
	Reason ReconnectFailureReason
	Err    error
}

// Error implements error.
func (e *ReconnectError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("plugin reconnect failed (%s)", e.Reason)
	}
	return fmt.Sprintf("plugin reconnect failed (%s): %s", e.Reason, e.Err)
}

// Unwrap returns the underlying cause so errors.Is/As work as usual.
func (e *ReconnectError) Unwrap() error { return e.Err }

// InstanceView is an immutable snapshot of one plugin instance. The process
// identity, handshake endpoint and negotiated protocol version are published
// together, so a view can never mix evidence from two different instances.
//
// A view is obtained from Client.InstanceView. Reconnection only ever targets
// the same Identity and keeps the Protocol and ProtocolVersion fixed; a fresh
// plugin process must be started through a new Client instead.
type InstanceView struct {
	// ID is the process identity: the runner's ID (the PID for command
	// runners, or the ID supplied by a custom runner).
	ID string

	// Address is the plugin's handshake endpoint as advertised over stdout.
	Address net.Addr

	// Protocol is the negotiated transport protocol.
	Protocol Protocol

	// ProtocolVersion is the negotiated application protocol version.
	ProtocolVersion int

	// CoreProtocolVersion is the go-plugin core protocol version advertised
	// in the handshake.
	CoreProtocolVersion int

	// ConnectionGeneration is incremented every time a control connection is
	// published for this instance. It is the token used to make reconnection
	// an all-or-nothing transition: only the connection whose generation is
	// still current may become the published connection.
	ConnectionGeneration uint64

	// Connected reports whether a control connection is currently published.
	Connected bool

	// State is the lifecycle state of the underlying process.
	State InstanceState
}

// SameInstance reports whether other describes the same plugin process and
// handshake as v, ignoring the connection generation and connectivity. It is
// the identity binding used to decide whether a reconnect is targeting the
// instance this client is bound to.
func (v InstanceView) SameInstance(other InstanceView) bool {
	return v.ID == other.ID &&
		sameAddr(v.Address, other.Address) &&
		v.Protocol == other.Protocol &&
		v.ProtocolVersion == other.ProtocolVersion &&
		v.CoreProtocolVersion == other.CoreProtocolVersion
}

// String returns a compact description of the instance identity.
func (v InstanceView) String() string {
	addr := "<nil>"
	if v.Address != nil {
		addr = v.Address.Network() + ":" + v.Address.String()
	}
	return fmt.Sprintf("id=%s addr=%s protocol=%s version=%d", v.ID, addr, v.Protocol, v.ProtocolVersion)
}

// sameAddr compares two network addresses by their transport and textual
// representation, tolerating nil.
func sameAddr(a, b net.Addr) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Network() == b.Network() && a.String() == b.String()
}

// InstanceView returns an immutable snapshot of the instance this client is
// bound to, and whether the client has been started at all.
func (c *Client) InstanceView() (InstanceView, bool) {
	c.l.Lock()
	defer c.l.Unlock()

	if c.instance == nil {
		return InstanceView{}, false
	}
	return *c.instance, true
}

// LastReconnectError returns the error recorded by the most recent
// Client.Reconnect call, or nil if the most recent call succeeded. The value
// is retained after a failure so callers can diagnose a reconnect that was
// abandoned before it completed.
func (c *Client) LastReconnectError() error {
	c.l.Lock()
	defer c.l.Unlock()
	return c.lastReconnectErr
}

// publishInstanceLocked records the identity of a freshly started (or
// reattached) plugin. The caller must hold c.l.
func (c *Client) publishInstanceLocked(id string) {
	// A fresh instance is never closed, even if a prior Kill or start attempt
	// on this Client set the teardown flag.
	c.closing = false
	c.instance = &InstanceView{
		ID:                  id,
		Address:             c.address,
		Protocol:            c.protocol,
		ProtocolVersion:     c.negotiatedVersion,
		CoreProtocolVersion: CoreProtocolVersion,
		State:               InstanceStateActive,
	}
}

// publishConnectedLocked marks the current instance as having a published
// control connection and advances its generation. The caller must hold c.l.
func (c *Client) publishConnectedLocked() {
	if c.instance == nil {
		return
	}
	next := *c.instance
	next.ConnectionGeneration++
	next.Connected = true
	c.instance = &next
}

// markExitedLocked records that the plugin process has exited. The caller must
// hold c.l.
func (c *Client) markExitedLocked() {
	c.exited = true
	if c.instance == nil {
		return
	}
	next := *c.instance
	next.State = InstanceStateExited
	next.Connected = false
	c.instance = &next
}

// recordReconnectFailure stores err as the last reconnect failure and returns
// it unchanged so callers can return c.recordReconnectFailure(err) directly.
func (c *Client) recordReconnectFailure(err error) error {
	c.l.Lock()
	c.lastReconnectErr = err
	c.l.Unlock()
	return err
}

// Reconnect establishes a new control connection to the plugin instance this
// client is already bound to. It never starts a new plugin process.
//
// Before dialing, Reconnect verifies the recorded discovery evidence: the
// ReattachConfig (when one was supplied), the process identity held by the
// runner, and the handshake endpoint published in the instance view. If any of
// them no longer match, the attempt is refused with
// ErrInstanceIdentityMismatch rather than binding the caller to a different
// instance that happens to occupy the same address.
//
// A successful attempt publishes the new connection together with the process
// identity and negotiated protocol version as one instance view, advancing its
// connection generation. A superseded connection is closed, but a close that
// arrives after the publication can never clear the newly published one.
//
// If ctx is canceled, the attempt times out, the process exits, or the
// handshake fails, the new connection is released and a *ReconnectError is
// returned recording the reason. The previously published connection, if any,
// is left untouched.
//
// When two reconnects race, only one connection is published; the loser
// discards its own connection and returns the connection that won.
func (c *Client) Reconnect(ctx context.Context) (ClientProtocol, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	base, err := c.reconnectSnapshot(ctx)
	if err != nil {
		return nil, c.recordReconnectFailure(err)
	}

	timeout := c.config.ReconnectTimeout
	if timeout <= 0 {
		timeout = c.config.StartTimeout
	}
	if timeout <= 0 {
		timeout = time.Minute
	}
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	candidate, err := c.dialReconnectCandidate(attemptCtx, base)
	if err != nil {
		return nil, c.recordReconnectFailure(err)
	}

	current, won, err := c.installConnection(base, candidate)
	if err != nil {
		_ = releaseConnection(candidate)
		return nil, c.recordReconnectFailure(err)
	}
	if !won {
		// Another reconnect published a connection while this attempt was
		// dialing. Roll our candidate back and hand back the winner.
		_ = releaseConnection(candidate)
	}

	c.l.Lock()
	c.lastReconnectErr = nil
	c.l.Unlock()

	return current, nil
}

// reconnectSnapshot validates that the client is still bound to a live
// instance and returns the view the attempt is based on.
func (c *Client) reconnectSnapshot(ctx context.Context) (InstanceView, error) {
	if err := ctx.Err(); err != nil {
		return InstanceView{}, reconnectContextError(err)
	}

	c.l.Lock()
	defer c.l.Unlock()

	if c.instance == nil || c.closing {
		return InstanceView{}, newReconnectError(ReconnectReasonNotRunning, ErrNoLiveInstance)
	}
	if c.instance.State != InstanceStateActive {
		return InstanceView{}, newReconnectError(ReconnectReasonProcessExited, ErrReconnectProcessExited)
	}
	view := *c.instance

	if err := verifyReattachRecord(c.config.Reattach, view); err != nil {
		return InstanceView{}, newReconnectError(ReconnectReasonIdentity, err)
	}
	if err := verifyProcessIdentity(c.runner, view); err != nil {
		return InstanceView{}, err
	}
	if c.config.GRPCBrokerMultiplex && view.Protocol == ProtocolGRPC {
		// A multiplexed broker session is bound to the original connection,
		// so a replacement control connection cannot be built without also
		// replacing the session. Refuse rather than publish a connection that
		// cannot carry broker traffic.
		return InstanceView{}, newReconnectError(ReconnectReasonHandshake,
			fmt.Errorf("%w: reconnect is not supported with gRPC broker multiplexing", ErrReconnectHandshake))
	}

	if c.doneCtx != nil {
		select {
		case <-c.doneCtx.Done():
			return InstanceView{}, newReconnectError(ReconnectReasonProcessExited, ErrReconnectProcessExited)
		default:
		}
	}

	return view, nil
}

// verifyReattachRecord checks that the discovery record the client was
// configured with still describes the bound instance.
func verifyReattachRecord(cfg *ReattachConfig, view InstanceView) error {
	if cfg == nil {
		return nil
	}

	if cfg.Addr != nil && !sameAddr(cfg.Addr, view.Address) {
		return fmt.Errorf("%w: discovery record address %s:%s does not match instance address %s",
			ErrInstanceIdentityMismatch, cfg.Addr.Network(), cfg.Addr, view)
	}

	// Custom reattach functions may identify something other than a plain
	// PID (for example a container), so the PID check only applies to the
	// built-in process-based reattach path.
	if cfg.ReattachFunc == nil && cfg.Pid != 0 && view.ID != "" && strconv.Itoa(cfg.Pid) != view.ID {
		return fmt.Errorf("%w: discovery record pid %d does not match instance process %s",
			ErrInstanceIdentityMismatch, cfg.Pid, view.ID)
	}

	if cfg.Protocol != "" && cfg.Protocol != ProtocolInvalid && cfg.Protocol != view.Protocol {
		return fmt.Errorf("%w: discovery record protocol %q does not match instance protocol %q",
			ErrInstanceIdentityMismatch, cfg.Protocol, view.Protocol)
	}

	// A reattached client has no handshake of its own, so its negotiated
	// version is only known when the discovery record carries one.
	if cfg.ProtocolVersion != 0 && view.ProtocolVersion != 0 && cfg.ProtocolVersion != view.ProtocolVersion {
		return fmt.Errorf("%w: discovery record protocol version %d does not match instance version %d",
			ErrInstanceIdentityMismatch, cfg.ProtocolVersion, view.ProtocolVersion)
	}

	return nil
}

// verifyProcessIdentity checks that the runner still represents the process
// recorded in the view, and that it is alive when the runner can answer that
// question.
func verifyProcessIdentity(r interface {
	ID() string
}, view InstanceView) error {
	if r == nil || view.ID == "" {
		return nil
	}
	if r.ID() != view.ID {
		return newReconnectError(ReconnectReasonIdentity,
			fmt.Errorf("%w: runner identity %s does not match instance process %s",
				ErrInstanceIdentityMismatch, r.ID(), view.ID))
	}
	if alive, ok := r.(interface{ Alive() bool }); ok && !alive.Alive() {
		return newReconnectError(ReconnectReasonProcessExited, ErrReconnectProcessExited)
	}
	return nil
}

// dialReconnectCandidate creates and health-checks a new control connection.
// The candidate is not published: it is returned to the caller, which either
// installs it or rolls it back.
func (c *Client) dialReconnectCandidate(ctx context.Context, view InstanceView) (ClientProtocol, error) {
	ch := make(chan reconnectCandidate, 1)
	go func() {
		client, err := c.newInstanceClient(ctx, view)
		ch <- reconnectCandidate{client: client, err: err}
	}()

	c.l.Lock()
	var exited <-chan struct{}
	if c.doneCtx != nil {
		exited = c.doneCtx.Done()
	}
	c.l.Unlock()

	select {
	case res := <-ch:
		if res.err != nil {
			return nil, classifyReconnectError(res.err)
		}
		return res.client, nil

	case <-ctx.Done():
		go discardCandidate(ch)
		return nil, reconnectContextError(ctx.Err())

	case <-exited:
		go discardCandidate(ch)
		return nil, newReconnectError(ReconnectReasonProcessExited, ErrReconnectProcessExited)
	}
}

// reconnectCandidate is the result of establishing a candidate connection.
type reconnectCandidate struct {
	client ClientProtocol
	err    error
}

// discardCandidate releases a connection that arrived after its attempt was
// abandoned, if any. It must not ask the plugin to shut down: the instance is
// still the current one.
func discardCandidate(ch <-chan reconnectCandidate) {
	res := <-ch
	if res.client != nil {
		_ = releaseConnection(res.client)
	}
}

// releaseConnection closes conn without asking the plugin process to quit.
// Connections that are not published (candidates that lost a race, or that
// could not be health-checked) are released through this so that rolling them
// back can never tear down the live instance.
func releaseConnection(conn ClientProtocol) error {
	if conn == nil {
		return nil
	}
	if releaser, ok := conn.(ConnectionReleaser); ok {
		return releaser.ReleaseConnection()
	}
	return conn.Close()
}

// newInstanceClient creates a protocol client against the bound instance and
// verifies the control connection with a ping. It is replaceable in tests.
func (c *Client) newInstanceClient(ctx context.Context, view InstanceView) (ClientProtocol, error) {
	if c.dialInstance != nil {
		return c.dialInstance(ctx, view)
	}

	c.l.Lock()
	doneCtx := c.doneCtx
	c.l.Unlock()
	if doneCtx == nil {
		doneCtx = ctx
	}

	var client ClientProtocol
	var err error
	switch view.Protocol {
	case ProtocolNetRPC:
		client, err = newRPCClientContext(ctx, c)
	case ProtocolGRPC:
		client, err = newGRPCClientContext(ctx, doneCtx, c)
	default:
		err = fmt.Errorf("unknown server protocol: %s", view.Protocol)
	}
	if err != nil {
		return nil, err
	}

	if err := pingClientContext(ctx, client); err != nil {
		_ = releaseConnection(client)
		return nil, fmt.Errorf("%w: %w", ErrReconnectHandshake, err)
	}

	return client, nil
}

// pingClientContext performs the protocol health check under ctx.
func pingClientContext(ctx context.Context, client ClientProtocol) error {
	switch c := client.(type) {
	case interface {
		pingContext(context.Context) error
	}:
		return c.pingContext(ctx)
	}

	done := make(chan error, 1)
	go func() { done <- client.Ping() }()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// installConnection publishes candidate as the current control connection if
// no other attempt has superseded base. It returns the published connection
// and whether this attempt won.
func (c *Client) installConnection(base InstanceView, candidate ClientProtocol) (ClientProtocol, bool, error) {
	c.l.Lock()

	if c.instance == nil || c.instance.ConnectionGeneration != base.ConnectionGeneration {
		// Another reconnect published first. Only the winner becomes current.
		current := c.client
		c.l.Unlock()
		if current == nil {
			return nil, false, newReconnectError(ReconnectReasonNotRunning, ErrNoLiveInstance)
		}
		return current, false, nil
	}

	if c.instance.State != InstanceStateActive {
		c.l.Unlock()
		return nil, false, newReconnectError(ReconnectReasonProcessExited, ErrReconnectProcessExited)
	}

	if c.closing {
		c.l.Unlock()
		return nil, false, newReconnectError(ReconnectReasonNotRunning, ErrNoLiveInstance)
	}

	// Close the race with process exit between the snapshot and publication.
	if err := verifyProcessIdentity(c.runner, base); err != nil {
		c.l.Unlock()
		return nil, false, err
	}

	old := c.client
	oldGeneration := c.instance.ConnectionGeneration

	next := *c.instance
	next.ConnectionGeneration = base.ConnectionGeneration + 1
	next.Connected = true
	c.instance = &next
	c.client = candidate
	c.l.Unlock()

	if old != nil {
		// The generation guard ensures this late close of the superseded
		// connection cannot clear the connection just published. Release
		// rather than Close so the old connection's teardown does not ask the
		// plugin process to shut down.
		_ = c.closeConnection(old, oldGeneration)
	}

	return candidate, true, nil
}

// closeConnection releases conn and clears it from the client only if it is
// still the published connection for generation. A close that arrives after a
// newer connection was published therefore leaves the newer connection
// intact.
func (c *Client) closeConnection(conn ClientProtocol, generation uint64) error {
	c.l.Lock()
	if c.instance != nil && c.instance.ConnectionGeneration == generation && c.client == conn {
		c.client = nil
		next := *c.instance
		next.Connected = false
		c.instance = &next
	}
	c.l.Unlock()

	return releaseConnection(conn)
}

// classifyReconnectError maps a dial or health-check error to a reconnect
// failure reason.
func classifyReconnectError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return newReconnectError(ReconnectReasonTimeout,
			fmt.Errorf("%w: %w", ErrReconnectTimeout, err))
	}
	if errors.Is(err, context.Canceled) {
		return newReconnectError(ReconnectReasonCanceled,
			fmt.Errorf("%w: %w", ErrReconnectCanceled, err))
	}
	return newReconnectError(ReconnectReasonHandshake, err)
}

// reconnectContextError builds the failure for an attempt context that ended.
func reconnectContextError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return newReconnectError(ReconnectReasonTimeout,
			fmt.Errorf("%w: %w", ErrReconnectTimeout, err))
	}
	if err == nil {
		err = context.Canceled
	}
	return newReconnectError(ReconnectReasonCanceled,
		fmt.Errorf("%w: %w", ErrReconnectCanceled, err))
}

// newReconnectError builds a ReconnectError with a stable reason.
func newReconnectError(reason ReconnectFailureReason, err error) error {
	return &ReconnectError{Reason: reason, Err: err}
}
