// Copyright IBM Corp. 2016, 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"errors"
	"fmt"
)

// Sentinel errors describing the gRPC broker's deterministic session and
// capacity boundaries. Callers can use [errors.Is] to recognise them without
// parsing error strings.
var (
	// ErrBrokerSessionClosed is returned by gRPC broker calls (Dial, Accept
	// and the multiplexing knock handshake) when the bidirectional stream
	// session they were attached to has been closed before the call could
	// complete. A call fails at most once with this error: late messages that
	// arrive on a replaced stream are dropped and never delivered to the
	// replacement session.
	ErrBrokerSessionClosed = errors.New("grpc broker stream session closed")

	// ErrBrokerQueueFull is returned when a gRPC broker connection's bounded
	// forwarding queue is full. The queue is per broker connection, so
	// backpressure on one connection never blocks another. The service ID of
	// the message that could not be queued is available via
	// [BrokerQueueFullError.ServiceID].
	ErrBrokerQueueFull = errors.New("grpc broker forwarding queue full")

	// ErrBrokerClosed is returned after the gRPC broker itself has been shut
	// down permanently.
	ErrBrokerClosed = errors.New("grpc broker closed")

	// ErrBrokerStreamSuperseded is returned by a StartStream invocation that
	// lost the race against a concurrent duplicate Attach/reconnect: only one
	// bidirectional stream is current at a time, and the losing stream is shut
	// down instead of replacing the winner.
	ErrBrokerStreamSuperseded = errors.New("grpc broker stream superseded by a newer stream")
)

// BrokerSessionClosedError wraps [ErrBrokerSessionClosed] with the service ID
// whose call failed when its stream session ended.
type BrokerSessionClosedError struct {
	// ServiceID is the brokered service the failed call was attached to. It is
	// zero when the failure is not scoped to a specific service, e.g. when the
	// whole broker is shutting down.
	ServiceID uint32
}

func (e *BrokerSessionClosedError) Error() string {
	return fmt.Sprintf("%s: service_id=%d", ErrBrokerSessionClosed, e.ServiceID)
}

func (e *BrokerSessionClosedError) Unwrap() error { return ErrBrokerSessionClosed }

// BrokerQueueFullError wraps [ErrBrokerQueueFull] with the service ID of the
// message that could not be enqueued. It lets callers apply per-service
// backpressure even though the queue itself is per-connection.
type BrokerQueueFullError struct {
	// ServiceID is the brokered service whose message could not be queued.
	ServiceID uint32
}

func (e *BrokerQueueFullError) Error() string {
	return fmt.Sprintf("%s: service_id=%d", ErrBrokerQueueFull, e.ServiceID)
}

func (e *BrokerQueueFullError) Unwrap() error { return ErrBrokerQueueFull }

// DefaultGRPCBrokerQueueCapacity is the default capacity of the bounded send
// and receive forwarding queues used by a gRPC broker connection. It can be
// overridden per client with ClientConfig.GRPCBrokerQueueCapacity.
const DefaultGRPCBrokerQueueCapacity = 64
