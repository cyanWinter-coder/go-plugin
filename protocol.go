// Copyright IBM Corp. 2016, 2025
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"io"
	"net"
)

// Protocol is an enum representing the types of protocols.
type Protocol string

const (
	ProtocolInvalid Protocol = ""
	ProtocolNetRPC  Protocol = "netrpc"
	ProtocolGRPC    Protocol = "grpc"
)

// ServerProtocol is an interface that must be implemented for new plugin
// protocols to be servers.
type ServerProtocol interface {
	// Init is called once to configure and initialize the protocol, but
	// not start listening. This is the point at which all validation should
	// be done and errors returned.
	Init() error

	// Config is extra configuration to be outputted to stdout. This will
	// be automatically base64 encoded to ensure it can be parsed properly.
	// This can be an empty string if additional configuration is not needed.
	Config() string

	// Serve is called to serve connections on the given listener. This should
	// continue until the listener is closed.
	Serve(net.Listener)
}

// ClientProtocol is an interface that must be implemented for new plugin
// protocols to be clients.
type ClientProtocol interface {
	io.Closer

	// Dispense dispenses a new instance of the plugin with the given name.
	Dispense(string) (interface{}, error)

	// Ping checks that the client connection is still healthy.
	Ping() error
}

// ConnectionReleaser is an optional interface a ClientProtocol may implement
// to release a connection without asking the plugin process to shut down.
//
// Close is the graceful shutdown used by Client.Kill: for the built-in
// protocols it asks the plugin to quit. A connection that is being rolled back
// from a failed or superseded reconnect must not do that, because the plugin
// process is still the current instance. Implementations that do not provide
// this interface fall back to Close.
type ConnectionReleaser interface {
	ReleaseConnection() error
}
