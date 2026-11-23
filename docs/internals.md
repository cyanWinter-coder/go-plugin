# go-plugin Internals

This section discusses the internals of how go-plugin works.

go-plugin operates by either _serving_ a plugin or being a _client_
connecting to a remote plugin. The "client" is the host process or the
process that itself uses plugins. The "server" is the plugin process.

For a server:

  1. Output handshake to stdout
  2. Wait for connection on control address
  3. Serve plugins over control address

For a client:

  1. Launch a plugin binary
  2. Read and verify handshake from plugin stdout
  3. Connect to plugin control address using desired protocol
  4. Dispense plugins using control connection

## Handshake

The handshake is the initial communication between a plugin and a host
process to determine how the host process can connect and communicate to
the plugin. This handshake is done over the plugin process's stdout.

The `go-plugin` library itself handles the handshake when using the
`Server` to serve a plugin. **You do not need to understand the internals
of the handshake,** unless you're building a go-plugin compatible plugin
in another language.

The handshake is a single line of data terminated with a newline character
`\n`. It looks like the following:

```
1|3|unix|/path/to/socket|grpc
```

The structure is:

```
CORE-PROTOCOL-VERSION | APP-PROTOCOL-VERSION | NETWORK-TYPE | NETWORK-ADDR | PROTOCOL
```

Where:

  * `CORE-PROTOCOL-VERSION` is the protocol version for go-plugin itself.
    The current value is `1`. Please use this value. Any other value will
    cause your plugin to not load.

  * `APP-PROTOCOL-VERSION` is the protocol version for the application data.
    This is determined by the application. You must reference the documentation
    for your application to determine the desired value.

  * `NETWORK-TYPE` and `NETWORK-ADDR` are the networking information for
    connecting to this plugin. The type must be "unix" or "tcp". The address
    is a path to the Unix socket for "unix" and an IP address for "tcp".

  * `PROTOCOL` is the named protocol that the connection will use. If this
    is omitted (older versions), this is "netrpc" for Go net/rpc. This can
    also be "grpc". This is the protocol that the plugin wants to speak to
    the host process with.

## Reconnecting to a Running Instance

A plugin process outlives the control connection when the host loses it (for
example, after a host restart or a dropped socket). `Client.Reconnect`
re-establishes the control connection to the *same* plugin process; it never
launches a new one.

The client publishes an immutable `InstanceView` as soon as the handshake
completes. The view binds together the process identity (the runner ID, or the
PID), the handshake endpoint, the negotiated protocol and the protocol
version. Its `ConnectionGeneration` advances each time a control connection is
published.

`Reconnect` is an all-or-nothing transition:

  1. It verifies the bound evidence. When `ClientConfig.Reattach` is set, the
     record's address, PID, protocol and protocol version must still match the
     view; the runner's identity must match too, and it must report alive when
     it can. A record that reuses a PID or port but describes a different
     process is refused with `ErrInstanceIdentityMismatch`.
  2. It dials the recorded endpoint and pings the new connection. Nothing is
     published yet, so a failed attempt leaves the previous connection
     untouched.
  3. It publishes the new connection against the generation it started from.
     If another reconnect published first, this attempt has lost: its own
     connection is released and the winner is returned. Releasing a connection
     uses `ConnectionReleaser.ReleaseConnection` where available, so rolling
     back never asks the plugin process to shut down.

A canceled `Reconnect` returns `ErrReconnectCanceled`, a timed-out one
`ErrReconnectTimeout`, an exited process `ErrReconnectProcessExited`, and an
unreachable or unhealthy endpoint `ErrReconnectHandshake`. Each is wrapped in a
`*ReconnectError` that records the reason, and the last failure is available
from `Client.LastReconnectError`.

Connections that are superseded are released with a generation guard, so a
late close from an old connection can never clear the currently published one.

Reconnection is not supported with gRPC broker multiplexing, because the
multiplexed broker session is tied to the original connection. The attempt is
refused rather than publishing a connection that cannot carry broker traffic.

## Environment Variables

When serving a plugin over TCP, the following environment variables can be
specified to restrict the port that will be assigned to be from within a
specific range. If not values are provided, the port will be randomly assigned
by the operating system.

 * `PLUGIN_MIN_PORT`: Specifies the minimum port value that will be assigned to the listener.
 
 * `PLUGIN_MAX_PORT`: Specifies the maximum port value that will be assigned to the listener.
