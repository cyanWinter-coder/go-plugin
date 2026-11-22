# Versioned Discovery Lifecycle

Reattaching to a running plugin is traditionally driven by a PID and a
listener address (`ReattachConfig.Pid` + `ReattachConfig.Addr`). Both of these
can be reused: a process upgrade can leave a stale PID entry behind, and a
restarted plugin can bind a port that a previous instance used. Reattaching on
that evidence alone can bind the host to the wrong version of a plugin.

The versioned discovery lifecycle replaces PID/port guessing with a persisted,
state-machine-driven record of each plugin instance. It is opt-in: clients that
do not configure it keep the legacy behavior exactly.

## Identity and generation

Every discovery record is keyed by a `PluginIdentity`, which identifies the
_program_ rather than the process:

* the executable path,
* an optional digest of the executable (so an upgraded binary at the same path
  is a different identity),
* the negotiated application protocol version,
* the negotiated transport (`grpc` or `netrpc`).

Within one identity, each start gets a strictly monotonic **generation**. The
PID (or custom runner ID) and the listener address are recorded on the record
as evidence, but they are never the key and never the sole basis for
reattachment.

Build an identity with `NewPluginIdentity`, which hashes the executable:

```go
identity, err := plugin.NewPluginIdentity(executablePath, "sha256", handshake.ProtocolVersion, plugin.ProtocolGRPC)
```

## State machine

A record moves in one direction through four states:

```
pending ──commit──▶ active ──kill/supersede/expire──▶ tombstoned ──cleanup──▶ cleaned (file deleted)
   └────────tombstone──────────▶ tombstoned
```

* **pending** — the process has started and a generation has been claimed, but
  the full handshake has not completed yet. Pending records are never
  reattachable.
* **active** — the full handshake committed. This is the only reattachable
  state, and only the most recently committed generation of an identity is
  current.
* **tombstoned** — killed, superseded by a newer generation, expired, or found
  dead during a liveness check. A tombstoned record can never be reattached.
* **cleaned** — all owned resources have been released and the record file is
  deleted. This is the single terminal state.

Illegal transitions (for example reactivating a tombstoned record) are rejected
with `ErrRecordTerminal`.

### Upgrade safety

`Claim` reserves the next generation without touching the identity's current
active record. `Commit` is the only operation that can replace it, and it runs
only after the full handshake succeeds:

1. the new process starts and claims a pending generation;
2. the old active generation keeps serving and stays reattachable;
3. the new handshake completes and `Commit` makes the new generation current,
   tombstoning the old one;
4. if the handshake fails, the pending record is tombstoned and the old
   instance is untouched.

A reused PID or port therefore cannot redirect a host to the wrong version:
the versioned identity and generation would not match, and a superseded
generation is rejected even if its address still answers.

## Reattachment

Reattachment always re-validates the record through the registry, which checks
the state, the current-generation pointer, the expiry time, and that the
process is still alive (a dial of the negotiated address plus any configured
`LivenessChecker`):

```go
reattachCfg, err := plugin.ReattachConfigFromRecord(ctx, registry, identity)
if err != nil {
    // no reattachable instance: start a fresh plugin instead
}
client := plugin.NewClient(&plugin.ClientConfig{
    Reattach:        reattachCfg,
    HandshakeConfig: handshake,
    Plugins:         plugins,
})
```

Callers that cached a specific record can pin the generation with
`ReattachConfigForGeneration` / `ReattachGeneration` so they are never silently
redirected to a newer instance.

## Bounded cleanup and ownership

Resources owned exclusively by an instance (its Unix socket directory, plus any
`CleanupFunc` a host registers) are recorded on the claim and released when the
record leaves the active state. Cleanup is:

* **bounded** — every attempt runs under `CleanupTimeout`, retried at most
  `MaxCleanupAttempts` times with `CleanupRetryInterval` backoff;
* **non-blocking** — scheduled asynchronously, so a failing cleanup of an old
  instance never blocks the new instance from committing;
* **single-shot** — each record has one cleanup job; concurrent `Kill`,
  `Tombstone` and `Sweep` calls converge on the same terminal state and the
  cleanup function runs at most once;
* **durable** — resource paths are persisted in the record, so reopening the
  registry (including after a host restart) re-drives cleanup for anything left
  behind. Pending records past `ClaimTTL` (an abandoned handshake) and expired
  active records are tombstoned; already-tombstoned records are cleaned.

`Sweep` reports per-generation cleanup failures instead of returning an error,
so one stuck resource cannot hide the others. A record whose cleanup keeps
failing stays tombstoned and un-reattachable; it is never reactivated.

## Persistence and concurrency

Records are JSON files in the registry directory, written atomically
(temp-file + rename) so a crash can never leave a half-written reattachable
record. All mutating operations take an inter-process advisory lock on the
directory (POSIX `flock` / Windows `LockFileEx`) layered over an in-process
mutex, and every operation re-syncs its in-memory index from disk under that
lock. Multiple host processes sharing one registry directory therefore observe
one authoritative sequence of generations and one terminal state per record.

## Wiring it into a Client

Set `DiscoveryRegistry` and `DiscoveryIdentity` on `ClientConfig`. The client
then claims after the process starts, commits when its handshake completes,
and tombstones the record from `Kill()`:

```go
registry, err := plugin.NewRegistry(ctx, plugin.RegistryConfig{
    Directory: discoveryDir,
    RecordTTL: 24 * time.Hour,
})
if err != nil { /* ... */ }

client := plugin.NewClient(&plugin.ClientConfig{
    Cmd:               exec.Command(executablePath),
    HandshakeConfig:   handshake,
    Plugins:           plugins,
    AllowedProtocols:  []plugin.Protocol{plugin.ProtocolGRPC},
    DiscoveryRegistry: registry,
    DiscoveryIdentity: identity,
})
```

When these fields are nil the client behaves exactly as before: no records are
written and `Discover`, the runner interface, the discovery directory layout
and the server mux are unchanged.
