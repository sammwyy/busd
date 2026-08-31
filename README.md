# busd

`busd` is a generic local message broker for Linux IPC. It routes opaque binary payloads; applications own payload semantics.

The project is an early BUS/1 implementation. It establishes crate boundaries,
the BUS/1-preview frame ABI and codec, authenticated native sessions, namespace
and peer routing, filtered channels, broker discovery, requests, acknowledgements,
deadlines, bounded retries, and versioned native authorization policy. The Unix
transport also provides a bounded, ownership-safe `SCM_RIGHTS` primitive.
FD-bearing BUS messages and memfd payload routing are not enabled by the broker
until a negotiated protocol extension defines their forwarding semantics.

## Run

Rust 1.85 or newer is required.

```bash
cargo run -p busd-server -- daemon --socket /tmp/busd.sock
```

The built-in daemon policy permits ordinary local messaging but denies broadcast,
monitoring, and D-Bus registration. Use `--policy /etc/busd/policy.conf` for a
production policy; see the [operations guide](docs/operations.md).

The native listener uses `AF_UNIX` with `SOCK_SEQPACKET`. The process intentionally leaves an existing socket path untouched; remove a stale development socket before restarting it.

```bash
rm -f /tmp/busd.sock
```

The default socket is `/run/busd/busd.sock`; its parent directory must be managed by the service installation.

## Client status

`busd-client` opens an authenticated native session, performs `HELLO`/`WELCOME`,
supports typed signals, requests, responses, claims, subscriptions, namespace
resolution, acknowledgements, and bounded receiver-side deduplication. Use
`ReconnectingBus` when a client must restore claims and subscriptions after a
daemon restart; the restored session always accepts its new peer ID.

See the [BUS/1-preview wire protocol](docs/wire-protocol.md) for the packet ABI.

## Workspace

- `bus-protocol`: transport-independent BUS/1 model and preview codec.
- `busd-client`: application-facing dependency; it re-exports BUS/1 types and opens native connections.
- `bus-policy`: authorization boundary and authenticated credentials.
- `bus-broker`: in-memory peers, exclusive namespaces, and channel subscriptions.
- `bus-transport-unix`: Linux native socket primitives.
- `bus-transport-dbus`: optional direct D-Bus connections and name registration.
- `busd-server`: broker executable assembly.

## Scope

BUS/1 keeps peer identity, claimed client identity, credentials, namespace ownership, and unowned channel subscriptions separate. It does not define application APIs or payload encodings.

See the [documentation index](docs/README.md) for the project documentation,
the [BUS/1 specification](docs/SPECS.md) for the design, and
[ROADMAP.md](ROADMAP.md) for implementation milestones.

Applications should depend on `busd-client`, not the transport crate directly:

```rust
use bus_client::{Bus, ConnectOptions};

let bus = Bus::connect(ConnectOptions::new("/run/busd/busd.sock"))?;
println!("connected as {}", bus.peer_id());
bus.disconnect()?;
```

Applications that need direct D-Bus access may opt into `bus-transport-dbus`.
It provides transport primitives only; mapping D-Bus methods to BUS APIs is a
separate semantic adapter. The `busd` binary keeps D-Bus disabled unless built
with `--features dbus`.

For a reconnecting client, retain durable session state in the public client
wrapper:

```rust,no_run
use bus_client::{Channel, ConnectOptions, Namespace, ReconnectingBus};

let mut bus = ReconnectingBus::connect(ConnectOptions::default())?;
bus.claim(Namespace::parse("bus://example")?, Default::default())?;
bus.subscribe(Channel::parse("events")?, Vec::new())?;
let _new_peer = bus.reconnect()?;
```

## License

Copyright 2026 Sammwy. Licensed under [Apache License, Version 2.0](LICENSE).
