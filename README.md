# busd

`busd` is a generic local message broker for Linux IPC. It routes opaque binary payloads; applications own payload semantics.

The project is an early BUS/1 implementation. It establishes crate boundaries,
the BUS/1-preview frame ABI and codec, authenticated native sessions, namespace
and peer routing, filtered channels, and broker discovery state. FD passing,
acknowledgement tracking, and production policy configuration are not implemented yet.

## Run

Rust 1.85 or newer is required.

```bash
cargo run -p busd -- daemon --socket /tmp/busd.sock
```

The native listener uses `AF_UNIX` with `SOCK_SEQPACKET`. The process intentionally leaves an existing socket path untouched; remove a stale development socket before restarting it.

```bash
rm -f /tmp/busd.sock
```

The default socket is `/run/busd/busd.sock`; its parent directory must be managed by the service installation.

## Client status

`bus-client` opens an authenticated native session, performs `HELLO`/`WELCOME`,
supports claims, subscriptions, namespace resolution, and message delivery.

See the [BUS/1-preview wire protocol](docs/wire-protocol.md) for the packet ABI.

## Workspace

- `bus-protocol`: transport-independent BUS/1 model and preview codec.
- `bus-client`: application-facing dependency; it re-exports BUS/1 types and opens native connections.
- `bus-policy`: authorization boundary and authenticated credentials.
- `bus-broker`: in-memory peers, exclusive namespaces, and channel subscriptions.
- `bus-transport-unix`: Linux native socket primitives.
- `busd`: broker executable assembly.

## Scope

BUS/1 keeps peer identity, claimed client identity, credentials, namespace ownership, and unowned channel subscriptions separate. It does not define application APIs or payload encodings.

See the [documentation index](docs/README.md) for the project documentation,
the [BUS/1 specification](docs/SPECS.md) for the design, and
[ROADMAP.md](ROADMAP.md) for implementation milestones.

Applications should depend on `bus-client`, not the transport crate directly:

```rust
use bus_client::{Bus, ConnectOptions};

let bus = Bus::connect(ConnectOptions::new("/run/busd/busd.sock"))?;
println!("connected as {}", bus.peer_id());
bus.disconnect()?;
```

## License

Copyright 2026 Sammwy. Licensed under [Apache License, Version 2.0](LICENSE).
