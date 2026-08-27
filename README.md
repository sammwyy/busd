# busd

`busd` is a generic local message broker for Linux IPC. It routes opaque binary payloads; applications own payload semantics.

The project is an early BUS/1 implementation scaffold. It establishes the crate boundaries and validates the core broker invariants, but the BUS/1 wire ABI, framing, FD passing, routing execution, acknowledgement tracking, and production policy configuration are not implemented yet.

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

## Debug client

`busd` also provides raw-packet diagnostics over the native transport. These commands do not encode BUS/1 messages and must not be used as an application API.

```bash
cargo run -p busd -- client --socket /tmp/busd.sock
cargo run -p busd -- send --socket /tmp/busd.sock --hex DEADBEEF
```

The interactive client accepts one hexadecimal packet per line and exits with `quit`. The daemon reports each received debug packet's size.

## Workspace

- `bus-protocol`: transport-independent BUS/1 model.
- `bus-client`: application-facing dependency; it re-exports BUS/1 types and opens native connections.
- `bus-policy`: authorization boundary and authenticated credentials.
- `bus-broker`: in-memory peers, exclusive namespaces, and channel subscriptions.
- `bus-transport-unix`: Linux native socket primitives.
- `busd`: broker executable assembly.

## Scope

BUS/1 keeps peer identity, claimed client identity, credentials, namespace ownership, and unowned channel subscriptions separate. It does not define application APIs or payload encodings.

See [SPECS.md](SPECS.md) for the design specification.

Applications should depend on `bus-client`, not the transport crate directly:

```rust
use bus_client::{Bus, ConnectOptions};

let bus = Bus::connect(ConnectOptions::new("/run/busd/busd.sock"))?;
```

## License

Copyright 2026 Sammwy. Licensed under [Apache License, Version 2.0](LICENSE).
