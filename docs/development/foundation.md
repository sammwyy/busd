# Foundation guide

This guide defines the project rules established in roadmap phase 0. It is the
reference for contributors before the BUS/1 wire protocol is introduced.

## Supported environments

`busd` currently supports native Linux only. Its transport uses Unix-domain
`SOCK_SEQPACKET` sockets and is compiled behind Linux-specific code.

| Area | Supported baseline | CI coverage |
| --- | --- | --- |
| Operating system | Linux on a Unix-domain-socket-capable host | `ubuntu-latest` |
| Architecture | `x86_64-unknown-linux-gnu` | GitHub-hosted x86_64 runner |
| Rust | Rust 1.85 (MSRV) and the current stable toolchain | Both toolchains |

Other Linux architectures may work, but are not yet part of the supported test
matrix. Non-Linux targets are unsupported until a transport implementation and
CI coverage are added.

## Workspace ownership

The crate boundaries are intentional. Add a dependency only in the direction
shown below; avoid moving functionality across a boundary merely for
convenience.

```text
busd (binary assembly)
 ├── bus-broker (broker state and routing)
 │    ├── bus-policy (authorization boundary)
 │    └── bus-protocol (transport-independent model and future codec)
 ├── bus-client (public application API)
 │    ├── bus-protocol
 │    └── bus-transport-unix (native connection setup)
 └── bus-transport-unix (Linux socket primitives)
```

| Crate | Owns | Must not own |
| --- | --- | --- |
| `bus-protocol` | Transport-independent types and, later, the BUS/1 codec | Sockets, broker state, policy, or D-Bus integration |
| `bus-policy` | Authenticated credentials and authorization decisions | Socket credential collection or message routing |
| `bus-broker` | Peer lifecycle, namespaces, subscriptions, routing, and delivery state | Listener lifecycle or application payload semantics |
| `bus-transport-unix` | Linux `AF_UNIX` transport, packet boundaries, and future FD passing | BUS routing and policy decisions |
| `bus-client` | Stable application-facing API and connection orchestration | A second wire protocol or direct broker-state access |
| `busd` | Process startup, configuration, transport wiring, and operations | Protocol model or business API semantics |

The native transport is the only crate allowed to contain the small, audited
unsafe FFI boundary needed for Linux sockets. All other crates remain
`forbid(unsafe_code)`.

## Current implementation boundary

The current implementation is a scaffold, not a complete BUS/1 broker:

- `bus-protocol` models core identifiers, headers, message kinds, and
  acknowledgement policy, but does not yet define the wire ABI.
- `bus-broker` stores peers, exclusive namespace ownership, and unowned channel
  subscriptions, but does not execute routed messages.
- `bus-transport-unix` sends and receives bounded raw packets for diagnostics.
- `busd` accepts native connections but does not yet dispatch BUS/1 frames.

Do not present raw diagnostic packets as a supported application protocol.

## Required local checks

Run these commands before opening a change:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps
./scripts/check-doc-links.sh
```

The test suite includes a real Unix-socket test. It needs an environment that
permits creating Unix-domain sockets; restricted sandboxes may need an approved
execution mode.

## Documentation rules

- Keep intended protocol behavior in the [BUS/1 specification](../SPECS.md).
- Keep current, implemented behavior in `README.md` and this guide.
- Add future implementation work to [the roadmap](../../ROADMAP.md) before
  starting a new phase.
- Use relative links within Markdown. Run `./scripts/check-doc-links.sh` after
  adding or moving documentation.
- Update this guide when a crate gains responsibility or the support matrix changes.
