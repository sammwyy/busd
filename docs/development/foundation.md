# Foundation guide

This guide defines the project rules established in roadmap phase 0. It is the
reference for contributors through the BUS/1-preview wire-protocol phase.

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
 ├── busd-broker (broker state and routing)
 │    ├── busd-policy (authorization boundary)
 │    └── busd-protocol (transport-independent model and future codec)
 ├── busd-client (public application API)
 │    ├── busd-protocol
 │    └── busd-transport-unix (native connection setup)
 └── busd-transport-unix (Linux socket primitives)
```

| Crate | Owns | Must not own |
| --- | --- | --- |
| `busd-protocol` | Transport-independent types and the BUS/1-preview codec | Sockets, broker state, policy, or D-Bus integration |
| `busd-policy` | Authenticated credentials and authorization decisions | Socket credential collection or message routing |
| `busd-broker` | Peer lifecycle, namespaces, subscriptions, routing, and delivery state | Listener lifecycle or application payload semantics |
| `busd-transport-unix` | Linux `AF_UNIX` transport, packet boundaries, and future FD passing | BUS routing and policy decisions |
| `busd-client` | Stable application-facing API and connection orchestration | A second wire protocol or direct broker-state access |
| `busd` | Process startup, configuration, transport wiring, and operations | Protocol model or business API semantics |

The native transport is the only crate allowed to contain the small, audited
unsafe FFI boundary needed for Linux sockets. All other crates remain
`forbid(unsafe_code)`.

## Current implementation boundary

The current implementation is a scaffold, not a complete BUS/1 broker:

- `busd-protocol` owns the versioned BUS/1-preview ABI, canonical codec, and
  packet validation limits.
- `busd-broker` stores peers, exclusive namespace ownership, and unowned channel
  subscriptions, but does not execute routed messages.
- `busd-transport-unix` preserves bounded native packet boundaries below the
  BUS/1 API.
- `busd` authenticates Unix peers, routes namespace, direct, client-ID, channel,
  and authorized broadcast messages, manages in-flight request and acknowledged
  delivery deadlines and retries, and releases broker state on disconnect.

Do not expose raw native packets through an application API.

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
