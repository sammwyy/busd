# busd roadmap

This roadmap turns the BUS/1 design into a usable local Linux message bus. It
prioritizes the native protocol and a safe broker before optional compatibility
layers. Checkboxes record repository status, not an estimated schedule.

## Release milestones

- [x] **M0 — Project scaffold:** crate boundaries, core protocol types, in-memory
  namespace and subscription state, policy boundary, and Linux
  `AF_UNIX`/`SOCK_SEQPACKET` primitives.
- [ ] **M1 — Wire protocol preview:** a documented, versioned BUS/1 packet ABI
  and a round-trippable codec.
- [ ] **M2 — Native broker alpha:** authenticated connections, handshake, and
  live namespace, peer, and channel routing.
- [ ] **M3 — Reliable messaging beta:** requests, responses, acknowledgements,
  deadlines, and explicit failure results.
- [ ] **M4 — Secure native 1.0:** configurable policy, bounded resource use,
  observability, and release-quality operational documentation.
- [ ] **M5 — Optional extensions:** D-Bus interoperability and other negotiated
  capabilities, delivered independently of native BUS/1 stability.

## Phase 0 — Foundation and project rules

**Milestone:** M0 — complete.

- [x] Split the project into protocol, client, policy, broker, Unix transport,
  and daemon crates.
- [x] Model peer IDs, client IDs, namespaces, channels, claimed headers,
  message kinds, and acknowledgement policies.
- [x] Enforce exclusive namespace ownership and unowned channel subscriptions in
  broker state.
- [x] Bind and connect native `AF_UNIX` `SOCK_SEQPACKET` sockets.
- [x] Keep raw transport diagnostics explicitly separate from the application API.
- [x] Add a CI workflow that runs formatting, Clippy, unit tests, and documentation checks.
- [x] Define the project's supported Linux and Rust-version test matrix.

**Exit criteria**

- [x] The workspace builds and its current unit tests demonstrate the stated core invariants.
- [x] Every later phase can add functionality without changing crate ownership casually.

## Phase 1 — Freeze the BUS/1 wire protocol

**Milestone:** M1 — Wire protocol preview.

- [ ] Write `docs/wire-protocol.md` as the normative packet-format specification.
- [ ] Define a bounded frame envelope: magic/version, frame kind, flags, length,
  destination selector, message identifiers, headers, and payload.
- [ ] Define a canonical binary representation for names, IDs, header values,
  filters, acknowledgement policies, statuses, and protocol errors.
- [ ] Specify byte order, size limits, duplicate-header handling, reserved fields,
  and forward-compatible unknown-field behavior.
- [ ] Define control frames for `HELLO`, `WELCOME`, claim, subscribe,
  unsubscribe, and protocol errors.
- [ ] Implement encoder and decoder types in `bus-protocol` without socket dependencies.
- [ ] Reject malformed, truncated, oversized, and non-canonical frames before allocating unbounded memory.
- [ ] Add golden vectors plus encode/decode, boundary, and fuzz/property tests.

**Exit criteria**

- [ ] Independent encoder and decoder implementations can exchange golden vectors.
- [ ] The native transport no longer accepts application-defined raw packets as BUS messages.
- [ ] The packet ABI has an explicit BUS/1-preview compatibility policy.

## Phase 2 — Establish authenticated sessions

**Milestone:** M2 — Native broker alpha.

- [ ] Obtain authenticated peer credentials (`PID`, `UID`, and `GID`) from the
  Unix socket and attach them as broker-owned metadata.
- [ ] Implement the `HELLO`/`WELCOME` handshake, peer ID assignment, claimed
  headers, and capability negotiation.
- [ ] Validate all claimed metadata and ensure clients cannot forge broker headers.
- [ ] Connect accepted transport sessions to `bus-broker` peer lifecycle state.
- [ ] Release namespaces and subscriptions automatically on disconnect.
- [ ] Add structured protocol-error responses and disconnect rules for invalid peers.
- [ ] Replace the daemon's debug receive loop with a session dispatcher.
- [ ] Expose typed `Bus::connect`, handshake, and orderly disconnect behavior in `bus-client`.

**Exit criteria**

- [ ] Two native clients can handshake concurrently and observe distinct ephemeral peer IDs.
- [ ] A disconnected provider's namespace becomes claimable without manual cleanup.
- [ ] Credential-derived policy decisions never rely on client-claimed identity.

## Phase 3 — Implement routing and discovery

**Milestone:** M2 — Native broker alpha.

- [ ] Implement namespace routing to exactly one current provider.
- [ ] Implement direct peer routing and response correlation.
- [ ] Implement channel publication to explicit subscribers only.
- [ ] Implement client-ID selection (`FIRST`, `ANY`, and `ALL`) with documented
  behavior when no peer matches.
- [ ] Implement initial structural header filters: `EXISTS`, `NOT_EXISTS`,
  `EQUAL`, `NOT_EQUAL`, and `PREFIX`.
- [ ] Implement controlled global broadcast; keep it disabled unless policy allows it.
- [ ] Add discovery operations for peers, namespaces, subscriptions, and broker capabilities.
- [ ] Emit lifecycle events for peer connection, disconnection, namespace acquisition,
  release, and ownership change.
- [ ] Test routing cardinality, filter matching, cleanup, and all no-recipient paths.

**Exit criteria**

- [ ] A client can claim a namespace, another client can resolve it, and a typed
  message reaches only its provider.
- [ ] Channel subscribers receive matching messages while unrelated peers receive none.
- [ ] Discovery output clearly distinguishes claimed metadata from authenticated metadata.

## Phase 4 — Requests, acknowledgements, and reliability

**Milestone:** M3 — Reliable messaging beta.

- [ ] Implement `SIGNAL`, `REQUEST`, and `RESPONSE` frames with stable message
  and correlation IDs.
- [ ] Implement `ACK_NONE`, `ACK_ACCEPTED`, `ACK_RECEIVED`, and `ACK_PROCESSED`.
- [ ] Implement multicast acknowledgement requirements: `NONE`, `ANY`, `ALL`,
  and `MINIMUM(N)`.
- [ ] Implement request routing policies: `EXACT`, `FIRST`, `FIRST_SUCCESS`, and `ALL`.
- [ ] Enforce deadlines and return explicit `TIMEOUT`, `NO_RECIPIENT`,
  `RECIPIENT_DISCONNECTED`, and `DELIVERY_FAILED` outcomes.
- [ ] Add broker-managed retry scheduling with bounded exponential backoff.
- [ ] Retain one logical message ID across retries and provide bounded receiver-side deduplication support.
- [ ] Document at-least-once delivery and the absence of exactly-once execution guarantees.
- [ ] Add deterministic tests for timeout, retry, duplicate, and recipient-loss races.

**Exit criteria**

- [ ] A request completes with a response, a documented failure result, or a deadline.
- [ ] Retried messages retain their ID and cannot silently claim exactly-once execution.
- [ ] A slow or disconnected recipient cannot make an unrelated request wait indefinitely.

## Phase 5 — Policy, isolation, and operational safety

**Milestone:** M4 — Secure native 1.0.

- [ ] Define a versioned, documented policy configuration format in `bus-policy`.
- [ ] Authorize connection, claims, sends, publishes, subscriptions, broadcasts,
  acknowledgements, monitoring, and future D-Bus registration separately.
- [ ] Support rules based on broker-authenticated credentials and, where available,
  executable path, security label, and cgroup.
- [ ] Treat client IDs and claimed headers as untrusted unless an explicit policy rule uses them.
- [ ] Set configurable limits for packet size, queued bytes, queued messages,
  in-flight requests, subscriptions, and namespace claims.
- [ ] Add queue backpressure and priority scheduling; never allow priority to bypass authorization.
- [ ] Add safe handling for oversized packets, malformed frames, allocation pressure,
  and abusive peers.
- [ ] Add privileged monitoring with metadata redaction and policy enforcement.
- [ ] Add structured logs, metrics, health checks, and an operational troubleshooting guide.
- [ ] Fuzz the frame parser and broker state transitions; run sanitizers or equivalent
  memory-safety checks where practical.

**Exit criteria**

- [ ] A default installation is deny-by-policy for privileged operations and has documented safe defaults.
- [ ] Resource exhaustion and malformed peers are isolated to the offending connection.
- [ ] Operators can determine who connected, what was denied, and why without logging opaque payloads by default.

## Phase 6 — Complete the native client and Linux data paths

**Milestone:** M4 — Secure native 1.0.

- [ ] Provide ergonomic typed client operations for claims, discovery, subscriptions,
  signals, requests, responses, and acknowledgements.
- [ ] Implement reconnection helpers that restore headers, namespace claims, and subscriptions while accepting a new peer ID.
- [ ] Support `SCM_RIGHTS` file-descriptor passing with strict descriptor-count and
  ownership rules.
- [ ] Define memfd-based large-payload transfer as an optional negotiated capability.
- [ ] Add end-to-end integration tests using real Unix sockets and separate processes.
- [ ] Ship a system-service installation example, socket-directory ownership rules,
  and upgrade/restart behavior.
- [ ] Replace debug-client documentation with supported typed-client examples.

**Exit criteria**

- [ ] A production client can use the public library without touching raw transport packets.
- [ ] FD passing and large-payload support are either safely negotiated or explicitly unavailable.
- [ ] Restart and reconnect behavior is tested and documented.

## Phase 7 — Compatibility and optional extensions

**Milestone:** M5 — Optional extensions.

- [ ] Define extension registration and capability negotiation rules before adding new frame behavior.
- [ ] Implement `bus-transport-dbus` behind the existing `dbus` feature flag.
- [ ] Choose and document supported D-Bus modes: direct transport, BUS personality,
  and/or export registration.
- [ ] Keep semantic API adapters separate from transport translation.
- [ ] Add D-Bus name-registration policy controls and compatibility integration tests.
- [ ] Evaluate durable queues, service activation, remote gateways, federation, and
  shared providers only as separately negotiated future capabilities.

**Exit criteria**

- [ ] Native BUS/1 remains usable and stable when every optional feature is disabled.
- [ ] Compatibility work does not make D-Bus or Freedesktop naming conventions mandatory for BUS APIs.

## Definition of done for a milestone

- [ ] Public behavior is specified in `docs/` and represented by tests.
- [ ] API and wire compatibility impact is documented.
- [ ] Errors, limits, authorization, and cleanup paths have test coverage.
- [ ] `cargo fmt --check`, `cargo clippy --workspace --all-targets`, and
  `cargo test --workspace` pass.
- [ ] The README and relevant examples describe only supported behavior.
