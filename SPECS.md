# busd — Generic Local Message Bus and IPC Broker Design Specification

**Status:** Draft
**Protocol:** BUS/1
**Scope:** Generic local IPC broker and transport protocol
**Implementation language:** Rust
**Primary platform:** Linux
**Transport:** Unix domain sockets
**Protocol payload:** Opaque binary data

---

## 1. Overview

`busd` is a generic local message broker intended for communication between operating-system components, daemons, desktop services, applications, and other processes.

It is not tied to a specific init system, service manager, networking daemon, storage manager, desktop environment, or distribution.

The broker provides a common transport and routing layer supporting:

* direct peer-to-peer messaging;
* API namespace ownership;
* publish/subscribe channels;
* global broadcasts;
* request/response communication;
* acknowledged delivery;
* fire-and-forget signals;
* retry policies;
* message filtering;
* peer metadata;
* software identification;
* capability discovery;
* arbitrary binary payloads;
* file descriptor passing;
* access control;
* optional D-Bus interoperability.

`busd` does **not** define APIs such as:

```text
bus://services
bus://network
bus://storage
bus://sessions
```

Those APIs may be standardized independently in the future.

The core broker only defines how an API namespace can be claimed, discovered, addressed, consumed, and replaced.

For example:

```text
serviced
    provides bus://services
```

is conceptually different from:

```text
serviced == bus://services
```

`serviced` is merely one implementation.

Another implementation could replace it:

```text
superserviced
    provides bus://services
```

without requiring consumers of `bus://services` to change.

---

# 2. Design principles

## 2.1 Implementations are not APIs

Software identity must never become the public API identity.

A process may identify itself as:

```text
client.id = "superserviced"
```

while simultaneously providing:

```text
bus://services
```

Applications should communicate with:

```text
bus://services
```

when they need the standardized service-management API.

They should communicate with a specific software implementation only when intentionally using implementation-specific extensions.

---

## 2.2 The broker is protocol-neutral regarding payloads

`busd` routes packets.

It does not need to understand their payload.

```text
┌────────────── header ──────────────┐
│ routing information                │
│ message type                       │
│ correlation information            │
│ sender metadata                    │
│ delivery requirements              │
└────────────────────────────────────┘

┌────────────── payload ─────────────┐
│ arbitrary bytes                    │
└────────────────────────────────────┘
```

The receiver decides how to interpret the payload.

Possible payload formats include:

* custom binary protocols;
* FlatBuffers;
* Protocol Buffers;
* MessagePack;
* CBOR;
* JSON;
* raw structs with a standardized ABI;
* compressed data;
* encrypted data;
* application-defined encodings.

The broker must not require any particular serialization framework.

---

## 2.3 Peer identity and API identity are separate

Every connection has several independent identities:

```text
Peer ID
    unique connection assigned by busd

Client ID
    non-unique software identifier supplied by client

Peer headers
    advertised software metadata

Credentials
    broker-verified operating-system identity

Namespaces
    optionally owned APIs

Channels
    optionally subscribed multicast groups
```

These concepts must never be collapsed into one identifier.

---

## 2.4 Channels do not have owners

A channel is not a service.

For example:

```text
system.hardware.events
network.events
service.events
desktop.notifications
```

may have:

```text
0..N publishers
0..N subscribers
```

There is no process that owns the channel.

Channels exist only as routing constructs.

---

## 2.5 Namespace ownership is exclusive by default

An API namespace normally has exactly one current provider.

```text
Peer A:

CLAIM bus://services
    → OK
```

followed by:

```text
Peer B:

CLAIM bus://services
    → ERROR_ALREADY_OWNED
```

When Peer A disconnects, the namespace becomes available again.

This enables replaceable implementations without requiring consumers to know which implementation is active.

---

# 3. Terminology

## 3.1 Broker

The `busd` process.

It owns the local IPC socket, authenticates connections and routes messages.

---

## 3.2 Peer

One active connection to the broker.

A single process may theoretically create multiple peers.

Every peer receives a broker-generated unique identifier:

```text
PeerId
```

Example:

```text
:1
:2
:3
```

or an equivalent binary identifier.

Peer IDs are:

* unique;
* ephemeral;
* broker assigned;
* valid only during a bus instance.

They must never be treated as persistent software identities.

---

## 3.3 Client ID

A peer may advertise a `client.id`.

Example:

```text
client.id = "serviced"
```

or:

```text
client.id = "org.example.superserviced"
```

Unlike `PeerId`, a Client ID is deliberately **not unique**.

Multiple processes may simultaneously connect with:

```text
client.id = "foo"
```

This allows Client ID to represent the software implementation rather than a specific process.

For example:

```text
Peer :27
client.id = "browser"

Peer :31
client.id = "browser"

Peer :42
client.id = "browser"
```

may represent three browser processes.

A Client ID is primarily useful for:

* implementation discovery;
* protocol extensions;
* debugging;
* compatibility checks;
* targeted multicast;
* feature negotiation.

It must **not** be considered proof of identity.

A malicious process can claim:

```text
client.id = "serviced"
```

unless security policy explicitly prevents it.

---

# 4. Peer metadata

Every peer can register metadata at connection time.

Example:

```text
client.id       = "superserviced"
client.version  = "2.4.1"

protocol.foo    = "2"
protocol.bar    = true

feature.jobs-v2 = true
feature.foo     = true
```

These values are called **peer headers**.

Peer headers differ from message headers because they describe the connection rather than an individual packet.

---

# 5. Header classes

There should be three classes of metadata.

## 5.1 Claimed headers

Provided by the client.

Examples:

```text
client.id
client.version
application.name
protocol.foo
feature.fast-start
vendor.extension
```

These are untrusted unless policy explicitly verifies them.

---

## 5.2 Broker headers

Generated by `busd`.

Examples:

```text
peer.id
process.pid
process.uid
process.gid
process.exe
process.cgroup
process.security_label
connection.time
```

On Linux these should be derived from trusted kernel information where possible.

For a Unix domain socket, credentials such as PID, UID and GID can be obtained from the peer instead of trusting data supplied by the application.

Clients must not be allowed to forge broker headers.

---

## 5.3 Message headers

Associated with one packet.

Examples:

```text
content.type
content.schema
content.encoding

trace.id
extension.foo
priority
deadline
```

Message headers are separate from peer headers.

---

# 6. Sender metadata delivery

When `busd` delivers a message, the recipient must be able to inspect information about the sender.

Conceptually:

```text
ReceivedMessage {
    sender: {
        peer_id,
        client_id,
        credentials,
        headers
    },

    message: {
        ...
    }
}
```

The sender information attached by the broker cannot be modified by the sending process after routing.

This allows code such as:

```rust
if message.sender.client_id == "superserviced-client" {
    ...
}
```

More importantly, extension negotiation can use explicit capability headers:

```rust
if message.sender.headers.contains("protocol.superservice.v2") {
    ...
}
```

rather than depending exclusively on Client ID.

---

# 7. Client ID is not an authentication mechanism

This distinction is critical.

This:

```text
client.id = "trusted-admin"
```

does not mean:

```text
this process is trusted
```

Authorization should instead use broker-attested attributes such as:

```text
UID
GID
PID
executable
security label
cgroup
capabilities
namespace ownership
policy grants
```

Client IDs and custom headers are discovery metadata unless policy elevates them.

---

# 8. Namespace model

An API namespace represents one logical service or API.

Example syntax:

```text
bus://services
bus://network
bus://storage
bus://sessions
```

These examples are illustrative only.

BUS/1 does not define their semantics.

---

## 8.1 Names are generic

`busd` must not require vendor-specific prefixes.

The following may all be syntactically valid depending on the final naming grammar:

```text
bus://services

bus://network

bus://com.example.foo

bus://my-experimental-api
```

The protocol defines syntax.

Communities and specifications define naming conventions.

This distinction intentionally mirrors the useful part of D-Bus naming without requiring BUS APIs to imitate D-Bus names.

D-Bus itself does not require all APIs to use `org.freedesktop.*`; well-known names follow a generic syntax, with reversed DNS recommended as a collision-avoidance convention. `org.freedesktop.*` is therefore a convention/ownership namespace for Freedesktop APIs rather than a requirement of D-Bus itself.

---

## 8.2 Standard namespaces

BUS may eventually establish a registry of standard namespaces.

For example:

```text
bus://services
bus://network
bus://storage
```

But these belong to separate specifications.

BUS/1 itself must not define their API.

---

## 8.3 Third-party namespaces

Third parties should have a recommended collision-resistant convention.

For example:

```text
bus://com.example.product
```

However, this should remain a naming convention rather than a fundamental routing concept.

---

# 9. Namespace ownership

A peer claims a namespace:

```text
CLAIM
namespace = bus://services
```

The broker performs:

```text
1. validate namespace
2. authenticate peer
3. evaluate ACL
4. check existing owner
5. register ownership
6. emit ownership event
```

Result:

```text
CLAIM_OK
```

or:

```text
CLAIM_DENIED
CLAIM_ALREADY_OWNED
CLAIM_INVALID
```

---

## 9.1 Ownership lifetime

Ownership is tied to the peer connection.

If the peer disappears:

```text
peer disconnect
       ↓
busd releases namespace
       ↓
namespace available
```

No explicit cleanup from the daemon should be required.

---

## 9.2 Consumers

Consumers do not need to register themselves as consumers.

They simply send a message to:

```text
bus://services
```

The broker resolves the current provider.

---

# 10. Provider and consumer roles

Provider and consumer are not peer types.

A peer can simultaneously:

```text
provide bus://foo
consume bus://bar
publish channel X
subscribe channel Y
send direct messages
receive direct messages
```

For example:

```text
serviced

provides:
    bus://services

consumes:
    bus://storage
    bus://sessions

publishes:
    service.events

subscribes:
    system.shutdown
```

No fixed role is assigned when connecting.

---

# 11. Addressing modes

BUS/1 should support several independent routing mechanisms.

---

## 11.1 Namespace addressing

Send to the current provider of an API:

```text
destination:
    namespace = bus://services
```

Exactly one provider normally receives it.

---

## 11.2 Peer addressing

Send directly to a specific connection:

```text
destination:
    peer = :42
```

Useful for:

* replies;
* private communication;
* negotiated sessions;
* diagnostics.

---

## 11.3 Client ID addressing

Send to software matching a particular Client ID:

```text
destination:
    client.id = "foo"
```

Because Client IDs are non-unique, the sender must specify selection semantics.

Possible modes:

```text
FIRST
ANY
ALL
```

Example:

```text
destination:
    client.id = "desktop-shell"

mode:
    ALL
```

sends to every currently connected matching instance.

---

## 11.4 Filtered client addressing

Client IDs may be combined with peer-header predicates.

Example:

```text
client.id = "superservice-client"

where:
    protocol.superservice >= 2
    feature.extended-jobs == true
```

This enables implementation-specific protocols without polluting standard APIs.

---

# 12. Channels

Channels provide multicast publish/subscribe communication.

A channel:

* has no owner;
* does not need a provider;
* can have zero publishers;
* can have zero subscribers;
* can have many publishers;
* can have many subscribers.

Example:

```text
channel = service.events
```

Subscribers explicitly register interest:

```text
SUBSCRIBE service.events
```

A publisher sends:

```text
PUBLISH service.events
```

The broker sends the message only to matching subscribers.

This is intentional.

Broadcast-style D-Bus signals are similarly routed using match rules so uninterested clients do not have to receive or process every signal.

---

# 13. Channel lifetime

Channels should not need to be explicitly created.

They exist conceptually whenever they are referenced.

For example:

```text
SUBSCRIBE foo.bar
```

can succeed even if no publisher currently exists.

Likewise:

```text
PUBLISH foo.bar
```

can succeed even with zero subscribers.

There is therefore no global channel ownership table.

The broker only needs subscription state.

---

# 14. Channel subscriptions

A basic subscription contains:

```text
channel
```

An advanced subscription may contain filters.

Example:

```text
channel = service.events

filter:
    sender.client.id == "serviced"
```

or:

```text
channel = hardware.events

filter:
    sender.process.uid == 0
    message.content.type == "application/x-device-event"
```

---

# 15. Header-based filtering

BUS should support routing predicates over:

```text
sender headers
broker headers
message headers
client ID
namespace
channel
message type
```

Instead of initially defining a textual query language, the binary protocol should encode filters structurally.

Conceptually:

```rust
Filter {
    key: "feature.foo",
    operation: Equals,
    value: true,
}
```

Possible initial operators:

```text
EXISTS
NOT_EXISTS

EQUAL
NOT_EQUAL

PREFIX
```

More sophisticated operations can be added later.

Numeric comparison or semantic-version matching should not be required for BUS/1 unless clearly needed.

---

# 16. Example: protocol extension detection

Suppose an alternative service manager exists:

```text
client.id = "superserviced"

headers:
    extension.superjobs = "2"
```

It still provides the standardized API:

```text
bus://services
```

A generic consumer sends normal BUS service messages.

An enhanced consumer might connect with:

```text
client.id = "superctl"

headers:
    extension.superjobs = "2"
```

When `superserviced` receives a request, it can inspect:

```text
sender.client.id

sender.headers["extension.superjobs"]
```

and enable its private extension protocol.

The standardized API remains unchanged.

---

# 17. Prefer capability detection over Client-ID detection

Although Client ID can help:

```text
if client.id == "superctl"
```

protocol extensions should preferably use explicit capabilities:

```text
extension.superjobs = 2
```

This allows third-party implementations to participate without pretending to be another application.

For example:

```text
client.id = "alternative-superctl"

extension.superjobs = 2
```

can still use the extension.

---

# 18. Message model

BUS/1 should expose three fundamental communication semantics.

```text
SIGNAL
REQUEST
RESPONSE
```

Acknowledgement is orthogonal.

---

# 19. Signal

A signal does not inherently require an application-level response.

Example:

```text
SIGNAL service.changed
```

Possible acknowledgement policies:

```text
NONE
ACCEPTED
RECEIVED
PROCESSED
```

---

## 19.1 ACK_NONE

Pure fire-and-forget.

```text
sender
  │
  ▼
busd
  │
  ▼
receiver
```

The sender does not wait.

---

## 19.2 ACK_ACCEPTED

The sender only needs confirmation that `busd` accepted the packet.

```text
sender
  │
  ▼
busd
  │
  └── ACK_ACCEPTED
```

This says nothing about recipients.

---

## 19.3 ACK_RECEIVED

At least the required receiver set must acknowledge receipt.

```text
sender
   │
   ▼
 busd
   │
   ▼
receiver
   │
   └── RECEIVED
```

---

## 19.4 ACK_PROCESSED

The recipient explicitly confirms that the operation was processed.

```text
sender
   │
   ▼
 busd
   │
   ▼
receiver
   │
   └── PROCESSED
```

The distinction between `RECEIVED` and `PROCESSED` is essential.

Receiving bytes is not equivalent to successfully carrying out an operation.

---

# 20. Request

A request always expects an application response.

```text
REQUEST #150
      ↓
   receiver
      ↓
RESPONSE #150
```

Every request has:

```text
message_id
correlation_id
timeout
```

A response references the request through its correlation ID.

---

# 21. Response

A response can contain:

```text
status
headers
payload
```

For example:

```text
SUCCESS
ERROR
UNSUPPORTED
DENIED
BUSY
NOT_FOUND
```

The application may additionally define its own status codes inside the payload or headers.

---

# 22. Delivery cardinality

Multicast operations require explicit acknowledgement semantics.

A sender should be able to request:

```text
NONE
ANY
ALL
MINIMUM(N)
```

For example:

```text
channel = shutdown.handlers

ack = PROCESSED
require = ALL
```

means every selected subscriber must acknowledge processing.

Another operation may require:

```text
require = ANY
```

meaning success after at least one matching peer acknowledges.

---

# 23. Request routing cardinality

Requests should support:

```text
EXACT
FIRST
FIRST_SUCCESS
ALL
```

`EXACT` is normally used for:

```text
namespace provider
peer ID
```

`FIRST` or `FIRST_SUCCESS` can be useful with:

```text
client ID selectors
header-filtered selectors
```

This provides RabbitMQ-like worker-selection semantics without requiring channels to have owners.

---

# 24. Retries

Retries are handled by `busd`, not by every sender.

Example:

```text
retry:
    attempts = 3
    initial_delay = 100ms
    backoff = exponential
```

The sender submits one logical message.

The broker handles delivery attempts.

---

# 25. Delivery guarantees

BUS/1 must **not claim exactly-once execution**.

If:

```text
busd → receiver

receiver executes operation

receiver crashes before ACK
```

the broker cannot know whether the operation happened.

Retrying could execute it twice.

Therefore acknowledged retryable messages generally provide:

```text
at-least-once delivery
```

Receivers should make important operations idempotent whenever possible.

---

# 26. Message IDs and deduplication

Every logical message receives a unique message ID.

Example:

```text
MessageId = 128-bit random ID
```

Retries retain the same ID.

A receiver can maintain a bounded deduplication cache:

```text
message 89 processed

retry 89 arrives

→ ALREADY_PROCESSED
```

This greatly reduces duplicate side effects.

It still does not magically create strict exactly-once semantics across arbitrary crashes.

---

# 27. Timeouts

Messages that require acknowledgement or response must have deadlines.

Example:

```text
timeout = 5s
```

The broker may return:

```text
TIMEOUT
NO_RECIPIENT
RECIPIENT_DISCONNECTED
DELIVERY_FAILED
```

Applications must not wait forever.

---

# 28. Offline peers

BUS/1 should initially be an in-memory local broker.

If a target does not exist:

```text
NO_RECIPIENT
```

should normally be returned.

RabbitMQ-style durable queues should **not** be a BUS/1 requirement.

Persistent queues could be introduced later as an optional broker capability.

This avoids turning a local operating-system IPC layer into a persistent message-queue database.

---

# 29. Broadcast

BUS should also support an actual global broadcast.

```text
destination = BROADCAST
```

This differs from channels.

Broadcast targets every eligible connected peer.

Because waking every process is expensive and can be abused, global broadcast should be rare and subject to strict ACL.

Normal multicast communication should use channels.

---

# 30. Channels versus broadcast

Use:

```text
PUBLISH channel
```

when recipients must explicitly opt in.

Use:

```text
BROADCAST
```

only when the message genuinely concerns every peer.

Example possible global events:

```text
broker.shutdown
system.shutdown
system.suspend
```

Even those may ultimately be better represented by well-known channels.

---

# 31. Binary protocol

The native transport should use a binary packet protocol.

A conceptual packet:

```text
┌───────────────────────┐
│ BUS header            │
├───────────────────────┤
│ destination selector  │
├───────────────────────┤
│ message headers       │
├───────────────────────┤
│ binary payload        │
└───────────────────────┘
```

The exact ABI should be separately specified as the BUS/1 Wire Protocol.

---

# 32. Native Linux transport

The preferred initial transport is:

```text
AF_UNIX
SOCK_SEQPACKET
```

`SOCK_SEQPACKET` preserves packet boundaries while retaining connection semantics.

This matches BUS particularly well because BUS communication is inherently message-oriented rather than byte-stream-oriented.

---

# 33. File descriptor passing

BUS should support Unix file descriptor passing using:

```text
SCM_RIGHTS
```

This enables applications to transfer:

* sockets;
* pipes;
* files;
* eventfds;
* memfds;
* device handles.

without reopening them by path.

---

# 34. Large payloads

Large payloads should not necessarily be copied through `busd`.

For sufficiently large data:

```text
sender
   │
   ├─ create memfd
   ├─ write data
   │
   ▼
 busd
   │ SCM_RIGHTS
   ▼
receiver
```

The regular packet then carries metadata plus the FD.

This allows efficient transfer of large binary objects while preserving the message abstraction.

---

# 35. Backpressure

A slow peer must never be allowed to block the entire system bus.

Every connection should have bounded resources:

```text
max queued messages
max queued bytes
max in-flight requests
max subscriptions
max namespace claims
```

When limits are exceeded, policies may include:

```text
reject sender
drop low-priority signal
disconnect abusive peer
```

Requests and acknowledged control messages should generally fail explicitly rather than being silently dropped.

---

# 36. Priorities

BUS may support message priority.

For example:

```text
LOW
NORMAL
HIGH
CRITICAL
```

This should affect scheduling, not authorization.

`CRITICAL` must not allow a process to bypass ACL.

---

# 37. Broker-assigned credentials

On connection, `busd` should collect trusted process metadata.

Linux examples:

```text
PID
UID
GID
security label
```

Additional information can be resolved from the process when appropriate.

These values become broker-authenticated peer metadata.

---

# 38. ACL model

Security policy should be able to control at least:

```text
connect
claim namespace
send to namespace
send direct peer message
publish channel
subscribe channel
broadcast
request acknowledgement
register D-Bus name
monitor bus
```

Rules may inspect:

```text
UID
GID
executable
security label
client ID
peer headers
destination
channel
namespace
message type
```

But security-sensitive policies should prefer broker-authenticated data over claimed headers.

---

# 39. Namespace claim security

Core system APIs may be protected.

Example policy:

```text
bus://services:
    claim:
        uid = 0
```

A stronger configuration could require:

```text
uid = 0
executable = /usr/libexec/serviced
```

An alternative distribution could change that policy without changing BUS itself.

This keeps:

```text
bus://services
```

implementation-neutral while still allowing a system administrator to define who is authorized to provide it.

---

# 40. Discovery

The broker should expose introspection operations.

Examples:

```text
ListPeers
GetPeer
ListNamespaces
ResolveNamespace
ListSubscriptions
GetBrokerCapabilities
```

Example output:

```text
PEER :19

client.id:
    superserviced

pid:
    482

uid:
    0

provides:
    bus://services

headers:
    extension.superjobs = 2
```

---

# 41. Peer lifecycle events

The broker should expose optional lifecycle channels or notifications:

```text
peer.connected
peer.disconnected

namespace.acquired
namespace.released
namespace.owner_changed
```

Consumers should be able to discover when a provider disappears and when another implementation takes its place.

---

# 42. Broker capability negotiation

BUS must itself be extensible.

During connection:

```text
HELLO BUS/1
```

the client and broker negotiate capabilities.

Example:

```text
fd-passing
ack-processed
header-filtering
dbus-compat
large-payload-memfd
```

Clients must not assume optional capabilities exist.

---

# 43. Protocol versioning

The transport protocol and API namespaces must be versioned independently.

For example:

```text
BUS transport:
    BUS/1

bus://foo API:
    version 3

custom extension:
    version 7
```

Upgrading the broker protocol must not inherently change every API carried through it.

---

# 44. API version advertisement

Namespace claims may include metadata:

```text
CLAIM bus://foo

api.version = 3
api.features = [...]
```

A consumer may query this before sending API-specific messages.

The exact API versioning policy should be defined separately from BUS/1.

---

# 45. Monitoring

A privileged peer should be able to enter monitor mode.

Monitor mode may observe:

```text
routing
peer connections
namespace changes
channel activity
messages
ACKs
requests
responses
timeouts
```

Payload visibility should remain policy-controlled.

Monitoring is essential for debugging system daemons.

---

# 46. Observability

Every message should support optional:

```text
trace.id
span.id
```

or equivalent opaque tracing metadata.

`busd` itself should provide metrics such as:

```text
connected peers
owned namespaces
subscriptions
messages routed
bytes routed
timeouts
retries
dropped signals
queue pressure
```

This is particularly valuable for debugging operating-system boot and daemon interaction.

---

# 47. D-Bus interoperability

D-Bus compatibility is optional.

BUS/1 does not depend on D-Bus.

A minimal system may contain:

```text
busd
```

and no D-Bus implementation at all.

---

# 48. D-Bus names are not inherently Freedesktop names

D-Bus defines generic well-known bus names.

The specification requires a dotted naming structure and recommends reverse-DNS-style names for well-known names and interfaces. It does **not** require arbitrary software to use `org.freedesktop.*`.

Therefore BUS D-Bus compatibility must permit arbitrary valid D-Bus names.

Examples:

```text
org.freedesktop.systemd1
org.freedesktop.login1

com.example.Foo

org.example.Bar
```

Freedesktop names should only be used when implementing the corresponding Freedesktop API.

---

# 49. D-Bus compatibility modes

A daemon should be able to operate in three important configurations.

---

## 49.1 BUS-only

```text
daemon
   │
   │ BUS/1
   ▼
 busd
```

No D-Bus.

---

## 49.2 Direct D-Bus

```text
daemon
   │
   │ D-Bus
   ▼
dbus-broker
```

Useful when running the daemon on an existing Linux distribution.

---

## 49.3 BUS with D-Bus personality

```text
daemon
   │
   │ BUS/1
   ▼
 busd
   │
   ├── BUS/1 clients
   │
   └── D-Bus clients
```

Here `busd` itself exposes the D-Bus system-bus transport.

---

# 50. D-Bus export registration

A BUS peer may optionally ask `busd` to expose a D-Bus identity on its behalf.

Conceptually:

```text
REGISTER_DBUS_NAME
    name = org.freedesktop.systemd1
```

Additional declarations may include:

```text
object path
interface
methods
signals
properties
```

This does **not** mean `busd` understands the semantic meaning of systemd.

It only means `busd` can route the D-Bus-facing communication to the owning BUS peer.

---

# 51. Semantic adapters are separate from transport translation

This distinction is essential.

Mapping:

```text
D-Bus packet
        ↕
BUS envelope
```

can be generic.

Mapping:

```text
org.freedesktop.systemd1.Manager.StartUnit
        ↕

some completely different native bus://services operation
```

is semantic and cannot generally be inferred by the broker.

Such mappings require an adapter.

The adapter may live:

* inside the daemon;
* in a shared compatibility crate;
* in a dedicated compatibility service;
* in a future declarative adapter framework.

`busd` itself should remain generic.

---

# 52. Example D-Bus compatibility

An implementation might provide:

```text
client.id = serviced

provides:
    bus://services

D-Bus exports:
    org.freedesktop.systemd1
```

Native clients use:

```text
bus://services
```

Legacy clients use:

```text
org.freedesktop.systemd1
```

Both eventually execute the same internal service-manager implementation.

---

# 53. Direct D-Bus transport

The Rust IPC library should permit:

```rust
BusTransport
```

and:

```rust
DbusTransport
```

as independent transport backends.

A daemon core must not depend directly on either.

Conceptually:

```text
                 daemon core
                     │
                 logical API
                     │
             ┌───────┴───────┐
             │               │
         BUS adapter     D-Bus adapter
             │               │
            busd        dbus-broker
```

---

# 54. Mixed environments

A system may contain simultaneously:

```text
serviced → BUS
diskd    → BUS
netd     → D-Bus
sessiond → BUS
```

Provided the appropriate adapters/gateways exist, implementations should remain interoperable.

BUS must not assume all components use the same transport.

---

# 55. D-Bus signals and BUS channels

D-Bus signals should not be translated into unconditional global BUS broadcasts.

D-Bus itself uses match rules to select which clients receive broadcast signals.

BUS channels are a natural equivalent:

```text
D-Bus signal
       ↓
compatibility routing
       ↓
BUS channel
       ↓
subscribed peers
```

Exact semantic mapping remains adapter-specific.

---

# 56. Software architecture

The Rust project should be split into reusable crates.

Possible structure:

```text
bus/
├── bus-protocol
├── bus-client
├── bus-transport-unix
├── bus-transport-dbus
├── bus-broker
├── bus-policy
└── busd
```

---

# 57. `bus-protocol`

Contains only protocol-level structures.

Example:

```rust
Message
MessageId
PeerId
Namespace
Channel
Headers
Filter
Destination
AckPolicy
DeliveryPolicy
Request
Response
```

No sockets.

No broker.

No D-Bus.

---

# 58. `bus-client`

High-level API used by applications.

Conceptually:

```rust
let bus = Bus::connect().await?;

bus.claim("bus://foo").await?;

bus.subscribe("foo.events").await?;

bus.publish(
    "foo.events",
    payload
).await?;
```

---

# 59. `bus-transport-unix`

Native BUS/1 transport implementation.

Responsible for:

```text
AF_UNIX
SOCK_SEQPACKET
packet framing
SCM_RIGHTS
peer credentials
```

---

# 60. `bus-transport-dbus`

Optional D-Bus transport implementation.

Allows BUS-aware software to operate directly on a D-Bus system without `busd`.

This crate does not need to be linked into minimal builds.

---

# 61. `bus-broker`

Contains generic broker state:

```text
peers
namespaces
subscriptions
routing
ACK tracking
request tracking
retry scheduling
queues
capabilities
```

It should ideally be mostly independent of the concrete socket listener.

---

# 62. `bus-policy`

Contains security policy parsing and authorization.

Keeping this separate makes the policy engine testable without running the broker.

---

# 63. `busd`

Small executable assembling:

```text
broker
native transport
policy
optional D-Bus frontend
logging
configuration
```

---

# 64. Feature flags

Example Cargo configuration:

```toml
[features]
default = ["unix"]

unix = [...]
dbus = [...]
monitoring = [...]
```

Minimal system:

```text
busd = unix
```

Desktop-compatible system:

```text
busd = unix + dbus
```

---

# 65. Connection handshake

A native connection could conceptually begin with:

```text
CLIENT → HELLO

protocol = BUS/1

headers:
    client.id = serviced
    client.version = 0.1.0
    extension.foo = 2
```

`busd` responds:

```text
SERVER → WELCOME

peer.id = :42

capabilities:
    fd-passing
    acknowledged-signals
    header-filtering
```

After `WELCOME`, normal traffic can begin.

---

# 66. Example namespace registration

```text
:42 → busd

CLAIM
    bus://services
```

Broker:

```text
busd → :42

CLAIMED
    bus://services
```

Another peer:

```text
:63 → busd

CLAIM
    bus://services
```

returns:

```text
ERROR
    NAMESPACE_ALREADY_OWNED
```

---

# 67. Example native request

```text
superctl
    │
    │ destination = bus://services
    │ REQUEST #519
    │
    ▼
  busd
    │
    │ resolve owner
    ▼
serviced
    │
    │ RESPONSE #519
    ▼
  busd
    │
    ▼
superctl
```

Neither `superctl` nor the protocol needs to know the provider process name.

---

# 68. Example channel publish

Subscribers:

```text
desktop-shell
    SUBSCRIBE service.events

monitor
    SUBSCRIBE service.events
```

Publisher:

```text
serviced
    PUBLISH service.events
```

Result:

```text
                 ┌── desktop-shell
serviced → busd ─┤
                 └── monitor
```

Unsubscribed clients receive nothing.

---

# 69. Example filtered channel

Subscriber:

```text
SUBSCRIBE service.events

FILTER:
    sender.headers["extension.superjobs"] EXISTS
```

Only matching publishers cause delivery.

---

# 70. Example Client-ID multicast

Three processes:

```text
:12 client.id = ui-worker
:15 client.id = ui-worker
:19 client.id = unrelated
```

Message:

```text
destination:
    client.id = ui-worker

mode:
    ALL
```

Result:

```text
:12 ← message
:15 ← message
```

`:19` receives nothing.

---

# 71. Example implementation-specific extension

Provider:

```text
client.id = superserviced

provides:
    bus://services

headers:
    extension.superjobs = 2
```

Consumer:

```text
client.id = superctl

headers:
    extension.superjobs = 2
```

Generic requests still use:

```text
bus://services
```

But the peers can detect their shared extension and exchange additional private operations.

Another implementation can still provide:

```text
bus://services
```

without supporting `extension.superjobs`.

---

# 72. Failure isolation

A malformed peer must not crash `busd`.

All parsing must be:

* bounded;
* validated;
* allocation-limited;
* fuzz tested.

Malformed packets should normally result in:

```text
protocol error
disconnect offending peer
```

rather than broker failure.

---

# 73. Broker failure

Because `busd` is critical infrastructure, clients should support automatic reconnection.

After broker restart:

```text
peer reconnects
      ↓
receives new PeerId
      ↓
restores headers
      ↓
reclaims namespaces
      ↓
restores subscriptions
```

Client libraries should automate most of this.

Applications must assume Peer IDs change after reconnect.

---

# 74. What BUS/1 intentionally does not define

BUS/1 should initially avoid specifying:

* service management;
* network management;
* disk management;
* sessions;
* login management;
* power management;
* desktop APIs;
* storage APIs;
* standardized payload schemas;
* persistent queues;
* distributed networking;
* cross-machine routing;
* consensus;
* remote brokers.

These may be separate specifications later.

---

# 75. Future possibilities

The architecture permits later additions such as:

```text
persistent subscriptions
durable queues
remote BUS gateways
broker federation
shared namespace providers
load-balanced providers
schema discovery
IDL generation
service activation
capability tokens
zero-copy shared memory
namespaced user buses
container buses
per-session buses
```

None should be necessary for BUS/1.

---

# 76. Core invariants

The initial implementation should preserve the following rules.

### Identity

```text
PeerId:
    unique

ClientId:
    non-unique

namespace:
    exclusively owned by one peer by default

channel:
    unowned
```

### Security

```text
client headers:
    claimed

broker headers:
    trusted

ClientId:
    never authentication by itself
```

### Messaging

```text
Signal:
    optional ACK

Request:
    mandatory response or timeout

Payload:
    opaque bytes

Retry:
    broker managed

Exactly once:
    not promised
```

### Routing

```text
namespace
peer
client-id selector
channel
broadcast
```

### Compatibility

```text
BUS native protocol:
    primary

D-Bus:
    optional transport/personality

Freedesktop APIs:
    optional compatibility contracts

org.freedesktop:
    not required by BUS or generic D-Bus usage
```

---

# 77. Proposed conceptual architecture

```text
                           ┌───────────────────────┐
                           │         busd          │
                           │                       │
                           │  identities           │
                           │  namespace registry   │
                           │  routing              │
                           │  subscriptions        │
                           │  filters              │
                           │  ACK / retry          │
                           │  security             │
                           └───────┬───────────────┘
                                   │
                    Native BUS/1   │   optional D-Bus
                     ┌─────────────┴──────────────┐
                     │                            │
                     ▼                            ▼

                 native peers                legacy software
                     │
       ┌─────────────┼─────────────┐
       │             │             │
       ▼             ▼             ▼
    serviced       diskd         netd
       │
       │ claims
       ▼
 bus://services
```

Applications communicate with capabilities:

```text
bus://services
```

not implementations:

```text
serviced
```

Implementation identity remains available through:

```text
client.id
peer headers
```

when intentionally required.

Channels allow scalable multicast:

```text
service.events
```

without forcing irrelevant clients to receive the messages.

Direct peer IDs permit private communication.

Client IDs permit targeting software families.

Header predicates permit capability-based routing and protocol extensions.

D-Bus support allows BUS daemons to participate in the existing Linux ecosystem without making D-Bus or Freedesktop conventions fundamental parts of the new protocol.

---

# 78. Final architectural model

The fundamental BUS abstraction should therefore be:

```text
                         PEER

       identity ───────── PeerId
       software ───────── ClientId
       metadata ───────── Headers
       security ───────── Credentials

                │
                ├── claims APIs
                │      bus://...
                │
                ├── subscribes channels
                │
                ├── publishes channels
                │
                ├── sends requests
                │
                ├── sends signals
                │
                └── talks directly to peers
```

The broker is responsible for:

```text
WHO
    authenticated peer identity

WHERE
    routing

WHO OWNS WHAT
    namespace ownership

WHO CARES
    channel subscriptions and filters

DID IT ARRIVE
    acknowledgement

SHOULD IT RETRY
    delivery policy

WHO MAY DO IT
    authorization
```

The applications are responsible for:

```text
WHAT THE PAYLOAD MEANS
```

That boundary should remain the central design principle of `busd`.
