# Namespaces, addressing, and channels

[← Specification index](../SPECS.md)

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



