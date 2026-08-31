# Security, discovery, and evolution

[← Specification index](../SPECS.md)

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

## Extension registration rules

An extension is identified by one capability name and one independent semantic
version. Capability names are canonical, case-sensitive ASCII names with
components separated by `.`; each component starts with an ASCII letter or
underscore and continues with ASCII letters, digits, or underscores. Names are
limited to 255 bytes and are compared bytewise.

The broker advertises the extension versions it implements. A client offers
the versions it understands in `HELLO`; the `WELCOME` capability set contains
only the exact names selected by intersection. An absent selected capability
means that the extension is unavailable for that session. Extensions must not
change the meaning of BUS/1-preview frames unless their capability was
selected, and an extension must reject unsupported versions explicitly.

Registration of an extension-owned identity is a separate authorized broker
operation. Registration is exclusive, is released when its peer disconnects,
and never creates a BUS namespace implicitly. Transport translation and API
semantics remain separate adapter responsibilities.

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


