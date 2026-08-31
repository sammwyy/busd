# Software architecture

[← Specification index](../SPECS.md)

# 56. Software architecture

The Rust project should be split into reusable crates.

Possible structure:

```text
bus/
├── busd-protocol
├── busd-client
├── busd-transport-unix
├── busd-transport-dbus
├── busd-broker
├── busd-policy
└── busd
```

---

# 57. `busd-protocol`

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

# 58. `busd-client`

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

# 59. `busd-transport-unix`

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

# 60. `busd-transport-dbus`

Optional D-Bus transport implementation, provided by `busd-transport-dbus` and
enabled by the `busd/dbus` feature.

Allows BUS-aware software to operate directly on a D-Bus system without `busd`.

The transport exposes validated D-Bus names, standard session/system-bus
connections, and well-known-name registration. It does not decide how a
D-Bus method maps to a BUS namespace or message; that semantic adapter remains
an application or compatibility-service concern.

This crate does not need to be linked into minimal builds.

---

# 61. `busd-broker`

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

# 62. `busd-policy`

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

