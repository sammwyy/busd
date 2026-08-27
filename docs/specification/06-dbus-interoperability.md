# D-Bus interoperability

[← Specification index](../SPECS.md)

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



