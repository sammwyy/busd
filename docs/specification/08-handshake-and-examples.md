# Handshake and examples

[← Specification index](../SPECS.md)

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



