# Resilience and architectural model

[← Specification index](../SPECS.md)

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


