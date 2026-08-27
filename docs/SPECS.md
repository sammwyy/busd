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



## Reading guide

Read the chapters in order for the complete specification, or jump to a topic:

1. [Design principles and identity](specification/01-principles-and-identity.md)
2. [Namespaces, addressing, and channels](specification/02-namespaces-addressing-and-channels.md)
3. [Messaging and delivery](specification/03-messaging-and-delivery.md)
4. [Native transport and performance](specification/04-native-transport-and-performance.md)
5. [Security, discovery, and evolution](specification/05-security-discovery-and-evolution.md)
6. [D-Bus interoperability](specification/06-dbus-interoperability.md)
7. [Software architecture](specification/07-software-architecture.md)
8. [Handshake and examples](specification/08-handshake-and-examples.md)
9. [Resilience and architectural model](specification/09-resilience-and-architecture.md)

Each chapter retains the original section numbers so references to BUS/1 remain stable.

