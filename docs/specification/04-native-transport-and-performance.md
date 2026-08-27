# Native transport and performance

[← Specification index](../SPECS.md)

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



