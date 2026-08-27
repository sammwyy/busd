# Messaging and delivery

[← Specification index](../SPECS.md)

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



