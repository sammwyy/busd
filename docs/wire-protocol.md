# BUS/1-preview wire protocol

**Status:** Normative for BUS/1-preview.  
**Compatibility:** Preview implementations must reject unsupported versions and
unknown mandatory frame kinds. The ABI may change before BUS/1.0; such a change
increments the version byte and never reinterprets a previously valid packet.

This document specifies packets carried by the native `AF_UNIX` `SOCK_SEQPACKET`
transport. One socket packet contains exactly one BUS frame. A conforming
implementation must validate a complete packet before treating any field as a
BUS message.

## Common header

All integer fields are unsigned, network-byte-order (big endian). Every frame
starts with this 12-byte header:

| Offset | Size | Field | Value |
| --- | ---: | --- | --- |
| 0 | 4 | magic | ASCII `BUS1` (`42 55 53 31`) |
| 4 | 1 | version | `3` for BUS/1-preview |
| 5 | 1 | kind | Frame kind below |
| 6 | 2 | flags | Must be zero |
| 8 | 4 | body length | Exact number of following bytes |

The complete packet length is `12 + body length`. The default maximum complete
packet size is 1,048,576 bytes. Implementations may choose a smaller configured
limit, but must reject a packet before allocating from its length if it exceeds
that limit. A frame with trailing data, a non-zero reserved field, an unknown
version, or an unknown kind is invalid.

## Primitive encodings

`u8`, `u16`, `u32`, and `u64` are fixed-width big-endian integers. `id` is
exactly 16 opaque bytes; the all-zero ID means absent and is the only absent-ID
representation. `peer-id` is a non-zero `u64`.

`name` is `u16 byte-length` followed by 1 to 255 visible ASCII bytes other than
`/` and whitespace. A namespace uses the same `u16 byte-length` representation,
but is validated as literal `bus://` followed by a name. A text value is `u16
byte-length` followed by valid UTF-8. Binary data is `u32 byte-length` followed
by bytes.

Header values are encoded as:

| Tag | Encoding |
| ---: | --- |
| 0 | UTF-8 text |
| 1 | `u64` unsigned integer |
| 2 | one byte: `0` false or `1` true |
| 3 | binary data |

Headers are `u16 count` followed by `(name, value)` pairs. Header names must be
strictly increasing bytewise. Duplicate, unsorted, invalid, or over-limit
header names are invalid. This is the canonical duplicate-header rule: a header
may occur once only. Header counts are limited to 128.

Capabilities are `u16 count` followed by names, strictly increasing and unique;
the default limit is 128 capabilities.
Filters are `u16 count` followed by a filter tag and name, plus a header value
for `EQUAL`, `NOT_EQUAL`, and `PREFIX`. Filter tags are `0 EXISTS`, `1
NOT_EXISTS`, `2 EQUAL`, `3 NOT_EQUAL`, and `4 PREFIX`. Filters must be strictly
increasing by their complete binary encoding and are limited to 64.

## Frame kinds

| Value | Frame | Body |
| ---: | --- | --- |
| 1 | `HELLO` | client ID, claimed headers, capabilities |
| 2 | `WELCOME` | assigned peer ID, capabilities |
| 3 | `CLAIM` | namespace, claimed headers |
| 4 | `SUBSCRIBE` | channel, filters |
| 5 | `UNSUBSCRIBE` | channel |
| 6 | `MESSAGE` | message envelope |
| 7 | `PROTOCOL_ERROR` | protocol error code and text |
| 8 | `CONTROL_RESULT` | accepted control-operation tag |
| 9 | `RESOLVE_NAMESPACE` | namespace |
| 10 | `NAMESPACE_RESOLVED` | namespace and optional owner peer ID |
| 11 | `ACKNOWLEDGE` | message ID and `RECEIVED` or `PROCESSED` stage |
| 12 | `DELIVERY_RESULT` | message ID and broker delivery outcome |

`HELLO` starts with one byte (`0` no client ID, `1` name follows), then headers
and capabilities. `WELCOME` is `peer-id` then capabilities. `CLAIM` is a
namespace then headers. `SUBSCRIBE` and `UNSUBSCRIBE` operate on an unowned
channel. A peer may send `HELLO` only once before normal frames; session-state
rules and authorization are enforced by the broker, not by this codec.

## Native session rules

On Linux, the broker obtains `PID`, `UID`, and `GID` with `SO_PEERCRED` from the
accepted Unix socket. These values are broker-owned metadata; a `HELLO` header
cannot set or replace them. Header names beginning with `auth.`, `broker.`, or
`peer.` are reserved and cause handshake rejection.

The first frame from a client must be `HELLO`. A broker that accepts it assigns
a non-zero peer ID, intersects the offered capabilities with its own supported
capabilities, records the claimed metadata separately from authenticated
credentials, and replies with `WELCOME`. The client may begin normal control
traffic only after receiving `WELCOME`.

An invalid first frame, duplicate `HELLO`, malformed frame, or frame that is
not valid for the current session produces a `PROTOCOL_ERROR` and closes the
connection. Rejected claim or subscription operations produce a structured
`PROTOCOL_ERROR` while leaving an otherwise valid session connected. Closing a
native connection releases its peer, namespace claims, and subscriptions.

## Message envelope

`MESSAGE` bodies have this exact order:

| Field | Encoding |
| --- | --- |
| message kind | `0 SIGNAL`, `1 REQUEST`, `2 RESPONSE` |
| acknowledgement policy | `0 NONE`, `1 ACCEPTED`, `2 RECEIVED`, `3 PROCESSED` |
| acknowledgement requirement | `0 NONE`, `1 ANY`, `2 ALL`, or `3` plus non-zero `u16 MINIMUM(N)` |
| request policy | `0 EXACT`, `1 FIRST`, `2 FIRST_SUCCESS`, `3 ALL` |
| deadline | relative `u32` milliseconds; zero means absent |
| retry policy | `0 NONE`, or `1` plus initial `u32` backoff milliseconds and total-attempt `u8` |
| destination selector | selector below |
| message ID | `id` |
| correlation ID | `id` |
| status | status byte |
| headers | headers encoding |
| payload | binary data |

Destination selectors are `0 BROKER`, `1 PEER` plus `peer-id`, `2 NAMESPACE`
plus namespace, `3 CHANNEL` plus channel, `4 BROADCAST`, and `5 CLIENT_ID`
plus client ID and a selection byte (`0 FIRST`, `1 ANY`, `2 ALL`). `FIRST` and
preview `ANY` select the lowest matching peer ID; `ALL` selects every matching
peer. Direct peer, namespace, and client-ID sends return `NO_RECIPIENT` when
no peer matches. Publishing to an empty channel and broadcasting to no other
peer succeed with no recipients. `SIGNAL` and `REQUEST` must use status
`SUCCESS`; a `RESPONSE` may use any status and must have a non-zero correlation
ID. Requests require a non-zero deadline. `ACK_RECEIVED` and `ACK_PROCESSED`
require both a non-zero deadline and a non-`NONE` acknowledgement requirement;
`NONE` and `ACCEPTED` require `NONE`. Retry is valid for a request or for
`RECEIVED` and `PROCESSED`. Non-request messages use `EXACT`; responses use
neither deadline nor retry. The current statuses are `0 SUCCESS`, `1
ERROR`, `2 UNSUPPORTED`, `3 DENIED`, `4 BUSY`, and `5 NOT_FOUND`.

`ACKNOWLEDGE` names a delivered non-zero message ID and uses only `RECEIVED` or
`PROCESSED`; `PROCESSED` also satisfies a `RECEIVED` requirement.
`DELIVERY_RESULT` outcomes are `0 ACCEPTED`, `1 RECEIVED`, `2 PROCESSED`, `3
TIMEOUT`, `4 NO_RECIPIENT`, `5 RECIPIENT_DISCONNECTED`, and `6 DELIVERY_FAILED`.
They are broker-to-originator frames only.

## Reliable delivery

The broker computes deadlines from receipt using its scheduler clock. It returns
`NO_RECIPIENT` when a request or required acknowledgement has no eligible
recipient, `TIMEOUT` at the deadline, `RECIPIENT_DISCONNECTED` when a selected
recipient disconnects before completion, and `DELIVERY_FAILED` after the
bounded retry count is exhausted. Exponential retry retains the same logical
message ID on every delivery attempt.

`EXACT` completes a request with the selected recipient set, `FIRST` with the
first response, `FIRST_SUCCESS` with the first successful response (or a
failure after all selected recipients respond), and `ALL` after all selected
recipients respond. A response is correlated by its correlation ID and is only
accepted from a selected request recipient.

Acknowledged retryable delivery is at-least-once. It does not provide
exactly-once execution: a receiver may execute work and disconnect before its
acknowledgement. Receivers can retain a bounded cache of message IDs to reduce
duplicate side effects and should make important operations idempotent.

## Protocol errors and extension behavior

`PROTOCOL_ERROR` is an error-code byte followed by UTF-8 text. Codes are `0
MALFORMED_FRAME`, `1 UNSUPPORTED_VERSION`, `2 UNKNOWN_FRAME_KIND`, `3
LIMIT_EXCEEDED`, `4 NON_CANONICAL`, and `5 INVALID_STATE`. Text is diagnostic
only and must not be parsed for behavior. Version 2 adds `6 NO_RECIPIENT` for
unicast routing with no matching peer.

All reserved fields and flags are mandatory-zero in this preview. Unknown
values are rejected, rather than ignored, because no extension point has yet
been negotiated. Future versions may define optional extensions only through a
new version or an explicitly negotiated capability; they must preserve these
validation rules for BUS/1-preview packets.

## Routing control and discovery

The broker replies to accepted `CLAIM`, `SUBSCRIBE`, and `UNSUBSCRIBE` frames
with `CONTROL_RESULT`; its operation tags are `0 CLAIM`, `1 SUBSCRIBE`, and `2
UNSUBSCRIBE`. Failed operations use `PROTOCOL_ERROR` and do not disconnect an
otherwise valid session. `RESOLVE_NAMESPACE` returns `NAMESPACE_RESOLVED` with
the requested namespace and either marker `0` (unclaimed) or marker `1` plus a
non-zero owner peer ID.

`MESSAGE` delivery preserves the complete canonical message frame except that
the broker adds the reserved unsigned `broker.sender` header while forwarding.
Clients must not send `auth.`, `broker.`, or `peer.` message headers. Namespace
delivery selects one provider, peer delivery selects one peer, channel delivery
selects only matching subscribers, client-ID delivery follows its selection
mode, and broadcast is authorized through the broker policy before selecting
every other peer. Subscription filters inspect message headers: `PREFIX`
supports text and binary values; the other structural filters compare canonical
header values directly. Direct responses retain their message and correlation
IDs unchanged, allowing the receiving client to associate them with its request.

## Reference vectors

The `bus-protocol` test suite contains byte-for-byte vectors for `HELLO` and a
signal `MESSAGE`. They are normative examples of this document and exercise an
independent encoder/decoder interoperability boundary.
