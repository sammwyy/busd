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
| 4 | 1 | version | `1` for BUS/1-preview |
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
`/` and whitespace. A namespace is encoded as a `name` whose text includes the
literal `bus:` prefix and whose value passes the namespace validation rule
(`bus://` followed by a name). A text value is `u16 byte-length` followed by
valid UTF-8. Binary data is `u32 byte-length` followed by bytes.

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

Capabilities are `u16 count` followed by names, strictly increasing and unique.
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

`HELLO` starts with one byte (`0` no client ID, `1` name follows), then headers
and capabilities. `WELCOME` is `peer-id` then capabilities. `CLAIM` is a
namespace then headers. `SUBSCRIBE` and `UNSUBSCRIBE` operate on an unowned
channel. A peer may send `HELLO` only once before normal frames; session-state
rules and authorization are enforced by the broker, not by this codec.

## Message envelope

`MESSAGE` bodies have this exact order:

| Field | Encoding |
| --- | --- |
| message kind | `0 SIGNAL`, `1 REQUEST`, `2 RESPONSE` |
| acknowledgement policy | `0 NONE`, `1 ACCEPTED`, `2 RECEIVED`, `3 PROCESSED` |
| destination selector | selector below |
| message ID | `id` |
| correlation ID | `id` |
| status | status byte |
| headers | headers encoding |
| payload | binary data |

Destination selectors are `0 BROKER`, `1 PEER` plus `peer-id`, `2 NAMESPACE`
plus namespace, `3 CHANNEL` plus channel, and `4 BROADCAST`. `SIGNAL` and
`REQUEST` must use status `SUCCESS`; a `RESPONSE` may use any status. A response
must have a non-zero correlation ID. The current statuses are `0 SUCCESS`, `1
ERROR`, `2 UNSUPPORTED`, `3 DENIED`, `4 BUSY`, and `5 NOT_FOUND`.

## Protocol errors and extension behavior

`PROTOCOL_ERROR` is an error-code byte followed by UTF-8 text. Codes are `0
MALFORMED_FRAME`, `1 UNSUPPORTED_VERSION`, `2 UNKNOWN_FRAME_KIND`, `3
LIMIT_EXCEEDED`, `4 NON_CANONICAL`, and `5 INVALID_STATE`. Text is diagnostic
only and must not be parsed for behavior.

All reserved fields and flags are mandatory-zero in this preview. Unknown
values are rejected, rather than ignored, because no extension point has yet
been negotiated. Future versions may define optional extensions only through a
new version or an explicitly negotiated capability; they must preserve these
validation rules for BUS/1-preview packets.

## Reference vectors

The `bus-protocol` test suite contains byte-for-byte vectors for `HELLO` and a
signal `MESSAGE`. They are normative examples of this document and exercise an
independent encoder/decoder interoperability boundary.
