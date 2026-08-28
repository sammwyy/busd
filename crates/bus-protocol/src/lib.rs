#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! Transport-independent BUS/1-preview protocol types and frame codec.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

const MAGIC: [u8; 4] = *b"BUS1";
const VERSION: u8 = 1;
const HEADER_SIZE: usize = 12;
const MAX_NAME_LENGTH: usize = 255;

/// A broker-assigned, ephemeral connection identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PeerId(u64);

impl PeerId {
    /// Creates a peer identifier from its broker-local sequence number.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
    /// Returns the broker-local sequence number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, ":{}", self.0)
    }
}

/// A stable 128-bit logical message identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MessageId([u8; 16]);

impl MessageId {
    /// Creates an identifier from its wire bytes.
    #[must_use]
    pub const fn new(value: [u8; 16]) -> Self {
        Self(value)
    }
    /// Returns the wire bytes.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 16] {
        self.0
    }
    /// Returns the reserved all-zero absent identifier.
    #[must_use]
    pub const fn absent() -> Self {
        Self([0; 16])
    }
    /// Reports whether this is the reserved absent identifier.
    #[must_use]
    pub fn is_absent(self) -> bool {
        self.0 == [0; 16]
    }
}

/// A non-unique client implementation identifier.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClientId(String);

impl ClientId {
    /// Parses a client identifier.
    pub fn parse(value: impl Into<String>) -> Result<Self, NameError> {
        let value = value.into();
        validate_name(&value)?;
        Ok(Self(value))
    }
    /// Returns the identifier as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ClientId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}
impl FromStr for ClientId {
    type Err = NameError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

/// A generic API namespace with the `bus://` scheme.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Namespace(String);

impl Namespace {
    /// Parses a namespace URI.
    pub fn parse(value: impl Into<String>) -> Result<Self, NameError> {
        let value = value.into();
        let Some(name) = value.strip_prefix("bus://") else {
            return Err(NameError::MissingNamespaceScheme);
        };
        validate_name(name)?;
        Ok(Self(value))
    }
    /// Returns the namespace URI.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl fmt::Display for Namespace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}
impl FromStr for Namespace {
    type Err = NameError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

/// An unowned multicast channel.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Channel(String);

impl Channel {
    /// Parses a channel name.
    pub fn parse(value: impl Into<String>) -> Result<Self, NameError> {
        let value = value.into();
        validate_name(&value)?;
        Ok(Self(value))
    }
    /// Returns the channel name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl fmt::Display for Channel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}
impl FromStr for Channel {
    type Err = NameError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

/// A validation error for a protocol name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NameError {
    /// The namespace does not use the `bus://` scheme.
    MissingNamespaceScheme,
    /// The name is empty, too long, or contains unsupported characters.
    Invalid,
}
impl fmt::Display for NameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingNamespaceScheme => formatter.write_str("namespace must start with bus://"),
            Self::Invalid => formatter.write_str(
                "name must contain at most 255 visible ASCII characters except / and whitespace",
            ),
        }
    }
}
impl std::error::Error for NameError {}

/// A claimed client or message header value.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum HeaderValue {
    /// UTF-8 text.
    Text(String),
    /// An unsigned integer.
    Unsigned(u64),
    /// A boolean.
    Boolean(bool),
    /// Opaque binary data.
    Binary(Vec<u8>),
}

/// Claimed or message headers, ordered for canonical encoding.
pub type Headers = BTreeMap<String, HeaderValue>;
/// Negotiated capability names, ordered for canonical encoding.
pub type Capabilities = BTreeSet<String>;

/// The type of a routed message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessageKind {
    /// A one-way message.
    Signal,
    /// A message that expects a response.
    Request,
    /// A reply to a request.
    Response,
}

/// Acknowledgement requested for a message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AckPolicy {
    /// Do not acknowledge delivery.
    None,
    /// Acknowledge broker acceptance.
    Accepted,
    /// Acknowledge recipient receipt.
    Received,
    /// Acknowledge recipient processing.
    Processed,
}

/// A message destination selector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Destination {
    /// The connected broker.
    Broker,
    /// One broker-local peer.
    Peer(PeerId),
    /// The current provider for a namespace.
    Namespace(Namespace),
    /// Explicit subscribers of a channel.
    Channel(Channel),
    /// Every policy-eligible peer.
    Broadcast,
}

/// A structural header filter used by a subscription.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum HeaderFilter {
    /// The named header must exist.
    Exists(String),
    /// The named header must not exist.
    NotExists(String),
    /// The named header must equal the value.
    Equal(String, HeaderValue),
    /// The named header must not equal the value.
    NotEqual(String, HeaderValue),
    /// The named header must have the value as a prefix.
    Prefix(String, HeaderValue),
}

/// A response status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Status {
    /// The operation succeeded.
    Success,
    /// The operation failed without a more specific status.
    Error,
    /// The operation is not supported.
    Unsupported,
    /// The operation was denied.
    Denied,
    /// The recipient is busy.
    Busy,
    /// The requested item was not found.
    NotFound,
}

/// A machine-readable protocol error code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtocolErrorCode {
    /// A frame could not be parsed.
    MalformedFrame,
    /// The peer used an unsupported protocol version.
    UnsupportedVersion,
    /// The peer used an unknown frame kind.
    UnknownFrameKind,
    /// A peer exceeded a configured limit.
    LimitExceeded,
    /// The frame had a non-canonical representation.
    NonCanonical,
    /// The frame is invalid in the current session state.
    InvalidState,
}

/// A complete BUS/1-preview frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Frame {
    /// Begins a session and advertises claimed client metadata.
    Hello {
        /// Optional non-authenticating client implementation identifier.
        client_id: Option<ClientId>,
        /// Claimed client metadata.
        headers: Headers,
        /// Supported optional capabilities.
        capabilities: Capabilities,
    },
    /// Accepts a session and assigns its peer identity.
    Welcome {
        /// Broker-assigned peer identity.
        peer_id: PeerId,
        /// Negotiated optional capabilities.
        capabilities: Capabilities,
    },
    /// Claims a namespace for the sending peer.
    Claim {
        /// The namespace to claim.
        namespace: Namespace,
        /// Claimed namespace metadata.
        headers: Headers,
    },
    /// Adds a channel subscription.
    Subscribe {
        /// The unowned channel to subscribe to.
        channel: Channel,
        /// Structural filters applied to messages on the channel.
        filters: Vec<HeaderFilter>,
    },
    /// Removes a channel subscription.
    Unsubscribe {
        /// The unowned channel to unsubscribe from.
        channel: Channel,
    },
    /// Carries an application message.
    Message {
        /// Routing semantics.
        kind: MessageKind,
        /// Requested delivery acknowledgement.
        ack_policy: AckPolicy,
        /// The intended recipients.
        destination: Destination,
        /// Stable logical message identifier.
        message_id: MessageId,
        /// Request identifier referenced by a response, otherwise absent.
        correlation_id: MessageId,
        /// Response outcome, or success for a signal or request.
        status: Status,
        /// Application metadata.
        headers: Headers,
        /// Opaque application payload.
        payload: Vec<u8>,
    },
    /// Reports a protocol failure to a peer.
    ProtocolError {
        /// Machine-readable error code.
        code: ProtocolErrorCode,
        /// Non-semantic UTF-8 diagnostic text.
        message: String,
    },
}

/// Bounds used by the frame encoder and decoder.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameLimits {
    /// Maximum complete packet size, including the 12-byte common header.
    pub maximum_frame_size: usize,
    /// Maximum number of header pairs in one header block.
    pub maximum_headers: usize,
    /// Maximum capability names in one capability block.
    pub maximum_capabilities: usize,
    /// Maximum filters in one subscription.
    pub maximum_filters: usize,
}
impl Default for FrameLimits {
    fn default() -> Self {
        Self {
            maximum_frame_size: 1_048_576,
            maximum_headers: 128,
            maximum_capabilities: 128,
            maximum_filters: 64,
        }
    }
}

/// A frame encoding or decoding error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CodecError {
    /// The packet is shorter than the common header or a required field.
    Truncated,
    /// The packet magic is not `BUS1`.
    InvalidMagic,
    /// The packet version is unsupported.
    UnsupportedVersion(u8),
    /// The frame kind is unknown.
    UnknownFrameKind(u8),
    /// Reserved flags were non-zero.
    ReservedFlags(u16),
    /// The declared body length did not match the supplied packet.
    LengthMismatch,
    /// The packet exceeds the configured maximum size.
    FrameTooLarge,
    /// A collection exceeds its configured maximum count.
    LimitExceeded(&'static str),
    /// A scalar value is invalid.
    InvalidValue(&'static str),
    /// A valid-looking value is not in canonical form.
    NonCanonical(&'static str),
    /// A protocol name is invalid.
    InvalidName(NameError),
    /// Text is not valid UTF-8.
    InvalidUtf8,
}
impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => formatter.write_str("truncated BUS/1 frame"),
            Self::InvalidMagic => formatter.write_str("invalid BUS/1 frame magic"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported BUS/1 version {version}")
            }
            Self::UnknownFrameKind(kind) => write!(formatter, "unknown BUS/1 frame kind {kind}"),
            Self::ReservedFlags(flags) => {
                write!(formatter, "reserved BUS/1 flags are set: {flags:#06x}")
            }
            Self::LengthMismatch => {
                formatter.write_str("BUS/1 body length does not match packet length")
            }
            Self::FrameTooLarge => formatter.write_str("BUS/1 frame exceeds configured size limit"),
            Self::LimitExceeded(name) => write!(formatter, "BUS/1 {name} exceeds configured limit"),
            Self::InvalidValue(name) => write!(formatter, "invalid BUS/1 {name}"),
            Self::NonCanonical(name) => write!(formatter, "non-canonical BUS/1 {name}"),
            Self::InvalidName(error) => error.fmt(formatter),
            Self::InvalidUtf8 => formatter.write_str("BUS/1 text is not valid UTF-8"),
        }
    }
}
impl std::error::Error for CodecError {}

/// A configured BUS/1-preview frame encoder.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Encoder {
    limits: FrameLimits,
}

impl Encoder {
    /// Creates an encoder that enforces `limits`.
    #[must_use]
    pub const fn new(limits: FrameLimits) -> Self {
        Self { limits }
    }

    /// Encodes `frame` into one complete native packet.
    pub fn encode(&self, frame: &Frame) -> Result<Vec<u8>, CodecError> {
        frame.encode_with_limits(self.limits)
    }
}

/// A configured BUS/1-preview frame decoder.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Decoder {
    limits: FrameLimits,
}

impl Decoder {
    /// Creates a decoder that enforces `limits`.
    #[must_use]
    pub const fn new(limits: FrameLimits) -> Self {
        Self { limits }
    }

    /// Decodes one complete native packet into a frame.
    pub fn decode(&self, packet: &[u8]) -> Result<Frame, CodecError> {
        Frame::decode_with_limits(packet, self.limits)
    }
}

impl Frame {
    /// Encodes this frame using the standard preview limits.
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        self.encode_with_limits(FrameLimits::default())
    }
    /// Encodes this frame using `limits`.
    pub fn encode_with_limits(&self, limits: FrameLimits) -> Result<Vec<u8>, CodecError> {
        let (kind, body) = self.encode_body(limits)?;
        let total = HEADER_SIZE
            .checked_add(body.len())
            .ok_or(CodecError::FrameTooLarge)?;
        if total > limits.maximum_frame_size || body.len() > u32::MAX as usize {
            return Err(CodecError::FrameTooLarge);
        }
        let mut packet = Vec::with_capacity(total);
        packet.extend_from_slice(&MAGIC);
        packet.push(VERSION);
        packet.push(kind);
        push_u16(&mut packet, 0);
        push_u32(&mut packet, body.len() as u32);
        packet.extend_from_slice(&body);
        Ok(packet)
    }
    /// Decodes one complete frame using the standard preview limits.
    pub fn decode(packet: &[u8]) -> Result<Self, CodecError> {
        Self::decode_with_limits(packet, FrameLimits::default())
    }
    /// Decodes one complete frame using `limits`.
    pub fn decode_with_limits(packet: &[u8], limits: FrameLimits) -> Result<Self, CodecError> {
        if packet.len() < HEADER_SIZE {
            return Err(CodecError::Truncated);
        }
        if packet[..4] != MAGIC {
            return Err(CodecError::InvalidMagic);
        }
        if packet[4] != VERSION {
            return Err(CodecError::UnsupportedVersion(packet[4]));
        }
        let flags = u16::from_be_bytes([packet[6], packet[7]]);
        if flags != 0 {
            return Err(CodecError::ReservedFlags(flags));
        }
        let length = u32::from_be_bytes([packet[8], packet[9], packet[10], packet[11]]) as usize;
        let expected = HEADER_SIZE
            .checked_add(length)
            .ok_or(CodecError::FrameTooLarge)?;
        if expected != packet.len() {
            return Err(CodecError::LengthMismatch);
        }
        if expected > limits.maximum_frame_size {
            return Err(CodecError::FrameTooLarge);
        }
        let mut reader = Reader::new(&packet[HEADER_SIZE..]);
        let frame = match packet[5] {
            1 => Self::decode_hello(&mut reader, limits),
            2 => Self::decode_welcome(&mut reader, limits),
            3 => Self::decode_claim(&mut reader, limits),
            4 => Self::decode_subscribe(&mut reader, limits),
            5 => Self::decode_unsubscribe(&mut reader),
            6 => Self::decode_message(&mut reader, limits),
            7 => Self::decode_protocol_error(&mut reader),
            kind => return Err(CodecError::UnknownFrameKind(kind)),
        }?;
        if !reader.is_empty() {
            return Err(CodecError::NonCanonical("trailing frame body"));
        }
        Ok(frame)
    }

    fn encode_body(&self, limits: FrameLimits) -> Result<(u8, Vec<u8>), CodecError> {
        let mut body = Vec::new();
        let kind = match self {
            Self::Hello {
                client_id,
                headers,
                capabilities,
            } => {
                match client_id {
                    Some(id) => {
                        body.push(1);
                        push_name(&mut body, id.as_str())?
                    }
                    None => body.push(0),
                }
                push_headers(&mut body, headers, limits)?;
                push_capabilities(&mut body, capabilities, limits)?;
                1
            }
            Self::Welcome {
                peer_id,
                capabilities,
            } => {
                if peer_id.get() == 0 {
                    return Err(CodecError::InvalidValue("peer ID"));
                }
                push_u64(&mut body, peer_id.get());
                push_capabilities(&mut body, capabilities, limits)?;
                2
            }
            Self::Claim { namespace, headers } => {
                push_namespace(&mut body, namespace)?;
                push_headers(&mut body, headers, limits)?;
                3
            }
            Self::Subscribe { channel, filters } => {
                push_name(&mut body, channel.as_str())?;
                push_filters(&mut body, filters, limits)?;
                4
            }
            Self::Unsubscribe { channel } => {
                push_name(&mut body, channel.as_str())?;
                5
            }
            Self::Message {
                kind,
                ack_policy,
                destination,
                message_id,
                correlation_id,
                status,
                headers,
                payload,
            } => {
                validate_message(*kind, *message_id, *correlation_id, *status)?;
                body.push(message_kind_tag(*kind));
                body.push(ack_policy_tag(*ack_policy));
                push_destination(&mut body, destination)?;
                body.extend_from_slice(&message_id.as_bytes());
                body.extend_from_slice(&correlation_id.as_bytes());
                body.push(status_tag(*status));
                push_headers(&mut body, headers, limits)?;
                push_binary(&mut body, payload)?;
                6
            }
            Self::ProtocolError { code, message } => {
                body.push(protocol_error_code_tag(*code));
                push_text(&mut body, message)?;
                7
            }
        };
        Ok((kind, body))
    }
    fn decode_hello(reader: &mut Reader<'_>, limits: FrameLimits) -> Result<Self, CodecError> {
        let client_id = match reader.u8()? {
            0 => None,
            1 => Some(ClientId::parse(reader.name()?).map_err(CodecError::InvalidName)?),
            _ => return Err(CodecError::InvalidValue("HELLO client ID marker")),
        };
        Ok(Self::Hello {
            client_id,
            headers: reader.headers(limits)?,
            capabilities: reader.capabilities(limits)?,
        })
    }
    fn decode_welcome(reader: &mut Reader<'_>, limits: FrameLimits) -> Result<Self, CodecError> {
        let peer_id = PeerId::new(reader.u64()?);
        if peer_id.get() == 0 {
            return Err(CodecError::InvalidValue("peer ID"));
        }
        Ok(Self::Welcome {
            peer_id,
            capabilities: reader.capabilities(limits)?,
        })
    }
    fn decode_claim(reader: &mut Reader<'_>, limits: FrameLimits) -> Result<Self, CodecError> {
        Ok(Self::Claim {
            namespace: reader.namespace()?,
            headers: reader.headers(limits)?,
        })
    }
    fn decode_subscribe(reader: &mut Reader<'_>, limits: FrameLimits) -> Result<Self, CodecError> {
        Ok(Self::Subscribe {
            channel: Channel::parse(reader.name()?).map_err(CodecError::InvalidName)?,
            filters: reader.filters(limits)?,
        })
    }
    fn decode_unsubscribe(reader: &mut Reader<'_>) -> Result<Self, CodecError> {
        Ok(Self::Unsubscribe {
            channel: Channel::parse(reader.name()?).map_err(CodecError::InvalidName)?,
        })
    }
    fn decode_message(reader: &mut Reader<'_>, limits: FrameLimits) -> Result<Self, CodecError> {
        let kind = decode_message_kind(reader.u8()?)?;
        let ack_policy = decode_ack_policy(reader.u8()?)?;
        let destination = reader.destination()?;
        let message_id = MessageId::new(reader.array_16()?);
        let correlation_id = MessageId::new(reader.array_16()?);
        let status = decode_status(reader.u8()?)?;
        validate_message(kind, message_id, correlation_id, status)?;
        Ok(Self::Message {
            kind,
            ack_policy,
            destination,
            message_id,
            correlation_id,
            status,
            headers: reader.headers(limits)?,
            payload: reader.binary()?,
        })
    }
    fn decode_protocol_error(reader: &mut Reader<'_>) -> Result<Self, CodecError> {
        Ok(Self::ProtocolError {
            code: decode_protocol_error_code(reader.u8()?)?,
            message: reader.text()?,
        })
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
    fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8], CodecError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(CodecError::Truncated)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(CodecError::Truncated)?;
        self.position = end;
        Ok(value)
    }
    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, CodecError> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().expect("fixed length"),
        ))
    }
    fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("fixed length"),
        ))
    }
    fn u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("fixed length"),
        ))
    }
    fn array_16(&mut self) -> Result<[u8; 16], CodecError> {
        Ok(self.take(16)?.try_into().expect("fixed length"))
    }
    fn text(&mut self) -> Result<String, CodecError> {
        let length = self.u16()? as usize;
        let value = std::str::from_utf8(self.take(length)?).map_err(|_| CodecError::InvalidUtf8)?;
        Ok(value.to_owned())
    }
    fn name(&mut self) -> Result<String, CodecError> {
        let value = self.text()?;
        validate_name(&value).map_err(CodecError::InvalidName)?;
        Ok(value)
    }
    fn namespace(&mut self) -> Result<Namespace, CodecError> {
        Namespace::parse(self.text()?).map_err(CodecError::InvalidName)
    }
    fn binary(&mut self) -> Result<Vec<u8>, CodecError> {
        let length = self.u32()? as usize;
        Ok(self.take(length)?.to_vec())
    }
    fn value(&mut self) -> Result<HeaderValue, CodecError> {
        match self.u8()? {
            0 => Ok(HeaderValue::Text(self.text()?)),
            1 => Ok(HeaderValue::Unsigned(self.u64()?)),
            2 => match self.u8()? {
                0 => Ok(HeaderValue::Boolean(false)),
                1 => Ok(HeaderValue::Boolean(true)),
                _ => Err(CodecError::InvalidValue("boolean")),
            },
            3 => Ok(HeaderValue::Binary(self.binary()?)),
            _ => Err(CodecError::InvalidValue("header value tag")),
        }
    }
    fn headers(&mut self, limits: FrameLimits) -> Result<Headers, CodecError> {
        let count = self.u16()? as usize;
        if count > limits.maximum_headers {
            return Err(CodecError::LimitExceeded("header count"));
        }
        let mut headers = Headers::new();
        let mut previous = None;
        for _ in 0..count {
            let name = self.name()?;
            if previous
                .as_ref()
                .is_some_and(|value: &String| name <= *value)
            {
                return Err(CodecError::NonCanonical("headers"));
            }
            previous = Some(name.clone());
            headers.insert(name, self.value()?);
        }
        Ok(headers)
    }
    fn capabilities(&mut self, limits: FrameLimits) -> Result<Capabilities, CodecError> {
        let count = self.u16()? as usize;
        if count > limits.maximum_capabilities {
            return Err(CodecError::LimitExceeded("capability count"));
        }
        let mut capabilities = Capabilities::new();
        let mut previous = None;
        for _ in 0..count {
            let capability = self.name()?;
            if previous
                .as_ref()
                .is_some_and(|value: &String| capability <= *value)
            {
                return Err(CodecError::NonCanonical("capabilities"));
            }
            previous = Some(capability.clone());
            capabilities.insert(capability);
        }
        Ok(capabilities)
    }
    fn filters(&mut self, limits: FrameLimits) -> Result<Vec<HeaderFilter>, CodecError> {
        let count = self.u16()? as usize;
        if count > limits.maximum_filters {
            return Err(CodecError::LimitExceeded("filter count"));
        }
        let mut filters = Vec::with_capacity(count);
        let mut previous = None;
        for _ in 0..count {
            let start = self.position;
            let tag = self.u8()?;
            let name = self.name()?;
            let filter = match tag {
                0 => HeaderFilter::Exists(name),
                1 => HeaderFilter::NotExists(name),
                2 => HeaderFilter::Equal(name, self.value()?),
                3 => HeaderFilter::NotEqual(name, self.value()?),
                4 => HeaderFilter::Prefix(name, self.value()?),
                _ => return Err(CodecError::InvalidValue("filter tag")),
            };
            let encoded = &self.bytes[start..self.position];
            if previous
                .as_ref()
                .is_some_and(|value: &&[u8]| encoded <= *value)
            {
                return Err(CodecError::NonCanonical("filters"));
            }
            previous = Some(encoded);
            filters.push(filter);
        }
        Ok(filters)
    }
    fn destination(&mut self) -> Result<Destination, CodecError> {
        match self.u8()? {
            0 => Ok(Destination::Broker),
            1 => {
                let peer = PeerId::new(self.u64()?);
                if peer.get() == 0 {
                    Err(CodecError::InvalidValue("peer ID"))
                } else {
                    Ok(Destination::Peer(peer))
                }
            }
            2 => Ok(Destination::Namespace(self.namespace()?)),
            3 => Ok(Destination::Channel(
                Channel::parse(self.name()?).map_err(CodecError::InvalidName)?,
            )),
            4 => Ok(Destination::Broadcast),
            _ => Err(CodecError::InvalidValue("destination selector")),
        }
    }
}

fn push_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_be_bytes());
}
fn push_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_be_bytes());
}
fn push_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}
fn push_text(output: &mut Vec<u8>, value: &str) -> Result<(), CodecError> {
    let length =
        u16::try_from(value.len()).map_err(|_| CodecError::LimitExceeded("text length"))?;
    push_u16(output, length);
    output.extend_from_slice(value.as_bytes());
    Ok(())
}
fn push_name(output: &mut Vec<u8>, value: &str) -> Result<(), CodecError> {
    validate_name(value).map_err(CodecError::InvalidName)?;
    push_text(output, value)
}
fn push_namespace(output: &mut Vec<u8>, value: &Namespace) -> Result<(), CodecError> {
    push_text(output, value.as_str())
}
fn push_binary(output: &mut Vec<u8>, value: &[u8]) -> Result<(), CodecError> {
    let length =
        u32::try_from(value.len()).map_err(|_| CodecError::LimitExceeded("binary length"))?;
    push_u32(output, length);
    output.extend_from_slice(value);
    Ok(())
}
fn push_value(output: &mut Vec<u8>, value: &HeaderValue) -> Result<(), CodecError> {
    match value {
        HeaderValue::Text(value) => {
            output.push(0);
            push_text(output, value)
        }
        HeaderValue::Unsigned(value) => {
            output.push(1);
            push_u64(output, *value);
            Ok(())
        }
        HeaderValue::Boolean(value) => {
            output.push(2);
            output.push(u8::from(*value));
            Ok(())
        }
        HeaderValue::Binary(value) => {
            output.push(3);
            push_binary(output, value)
        }
    }
}
fn push_headers(
    output: &mut Vec<u8>,
    headers: &Headers,
    limits: FrameLimits,
) -> Result<(), CodecError> {
    if headers.len() > limits.maximum_headers {
        return Err(CodecError::LimitExceeded("header count"));
    }
    push_u16(
        output,
        u16::try_from(headers.len()).map_err(|_| CodecError::LimitExceeded("header count"))?,
    );
    for (name, value) in headers {
        push_name(output, name)?;
        push_value(output, value)?;
    }
    Ok(())
}
fn push_capabilities(
    output: &mut Vec<u8>,
    capabilities: &Capabilities,
    limits: FrameLimits,
) -> Result<(), CodecError> {
    if capabilities.len() > limits.maximum_capabilities {
        return Err(CodecError::LimitExceeded("capability count"));
    }
    push_u16(
        output,
        u16::try_from(capabilities.len())
            .map_err(|_| CodecError::LimitExceeded("capability count"))?,
    );
    for capability in capabilities {
        push_name(output, capability)?;
    }
    Ok(())
}
fn push_filters(
    output: &mut Vec<u8>,
    filters: &[HeaderFilter],
    limits: FrameLimits,
) -> Result<(), CodecError> {
    if filters.len() > limits.maximum_filters {
        return Err(CodecError::LimitExceeded("filter count"));
    }
    let mut encoded = Vec::with_capacity(filters.len());
    for filter in filters {
        let mut value = Vec::new();
        match filter {
            HeaderFilter::Exists(name) => {
                value.push(0);
                push_name(&mut value, name)?
            }
            HeaderFilter::NotExists(name) => {
                value.push(1);
                push_name(&mut value, name)?
            }
            HeaderFilter::Equal(name, header) => {
                value.push(2);
                push_name(&mut value, name)?;
                push_value(&mut value, header)?
            }
            HeaderFilter::NotEqual(name, header) => {
                value.push(3);
                push_name(&mut value, name)?;
                push_value(&mut value, header)?
            }
            HeaderFilter::Prefix(name, header) => {
                value.push(4);
                push_name(&mut value, name)?;
                push_value(&mut value, header)?
            }
        }
        encoded.push(value);
    }
    if encoded.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(CodecError::NonCanonical("filters"));
    }
    push_u16(
        output,
        u16::try_from(encoded.len()).map_err(|_| CodecError::LimitExceeded("filter count"))?,
    );
    for filter in encoded {
        output.extend_from_slice(&filter);
    }
    Ok(())
}
fn push_destination(output: &mut Vec<u8>, destination: &Destination) -> Result<(), CodecError> {
    match destination {
        Destination::Broker => output.push(0),
        Destination::Peer(peer) => {
            if peer.get() == 0 {
                return Err(CodecError::InvalidValue("peer ID"));
            }
            output.push(1);
            push_u64(output, peer.get());
        }
        Destination::Namespace(namespace) => {
            output.push(2);
            push_namespace(output, namespace)?
        }
        Destination::Channel(channel) => {
            output.push(3);
            push_name(output, channel.as_str())?
        }
        Destination::Broadcast => output.push(4),
    }
    Ok(())
}
fn validate_message(
    kind: MessageKind,
    message_id: MessageId,
    correlation_id: MessageId,
    status: Status,
) -> Result<(), CodecError> {
    if message_id.is_absent() {
        return Err(CodecError::InvalidValue("message ID"));
    }
    match kind {
        MessageKind::Response if correlation_id.is_absent() => {
            Err(CodecError::InvalidValue("response correlation ID"))
        }
        MessageKind::Response => Ok(()),
        _ if !correlation_id.is_absent() => {
            Err(CodecError::NonCanonical("non-response correlation ID"))
        }
        _ if status != Status::Success => Err(CodecError::InvalidValue("non-response status")),
        _ => Ok(()),
    }
}
fn message_kind_tag(value: MessageKind) -> u8 {
    match value {
        MessageKind::Signal => 0,
        MessageKind::Request => 1,
        MessageKind::Response => 2,
    }
}
fn ack_policy_tag(value: AckPolicy) -> u8 {
    match value {
        AckPolicy::None => 0,
        AckPolicy::Accepted => 1,
        AckPolicy::Received => 2,
        AckPolicy::Processed => 3,
    }
}
fn status_tag(value: Status) -> u8 {
    match value {
        Status::Success => 0,
        Status::Error => 1,
        Status::Unsupported => 2,
        Status::Denied => 3,
        Status::Busy => 4,
        Status::NotFound => 5,
    }
}
fn protocol_error_code_tag(value: ProtocolErrorCode) -> u8 {
    match value {
        ProtocolErrorCode::MalformedFrame => 0,
        ProtocolErrorCode::UnsupportedVersion => 1,
        ProtocolErrorCode::UnknownFrameKind => 2,
        ProtocolErrorCode::LimitExceeded => 3,
        ProtocolErrorCode::NonCanonical => 4,
        ProtocolErrorCode::InvalidState => 5,
    }
}
fn decode_message_kind(value: u8) -> Result<MessageKind, CodecError> {
    match value {
        0 => Ok(MessageKind::Signal),
        1 => Ok(MessageKind::Request),
        2 => Ok(MessageKind::Response),
        _ => Err(CodecError::InvalidValue("message kind")),
    }
}
fn decode_ack_policy(value: u8) -> Result<AckPolicy, CodecError> {
    match value {
        0 => Ok(AckPolicy::None),
        1 => Ok(AckPolicy::Accepted),
        2 => Ok(AckPolicy::Received),
        3 => Ok(AckPolicy::Processed),
        _ => Err(CodecError::InvalidValue("acknowledgement policy")),
    }
}
fn decode_status(value: u8) -> Result<Status, CodecError> {
    match value {
        0 => Ok(Status::Success),
        1 => Ok(Status::Error),
        2 => Ok(Status::Unsupported),
        3 => Ok(Status::Denied),
        4 => Ok(Status::Busy),
        5 => Ok(Status::NotFound),
        _ => Err(CodecError::InvalidValue("status")),
    }
}
fn decode_protocol_error_code(value: u8) -> Result<ProtocolErrorCode, CodecError> {
    match value {
        0 => Ok(ProtocolErrorCode::MalformedFrame),
        1 => Ok(ProtocolErrorCode::UnsupportedVersion),
        2 => Ok(ProtocolErrorCode::UnknownFrameKind),
        3 => Ok(ProtocolErrorCode::LimitExceeded),
        4 => Ok(ProtocolErrorCode::NonCanonical),
        5 => Ok(ProtocolErrorCode::InvalidState),
        _ => Err(CodecError::InvalidValue("protocol error code")),
    }
}
fn validate_name(value: &str) -> Result<(), NameError> {
    if value.is_empty()
        || value.len() > MAX_NAME_LENGTH
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != b'/')
    {
        return Err(NameError::Invalid);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn message_id(start: u8) -> MessageId {
        MessageId::new(std::array::from_fn(|index| start.wrapping_add(index as u8)))
    }
    #[test]
    fn namespace_requires_scheme() {
        assert_eq!(
            Namespace::parse("services"),
            Err(NameError::MissingNamespaceScheme)
        );
        assert_eq!(
            Namespace::parse("bus://services").unwrap().as_str(),
            "bus://services"
        );
    }
    #[test]
    fn channel_rejects_whitespace() {
        assert_eq!(Channel::parse("service events"), Err(NameError::Invalid));
    }
    #[test]
    fn hello_golden_vector_round_trips() {
        let frame = Frame::Hello {
            client_id: Some(ClientId::parse("app").unwrap()),
            headers: [("version".into(), HeaderValue::Text("one".into()))].into(),
            capabilities: ["alpha".into()].into(),
        };
        let expected = [
            0x42, 0x55, 0x53, 0x31, 1, 1, 0, 0, 0, 0, 0, 32, 1, 0, 3, b'a', b'p', b'p', 0, 1, 0, 7,
            b'v', b'e', b'r', b's', b'i', b'o', b'n', 0, 0, 3, b'o', b'n', b'e', 0, 1, 0, 5, b'a',
            b'l', b'p', b'h', b'a',
        ];
        assert_eq!(Encoder::default().encode(&frame).unwrap(), expected);
        assert_eq!(Decoder::default().decode(&expected).unwrap(), frame);
    }
    #[test]
    fn message_golden_vector_round_trips() {
        let frame = Frame::Message {
            kind: MessageKind::Signal,
            ack_policy: AckPolicy::None,
            destination: Destination::Namespace(Namespace::parse("bus://svc").unwrap()),
            message_id: message_id(1),
            correlation_id: MessageId::absent(),
            status: Status::Success,
            headers: Headers::new(),
            payload: b"abc".to_vec(),
        };
        let expected = [
            0x42, 0x55, 0x53, 0x31, 1, 6, 0, 0, 0, 0, 0, 56, 0, 0, 2, 0, 9, b'b', b'u', b's', b':',
            b'/', b'/', b's', b'v', b'c', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, b'a', b'b', b'c',
        ];
        assert_eq!(frame.encode().unwrap(), expected);
        assert_eq!(Frame::decode(&expected).unwrap(), frame);
    }
    #[test]
    fn rejects_non_canonical_headers_and_reserved_flags() {
        let mut duplicate_headers = Frame::Hello {
            client_id: None,
            headers: [
                ("a".into(), HeaderValue::Unsigned(0)),
                ("b".into(), HeaderValue::Unsigned(0)),
            ]
            .into(),
            capabilities: Capabilities::new(),
        }
        .encode()
        .unwrap();
        duplicate_headers[29] = b'a';
        assert_eq!(
            Frame::decode(&duplicate_headers),
            Err(CodecError::NonCanonical("headers"))
        );
        let mut valid = Frame::ProtocolError {
            code: ProtocolErrorCode::MalformedFrame,
            message: String::new(),
        }
        .encode()
        .unwrap();
        valid[7] = 1;
        assert_eq!(Frame::decode(&valid), Err(CodecError::ReservedFlags(1)));
    }
    #[test]
    fn enforces_frame_and_collection_limits() {
        let frame = Frame::ProtocolError {
            code: ProtocolErrorCode::MalformedFrame,
            message: "x".repeat(32),
        };
        assert_eq!(
            frame.encode_with_limits(FrameLimits {
                maximum_frame_size: 16,
                ..FrameLimits::default()
            }),
            Err(CodecError::FrameTooLarge)
        );
        let frame = Frame::Hello {
            client_id: None,
            headers: Headers::new(),
            capabilities: ["alpha".into()].into(),
        };
        assert_eq!(
            frame.encode_with_limits(FrameLimits {
                maximum_capabilities: 0,
                ..FrameLimits::default()
            }),
            Err(CodecError::LimitExceeded("capability count"))
        );
    }
    #[test]
    fn control_frames_and_header_values_round_trip() {
        let frames = [
            Frame::Welcome {
                peer_id: PeerId::new(7),
                capabilities: ["fd-passing".into()].into(),
            },
            Frame::Claim {
                namespace: Namespace::parse("bus://service").unwrap(),
                headers: [
                    ("data".into(), HeaderValue::Binary(vec![1, 2])),
                    ("enabled".into(), HeaderValue::Boolean(true)),
                ]
                .into(),
            },
            Frame::Subscribe {
                channel: Channel::parse("events").unwrap(),
                filters: vec![
                    HeaderFilter::Exists("kind".into()),
                    HeaderFilter::Prefix("source".into(), HeaderValue::Text("sys".into())),
                ],
            },
            Frame::Unsubscribe {
                channel: Channel::parse("events").unwrap(),
            },
            Frame::ProtocolError {
                code: ProtocolErrorCode::InvalidState,
                message: "HELLO already received".into(),
            },
        ];
        let encoder = Encoder::default();
        let decoder = Decoder::default();
        for frame in frames {
            assert_eq!(
                decoder.decode(&encoder.encode(&frame).unwrap()).unwrap(),
                frame
            );
        }
    }
    #[test]
    fn arbitrary_input_never_panics() {
        let mut seed = 0x7a5b_1234_9876_u64;
        for length in 0..512 {
            let mut bytes = vec![0; length];
            for byte in &mut bytes {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                *byte = (seed >> 24) as u8;
            }
            assert!(std::panic::catch_unwind(|| Frame::decode(&bytes)).is_ok());
        }
    }
    #[test]
    fn canonical_frames_round_trip_over_payload_boundaries() {
        for length in [0, 1, 255, 256, 65_535] {
            let frame = Frame::Message {
                kind: MessageKind::Signal,
                ack_policy: AckPolicy::Accepted,
                destination: Destination::Channel(Channel::parse("events").unwrap()),
                message_id: message_id(4),
                correlation_id: MessageId::absent(),
                status: Status::Success,
                headers: [("count".into(), HeaderValue::Unsigned(length as u64))].into(),
                payload: vec![0xa5; length],
            };
            let encoded = frame.encode().unwrap();
            assert_eq!(Frame::decode(&encoded).unwrap(), frame);
        }
    }
}
