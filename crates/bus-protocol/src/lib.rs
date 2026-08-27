#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! Protocol-level types for BUS/1.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

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
    /// The name is empty or contains unsupported characters.
    Invalid,
}

impl fmt::Display for NameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingNamespaceScheme => formatter.write_str("namespace must start with bus://"),
            Self::Invalid => formatter
                .write_str("name must use visible ASCII characters except / and whitespace"),
        }
    }
}

impl std::error::Error for NameError {}

/// A claimed client header value.
#[derive(Clone, Debug, Eq, PartialEq)]
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

/// Claimed or message headers, ordered for deterministic processing.
pub type Headers = BTreeMap<String, HeaderValue>;

/// The type of a message routed by BUS/1.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessageKind {
    /// A one-way message.
    Signal,
    /// A message that requires a response.
    Request,
    /// A reply to a request.
    Response,
}

/// Acknowledgement requested for a signal.
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

fn validate_name(value: &str) -> Result<(), NameError> {
    if value.is_empty()
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
}
