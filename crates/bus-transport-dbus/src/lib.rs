#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! Optional D-Bus transport primitives.
//!
//! This crate owns D-Bus wire access only. It deliberately does not translate
//! D-Bus methods into BUS API semantics; applications or a compatibility
//! adapter must make that decision explicitly.

use std::fmt;

/// A validated D-Bus well-known or interface name.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Name(String);

impl Name {
    /// Parses a D-Bus name using the well-known-name grammar.
    pub fn parse(value: impl Into<String>) -> Result<Self, NameError> {
        let value = value.into();
        validate_name(&value)?;
        Ok(Self(value))
    }

    /// Returns the validated D-Bus name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Name {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A D-Bus name validation error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NameError {
    /// The name is empty, too long, or has invalid separators or characters.
    Invalid,
}

impl fmt::Display for NameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid D-Bus name")
    }
}

impl std::error::Error for NameError {}

/// The D-Bus bus personality used by a compatibility transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BusKind {
    /// The per-user session bus.
    Session,
    /// The system bus.
    System,
}

/// A blocking D-Bus connection used by an explicit transport adapter.
pub struct DbusTransport {
    connection: zbus::blocking::Connection,
}

impl DbusTransport {
    /// Connects to the selected standard D-Bus bus.
    pub fn connect(kind: BusKind) -> zbus::Result<Self> {
        let connection = match kind {
            BusKind::Session => zbus::blocking::Connection::session()?,
            BusKind::System => zbus::blocking::Connection::system()?,
        };
        Ok(Self { connection })
    }

    /// Requests a validated well-known name on this connection.
    pub fn request_name(&self, name: &Name) -> zbus::Result<()> {
        self.connection.request_name(name.as_str())
    }

    /// Returns the underlying D-Bus connection for a semantic adapter.
    pub fn connection(&self) -> &zbus::blocking::Connection {
        &self.connection
    }
}

fn validate_name(value: &str) -> Result<(), NameError> {
    if value.is_empty() || value.len() > 255 || value.starts_with('.') || value.ends_with('.') {
        return Err(NameError::Invalid);
    }
    if value.split('.').any(|component| {
        component.is_empty()
            || !component
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    }) {
        return Err(NameError::Invalid);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_generic_dbus_names_without_freedesktop_bias() {
        assert!(Name::parse("com.example.Service").is_ok());
        assert!(Name::parse("org.freedesktop.systemd1").is_ok());
        assert!(Name::parse("example..Service").is_err());
        assert!(Name::parse("-example.Service").is_err());
    }
}
