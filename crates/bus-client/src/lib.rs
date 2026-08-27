#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! Application-facing native BUS/1 client API.
//!
//! BUS/1 command encoding begins once the wire ABI is specified. This crate
//! already provides the stable application dependency boundary and native
//! connection setup, while re-exporting all protocol model types.

use std::io;
use std::path::{Path, PathBuf};

pub use bus_protocol::*;
pub use bus_transport_unix::Connection;

/// Configuration for connecting to a local broker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectOptions {
    socket: PathBuf,
}

impl ConnectOptions {
    /// Uses `socket` as the native BUS socket path.
    #[must_use]
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    /// Returns the configured native BUS socket path.
    #[must_use]
    pub fn socket(&self) -> &Path {
        &self.socket
    }
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self::new("/run/busd/busd.sock")
    }
}

/// A connection to a local BUS broker.
pub struct Bus {
    connection: Connection,
}

impl Bus {
    /// Opens the native transport connection to a broker.
    pub fn connect(options: ConnectOptions) -> io::Result<Self> {
        Ok(Self {
            connection: Connection::connect(options.socket)?,
        })
    }

    /// Returns the underlying native transport connection.
    #[must_use]
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Sends a raw native packet for diagnostics.
    ///
    /// This does not encode a BUS/1 message and is not a substitute for the
    /// future typed BUS/1 API.
    pub fn send_debug_packet(&self, packet: &[u8]) -> io::Result<()> {
        self.connection.send_packet(packet)
    }
}
