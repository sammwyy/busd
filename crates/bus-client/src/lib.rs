#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! Application-facing native BUS/1 client API.
//!
//! This crate provides the application-facing native connection and frame API.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

pub use bus_protocol::*;
use bus_transport_unix::Connection;

/// Configuration for connecting to a local broker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectOptions {
    socket: PathBuf,
    frame_limits: FrameLimits,
}

impl ConnectOptions {
    /// Uses `socket` as the native BUS socket path.
    #[must_use]
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
            frame_limits: FrameLimits::default(),
        }
    }

    /// Returns the configured native BUS socket path.
    #[must_use]
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// Uses `limits` while encoding and decoding protocol frames.
    #[must_use]
    pub fn with_frame_limits(mut self, limits: FrameLimits) -> Self {
        self.frame_limits = limits;
        self
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
    frame_limits: FrameLimits,
}

impl Bus {
    /// Opens the native transport connection to a broker.
    pub fn connect(options: ConnectOptions) -> io::Result<Self> {
        Ok(Self {
            connection: Connection::connect(options.socket)?,
            frame_limits: options.frame_limits,
        })
    }

    /// Encodes and sends one BUS/1-preview frame.
    pub fn send_frame(&self, frame: &Frame) -> Result<(), Error> {
        let packet = frame.encode_with_limits(self.frame_limits)?;
        self.connection.send_packet(&packet)?;
        Ok(())
    }

    /// Receives and decodes one BUS/1-preview frame.
    ///
    /// Returns `None` when the broker disconnects.
    pub fn receive_frame(&self) -> Result<Option<Frame>, Error> {
        let Some(packet) = self
            .connection
            .receive_packet(self.frame_limits.maximum_frame_size)?
        else {
            return Ok(None);
        };
        Ok(Some(Frame::decode_with_limits(&packet, self.frame_limits)?))
    }
}

/// A native transport or BUS/1 frame error.
#[derive(Debug)]
pub enum Error {
    /// The native Unix transport failed.
    Transport(io::Error),
    /// A frame could not be encoded or decoded.
    Codec(CodecError),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => error.fmt(formatter),
            Self::Codec(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Codec(error) => Some(error),
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Transport(error)
    }
}

impl From<CodecError> for Error {
    fn from(error: CodecError) -> Self {
        Self::Codec(error)
    }
}
