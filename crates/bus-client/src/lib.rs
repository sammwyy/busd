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
    client_id: Option<ClientId>,
    headers: Headers,
    capabilities: Capabilities,
}

impl ConnectOptions {
    /// Uses `socket` as the native BUS socket path.
    #[must_use]
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
            frame_limits: FrameLimits::default(),
            client_id: None,
            headers: Headers::new(),
            capabilities: Capabilities::new(),
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

    /// Advertises a non-authenticating client implementation identifier.
    #[must_use]
    pub fn with_client_id(mut self, client_id: ClientId) -> Self {
        self.client_id = Some(client_id);
        self
    }

    /// Advertises claimed client metadata during the handshake.
    #[must_use]
    pub fn with_headers(mut self, headers: Headers) -> Self {
        self.headers = headers;
        self
    }

    /// Offers optional protocol capabilities during the handshake.
    #[must_use]
    pub fn with_capabilities(mut self, capabilities: Capabilities) -> Self {
        self.capabilities = capabilities;
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
    peer_id: PeerId,
    capabilities: Capabilities,
}

impl Bus {
    /// Opens the native transport connection to a broker.
    pub fn connect(options: ConnectOptions) -> Result<Self, Error> {
        let connection = Connection::connect(options.socket)?;
        let hello = Frame::Hello {
            client_id: options.client_id,
            headers: options.headers,
            capabilities: options.capabilities,
        };
        connection.send_packet(&hello.encode_with_limits(options.frame_limits)?)?;
        let Some(packet) = connection.receive_packet(options.frame_limits.maximum_frame_size)?
        else {
            return Err(Error::Handshake("broker disconnected before WELCOME"));
        };
        match Frame::decode_with_limits(&packet, options.frame_limits)? {
            Frame::Welcome {
                peer_id,
                capabilities,
            } => Ok(Self {
                connection,
                frame_limits: options.frame_limits,
                peer_id,
                capabilities,
            }),
            Frame::ProtocolError { code, message } => Err(Error::Rejected { code, message }),
            _ => Err(Error::Handshake(
                "broker sent a non-WELCOME handshake frame",
            )),
        }
    }

    /// Returns the broker-assigned identity for this connection.
    #[must_use]
    pub const fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// Returns the capabilities selected by the broker.
    #[must_use]
    pub fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    /// Encodes and sends one BUS/1-preview frame.
    pub fn send_frame(&self, frame: &Frame) -> Result<(), Error> {
        let packet = frame.encode_with_limits(self.frame_limits)?;
        self.connection.send_packet(&packet)?;
        Ok(())
    }

    /// Claims `namespace` for this connected peer.
    pub fn claim(&self, namespace: Namespace) -> Result<(), Error> {
        self.send_control(
            Frame::Claim {
                namespace,
                headers: Headers::new(),
            },
            ControlOperation::Claim,
        )
    }

    /// Subscribes this peer to a channel using structural message-header filters.
    pub fn subscribe(&self, channel: Channel, filters: Vec<HeaderFilter>) -> Result<(), Error> {
        self.send_control(
            Frame::Subscribe { channel, filters },
            ControlOperation::Subscribe,
        )
    }

    /// Removes this peer's subscription from a channel.
    pub fn unsubscribe(&self, channel: Channel) -> Result<(), Error> {
        self.send_control(
            Frame::Unsubscribe { channel },
            ControlOperation::Unsubscribe,
        )
    }

    /// Resolves the current owner of `namespace`.
    pub fn resolve_namespace(&self, namespace: Namespace) -> Result<Option<PeerId>, Error> {
        self.send_frame(&Frame::ResolveNamespace {
            namespace: namespace.clone(),
        })?;
        match self.receive_frame()? {
            Some(Frame::NamespaceResolved {
                namespace: resolved,
                owner,
            }) if resolved == namespace => Ok(owner),
            Some(Frame::ProtocolError { code, message }) => Err(Error::Rejected { code, message }),
            Some(_) => Err(Error::Handshake(
                "broker sent an unexpected discovery frame",
            )),
            None => Err(Error::Handshake(
                "broker disconnected during namespace resolution",
            )),
        }
    }

    /// Sends an application message through the broker.
    pub fn send_message(&self, message: &Frame) -> Result<(), Error> {
        if !matches!(message, Frame::Message { .. }) {
            return Err(Error::Handshake("send_message requires a MESSAGE frame"));
        }
        self.send_frame(message)
    }

    /// Confirms that this peer received or processed a routed logical message.
    pub fn acknowledge(&self, message_id: MessageId, policy: AckPolicy) -> Result<(), Error> {
        self.send_frame(&Frame::Acknowledge { message_id, policy })
    }

    /// Sends a request and waits for its correlated response or terminal delivery outcome.
    pub fn request(&self, request: &Frame) -> Result<Frame, Error> {
        let Frame::Message {
            kind: MessageKind::Request,
            message_id,
            ..
        } = request
        else {
            return Err(Error::Handshake("request requires a REQUEST message"));
        };
        self.send_message(request)?;
        loop {
            match self.receive_frame()? {
                Some(
                    response @ Frame::Message {
                        kind: MessageKind::Response,
                        correlation_id,
                        ..
                    },
                ) if correlation_id == *message_id => return Ok(response),
                Some(Frame::DeliveryResult {
                    message_id: result_id,
                    outcome,
                }) if result_id == *message_id => match outcome {
                    DeliveryOutcome::Accepted
                    | DeliveryOutcome::Received
                    | DeliveryOutcome::Processed => {}
                    _ => return Err(Error::Delivery(outcome)),
                },
                Some(Frame::ProtocolError { code, message }) => {
                    return Err(Error::Rejected { code, message });
                }
                Some(_) => {
                    return Err(Error::Handshake(
                        "broker sent an unrelated frame during request",
                    ));
                }
                None => return Err(Error::Handshake("broker disconnected during request")),
            }
        }
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

    /// Shuts down the native connection and releases the broker session.
    pub fn disconnect(self) -> Result<(), Error> {
        self.connection.disconnect()?;
        Ok(())
    }

    fn send_control(&self, frame: Frame, operation: ControlOperation) -> Result<(), Error> {
        self.send_frame(&frame)?;
        match self.receive_frame()? {
            Some(Frame::ControlResult { operation: result }) if result == operation => Ok(()),
            Some(Frame::ProtocolError { code, message }) => Err(Error::Rejected { code, message }),
            Some(_) => Err(Error::Handshake("broker sent an unexpected control frame")),
            None => Err(Error::Handshake(
                "broker disconnected during a control operation",
            )),
        }
    }
}

/// A native transport or BUS/1 frame error.
#[derive(Debug)]
pub enum Error {
    /// The native Unix transport failed.
    Transport(io::Error),
    /// A frame could not be encoded or decoded.
    Codec(CodecError),
    /// The broker violated the required handshake sequence.
    Handshake(&'static str),
    /// The broker rejected the handshake with a structured protocol error.
    Rejected {
        /// Machine-readable protocol error code.
        code: ProtocolErrorCode,
        /// Diagnostic text supplied by the broker.
        message: String,
    },
    /// The broker completed delivery without an application response.
    Delivery(DeliveryOutcome),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => error.fmt(formatter),
            Self::Codec(error) => error.fmt(formatter),
            Self::Handshake(message) => formatter.write_str(message),
            Self::Rejected { code, message } => {
                write!(formatter, "broker rejected handshake ({code:?}): {message}")
            }
            Self::Delivery(outcome) => write!(formatter, "broker delivery failed: {outcome:?}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Codec(error) => Some(error),
            Self::Handshake(_) | Self::Rejected { .. } | Self::Delivery(_) => None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use bus_transport_unix::Listener;
    use std::process;
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn connect_performs_handshake_and_disconnects() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("busd-client-{}-{nonce}.sock", process::id()));
        let listener = Listener::bind(&path).unwrap();
        let server = thread::spawn(move || {
            let peer = listener.accept().unwrap();
            let hello = Frame::decode(
                &peer
                    .receive_packet(FrameLimits::default().maximum_frame_size)
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            peer.send_packet(
                &Frame::Welcome {
                    peer_id: PeerId::new(9),
                    capabilities: ["fd-passing".into()].into(),
                }
                .encode()
                .unwrap(),
            )
            .unwrap();
            assert!(
                peer.receive_packet(FrameLimits::default().maximum_frame_size)
                    .unwrap()
                    .is_none()
            );
            hello
        });

        let bus = Bus::connect(
            ConnectOptions::new(&path)
                .with_client_id(ClientId::parse("client").unwrap())
                .with_capabilities(["fd-passing".into()].into()),
        )
        .unwrap();
        assert_eq!(bus.peer_id(), PeerId::new(9));
        assert_eq!(
            bus.capabilities(),
            &Capabilities::from(["fd-passing".into()])
        );
        bus.disconnect().unwrap();
        assert!(matches!(server.join().unwrap(), Frame::Hello { .. }));
        std::fs::remove_file(path).unwrap();
    }
}
