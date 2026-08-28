#![cfg_attr(not(feature = "unix"), allow(dead_code))]

use std::collections::BTreeMap;
use std::env;
use std::ffi::OsString;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bus_broker::{Broker, ClientHello, Error as BrokerError};
use bus_policy::{AllowAll, Credentials};
use bus_protocol::{CodecError, Frame, FrameLimits, ProtocolErrorCode};

#[cfg(feature = "unix")]
use bus_transport_unix::{Connection, Listener};

const DEFAULT_SOCKET: &str = "/run/busd/busd.sock";

struct Command {
    socket: PathBuf,
}

#[cfg(feature = "unix")]
fn main() -> io::Result<()> {
    let command = parse_command(env::args_os().skip(1))?;
    run_daemon(command.socket)
}

#[cfg(not(feature = "unix"))]
fn main() -> io::Result<()> {
    let _ = parse_command(env::args_os().skip(1))?;
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "busd requires the unix feature",
    ))
}

fn parse_command(arguments: impl Iterator<Item = OsString>) -> io::Result<Command> {
    let mut arguments = arguments;
    match arguments
        .next()
        .and_then(|value| value.into_string().ok())
        .as_deref()
    {
        Some("daemon") => {}
        _ => return Err(usage()),
    }

    let mut socket = PathBuf::from(DEFAULT_SOCKET);
    while let Some(argument) = arguments.next() {
        match argument.to_str() {
            Some("--socket") => socket = arguments.next().ok_or_else(usage)?.into(),
            _ => return Err(usage()),
        }
    }
    Ok(Command { socket })
}

fn usage() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "usage: busd daemon [--socket PATH]",
    )
}

#[cfg(feature = "unix")]
fn run_daemon(socket: PathBuf) -> io::Result<()> {
    let broker = Arc::new(Mutex::new(Broker::new(AllowAll)));
    let sessions = Arc::new(Mutex::new(BTreeMap::new()));
    let listener = Listener::bind(&socket)?;
    eprintln!("busd: listening on {}", listener.path().display());
    loop {
        let peer = listener.accept()?;
        let broker = Arc::clone(&broker);
        let sessions = Arc::clone(&sessions);
        std::thread::spawn(move || {
            if let Err(error) = serve_peer(peer, broker, sessions) {
                eprintln!("busd: protocol peer failed: {error}");
            }
        });
    }
}

#[cfg(feature = "unix")]
fn serve_peer(
    peer: Connection,
    broker: Arc<Mutex<Broker<AllowAll>>>,
    sessions: Arc<Mutex<BTreeMap<bus_protocol::PeerId, Connection>>>,
) -> io::Result<()> {
    let limits = FrameLimits::default();
    let credentials = peer.peer_credentials()?;
    let Some(packet) = peer.receive_packet(limits.maximum_frame_size)? else {
        return Ok(());
    };
    let hello = match Frame::decode_with_limits(&packet, limits) {
        Ok(Frame::Hello {
            client_id,
            headers,
            capabilities,
        }) => ClientHello {
            client_id,
            headers,
            capabilities,
        },
        Ok(_) => {
            return reject(
                &peer,
                ProtocolErrorCode::InvalidState,
                "HELLO must be the first frame",
            );
        }
        Err(error) => return reject(&peer, codec_error_code(&error), &error.to_string()),
    };
    let session = match broker.lock().map_err(lock_error)?.connect_session(
        Credentials {
            pid: credentials.pid,
            uid: credentials.uid,
            gid: credentials.gid,
        },
        hello,
    ) {
        Ok(session) => session,
        Err(error) => return reject(&peer, ProtocolErrorCode::InvalidState, &error.to_string()),
    };
    peer.send_packet(
        &Frame::Welcome {
            peer_id: session.id,
            capabilities: session.capabilities,
        }
        .encode_with_limits(limits)
        .map_err(codec_io_error)?,
    )?;
    sessions
        .lock()
        .map_err(lock_error)?
        .insert(session.id, peer.try_clone()?);

    let result = dispatch_session(&peer, session.id, &broker, &sessions, limits);
    sessions.lock().map_err(lock_error)?.remove(&session.id);
    let disconnect = broker.lock().map_err(lock_error)?.disconnect(session.id);
    if let Err(error) = disconnect {
        return Err(io::Error::other(error));
    }
    result
}

#[cfg(feature = "unix")]
fn dispatch_session(
    peer: &Connection,
    peer_id: bus_protocol::PeerId,
    broker: &Arc<Mutex<Broker<AllowAll>>>,
    sessions: &Arc<Mutex<BTreeMap<bus_protocol::PeerId, Connection>>>,
    limits: FrameLimits,
) -> io::Result<()> {
    loop {
        let packet = match peer.receive_packet(limits.maximum_frame_size) {
            Ok(Some(packet)) => packet,
            Ok(None) => break,
            Err(error) if error.kind() == io::ErrorKind::ConnectionReset => break,
            Err(error) => return Err(error),
        };
        let frame = match Frame::decode_with_limits(&packet, limits) {
            Ok(frame) => frame,
            Err(error) => return reject(peer, codec_error_code(&error), &error.to_string()),
        };
        match frame {
            Frame::Claim { namespace, headers } if headers.is_empty() => {
                let result = broker.lock().map_err(lock_error)?.claim(peer_id, namespace);
                send_control_result(peer, result, bus_protocol::ControlOperation::Claim, limits)?;
                continue;
            }
            Frame::Claim { .. } => {
                return reject(
                    peer,
                    ProtocolErrorCode::InvalidState,
                    "claim metadata is not supported in this phase",
                );
            }
            Frame::Subscribe { channel, filters } => {
                let result = broker
                    .lock()
                    .map_err(lock_error)?
                    .subscribe_with_filters(peer_id, channel, filters);
                send_control_result(
                    peer,
                    result,
                    bus_protocol::ControlOperation::Subscribe,
                    limits,
                )?;
                continue;
            }
            Frame::Unsubscribe { channel } => {
                let result = broker
                    .lock()
                    .map_err(lock_error)?
                    .unsubscribe(peer_id, &channel);
                send_control_result(
                    peer,
                    result,
                    bus_protocol::ControlOperation::Unsubscribe,
                    limits,
                )?;
                continue;
            }
            Frame::ResolveNamespace { namespace } => {
                let owner = broker
                    .lock()
                    .map_err(lock_error)?
                    .namespace_owner(&namespace);
                peer.send_packet(
                    &Frame::NamespaceResolved { namespace, owner }
                        .encode_with_limits(limits)
                        .map_err(codec_io_error)?,
                )?;
                continue;
            }
            Frame::Message {
                ref destination,
                ref headers,
                ..
            } => {
                let recipients =
                    match broker
                        .lock()
                        .map_err(lock_error)?
                        .route(peer_id, destination, headers)
                    {
                        Ok(recipients) => recipients,
                        Err(error) => {
                            peer.send_packet(
                                &Frame::ProtocolError {
                                    code: broker_error_code(&error),
                                    message: error.to_string(),
                                }
                                .encode_with_limits(limits)
                                .map_err(codec_io_error)?,
                            )?;
                            continue;
                        }
                    };
                let outputs: Vec<_> = sessions
                    .lock()
                    .map_err(lock_error)?
                    .iter()
                    .filter(|(id, _)| recipients.contains(id))
                    .map(|(_, connection)| connection.try_clone())
                    .collect::<io::Result<_>>()?;
                let encoded = frame.encode_with_limits(limits).map_err(codec_io_error)?;
                for connection in outputs {
                    connection.send_packet(&encoded)?;
                }
                continue;
            }
            Frame::Hello { .. } => {
                return reject(
                    peer,
                    ProtocolErrorCode::InvalidState,
                    "HELLO was already received",
                );
            }
            Frame::Welcome { .. }
            | Frame::ProtocolError { .. }
            | Frame::ControlResult { .. }
            | Frame::NamespaceResolved { .. } => {
                return reject(
                    peer,
                    ProtocolErrorCode::InvalidState,
                    "frame is not valid for a client session",
                );
            }
        };
    }
    Ok(())
}

#[cfg(feature = "unix")]
fn send_control_result(
    peer: &Connection,
    result: Result<(), bus_broker::Error>,
    operation: bus_protocol::ControlOperation,
    limits: FrameLimits,
) -> io::Result<()> {
    let frame = match result {
        Ok(()) => Frame::ControlResult { operation },
        Err(error) => Frame::ProtocolError {
            code: ProtocolErrorCode::InvalidState,
            message: error.to_string(),
        },
    };
    peer.send_packet(&frame.encode_with_limits(limits).map_err(codec_io_error)?)
}

#[cfg(feature = "unix")]
fn broker_error_code(error: &BrokerError) -> ProtocolErrorCode {
    match error {
        BrokerError::NoRecipient => ProtocolErrorCode::NoRecipient,
        _ => ProtocolErrorCode::InvalidState,
    }
}

#[cfg(feature = "unix")]
fn reject(peer: &Connection, code: ProtocolErrorCode, message: &str) -> io::Result<()> {
    let frame = Frame::ProtocolError {
        code,
        message: message.into(),
    };
    peer.send_packet(
        &frame
            .encode_with_limits(FrameLimits::default())
            .map_err(codec_io_error)?,
    )
}

#[cfg(feature = "unix")]
fn codec_error_code(error: &CodecError) -> ProtocolErrorCode {
    match error {
        CodecError::UnsupportedVersion(_) => ProtocolErrorCode::UnsupportedVersion,
        CodecError::UnknownFrameKind(_) => ProtocolErrorCode::UnknownFrameKind,
        CodecError::FrameTooLarge | CodecError::LimitExceeded(_) => {
            ProtocolErrorCode::LimitExceeded
        }
        CodecError::NonCanonical(_) | CodecError::ReservedFlags(_) => {
            ProtocolErrorCode::NonCanonical
        }
        _ => ProtocolErrorCode::MalformedFrame,
    }
}

#[cfg(feature = "unix")]
fn codec_io_error(error: CodecError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[cfg(feature = "unix")]
fn lock_error<T>(_: std::sync::PoisonError<T>) -> io::Error {
    io::Error::other("broker state lock poisoned")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "unix")]
    use bus_protocol::{Capabilities, Channel, Namespace};
    #[cfg(feature = "unix")]
    use std::process;
    #[cfg(feature = "unix")]
    use std::thread;
    #[cfg(feature = "unix")]
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn command_requires_daemon_mode() {
        assert!(parse_command(std::iter::empty()).is_err());
        assert!(parse_command(["send".into()].into_iter()).is_err());
    }

    #[test]
    fn daemon_uses_the_system_socket_by_default() {
        let command = parse_command(["daemon".into()].into_iter()).unwrap();
        assert_eq!(command.socket, PathBuf::from(DEFAULT_SOCKET));
    }

    #[test]
    fn daemon_accepts_a_custom_socket() {
        let command = parse_command(
            ["daemon", "--socket", "/tmp/busd.sock"]
                .into_iter()
                .map(OsString::from),
        )
        .unwrap();
        assert_eq!(command.socket, PathBuf::from("/tmp/busd.sock"));
    }

    #[cfg(feature = "unix")]
    #[test]
    fn session_disconnect_releases_broker_state() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("busd-session-{}-{nonce}.sock", process::id()));
        let listener = Listener::bind(&path).unwrap();
        let broker = Arc::new(Mutex::new(Broker::new(AllowAll)));
        let server_broker = Arc::clone(&broker);
        let server = thread::spawn(move || {
            serve_peer(
                listener.accept().unwrap(),
                server_broker,
                Arc::new(Mutex::new(BTreeMap::new())),
            )
        });
        let connection = Connection::connect(&path).unwrap();
        connection
            .send_packet(
                &Frame::Hello {
                    client_id: None,
                    headers: Default::default(),
                    capabilities: Capabilities::new(),
                }
                .encode()
                .unwrap(),
            )
            .unwrap();
        assert!(matches!(
            Frame::decode(
                &connection
                    .receive_packet(FrameLimits::default().maximum_frame_size)
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            Frame::Welcome { .. }
        ));
        let namespace = Namespace::parse("bus://service").unwrap();
        connection
            .send_packet(
                &Frame::Claim {
                    namespace: namespace.clone(),
                    headers: Default::default(),
                }
                .encode()
                .unwrap(),
            )
            .unwrap();
        for _ in 0..100 {
            if broker.lock().unwrap().namespace_owner(&namespace).is_some() {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        assert!(broker.lock().unwrap().namespace_owner(&namespace).is_some());
        connection.disconnect().unwrap();
        server.join().unwrap().unwrap();
        assert_eq!(broker.lock().unwrap().namespace_owner(&namespace), None);
        std::fs::remove_file(path).unwrap();
    }

    #[cfg(feature = "unix")]
    #[test]
    fn invalid_first_frame_receives_protocol_error() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("busd-reject-{}-{nonce}.sock", process::id()));
        let listener = Listener::bind(&path).unwrap();
        let broker = Arc::new(Mutex::new(Broker::new(AllowAll)));
        let server = thread::spawn(move || {
            serve_peer(
                listener.accept().unwrap(),
                broker,
                Arc::new(Mutex::new(BTreeMap::new())),
            )
        });
        let connection = Connection::connect(&path).unwrap();
        connection
            .send_packet(
                &Frame::Unsubscribe {
                    channel: Channel::parse("events").unwrap(),
                }
                .encode()
                .unwrap(),
            )
            .unwrap();
        let frame = Frame::decode(
            &connection
                .receive_packet(FrameLimits::default().maximum_frame_size)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            frame,
            Frame::ProtocolError {
                code: ProtocolErrorCode::InvalidState,
                ..
            }
        ));
        server.join().unwrap().unwrap();
        std::fs::remove_file(path).unwrap();
    }
}
