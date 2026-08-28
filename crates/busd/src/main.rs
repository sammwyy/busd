#![cfg_attr(not(feature = "unix"), allow(dead_code))]

use std::env;
use std::ffi::OsString;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bus_broker::{Broker, ClientHello};
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
    let listener = Listener::bind(&socket)?;
    eprintln!("busd: listening on {}", listener.path().display());
    loop {
        let peer = listener.accept()?;
        let broker = Arc::clone(&broker);
        std::thread::spawn(move || {
            if let Err(error) = serve_peer(peer, broker) {
                eprintln!("busd: protocol peer failed: {error}");
            }
        });
    }
}

#[cfg(feature = "unix")]
fn serve_peer(peer: Connection, broker: Arc<Mutex<Broker<AllowAll>>>) -> io::Result<()> {
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

    let result = dispatch_session(&peer, session.id, &broker, limits);
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
    limits: FrameLimits,
) -> io::Result<()> {
    while let Some(packet) = peer.receive_packet(limits.maximum_frame_size)? {
        let frame = match Frame::decode_with_limits(&packet, limits) {
            Ok(frame) => frame,
            Err(error) => return reject(peer, codec_error_code(&error), &error.to_string()),
        };
        let result = match frame {
            Frame::Claim { namespace, headers } if headers.is_empty() => {
                broker.lock().map_err(lock_error)?.claim(peer_id, namespace)
            }
            Frame::Claim { .. } => {
                return reject(
                    peer,
                    ProtocolErrorCode::InvalidState,
                    "claim metadata is not supported in this phase",
                );
            }
            Frame::Subscribe { channel, filters } if filters.is_empty() => broker
                .lock()
                .map_err(lock_error)?
                .subscribe(peer_id, channel),
            Frame::Subscribe { .. } => {
                return reject(
                    peer,
                    ProtocolErrorCode::InvalidState,
                    "subscription filters are not supported in this phase",
                );
            }
            Frame::Unsubscribe { channel } => broker
                .lock()
                .map_err(lock_error)?
                .unsubscribe(peer_id, &channel),
            Frame::Hello { .. } => {
                return reject(
                    peer,
                    ProtocolErrorCode::InvalidState,
                    "HELLO was already received",
                );
            }
            Frame::Welcome { .. } | Frame::Message { .. } | Frame::ProtocolError { .. } => {
                return reject(
                    peer,
                    ProtocolErrorCode::InvalidState,
                    "frame is not valid for a client session",
                );
            }
        };
        if let Err(error) = result {
            peer.send_packet(
                &Frame::ProtocolError {
                    code: ProtocolErrorCode::InvalidState,
                    message: error.to_string(),
                }
                .encode_with_limits(limits)
                .map_err(codec_io_error)?,
            )?;
        }
    }
    Ok(())
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
        let server = thread::spawn(move || serve_peer(listener.accept().unwrap(), server_broker));
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
        let server = thread::spawn(move || serve_peer(listener.accept().unwrap(), broker));
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
