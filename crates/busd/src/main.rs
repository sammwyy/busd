#![cfg_attr(not(feature = "unix"), allow(dead_code))]
#![cfg_attr(not(feature = "unix"), allow(unused_imports))]

use std::collections::BTreeMap;
use std::env;
use std::ffi::OsString;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use busd_broker::{Broker, ClientHello, DeliveryEvent, Error as BrokerError, Limits};
use busd_policy::{AllowAll, ConfigPolicy, Credentials, Policy, SafeDefaults};
use busd_protocol::{CodecError, Frame, FrameLimits, HeaderValue, ProtocolErrorCode};

#[cfg(feature = "unix")]
use busd_transport_unix::{Connection, Listener};

const DEFAULT_SOCKET: &str = "/run/busd/busd.sock";

struct DaemonCommand {
    socket: PathBuf,
    policy: PolicySource,
    limits: Limits,
    maximum_frame_size: usize,
}

enum Command {
    Daemon(DaemonCommand),
    Health { socket: PathBuf },
}

enum PolicySource {
    SafeDefaults,
    File(PathBuf),
    UnsafeAllowAll,
}

enum DaemonPolicy {
    SafeDefaults(SafeDefaults),
    File(ConfigPolicy),
    UnsafeAllowAll(AllowAll),
}

impl Policy for DaemonPolicy {
    fn permits(&self, request: &busd_policy::Request<'_>) -> bool {
        match self {
            Self::SafeDefaults(policy) => policy.permits(request),
            Self::File(policy) => policy.permits(request),
            Self::UnsafeAllowAll(policy) => policy.permits(request),
        }
    }
}

#[cfg(feature = "unix")]
fn main() -> io::Result<()> {
    let command = parse_command(env::args_os().skip(1))?;
    match command {
        Command::Daemon(command) => run_daemon(command),
        Command::Health { socket } => run_health(socket),
    }
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
        Some("daemon") => parse_daemon_command(arguments),
        Some("health") => parse_health_command(arguments),
        _ => Err(usage()),
    }
}

fn parse_daemon_command(mut arguments: impl Iterator<Item = OsString>) -> io::Result<Command> {
    let mut socket = PathBuf::from(DEFAULT_SOCKET);
    let mut policy = PolicySource::SafeDefaults;
    let mut limits = Limits::default();
    let mut maximum_frame_size = FrameLimits::default().maximum_frame_size;
    while let Some(argument) = arguments.next() {
        match argument.to_str() {
            Some("--socket") => socket = arguments.next().ok_or_else(usage)?.into(),
            Some("--policy") => {
                if matches!(policy, PolicySource::UnsafeAllowAll) {
                    return Err(usage());
                }
                policy = PolicySource::File(arguments.next().ok_or_else(usage)?.into());
            }
            Some("--unsafe-allow-all") => {
                if matches!(policy, PolicySource::File(_)) {
                    return Err(usage());
                }
                policy = PolicySource::UnsafeAllowAll;
            }
            Some("--maximum-frame-size") => {
                maximum_frame_size = parse_limit(arguments.next().ok_or_else(usage)?)?;
            }
            Some("--maximum-queued-bytes") => {
                limits.maximum_queued_bytes = parse_limit(arguments.next().ok_or_else(usage)?)?;
            }
            Some("--maximum-queued-messages") => {
                limits.maximum_queued_messages = parse_limit(arguments.next().ok_or_else(usage)?)?;
            }
            Some("--maximum-in-flight-requests") => {
                limits.maximum_in_flight_requests =
                    parse_limit(arguments.next().ok_or_else(usage)?)?;
            }
            Some("--maximum-subscriptions") => {
                limits.maximum_subscriptions = parse_limit(arguments.next().ok_or_else(usage)?)?;
            }
            Some("--maximum-namespace-claims") => {
                limits.maximum_namespace_claims = parse_limit(arguments.next().ok_or_else(usage)?)?;
            }
            _ => return Err(usage()),
        }
    }
    Ok(Command::Daemon(DaemonCommand {
        socket,
        policy,
        limits,
        maximum_frame_size,
    }))
}

fn parse_health_command(mut arguments: impl Iterator<Item = OsString>) -> io::Result<Command> {
    let mut socket = PathBuf::from(DEFAULT_SOCKET);
    while let Some(argument) = arguments.next() {
        if argument == "--socket" {
            socket = arguments.next().ok_or_else(usage)?.into();
        } else {
            return Err(usage());
        }
    }
    Ok(Command::Health { socket })
}

fn parse_limit(value: OsString) -> io::Result<usize> {
    let value = value.into_string().map_err(|_| usage())?;
    let value = value.parse().map_err(|_| usage())?;
    if value == 0 {
        return Err(usage());
    }
    Ok(value)
}

fn usage() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "usage: busd daemon [--socket PATH] [--policy PATH | --unsafe-allow-all] [--maximum-frame-size BYTES] [--maximum-queued-bytes BYTES] [--maximum-queued-messages COUNT] [--maximum-in-flight-requests COUNT] [--maximum-subscriptions COUNT] [--maximum-namespace-claims COUNT] | busd health [--socket PATH]",
    )
}

#[cfg(feature = "unix")]
fn run_daemon(command: DaemonCommand) -> io::Result<()> {
    let policy = match command.policy {
        PolicySource::SafeDefaults => DaemonPolicy::SafeDefaults(SafeDefaults),
        PolicySource::File(path) => DaemonPolicy::File(
            ConfigPolicy::parse(&std::fs::read_to_string(path)?).map_err(io::Error::other)?,
        ),
        PolicySource::UnsafeAllowAll => DaemonPolicy::UnsafeAllowAll(AllowAll),
    };
    let limits = FrameLimits {
        maximum_frame_size: command.maximum_frame_size,
        ..FrameLimits::default()
    };
    let broker = Arc::new(Mutex::new(Broker::with_limits(policy, command.limits)));
    let sessions = Arc::new(Mutex::new(BTreeMap::new()));
    let scheduler_broker = Arc::clone(&broker);
    let scheduler_sessions = Arc::clone(&sessions);
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(std::time::Duration::from_millis(5));
            let (events, metrics) = match scheduler_broker.lock() {
                Ok(mut broker) => {
                    let events = broker.tick(now_ms());
                    (events, broker.metrics())
                }
                Err(_) => return,
            };
            let _ = dispatch_delivery_events(&scheduler_sessions, events, limits);
            if now_ms() % 30_000 < 5 {
                eprintln!(
                    "busd event=metrics peers={} namespaces={} subscriptions={} messages_routed={} bytes_routed={} timeouts={} retries={} dropped_signals={}",
                    metrics.connected_peers,
                    metrics.namespaces,
                    metrics.subscriptions,
                    metrics.totals.messages_routed,
                    metrics.totals.bytes_routed,
                    metrics.totals.timeouts,
                    metrics.totals.retries,
                    metrics.totals.dropped_signals,
                );
            }
        }
    });
    let listener = Listener::bind(&command.socket)?;
    eprintln!("busd: listening on {}", listener.path().display());
    loop {
        let peer = listener.accept()?;
        let broker = Arc::clone(&broker);
        let sessions = Arc::clone(&sessions);
        std::thread::spawn(move || {
            if let Err(error) = serve_peer(peer, broker, sessions, limits) {
                eprintln!("busd: protocol peer failed: {error}");
            }
        });
    }
}

#[cfg(feature = "unix")]
fn run_health(socket: PathBuf) -> io::Result<()> {
    let connection = Connection::connect(&socket)?;
    drop(connection);
    println!("busd health=ok socket={}", socket.display());
    Ok(())
}

#[cfg(feature = "unix")]
fn serve_peer<P: Policy>(
    peer: Connection,
    broker: Arc<Mutex<Broker<P>>>,
    sessions: Arc<Mutex<BTreeMap<busd_protocol::PeerId, Connection>>>,
    limits: FrameLimits,
) -> io::Result<()> {
    let credentials = credentials_from_peer(peer.peer_credentials()?);
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
    let session = match broker
        .lock()
        .map_err(lock_error)?
        .connect_session(credentials.clone(), hello)
    {
        Ok(session) => session,
        Err(error) => {
            log_connection_error(&credentials, &error);
            return reject(&peer, ProtocolErrorCode::InvalidState, &error.to_string());
        }
    };
    peer.send_packet(
        &Frame::Welcome {
            peer_id: session.id,
            capabilities: session.capabilities,
        }
        .encode_with_limits(limits)
        .map_err(codec_io_error)?,
    )?;
    eprintln!(
        "busd event=peer_connected peer={} pid={} uid={} gid={}",
        session.id, credentials.pid, credentials.uid, credentials.gid
    );
    sessions
        .lock()
        .map_err(lock_error)?
        .insert(session.id, peer.try_clone()?);

    let result = dispatch_session(&peer, session.id, &broker, &sessions, limits);
    sessions.lock().map_err(lock_error)?.remove(&session.id);
    let disconnect = broker
        .lock()
        .map_err(lock_error)?
        .disconnect_with_events(session.id);
    let disconnect_events = match disconnect {
        Ok(events) => events,
        Err(error) => {
            return Err(io::Error::other(error));
        }
    };
    if let Err(error) = dispatch_delivery_events(&sessions, disconnect_events, limits) {
        return Err(io::Error::other(error));
    }
    result
}

#[cfg(feature = "unix")]
fn dispatch_session<P: Policy>(
    peer: &Connection,
    peer_id: busd_protocol::PeerId,
    broker: &Arc<Mutex<Broker<P>>>,
    sessions: &Arc<Mutex<BTreeMap<busd_protocol::PeerId, Connection>>>,
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
                send_control_result(
                    peer,
                    peer_id,
                    result,
                    busd_protocol::ControlOperation::Claim,
                    limits,
                )?;
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
                    peer_id,
                    result,
                    busd_protocol::ControlOperation::Subscribe,
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
                    peer_id,
                    result,
                    busd_protocol::ControlOperation::Unsubscribe,
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
            Frame::Message { .. } => {
                let events = match broker.lock().map_err(lock_error)?.begin_delivery(
                    peer_id,
                    frame,
                    now_ms(),
                ) {
                    Ok(events) => events,
                    Err(error) => {
                        log_operation_error(peer_id, &error);
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
                dispatch_delivery_events(sessions, events, limits)?;
                continue;
            }
            Frame::Acknowledge { message_id, policy } => {
                let events = match broker
                    .lock()
                    .map_err(lock_error)?
                    .acknowledge(peer_id, message_id, policy)
                {
                    Ok(events) => events,
                    Err(error) => {
                        log_operation_error(peer_id, &error);
                        peer.send_packet(
                            &Frame::ProtocolError {
                                code: ProtocolErrorCode::InvalidState,
                                message: error.to_string(),
                            }
                            .encode_with_limits(limits)
                            .map_err(codec_io_error)?,
                        )?;
                        continue;
                    }
                };
                dispatch_delivery_events(sessions, events, limits)?;
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
            | Frame::NamespaceResolved { .. }
            | Frame::DeliveryResult { .. } => {
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
fn dispatch_delivery_events(
    sessions: &Arc<Mutex<BTreeMap<busd_protocol::PeerId, Connection>>>,
    events: Vec<DeliveryEvent>,
    limits: FrameLimits,
) -> io::Result<()> {
    for event in events {
        let (recipients, frame) = match event {
            DeliveryEvent::Deliver {
                sender,
                recipients,
                mut message,
            } => {
                if let Frame::Message { headers, .. } = &mut message {
                    headers.insert("broker.sender".into(), HeaderValue::Unsigned(sender.get()));
                }
                (recipients, message)
            }
            DeliveryEvent::Result {
                sender,
                message_id,
                outcome,
            } => (
                vec![sender],
                Frame::DeliveryResult {
                    message_id,
                    outcome,
                },
            ),
        };
        let packet = frame.encode_with_limits(limits).map_err(codec_io_error)?;
        let outputs: Vec<_> = sessions
            .lock()
            .map_err(lock_error)?
            .iter()
            .filter(|(id, _)| recipients.contains(id))
            .map(|(_, connection)| connection.try_clone())
            .collect::<io::Result<_>>()?;
        for output in outputs {
            if let Err(error) = output.send_packet(&packet) {
                if !matches!(
                    error.kind(),
                    io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
                ) {
                    return Err(error);
                }
            }
        }
    }
    Ok(())
}

#[cfg(feature = "unix")]
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(feature = "unix")]
fn credentials_from_peer(peer: busd_transport_unix::PeerCredentials) -> Credentials {
    let root = PathBuf::from("/proc").join(peer.pid.to_string());
    let executable = std::fs::read_link(root.join("exe"))
        .ok()
        .map(|path| path.to_string_lossy().into_owned());
    let security_label = std::fs::read_to_string(root.join("attr/current"))
        .ok()
        .map(|value| value.trim().into())
        .filter(|value: &String| !value.is_empty());
    let cgroup = std::fs::read_to_string(root.join("cgroup"))
        .ok()
        .and_then(|value| {
            value
                .lines()
                .next()
                .and_then(|line| line.splitn(3, ':').nth(2))
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
        });
    Credentials {
        pid: peer.pid,
        uid: peer.uid,
        gid: peer.gid,
        executable,
        security_label,
        cgroup,
    }
}

#[cfg(feature = "unix")]
fn send_control_result(
    peer: &Connection,
    peer_id: busd_protocol::PeerId,
    result: Result<(), busd_broker::Error>,
    operation: busd_protocol::ControlOperation,
    limits: FrameLimits,
) -> io::Result<()> {
    let frame = match result {
        Ok(()) => Frame::ControlResult { operation },
        Err(error) => {
            log_operation_error(peer_id, &error);
            Frame::ProtocolError {
                code: broker_error_code(&error),
                message: error.to_string(),
            }
        }
    };
    peer.send_packet(&frame.encode_with_limits(limits).map_err(codec_io_error)?)
}

#[cfg(feature = "unix")]
fn broker_error_code(error: &BrokerError) -> ProtocolErrorCode {
    match error {
        BrokerError::NoRecipient => ProtocolErrorCode::NoRecipient,
        BrokerError::LimitExceeded(_) => ProtocolErrorCode::LimitExceeded,
        _ => ProtocolErrorCode::InvalidState,
    }
}

#[cfg(feature = "unix")]
fn log_connection_error(credentials: &Credentials, error: &BrokerError) {
    eprintln!(
        "busd event=connection_denied pid={} uid={} gid={} reason={}",
        credentials.pid, credentials.uid, credentials.gid, error
    );
}

#[cfg(feature = "unix")]
fn log_operation_error(peer: busd_protocol::PeerId, error: &BrokerError) {
    match error {
        BrokerError::Denied(action) => eprintln!(
            "busd event=policy_denied peer={} action={}",
            peer,
            action.name()
        ),
        BrokerError::LimitExceeded(limit) => {
            eprintln!("busd event=limit_exceeded peer={} limit={limit:?}", peer)
        }
        _ => eprintln!("busd event=operation_failed peer={} reason={}", peer, error),
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
    use busd_protocol::{Capabilities, Channel, Namespace};
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
        let Command::Daemon(command) = parse_command(["daemon".into()].into_iter()).unwrap() else {
            panic!("expected daemon command");
        };
        assert_eq!(command.socket, PathBuf::from(DEFAULT_SOCKET));
    }

    #[test]
    fn daemon_accepts_a_custom_socket() {
        let Command::Daemon(command) = parse_command(
            ["daemon", "--socket", "/tmp/busd.sock"]
                .into_iter()
                .map(OsString::from),
        )
        .unwrap() else {
            panic!("expected daemon command");
        };
        assert_eq!(command.socket, PathBuf::from("/tmp/busd.sock"));
    }

    #[test]
    fn health_command_accepts_a_custom_socket() {
        let Command::Health { socket } = parse_command(
            ["health", "--socket", "/tmp/busd.sock"]
                .into_iter()
                .map(OsString::from),
        )
        .unwrap() else {
            panic!("expected health command");
        };
        assert_eq!(socket, PathBuf::from("/tmp/busd.sock"));
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
                FrameLimits::default(),
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
                FrameLimits::default(),
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
