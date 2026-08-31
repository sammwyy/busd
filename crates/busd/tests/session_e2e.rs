#![cfg(all(target_os = "linux", feature = "unix"))]

use std::path::PathBuf;
use std::process::{self, Command};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bus_client::{
    AckPolicy, AckRequirement, Bus, Channel, ClientId, ConnectOptions, DeliveryOutcome,
    Destination, Frame, HeaderFilter, HeaderValue, MessageId, MessageKind, Namespace,
    ReconnectingBus, RequestPolicy, RetryPolicy, Status,
};

struct Daemon {
    child: std::process::Child,
    socket: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.socket);
    }
}

#[test]
fn concurrent_clients_receive_distinct_peer_ids() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let socket = std::env::temp_dir().join(format!("busd-e2e-{}-{nonce}.sock", process::id()));
    let child = Command::new(env!("CARGO_BIN_EXE_busd"))
        .args(["daemon", "--socket"])
        .arg(&socket)
        .spawn()
        .unwrap();
    let _daemon = Daemon {
        child,
        socket: socket.clone(),
    };
    for _ in 0..100 {
        if socket.exists() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(socket.exists());

    let start = Arc::new(Barrier::new(3));
    let clients: Vec<_> = ["one", "two"]
        .into_iter()
        .map(|name| {
            let socket = socket.clone();
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                let bus = Bus::connect(
                    ConnectOptions::new(socket).with_client_id(ClientId::parse(name).unwrap()),
                )
                .unwrap();
                let peer_id = bus.peer_id();
                bus.disconnect().unwrap();
                peer_id
            })
        })
        .collect();
    start.wait();
    let first = clients
        .into_iter()
        .map(|client| client.join().unwrap())
        .collect::<Vec<_>>();
    assert_ne!(first[0], first[1]);
    assert_ne!(first[0].get(), 0);
    assert_ne!(first[1].get(), 0);
}

#[test]
fn namespace_and_filtered_channel_deliver_only_to_selected_peers() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let socket = std::env::temp_dir().join(format!("busd-routing-{}-{nonce}.sock", process::id()));
    let child = Command::new(env!("CARGO_BIN_EXE_busd"))
        .args(["daemon", "--socket"])
        .arg(&socket)
        .spawn()
        .unwrap();
    let _daemon = Daemon {
        child,
        socket: socket.clone(),
    };
    for _ in 0..100 {
        if socket.exists() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    let namespace = Namespace::parse("bus://service").unwrap();
    let provider = Bus::connect(
        ConnectOptions::new(&socket).with_client_id(ClientId::parse("provider").unwrap()),
    )
    .unwrap();
    provider.claim(namespace.clone()).unwrap();
    let consumer = Bus::connect(
        ConnectOptions::new(&socket).with_client_id(ClientId::parse("consumer").unwrap()),
    )
    .unwrap();
    assert_eq!(
        consumer.resolve_namespace(namespace.clone()).unwrap(),
        Some(provider.peer_id())
    );
    consumer
        .send_message(&message(Destination::Namespace(namespace), b"namespace"))
        .unwrap();
    assert!(
        matches!(provider.receive_frame().unwrap(), Some(Frame::Message { payload, .. }) if payload == b"namespace")
    );

    let channel = Channel::parse("events").unwrap();
    let subscriber = Bus::connect(ConnectOptions::new(&socket)).unwrap();
    subscriber
        .subscribe(
            channel.clone(),
            vec![HeaderFilter::Equal(
                "kind".into(),
                HeaderValue::Text("match".into()),
            )],
        )
        .unwrap();
    consumer
        .send_message(&message(Destination::Channel(channel), b"channel"))
        .unwrap();
    assert!(
        matches!(subscriber.receive_frame().unwrap(), Some(Frame::Message { payload, .. }) if payload == b"channel")
    );
    consumer.disconnect().unwrap();
    subscriber.disconnect().unwrap();
    provider.disconnect().unwrap();
}

#[test]
fn requests_acknowledgements_and_no_recipient_outcomes_work_over_native_sockets() {
    let daemon = start_daemon("reliable");
    let namespace = Namespace::parse("bus://worker").unwrap();
    let provider = Bus::connect(ConnectOptions::new(&daemon.socket)).unwrap();
    provider.claim(namespace.clone()).unwrap();
    let consumer = Bus::connect(ConnectOptions::new(&daemon.socket)).unwrap();

    let request_namespace = namespace.clone();
    let provider_thread = thread::spawn(move || {
        let Some(Frame::Message {
            kind: MessageKind::Request,
            message_id,
            headers,
            ..
        }) = provider.receive_frame().unwrap()
        else {
            panic!("provider did not receive request");
        };
        let Some(HeaderValue::Unsigned(sender)) = headers.get("broker.sender") else {
            panic!("request did not contain broker-authenticated sender");
        };
        provider
            .send_message(&Frame::Message {
                kind: MessageKind::Response,
                ack_policy: AckPolicy::None,
                ack_requirement: AckRequirement::None,
                request_policy: RequestPolicy::Exact,
                deadline_ms: 0,
                retry: RetryPolicy::None,
                destination: Destination::Peer(bus_client::PeerId::new(*sender)),
                message_id: MessageId::new([22; 16]),
                correlation_id: message_id,
                status: Status::Success,
                headers: Default::default(),
                payload: b"done".to_vec(),
            })
            .unwrap();
        provider.disconnect().unwrap();
    });
    let response = consumer
        .request(&request_message(
            Destination::Namespace(request_namespace),
            MessageId::new([21; 16]),
        ))
        .unwrap();
    assert!(matches!(response, Frame::Message { payload, .. } if payload == b"done"));
    provider_thread.join().unwrap();

    let receiver = Bus::connect(ConnectOptions::new(&daemon.socket)).unwrap();
    let sender = Bus::connect(ConnectOptions::new(&daemon.socket)).unwrap();
    let receiver_id = receiver.peer_id();
    let signal_id = MessageId::new([24; 16]);
    let receiver_thread = thread::spawn(move || {
        assert!(matches!(
            receiver.receive_frame().unwrap(),
            Some(Frame::Message { message_id, .. }) if message_id == signal_id
        ));
        receiver
            .acknowledge(signal_id, AckPolicy::Processed)
            .unwrap();
        receiver.disconnect().unwrap();
    });
    sender
        .send_message(&Frame::Message {
            kind: MessageKind::Signal,
            ack_policy: AckPolicy::Processed,
            ack_requirement: AckRequirement::All,
            request_policy: RequestPolicy::Exact,
            deadline_ms: 100,
            retry: RetryPolicy::None,
            destination: Destination::Peer(receiver_id),
            message_id: signal_id,
            correlation_id: MessageId::absent(),
            status: Status::Success,
            headers: Default::default(),
            payload: Vec::new(),
        })
        .unwrap();
    assert!(matches!(
        sender.receive_frame().unwrap(),
        Some(Frame::DeliveryResult { message_id, outcome: DeliveryOutcome::Processed }) if message_id == signal_id
    ));
    receiver_thread.join().unwrap();
    sender.disconnect().unwrap();

    assert!(matches!(
        consumer.request(&request_message(
            Destination::Namespace(Namespace::parse("bus://missing").unwrap()),
            MessageId::new([23; 16]),
        )),
        Err(bus_client::Error::Delivery(DeliveryOutcome::NoRecipient))
    ));
    consumer.disconnect().unwrap();
}

#[test]
fn reconnect_restores_claims_and_subscriptions_after_daemon_restart() {
    let first_daemon = start_daemon("reconnect");
    let namespace = Namespace::parse("bus://reconnect").unwrap();
    let channel = Channel::parse("reconnect-events").unwrap();
    let mut client = ReconnectingBus::connect(ConnectOptions::new(&first_daemon.socket)).unwrap();
    let first_peer = client.peer_id();
    client.claim(namespace.clone(), Default::default()).unwrap();
    client.subscribe(channel.clone(), Vec::new()).unwrap();

    let mut first_daemon = first_daemon;
    first_daemon.child.kill().unwrap();
    first_daemon.child.wait().unwrap();
    std::fs::remove_file(&first_daemon.socket).unwrap();
    let second_daemon = start_daemon_at(first_daemon.socket.clone());
    let observer = Bus::connect(ConnectOptions::new(&second_daemon.socket)).unwrap();

    let second_peer = client.reconnect().unwrap();
    assert_ne!(first_peer, second_peer);
    assert_eq!(
        observer.resolve_namespace(namespace).unwrap(),
        Some(second_peer)
    );
    observer.disconnect().unwrap();
    client.disconnect().unwrap();
    drop(second_daemon);
}

#[test]
fn configured_policy_denies_broadcast_and_health_check_reaches_daemon() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let socket = std::env::temp_dir().join(format!("busd-policy-{nonce}.sock"));
    let policy = std::env::temp_dir().join(format!("busd-policy-{nonce}.conf"));
    std::fs::write(
        &policy,
        "version = 1\ndefault = allow\n[[rule]]\neffect = deny\nactions = broadcast\n",
    )
    .unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_busd"))
        .args(["daemon", "--socket"])
        .arg(&socket)
        .args(["--policy"])
        .arg(&policy)
        .spawn()
        .unwrap();
    let daemon = Daemon {
        child,
        socket: socket.clone(),
    };
    for _ in 0..100 {
        if socket.exists() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(socket.exists());
    let health = Command::new(env!("CARGO_BIN_EXE_busd"))
        .args(["health", "--socket"])
        .arg(&socket)
        .output()
        .unwrap();
    assert!(health.status.success());
    assert!(
        String::from_utf8(health.stdout)
            .unwrap()
            .contains("health=ok")
    );

    let sender = Bus::connect(ConnectOptions::new(&socket)).unwrap();
    sender
        .send_message(&message(Destination::Broadcast, b"denied"))
        .unwrap();
    assert!(matches!(
        sender.receive_frame().unwrap(),
        Some(Frame::ProtocolError { .. })
    ));
    sender.disconnect().unwrap();
    drop(daemon);
    std::fs::remove_file(policy).unwrap();
}

fn message(destination: Destination, payload: &[u8]) -> Frame {
    Frame::Message {
        kind: MessageKind::Signal,
        ack_policy: AckPolicy::None,
        ack_requirement: bus_client::AckRequirement::None,
        request_policy: bus_client::RequestPolicy::Exact,
        deadline_ms: 0,
        retry: bus_client::RetryPolicy::None,
        destination,
        message_id: MessageId::new([7; 16]),
        correlation_id: MessageId::absent(),
        status: Status::Success,
        headers: [("kind".into(), HeaderValue::Text("match".into()))].into(),
        payload: payload.to_vec(),
    }
}

fn request_message(destination: Destination, message_id: MessageId) -> Frame {
    Frame::Message {
        kind: MessageKind::Request,
        ack_policy: AckPolicy::None,
        ack_requirement: AckRequirement::None,
        request_policy: RequestPolicy::Exact,
        deadline_ms: 100,
        retry: RetryPolicy::None,
        destination,
        message_id,
        correlation_id: MessageId::absent(),
        status: Status::Success,
        headers: Default::default(),
        payload: Vec::new(),
    }
}

fn start_daemon(label: &str) -> Daemon {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let socket = std::env::temp_dir().join(format!("busd-{label}-{}-{nonce}.sock", process::id()));
    start_daemon_at(socket)
}

fn start_daemon_at(socket: PathBuf) -> Daemon {
    let child = Command::new(env!("CARGO_BIN_EXE_busd"))
        .args(["daemon", "--socket"])
        .arg(&socket)
        .spawn()
        .unwrap();
    for _ in 0..100 {
        if socket.exists() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(socket.exists());
    Daemon { child, socket }
}
