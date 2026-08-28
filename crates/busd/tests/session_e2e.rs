#![cfg(target_os = "linux")]

use std::path::PathBuf;
use std::process::{self, Command};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bus_client::{
    AckPolicy, Bus, Channel, ClientId, ConnectOptions, Destination, Frame, HeaderFilter,
    HeaderValue, MessageId, MessageKind, Namespace, Status,
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

fn message(destination: Destination, payload: &[u8]) -> Frame {
    Frame::Message {
        kind: MessageKind::Signal,
        ack_policy: AckPolicy::None,
        destination,
        message_id: MessageId::new([7; 16]),
        correlation_id: MessageId::absent(),
        status: Status::Success,
        headers: [("kind".into(), HeaderValue::Text("match".into()))].into(),
        payload: payload.to_vec(),
    }
}
