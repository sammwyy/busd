#![cfg_attr(not(feature = "unix"), allow(dead_code))]

use std::env;
use std::ffi::OsString;
use std::io;
use std::path::PathBuf;

use bus_broker::Broker;
use bus_policy::AllowAll;
use bus_protocol::{Frame, FrameLimits};

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
    let _broker = Broker::new(AllowAll);
    let listener = Listener::bind(&socket)?;
    eprintln!("busd: listening on {}", listener.path().display());
    loop {
        let peer = listener.accept()?;
        std::thread::spawn(move || {
            if let Err(error) = serve_peer(peer) {
                eprintln!("busd: protocol peer failed: {error}");
            }
        });
    }
}

#[cfg(feature = "unix")]
fn serve_peer(peer: Connection) -> io::Result<()> {
    let limits = FrameLimits::default();
    while let Some(packet) = peer.receive_packet(limits.maximum_frame_size)? {
        Frame::decode_with_limits(&packet, limits).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("rejected non-BUS/1 frame: {error}"),
            )
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
