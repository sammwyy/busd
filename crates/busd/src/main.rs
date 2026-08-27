#![cfg_attr(not(feature = "unix"), allow(dead_code))]

use std::env;
use std::ffi::OsString;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

use bus_broker::Broker;
use bus_client::{Bus, ConnectOptions};
use bus_policy::AllowAll;

#[cfg(feature = "unix")]
use bus_transport_unix::{Connection, Listener};

const DEFAULT_SOCKET: &str = "/run/busd/busd.sock";
const MAX_DEBUG_PACKET_SIZE: usize = 65_536;

enum Mode {
    Daemon,
    Client,
    Send(Vec<u8>),
}

struct Command {
    mode: Mode,
    socket: PathBuf,
}

#[cfg(feature = "unix")]
fn main() -> io::Result<()> {
    match parse_command(env::args_os().skip(1))? {
        Command {
            mode: Mode::Daemon,
            socket,
        } => run_daemon(socket),
        Command {
            mode: Mode::Client,
            socket,
        } => run_client(socket),
        Command {
            mode: Mode::Send(packet),
            socket,
        } => send_packet(socket, &packet),
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
    let mode = arguments
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or_else(usage)?;
    let mut socket = PathBuf::from(DEFAULT_SOCKET);
    let mut packet = None;

    while let Some(argument) = arguments.next() {
        match argument.to_str() {
            Some("--socket") => socket = arguments.next().ok_or_else(usage)?.into(),
            Some("--hex") => {
                if packet.is_some() {
                    return Err(usage());
                }
                let value = arguments.next().and_then(|value| value.into_string().ok());
                packet = Some(parse_hex(&value.ok_or_else(usage)?)?);
            }
            _ => return Err(usage()),
        }
    }

    let mode = match mode.as_str() {
        "daemon" if packet.is_none() => Mode::Daemon,
        "client" if packet.is_none() => Mode::Client,
        "send" => Mode::Send(packet.ok_or_else(usage)?),
        _ => return Err(usage()),
    };
    Ok(Command { mode, socket })
}

fn parse_hex(value: &str) -> io::Result<Vec<u8>> {
    if value.is_empty() || value.len() % 2 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "hex packets must contain a non-empty even number of digits",
        ));
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_digit(pair[0])?;
            let low = hex_digit(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_digit(value: u8) -> io::Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "hex packets may only contain hexadecimal digits",
        )),
    }
}

fn usage() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "usage: busd <daemon|client|send> [--socket PATH] [--hex HEX]",
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
            if let Err(error) = serve_debug_peer(peer) {
                eprintln!("busd: transport peer failed: {error}");
            }
        });
    }
}

#[cfg(feature = "unix")]
fn serve_debug_peer(peer: Connection) -> io::Result<()> {
    while let Some(packet) = peer.receive_packet(MAX_DEBUG_PACKET_SIZE)? {
        eprintln!("busd: received {} raw debug bytes", packet.len());
    }
    Ok(())
}

#[cfg(feature = "unix")]
fn run_client(socket: PathBuf) -> io::Result<()> {
    let bus = Bus::connect(ConnectOptions::new(socket))?;
    eprintln!("busd client: enter hexadecimal packets; use 'quit' to exit");
    for line in io::stdin().lock().lines() {
        let line = line?;
        if line.trim() == "quit" {
            return Ok(());
        }
        let packet = parse_hex(line.trim())?;
        bus.send_debug_packet(&packet)?;
        println!("sent {} bytes", packet.len());
        io::stdout().flush()?;
    }
    Ok(())
}

#[cfg(feature = "unix")]
fn send_packet(socket: PathBuf, packet: &[u8]) -> io::Result<()> {
    let bus = Bus::connect(ConnectOptions::new(socket))?;
    bus.send_debug_packet(packet)?;
    println!("sent {} bytes", packet.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_requires_mode() {
        assert!(parse_command(std::iter::empty()).is_err());
    }

    #[test]
    fn daemon_uses_the_system_socket_by_default() {
        let command = parse_command(["daemon".into()].into_iter()).unwrap();
        assert!(matches!(command.mode, Mode::Daemon));
        assert_eq!(command.socket, PathBuf::from(DEFAULT_SOCKET));
    }

    #[test]
    fn send_accepts_hex_with_socket_in_any_option_order() {
        let command = parse_command(
            ["send", "--hex", "DEADBEEF", "--socket", "/tmp/busd.sock"]
                .into_iter()
                .map(OsString::from),
        )
        .unwrap();
        assert_eq!(command.socket, PathBuf::from("/tmp/busd.sock"));
        assert!(matches!(command.mode, Mode::Send(packet) if packet == [0xde, 0xad, 0xbe, 0xef]));
    }

    #[test]
    fn hex_rejects_invalid_packets() {
        assert!(parse_hex("").is_err());
        assert!(parse_hex("f").is_err());
        assert!(parse_hex("fg").is_err());
    }
}
