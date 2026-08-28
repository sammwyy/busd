#![cfg(target_os = "linux")]
#![warn(missing_docs)]
//! Linux native transport primitives for BUS/1.

use std::ffi::{c_char, c_void};
use std::io;
use std::mem::{MaybeUninit, offset_of};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

const AF_UNIX: i32 = 1;
const SOCK_SEQPACKET: i32 = 5;
const SOCK_CLOEXEC: i32 = 0o2_000_000;
const MSG_NOSIGNAL: i32 = 0x4000;
const MSG_TRUNC: i32 = 0x20;
const SOL_SOCKET: i32 = 1;
const SO_PEERCRED: i32 = 17;
const SUN_PATH_CAPACITY: usize = 108;

#[repr(C)]
struct SockAddrUn {
    family: u16,
    path: [c_char; SUN_PATH_CAPACITY],
}

#[repr(C)]
struct UCred {
    pid: i32,
    uid: u32,
    gid: u32,
}

unsafe extern "C" {
    fn socket(domain: i32, socket_type: i32, protocol: i32) -> i32;
    fn bind(socket: i32, address: *const SockAddrUn, address_length: u32) -> i32;
    fn connect(socket: i32, address: *const SockAddrUn, address_length: u32) -> i32;
    fn listen(socket: i32, backlog: i32) -> i32;
    fn accept4(socket: i32, address: *mut (), address_length: *mut u32, flags: i32) -> i32;
    fn send(socket: i32, buffer: *const u8, length: usize, flags: i32) -> isize;
    fn recv(socket: i32, buffer: *mut u8, length: usize, flags: i32) -> isize;
    fn getsockopt(
        socket: i32,
        level: i32,
        option_name: i32,
        option_value: *mut c_void,
        option_length: *mut u32,
    ) -> i32;
}

#[cfg(test)]
unsafe extern "C" {
    fn getuid() -> u32;
    fn getgid() -> u32;
}

/// Kernel-authenticated identity of a Unix-domain socket peer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerCredentials {
    /// The peer process identifier.
    pub pid: u32,
    /// The peer user identifier.
    pub uid: u32,
    /// The peer primary group identifier.
    pub gid: u32,
}

/// A connected native BUS transport.
pub struct Connection {
    fd: OwnedFd,
}

impl Connection {
    /// Connects to an `AF_UNIX` `SOCK_SEQPACKET` BUS socket.
    pub fn connect(path: impl AsRef<Path>) -> io::Result<Self> {
        let (address, length) = socket_address(path.as_ref())?;
        let fd = unsafe { socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        if unsafe { connect(fd.as_raw_fd(), &address, length) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { fd })
    }

    /// Sends one complete native packet.
    pub fn send_packet(&self, packet: &[u8]) -> io::Result<()> {
        if packet.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "native packets must not be empty",
            ));
        }
        let written = unsafe {
            send(
                self.fd.as_raw_fd(),
                packet.as_ptr(),
                packet.len(),
                MSG_NOSIGNAL,
            )
        };
        if written < 0 {
            return Err(io::Error::last_os_error());
        }
        if written as usize != packet.len() {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "native packet was not sent completely",
            ));
        }
        Ok(())
    }

    /// Receives one packet, up to `maximum_size` bytes.
    ///
    /// Returns `None` when the peer disconnects.
    pub fn receive_packet(&self, maximum_size: usize) -> io::Result<Option<Vec<u8>>> {
        if maximum_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "maximum packet size must be non-zero",
            ));
        }
        let mut packet = vec![0; maximum_size];
        let received = unsafe {
            recv(
                self.fd.as_raw_fd(),
                packet.as_mut_ptr(),
                packet.len(),
                MSG_TRUNC,
            )
        };
        if received < 0 {
            return Err(io::Error::last_os_error());
        }
        if received == 0 {
            return Ok(None);
        }
        if received as usize > maximum_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "native packet exceeds the configured maximum size",
            ));
        }
        packet.truncate(received as usize);
        Ok(Some(packet))
    }

    /// Returns credentials authenticated by the Linux kernel for this peer.
    pub fn peer_credentials(&self) -> io::Result<PeerCredentials> {
        let mut credentials = MaybeUninit::<UCred>::uninit();
        let mut length = std::mem::size_of::<UCred>() as u32;
        let result = unsafe {
            getsockopt(
                self.fd.as_raw_fd(),
                SOL_SOCKET,
                SO_PEERCRED,
                credentials.as_mut_ptr().cast(),
                &mut length,
            )
        };
        if result < 0 {
            return Err(io::Error::last_os_error());
        }
        if length as usize != std::mem::size_of::<UCred>() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "kernel returned an invalid peer credential record",
            ));
        }
        let credentials = unsafe { credentials.assume_init() };
        let pid = u32::try_from(credentials.pid).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "kernel returned a negative peer process identifier",
            )
        })?;
        Ok(PeerCredentials {
            pid,
            uid: credentials.uid,
            gid: credentials.gid,
        })
    }
}

impl AsRawFd for Connection {
    fn as_raw_fd(&self) -> i32 {
        self.fd.as_raw_fd()
    }
}

/// A bound native BUS listener.
pub struct Listener {
    fd: OwnedFd,
    path: PathBuf,
}

impl Listener {
    /// Binds an `AF_UNIX` `SOCK_SEQPACKET` socket at `path`.
    ///
    /// The path must not already exist. This avoids unlinking an unknown socket.
    pub fn bind(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        if path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "socket path already exists",
            ));
        }
        let (address, length) = socket_address(path)?;

        let fd = unsafe { socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };

        if unsafe { bind(fd.as_raw_fd(), &address, length) } < 0 {
            return Err(io::Error::last_os_error());
        }
        if unsafe { listen(fd.as_raw_fd(), 128) } < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self {
            fd,
            path: path.into(),
        })
    }

    /// Accepts one native BUS peer.
    pub fn accept(&self) -> io::Result<Connection> {
        let fd = unsafe {
            accept4(
                self.fd.as_raw_fd(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                SOCK_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Connection {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        })
    }

    /// Returns the filesystem path at which this listener is bound.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn socket_address(path: &Path) -> io::Result<(SockAddrUn, u32)> {
    let bytes = path.as_os_str().as_bytes();
    if bytes.is_empty() || bytes.len() >= SUN_PATH_CAPACITY || bytes.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "socket path must be non-empty, NUL-free, and shorter than 108 bytes",
        ));
    }

    let mut address = unsafe { MaybeUninit::<SockAddrUn>::zeroed().assume_init() };
    address.family = AF_UNIX as u16;
    for (destination, source) in address.path.iter_mut().zip(bytes.iter().copied()) {
        *destination = source as c_char;
    }
    let length = (offset_of!(SockAddrUn, path) + bytes.len() + 1) as u32;
    Ok((address, length))
}

impl AsRawFd for Listener {
    fn as_raw_fd(&self) -> i32 {
        self.fd.as_raw_fd()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process;
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn connection_reaches_seqpacket_listener() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("busd-{}-{nonce}.sock", process::id()));
        let listener = Listener::bind(&path).unwrap();
        let accepted = thread::spawn(move || {
            let peer = listener.accept().unwrap();
            let credentials = peer.peer_credentials().unwrap();
            (credentials, peer.receive_packet(64).unwrap().unwrap())
        });

        let connection = Connection::connect(&path).unwrap();
        connection.send_packet(&[0xde, 0xad, 0xbe, 0xef]).unwrap();
        let (credentials, packet) = accepted.join().unwrap();

        assert!(connection.as_raw_fd() >= 0);
        assert_eq!(credentials.pid, process::id());
        assert_eq!(credentials.uid, unsafe { getuid() });
        assert_eq!(credentials.gid, unsafe { getgid() });
        assert_eq!(packet, [0xde, 0xad, 0xbe, 0xef]);
        std::fs::remove_file(path).unwrap();
    }
}
