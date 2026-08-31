#![cfg(target_os = "linux")]
#![warn(missing_docs)]
//! Linux native transport primitives for BUS/1.

use std::ffi::{c_char, c_void};
use std::io;
use std::mem::{MaybeUninit, offset_of};
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

const AF_UNIX: i32 = 1;
const SOCK_SEQPACKET: i32 = 5;
const SOCK_CLOEXEC: i32 = 0o2_000_000;
const MSG_NOSIGNAL: i32 = 0x4000;
const MSG_TRUNC: i32 = 0x20;
const MSG_CMSG_CLOEXEC: i32 = 0x4000_0000;
const MSG_CTRUNC: i32 = 0x8;
const SOL_SOCKET: i32 = 1;
const SO_PEERCRED: i32 = 17;
const SHUT_RDWR: i32 = 2;
const SUN_PATH_CAPACITY: usize = 108;
/// Maximum number of descriptors accepted in one native packet.
pub const MAX_FILE_DESCRIPTORS: usize = 16;

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

#[repr(C)]
struct Iovec {
    base: *mut u8,
    length: usize,
}

#[repr(C)]
struct MessageHeader {
    name: *mut (),
    name_length: u32,
    iov: *mut Iovec,
    iov_length: usize,
    control: *mut c_void,
    control_length: usize,
    flags: i32,
}

#[repr(C)]
struct ControlMessageHeader {
    length: usize,
    level: i32,
    message_type: i32,
}

unsafe extern "C" {
    fn socket(domain: i32, socket_type: i32, protocol: i32) -> i32;
    fn bind(socket: i32, address: *const SockAddrUn, address_length: u32) -> i32;
    fn connect(socket: i32, address: *const SockAddrUn, address_length: u32) -> i32;
    fn listen(socket: i32, backlog: i32) -> i32;
    fn accept4(socket: i32, address: *mut (), address_length: *mut u32, flags: i32) -> i32;
    fn send(socket: i32, buffer: *const u8, length: usize, flags: i32) -> isize;
    fn sendmsg(socket: i32, message: *const MessageHeader, flags: i32) -> isize;
    fn recvmsg(socket: i32, message: *mut MessageHeader, flags: i32) -> isize;
    fn getsockopt(
        socket: i32,
        level: i32,
        option_name: i32,
        option_value: *mut c_void,
        option_length: *mut u32,
    ) -> i32;
    fn shutdown(socket: i32, how: i32) -> i32;
    fn dup(oldfd: i32) -> i32;
}

const SCM_RIGHTS: i32 = 1;
const SOL_SOCKET_CMSG: i32 = 1;

const fn cmsg_align(length: usize) -> usize {
    (length + std::mem::align_of::<usize>() - 1) & !(std::mem::align_of::<usize>() - 1)
}

const fn cmsg_space(length: usize) -> usize {
    cmsg_align(std::mem::size_of::<ControlMessageHeader>()) + cmsg_align(length)
}

const fn cmsg_len(length: usize) -> usize {
    std::mem::size_of::<ControlMessageHeader>() + length
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
        check_packet_write(written, packet.len())
    }

    /// Sends one complete native packet with borrowed file descriptors.
    ///
    /// The descriptors remain owned by the caller. At most
    /// [`MAX_FILE_DESCRIPTORS`] descriptors may accompany one packet.
    pub fn send_packet_with_fds(
        &self,
        packet: &[u8],
        descriptors: &[BorrowedFd<'_>],
    ) -> io::Result<()> {
        validate_packet(packet)?;
        if descriptors.len() > MAX_FILE_DESCRIPTORS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many file descriptors in native packet",
            ));
        }
        if descriptors.is_empty() {
            return self.send_packet(packet);
        }
        let mut control = vec![0_u8; cmsg_space(std::mem::size_of::<i32>() * descriptors.len())];
        let header = control.as_mut_ptr().cast::<ControlMessageHeader>();
        unsafe {
            (*header).length = cmsg_len(std::mem::size_of::<i32>() * descriptors.len());
            (*header).level = SOL_SOCKET_CMSG;
            (*header).message_type = SCM_RIGHTS;
            let destination = control
                .as_mut_ptr()
                .add(cmsg_align(std::mem::size_of::<ControlMessageHeader>()))
                .cast::<i32>();
            for (index, descriptor) in descriptors.iter().enumerate() {
                *destination.add(index) = descriptor.as_raw_fd();
            }
            let mut iovec = Iovec {
                base: packet.as_ptr() as *mut u8,
                length: packet.len(),
            };
            let message = MessageHeader {
                name: std::ptr::null_mut(),
                name_length: 0,
                iov: &mut iovec,
                iov_length: 1,
                control: control.as_mut_ptr().cast(),
                control_length: control.len(),
                flags: 0,
            };
            let written = sendmsg(self.fd.as_raw_fd(), &message, MSG_NOSIGNAL);
            check_packet_write(written, packet.len())
        }
    }

    /// Receives one packet and its owned ancillary file descriptors.
    ///
    /// A packet carrying more than `maximum_descriptors` descriptors is
    /// rejected and all received descriptors are closed with their owners.
    pub fn receive_packet_with_fds(
        &self,
        maximum_size: usize,
        maximum_descriptors: usize,
    ) -> io::Result<Option<(Vec<u8>, Vec<OwnedFd>)>> {
        if maximum_descriptors > MAX_FILE_DESCRIPTORS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "configured descriptor limit exceeds native transport limit",
            ));
        }
        if maximum_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "maximum packet size must be non-zero",
            ));
        }
        let mut packet = vec![0; maximum_size];
        let mut control = vec![0_u8; cmsg_space(std::mem::size_of::<i32>() * maximum_descriptors)];
        let mut iovec = Iovec {
            base: packet.as_mut_ptr(),
            length: packet.len(),
        };
        let mut message = MessageHeader {
            name: std::ptr::null_mut(),
            name_length: 0,
            iov: &mut iovec,
            iov_length: 1,
            control: control.as_mut_ptr().cast(),
            control_length: control.len(),
            flags: 0,
        };
        let received = unsafe {
            recvmsg(
                self.fd.as_raw_fd(),
                &mut message,
                MSG_TRUNC | MSG_CMSG_CLOEXEC,
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
        if message.flags & MSG_CTRUNC != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "native packet contains too many file descriptors",
            ));
        }
        packet.truncate(received as usize);
        let descriptors = unsafe { parse_descriptors(&control, message.control_length)? };
        if descriptors.len() > maximum_descriptors {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "native packet contains too many file descriptors",
            ));
        }
        Ok(Some((packet, descriptors)))
    }

    /// Receives one packet, up to `maximum_size` bytes, without descriptors.
    ///
    /// Returns `None` when the peer disconnects. Packets carrying descriptors
    /// are rejected because dropping them without an explicit ownership rule
    /// would leak the received descriptors.
    pub fn receive_packet(&self, maximum_size: usize) -> io::Result<Option<Vec<u8>>> {
        let Some((packet, descriptors)) = self.receive_packet_with_fds(maximum_size, 0)? else {
            return Ok(None);
        };
        if !descriptors.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "native packet unexpectedly contains file descriptors",
            ));
        }
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

    /// Shuts down both directions of the connected socket.
    pub fn disconnect(self) -> io::Result<()> {
        if unsafe { shutdown(self.fd.as_raw_fd(), SHUT_RDWR) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Duplicates this connection for an independent sender handle.
    pub fn try_clone(&self) -> io::Result<Self> {
        let fd = unsafe { dup(self.fd.as_raw_fd()) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
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
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
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

fn validate_packet(packet: &[u8]) -> io::Result<()> {
    if packet.is_empty() {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "native packets must not be empty",
        ))
    } else {
        Ok(())
    }
}

fn check_packet_write(written: isize, expected: usize) -> io::Result<()> {
    if written < 0 {
        return Err(io::Error::last_os_error());
    }
    if written as usize != expected {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "native packet was not sent completely",
        ));
    }
    Ok(())
}

unsafe fn parse_descriptors(control: &[u8], control_length: usize) -> io::Result<Vec<OwnedFd>> {
    let mut descriptors = Vec::new();
    let mut offset = 0;
    while offset + std::mem::size_of::<ControlMessageHeader>() <= control_length {
        let header = unsafe { &*(control.as_ptr().add(offset).cast::<ControlMessageHeader>()) };
        if header.length < cmsg_len(0) || offset + header.length > control_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "native packet contains malformed control data",
            ));
        }
        if header.level == SOL_SOCKET_CMSG && header.message_type == SCM_RIGHTS {
            let bytes = header.length - cmsg_len(0);
            if bytes % std::mem::size_of::<i32>() != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "native packet contains malformed file descriptors",
                ));
            }
            let data = unsafe {
                control
                    .as_ptr()
                    .add(offset + cmsg_align(std::mem::size_of::<ControlMessageHeader>()))
                    .cast::<i32>()
            };
            for index in 0..bytes / std::mem::size_of::<i32>() {
                let descriptor = unsafe { *data.add(index) };
                descriptors.push(unsafe { OwnedFd::from_raw_fd(descriptor) });
            }
        }
        offset += cmsg_align(header.length);
    }
    Ok(descriptors)
}

impl AsRawFd for Listener {
    fn as_raw_fd(&self) -> i32 {
        self.fd.as_raw_fd()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::os::fd::AsFd;
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

    #[test]
    fn connection_transfers_owned_file_descriptors() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("busd-fd-{}-{nonce}.sock", process::id()));
        let listener = Listener::bind(&path).unwrap();
        let accepted = thread::spawn(move || {
            let peer = listener.accept().unwrap();
            let (packet, descriptors) = peer.receive_packet_with_fds(64, 1).unwrap().unwrap();
            (packet, descriptors.len(), descriptors[0].as_raw_fd())
        });

        let connection = Connection::connect(&path).unwrap();
        let file = File::open("/dev/null").unwrap();
        connection
            .send_packet_with_fds(b"fd", &[file.as_fd()])
            .unwrap();
        let (packet, count, descriptor) = accepted.join().unwrap();

        assert_eq!(packet, b"fd");
        assert_eq!(count, 1);
        assert!(descriptor >= 0);
        std::fs::remove_file(path).unwrap();
    }
}
