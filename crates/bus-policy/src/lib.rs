#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! Authorization boundaries for BUS/1.

use bus_protocol::{Channel, ClientId, Namespace, PeerId};

/// Broker-verified operating-system identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Credentials {
    /// Process identifier.
    pub pid: u32,
    /// User identifier.
    pub uid: u32,
    /// Primary group identifier.
    pub gid: u32,
}

/// An action presented to a policy implementation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Action {
    /// Open a connection.
    Connect,
    /// Claim an API namespace.
    ClaimNamespace(Namespace),
    /// Subscribe to a multicast channel.
    Subscribe(Channel),
    /// Publish to a multicast channel.
    Publish(Channel),
    /// Send to a direct peer.
    SendPeer(PeerId),
    /// Send to a namespace provider.
    SendNamespace(Namespace),
    /// Send to peers selected by a client implementation identifier.
    SendClient(ClientId),
    /// Send a global broadcast.
    Broadcast,
}

/// Decides whether an authenticated peer may perform an action.
pub trait Policy: Send + Sync {
    /// Returns whether the action is permitted.
    fn permits(&self, credentials: Credentials, action: &Action) -> bool;
}

/// A permissive policy suitable only for development.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowAll;

impl Policy for AllowAll {
    fn permits(&self, _: Credentials, _: &Action) -> bool {
        true
    }
}
