#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! Transport-independent broker state.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use bus_policy::{Action, Credentials, Policy};
use bus_protocol::{Channel, ClientId, Headers, Namespace, PeerId};

/// Claimed metadata advertised during peer setup.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClientHello {
    /// A non-authenticating implementation identifier.
    pub client_id: Option<ClientId>,
    /// Additional claimed metadata.
    pub headers: Headers,
}

/// State associated with a connected peer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Peer {
    /// Broker-assigned identity.
    pub id: PeerId,
    /// Broker-verified identity.
    pub credentials: Credentials,
    /// Claimed client metadata.
    pub hello: ClientHello,
}

/// In-memory state for one broker instance.
pub struct Broker<P> {
    policy: P,
    next_peer: u64,
    peers: BTreeMap<PeerId, Peer>,
    namespaces: BTreeMap<Namespace, PeerId>,
    subscriptions: BTreeMap<Channel, BTreeSet<PeerId>>,
}

impl<P: Policy> Broker<P> {
    /// Creates an empty broker using `policy` for authorization decisions.
    #[must_use]
    pub fn new(policy: P) -> Self {
        Self {
            policy,
            next_peer: 1,
            peers: BTreeMap::new(),
            namespaces: BTreeMap::new(),
            subscriptions: BTreeMap::new(),
        }
    }

    /// Registers a peer after its transport authenticated credentials.
    pub fn connect(
        &mut self,
        credentials: Credentials,
        hello: ClientHello,
    ) -> Result<PeerId, Error> {
        self.authorize(credentials, Action::Connect)?;
        let id = PeerId::new(self.next_peer);
        self.next_peer = self
            .next_peer
            .checked_add(1)
            .ok_or(Error::PeerIdExhausted)?;
        self.peers.insert(
            id,
            Peer {
                id,
                credentials,
                hello,
            },
        );
        Ok(id)
    }

    /// Removes a peer and releases all state owned by that connection.
    pub fn disconnect(&mut self, peer: PeerId) -> Result<(), Error> {
        if self.peers.remove(&peer).is_none() {
            return Err(Error::UnknownPeer(peer));
        }
        self.namespaces.retain(|_, owner| *owner != peer);
        self.subscriptions.retain(|_, peers| {
            peers.remove(&peer);
            !peers.is_empty()
        });
        Ok(())
    }

    /// Exclusively claims a namespace for a connected peer.
    pub fn claim(&mut self, peer: PeerId, namespace: Namespace) -> Result<(), Error> {
        let credentials = self.credentials(peer)?;
        self.authorize(credentials, Action::ClaimNamespace(namespace.clone()))?;
        if let Some(owner) = self.namespaces.get(&namespace) {
            return Err(Error::NamespaceAlreadyOwned {
                namespace,
                owner: *owner,
            });
        }
        self.namespaces.insert(namespace, peer);
        Ok(())
    }

    /// Subscribes a connected peer to an unowned channel.
    pub fn subscribe(&mut self, peer: PeerId, channel: Channel) -> Result<(), Error> {
        let credentials = self.credentials(peer)?;
        self.authorize(credentials, Action::Subscribe(channel.clone()))?;
        self.subscriptions.entry(channel).or_default().insert(peer);
        Ok(())
    }

    /// Returns the current provider for a namespace, if present.
    #[must_use]
    pub fn namespace_owner(&self, namespace: &Namespace) -> Option<PeerId> {
        self.namespaces.get(namespace).copied()
    }

    /// Returns the currently subscribed peers for a channel.
    #[must_use]
    pub fn subscribers(&self, channel: &Channel) -> Vec<PeerId> {
        self.subscriptions
            .get(channel)
            .map_or_else(Vec::new, |peers| peers.iter().copied().collect())
    }

    /// Returns a connected peer's state.
    #[must_use]
    pub fn peer(&self, peer: PeerId) -> Option<&Peer> {
        self.peers.get(&peer)
    }

    fn credentials(&self, peer: PeerId) -> Result<Credentials, Error> {
        self.peers
            .get(&peer)
            .map(|peer| peer.credentials)
            .ok_or(Error::UnknownPeer(peer))
    }

    fn authorize(&self, credentials: Credentials, action: Action) -> Result<(), Error> {
        if self.policy.permits(credentials, &action) {
            Ok(())
        } else {
            Err(Error::Denied(action))
        }
    }
}

/// A broker operation error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    /// The addressed peer is not connected.
    UnknownPeer(PeerId),
    /// A namespace already has a connected owner.
    NamespaceAlreadyOwned {
        /// The requested namespace.
        namespace: Namespace,
        /// Its current owner.
        owner: PeerId,
    },
    /// The active policy rejected an action.
    Denied(Action),
    /// No further peer identifiers can be assigned.
    PeerIdExhausted,
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPeer(peer) => write!(formatter, "unknown peer {peer}"),
            Self::NamespaceAlreadyOwned { namespace, owner } => {
                write!(
                    formatter,
                    "namespace {namespace} is already owned by {owner}"
                )
            }
            Self::Denied(_) => formatter.write_str("operation denied by policy"),
            Self::PeerIdExhausted => formatter.write_str("peer identifier space exhausted"),
        }
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::*;
    use bus_policy::AllowAll;

    fn credentials() -> Credentials {
        Credentials {
            pid: 1,
            uid: 1000,
            gid: 1000,
        }
    }

    #[test]
    fn namespace_is_exclusive_and_released_on_disconnect() {
        let namespace = Namespace::parse("bus://services").unwrap();
        let mut broker = Broker::new(AllowAll);
        let first = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let second = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();

        broker.claim(first, namespace.clone()).unwrap();
        assert!(matches!(
            broker.claim(second, namespace.clone()),
            Err(Error::NamespaceAlreadyOwned { .. })
        ));
        broker.disconnect(first).unwrap();
        broker.claim(second, namespace.clone()).unwrap();
        assert_eq!(broker.namespace_owner(&namespace), Some(second));
    }

    #[test]
    fn channels_have_no_owner() {
        let channel = Channel::parse("service.events").unwrap();
        let mut broker = Broker::new(AllowAll);
        let first = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let second = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();

        broker.subscribe(first, channel.clone()).unwrap();
        broker.subscribe(second, channel.clone()).unwrap();
        assert_eq!(broker.subscribers(&channel), vec![first, second]);
    }
}
