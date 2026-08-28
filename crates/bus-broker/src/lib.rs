#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! Transport-independent broker state.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use bus_policy::{Action, Credentials, Policy};
use bus_protocol::{Capabilities, Channel, ClientId, Headers, Namespace, PeerId};

/// Claimed metadata advertised during peer setup.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClientHello {
    /// A non-authenticating implementation identifier.
    pub client_id: Option<ClientId>,
    /// Additional claimed metadata.
    pub headers: Headers,
    /// Optional client capabilities offered for negotiation.
    pub capabilities: Capabilities,
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
    /// Capabilities selected by the broker for this session.
    pub capabilities: Capabilities,
}

/// The result of successfully creating a broker session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectedPeer {
    /// The broker-assigned peer identity.
    pub id: PeerId,
    /// Capabilities selected from the client offer and broker support.
    pub capabilities: Capabilities,
}

/// In-memory state for one broker instance.
pub struct Broker<P> {
    policy: P,
    capabilities: Capabilities,
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
            capabilities: Capabilities::new(),
            next_peer: 1,
            peers: BTreeMap::new(),
            namespaces: BTreeMap::new(),
            subscriptions: BTreeMap::new(),
        }
    }

    /// Creates an empty broker with the supplied optional capabilities.
    #[must_use]
    pub fn with_capabilities(policy: P, capabilities: Capabilities) -> Self {
        Self {
            policy,
            capabilities,
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
        Ok(self.connect_session(credentials, hello)?.id)
    }

    /// Registers a peer and selects capabilities from its `HELLO` offer.
    pub fn connect_session(
        &mut self,
        credentials: Credentials,
        hello: ClientHello,
    ) -> Result<ConnectedPeer, Error> {
        validate_claimed_headers(&hello.headers)?;
        self.authorize(credentials, Action::Connect)?;
        let id = PeerId::new(self.next_peer);
        self.next_peer = self
            .next_peer
            .checked_add(1)
            .ok_or(Error::PeerIdExhausted)?;
        let capabilities: Capabilities = self
            .capabilities
            .intersection(&hello.capabilities)
            .cloned()
            .collect();
        self.peers.insert(
            id,
            Peer {
                id,
                credentials,
                hello,
                capabilities: capabilities.clone(),
            },
        );
        Ok(ConnectedPeer { id, capabilities })
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

    /// Removes a connected peer's subscription from a channel.
    pub fn unsubscribe(&mut self, peer: PeerId, channel: &Channel) -> Result<(), Error> {
        self.credentials(peer)?;
        if let Some(peers) = self.subscriptions.get_mut(channel) {
            peers.remove(&peer);
            if peers.is_empty() {
                self.subscriptions.remove(channel);
            }
        }
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
    /// A client attempted to claim broker-owned metadata.
    ReservedHeader(String),
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
            Self::ReservedHeader(header) => {
                write!(
                    formatter,
                    "claimed header {header} is reserved for the broker"
                )
            }
        }
    }
}

fn validate_claimed_headers(headers: &Headers) -> Result<(), Error> {
    for name in headers.keys() {
        if name.starts_with("auth.") || name.starts_with("broker.") || name.starts_with("peer.") {
            return Err(Error::ReservedHeader(name.clone()));
        }
    }
    Ok(())
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

    #[test]
    fn capabilities_are_intersected_and_broker_headers_are_rejected() {
        let mut broker = Broker::with_capabilities(AllowAll, ["fd-passing".into()].into());
        let session = broker
            .connect_session(
                credentials(),
                ClientHello {
                    capabilities: ["fd-passing".into(), "other".into()].into(),
                    ..ClientHello::default()
                },
            )
            .unwrap();
        assert_eq!(session.capabilities, ["fd-passing".into()].into());
        assert_eq!(broker.peer(session.id).unwrap().credentials.uid, 1000);

        let error = broker.connect(
            credentials(),
            ClientHello {
                headers: [("peer.uid".into(), bus_protocol::HeaderValue::Unsigned(0))].into(),
                ..ClientHello::default()
            },
        );
        assert!(matches!(error, Err(Error::ReservedHeader(header)) if header == "peer.uid"));
    }
}
