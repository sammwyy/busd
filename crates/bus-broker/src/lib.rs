#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! Transport-independent broker state.

use std::collections::BTreeMap;
use std::fmt;

use bus_policy::{Action, Credentials, Policy};
use bus_protocol::{
    Capabilities, Channel, ClientId, ClientSelection, Destination, HeaderFilter, HeaderValue,
    Headers, Namespace, PeerId,
};

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
    subscriptions: BTreeMap<Channel, BTreeMap<PeerId, Vec<HeaderFilter>>>,
    events: Vec<LifecycleEvent>,
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
            events: Vec::new(),
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
            events: Vec::new(),
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
        self.events.push(LifecycleEvent::PeerConnected { peer: id });
        Ok(ConnectedPeer { id, capabilities })
    }

    /// Removes a peer and releases all state owned by that connection.
    pub fn disconnect(&mut self, peer: PeerId) -> Result<(), Error> {
        if self.peers.remove(&peer).is_none() {
            return Err(Error::UnknownPeer(peer));
        }
        let released: Vec<_> = self
            .namespaces
            .iter()
            .filter(|(_, owner)| **owner == peer)
            .map(|(namespace, _)| namespace.clone())
            .collect();
        self.namespaces.retain(|_, owner| *owner != peer);
        for namespace in released {
            self.events.push(LifecycleEvent::NamespaceOwnerChanged {
                namespace,
                previous: Some(peer),
                current: None,
            });
        }
        self.subscriptions.retain(|_, peers| {
            peers.remove(&peer);
            !peers.is_empty()
        });
        self.events.push(LifecycleEvent::PeerDisconnected { peer });
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
        self.namespaces.insert(namespace.clone(), peer);
        self.events.push(LifecycleEvent::NamespaceOwnerChanged {
            namespace: namespace.clone(),
            previous: None,
            current: Some(peer),
        });
        Ok(())
    }

    /// Subscribes a connected peer to an unowned channel.
    pub fn subscribe(&mut self, peer: PeerId, channel: Channel) -> Result<(), Error> {
        self.subscribe_with_filters(peer, channel, Vec::new())
    }

    /// Subscribes a connected peer using structural message-header filters.
    pub fn subscribe_with_filters(
        &mut self,
        peer: PeerId,
        channel: Channel,
        filters: Vec<HeaderFilter>,
    ) -> Result<(), Error> {
        let credentials = self.credentials(peer)?;
        self.authorize(credentials, Action::Subscribe(channel.clone()))?;
        self.subscriptions
            .entry(channel)
            .or_default()
            .insert(peer, filters);
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
            .map_or_else(Vec::new, |peers| peers.keys().copied().collect())
    }

    /// Returns a connected peer's state.
    #[must_use]
    pub fn peer(&self, peer: PeerId) -> Option<&Peer> {
        self.peers.get(&peer)
    }

    /// Selects recipients for a message without inspecting its payload.
    pub fn route(
        &self,
        sender: PeerId,
        destination: &Destination,
        headers: &Headers,
    ) -> Result<Vec<PeerId>, Error> {
        let credentials = self.credentials(sender)?;
        let recipients = match destination {
            Destination::Broker => return Err(Error::NoRecipient),
            Destination::Peer(peer) => {
                self.authorize(credentials, Action::SendPeer(*peer))?;
                self.peers
                    .contains_key(peer)
                    .then_some(*peer)
                    .into_iter()
                    .collect()
            }
            Destination::Namespace(namespace) => {
                self.authorize(credentials, Action::SendNamespace(namespace.clone()))?;
                self.namespace_owner(namespace).into_iter().collect()
            }
            Destination::Channel(channel) => {
                self.authorize(credentials, Action::Publish(channel.clone()))?;
                self.subscriptions
                    .get(channel)
                    .map_or_else(Vec::new, |subscribers| {
                        subscribers
                            .iter()
                            .filter_map(|(peer, filters)| {
                                filters_match(filters, headers).then_some(*peer)
                            })
                            .collect()
                    })
            }
            Destination::ClientId {
                client_id,
                selection,
            } => {
                self.authorize(credentials, Action::SendClient(client_id.clone()))?;
                let mut matching: Vec<_> = self
                    .peers
                    .values()
                    .filter(|peer| peer.hello.client_id.as_ref() == Some(client_id))
                    .map(|peer| peer.id)
                    .collect();
                match selection {
                    ClientSelection::First | ClientSelection::Any => matching.truncate(1),
                    ClientSelection::All => {}
                }
                matching
            }
            Destination::Broadcast => {
                self.authorize(credentials, Action::Broadcast)?;
                self.peers
                    .keys()
                    .copied()
                    .filter(|peer| *peer != sender)
                    .collect()
            }
        };
        match destination {
            Destination::Channel(_) | Destination::Broadcast => Ok(recipients),
            _ if recipients.is_empty() => Err(Error::NoRecipient),
            _ => Ok(recipients),
        }
    }

    /// Returns discovery records that keep claimed and authenticated metadata separate.
    #[must_use]
    pub fn peers(&self) -> Vec<PeerInfo> {
        self.peers.values().map(PeerInfo::from).collect()
    }

    /// Returns every currently claimed namespace and its owner.
    #[must_use]
    pub fn namespaces(&self) -> Vec<(Namespace, PeerId)> {
        self.namespaces
            .iter()
            .map(|(namespace, peer)| (namespace.clone(), *peer))
            .collect()
    }

    /// Returns every channel with its current subscribers and filters.
    #[must_use]
    pub fn subscriptions(&self) -> Vec<SubscriptionInfo> {
        self.subscriptions
            .iter()
            .map(|(channel, peers)| SubscriptionInfo {
                channel: channel.clone(),
                subscribers: peers
                    .iter()
                    .map(|(peer, filters)| (*peer, filters.clone()))
                    .collect(),
            })
            .collect()
    }

    /// Returns broker capabilities available for future sessions.
    #[must_use]
    pub fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    /// Drains lifecycle events in generation order.
    pub fn drain_events(&mut self) -> Vec<LifecycleEvent> {
        std::mem::take(&mut self.events)
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

/// A discovery-safe view of a connected peer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerInfo {
    /// Broker-assigned peer identity.
    pub id: PeerId,
    /// Client-claimed implementation identifier.
    pub client_id: Option<ClientId>,
    /// Client-claimed headers.
    pub claimed_headers: Headers,
    /// Kernel-authenticated credentials.
    pub credentials: Credentials,
    /// Negotiated session capabilities.
    pub capabilities: Capabilities,
}

impl From<&Peer> for PeerInfo {
    fn from(peer: &Peer) -> Self {
        Self {
            id: peer.id,
            client_id: peer.hello.client_id.clone(),
            claimed_headers: peer.hello.headers.clone(),
            credentials: peer.credentials,
            capabilities: peer.capabilities.clone(),
        }
    }
}

/// A discovery-safe view of a channel subscription set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscriptionInfo {
    /// Channel name.
    pub channel: Channel,
    /// Each subscriber and its structural filters.
    pub subscribers: Vec<(PeerId, Vec<HeaderFilter>)>,
}

/// A broker lifecycle event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LifecycleEvent {
    /// A peer completed the handshake.
    PeerConnected {
        /// Connected peer.
        peer: PeerId,
    },
    /// A peer disconnected.
    PeerDisconnected {
        /// Disconnected peer.
        peer: PeerId,
    },
    /// A namespace changed provider.
    NamespaceOwnerChanged {
        /// Namespace whose owner changed.
        namespace: Namespace,
        /// Previous owner, if any.
        previous: Option<PeerId>,
        /// Current owner, if any.
        current: Option<PeerId>,
    },
}

fn filters_match(filters: &[HeaderFilter], headers: &Headers) -> bool {
    filters.iter().all(|filter| match filter {
        HeaderFilter::Exists(name) => headers.contains_key(name),
        HeaderFilter::NotExists(name) => !headers.contains_key(name),
        HeaderFilter::Equal(name, value) => headers.get(name) == Some(value),
        HeaderFilter::NotEqual(name, value) => headers.get(name) != Some(value),
        HeaderFilter::Prefix(name, value) => match (headers.get(name), value) {
            (Some(HeaderValue::Text(actual)), HeaderValue::Text(prefix)) => {
                actual.starts_with(prefix)
            }
            (Some(HeaderValue::Binary(actual)), HeaderValue::Binary(prefix)) => {
                actual.starts_with(prefix)
            }
            _ => false,
        },
    })
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
    /// No eligible recipient matched a unicast destination.
    NoRecipient,
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
            Self::NoRecipient => formatter.write_str("no eligible recipient"),
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
    use bus_policy::{AllowAll, Policy};

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
        broker.disconnect(first).unwrap();
        assert_eq!(broker.subscribers(&channel), vec![second]);
        broker.disconnect(second).unwrap();
        assert!(broker.subscribers(&channel).is_empty());
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

    #[test]
    fn policy_uses_authenticated_credentials_not_claimed_headers() {
        struct RootOnly;

        impl Policy for RootOnly {
            fn permits(&self, credentials: Credentials, _: &Action) -> bool {
                credentials.uid == 0
            }
        }

        let mut broker = Broker::new(RootOnly);
        let result = broker.connect(
            credentials(),
            ClientHello {
                headers: [("client.uid".into(), bus_protocol::HeaderValue::Unsigned(0))].into(),
                ..ClientHello::default()
            },
        );
        assert!(matches!(result, Err(Error::Denied(Action::Connect))));
    }

    #[test]
    fn routing_selects_expected_recipients_and_filters() {
        let mut broker = Broker::new(AllowAll);
        let sender = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let provider = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let matching = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let other = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let unrelated = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let namespace = Namespace::parse("bus://service").unwrap();
        let channel = Channel::parse("events").unwrap();
        broker.claim(provider, namespace.clone()).unwrap();
        broker
            .subscribe_with_filters(
                matching,
                channel.clone(),
                vec![HeaderFilter::Equal(
                    "kind".into(),
                    HeaderValue::Text("match".into()),
                )],
            )
            .unwrap();
        broker.subscribe(other, channel.clone()).unwrap();
        let headers = [("kind".into(), HeaderValue::Text("match".into()))].into();
        assert_eq!(
            broker
                .route(sender, &Destination::Namespace(namespace), &headers)
                .unwrap(),
            vec![provider]
        );
        assert_eq!(
            broker
                .route(sender, &Destination::Peer(other), &headers)
                .unwrap(),
            vec![other]
        );
        assert_eq!(
            broker
                .route(sender, &Destination::Channel(channel.clone()), &headers)
                .unwrap(),
            vec![matching, other]
        );
        assert!(
            !broker
                .route(sender, &Destination::Channel(channel.clone()), &headers)
                .unwrap()
                .contains(&unrelated)
        );
        assert!(matches!(
            broker.route(sender, &Destination::Peer(PeerId::new(99)), &headers),
            Err(Error::NoRecipient)
        ));
    }

    #[test]
    fn client_selection_and_broadcast_policy_are_explicit() {
        let mut broker = Broker::new(AllowAll);
        let sender = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let first = broker
            .connect(
                credentials(),
                ClientHello {
                    client_id: Some(ClientId::parse("worker").unwrap()),
                    ..ClientHello::default()
                },
            )
            .unwrap();
        let second = broker
            .connect(
                credentials(),
                ClientHello {
                    client_id: Some(ClientId::parse("worker").unwrap()),
                    ..ClientHello::default()
                },
            )
            .unwrap();
        let destination = |selection| Destination::ClientId {
            client_id: ClientId::parse("worker").unwrap(),
            selection,
        };
        assert_eq!(
            broker
                .route(
                    sender,
                    &destination(ClientSelection::First),
                    &Headers::new()
                )
                .unwrap(),
            vec![first]
        );
        assert_eq!(
            broker
                .route(sender, &destination(ClientSelection::Any), &Headers::new())
                .unwrap(),
            vec![first]
        );
        assert_eq!(
            broker
                .route(sender, &destination(ClientSelection::All), &Headers::new())
                .unwrap(),
            vec![first, second]
        );
        assert_eq!(
            broker
                .route(sender, &Destination::Broadcast, &Headers::new())
                .unwrap(),
            vec![first, second]
        );

        struct NoBroadcast;
        impl Policy for NoBroadcast {
            fn permits(&self, _: Credentials, action: &Action) -> bool {
                !matches!(action, Action::Broadcast)
            }
        }
        let mut restricted = Broker::new(NoBroadcast);
        let peer = restricted
            .connect(credentials(), ClientHello::default())
            .unwrap();
        assert!(matches!(
            restricted.route(peer, &Destination::Broadcast, &Headers::new()),
            Err(Error::Denied(Action::Broadcast))
        ));
    }

    #[test]
    fn structural_filters_and_discovery_preserve_metadata_provenance() {
        let headers: Headers = [
            (
                "binary".into(),
                HeaderValue::Binary(b"prefix-value".to_vec()),
            ),
            ("enabled".into(), HeaderValue::Boolean(true)),
            ("text".into(), HeaderValue::Text("prefix-value".into())),
        ]
        .into();
        assert!(filters_match(
            &[
                HeaderFilter::Exists("enabled".into()),
                HeaderFilter::NotExists("missing".into()),
                HeaderFilter::Equal("enabled".into(), HeaderValue::Boolean(true)),
                HeaderFilter::NotEqual("enabled".into(), HeaderValue::Boolean(false)),
                HeaderFilter::Prefix("text".into(), HeaderValue::Text("prefix".into())),
                HeaderFilter::Prefix("binary".into(), HeaderValue::Binary(b"prefix".to_vec())),
            ],
            &headers
        ));

        let mut broker = Broker::with_capabilities(AllowAll, ["capability".into()].into());
        let peer = broker
            .connect(
                credentials(),
                ClientHello {
                    client_id: Some(ClientId::parse("client").unwrap()),
                    headers: [("client.version".into(), HeaderValue::Text("1".into()))].into(),
                    capabilities: ["capability".into()].into(),
                },
            )
            .unwrap();
        let info = broker.peers().pop().unwrap();
        assert_eq!(info.id, peer);
        assert_eq!(
            info.claimed_headers.get("client.version"),
            Some(&HeaderValue::Text("1".into()))
        );
        assert_eq!(info.credentials, credentials());
        assert_eq!(
            broker.capabilities(),
            &Capabilities::from(["capability".into()])
        );
        assert!(
            matches!(broker.drain_events().as_slice(), [LifecycleEvent::PeerConnected { peer: id }] if *id == peer)
        );
    }
}
