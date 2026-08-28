#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! Transport-independent broker state.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use bus_policy::{Action, Credentials, Policy, Request as PolicyRequest};
use bus_protocol::{
    AckPolicy, AckRequirement, Capabilities, Channel, ClientId, ClientSelection, DeliveryOutcome,
    Destination, Frame, HeaderFilter, HeaderValue, Headers, MessageId, MessageKind, Namespace,
    PeerId, RequestPolicy, RetryPolicy, Status,
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
    deliveries: BTreeMap<MessageId, PendingDelivery>,
    events: Vec<LifecycleEvent>,
}

struct PendingDelivery {
    sender: PeerId,
    message: Frame,
    recipients: BTreeSet<PeerId>,
    acknowledgements: BTreeSet<PeerId>,
    responses: BTreeMap<PeerId, Frame>,
    acknowledgement_complete: bool,
    deadline_ms: u64,
    attempts: u8,
    next_retry_ms: Option<u64>,
}

/// Work the daemon must perform after a broker reliability transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliveryEvent {
    /// Forward a canonical message to each listed recipient.
    Deliver {
        /// Authenticated origin of the forwarded message.
        sender: PeerId,
        /// Recipients selected by the broker.
        recipients: Vec<PeerId>,
        /// The message to forward without changing its logical ID.
        message: Frame,
    },
    /// Return a terminal or acknowledgement outcome to the originating peer.
    Result {
        /// Original sender.
        sender: PeerId,
        /// Stable logical message identifier.
        message_id: MessageId,
        /// Broker outcome.
        outcome: DeliveryOutcome,
    },
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
            deliveries: BTreeMap::new(),
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
            deliveries: BTreeMap::new(),
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
        self.authorize_hello(&credentials, &hello, Action::Connect)?;
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
        self.disconnect_with_events(peer).map(|_| ())
    }

    fn remove_peer(&mut self, peer: PeerId) -> Result<(), Error> {
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
        self.authorize(peer, Action::ClaimNamespace(namespace.clone()))?;
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
        self.authorize(peer, Action::Subscribe(channel.clone()))?;
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
        self.credentials(sender)?;
        let recipients = match destination {
            Destination::Broker => return Err(Error::NoRecipient),
            Destination::Peer(peer) => {
                self.authorize(sender, Action::SendPeer(*peer))?;
                self.peers
                    .contains_key(peer)
                    .then_some(*peer)
                    .into_iter()
                    .collect()
            }
            Destination::Namespace(namespace) => {
                self.authorize(sender, Action::SendNamespace(namespace.clone()))?;
                self.namespace_owner(namespace).into_iter().collect()
            }
            Destination::Channel(channel) => {
                self.authorize(sender, Action::Publish(channel.clone()))?;
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
                self.authorize(sender, Action::SendClient(client_id.clone()))?;
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
                self.authorize(sender, Action::Broadcast)?;
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

    /// Begins routing one logical message and records any required reliability state.
    pub fn begin_delivery(
        &mut self,
        sender: PeerId,
        message: Frame,
        now_ms: u64,
    ) -> Result<Vec<DeliveryEvent>, Error> {
        let Frame::Message {
            kind,
            ack_policy,
            ack_requirement,
            request_policy: _,
            deadline_ms,
            retry,
            destination,
            message_id,
            headers,
            ..
        } = &message
        else {
            return Err(Error::InvalidDelivery);
        };
        self.credentials(sender)?;
        validate_message_headers(headers)?;
        if *kind == MessageKind::Response {
            return self.complete_response(sender, message);
        }
        if self.deliveries.contains_key(message_id) {
            return Err(Error::DuplicateMessage(*message_id));
        }
        let retry = *retry;
        let recipients = match self.route(sender, destination, headers) {
            Ok(recipients) => recipients,
            Err(Error::NoRecipient) => {
                return Ok(vec![DeliveryEvent::Result {
                    sender,
                    message_id: *message_id,
                    outcome: DeliveryOutcome::NoRecipient,
                }]);
            }
            Err(error) => return Err(error),
        };
        let recipients: BTreeSet<_> = recipients.into_iter().collect();
        if recipients.is_empty()
            && (*kind == MessageKind::Request
                || matches!(ack_policy, AckPolicy::Received | AckPolicy::Processed))
        {
            return Ok(vec![DeliveryEvent::Result {
                sender,
                message_id: *message_id,
                outcome: DeliveryOutcome::NoRecipient,
            }]);
        }
        if !requirement_possible(*ack_requirement, recipients.len()) {
            return Ok(vec![DeliveryEvent::Result {
                sender,
                message_id: *message_id,
                outcome: DeliveryOutcome::NoRecipient,
            }]);
        }

        let mut events = vec![DeliveryEvent::Deliver {
            sender,
            recipients: recipients.iter().copied().collect(),
            message: message.clone(),
        }];
        if *ack_policy == AckPolicy::Accepted {
            events.push(DeliveryEvent::Result {
                sender,
                message_id: *message_id,
                outcome: DeliveryOutcome::Accepted,
            });
        }
        let tracked = *kind == MessageKind::Request
            || matches!(ack_policy, AckPolicy::Received | AckPolicy::Processed);
        if tracked {
            let deadline_ms = now_ms
                .checked_add(u64::from(*deadline_ms))
                .ok_or(Error::DeadlineOverflow)?;
            self.deliveries.insert(
                *message_id,
                PendingDelivery {
                    sender,
                    message,
                    recipients,
                    acknowledgements: BTreeSet::new(),
                    responses: BTreeMap::new(),
                    acknowledgement_complete: false,
                    deadline_ms,
                    attempts: 1,
                    next_retry_ms: retry_next(retry, now_ms, 1),
                },
            );
        }
        Ok(events)
    }

    /// Records a receiver acknowledgement and emits an outcome when its requirement is met.
    pub fn acknowledge(
        &mut self,
        peer: PeerId,
        message_id: MessageId,
        policy: AckPolicy,
    ) -> Result<Vec<DeliveryEvent>, Error> {
        self.authorize(peer, Action::Acknowledge)?;
        let pending = self
            .deliveries
            .get_mut(&message_id)
            .ok_or(Error::UnknownDelivery(message_id))?;
        let Frame::Message {
            kind,
            ack_policy,
            ack_requirement,
            ..
        } = &pending.message
        else {
            return Err(Error::InvalidDelivery);
        };
        if !pending.recipients.contains(&peer) || !acknowledgement_satisfies(*ack_policy, policy) {
            return Err(Error::UnexpectedAcknowledgement(message_id));
        }
        pending.acknowledgements.insert(peer);
        if pending.acknowledgement_complete
            || !requirement_met(
                *ack_requirement,
                pending.acknowledgements.len(),
                pending.recipients.len(),
            )
        {
            return Ok(Vec::new());
        }
        pending.acknowledgement_complete = true;
        let outcome = match ack_policy {
            AckPolicy::Received => DeliveryOutcome::Received,
            AckPolicy::Processed => DeliveryOutcome::Processed,
            AckPolicy::None | AckPolicy::Accepted => {
                return Err(Error::UnexpectedAcknowledgement(message_id));
            }
        };
        let sender = pending.sender;
        if *kind != MessageKind::Request {
            self.deliveries.remove(&message_id);
        }
        Ok(vec![DeliveryEvent::Result {
            sender,
            message_id,
            outcome,
        }])
    }

    /// Advances deadlines and bounded retry schedules using a caller-provided monotonic clock.
    pub fn tick(&mut self, now_ms: u64) -> Vec<DeliveryEvent> {
        let ids: Vec<_> = self.deliveries.keys().copied().collect();
        let mut events = Vec::new();
        for message_id in ids {
            let Some(pending) = self.deliveries.get(&message_id) else {
                continue;
            };
            if now_ms >= pending.deadline_ms {
                let sender = pending.sender;
                self.deliveries.remove(&message_id);
                events.push(DeliveryEvent::Result {
                    sender,
                    message_id,
                    outcome: DeliveryOutcome::Timeout,
                });
                continue;
            }
            let Some(next_retry_ms) = pending.next_retry_ms else {
                continue;
            };
            if now_ms < next_retry_ms {
                continue;
            }
            let pending = self
                .deliveries
                .get_mut(&message_id)
                .expect("delivery exists");
            let retry = match &pending.message {
                Frame::Message { retry, .. } => *retry,
                _ => RetryPolicy::None,
            };
            let RetryPolicy::Exponential { max_attempts, .. } = retry else {
                continue;
            };
            if pending.attempts >= max_attempts {
                let sender = pending.sender;
                self.deliveries.remove(&message_id);
                events.push(DeliveryEvent::Result {
                    sender,
                    message_id,
                    outcome: DeliveryOutcome::DeliveryFailed,
                });
                continue;
            }
            pending.attempts += 1;
            pending.next_retry_ms = retry_next(retry, now_ms, pending.attempts);
            events.push(DeliveryEvent::Deliver {
                sender: pending.sender,
                recipients: pending
                    .recipients
                    .difference(&pending.acknowledgements)
                    .copied()
                    .collect(),
                message: pending.message.clone(),
            });
        }
        events
    }

    /// Removes a peer and returns any reliability outcomes caused by that disconnect.
    pub fn disconnect_with_events(&mut self, peer: PeerId) -> Result<Vec<DeliveryEvent>, Error> {
        self.remove_peer(peer)?;
        let affected: Vec<_> = self
            .deliveries
            .iter()
            .filter(|(_, pending)| pending.recipients.contains(&peer))
            .map(|(message_id, pending)| (*message_id, pending.sender))
            .collect();
        let mut events = Vec::new();
        for (message_id, sender) in affected {
            self.deliveries.remove(&message_id);
            events.push(DeliveryEvent::Result {
                sender,
                message_id,
                outcome: DeliveryOutcome::RecipientDisconnected,
            });
        }
        Ok(events)
    }

    fn complete_response(
        &mut self,
        sender: PeerId,
        message: Frame,
    ) -> Result<Vec<DeliveryEvent>, Error> {
        let (correlation_id, status) = match &message {
            Frame::Message {
                correlation_id,
                status,
                ..
            } => (*correlation_id, *status),
            _ => return Err(Error::InvalidDelivery),
        };
        let pending = self
            .deliveries
            .get_mut(&correlation_id)
            .ok_or(Error::UnknownDelivery(correlation_id))?;
        let Frame::Message {
            kind: MessageKind::Request,
            request_policy,
            ..
        } = &pending.message
        else {
            return Err(Error::UnexpectedResponse(correlation_id));
        };
        if !pending.recipients.contains(&sender) || pending.responses.contains_key(&sender) {
            return Err(Error::UnexpectedResponse(correlation_id));
        }
        let requester = pending.sender;
        pending.responses.insert(sender, message.clone());
        let completed = match request_policy {
            RequestPolicy::Exact | RequestPolicy::First => true,
            RequestPolicy::FirstSuccess => {
                status == Status::Success || pending.responses.len() == pending.recipients.len()
            }
            RequestPolicy::All => pending.responses.len() == pending.recipients.len(),
        };
        let forward = match request_policy {
            RequestPolicy::Exact | RequestPolicy::First | RequestPolicy::All => Some(message),
            RequestPolicy::FirstSuccess if status == Status::Success => Some(message),
            RequestPolicy::FirstSuccess if completed => pending.responses.values().next().cloned(),
            RequestPolicy::FirstSuccess => None,
        };
        if completed {
            self.deliveries.remove(&correlation_id);
        }
        Ok(forward
            .into_iter()
            .map(|message| DeliveryEvent::Deliver {
                sender,
                recipients: vec![requester],
                message,
            })
            .collect())
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
            .map(|peer| peer.credentials.clone())
            .ok_or(Error::UnknownPeer(peer))
    }

    fn authorize(&self, peer: PeerId, action: Action) -> Result<(), Error> {
        let peer = self.peers.get(&peer).ok_or(Error::UnknownPeer(peer))?;
        if self.policy.permits(&PolicyRequest {
            credentials: &peer.credentials,
            client_id: peer.hello.client_id.as_ref(),
            claimed_headers: &peer.hello.headers,
            action: &action,
        }) {
            Ok(())
        } else {
            Err(Error::Denied(action))
        }
    }

    fn authorize_hello(
        &self,
        credentials: &Credentials,
        hello: &ClientHello,
        action: Action,
    ) -> Result<(), Error> {
        if self.policy.permits(&PolicyRequest {
            credentials,
            client_id: hello.client_id.as_ref(),
            claimed_headers: &hello.headers,
            action: &action,
        }) {
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
            credentials: peer.credentials.clone(),
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

fn validate_message_headers(headers: &Headers) -> Result<(), Error> {
    for name in headers.keys() {
        if name.starts_with("broker.") || name.starts_with("peer.") || name.starts_with("auth.") {
            return Err(Error::ReservedHeader(name.clone()));
        }
    }
    Ok(())
}

fn requirement_possible(requirement: AckRequirement, recipients: usize) -> bool {
    match requirement {
        AckRequirement::None => true,
        AckRequirement::Any | AckRequirement::All => recipients != 0,
        AckRequirement::Minimum(minimum) => recipients >= usize::from(minimum),
    }
}

fn requirement_met(
    requirement: AckRequirement,
    acknowledgements: usize,
    recipients: usize,
) -> bool {
    match requirement {
        AckRequirement::None => true,
        AckRequirement::Any => acknowledgements != 0,
        AckRequirement::All => acknowledgements == recipients,
        AckRequirement::Minimum(minimum) => acknowledgements >= usize::from(minimum),
    }
}

fn acknowledgement_satisfies(required: AckPolicy, actual: AckPolicy) -> bool {
    matches!(
        (required, actual),
        (
            AckPolicy::Received,
            AckPolicy::Received | AckPolicy::Processed
        ) | (AckPolicy::Processed, AckPolicy::Processed)
    )
}

fn retry_next(retry: RetryPolicy, now_ms: u64, attempts: u8) -> Option<u64> {
    let RetryPolicy::Exponential {
        initial_backoff_ms,
        max_attempts,
    } = retry
    else {
        return None;
    };
    if attempts >= max_attempts {
        return Some(now_ms);
    }
    let shift = u32::from(attempts.saturating_sub(1)).min(16);
    let delay = u64::from(initial_backoff_ms)
        .saturating_mul(1_u64 << shift)
        .min(60_000);
    Some(now_ms.saturating_add(delay))
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
    /// A frame was not a client-originated message.
    InvalidDelivery,
    /// A logical message ID is already in flight.
    DuplicateMessage(MessageId),
    /// No reliability state exists for this logical message.
    UnknownDelivery(MessageId),
    /// A peer acknowledged a message it was not selected to receive.
    UnexpectedAcknowledgement(MessageId),
    /// A peer attempted an invalid response correlation.
    UnexpectedResponse(MessageId),
    /// A relative deadline overflowed the broker clock.
    DeadlineOverflow,
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
            Self::InvalidDelivery => formatter.write_str("invalid delivery frame"),
            Self::DuplicateMessage(message_id) => {
                write!(formatter, "message {message_id:?} is already in flight")
            }
            Self::UnknownDelivery(message_id) => {
                write!(formatter, "unknown delivery {message_id:?}")
            }
            Self::UnexpectedAcknowledgement(message_id) => {
                write!(formatter, "unexpected acknowledgement for {message_id:?}")
            }
            Self::UnexpectedResponse(message_id) => {
                write!(formatter, "unexpected response for {message_id:?}")
            }
            Self::DeadlineOverflow => {
                formatter.write_str("message deadline overflowed broker clock")
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
    use bus_policy::{AllowAll, Policy};

    fn credentials() -> Credentials {
        Credentials {
            pid: 1,
            uid: 1000,
            gid: 1000,
            ..Credentials::default()
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
            fn permits(&self, request: &PolicyRequest<'_>) -> bool {
                request.credentials.uid == 0
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
            fn permits(&self, request: &PolicyRequest<'_>) -> bool {
                !matches!(request.action, Action::Broadcast)
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

    fn reliable_message(
        kind: MessageKind,
        destination: Destination,
        message_id: u8,
        ack_policy: AckPolicy,
        retry: RetryPolicy,
    ) -> Frame {
        Frame::Message {
            kind,
            ack_policy,
            ack_requirement: if matches!(ack_policy, AckPolicy::None | AckPolicy::Accepted) {
                AckRequirement::None
            } else {
                AckRequirement::All
            },
            request_policy: RequestPolicy::Exact,
            deadline_ms: 100,
            retry,
            destination,
            message_id: MessageId::new([message_id; 16]),
            correlation_id: MessageId::absent(),
            status: Status::Success,
            headers: Headers::new(),
            payload: Vec::new(),
        }
    }

    #[test]
    fn retries_keep_the_logical_message_id_and_are_bounded() {
        let mut broker = Broker::new(AllowAll);
        let sender = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let recipient = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let message = reliable_message(
            MessageKind::Signal,
            Destination::Peer(recipient),
            7,
            AckPolicy::Processed,
            RetryPolicy::Exponential {
                initial_backoff_ms: 10,
                max_attempts: 2,
            },
        );
        let message_id = match &message {
            Frame::Message { message_id, .. } => *message_id,
            _ => unreachable!(),
        };
        assert!(matches!(
            broker.begin_delivery(sender, message, 0).unwrap().as_slice(),
            [DeliveryEvent::Deliver { message, .. }]
                if matches!(message, Frame::Message { message_id: id, .. } if *id == message_id)
        ));
        assert!(matches!(
            broker.tick(10).as_slice(),
            [DeliveryEvent::Deliver { message, .. }]
                if matches!(message, Frame::Message { message_id: id, .. } if *id == message_id)
        ));
        assert_eq!(
            broker.tick(30),
            vec![DeliveryEvent::Result {
                sender,
                message_id,
                outcome: DeliveryOutcome::DeliveryFailed,
            }]
        );
    }

    #[test]
    fn timeout_and_duplicate_ids_are_deterministic() {
        let mut broker = Broker::new(AllowAll);
        let sender = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let recipient = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let message = reliable_message(
            MessageKind::Request,
            Destination::Peer(recipient),
            8,
            AckPolicy::None,
            RetryPolicy::None,
        );
        let message_id = match &message {
            Frame::Message { message_id, .. } => *message_id,
            _ => unreachable!(),
        };
        broker.begin_delivery(sender, message.clone(), 10).unwrap();
        assert_eq!(
            broker.begin_delivery(sender, message, 10),
            Err(Error::DuplicateMessage(message_id))
        );
        assert_eq!(
            broker.tick(110),
            vec![DeliveryEvent::Result {
                sender,
                message_id,
                outcome: DeliveryOutcome::Timeout,
            }]
        );
    }

    #[test]
    fn acknowledgements_obey_cardinality_and_recipient_loss_is_explicit() {
        let mut broker = Broker::new(AllowAll);
        let sender = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let first = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let second = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let message = Frame::Message {
            kind: MessageKind::Signal,
            ack_policy: AckPolicy::Processed,
            ack_requirement: AckRequirement::Minimum(2),
            request_policy: RequestPolicy::Exact,
            deadline_ms: 100,
            retry: RetryPolicy::None,
            destination: Destination::Broadcast,
            message_id: MessageId::new([9; 16]),
            correlation_id: MessageId::absent(),
            status: Status::Success,
            headers: Headers::new(),
            payload: Vec::new(),
        };
        let message_id = MessageId::new([9; 16]);
        broker.begin_delivery(sender, message, 0).unwrap();
        assert!(
            broker
                .acknowledge(first, message_id, AckPolicy::Processed)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            broker
                .acknowledge(second, message_id, AckPolicy::Processed)
                .unwrap(),
            vec![DeliveryEvent::Result {
                sender,
                message_id,
                outcome: DeliveryOutcome::Processed,
            }]
        );

        let message = reliable_message(
            MessageKind::Request,
            Destination::Peer(first),
            10,
            AckPolicy::None,
            RetryPolicy::None,
        );
        let message_id = MessageId::new([10; 16]);
        broker.begin_delivery(sender, message, 0).unwrap();
        assert_eq!(
            broker.disconnect_with_events(first).unwrap(),
            vec![DeliveryEvent::Result {
                sender,
                message_id,
                outcome: DeliveryOutcome::RecipientDisconnected,
            }]
        );
    }
}
