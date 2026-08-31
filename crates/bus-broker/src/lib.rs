#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! Transport-independent broker state.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;

use busd_policy::{Action, Credentials, Policy, Request as PolicyRequest};
use busd_protocol::{
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
    limits: Limits,
    capabilities: Capabilities,
    next_peer: u64,
    peers: BTreeMap<PeerId, Peer>,
    namespaces: BTreeMap<Namespace, PeerId>,
    dbus_names: BTreeMap<String, PeerId>,
    subscriptions: BTreeMap<Channel, BTreeMap<PeerId, Vec<HeaderFilter>>>,
    deliveries: BTreeMap<MessageId, PendingDelivery>,
    events: Vec<LifecycleEvent>,
    metrics: Metrics,
    monitor_events: VecDeque<MonitorEvent>,
    next_monitor_sequence: u64,
}

/// A redacted monitoring record.
///
/// Message payloads, headers, claimed metadata, and authenticated credentials
/// are intentionally absent from this record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MonitorEvent {
    /// Monotonic broker-local record sequence.
    pub sequence: u64,
    /// The observed broker transition.
    pub kind: MonitorKind,
    /// Peer responsible for the transition, where applicable.
    pub peer: PeerId,
    /// The operation target, where applicable.
    pub target: Option<String>,
    /// Stable logical message identifier, where applicable.
    pub message_id: Option<MessageId>,
    /// Opaque payload length; payload bytes are never exposed.
    pub payload_bytes: usize,
}

/// The type of a redacted monitoring record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MonitorKind {
    /// A peer connected.
    Connected,
    /// A peer disconnected.
    Disconnected,
    /// A namespace was claimed.
    NamespaceClaimed,
    /// A channel was subscribed.
    Subscribed,
    /// A message was routed.
    Delivered,
    /// A receiver acknowledged a message.
    Acknowledged,
    /// A reliable delivery timed out.
    TimedOut,
    /// A reliable delivery was retried.
    Retried,
    /// A best-effort signal had no recipients.
    DroppedSignal,
}

/// Broker counters suitable for structured operational reporting.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Metrics {
    /// Messages selected for delivery.
    pub messages_routed: u64,
    /// Opaque payload bytes selected for delivery.
    pub bytes_routed: u64,
    /// Expired reliable deliveries.
    pub timeouts: u64,
    /// Broker-managed repeated deliveries.
    pub retries: u64,
    /// Best-effort signals with no recipient.
    pub dropped_signals: u64,
}

/// A point-in-time broker metrics report.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MetricsSnapshot {
    /// Connected peer count.
    pub connected_peers: usize,
    /// Claimed namespace count.
    pub namespaces: usize,
    /// Channel subscription count.
    pub subscriptions: usize,
    /// Cumulative broker counters.
    pub totals: Metrics,
}

/// Per-peer resource limits enforced by the broker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limits {
    /// Maximum retained reliable-delivery bytes per sender.
    pub maximum_queued_bytes: usize,
    /// Maximum retained reliable deliveries per sender.
    pub maximum_queued_messages: usize,
    /// Maximum in-flight requests per sender.
    pub maximum_in_flight_requests: usize,
    /// Maximum channel subscriptions per peer.
    pub maximum_subscriptions: usize,
    /// Maximum namespace claims per peer.
    pub maximum_namespace_claims: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            maximum_queued_bytes: 4 * 1_024 * 1_024,
            maximum_queued_messages: 1_024,
            maximum_in_flight_requests: 128,
            maximum_subscriptions: 256,
            maximum_namespace_claims: 64,
        }
    }
}

/// A named broker limit that an operation exceeded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Limit {
    /// Retained reliable-delivery bytes.
    QueuedBytes,
    /// Retained reliable-delivery count.
    QueuedMessages,
    /// Request delivery count.
    InFlightRequests,
    /// Subscription count.
    Subscriptions,
    /// Namespace claim count.
    NamespaceClaims,
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
    priority: u8,
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
        Self::with_limits(policy, Limits::default())
    }

    /// Creates an empty broker with explicit per-peer resource limits.
    #[must_use]
    pub fn with_limits(policy: P, limits: Limits) -> Self {
        Self {
            policy,
            limits,
            capabilities: Capabilities::new(),
            next_peer: 1,
            peers: BTreeMap::new(),
            namespaces: BTreeMap::new(),
            dbus_names: BTreeMap::new(),
            subscriptions: BTreeMap::new(),
            deliveries: BTreeMap::new(),
            events: Vec::new(),
            metrics: Metrics::default(),
            monitor_events: VecDeque::new(),
            next_monitor_sequence: 1,
        }
    }

    /// Creates an empty broker with the supplied optional capabilities.
    #[must_use]
    pub fn with_capabilities(policy: P, capabilities: Capabilities) -> Self {
        Self {
            policy,
            limits: Limits::default(),
            capabilities,
            next_peer: 1,
            peers: BTreeMap::new(),
            namespaces: BTreeMap::new(),
            dbus_names: BTreeMap::new(),
            subscriptions: BTreeMap::new(),
            deliveries: BTreeMap::new(),
            events: Vec::new(),
            metrics: Metrics::default(),
            monitor_events: VecDeque::new(),
            next_monitor_sequence: 1,
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
        self.record_monitor(MonitorKind::Connected, id, None, None, 0);
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
        self.dbus_names.retain(|_, owner| *owner != peer);
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
        self.record_monitor(MonitorKind::Disconnected, peer, None, None, 0);
        Ok(())
    }

    /// Exclusively claims a namespace for a connected peer.
    pub fn claim(&mut self, peer: PeerId, namespace: Namespace) -> Result<(), Error> {
        self.authorize(peer, Action::ClaimNamespace(namespace.clone()))?;
        if self
            .namespaces
            .values()
            .filter(|owner| **owner == peer)
            .count()
            >= self.limits.maximum_namespace_claims
        {
            return Err(Error::LimitExceeded(Limit::NamespaceClaims));
        }
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
        self.record_monitor(
            MonitorKind::NamespaceClaimed,
            peer,
            Some(namespace.to_string()),
            None,
            0,
        );
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
        let already_subscribed = self
            .subscriptions
            .get(&channel)
            .is_some_and(|subscribers| subscribers.contains_key(&peer));
        if !already_subscribed
            && self
                .subscriptions
                .values()
                .filter(|subscribers| subscribers.contains_key(&peer))
                .count()
                >= self.limits.maximum_subscriptions
        {
            return Err(Error::LimitExceeded(Limit::Subscriptions));
        }
        self.subscriptions
            .entry(channel.clone())
            .or_default()
            .insert(peer, filters);
        self.record_monitor(
            MonitorKind::Subscribed,
            peer,
            Some(channel.to_string()),
            None,
            0,
        );
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

    /// Registers a D-Bus well-known name for a connected peer.
    ///
    /// D-Bus registration is an optional compatibility operation. It does not
    /// create a BUS namespace or imply any semantic method translation.
    pub fn register_dbus_name(&mut self, peer: PeerId, name: String) -> Result<(), Error> {
        validate_dbus_name(&name).map_err(|_| Error::InvalidDbusName(name.clone()))?;
        self.authorize(peer, Action::RegisterDbusName(name.clone()))?;
        if let Some(owner) = self.dbus_names.get(&name) {
            return Err(Error::DbusNameAlreadyOwned {
                name,
                owner: *owner,
            });
        }
        self.dbus_names.insert(name, peer);
        Ok(())
    }

    /// Returns the peer currently registered for a D-Bus name, if any.
    #[must_use]
    pub fn dbus_name_owner(&self, name: &str) -> Option<PeerId> {
        self.dbus_names.get(name).copied()
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
        let priority = message_priority(headers)?;
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
        let payload_bytes = payload_bytes(&message);
        self.metrics.messages_routed = self
            .metrics
            .messages_routed
            .saturating_add(recipients.len() as u64);
        self.metrics.bytes_routed = self
            .metrics
            .bytes_routed
            .saturating_add((payload_bytes as u64).saturating_mul(recipients.len() as u64));
        self.record_monitor(
            if recipients.is_empty() {
                MonitorKind::DroppedSignal
            } else {
                MonitorKind::Delivered
            },
            sender,
            Some(destination_label(destination)),
            Some(*message_id),
            payload_bytes,
        );
        if recipients.is_empty() && *kind == MessageKind::Signal {
            self.metrics.dropped_signals = self.metrics.dropped_signals.saturating_add(1);
        }
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
            self.ensure_delivery_capacity(sender, &message, *kind == MessageKind::Request)?;
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
                    priority,
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
        let (complete, sender, outcome, remove) = {
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
            if !pending.recipients.contains(&peer)
                || !acknowledgement_satisfies(*ack_policy, policy)
            {
                return Err(Error::UnexpectedAcknowledgement(message_id));
            }
            pending.acknowledgements.insert(peer);
            let complete = !pending.acknowledgement_complete
                && requirement_met(
                    *ack_requirement,
                    pending.acknowledgements.len(),
                    pending.recipients.len(),
                );
            let outcome = match ack_policy {
                AckPolicy::Received => DeliveryOutcome::Received,
                AckPolicy::Processed => DeliveryOutcome::Processed,
                AckPolicy::None | AckPolicy::Accepted => {
                    return Err(Error::UnexpectedAcknowledgement(message_id));
                }
            };
            if complete {
                pending.acknowledgement_complete = true;
            }
            (
                complete,
                pending.sender,
                outcome,
                *kind != MessageKind::Request,
            )
        };
        self.record_monitor(MonitorKind::Acknowledged, peer, None, Some(message_id), 0);
        if !complete {
            return Ok(Vec::new());
        }
        if remove {
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
        let mut retries = Vec::new();
        for message_id in ids {
            let Some(pending) = self.deliveries.get(&message_id) else {
                continue;
            };
            if now_ms >= pending.deadline_ms {
                let sender = pending.sender;
                self.deliveries.remove(&message_id);
                self.metrics.timeouts = self.metrics.timeouts.saturating_add(1);
                self.record_monitor(MonitorKind::TimedOut, sender, None, Some(message_id), 0);
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
            let sender = pending.sender;
            let payload_bytes = delivery_bytes(&pending.message);
            retries.push((
                pending.priority,
                DeliveryEvent::Deliver {
                    sender,
                    recipients: pending
                        .recipients
                        .difference(&pending.acknowledgements)
                        .copied()
                        .collect(),
                    message: pending.message.clone(),
                },
            ));
            let _ = pending;
            self.metrics.retries = self.metrics.retries.saturating_add(1);
            self.record_monitor(
                MonitorKind::Retried,
                sender,
                None,
                Some(message_id),
                payload_bytes,
            );
        }
        retries.sort_by(|(left, _), (right, _)| right.cmp(left));
        events.extend(retries.into_iter().map(|(_, event)| event));
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

    /// Returns the enforced per-peer resource limits.
    #[must_use]
    pub const fn limits(&self) -> Limits {
        self.limits
    }

    /// Drains lifecycle events in generation order.
    pub fn drain_events(&mut self) -> Vec<LifecycleEvent> {
        std::mem::take(&mut self.events)
    }

    /// Returns redacted monitoring records after authorizing monitor access.
    pub fn monitor_events(&self, peer: PeerId) -> Result<Vec<MonitorEvent>, Error> {
        self.authorize(peer, Action::Monitor)?;
        Ok(self.monitor_events.iter().cloned().collect())
    }

    /// Returns a current metrics snapshot without exposing message contents.
    #[must_use]
    pub fn metrics(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            connected_peers: self.peers.len(),
            namespaces: self.namespaces.len(),
            subscriptions: self.subscriptions.values().map(BTreeMap::len).sum(),
            totals: self.metrics,
        }
    }

    fn record_monitor(
        &mut self,
        kind: MonitorKind,
        peer: PeerId,
        target: Option<String>,
        message_id: Option<MessageId>,
        payload_bytes: usize,
    ) {
        const MAX_MONITOR_EVENTS: usize = 1_024;
        if self.monitor_events.len() == MAX_MONITOR_EVENTS {
            self.monitor_events.pop_front();
        }
        self.monitor_events.push_back(MonitorEvent {
            sequence: self.next_monitor_sequence,
            kind,
            peer,
            target,
            message_id,
            payload_bytes,
        });
        self.next_monitor_sequence = self.next_monitor_sequence.saturating_add(1);
    }

    fn credentials(&self, peer: PeerId) -> Result<Credentials, Error> {
        self.peers
            .get(&peer)
            .map(|peer| peer.credentials.clone())
            .ok_or(Error::UnknownPeer(peer))
    }

    fn ensure_delivery_capacity(
        &self,
        sender: PeerId,
        message: &Frame,
        is_request: bool,
    ) -> Result<(), Error> {
        let queued: Vec<_> = self
            .deliveries
            .values()
            .filter(|pending| pending.sender == sender)
            .collect();
        if queued.len() >= self.limits.maximum_queued_messages {
            return Err(Error::LimitExceeded(Limit::QueuedMessages));
        }
        let used_bytes: usize = queued
            .iter()
            .map(|pending| delivery_bytes(&pending.message))
            .sum();
        if used_bytes.saturating_add(delivery_bytes(message)) > self.limits.maximum_queued_bytes {
            return Err(Error::LimitExceeded(Limit::QueuedBytes));
        }
        if is_request
            && queued
                .iter()
                .filter(|pending| {
                    matches!(
                        &pending.message,
                        Frame::Message {
                            kind: MessageKind::Request,
                            ..
                        }
                    )
                })
                .count()
                >= self.limits.maximum_in_flight_requests
        {
            return Err(Error::LimitExceeded(Limit::InFlightRequests));
        }
        Ok(())
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

fn message_priority(headers: &Headers) -> Result<u8, Error> {
    match headers.get("priority") {
        None => Ok(0),
        Some(HeaderValue::Unsigned(priority @ 0..=7)) => Ok(*priority as u8),
        Some(_) => Err(Error::InvalidPriority),
    }
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

fn delivery_bytes(message: &Frame) -> usize {
    match message {
        Frame::Message {
            headers, payload, ..
        } => payload.len().saturating_add(
            headers
                .iter()
                .map(|(name, value)| {
                    name.len()
                        + match value {
                            HeaderValue::Text(value) => value.len(),
                            HeaderValue::Unsigned(_) => std::mem::size_of::<u64>(),
                            HeaderValue::Boolean(_) => 1,
                            HeaderValue::Binary(value) => value.len(),
                        }
                })
                .sum::<usize>(),
        ),
        _ => 0,
    }
}

fn payload_bytes(message: &Frame) -> usize {
    match message {
        Frame::Message { payload, .. } => payload.len(),
        _ => 0,
    }
}

fn destination_label(destination: &Destination) -> String {
    match destination {
        Destination::Broker => "broker".into(),
        Destination::Peer(peer) => peer.to_string(),
        Destination::Namespace(namespace) => namespace.to_string(),
        Destination::Channel(channel) => channel.to_string(),
        Destination::Broadcast => "broadcast".into(),
        Destination::ClientId {
            client_id,
            selection,
        } => format!("{client_id}:{selection:?}"),
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
    /// A D-Bus name already has a connected owner.
    DbusNameAlreadyOwned {
        /// The requested D-Bus name.
        name: String,
        /// Its current owner.
        owner: PeerId,
    },
    /// A D-Bus name does not follow the well-known-name grammar.
    InvalidDbusName(String),
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
    /// A peer exceeded an enforced resource limit.
    LimitExceeded(Limit),
    /// A message used an invalid scheduling priority.
    InvalidPriority,
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
            Self::DbusNameAlreadyOwned { name, owner } => {
                write!(formatter, "D-Bus name {name} is already owned by {owner}")
            }
            Self::InvalidDbusName(name) => write!(formatter, "invalid D-Bus name {name}"),
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
            Self::LimitExceeded(limit) => write!(formatter, "broker limit exceeded: {limit:?}"),
            Self::InvalidPriority => {
                formatter.write_str("message priority must be an unsigned value from 0 through 7")
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

fn validate_dbus_name(name: &str) -> Result<(), ()> {
    if name.is_empty() || name.len() > 255 {
        return Err(());
    }
    for component in name.split('.') {
        let mut bytes = component.bytes();
        let Some(first) = bytes.next() else {
            return Err(());
        };
        if !(first.is_ascii_alphabetic() || first == b'_')
            || !bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err(());
        }
    }
    Ok(())
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::*;
    use busd_policy::{AllowAll, Policy};

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
                headers: [("peer.uid".into(), busd_protocol::HeaderValue::Unsigned(0))].into(),
                ..ClientHello::default()
            },
        );
        assert!(matches!(error, Err(Error::ReservedHeader(header)) if header == "peer.uid"));
    }

    #[test]
    fn dbus_names_are_exclusive_and_released() {
        let mut broker = Broker::new(AllowAll);
        let first = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let second = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();

        broker
            .register_dbus_name(first, "com.example.Service".into())
            .unwrap();
        assert_eq!(broker.dbus_name_owner("com.example.Service"), Some(first));
        assert!(matches!(
            broker.register_dbus_name(second, "com.example.Service".into()),
            Err(Error::DbusNameAlreadyOwned { .. })
        ));
        assert!(matches!(
            broker.register_dbus_name(second, "-invalid.Name".into()),
            Err(Error::InvalidDbusName(_))
        ));

        broker.disconnect(first).unwrap();
        assert_eq!(broker.dbus_name_owner("com.example.Service"), None);
    }

    #[test]
    fn dbus_name_registration_is_policy_gated() {
        struct NoDbus;

        impl Policy for NoDbus {
            fn permits(&self, request: &PolicyRequest<'_>) -> bool {
                !matches!(request.action, Action::RegisterDbusName(_))
            }
        }

        let mut broker = Broker::new(NoDbus);
        let peer = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        assert!(matches!(
            broker.register_dbus_name(peer, "com.example.Service".into()),
            Err(Error::Denied(Action::RegisterDbusName(name))) if name == "com.example.Service"
        ));
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
                headers: [("client.uid".into(), busd_protocol::HeaderValue::Unsigned(0))].into(),
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

    #[test]
    fn limits_isolate_exhausting_peers() {
        let limits = Limits {
            maximum_queued_bytes: 2,
            maximum_queued_messages: 1,
            maximum_in_flight_requests: 1,
            maximum_subscriptions: 1,
            maximum_namespace_claims: 1,
        };
        let mut broker = Broker::with_limits(AllowAll, limits);
        let sender = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let recipient = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let first = Namespace::parse("bus://first").unwrap();
        broker.claim(sender, first).unwrap();
        assert_eq!(
            broker.claim(sender, Namespace::parse("bus://second").unwrap()),
            Err(Error::LimitExceeded(Limit::NamespaceClaims))
        );
        let events = Channel::parse("events").unwrap();
        broker.subscribe(sender, events).unwrap();
        assert_eq!(
            broker.subscribe(sender, Channel::parse("other").unwrap()),
            Err(Error::LimitExceeded(Limit::Subscriptions))
        );
        let message = reliable_message(
            MessageKind::Request,
            Destination::Peer(recipient),
            41,
            AckPolicy::None,
            RetryPolicy::None,
        );
        broker.begin_delivery(sender, message, 0).unwrap();
        let next = reliable_message(
            MessageKind::Request,
            Destination::Peer(recipient),
            42,
            AckPolicy::None,
            RetryPolicy::None,
        );
        assert!(matches!(
            broker.begin_delivery(sender, next, 0),
            Err(Error::LimitExceeded(Limit::QueuedMessages))
        ));
        assert_eq!(broker.peer(recipient).unwrap().id, recipient);
    }

    #[test]
    fn privileged_monitoring_is_redacted_and_counted() {
        let mut broker = Broker::new(AllowAll);
        let sender = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let recipient = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let message = Frame::Message {
            kind: MessageKind::Signal,
            ack_policy: AckPolicy::None,
            ack_requirement: AckRequirement::None,
            request_policy: RequestPolicy::Exact,
            deadline_ms: 0,
            retry: RetryPolicy::None,
            destination: Destination::Peer(recipient),
            message_id: MessageId::new([51; 16]),
            correlation_id: MessageId::absent(),
            status: Status::Success,
            headers: [("secret".into(), HeaderValue::Text("do-not-expose".into()))].into(),
            payload: b"opaque".to_vec(),
        };
        broker.begin_delivery(sender, message, 0).unwrap();
        let records = broker.monitor_events(sender).unwrap();
        assert!(records.iter().any(|record| {
            record.kind == MonitorKind::Delivered
                && record.payload_bytes == 6
                && record.target.as_deref() == Some(&recipient.to_string())
        }));
        assert_eq!(broker.metrics().totals.messages_routed, 1);
        assert_eq!(broker.metrics().totals.bytes_routed, 6);

        struct NoMonitor;
        impl Policy for NoMonitor {
            fn permits(&self, request: &PolicyRequest<'_>) -> bool {
                !matches!(request.action, Action::Monitor)
            }
        }
        let mut denied = Broker::new(NoMonitor);
        let peer = denied
            .connect(credentials(), ClientHello::default())
            .unwrap();
        assert!(matches!(
            denied.monitor_events(peer),
            Err(Error::Denied(Action::Monitor))
        ));
    }

    #[test]
    fn retries_are_priority_scheduled_after_authorization() {
        let mut broker = Broker::new(AllowAll);
        let sender = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let recipient = broker
            .connect(credentials(), ClientHello::default())
            .unwrap();
        let mut low = reliable_message(
            MessageKind::Signal,
            Destination::Peer(recipient),
            61,
            AckPolicy::Processed,
            RetryPolicy::Exponential {
                initial_backoff_ms: 10,
                max_attempts: 2,
            },
        );
        let mut high = reliable_message(
            MessageKind::Signal,
            Destination::Peer(recipient),
            62,
            AckPolicy::Processed,
            RetryPolicy::Exponential {
                initial_backoff_ms: 10,
                max_attempts: 2,
            },
        );
        if let Frame::Message { headers, .. } = &mut low {
            headers.insert("priority".into(), HeaderValue::Unsigned(1));
        }
        if let Frame::Message { headers, .. } = &mut high {
            headers.insert("priority".into(), HeaderValue::Unsigned(7));
        }
        broker.begin_delivery(sender, low, 0).unwrap();
        broker.begin_delivery(sender, high, 0).unwrap();
        let retries = broker.tick(10);
        let ids: Vec<_> = retries
            .into_iter()
            .map(|event| match event {
                DeliveryEvent::Deliver {
                    message: Frame::Message { message_id, .. },
                    ..
                } => message_id,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(
            ids,
            vec![MessageId::new([62; 16]), MessageId::new([61; 16])]
        );

        let mut invalid = reliable_message(
            MessageKind::Signal,
            Destination::Peer(recipient),
            63,
            AckPolicy::Processed,
            RetryPolicy::None,
        );
        if let Frame::Message { headers, .. } = &mut invalid {
            headers.insert("priority".into(), HeaderValue::Unsigned(8));
        }
        assert_eq!(
            broker.begin_delivery(sender, invalid, 0),
            Err(Error::InvalidPriority)
        );
    }

    #[test]
    fn arbitrary_state_transitions_do_not_panic() {
        let result = std::panic::catch_unwind(|| {
            let mut broker = Broker::new(AllowAll);
            let peers: Vec<_> = (0..4)
                .map(|_| {
                    broker
                        .connect(credentials(), ClientHello::default())
                        .unwrap()
                })
                .collect();
            let mut seed = 0x9c4d_6e8f_1234_5678_u64;
            for index in 0..1_024_u64 {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                let peer = peers[(seed as usize) % peers.len()];
                let other = peers[((seed >> 8) as usize) % peers.len()];
                match (seed >> 16) % 5 {
                    0 => {
                        let _ = broker.claim(
                            peer,
                            Namespace::parse(format!("bus://service-{}", seed % 8)).unwrap(),
                        );
                    }
                    1 => {
                        let _ = broker.subscribe(
                            peer,
                            Channel::parse(format!("events-{}", seed % 8)).unwrap(),
                        );
                    }
                    2 => {
                        let channel = Channel::parse(format!("events-{}", seed % 8)).unwrap();
                        let _ = broker.unsubscribe(peer, &channel);
                    }
                    3 => {
                        let _ = broker.route(peer, &Destination::Peer(other), &Headers::new());
                    }
                    _ => {
                        let _ = broker.begin_delivery(
                            peer,
                            Frame::Message {
                                kind: MessageKind::Signal,
                                ack_policy: AckPolicy::None,
                                ack_requirement: AckRequirement::None,
                                request_policy: RequestPolicy::Exact,
                                deadline_ms: 0,
                                retry: RetryPolicy::None,
                                destination: Destination::Peer(other),
                                message_id: MessageId::new(
                                    index.to_be_bytes().repeat(2).try_into().unwrap(),
                                ),
                                correlation_id: MessageId::absent(),
                                status: Status::Success,
                                headers: Headers::new(),
                                payload: Vec::new(),
                            },
                            index,
                        );
                    }
                }
            }
        });
        assert!(result.is_ok());
    }
}
