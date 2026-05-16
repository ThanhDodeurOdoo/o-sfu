//! Consumer-side room state transitions.
//!
//! This file applies receiver download intent after the application layer has
//! translated any compatibility shape into source-keyed
//! [`SourceSubscriptionIntent`] values. The state layer persists those intents
//! by target user and stream id, then plans route activity changes and
//! bootstraps for the effect layer.
//!
//! The subscription state never decides what "camera" or "screen" means. It
//! only knows whether a receiver wants a generic source active and which layout
//! preference should be associated with that source.

use std::collections::BTreeMap;

use o_sfu_router::{
    ConsumerCapability, ConsumerRouteState as RouterConsumerRouteState,
    MediaKind as RouterMediaKind, MediaStream as RouterRtpParameters, can_consume,
    negotiate_consumer_rtp_parameters,
};
use tracing::{error, warn};

use super::{
    super::{
        super::{RoomEventRequest, outbound::OutboundSender, topology::RoutedProducerId},
        ids::{ConsumerRuntimeId, ProducerRuntimeId},
        shared::{
            ConsumerKey, ConsumerRouteTransportRef, ConsumerState, PublishedProducer, RoomState,
            SourceKey,
        },
    },
    relay::RelayRouteEffect,
};
use crate::runtime::{
    ConnectionId, UserId,
    media_transport::TransportMediaId,
    source_model::{
        ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceId,
        SourceSubscriptionIntent, UserStreamId,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Accepted consumer-route update that should be fanned out after state commit.
///
/// The route update only represents the receiver-local route choice. Producer
/// activity is handled through producer state and is combined with this value
/// when callers ask for the effective route.
pub(in crate::runtime::room) struct ConsumerRouteUpdate {
    route: ConsumerRouteTransportRef,
    stream_id: UserStreamId,
    media_kind: RouterMediaKind,
    active: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Effective room-level route state exposed to compatibility callers.
///
/// This is not the same type as the pure router's
/// [`RouterConsumerRouteState`]. The router type stores only the
/// receiver-local route choice. This value folds together producer activity,
/// consumer source selection and whether the consumer route exists at all.
pub enum ConsumerRouteState {
    /// No committed consumer route exists for the requested source.
    Absent,
    /// A route exists, but either the producer or the consumer-local selection
    /// currently prevents forwarding.
    Inactive,
    /// A committed route exists and all room-level activity inputs allow it.
    Active,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::room) struct ConsumerKeyframeRefreshTarget {
    pub(in crate::runtime::room) consumer_media: TransportMediaId,
    pub(in crate::runtime::room) producer_user_id: UserId,
    pub(in crate::runtime::room) producer_connection_id: ConnectionId,
    pub(in crate::runtime::room) source_media: TransportMediaId,
}

#[derive(Debug, Default)]
pub(in crate::runtime::room) struct PlannedSubscriptionChange {
    route_updates: Vec<ConsumerRouteUpdate>,
    bootstraps: Vec<PlannedConsumerBootstrap>,
    relay_effects: Vec<RelayRouteEffect>,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct PendingConsumerBootstrapTarget {
    pub(super) consumer_user_id: UserId,
    pub(super) consumer_connection_id: ConnectionId,
    producer: ConsumerBootstrapProducerSnapshot,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct ConsumerBootstrapProducerSnapshot {
    source_id: PublishedSourceId,
    owner_user_id: UserId,
    owner_connection_id: ConnectionId,
    producer_id: ProducerRuntimeId,
    stream_id: UserStreamId,
    media_kind: RouterMediaKind,
    transport_media_id: TransportMediaId,
    routed_producer_id: Option<RoutedProducerId>,
    active: Option<bool>,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct PreparedConsumerBootstrap {
    consumer_rtp_parameters: RouterRtpParameters,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct PendingConsumerBootstrap {
    consumer_key: ConsumerKey,
    sender: OutboundSender,
    bootstrap: RemoteTrackBootstrap,
    consumer_active: bool,
    producer: ConsumerBootstrapProducerSnapshot,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct PlannedConsumerBootstrap {
    target: PendingConsumerBootstrapTarget,
    prepared: PreparedConsumerBootstrap,
    pending_bootstrap: PendingConsumerBootstrap,
    relay_effects: Vec<RelayRouteEffect>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteTrackBootstrap {
    consumer_id: ConsumerRuntimeId,
    media_kind: RouterMediaKind,
    mid: String,
    producer_id: ProducerRuntimeId,
    rtp_parameters: RouterRtpParameters,
    source_descriptor: PublishedSourceDescriptor,
    user_id: UserId,
    active: bool,
    stream_id: UserStreamId,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime::room) enum ConsumerBootstrapOrigin {
    LateJoin,
    Publish,
    Subscribe,
}

impl RoomState {
    pub fn plan_subscription_change(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
    ) -> PlannedSubscriptionChange {
        if self.user_for_connection(user_id, connection_id).is_none() {
            return PlannedSubscriptionChange::default();
        }
        self.persist_source_subscription_intents(user_id, target_user_id, intents);
        let (route_updates, relay_effects) =
            self.apply_subscription_route_updates(user_id, connection_id, target_user_id, intents);
        let bootstraps = self.plan_consumer_bootstraps_for_targets(
            self.collect_missing_consumer_targets_for_peer(user_id, connection_id, target_user_id),
        );
        PlannedSubscriptionChange {
            route_updates,
            bootstraps,
            relay_effects,
        }
    }

    fn persist_source_subscription_intents(
        &mut self,
        user_id: &UserId,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
    ) {
        let Some(user) = self.users.get_mut(user_id) else {
            return;
        };
        let existing_states = user
            .desired_source_subscriptions
            .entry(target_user_id.clone())
            .or_default();
        for (stream_id, update) in intents {
            existing_states
                .entry(stream_id.clone())
                .and_modify(|intent| intent.merge(*update))
                .or_insert(*update);
        }
        existing_states.retain(|_, intent| !intent.is_empty());
        if existing_states.is_empty() {
            user.desired_source_subscriptions.remove(target_user_id);
        }
    }

    pub fn plan_missing_consumer_bootstraps_for_connection(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<Vec<PlannedConsumerBootstrap>> {
        let user = self.users.get(user_id)?;
        if user.connection_id != connection_id {
            return None;
        }
        if !user.negotiation.can_consume() {
            return Some(Vec::new());
        }
        Some(self.plan_consumer_bootstraps_for_targets(
            self.collect_missing_consumer_targets(user_id, connection_id),
        ))
    }

    pub fn plan_consumer_bootstraps_for_targets(
        &mut self,
        targets: Vec<PendingConsumerBootstrapTarget>,
    ) -> Vec<PlannedConsumerBootstrap> {
        targets
            .into_iter()
            .filter_map(|target| self.plan_consumer_bootstrap(&target))
            .collect()
    }

    fn apply_subscription_route_updates(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
    ) -> (Vec<ConsumerRouteUpdate>, Vec<RelayRouteEffect>) {
        let mut accepted_updates = Vec::new();
        let mut relay_effects = Vec::new();
        for (stream_id, intent) in intents {
            let Some(active) = intent.active() else {
                continue;
            };
            let Some(source_id) = self.source_id_for_subscription(target_user_id, stream_id) else {
                continue;
            };
            let Some(source) = self.media.sources.get(&source_id) else {
                continue;
            };
            let media_kind = source.media_kind();
            let source_user_id = source.owner().user_id().clone();
            let key = ConsumerKey::new(user_id, source_id);
            self.set_consumer_source_selection(&key, active);
            let Some(current_consumer_state) = self.media.consumer_index.get(&key).copied() else {
                continue;
            };
            if current_consumer_state.consumer_connection_id != connection_id {
                continue;
            }
            let route_state = if active {
                RouterConsumerRouteState::Active
            } else {
                RouterConsumerRouteState::Paused
            };
            if self
                .topology
                .set_consumer_route_state(current_consumer_state.routed_consumer_id, route_state)
                .is_err()
            {
                error!(
                    ?user_id,
                    ?target_user_id,
                    stream_id = %stream_id,
                    "failed to set consumer pause state in room router"
                );
                continue;
            }
            accepted_updates.push(ConsumerRouteUpdate {
                route: ConsumerRouteTransportRef::new(
                    &key,
                    current_consumer_state,
                    &source_user_id,
                ),
                stream_id: stream_id.clone(),
                media_kind,
                active,
            });
            relay_effects.extend(self.media.relay_routes.set_consumer_active(
                user_id,
                connection_id,
                source_id,
                active,
            ));
        }
        (accepted_updates, relay_effects)
    }

    fn collect_missing_consumer_targets(
        &self,
        user_id: &UserId,
        consumer_connection_id: ConnectionId,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        self.collect_missing_consumer_targets_where(user_id, consumer_connection_id, |_| true)
    }

    fn collect_missing_consumer_targets_for_peer(
        &self,
        user_id: &UserId,
        consumer_connection_id: ConnectionId,
        target_user_id: &UserId,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        self.collect_missing_consumer_targets_where(user_id, consumer_connection_id, |producer| {
            producer.owner_user_id == *target_user_id
        })
    }

    fn collect_missing_consumer_targets_where(
        &self,
        user_id: &UserId,
        consumer_connection_id: ConnectionId,
        should_include: impl Fn(&PublishedProducer) -> bool,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        self.media
            .producers
            .iter()
            .filter_map(|(producer_id, producer)| {
                if !should_include(producer) {
                    return None;
                }
                self.pending_consumer_target(
                    user_id,
                    consumer_connection_id,
                    *producer_id,
                    producer,
                )
            })
            .collect()
    }

    fn pending_consumer_target(
        &self,
        user_id: &UserId,
        consumer_connection_id: ConnectionId,
        producer_id: ProducerRuntimeId,
        producer: &PublishedProducer,
    ) -> Option<PendingConsumerBootstrapTarget> {
        let transport_media_id = producer.transport_media_id?;
        if producer.owner_user_id == *user_id {
            return None;
        }
        let consumer_key = ConsumerKey::new(user_id, producer.source_id);
        if self.media.consumer_bootstrap_exists(&consumer_key) {
            return None;
        }
        Some(PendingConsumerBootstrapTarget::new(
            user_id.clone(),
            consumer_connection_id,
            ConsumerBootstrapProducerSnapshot::pending(
                producer.source_id,
                producer.owner_user_id.clone(),
                producer.owner_connection_id,
                producer_id,
                producer.stream_id.clone(),
                producer.media_kind,
                transport_media_id,
            ),
        ))
    }

    fn source_id_for_subscription(
        &self,
        producer_user_id: &UserId,
        stream_id: &UserStreamId,
    ) -> Option<PublishedSourceId> {
        self.media
            .source_ids_by_owner_stream
            .get(&SourceKey::new(producer_user_id, stream_id))
            .copied()
    }

    fn set_consumer_source_selection(&mut self, key: &ConsumerKey, active: bool) {
        self.media.set_consumer_source_selection(key, active);
    }

    fn plan_consumer_bootstrap(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
    ) -> Option<PlannedConsumerBootstrap> {
        let (sender, client_capabilities) = {
            let user = self.users.get(&target.consumer_user_id)?;
            if user.connection_id != target.consumer_connection_id
                || !user.negotiation.can_consume()
            {
                return None;
            }
            (
                user.sender.clone(),
                user.parsed_client_rtp_capabilities.clone()?,
            )
        };
        let producer = self.media.producers.get(&target.producer.producer_id)?;
        if !target.producer.matches_pending_producer(producer) {
            return None;
        }
        let source_descriptor = self.media.sources.get(&target.producer.source_id)?.clone();
        let consumer_key = ConsumerKey::new(&target.consumer_user_id, target.source_id());
        if self.media.consumer_bootstrap_exists(&consumer_key) {
            return None;
        }
        let consumer_active = self
            .consumer_source_selection_for_bootstrap(target)
            .active();
        let producer_consumable_rtp_parameters = producer.consumable_rtp_parameters.clone();
        let prepared_producer = target
            .producer
            .with_commit_snapshot(producer.routed_producer_id, producer.active);
        if !can_consume(&producer_consumable_rtp_parameters, &client_capabilities) {
            return None;
        }
        let negotiated_rtp_parameters = negotiate_consumer_rtp_parameters(
            &producer_consumable_rtp_parameters,
            &client_capabilities,
        )
        .ok()?;
        self.media.ensure_consumer_source_selection(
            &consumer_key,
            ConsumerSourceSelection::open(consumer_active),
        );
        self.media
            .reserve_pending_consumer_bootstrap(consumer_key.clone());
        let consumer_id = ConsumerRuntimeId::allocate(&mut self.next_consumer_id);
        let relay_effects = self.reserve_relay_route(target, consumer_active);
        Some(PlannedConsumerBootstrap {
            target: target.clone(),
            prepared: PreparedConsumerBootstrap {
                consumer_rtp_parameters: negotiated_rtp_parameters.clone(),
            },
            pending_bootstrap: PendingConsumerBootstrap {
                consumer_key,
                sender,
                bootstrap: RemoteTrackBootstrap {
                    consumer_id,
                    media_kind: prepared_producer.media_kind,
                    mid: negotiated_rtp_parameters
                        .mid()
                        .map_or_else(|| consumer_id.into_wire_id(), ToOwned::to_owned),
                    producer_id: prepared_producer.producer_id,
                    rtp_parameters: negotiated_rtp_parameters,
                    source_descriptor,
                    user_id: prepared_producer.owner_user_id.clone(),
                    active: prepared_producer.active.unwrap_or(true),
                    stream_id: prepared_producer.stream_id.clone(),
                },
                consumer_active,
                producer: prepared_producer,
            },
            relay_effects,
        })
    }

    fn reserve_relay_route(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
        consumer_active: bool,
    ) -> Vec<RelayRouteEffect> {
        let Some((source_connection, source_media, target_worker)) =
            self.relay_route_for_target(target)
        else {
            return Vec::new();
        };
        self.media.relay_routes.reserve_consumer(
            target,
            source_connection,
            source_media,
            target_worker,
            consumer_active,
        )
    }

    fn relay_route_for_target(
        &self,
        target: &PendingConsumerBootstrapTarget,
    ) -> Option<(ConnectionId, TransportMediaId, usize)> {
        let source_worker = self
            .topology
            .home_placement_for_user(target.producer_user_id())?
            .media_worker;
        let target_worker = self
            .topology
            .home_placement_for_user(target.consumer_user_id())?
            .media_worker;
        if source_worker == target_worker {
            return None;
        }
        Some((
            target.producer_connection_id(),
            target.transport_media_id(),
            target_worker,
        ))
    }

    fn consumer_source_selection_for_bootstrap(
        &self,
        target: &PendingConsumerBootstrapTarget,
    ) -> ConsumerSourceSelection {
        let consumer_key = ConsumerKey::new(&target.consumer_user_id, target.source_id());
        self.media
            .consumer_source_selections
            .get(&consumer_key)
            .copied()
            .unwrap_or_else(|| {
                ConsumerSourceSelection::open(self.desired_source_subscription_active(
                    &target.consumer_user_id,
                    target.producer_user_id(),
                    target.stream_id(),
                ))
            })
    }

    pub fn commit_consumer_bootstrap(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
        mut pending: PendingConsumerBootstrap,
        consumer_transport_media_id: TransportMediaId,
        consumer_mid: Option<String>,
    ) -> Option<(OutboundSender, RemoteTrackBootstrap, bool)> {
        self.media
            .remove_pending_consumer_bootstrap(&pending.consumer_key);
        let user = self.users.get(&target.consumer_user_id)?;
        if user.connection_id != target.consumer_connection_id || !user.negotiation.can_consume() {
            return None;
        }
        let producer = self.media.producers.get(&pending.producer.producer_id)?;
        if !pending.producer.matches_committed_producer(producer) {
            return None;
        }
        if self
            .media
            .consumer_index
            .contains_key(&pending.consumer_key)
        {
            return None;
        }
        self.media.ensure_consumer_source_selection(
            &pending.consumer_key,
            ConsumerSourceSelection::open(pending.consumer_active),
        );
        let initial_route_state = if pending.consumer_active {
            RouterConsumerRouteState::Active
        } else {
            RouterConsumerRouteState::Paused
        };
        let routed_consumer_id = match self.topology.add_consumer_with_route_state(
            &target.consumer_user_id,
            pending.producer.routed_producer_id?,
            pending.producer.media_kind,
            ConsumerCapability::Compatible,
            initial_route_state,
        ) {
            Ok(id) => id,
            Err(error) => {
                warn!(
                    consumer_user_id = ?target.consumer_user_id,
                    producer_id = %pending.producer.producer_id,
                    ?error,
                    "router rejected consumer creation"
                );
                return None;
            }
        };
        if let Some(consumer_mid) = consumer_mid {
            pending.bootstrap.mid = consumer_mid;
        }
        let consumer_key = pending.consumer_key;
        self.media.insert_consumer_route(
            consumer_key,
            ConsumerState {
                routed_consumer_id,
                consumer_connection_id: target.consumer_connection_id,
                source_connection_id: pending.producer.owner_connection_id,
                source_media: target.transport_media_id(),
                consumer_media: consumer_transport_media_id,
            },
        );
        Some((pending.sender, pending.bootstrap, pending.consumer_active))
    }

    pub fn release_pending_consumer_bootstrap(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
    ) -> Vec<RelayRouteEffect> {
        let consumer_key = ConsumerKey::new(&target.consumer_user_id, target.source_id());
        self.media.remove_pending_consumer_bootstrap(&consumer_key);
        self.media.relay_routes.release_target(target)
    }

    pub fn desired_source_subscription_active(
        &self,
        user_id: &UserId,
        target_user_id: &UserId,
        stream_id: &UserStreamId,
    ) -> bool {
        self.users
            .get(user_id)
            .and_then(|user| user.desired_source_subscriptions.get(target_user_id))
            .and_then(|states| states.get(stream_id))
            .and_then(|intent| intent.active())
            .unwrap_or(true)
    }

    /// Returns the effective room route state for a source subscription.
    ///
    /// This is a cold-path query for signaling and diagnostics. It resolves the
    /// stream id to current room indexes and combines producer
    /// activity with the receiver-local source selection. Missing users return
    /// `None`, while missing routes for an existing user return
    /// [`ConsumerRouteState::Absent`].
    pub fn consumer_route_state(
        &self,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        stream_id: &UserStreamId,
    ) -> Option<ConsumerRouteState> {
        self.users.get(consumer_user_id)?;
        let Some(source_id) = self.source_id_for_subscription(producer_user_id, stream_id) else {
            return Some(ConsumerRouteState::Absent);
        };
        let consumer_key = ConsumerKey::new(consumer_user_id, source_id);
        if !self.media.consumer_index.contains_key(&consumer_key) {
            return Some(ConsumerRouteState::Absent);
        }
        let Some(producer_id) = self.media.producer_id_by_source_id.get(&source_id).copied() else {
            return Some(ConsumerRouteState::Absent);
        };
        let Some(producer) = self.media.producers.get(&producer_id) else {
            return Some(ConsumerRouteState::Absent);
        };
        let route_active = producer.active
            && self
                .media
                .consumer_source_selections
                .get(&consumer_key)
                .map_or_else(
                    || {
                        self.desired_source_subscription_active(
                            consumer_user_id,
                            producer_user_id,
                            stream_id,
                        )
                    },
                    |selection| selection.active(),
                );
        Some(if route_active {
            ConsumerRouteState::Active
        } else {
            ConsumerRouteState::Inactive
        })
    }

    pub fn active_video_consumer_keyframe_refresh_targets(
        &self,
        consumer_user_id: &UserId,
        consumer_connection_id: ConnectionId,
    ) -> Option<Vec<ConsumerKeyframeRefreshTarget>> {
        let user = self.users.get(consumer_user_id)?;
        if user.connection_id != consumer_connection_id {
            return None;
        }
        Some(
            self.media
                .consumer_keys_for_user(consumer_user_id)
                .into_iter()
                .filter_map(|key| {
                    let consumer_state = self.media.consumer_index.get(&key)?;
                    let source = self.media.sources.get(&key.source_id)?;
                    if key.consumer_user_id != *consumer_user_id
                        || consumer_state.consumer_connection_id != consumer_connection_id
                        || source.media_kind() != RouterMediaKind::Video
                    {
                        return None;
                    }
                    let producer_id = self.media.producer_id_by_source_id.get(&key.source_id)?;
                    let producer = self.media.producers.get(producer_id)?;
                    if !producer.active
                        || !self
                            .media
                            .consumer_source_selections
                            .get(&key)
                            .is_none_or(|selection| selection.active())
                    {
                        return None;
                    }
                    Some(ConsumerKeyframeRefreshTarget {
                        consumer_media: consumer_state.consumer_media,
                        producer_user_id: source.owner().user_id().clone(),
                        producer_connection_id: consumer_state.source_connection_id,
                        source_media: consumer_state.source_media,
                    })
                })
                .collect(),
        )
    }
}

impl PlannedSubscriptionChange {
    pub fn into_parts(
        self,
    ) -> (
        Vec<ConsumerRouteUpdate>,
        Vec<PlannedConsumerBootstrap>,
        Vec<RelayRouteEffect>,
    ) {
        (self.route_updates, self.bootstraps, self.relay_effects)
    }
}

impl ConsumerRouteUpdate {
    pub fn route(&self) -> &ConsumerRouteTransportRef {
        &self.route
    }

    pub fn stream_id(&self) -> &UserStreamId {
        &self.stream_id
    }

    pub const fn media_kind(&self) -> RouterMediaKind {
        self.media_kind
    }

    pub const fn active(&self) -> bool {
        self.active
    }
}

impl PendingConsumerBootstrapTarget {
    pub fn new(
        consumer_user_id: UserId,
        consumer_connection_id: ConnectionId,
        producer: ConsumerBootstrapProducerSnapshot,
    ) -> Self {
        Self {
            consumer_user_id,
            consumer_connection_id,
            producer,
        }
    }

    pub const fn consumer_connection_id(&self) -> ConnectionId {
        self.consumer_connection_id
    }

    pub fn consumer_user_id(&self) -> &UserId {
        &self.consumer_user_id
    }

    pub const fn source_id(&self) -> PublishedSourceId {
        self.producer.source_id
    }

    pub const fn media_kind(&self) -> RouterMediaKind {
        self.producer.media_kind
    }

    pub const fn producer_connection_id(&self) -> ConnectionId {
        self.producer.owner_connection_id
    }

    pub fn producer_user_id(&self) -> &UserId {
        &self.producer.owner_user_id
    }

    pub const fn transport_media_id(&self) -> TransportMediaId {
        self.producer.transport_media_id
    }

    pub fn stream_id(&self) -> &UserStreamId {
        &self.producer.stream_id
    }
}

impl PreparedConsumerBootstrap {
    pub fn consumer_rtp_parameters(&self) -> &RouterRtpParameters {
        &self.consumer_rtp_parameters
    }
}

impl PlannedConsumerBootstrap {
    pub fn into_parts(
        self,
    ) -> (
        PendingConsumerBootstrapTarget,
        PreparedConsumerBootstrap,
        PendingConsumerBootstrap,
        Vec<RelayRouteEffect>,
    ) {
        (
            self.target,
            self.prepared,
            self.pending_bootstrap,
            self.relay_effects,
        )
    }
}

impl ConsumerBootstrapProducerSnapshot {
    pub fn pending(
        source_id: PublishedSourceId,
        owner_user_id: UserId,
        owner_connection_id: ConnectionId,
        producer_id: ProducerRuntimeId,
        stream_id: UserStreamId,
        media_kind: RouterMediaKind,
        transport_media_id: TransportMediaId,
    ) -> Self {
        Self {
            source_id,
            owner_user_id,
            owner_connection_id,
            producer_id,
            stream_id,
            media_kind,
            transport_media_id,
            routed_producer_id: None,
            active: None,
        }
    }

    pub const fn source_id(&self) -> PublishedSourceId {
        self.source_id
    }

    pub fn owner_user_id(&self) -> &UserId {
        &self.owner_user_id
    }

    fn with_commit_snapshot(&self, routed_producer_id: RoutedProducerId, active: bool) -> Self {
        Self {
            source_id: self.source_id,
            owner_user_id: self.owner_user_id.clone(),
            owner_connection_id: self.owner_connection_id,
            producer_id: self.producer_id,
            stream_id: self.stream_id.clone(),
            media_kind: self.media_kind,
            transport_media_id: self.transport_media_id,
            routed_producer_id: Some(routed_producer_id),
            active: Some(active),
        }
    }

    fn matches_pending_producer(&self, producer: &PublishedProducer) -> bool {
        producer.source_id == self.source_id
            && producer.owner_user_id == self.owner_user_id
            && producer.owner_connection_id == self.owner_connection_id
            && producer.stream_id == self.stream_id
            && producer.media_kind == self.media_kind
            && producer.transport_media_id == Some(self.transport_media_id)
    }

    fn matches_committed_producer(&self, producer: &PublishedProducer) -> bool {
        let Some(routed_producer_id) = self.routed_producer_id else {
            return false;
        };
        let Some(active) = self.active else {
            return false;
        };
        self.matches_pending_producer(producer)
            && producer.routed_producer_id == routed_producer_id
            && producer.active == active
    }
}

impl RemoteTrackBootstrap {
    #[must_use]
    pub fn mid(&self) -> &str {
        &self.mid
    }

    #[cfg(test)]
    pub(crate) fn rtp_parameters(&self) -> &RouterRtpParameters {
        &self.rtp_parameters
    }

    #[must_use]
    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }

    #[must_use]
    pub fn source_descriptor(&self) -> &PublishedSourceDescriptor {
        &self.source_descriptor
    }

    #[must_use]
    pub const fn active(&self) -> bool {
        self.active
    }

    #[must_use]
    pub fn stream_id(&self) -> &UserStreamId {
        &self.stream_id
    }

    #[must_use]
    pub fn into_room_event_request(self) -> RoomEventRequest {
        RoomEventRequest::BootstrapRemoteTrack(self)
    }
}
