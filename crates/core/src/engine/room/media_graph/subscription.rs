//! Consumer-side room state transitions.
//!
//! This file applies receiver download intent after the application layer has
//! translated any compatibility shape into source-keyed
//! [`SourceSubscriptionIntent`] values. The state layer persists those intents
//! by target user and stream id, then plans route activity changes and
//! bootstraps for the effect layer.
//!
//! The subscription state never decides what "camera" or "screen" means. It
//! only knows whether a receiver wants a source active and which layout
//! preference should be associated with that source.

use std::collections::{BTreeMap, BTreeSet};

use o_sfu_router::{
    ConsumerCapability, ConsumerRouteState as RouterConsumerRouteState,
    MediaKind as RouterMediaKind, MediaStream as RouterRtpParameters, can_consume,
    negotiate_consumer_rtp_parameters,
};
use tracing::{error, warn};

use super::{
    super::{
        RoomEventRequest, outbound::OutboundSender, state::RoomState, topology::RoutedProducerId,
    },
    ConsumerKey, ConsumerRouteTransportRef, ConsumerRuntimeId, ConsumerState, ProducerRuntimeId,
    PublishedProducer,
    route_graph::RelayRouteEffect,
};
use crate::engine::{
    ConnectionId, MediaWorkerId, UserId,
    media_transport::{RelayRouteActivity, TransportMediaId},
    room::source_policy::VideoAdmissionRank,
    source_model::{
        ConsumerSourceSelection, PolicyPauseReason, PublishedSourceDescriptor, PublishedSourceId,
        SourceRoutePriority, SourceSubscriptionIntent, UserStreamId,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::engine::room) struct ConsumerRouteUpdate {
    pub(in crate::engine::room) route: ConsumerRouteTransportRef,
    pub(in crate::engine::room) stream: UserStreamId,
    pub(in crate::engine::room) kind: RouterMediaKind,
    pub(in crate::engine::room) active: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerRouteState {
    Absent,
    Inactive,
    Active,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerKeyframeRefreshTarget {
    pub consumer_media: TransportMediaId,
    pub producer_user_id: UserId,
    pub producer_connection_id: ConnectionId,
    pub source_media: TransportMediaId,
}

#[derive(Debug, Default)]
pub struct PlannedSubscriptionChange {
    updates: Vec<ConsumerRouteUpdate>,
    bootstraps: Vec<PlannedConsumerBootstrap>,
    relays: Vec<RelayRouteEffect>,
}

#[derive(Debug, Clone)]
pub struct PendingConsumerBootstrapTarget {
    user: UserId,
    connection: ConnectionId,
    producer: ConsumerBootstrapProducerSnapshot,
}

#[derive(Debug, Clone)]
pub struct ConsumerBootstrapProducerSnapshot {
    source_id: PublishedSourceId,
    user: UserId,
    connection: ConnectionId,
    id: ProducerRuntimeId,
    stream: UserStreamId,
    kind: RouterMediaKind,
    media: TransportMediaId,
    routed: Option<RoutedProducerId>,
    active: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct PreparedConsumerBootstrap {
    pub rtp: RouterRtpParameters,
}

#[derive(Debug, Clone)]
pub struct PendingConsumerBootstrap {
    key: ConsumerKey,
    sender: OutboundSender,
    track: RemoteTrackBootstrap,
    selection: ConsumerSourceSelection,
    producer: ConsumerBootstrapProducerSnapshot,
}

#[derive(Debug, Clone)]
pub struct PlannedConsumerBootstrap {
    target: PendingConsumerBootstrapTarget,
    prepared: PreparedConsumerBootstrap,
    pending: PendingConsumerBootstrap,
    relays: Vec<RelayRouteEffect>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteTrackBootstrap {
    consumer: ConsumerRuntimeId,
    kind: RouterMediaKind,
    mid: String,
    producer: ProducerRuntimeId,
    rtp: RouterRtpParameters,
    source: PublishedSourceDescriptor,
    user: UserId,
    active: bool,
    stream: UserStreamId,
}

#[derive(Debug, Clone, Copy)]
pub enum ConsumerBootstrapOrigin {
    LateJoin,
    Publish,
    Subscribe,
}

impl ConsumerBootstrapOrigin {
    pub(in crate::engine::room) const fn as_diagnostic_str(self) -> &'static str {
        match self {
            Self::LateJoin => "latejoin",
            Self::Publish => "publish",
            Self::Subscribe => "subscribe",
        }
    }
}

impl RoomState {
    pub fn plan_subscription_change(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
        worker_for: impl Fn(ConnectionId) -> MediaWorkerId,
    ) -> PlannedSubscriptionChange {
        if self.user_for_connection(user_id, connection_id).is_none() {
            return PlannedSubscriptionChange::default();
        }
        self.persist_intents(user_id, target_user_id, intents);
        let (updates, relays) =
            self.apply_route_updates(user_id, connection_id, target_user_id, intents);
        let bootstraps = self.plan_consumers(
            self.missing_targets_for_peer(user_id, connection_id, target_user_id),
            worker_for,
        );
        PlannedSubscriptionChange {
            updates,
            bootstraps,
            relays,
        }
    }

    fn persist_intents(
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

    pub fn plan_missing_consumers(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        worker_for: impl Fn(ConnectionId) -> MediaWorkerId,
    ) -> Option<Vec<PlannedConsumerBootstrap>> {
        let user = self.users.get(user_id)?;
        if user.connection_id != connection_id {
            return None;
        }
        if !user.negotiation.can_consume() {
            return Some(Vec::new());
        }
        Some(self.plan_consumers(self.missing_targets(user_id, connection_id), worker_for))
    }

    pub fn plan_consumers(
        &mut self,
        targets: Vec<PendingConsumerBootstrapTarget>,
        worker_for: impl Fn(ConnectionId) -> MediaWorkerId,
    ) -> Vec<PlannedConsumerBootstrap> {
        let mut targets = targets;
        let active_speakers = BTreeSet::new();
        targets.sort_by_key(|target| self.bootstrap_rank(target, &active_speakers));
        targets
            .into_iter()
            .filter_map(|target| self.plan_consumer(&target, &worker_for))
            .collect()
    }

    fn bootstrap_rank(
        &self,
        target: &PendingConsumerBootstrapTarget,
        active_speakers: &BTreeSet<UserId>,
    ) -> VideoAdmissionRank {
        if target.media_kind() != RouterMediaKind::Video {
            return VideoAdmissionRank::new(
                SourceRoutePriority::PinnedOrFeatured,
                None,
                target.source_id(),
            );
        }
        let Some(source) = self.media.source(target.source_id()) else {
            return VideoAdmissionRank::new(
                SourceRoutePriority::HiddenOrOverflow,
                None,
                target.source_id(),
            );
        };
        VideoAdmissionRank::new(
            self.receiver_video_layout_intent(target.consumer_user_id(), source, active_speakers)
                .priority(),
            None,
            target.source_id(),
        )
    }

    fn apply_route_updates(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
    ) -> (Vec<ConsumerRouteUpdate>, Vec<RelayRouteEffect>) {
        let mut updates = Vec::new();
        let mut relays = Vec::new();
        for (stream_id, intent) in intents {
            let Some(active) = intent.active() else {
                continue;
            };
            let Some(source_id) = self
                .media
                .source_id_for_owner_stream(target_user_id, stream_id)
            else {
                continue;
            };
            let key = ConsumerKey::new(user_id, source_id);
            self.media.set_consumer_source_selection(&key, active);
            let Some(route) = self.media.committed_consumer_route_for_key(&key) else {
                continue;
            };
            if route.state.consumer_connection_id != connection_id {
                continue;
            }
            let routed = route.state.routed_consumer_id;
            let route_ref = route.transport_ref();
            let kind = route.source.media_kind();
            let state = if active {
                RouterConsumerRouteState::Active
            } else {
                RouterConsumerRouteState::Paused
            };
            if self
                .topology
                .set_consumer_route_state(routed, state)
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
            updates.push(ConsumerRouteUpdate {
                route: route_ref,
                stream: stream_id.clone(),
                kind,
                active,
            });
            relays.extend(self.media.set_relay_consumer_active(
                user_id,
                connection_id,
                source_id,
                RelayRouteActivity::from_active(active),
            ));
        }
        (updates, relays)
    }

    fn missing_targets(
        &self,
        user_id: &UserId,
        consumer_connection_id: ConnectionId,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        self.missing_targets_where(user_id, consumer_connection_id, |_| true)
    }

    fn missing_targets_for_peer(
        &self,
        user_id: &UserId,
        consumer_connection_id: ConnectionId,
        target_user_id: &UserId,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        self.missing_targets_where(user_id, consumer_connection_id, |producer| {
            producer.owner_user_id == *target_user_id
        })
    }

    fn missing_targets_where(
        &self,
        user_id: &UserId,
        consumer_connection_id: ConnectionId,
        should_include: impl Fn(&PublishedProducer) -> bool,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        self.media
            .producers()
            .filter_map(|(producer_id, producer)| {
                if !should_include(producer) {
                    return None;
                }
                self.bootstrap_target(user_id, consumer_connection_id, producer_id, producer)
            })
            .collect()
    }

    fn bootstrap_target(
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
        let key = ConsumerKey::new(user_id, producer.source_id);
        if self.media.consumer_bootstrap_exists(&key) {
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

    fn plan_consumer(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
        worker_for: &impl Fn(ConnectionId) -> MediaWorkerId,
    ) -> Option<PlannedConsumerBootstrap> {
        let (sender, client_caps) = {
            let user = self.users.get(&target.user)?;
            if user.connection_id != target.connection || !user.negotiation.can_consume() {
                return None;
            }
            (
                user.sender.clone(),
                user.parsed_client_rtp_capabilities.clone()?,
            )
        };
        let (producer_active, producer_rtp, snapshot) = {
            let producer = self.media.producer(target.producer.id)?;
            if !target.producer.matches_pending_producer(producer) {
                return None;
            }
            (
                producer.active,
                producer.consumable_rtp_parameters.clone(),
                target
                    .producer
                    .with_commit_snapshot(producer.routed_producer_id, producer.active),
            )
        };
        let descriptor = self.media.source(target.producer.source_id)?.clone();
        let key = ConsumerKey::new(&target.user, target.source_id());
        if self.media.consumer_bootstrap_exists(&key) {
            return None;
        }
        let selection = self.bootstrap_selection(target, producer_active);
        let active = selection.delivery_active();
        if !can_consume(&producer_rtp, &client_caps) {
            return None;
        }
        let rtp = negotiate_consumer_rtp_parameters(&producer_rtp, &client_caps).ok()?;
        self.media.ensure_consumer_source_selection(&key, selection);
        self.media.reserve_consumer_bootstrap(key.clone());
        let consumer = ConsumerRuntimeId::allocate(&mut self.next_consumer_id);
        let relays = self.reserve_relay_route(target, active, worker_for);
        Some(PlannedConsumerBootstrap {
            target: target.clone(),
            prepared: PreparedConsumerBootstrap { rtp: rtp.clone() },
            pending: PendingConsumerBootstrap {
                key,
                sender,
                track: RemoteTrackBootstrap {
                    consumer,
                    kind: snapshot.kind,
                    mid: rtp
                        .mid()
                        .map_or_else(|| consumer.into_wire_id(), ToOwned::to_owned),
                    producer: snapshot.id,
                    rtp,
                    source: descriptor,
                    user: snapshot.user.clone(),
                    active: snapshot.active.unwrap_or(true),
                    stream: snapshot.stream.clone(),
                },
                selection,
                producer: snapshot,
            },
            relays,
        })
    }

    fn reserve_relay_route(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
        consumer_active: bool,
        worker_for: &impl Fn(ConnectionId) -> MediaWorkerId,
    ) -> Vec<RelayRouteEffect> {
        let Some((source_connection, source_media, target_worker)) =
            Self::relay_route_for_target(target, worker_for)
        else {
            return Vec::new();
        };
        self.media.reserve_relay_consumer(
            target,
            source_connection,
            source_media,
            target_worker,
            consumer_active,
        )
    }

    fn relay_route_for_target(
        target: &PendingConsumerBootstrapTarget,
        worker_for: &impl Fn(ConnectionId) -> MediaWorkerId,
    ) -> Option<(ConnectionId, TransportMediaId, MediaWorkerId)> {
        let source_worker = worker_for(target.producer_connection_id());
        let target_worker = worker_for(target.consumer_connection_id());
        if source_worker == target_worker {
            return None;
        }
        Some((
            target.producer_connection_id(),
            target.transport_media_id(),
            target_worker,
        ))
    }

    fn bootstrap_selection(
        &self,
        target: &PendingConsumerBootstrapTarget,
        producer_active: bool,
    ) -> ConsumerSourceSelection {
        let key = ConsumerKey::new(&target.user, target.source_id());
        let selection = self
            .media
            .consumer_source_selection(&key)
            .unwrap_or_else(|| {
                ConsumerSourceSelection::open(self.desired_source_active(
                    &target.user,
                    target.producer_user_id(),
                    target.stream_id(),
                ))
            });
        self.apply_initial_video_download_cap(target, producer_active, selection)
    }

    fn apply_initial_video_download_cap(
        &self,
        target: &PendingConsumerBootstrapTarget,
        producer_active: bool,
        mut selection: ConsumerSourceSelection,
    ) -> ConsumerSourceSelection {
        if target.media_kind() != RouterMediaKind::Video
            || !producer_active
            || !selection.delivery_active()
            || self.active_video_count(target.consumer_user_id())
                < self.media_limits.max_video_downloads_per_receiver()
        {
            return selection;
        }
        selection.set_policy_pause_reason(Some(PolicyPauseReason::VideoDownloadLimit));
        selection
    }

    fn active_video_count(&self, consumer_user_id: &UserId) -> usize {
        let committed = self
            .current_live_consumer_routes()
            .filter(|route| route.consumer_user_id == *consumer_user_id)
            .filter(|route| route.source.media_kind() == RouterMediaKind::Video)
            .filter(|route| route.producer.active)
            .filter(|route| {
                let desired_active = self.desired_source_active(
                    &route.consumer_user_id,
                    route.source.owner().user_id(),
                    route.source.stream_id(),
                );
                route.selection_or_open(desired_active).delivery_active()
            })
            .count();
        let pending = self
            .media
            .pending_consumer_routes_for_user(consumer_user_id)
            .filter(|route| route.source.media_kind() == RouterMediaKind::Video)
            .filter(|route| route.producer.is_some_and(|producer| producer.active))
            .filter(|route| {
                let desired_active = self.desired_source_active(
                    consumer_user_id,
                    route.source.owner().user_id(),
                    route.source.stream_id(),
                );
                route
                    .selection
                    .unwrap_or_else(|| ConsumerSourceSelection::open(desired_active))
                    .delivery_active()
            })
            .count();
        committed + pending
    }

    pub fn commit_bootstrap(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
        mut pending: PendingConsumerBootstrap,
        media: TransportMediaId,
        mid: Option<String>,
    ) -> Option<(OutboundSender, RemoteTrackBootstrap)> {
        self.media.remove_pending_consumer_bootstrap(&pending.key);
        let user = self.users.get(&target.user)?;
        if user.connection_id != target.connection || !user.negotiation.can_consume() {
            return None;
        }
        let producer = self.media.producer(pending.producer.id)?;
        if !pending.producer.matches_committed_producer(producer) {
            return None;
        }
        if self.media.contains_consumer(&pending.key) {
            return None;
        }
        let active = pending.selection.delivery_active();
        let state = if active {
            RouterConsumerRouteState::Active
        } else {
            RouterConsumerRouteState::Paused
        };
        let routed_consumer_id = match self.topology.add_consumer_with_route_state(
            &target.user,
            pending.producer.routed?,
            pending.producer.kind,
            ConsumerCapability::Compatible,
            state,
        ) {
            Ok(id) => id,
            Err(error) => {
                warn!(
                    consumer_user_id = ?target.user,
                    producer_id = %pending.producer.id,
                    ?error,
                    "router rejected consumer creation"
                );
                return None;
            }
        };
        if let Some(mid) = mid {
            pending.track.mid = mid;
        }
        let key = pending.key;
        let selection = pending.selection;
        if !self.media.commit_consumer(
            key,
            ConsumerState {
                routed_consumer_id,
                consumer_connection_id: target.connection,
                source_connection_id: pending.producer.connection,
                source_media: target.transport_media_id(),
                consumer_media: media,
            },
            selection,
        ) {
            if let Err(error) = self.topology.remove_consumer(routed_consumer_id) {
                warn!(
                    consumer_user_id = ?target.user,
                    routed_consumer_id = ?routed_consumer_id,
                    ?error,
                    "failed to roll back topology consumer after graph consumer commit rejection"
                );
            }
            return None;
        }
        Some((pending.sender, pending.track))
    }

    pub fn release_bootstrap(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
    ) -> Vec<RelayRouteEffect> {
        let key = ConsumerKey::new(&target.user, target.source_id());
        self.media.remove_pending_consumer_bootstrap(&key);
        self.media.release_pending_relay_target(target)
    }

    pub fn desired_source_active(
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

    pub fn consumer_route_state(
        &self,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        stream_id: &UserStreamId,
    ) -> Option<ConsumerRouteState> {
        self.users.get(consumer_user_id)?;
        let Some(source) = self
            .media
            .source_id_for_owner_stream(producer_user_id, stream_id)
        else {
            return Some(ConsumerRouteState::Absent);
        };
        let key = ConsumerKey::new(consumer_user_id, source);
        let Some(route) = self.media.committed_consumer_route_for_key(&key) else {
            return Some(ConsumerRouteState::Absent);
        };
        let desired_active =
            self.desired_source_active(consumer_user_id, producer_user_id, stream_id);
        let route_active =
            route.producer.active && route.selection_or_open(desired_active).delivery_active();
        Some(if route_active {
            ConsumerRouteState::Active
        } else {
            ConsumerRouteState::Inactive
        })
    }

    pub fn active_video_keyframe_targets(
        &self,
        consumer_user_id: &UserId,
        consumer_connection_id: ConnectionId,
    ) -> Option<Vec<ConsumerKeyframeRefreshTarget>> {
        let user = self.users.get(consumer_user_id)?;
        if user.connection_id != consumer_connection_id {
            return None;
        }
        Some(
            self.current_live_consumer_routes()
                .filter_map(|route| {
                    if route.consumer_user_id != *consumer_user_id
                        || route.state.consumer_connection_id != consumer_connection_id
                        || route.source.media_kind() != RouterMediaKind::Video
                    {
                        return None;
                    }
                    if !route.producer.active || !route.selection_or_open(true).delivery_active() {
                        return None;
                    }
                    Some(ConsumerKeyframeRefreshTarget {
                        consumer_media: route.state.consumer_media,
                        producer_user_id: route.source.owner().user_id().clone(),
                        producer_connection_id: route.state.source_connection_id,
                        source_media: route.state.source_media,
                    })
                })
                .collect(),
        )
    }
}

impl PlannedSubscriptionChange {
    pub fn touches_route_graph(&self) -> bool {
        !self.updates.is_empty() || !self.bootstraps.is_empty() || !self.relays.is_empty()
    }

    pub fn into_parts(
        self,
    ) -> (
        Vec<ConsumerRouteUpdate>,
        Vec<PlannedConsumerBootstrap>,
        Vec<RelayRouteEffect>,
    ) {
        (self.updates, self.bootstraps, self.relays)
    }
}

impl PendingConsumerBootstrapTarget {
    pub fn new(
        consumer_user_id: UserId,
        consumer_connection_id: ConnectionId,
        producer: ConsumerBootstrapProducerSnapshot,
    ) -> Self {
        Self {
            user: consumer_user_id,
            connection: consumer_connection_id,
            producer,
        }
    }

    pub const fn consumer_connection_id(&self) -> ConnectionId {
        self.connection
    }

    pub fn consumer_user_id(&self) -> &UserId {
        &self.user
    }

    pub const fn source_id(&self) -> PublishedSourceId {
        self.producer.source_id
    }

    pub const fn media_kind(&self) -> RouterMediaKind {
        self.producer.kind
    }

    pub const fn producer_connection_id(&self) -> ConnectionId {
        self.producer.connection
    }

    pub fn producer_user_id(&self) -> &UserId {
        &self.producer.user
    }

    pub const fn transport_media_id(&self) -> TransportMediaId {
        self.producer.media
    }

    pub fn stream_id(&self) -> &UserStreamId {
        &self.producer.stream
    }
}

impl PendingConsumerBootstrap {
    pub fn consumer_active(&self) -> bool {
        self.selection.delivery_active()
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
        (self.target, self.prepared, self.pending, self.relays)
    }
}

impl ConsumerBootstrapProducerSnapshot {
    pub fn pending(
        source_id: PublishedSourceId,
        user: UserId,
        connection: ConnectionId,
        id: ProducerRuntimeId,
        stream: UserStreamId,
        kind: RouterMediaKind,
        media: TransportMediaId,
    ) -> Self {
        Self {
            source_id,
            user,
            connection,
            id,
            stream,
            kind,
            media,
            routed: None,
            active: None,
        }
    }

    pub const fn source_id(&self) -> PublishedSourceId {
        self.source_id
    }

    pub fn owner_user_id(&self) -> &UserId {
        &self.user
    }

    fn with_commit_snapshot(&self, routed: RoutedProducerId, active: bool) -> Self {
        Self {
            source_id: self.source_id,
            user: self.user.clone(),
            connection: self.connection,
            id: self.id,
            stream: self.stream.clone(),
            kind: self.kind,
            media: self.media,
            routed: Some(routed),
            active: Some(active),
        }
    }

    fn matches_pending_producer(&self, producer: &PublishedProducer) -> bool {
        producer.source_id == self.source_id
            && producer.owner_user_id == self.user
            && producer.owner_connection_id == self.connection
            && producer.stream_id == self.stream
            && producer.media_kind == self.kind
            && producer.transport_media_id == Some(self.media)
    }

    fn matches_committed_producer(&self, producer: &PublishedProducer) -> bool {
        let Some(routed) = self.routed else {
            return false;
        };
        let Some(active) = self.active else {
            return false;
        };
        self.matches_pending_producer(producer)
            && producer.routed_producer_id == routed
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
        &self.rtp
    }

    #[must_use]
    pub fn user_id(&self) -> &UserId {
        &self.user
    }

    #[must_use]
    pub fn source_descriptor(&self) -> &PublishedSourceDescriptor {
        &self.source
    }

    #[must_use]
    pub const fn active(&self) -> bool {
        self.active
    }

    #[must_use]
    pub fn stream_id(&self) -> &UserStreamId {
        &self.stream
    }

    #[must_use]
    pub fn into_room_event_request(self) -> RoomEventRequest {
        RoomEventRequest::BootstrapRemoteTrack(self)
    }
}
