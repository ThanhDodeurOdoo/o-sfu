use std::collections::{BTreeMap, BTreeSet};

use o_sfu_router::{
    ConsumerCapability, ConsumerRouteState as RouterConsumerRouteState,
    MediaKind as RouterMediaKind, MediaStream as RouterRtpParameters,
    negotiate_consumer_rtp_parameters,
};
use tracing::{error, warn};

use super::{
    super::{
        RoomEventRequest, outbound::OutboundSender, routing::RoutedProducerId, state::RoomState,
    },
    ConsumerKey, ConsumerRouteTransportRef, ConsumerRuntimeId, ConsumerState, ProducerRuntimeId,
    PublishedProducer, RoomMediaGraph,
    route_graph::{ConsumerRouteReservation, RelayRouteEffect, ResolvedRelayRouteEffect},
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

/// observable receiver route state for room inspection
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
    setups: Vec<PendingConsumerSetup>,
    relays: Vec<ResolvedRelayRouteEffect>,
}

#[derive(Debug, Clone)]
pub struct ConsumerSetupTarget {
    user: UserId,
    connection: ConnectionId,
    producer: ConsumerSetupProducerSnapshot,
}

#[derive(Debug, Clone)]
pub struct ConsumerSetupProducerSnapshot {
    source_id: PublishedSourceId,
    user: UserId,
    connection: ConnectionId,
    id: ProducerRuntimeId,
    stream: UserStreamId,
    kind: RouterMediaKind,
    media: TransportMediaId,
    routed: RoutedProducerId,
    active: bool,
}

#[derive(Debug)]
#[must_use = "pending consumer setups reserve route graph state and must be committed or released"]
pub(in crate::engine::room) struct PendingConsumerSetup {
    target: ConsumerSetupTarget,
    reservation: ConsumerRouteReservation,
    sender: OutboundSender,
    track: RemoteTrackSetup,
    relays: Vec<ResolvedRelayRouteEffect>,
}

#[derive(Debug)]
pub(in crate::engine::room) struct ConsumerSetupCommit {
    pub(in crate::engine::room) sender: OutboundSender,
    pub(in crate::engine::room) track: RemoteTrackSetup,
    pub(in crate::engine::room) transport_activity_update: Option<bool>,
}

#[allow(
    clippy::large_enum_variant,
    reason = "consumer setup outcomes are returned and matched immediately so boxing the committed setup would allocate on every successful consumer setup"
)]
pub(in crate::engine::room) enum ConsumerSetupOutcome {
    Committed(ConsumerSetupCommit),
    Released(Vec<ResolvedRelayRouteEffect>),
}

pub(in crate::engine::room) struct ConsumerSetupTransportInput<'a> {
    pub target: &'a ConsumerSetupTarget,
    pub rtp: &'a RouterRtpParameters,
    pub active: bool,
    pub relays: &'a [ResolvedRelayRouteEffect],
}

/// consumer track setup payload sent to the receiver after transport commit
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteTrackSetup {
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
pub enum ConsumerSetupOrigin {
    LateJoin,
    Publish,
    Subscribe,
}

impl ConsumerSetupOrigin {
    pub(in crate::engine::room) const fn as_diagnostic_str(self) -> &'static str {
        match self {
            Self::LateJoin => "latejoin",
            Self::Publish => "publish",
            Self::Subscribe => "subscribe",
        }
    }
}

impl RoomMediaGraph {
    fn reserve_pending_consumer_setup(
        &mut self,
        target: ConsumerSetupTarget,
        sender: OutboundSender,
        track: RemoteTrackSetup,
        selection: ConsumerSourceSelection,
        source_worker: MediaWorkerId,
        target_worker: MediaWorkerId,
    ) -> Option<(PendingConsumerSetup, Vec<RelayRouteEffect>)> {
        let key = ConsumerKey::new(&target.user, target.source_id());
        let active = selection.delivery_active();
        let reservation = self.routes.reserve_consumer_setup(key, selection)?;
        let relay_effects = if source_worker == target_worker {
            Vec::new()
        } else {
            self.routes.reserve_relay(
                &reservation,
                &target,
                target.producer_connection_id(),
                target.transport_media_id(),
                target_worker,
                active,
            )
        };
        Some((
            PendingConsumerSetup {
                target,
                reservation,
                sender,
                track,
                relays: Vec::new(),
            },
            relay_effects,
        ))
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
        let relays = self.resolved_relay_route_effects(relays);
        let setups = self.plan_consumers(
            self.missing_targets_for_peer(user_id, connection_id, target_user_id),
            worker_for,
        );
        PlannedSubscriptionChange {
            updates,
            setups,
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
    ) -> Option<Vec<PendingConsumerSetup>> {
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
        targets: Vec<ConsumerSetupTarget>,
        worker_for: impl Fn(ConnectionId) -> MediaWorkerId,
    ) -> Vec<PendingConsumerSetup> {
        let mut targets = targets;
        let active_speakers = BTreeSet::new();
        targets.sort_by_key(|target| self.setup_rank(target, &active_speakers));
        targets
            .into_iter()
            .filter_map(|target| self.plan_consumer(target, &worker_for))
            .collect()
    }

    fn setup_rank(
        &self,
        target: &ConsumerSetupTarget,
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
            relays.extend(self.media.set_relay_consumer_active(
                user_id,
                connection_id,
                source_id,
                RelayRouteActivity::from_active(active),
            ));
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
                .routing
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
        }
        (updates, relays)
    }

    fn missing_targets(
        &self,
        user_id: &UserId,
        consumer_connection_id: ConnectionId,
    ) -> Vec<ConsumerSetupTarget> {
        self.missing_targets_where(user_id, consumer_connection_id, |_| true)
    }

    fn missing_targets_for_peer(
        &self,
        user_id: &UserId,
        consumer_connection_id: ConnectionId,
        target_user_id: &UserId,
    ) -> Vec<ConsumerSetupTarget> {
        self.missing_targets_where(user_id, consumer_connection_id, |producer| {
            producer.owner_user_id == *target_user_id
        })
    }

    fn missing_targets_where(
        &self,
        user_id: &UserId,
        consumer_connection_id: ConnectionId,
        should_include: impl Fn(&PublishedProducer) -> bool,
    ) -> Vec<ConsumerSetupTarget> {
        self.media
            .producers()
            .filter_map(|(producer_id, producer)| {
                if !should_include(producer) {
                    return None;
                }
                self.consumer_setup_target(user_id, consumer_connection_id, producer_id, producer)
            })
            .collect()
    }

    fn consumer_setup_target(
        &self,
        user_id: &UserId,
        consumer_connection_id: ConnectionId,
        producer_id: ProducerRuntimeId,
        producer: &PublishedProducer,
    ) -> Option<ConsumerSetupTarget> {
        let transport_media_id = producer.transport_media_id?;
        if producer.owner_user_id == *user_id {
            return None;
        }
        let key = ConsumerKey::new(user_id, producer.source_id);
        if self.media.has_consumer_setup_or_route(&key) {
            return None;
        }
        Some(ConsumerSetupTarget::new(
            user_id.clone(),
            consumer_connection_id,
            ConsumerSetupProducerSnapshot::from_producer(producer_id, producer, transport_media_id),
        ))
    }

    fn plan_consumer(
        &mut self,
        target: ConsumerSetupTarget,
        worker_for: &impl Fn(ConnectionId) -> MediaWorkerId,
    ) -> Option<PendingConsumerSetup> {
        let (sender, client_caps) = {
            let user = self.users.get(&target.user)?;
            if user.connection_id != target.connection || !user.negotiation.can_consume() {
                return None;
            }
            (
                user.sender.clone(),
                user.parsed_client_rtp_capabilities.as_ref()?,
            )
        };
        let producer_rtp = {
            let producer = self.media.producer(target.producer.id)?;
            if !target.producer.matches_identity(producer) {
                return None;
            }
            &producer.consumable_rtp_parameters
        };
        let descriptor = self.media.source(target.producer.source_id)?.clone();
        let selection = self.setup_selection(&target, target.producer.active);
        let rtp = negotiate_consumer_rtp_parameters(producer_rtp, client_caps).ok()?;
        let consumer = ConsumerRuntimeId::allocate(&mut self.next_consumer_id);
        let source_worker = worker_for(target.producer_connection_id());
        let target_worker = worker_for(target.consumer_connection_id());
        let track = RemoteTrackSetup {
            consumer,
            kind: target.producer.kind,
            mid: rtp
                .mid()
                .map_or_else(|| consumer.into_wire_id(), ToOwned::to_owned),
            producer: target.producer.id,
            rtp,
            source: descriptor,
            user: target.producer.user.clone(),
            active: target.producer.active,
            stream: target.producer.stream.clone(),
        };
        let (mut setup, relay_effects) = self.media.reserve_pending_consumer_setup(
            target,
            sender,
            track,
            selection,
            source_worker,
            target_worker,
        )?;
        setup.relays = self.resolved_relay_route_effects(relay_effects);
        Some(setup)
    }

    fn setup_selection(
        &self,
        target: &ConsumerSetupTarget,
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
        target: &ConsumerSetupTarget,
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
        !self.updates.is_empty() || !self.setups.is_empty() || !self.relays.is_empty()
    }

    pub fn into_parts(
        self,
    ) -> (
        Vec<ConsumerRouteUpdate>,
        Vec<PendingConsumerSetup>,
        Vec<ResolvedRelayRouteEffect>,
    ) {
        (self.updates, self.setups, self.relays)
    }
}

impl PendingConsumerSetup {
    pub(in crate::engine::room) fn transport_input(&self) -> ConsumerSetupTransportInput<'_> {
        ConsumerSetupTransportInput {
            target: &self.target,
            rtp: &self.track.rtp,
            active: self.reservation.selection().delivery_active(),
            relays: &self.relays,
        }
    }

    pub(in crate::engine::room) fn commit(
        mut self,
        state: &mut RoomState,
        media: TransportMediaId,
        mid: Option<String>,
    ) -> ConsumerSetupOutcome {
        let planned = self.reservation.selection();
        let Some(user) = state.users.get(&self.target.user) else {
            return self.release_into_outcome(state);
        };
        if user.connection_id != self.target.connection || !user.negotiation.can_consume() {
            return self.release_into_outcome(state);
        }
        let Some(producer) = state.media.producer(self.target.producer.id) else {
            return self.release_into_outcome(state);
        };
        if !self.target.producer.matches_identity(producer) {
            return self.release_into_outcome(state);
        }
        let producer_active = producer.active;
        if state.media.contains_consumer(self.reservation.key()) {
            return self.release_into_outcome(state);
        }
        let selection = state.setup_selection(&self.target, producer_active);
        let active = selection.delivery_active();
        let route_state = if active {
            RouterConsumerRouteState::Active
        } else {
            RouterConsumerRouteState::Paused
        };
        let routed_consumer_id = match state.routing.add_consumer_with_route_state(
            &self.target.user,
            self.target.producer.routed,
            self.target.producer.kind,
            ConsumerCapability::Compatible,
            route_state,
        ) {
            Ok(id) => id,
            Err(error) => {
                warn!(
                    consumer_user_id = ?self.target.user,
                    producer_id = %self.target.producer.id,
                    ?error,
                    "router rejected consumer creation"
                );
                return self.release_into_outcome(state);
            }
        };
        if let Some(mid) = mid {
            self.track.mid = mid;
        }
        self.track.active = producer_active;
        let transport_activity_update = (active != planned.delivery_active()).then_some(active);
        if !state.media.routes.commit(
            &self.reservation,
            ConsumerState {
                routed_consumer_id,
                consumer_connection_id: self.target.connection,
                source_connection_id: self.target.producer.connection,
                source_media: self.target.transport_media_id(),
                consumer_media: media,
            },
            selection,
        ) {
            if let Err(error) = state.routing.remove_consumer(routed_consumer_id) {
                warn!(
                    consumer_user_id = ?self.target.user,
                    routed_consumer_id = ?routed_consumer_id,
                    ?error,
                    "failed to roll back topology consumer after graph consumer commit rejection"
                );
            }
            return self.release_into_outcome(state);
        }
        ConsumerSetupOutcome::Committed(ConsumerSetupCommit {
            sender: self.sender,
            track: self.track,
            transport_activity_update,
        })
    }

    pub(in crate::engine::room) fn release(
        self,
        state: &mut RoomState,
    ) -> Vec<ResolvedRelayRouteEffect> {
        let relays = state.media.routes.release_consumer_setup(self.reservation);
        state.resolved_relay_route_effects(relays)
    }

    fn release_into_outcome(self, state: &mut RoomState) -> ConsumerSetupOutcome {
        ConsumerSetupOutcome::Released(self.release(state))
    }
}

impl ConsumerSetupTarget {
    pub fn new(
        consumer_user_id: UserId,
        consumer_connection_id: ConnectionId,
        producer: ConsumerSetupProducerSnapshot,
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

impl ConsumerSetupProducerSnapshot {
    pub fn from_producer(
        id: ProducerRuntimeId,
        producer: &PublishedProducer,
        media: TransportMediaId,
    ) -> Self {
        Self {
            source_id: producer.source_id,
            user: producer.owner_user_id.clone(),
            connection: producer.owner_connection_id,
            id,
            stream: producer.stream_id.clone(),
            kind: producer.media_kind,
            media,
            routed: producer.routed_producer_id,
            active: producer.active,
        }
    }

    fn matches_identity(&self, producer: &PublishedProducer) -> bool {
        producer.source_id == self.source_id
            && producer.owner_user_id == self.user
            && producer.owner_connection_id == self.connection
            && producer.stream_id == self.stream
            && producer.media_kind == self.kind
            && producer.transport_media_id == Some(self.media)
            && producer.routed_producer_id == self.routed
    }
}

impl RemoteTrackSetup {
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
        RoomEventRequest::SetupRemoteTrack(self)
    }
}
