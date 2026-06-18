use std::collections::{BTreeMap, BTreeSet};

use o_sfu_router::{MediaKind as RouterMediaKind, negotiate_consumer_rtp_parameters};
use tracing::error;

use super::{
    super::{RoomMediaCounts, state::RoomState},
    ConsumerKey, ConsumerRouteTarget, ConsumerRuntimeId, ProducerRuntimeId, PublishedProducer,
    consumer_setup::{ConsumerSetupTarget, PendingConsumerSetup, RemoteTrackSetup},
    route_graph::ResolvedRelayRouteEffect,
};
use crate::engine::{
    ConnectionId, MediaWorkerId, UserId,
    room::source_policy::VideoAdmissionRank,
    source_model::{
        ConsumerSourceSelection, PolicyPauseReason, SourceRoutePriority, SourceSubscriptionIntent,
        UserStreamId,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverRouteActivity {
    target: ConsumerRouteTarget,
    active: bool,
}

impl ReceiverRouteActivity {
    pub const fn new(target: ConsumerRouteTarget, active: bool) -> Self {
        Self { target, active }
    }

    pub const fn target(&self) -> &ConsumerRouteTarget {
        &self.target
    }

    pub const fn active(&self) -> bool {
        self.active
    }
}

/// observable receiver route state for room inspection
#[cfg(any(test, feature = "testing-transport"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerRouteState {
    Absent,
    Inactive,
    Active,
}

#[derive(Debug, Default)]
pub struct ReceiverRouteWork {
    activities: Vec<ReceiverRouteActivity>,
    setups: Vec<PendingConsumerSetup>,
    relays: Vec<ResolvedRelayRouteEffect>,
}

impl ReceiverRouteWork {
    pub(in crate::engine::room) fn new(
        activities: Vec<ReceiverRouteActivity>,
        setups: Vec<PendingConsumerSetup>,
        relays: Vec<ResolvedRelayRouteEffect>,
    ) -> Self {
        Self {
            activities,
            setups,
            relays,
        }
    }

    pub(in crate::engine::room) fn route_graph_changed(&self) -> bool {
        !self.activities.is_empty() || !self.setups.is_empty() || !self.relays.is_empty()
    }

    pub(in crate::engine::room) fn into_parts(
        self,
    ) -> (
        Vec<ReceiverRouteActivity>,
        Vec<PendingConsumerSetup>,
        Vec<ResolvedRelayRouteEffect>,
    ) {
        (self.activities, self.setups, self.relays)
    }
}

#[derive(Debug)]
pub struct ReceiverRouteCommit {
    pub(in crate::engine::room) before: RoomMediaCounts,
    pub(in crate::engine::room) after: RoomMediaCounts,
    pub(in crate::engine::room) media_worker_id: MediaWorkerId,
    pub(in crate::engine::room) work: ReceiverRouteWork,
}

impl RoomState {
    pub fn apply_receiver_intent(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
        worker_for: impl Fn(ConnectionId) -> MediaWorkerId,
    ) -> Option<ReceiverRouteCommit> {
        self.user_for_connection(user_id, connection_id)?;
        let before = self.media_counts();
        let media_worker_id = self.media_worker_id_for_connection(connection_id);
        let work = self.plan_receiver_intent_change(
            user_id,
            connection_id,
            target_user_id,
            intents,
            worker_for,
        );
        let after = self.media_counts();
        Some(ReceiverRouteCommit {
            before,
            after,
            media_worker_id,
            work,
        })
    }

    #[cfg(test)]
    pub fn plan_receiver_route_work(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
        worker_for: impl Fn(ConnectionId) -> MediaWorkerId,
    ) -> ReceiverRouteWork {
        if self.user_for_connection(user_id, connection_id).is_none() {
            return ReceiverRouteWork::default();
        }
        self.plan_receiver_intent_change(
            user_id,
            connection_id,
            target_user_id,
            intents,
            worker_for,
        )
    }

    fn plan_receiver_intent_change(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
        worker_for: impl Fn(ConnectionId) -> MediaWorkerId,
    ) -> ReceiverRouteWork {
        self.persist_intents(user_id, target_user_id, intents);
        let (updates, relays) =
            self.apply_route_updates(user_id, connection_id, target_user_id, intents);
        let setups = self.plan_consumers(
            self.missing_targets_for_peer(user_id, connection_id, target_user_id),
            worker_for,
        );
        ReceiverRouteWork::new(updates, setups, relays)
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

    pub fn refresh_consumer_readiness(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<ReceiverRouteCommit> {
        let can_consume = {
            let user = self.users.get(user_id)?;
            if user.connection_id != connection_id {
                return None;
            }
            user.negotiation.can_consume()
        };
        let before = self.media_counts();
        let setups = if can_consume {
            let worker_for = self.worker_lookup();
            self.plan_consumers(self.missing_targets(user_id, connection_id), worker_for)
        } else {
            Vec::new()
        };
        let after = self.media_counts();
        Some(ReceiverRouteCommit {
            before,
            after,
            media_worker_id: self.media_worker_id_for_connection(connection_id),
            work: ReceiverRouteWork::new(Vec::new(), setups, Vec::new()),
        })
    }

    #[cfg(test)]
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
        if target.kind != RouterMediaKind::Video {
            return VideoAdmissionRank::new(
                SourceRoutePriority::PinnedOrFeatured,
                None,
                target.source_id,
            );
        }
        let Some(source) = self.topology.media().source(target.source_id) else {
            return VideoAdmissionRank::new(
                SourceRoutePriority::HiddenOrOverflow,
                None,
                target.source_id,
            );
        };
        VideoAdmissionRank::new(
            self.receiver_video_layout_intent(&target.user, source, active_speakers)
                .priority(),
            None,
            target.source_id,
        )
    }

    fn apply_route_updates(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
    ) -> (Vec<ReceiverRouteActivity>, Vec<ResolvedRelayRouteEffect>) {
        let mut updates = Vec::new();
        let mut relays = Vec::new();
        for (stream_id, intent) in intents {
            let Some(active) = intent.active() else {
                continue;
            };
            let Some(commit) = self.topology.set_consumer_activity(
                user_id,
                connection_id,
                target_user_id,
                stream_id,
                active,
            ) else {
                continue;
            };
            relays.extend(commit.relay_effects);
            if let Some(error) = commit.routing_error {
                error!(
                    ?user_id,
                    ?target_user_id,
                    stream_id = %stream_id,
                    ?error,
                    "failed to set consumer pause state in room router"
                );
                continue;
            }
            if let Some(update) = commit.update {
                updates.push(update);
            }
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
        self.topology
            .media()
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
        if self.topology.media().has_consumer_setup_or_route(&key) {
            return None;
        }
        let consumer_session =
            self.committed_transport_user_key(user_id, consumer_connection_id)?;
        let producer_session = self
            .committed_transport_user_key(&producer.owner_user_id, producer.owner_connection_id)?;
        Some(ConsumerSetupTarget::new(
            user_id.clone(),
            consumer_connection_id,
            consumer_session,
            producer_session,
            producer_id,
            producer,
            transport_media_id,
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
        let (producer_rtp, producer_active, descriptor) = {
            let producer = self.topology.media().producer(target.producer_id)?;
            if !target.matches_identity(producer) {
                return None;
            }
            (
                &producer.consumable_rtp_parameters,
                producer.active,
                self.topology.media().source(target.source_id)?.clone(),
            )
        };
        let selection = self.setup_selection(&target, producer_active);
        let rtp = negotiate_consumer_rtp_parameters(producer_rtp, client_caps).ok()?;
        let consumer = ConsumerRuntimeId::allocate(&mut self.next_consumer_id);
        let source_worker = worker_for(target.producer_connection);
        let target_worker = worker_for(target.connection);
        let track = RemoteTrackSetup {
            consumer,
            kind: target.kind,
            mid: rtp
                .mid()
                .map_or_else(|| consumer.into_wire_id(), ToOwned::to_owned),
            producer: target.producer_id,
            rtp,
            source: descriptor,
            user: target.producer_user.clone(),
            active: producer_active,
            stream: target.stream.clone(),
        };
        self.topology.reserve_consumer_setup(
            target,
            selection,
            source_worker,
            target_worker,
            sender,
            track,
        )
    }

    pub(super) fn setup_selection(
        &self,
        target: &ConsumerSetupTarget,
        producer_active: bool,
    ) -> ConsumerSourceSelection {
        let key = target.consumer_key();
        let selection = self
            .topology
            .media()
            .consumer_source_selection(&key)
            .unwrap_or_else(|| {
                ConsumerSourceSelection::open(self.desired_source_active(
                    &target.user,
                    &target.producer_user,
                    &target.stream,
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
        if target.kind != RouterMediaKind::Video
            || !producer_active
            || !selection.delivery_active()
            || self.active_video_count(&target.user)
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
            .topology
            .media()
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

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn consumer_route_state(
        &self,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        stream_id: &UserStreamId,
    ) -> Option<ConsumerRouteState> {
        self.users.get(consumer_user_id)?;
        let Some(source) = self
            .topology
            .media()
            .source_id_for_owner_stream(producer_user_id, stream_id)
        else {
            return Some(ConsumerRouteState::Absent);
        };
        let key = ConsumerKey::new(consumer_user_id, source);
        let Some(route) = self.topology.media().committed_consumer_route_for_key(&key) else {
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
    ) -> Option<Vec<ConsumerRouteTarget>> {
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
                    let route_ref = route.transport_ref();
                    let transport_route = self.transport_consumer_route(&route_ref);
                    Some(route.target(transport_route))
                })
                .collect(),
        )
    }
}
