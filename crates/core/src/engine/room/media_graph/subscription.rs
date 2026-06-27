use std::collections::{BTreeMap, BTreeSet};

use o_sfu_router::{MediaKind as RouterMediaKind, negotiation::negotiate_consumer_rtp_parameters};
use tracing::error;

#[cfg(any(test, feature = "testing-transport"))]
use super::ConsumerKey;
use super::{
    super::{effects::RoomGaugeDelta, state::RoomState},
    ConsumerRouteTarget, ConsumerRuntimeId, ProducerRuntimeId,
    consumer_setup::{ConsumerSetupTarget, PendingConsumerSetup},
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
    pub(in crate::engine::room) activities: Vec<ReceiverRouteActivity>,
    pub(in crate::engine::room) setups: Vec<PendingConsumerSetup>,
    pub(in crate::engine::room) relays: Vec<ResolvedRelayRouteEffect>,
}

impl ReceiverRouteWork {
    pub(in crate::engine::room) fn route_graph_changed(&self) -> bool {
        !self.activities.is_empty() || !self.setups.is_empty() || !self.relays.is_empty()
    }
}

#[derive(Debug)]
pub struct ReceiverRouteCommit {
    pub(in crate::engine::room) receiver_user_id: UserId,
    pub(in crate::engine::room) receiver_connection_id: ConnectionId,
    pub(in crate::engine::room) counts: RoomGaugeDelta,
    pub(in crate::engine::room) media_worker_id: MediaWorkerId,
    pub(in crate::engine::room) work: ReceiverRouteWork,
}

#[derive(Clone, Copy)]
pub(super) enum ReceiverRouteScope<'a> {
    Producer(ProducerRuntimeId),
    Receiver(&'a UserId, ConnectionId),
    SourceUser(&'a UserId, ConnectionId, &'a UserId),
}

impl RoomState {
    pub fn apply_receiver_intent(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
    ) -> Option<ReceiverRouteCommit> {
        self.user_for_connection(user_id, connection_id)?;
        let before = self.media_counts();
        let media_worker_id = self
            .topology
            .routing()
            .media_worker_id_for_connection(connection_id);
        let work =
            self.plan_receiver_intent_change(user_id, connection_id, target_user_id, intents);
        Some(ReceiverRouteCommit {
            receiver_user_id: user_id.clone(),
            receiver_connection_id: connection_id,
            counts: RoomGaugeDelta::media(before, self.media_counts()),
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
    ) -> ReceiverRouteWork {
        if self.user_for_connection(user_id, connection_id).is_none() {
            return ReceiverRouteWork::default();
        }
        self.plan_receiver_intent_change(user_id, connection_id, target_user_id, intents)
    }

    fn plan_receiver_intent_change(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
    ) -> ReceiverRouteWork {
        self.persist_intents(user_id, target_user_id, intents);
        let (updates, relays) =
            self.apply_route_updates(user_id, connection_id, target_user_id, intents);
        let ReceiverRouteWork { setups, .. } = self.plan_missing_receiver_routes(
            ReceiverRouteScope::SourceUser(user_id, connection_id, target_user_id),
        );
        ReceiverRouteWork {
            activities: updates,
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
        let work = if can_consume {
            self.plan_missing_receiver_routes(ReceiverRouteScope::Receiver(user_id, connection_id))
        } else {
            ReceiverRouteWork::default()
        };
        Some(ReceiverRouteCommit {
            receiver_user_id: user_id.clone(),
            receiver_connection_id: connection_id,
            counts: RoomGaugeDelta::media(before, self.media_counts()),
            media_worker_id: self
                .topology
                .routing()
                .media_worker_id_for_connection(connection_id),
            work,
        })
    }

    #[cfg(test)]
    pub fn plan_missing_consumers(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<Vec<PendingConsumerSetup>> {
        let user = self.users.get(user_id)?;
        if user.connection_id != connection_id {
            return None;
        }
        if !user.negotiation.can_consume() {
            return Some(Vec::new());
        }
        let ReceiverRouteWork { setups, .. } =
            self.plan_missing_receiver_routes(ReceiverRouteScope::Receiver(user_id, connection_id));
        Some(setups)
    }

    pub(super) fn plan_missing_receiver_routes(
        &mut self,
        scope: ReceiverRouteScope<'_>,
    ) -> ReceiverRouteWork {
        ReceiverRouteWork {
            setups: self.plan_consumers(self.missing_receiver_route_targets(scope)),
            ..Default::default()
        }
    }

    fn plan_consumers(
        &mut self,
        mut targets: Vec<ConsumerSetupTarget>,
    ) -> Vec<PendingConsumerSetup> {
        let active_speakers = BTreeSet::new();
        targets.sort_by_key(|target| self.setup_rank(target, &active_speakers));
        targets
            .into_iter()
            .filter_map(|target| self.plan_consumer(target))
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
        let Some(source) = self.topology.source(target.source_id) else {
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

    fn missing_receiver_route_targets(
        &self,
        scope: ReceiverRouteScope<'_>,
    ) -> Vec<ConsumerSetupTarget> {
        match scope {
            ReceiverRouteScope::Producer(producer_id) => {
                let receivers = self.users.iter().filter_map(|(user, state)| {
                    state
                        .negotiation
                        .can_consume()
                        .then_some((user, state.connection_id))
                });
                self.topology
                    .missing_consumer_targets_for_producer(producer_id, receivers)
            }
            ReceiverRouteScope::Receiver(user, connection) => self
                .topology
                .missing_consumer_targets(user, connection, |_| true),
            ReceiverRouteScope::SourceUser(user, connection, source_user_id) => self
                .topology
                .missing_consumer_targets(user, connection, |producer| {
                    producer.owner_user_id == *source_user_id
                }),
        }
    }

    fn plan_consumer(&mut self, target: ConsumerSetupTarget) -> Option<PendingConsumerSetup> {
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
        let (producer_rtp, producer_active) = {
            let producer = self.topology.producer(target.producer_id)?;
            if !target.matches_identity(producer) {
                return None;
            }
            (&producer.consumable_rtp_parameters, producer.active)
        };
        let selection = self.setup_selection(&target, producer_active);
        let rtp = negotiate_consumer_rtp_parameters(producer_rtp, client_caps).ok()?;
        let consumer = ConsumerRuntimeId::allocate(&mut self.next_consumer_id);
        let fallback_mid = rtp
            .mid()
            .map_or_else(|| consumer.into_wire_id(), ToOwned::to_owned);
        self.topology
            .reserve_consumer_setup(target, selection, sender, fallback_mid, rtp)
    }

    pub(super) fn setup_selection(
        &self,
        target: &ConsumerSetupTarget,
        producer_active: bool,
    ) -> ConsumerSourceSelection {
        let key = target.consumer_key();
        let selection = self
            .topology
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
            .live_consumer_routes()
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
            .pending_consumer_routes_for_user(consumer_user_id)
            .filter(|route| route.source.media_kind() == RouterMediaKind::Video)
            .filter(|route| route.producer.active)
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
            .source_id_for_owner_stream(producer_user_id, stream_id)
        else {
            return Some(ConsumerRouteState::Absent);
        };
        let key = ConsumerKey::new(consumer_user_id, source);
        let Some(route) = self.topology.committed_consumer_route_for_key(&key) else {
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
            self.live_consumer_routes()
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
                    Some(
                        self.topology
                            .consumer_route_target_for_source(route.transport_ref(), route.source),
                    )
                })
                .collect(),
        )
    }
}
