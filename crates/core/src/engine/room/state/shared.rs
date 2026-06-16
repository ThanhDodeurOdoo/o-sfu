use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use o_sfu_router::{MediaCapabilities, MediaCapabilities as RouterRtpCapabilities};

use super::super::{
    RoomAdmissionPolicy, RoomMediaCounts, RoomUserPermissions,
    cleanup::TransportCleanupOperation,
    media_graph::{
        ConsumerRouteTransportRef, ConsumerRouteView, RelayRouteEffect, ResolvedRelayRouteEffect,
        RoomMediaGraph, TransportMediaRemoval,
    },
    outbound::OutboundSender,
    routing::{DisplacedRoutingSession, RoomRouterStateFactory, RoomRoutingState},
    user_negotiation::UserNegotiation,
};
use crate::{
    RoomMediaLimits, RoomSpilloverMode,
    engine::{
        ConnectionId, MediaWorkerId, PeerSnapshot, RecordingState, UserId, UserInfo,
        VideoLayoutIntent,
        media_transport::{TransportConsumerRoute, TransportSessionKey, TransportSourceKey},
        room::placement::{LoadTriggeredPlacementState, RoomPlacementUsageSnapshot},
        router_events::RoomRouterEventSink,
        source_model::{
            ActiveSpeakerGroup, ConsumerSourceSelection, PublishedSourceDescriptor,
            PublishedSourceId, SourceSubscriptionIntent, UserStreamId,
        },
    },
};

#[derive(Debug)]
pub struct RoomState {
    pub(super) admission_policy: RoomAdmissionPolicy,
    pub media_limits: RoomMediaLimits,
    pub users: BTreeMap<UserId, ActiveUser>,
    /// rejects stale async callbacks from previous connections
    pub(super) next_connection_id: u64,
    pub next_source_id: u64,
    pub next_source_encoding_id: u64,
    pub next_producer_id: u64,
    pub next_consumer_id: u64,
    pub(super) recording_state: RecordingState,
    pub media: RoomMediaGraph,
    pub routing: RoomRoutingState,
}

#[derive(Debug)]
pub struct ActiveUser {
    #[allow(
        dead_code,
        reason = "stored for future user display and recording metadata"
    )]
    pub(super) label: Option<String>,
    #[allow(dead_code, reason = "stored for future permission-gated actions")]
    pub(super) permissions: RoomUserPermissions,
    pub(super) info: UserInfo,
    pub(super) server_featured: Option<bool>,
    pub negotiation: UserNegotiation,
    pub desired_source_subscriptions:
        BTreeMap<UserId, BTreeMap<UserStreamId, SourceSubscriptionIntent>>,
    pub parsed_client_rtp_capabilities: Option<RouterRtpCapabilities>,
    pub connection_id: ConnectionId,
    pub sender: OutboundSender,
}

impl ActiveUser {
    pub(super) fn reset_presentation(&mut self) {
        self.info = UserInfo::default();
        self.server_featured = None;
    }

    pub(super) fn apply_info_update(&mut self, info: &UserInfo) {
        self.info.apply_partial_update(info);
    }

    pub(super) const fn featured(&self) -> Option<bool> {
        self.server_featured
    }

    pub(super) fn set_featured(&mut self, featured: Option<bool>) {
        self.server_featured = featured;
    }

    pub(super) fn project_info(&self) -> UserInfo {
        self.info
            .clone()
            .with_featured(self.server_featured)
            .snapshot_complete()
    }
}

impl RoomState {
    pub fn new(
        runtime_context: &super::super::RoomRuntimeContext,
        admission_policy: RoomAdmissionPolicy,
        media_limits: RoomMediaLimits,
        router_rtp_capabilities: MediaCapabilities,
        router_event_sink: Arc<dyn RoomRouterEventSink>,
    ) -> Self {
        Self {
            admission_policy,
            media_limits,
            users: BTreeMap::new(),
            next_connection_id: 0,
            next_source_id: 1,
            next_source_encoding_id: 1,
            next_producer_id: 1,
            next_consumer_id: 1,
            recording_state: RecordingState {
                recording: Some(false),
                audio: Some(false),
                transcription: Some(false),
                video: Some(false),
            },
            media: RoomMediaGraph::default(),
            routing: RoomRoutingState::new_with_router_state_factory(
                runtime_context.instance(),
                runtime_context.primary_router(),
                runtime_context.initial_local_router_placements().cloned(),
                router_rtp_capabilities,
                &RoomRouterStateFactory::new(router_event_sink),
            ),
        }
    }

    pub fn user_for_connection(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<&ActiveUser> {
        let user = self.users.get(user_id)?;
        if user.connection_id != connection_id {
            return None;
        }
        Some(user)
    }

    pub fn user_mut_for_connection(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<&mut ActiveUser> {
        let user = self.users.get_mut(user_id)?;
        if user.connection_id != connection_id {
            return None;
        }
        Some(user)
    }

    pub fn recording_state(&self) -> RecordingState {
        self.recording_state.clone()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn router_rtp_capabilities(&self) -> MediaCapabilities {
        self.routing.rtp_capabilities().clone()
    }

    pub fn transport_user_entries(&self) -> Vec<(UserId, ConnectionId)> {
        self.users
            .iter()
            .map(|(user_id, user)| (user_id.clone(), user.connection_id))
            .collect()
    }

    pub fn transport_user_key(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> TransportSessionKey {
        self.routing.transport_user_key(user_id, connection_id)
    }

    pub fn committed_transport_user_key(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<TransportSessionKey> {
        self.routing
            .committed_transport_user_key(user_id, connection_id)
    }

    pub fn transport_consumer_route(
        &self,
        route: &ConsumerRouteTransportRef,
    ) -> TransportConsumerRoute {
        TransportConsumerRoute::new(
            self.transport_user_key(&route.consumer_user_id, route.consumer_connection_id),
            route.consumer_media,
            TransportSourceKey::new(
                self.transport_user_key(&route.source_user_id, route.source_connection_id),
                route.source_media,
            ),
        )
    }

    pub fn transport_cleanup_operations(
        &self,
        removals: impl IntoIterator<Item = TransportMediaRemoval>,
    ) -> Vec<TransportCleanupOperation> {
        removals
            .into_iter()
            .map(|removal| {
                let connection_id = removal.connection;
                TransportCleanupOperation::RemoveMedia {
                    session_key: self.transport_user_key(&removal.user, connection_id),
                    connection_id,
                    transport_media_id: removal.transport_media,
                }
            })
            .collect()
    }

    pub fn resolved_relay_route_effects(
        &self,
        effects: impl IntoIterator<Item = RelayRouteEffect>,
    ) -> Vec<ResolvedRelayRouteEffect> {
        effects
            .into_iter()
            .map(|effect| ResolvedRelayRouteEffect {
                source_session_key: self
                    .transport_user_key(&effect.route.source_user, effect.route.source_connection),
                route: effect.route,
                action: effect.action,
            })
            .collect()
    }

    pub fn resolved_relay_route_effects_with_displaced(
        &self,
        effects: impl IntoIterator<Item = RelayRouteEffect>,
        user_id: &UserId,
        session: &DisplacedRoutingSession,
    ) -> Vec<ResolvedRelayRouteEffect> {
        effects
            .into_iter()
            .map(|effect| {
                let source_session_key = if effect.route.source_user == *user_id
                    && effect.route.source_connection == session.connection_id
                {
                    session.transport_session_key.clone()
                } else {
                    self.transport_user_key(
                        &effect.route.source_user,
                        effect.route.source_connection,
                    )
                };
                ResolvedRelayRouteEffect {
                    source_session_key,
                    route: effect.route,
                    action: effect.action,
                }
            })
            .collect()
    }

    pub fn placement_usage_snapshot(&self) -> RoomPlacementUsageSnapshot {
        self.routing.usage_snapshot()
    }

    pub fn media_worker_id_for_connection(&self, connection_id: ConnectionId) -> MediaWorkerId {
        self.routing.media_worker_id_for_connection(connection_id)
    }

    pub fn assigned_primary_media_worker_id(&self) -> Option<MediaWorkerId> {
        self.routing.assigned_primary_media_worker_id()
    }

    pub fn worker_lookup(&self) -> impl Fn(ConnectionId) -> MediaWorkerId + use<> {
        self.routing.worker_lookup()
    }

    pub fn transport_consumer_entries(&self) -> Vec<(UserId, ConnectionId)> {
        let mut entries = self
            .media
            .committed_consumer_transport_entries()
            .collect::<Vec<_>>();
        entries.extend(
            self.media
                .pending_consumer_user_ids()
                .filter_map(|consumer_user_id| {
                    self.users
                        .get(consumer_user_id)
                        .map(|user| (consumer_user_id.clone(), user.connection_id))
                }),
        );
        entries
    }

    pub fn source_fanout_pressure(
        &self,
        max_fanout_per_source: usize,
        media_worker_for_connection: impl Fn(ConnectionId) -> MediaWorkerId,
    ) -> bool {
        if max_fanout_per_source == 0 {
            return false;
        }
        self.media.sources().any(|source| {
            if !self
                .media
                .producer_for_source(source.source_id())
                .is_some_and(|producer| producer.active)
            {
                return false;
            }
            let mut deliveries_by_worker = BTreeMap::new();
            for key in self.media.consumer_keys_for_source(source.source_id()) {
                if !self.media.has_consumer_setup_or_route(&key) {
                    continue;
                }
                if self
                    .media
                    .consumer_source_selection(&key)
                    .is_some_and(|selection| !selection.delivery_active())
                {
                    continue;
                }
                let Some(user) = self.users.get(&key.consumer_user_id) else {
                    continue;
                };
                let media_worker = media_worker_for_connection(user.connection_id);
                deliveries_by_worker
                    .entry(media_worker)
                    .and_modify(|count: &mut usize| *count = count.saturating_add(1))
                    .or_insert(1);
            }
            !deliveries_by_worker.is_empty()
                && deliveries_by_worker
                    .values()
                    .all(|count| *count >= max_fanout_per_source)
        })
    }

    pub fn reconcile_spillover_routers(
        &mut self,
        spillover: RoomSpilloverMode,
        placement: &mut LoadTriggeredPlacementState,
    ) {
        match spillover {
            RoomSpilloverMode::StrictSingleRouter => {}
            RoomSpilloverMode::BoundedLocalSpillover => {
                let idle_router_ids = self.routing.idle_spillover_routers();
                self.routing.detach_spillover_routers(&idle_router_ids);
                placement.clear_cooldowns(&idle_router_ids);
            }
            RoomSpilloverMode::LoadTriggeredLocalSpillover(policy) => {
                let idle_router_ids = self.routing.idle_spillover_routers();
                let policy = policy.parts();
                let detachments =
                    placement.cooldown_detachments(&idle_router_ids, policy.cooldown_window);
                self.routing.detach_spillover_routers(&detachments);
            }
        }
    }

    pub fn user_connection_id(&self, user_id: &UserId) -> Option<ConnectionId> {
        self.users.get(user_id).map(|user| user.connection_id)
    }

    pub fn user_count(&self) -> usize {
        self.users.len()
    }

    pub fn user_snapshots_except(&self, excluded_user_id: &UserId) -> Vec<PeerSnapshot> {
        self.users
            .iter()
            .filter(|(user_id, _session)| *user_id != excluded_user_id)
            .map(|(user_id, user)| PeerSnapshot {
                user_id: user_id.clone(),
                info: user.project_info(),
            })
            .collect()
    }

    pub fn user_info_snapshot(&self, user_id: &UserId) -> Option<(UserId, UserInfo)> {
        let user = self.users.get(user_id)?;
        Some((user_id.clone(), user.project_info()))
    }

    pub fn user_info_snapshot_all(&self) -> BTreeMap<UserId, UserInfo> {
        self.users
            .iter()
            .map(|(user_id, user)| (user_id.clone(), user.project_info()))
            .collect()
    }

    pub fn user_stats_counts(&self) -> (u64, BTreeMap<UserStreamId, u64>) {
        let mut active_users_by_stream: BTreeMap<UserStreamId, BTreeSet<UserId>> = BTreeMap::new();
        for (stream_id, owner_user_id) in self.media.active_producer_stream_owners() {
            active_users_by_stream
                .entry(stream_id.clone())
                .or_default()
                .insert(owner_user_id.clone());
        }
        let active_stream_counts = active_users_by_stream
            .into_iter()
            .map(|(stream_id, users)| (stream_id, u64::try_from(users.len()).unwrap_or(u64::MAX)))
            .collect();
        (
            u64::try_from(self.users.len()).unwrap_or(u64::MAX),
            active_stream_counts,
        )
    }

    pub fn current_live_consumer_routes(&self) -> impl Iterator<Item = ConsumerRouteView<'_>> {
        self.media.live_consumer_routes().filter(|route| {
            self.user_connection_id(&route.consumer_user_id)
                .is_some_and(|connection_id| connection_id == route.state.consumer_connection_id)
        })
    }

    pub fn source_policy_media_limits(&self) -> RoomMediaLimits {
        self.media_limits
    }

    pub fn source_policy_source(
        &self,
        source_id: PublishedSourceId,
    ) -> Option<&PublishedSourceDescriptor> {
        self.media.source(source_id)
    }

    pub fn source_policy_owner_has_promotable_source_in_group(
        &self,
        owner_user_id: &UserId,
        group: ActiveSpeakerGroup,
    ) -> bool {
        self.media
            .owner_has_promotable_source_in_group(owner_user_id, group)
    }

    pub fn source_policy_layout_preference(
        &self,
        consumer_user_id: &UserId,
        source_user_id: &UserId,
        stream_id: &UserStreamId,
    ) -> Option<VideoLayoutIntent> {
        self.users
            .get(consumer_user_id)
            .and_then(|user| user.desired_source_subscriptions.get(source_user_id))
            .and_then(|states| states.get(stream_id))
            .and_then(|intent| intent.layout())
    }

    pub fn source_policy_user_featured_states(
        &self,
    ) -> impl Iterator<Item = (&UserId, Option<bool>)> {
        self.users
            .iter()
            .map(|(user_id, user)| (user_id, user.featured()))
    }

    pub fn update_source_policy_consumer_selection(
        &mut self,
        route: &ConsumerRouteTransportRef,
        source_id: PublishedSourceId,
        update_selection: impl FnOnce(&mut ConsumerSourceSelection),
    ) {
        self.media
            .update_consumer_source_selection(route, source_id, update_selection);
    }

    pub fn update_source_policy_featured_user(
        &mut self,
        user_id: &UserId,
        featured: Option<bool>,
    ) -> bool {
        let Some(user) = self.users.get_mut(user_id) else {
            return false;
        };
        if user.featured() == featured {
            return false;
        }
        user.set_featured(featured);
        true
    }

    pub fn publication_count(&self) -> usize {
        self.media.publication_count()
    }

    pub fn subscription_count(&self) -> usize {
        self.media.subscription_count()
    }

    pub fn media_counts(&self) -> RoomMediaCounts {
        RoomMediaCounts {
            publications: self.publication_count(),
            subscriptions: self.subscription_count(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.users.is_empty()
    }
}
