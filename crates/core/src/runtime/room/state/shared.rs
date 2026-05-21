use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use o_sfu_router::{MediaCapabilities, MediaCapabilities as RouterRtpCapabilities};

use super::{
    super::{
        RoomAdmissionPolicy, RoomMediaCounts, RoomUserPermissions,
        outbound::OutboundSender,
        placement::RoomPlacementUsageSnapshot,
        topology::{RoomRouterStateFactory, RoomTopology},
        user_negotiation::UserNegotiation,
    },
    layout::UserLayout,
    media::{ConsumerRouteView, RelayRouteEffect, RoomMediaGraph, TransportMediaRemoval},
    presence::UserPresence,
};
use crate::{
    RoomMediaLimits, RoomSpilloverMode,
    runtime::{
        ConnectionId, RecordingState, UserId,
        room::placement::LoadTriggeredPlacementState,
        router_events::RoomRouterEventSink,
        source_model::{SourceSubscriptionIntent, UserStreamId},
    },
};

/// Core mutable state for a single SFU room (room).
///
/// Owns room-level user state and the room media graph. Every mutation returns
/// an `*Outcome` value that carries deferred side effects such as fanout
/// messages or kicked senders. The caller is responsible for calling `.emit()`
/// on outcomes after releasing any lock on this state so the critical section
/// stays pure and non-blocking.
///
/// The two-phase patterns (`prepare_*` / `commit_*`) allow async transport work
/// to happen between phases without holding the state lock.
#[derive(Debug)]
pub(in crate::runtime::room) struct RoomState {
    pub(super) admission_policy: RoomAdmissionPolicy,
    pub(super) media_limits: RoomMediaLimits,
    pub(super) users: BTreeMap<UserId, ActiveUser>,
    /// Monotonically increasing: each join, including re-joins, gets a fresh id
    /// so stale async callbacks from a previous connection are rejected.
    pub(super) next_connection_id: u64,
    pub(super) next_source_id: u64,
    pub(super) next_source_encoding_id: u64,
    pub(super) next_producer_id: u64,
    pub(super) next_consumer_id: u64,
    pub(super) recording_state: RecordingState,
    pub(super) media: RoomMediaGraph,
    /// Shadow of user/producer/consumer state inside the pure router core.
    pub(super) topology: RoomTopology,
}

#[derive(Debug)]
pub(in crate::runtime::room) struct ActiveUser {
    #[allow(
        dead_code,
        reason = "stored for future user display and recording metadata"
    )]
    pub(super) label: Option<String>,
    #[allow(dead_code, reason = "stored for future permission-gated actions")]
    pub(super) permissions: RoomUserPermissions,
    pub(super) presence: UserPresence,
    pub(super) layout: UserLayout,
    pub(super) negotiation: UserNegotiation,
    pub(super) desired_source_subscriptions:
        BTreeMap<UserId, BTreeMap<UserStreamId, SourceSubscriptionIntent>>,
    pub(super) parsed_client_rtp_capabilities: Option<RouterRtpCapabilities>,
    pub(super) connection_id: ConnectionId,
    pub(super) sender: OutboundSender,
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
            topology: RoomTopology::new_with_router_state_factory(
                runtime_context.local_routers().clone(),
                router_rtp_capabilities,
                &RoomRouterStateFactory::new(router_event_sink),
            ),
        }
    }

    pub fn collect_user_transport_removals(
        &self,
        departing_user_ids: &BTreeSet<UserId>,
    ) -> Vec<TransportMediaRemoval> {
        self.media.transport_removals_for_users(departing_user_ids)
    }

    pub fn purge_user_media_state(&mut self, user_id: &UserId) -> Vec<RelayRouteEffect> {
        self.media.remove_user_media(user_id)
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

    pub fn router_rtp_capabilities(&self) -> MediaCapabilities {
        self.topology.rtp_capabilities().clone()
    }

    pub fn transport_user_entries(&self) -> Vec<(UserId, ConnectionId)> {
        self.users
            .iter()
            .map(|(user_id, user)| (user_id.clone(), user.connection_id))
            .collect()
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

    pub fn placement_usage_snapshot(&self) -> RoomPlacementUsageSnapshot {
        RoomPlacementUsageSnapshot::new(
            self.topology.primary_router_id(),
            self.topology.has_assigned_local_placements(),
            self.topology.local_placements(),
        )
    }

    pub fn source_fanout_pressure(&self, max_fanout_per_source: usize) -> bool {
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
                if !self.media.consumer_bootstrap_exists(&key) {
                    continue;
                }
                if self
                    .media
                    .consumer_source_selection(&key)
                    .is_some_and(|selection| !selection.active())
                {
                    continue;
                }
                let Some(placement) = self.topology.home_placement_for_user(&key.consumer_user_id)
                else {
                    continue;
                };
                deliveries_by_worker
                    .entry(placement.media_worker)
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
                let idle_router_ids = self.topology.idle_spillover_routers();
                self.topology.detach_spillover_routers(&idle_router_ids);
                placement.clear_cooldowns(&idle_router_ids);
            }
            RoomSpilloverMode::LoadTriggeredLocalSpillover(policy) => {
                let idle_router_ids = self.topology.idle_spillover_routers();
                let detachments =
                    placement.cooldown_detachments(&idle_router_ids, policy.cooldown_window());
                self.topology.detach_spillover_routers(&detachments);
            }
        }
    }

    pub fn user_connection_id(&self, user_id: &UserId) -> Option<ConnectionId> {
        self.users.get(user_id).map(|user| user.connection_id)
    }

    pub fn user_count(&self) -> usize {
        self.users.len()
    }

    pub(super) fn current_live_consumer_routes(
        &self,
    ) -> impl Iterator<Item = ConsumerRouteView<'_>> {
        self.media.live_consumer_routes().filter(|route| {
            self.user_connection_id(&route.consumer_user_id)
                .is_some_and(|connection_id| connection_id == route.state.consumer_connection_id)
        })
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
