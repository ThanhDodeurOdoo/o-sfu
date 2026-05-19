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
    media::{ConsumerRouteView, RelayRouteEffect, RoomMediaGraph},
    presence::UserPresence,
};
use crate::runtime::{
    ConnectionId, RecordingState, UserId,
    media_transport::TransportMediaId,
    router_events::RoomRouterEventSink,
    source_model::{SourceSubscriptionIntent, UserStreamId},
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::room) struct TransportMediaRemoval {
    pub user: UserId,
    pub connection: ConnectionId,
    pub transport_media: TransportMediaId,
}

impl RoomState {
    pub fn new(
        runtime_context: &super::super::RoomRuntimeContext,
        admission_policy: RoomAdmissionPolicy,
        router_rtp_capabilities: MediaCapabilities,
        router_event_sink: Arc<dyn RoomRouterEventSink>,
    ) -> Self {
        Self {
            admission_policy,
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

    pub fn collect_consumer_transport_removals(
        &self,
        departing_user_ids: &BTreeSet<UserId>,
    ) -> Vec<TransportMediaRemoval> {
        let mut keys = BTreeSet::new();
        for user_id in departing_user_ids {
            if let Some(user_keys) = self.media.consumer_keys_by_user.get(user_id) {
                keys.extend(user_keys.iter().cloned());
            }
            if let Some(source_ids) = self.media.source_ids_by_owner.get(user_id) {
                for source_id in source_ids {
                    if let Some(source_keys) = self.media.consumer_keys_by_source.get(source_id) {
                        keys.extend(source_keys.iter().cloned());
                    }
                }
            }
        }
        keys.into_iter()
            .filter_map(|key| {
                let consumer_state = self.media.consumer_index.get(&key)?;
                Some(TransportMediaRemoval {
                    user: key.consumer_user_id,
                    connection: consumer_state.consumer_connection_id,
                    transport_media: consumer_state.consumer_media,
                })
            })
            .collect()
    }

    pub fn collect_producer_transport_removals(
        &self,
        departing_user_ids: &BTreeSet<UserId>,
    ) -> Vec<TransportMediaRemoval> {
        departing_user_ids
            .iter()
            .filter_map(|user_id| self.media.producer_ids_by_owner.get(user_id))
            .flat_map(|producer_ids| producer_ids.iter())
            .filter_map(|producer_id| {
                let producer = self.media.producers.get(producer_id)?;
                let transport_media = producer.transport_media_id?;
                Some(TransportMediaRemoval {
                    user: producer.owner_user_id.clone(),
                    connection: producer.owner_connection_id,
                    transport_media,
                })
            })
            .collect()
    }

    pub fn collect_user_transport_removals(
        &self,
        departing_user_ids: &BTreeSet<UserId>,
    ) -> Vec<TransportMediaRemoval> {
        let mut removals = self.collect_producer_transport_removals(departing_user_ids);
        removals.extend(self.collect_consumer_transport_removals(departing_user_ids));
        removals
    }

    pub fn purge_user_media_state(&mut self, user_id: &UserId) -> Vec<RelayRouteEffect> {
        let mut relay_effects = Vec::new();
        let source_ids = self
            .media
            .take_source_ids_for_owner(user_id)
            .into_iter()
            .collect::<Vec<_>>();
        for source_id in source_ids {
            if let Some((_producer, effects)) = self.media.remove_source(source_id) {
                relay_effects.extend(effects);
            }
        }
        let consumer_keys = self
            .media
            .take_consumer_keys_for_user(user_id)
            .into_iter()
            .collect::<Vec<_>>();
        for key in consumer_keys {
            relay_effects.extend(self.media.remove_consumer_key_state(&key));
        }
        relay_effects
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
            .consumer_index
            .iter()
            .map(|(key, state)| (key.consumer_user_id.clone(), state.consumer_connection_id))
            .collect::<Vec<_>>();
        entries.extend(
            self.media
                .pending_consumer_bootstraps
                .iter()
                .filter_map(|key| {
                    self.users
                        .get(&key.consumer_user_id)
                        .map(|user| (key.consumer_user_id.clone(), user.connection_id))
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
        self.media.sources.len()
    }

    pub fn subscription_count(&self) -> usize {
        self.media
            .consumer_index
            .len()
            .saturating_add(self.media.pending_consumer_bootstraps.len())
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

impl TransportMediaRemoval {
    pub fn user(&self) -> &UserId {
        &self.user
    }

    pub const fn connection(&self) -> ConnectionId {
        self.connection
    }

    pub const fn transport_media(&self) -> TransportMediaId {
        self.transport_media
    }
}
