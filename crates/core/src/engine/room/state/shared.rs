use std::collections::BTreeMap;

use o_sfu_router::rtp::{MediaCapabilities, MediaCapabilities as RouterRtpCapabilities};

use super::super::{
    RoomAdmissionPolicy, RoomMediaCounts, RoomUserPermissions,
    media_graph::{ConsumerRouteView, RoomTopology},
    outbound::OutboundSender,
    transition::StagedPublishes,
    user_negotiation::UserNegotiation,
};
use crate::{
    RoomMediaLimits, RoomSpilloverMode,
    engine::{
        ConnectionId, MediaWorkerId, PeerSnapshot, RecordingState, UserId, UserInfo,
        media_transport::TransportSessionKey,
        room::placement::{LoadTriggeredPlacementState, RoutingPlacementSnapshot},
        source_model::{SourceSubscriptionIntent, UserStreamId},
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
    pub(in crate::engine::room) staged_publishes: StagedPublishes,
    pub(in crate::engine::room) topology: RoomTopology,
}

#[derive(Debug)]
pub struct ActiveUser {
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

    pub(in crate::engine::room) const fn featured(&self) -> Option<bool> {
        self.server_featured
    }

    pub(in crate::engine::room) fn set_featured(&mut self, featured: Option<bool>) {
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
            staged_publishes: StagedPublishes::default(),
            topology: RoomTopology::new(runtime_context, router_rtp_capabilities),
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
        self.topology.routing().rtp_capabilities().clone()
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
        self.topology.transport_user_key(user_id, connection_id)
    }

    pub fn committed_transport_user_key(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<TransportSessionKey> {
        self.topology
            .committed_transport_user_key(user_id, connection_id)
    }

    pub fn placement_usage_snapshot(&self) -> RoutingPlacementSnapshot {
        self.topology.routing().usage_snapshot()
    }

    pub fn assigned_primary_media_worker_id(&self) -> Option<MediaWorkerId> {
        self.topology.routing().assigned_primary_media_worker_id()
    }

    pub(in crate::engine::room) fn live_consumer_routes(
        &self,
    ) -> impl Iterator<Item = ConsumerRouteView<'_>> {
        self.topology.live_consumer_routes().filter(|route| {
            self.user_connection_id(&route.consumer_user_id)
                == Some(route.state.consumer_connection_id)
        })
    }

    pub fn source_fanout_pressure(&self, max_fanout_per_source: usize) -> bool {
        self.topology
            .source_fanout_pressure(max_fanout_per_source, |user_id| {
                self.user_connection_id(user_id)
            })
    }

    pub fn reconcile_spillover_routers(
        &mut self,
        spillover: RoomSpilloverMode,
        placement: &mut LoadTriggeredPlacementState,
    ) {
        self.topology
            .reconcile_spillover_routers(spillover, placement);
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
        (
            u64::try_from(self.users.len()).unwrap_or(u64::MAX),
            self.topology.active_stream_user_counts(),
        )
    }

    pub fn media_counts(&self) -> RoomMediaCounts {
        self.topology.media_counts()
    }

    pub fn is_empty(&self) -> bool {
        self.users.is_empty()
    }
}
