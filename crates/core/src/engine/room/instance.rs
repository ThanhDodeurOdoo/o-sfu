use std::{
    fmt,
    sync::{Arc, Mutex},
};

use tokio::sync::RwLock;

use super::{
    definition::RoomDefinition, factory::RoomInit, placement::LoadTriggeredPlacementState,
    state::RoomState,
};
use crate::{
    RoomWorkerPolicy,
    engine::{
        AvailableFeatures, ConnectionId, MediaWorkerId, PeerSnapshot, RecordingState,
        RoomInstanceId, UserId,
        diagnostics::DiagnosticsStore,
        media_transport::{MediaTransport, TransportSessionKey},
        metrics::RuntimeMetrics,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoomJoinError {
    RoomFull,
    RouterState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoomManagerJoinError {
    MissingRoom,
    RoomFull,
    RouterState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RoomMediaCounts {
    pub publications: usize,
    pub subscriptions: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct RoomUserOperation<'a> {
    pub room: &'a Room,
    pub user_id: &'a UserId,
    pub connection_id: ConnectionId,
    pub media_transport: &'a MediaTransport,
}

pub struct Room {
    pub(super) diagnostics: Arc<DiagnosticsStore>,
    pub(super) definition: RoomDefinition,
    pub(super) load_triggered_placement: Mutex<LoadTriggeredPlacementState>,
    pub(super) metrics: Arc<RuntimeMetrics>,
    pub(super) state: RwLock<RoomState>,
}

impl Room {
    pub(super) fn new(init: RoomInit) -> Self {
        let RoomInit {
            runtime_context,
            runtime_policy,
            issuer,
            key,
            config,
            services,
        } = init;
        let definition =
            RoomDefinition::new(&runtime_context, &runtime_policy, issuer, key, config);
        Self {
            diagnostics: services.diagnostics,
            definition,
            load_triggered_placement: Mutex::new(LoadTriggeredPlacementState::default()),
            metrics: services.metrics,
            state: RwLock::new(RoomState::new(
                &runtime_context,
                runtime_policy.admission_policy,
                runtime_policy.media_limits,
                runtime_policy.router_rtp_capabilities,
            )),
        }
    }

    pub(crate) fn user_operation<'a>(
        &'a self,
        user_id: &'a UserId,
        connection_id: ConnectionId,
        media_transport: &'a MediaTransport,
    ) -> RoomUserOperation<'a> {
        RoomUserOperation {
            room: self,
            user_id,
            connection_id,
            media_transport,
        }
    }

    #[must_use]
    pub fn uuid(&self) -> &str {
        self.definition.uuid()
    }

    #[must_use]
    pub fn issuer(&self) -> &str {
        self.definition.issuer()
    }

    #[must_use]
    pub fn key(&self) -> &str {
        self.definition.key()
    }

    #[must_use]
    pub(crate) fn available_features(&self) -> AvailableFeatures {
        self.definition.available_features()
    }

    pub async fn recording_state(&self) -> RecordingState {
        self.state.read().await.recording_state()
    }

    pub(crate) async fn user_snapshots_except(
        &self,
        excluded_user_id: &UserId,
    ) -> Vec<PeerSnapshot> {
        self.state
            .read()
            .await
            .user_snapshots_except(excluded_user_id)
    }

    #[must_use]
    pub fn web_rtc_enabled(&self) -> bool {
        self.definition.web_rtc_enabled()
    }

    #[must_use]
    pub async fn assigned_primary_media_worker_id(&self) -> Option<MediaWorkerId> {
        self.state.read().await.assigned_primary_media_worker_id()
    }

    #[must_use]
    pub async fn transport_user_key(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> TransportSessionKey {
        self.state
            .read()
            .await
            .transport_user_key(user_id, connection_id)
    }

    pub fn room_worker_policy(&self) -> RoomWorkerPolicy {
        self.definition.room_worker_policy()
    }

    #[must_use]
    pub(crate) fn instance_id(&self) -> RoomInstanceId {
        self.definition.instance_id()
    }
}

impl fmt::Debug for Room {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let media_worker_id = self
            .state
            .try_read()
            .ok()
            .and_then(|state| state.assigned_primary_media_worker_id())
            .map(MediaWorkerId::as_usize);
        formatter
            .debug_struct("Room")
            .field("instance_id", &self.definition.instance_id())
            .field("media_worker_id", &media_worker_id)
            .field("uuid", &self.definition.uuid())
            .field("issuer", &self.definition.issuer())
            .field("web_rtc_enabled", &self.definition.web_rtc_enabled())
            .finish_non_exhaustive()
    }
}
