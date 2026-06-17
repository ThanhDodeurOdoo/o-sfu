use std::{
    fmt,
    sync::{Arc, Mutex},
};

use tokio::sync::RwLock;

use super::{
    cleanup::CleanupReconciler, definition::RoomDefinition, init::RoomInit,
    operation::RoomUserOperation, placement::LoadTriggeredPlacementState, state::RoomState,
    transition::StagedPublishes,
};
#[cfg(test)]
use crate::engine::media_transport::TransportMediaId;
use crate::{
    RoomSpilloverMode, RoomWorkerPolicy,
    engine::{
        AvailableFeatures, ConnectionId, MediaWorkerId, PeerSnapshot, RecordingState,
        RoomInstanceId, UserId, diagnostics::DiagnosticsStore, media_transport::MediaTransport,
        metrics::RuntimeMetrics, sync::lock_unpoisoned,
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

pub struct Room {
    pub(super) diagnostics: Arc<DiagnosticsStore>,
    pub(super) definition: RoomDefinition,
    pub(super) load_triggered_placement: Mutex<LoadTriggeredPlacementState>,
    pub(super) metrics: Arc<RuntimeMetrics>,
    pub(super) cleanup_reconciler: Mutex<CleanupReconciler>,
    pub(super) staged_publishes: StagedPublishes,
    pub(super) state: RwLock<RoomState>,
    #[cfg(test)]
    pub(super) duplicate_staged_publish_after_reservation: Mutex<Option<TransportMediaId>>,
    #[cfg(test)]
    pub(super) duplicate_staged_publish_cleanup_target: Mutex<Option<TransportMediaId>>,
}

impl Room {
    pub(crate) fn new(init: RoomInit) -> Self {
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
            cleanup_reconciler: Mutex::new(CleanupReconciler::default()),
            staged_publishes: StagedPublishes::default(),
            #[cfg(test)]
            duplicate_staged_publish_after_reservation: Mutex::new(None),
            #[cfg(test)]
            duplicate_staged_publish_cleanup_target: Mutex::new(None),
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
        RoomUserOperation::new(self, user_id, connection_id, media_transport)
    }

    #[must_use]
    pub fn uuid(&self) -> &str {
        self.definition.uuid()
    }

    pub async fn reconcile_spillover_routers(&self) {
        let spillover = self.room_worker_policy().spillover();
        if matches!(spillover, RoomSpilloverMode::StrictSingleRouter) {
            return;
        }
        let mut state = self.state.write().await;
        let mut placement = lock_unpoisoned(&self.load_triggered_placement);
        state.reconcile_spillover_routers(spillover, &mut placement);
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
