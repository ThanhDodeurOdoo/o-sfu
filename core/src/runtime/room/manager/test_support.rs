use std::sync::Arc;

use super::{
    super::{RoomAdmissionPolicy, RoomRuntimePolicy, rtp_capabilities::router_rtp_capabilities},
    JoinUserRequest, RoomManager, RoomManagerConfig, RoomManagerDeps, RoomManagerJoinError,
};
use crate::{
    MediaCodecFlags, RuntimeFeatureFlags,
    runtime::{
        ConnectionId, UserId, diagnostics::DiagnosticsStore, media_transport::MediaTransport,
        metrics::RuntimeMetrics, recording::MediaTap,
    },
};

const DEFAULT_TEST_MAX_SESSIONS: usize = 100;

impl RoomManager {
    #[must_use]
    pub fn for_test() -> Self {
        Self::for_test_with_media_workers(1)
    }

    #[must_use]
    pub fn for_test_with_media_workers(media_worker_count: usize) -> Self {
        Self::for_test_with_config(RoomManagerConfig::new(
            media_worker_count,
            RoomRuntimePolicy::new(
                RoomAdmissionPolicy::new(DEFAULT_TEST_MAX_SESSIONS),
                RuntimeFeatureFlags::default(),
                router_rtp_capabilities(MediaCodecFlags::default()),
            ),
        ))
    }

    #[must_use]
    pub fn for_test_with_admission_policy(admission_policy: RoomAdmissionPolicy) -> Self {
        Self::for_test_with_config(RoomManagerConfig::new(
            1,
            RoomRuntimePolicy::new(
                admission_policy,
                RuntimeFeatureFlags::default(),
                router_rtp_capabilities(MediaCodecFlags::default()),
            ),
        ))
    }

    #[must_use]
    pub fn for_test_with_config(config: RoomManagerConfig) -> Self {
        Self::new(
            config,
            RoomManagerDeps {
                recording_media_tap: Arc::new(MediaTap::default()),
                diagnostics: Arc::new(DiagnosticsStore::default()),
                metrics: Arc::new(RuntimeMetrics::default()),
            },
        )
    }

    pub async fn join_session_for_test(
        &self,
        room_id: &str,
        request: JoinUserRequest,
        media_transport: &MediaTransport,
    ) -> Result<(Arc<super::super::Room>, ConnectionId), RoomManagerJoinError> {
        let Some((room, user_count_before, media_counts_before, join_result)) = self
            .with_current_room(room_id, |room| async move {
                let user_count_before = room.user_count().await;
                let media_counts_before = room.media_counts().await;
                let join_result = room
                    .test_api()
                    .lifecycle()
                    .join_session_without_transport_cleanup(
                        request.user_id,
                        request.label,
                        request.permissions,
                        request.sender,
                        media_transport,
                    )
                    .await;
                (room, user_count_before, media_counts_before, join_result)
            })
            .await
        else {
            return Err(RoomManagerJoinError::MissingRoom);
        };
        let connection_id = join_result.map_err(|error| match error {
            super::super::RoomJoinError::RoomFull => RoomManagerJoinError::RoomFull,
            super::super::RoomJoinError::RouterState => RoomManagerJoinError::RouterState,
        })?;
        self.record_live_count_deltas(
            user_count_before,
            media_counts_before,
            room.user_count().await,
            room.media_counts().await,
        );
        Ok((room, connection_id))
    }

    pub async fn leave_session_for_test(
        &self,
        room_id: &str,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_transport: &MediaTransport,
    ) -> bool {
        let Some((room, user_count_before, media_counts_before, did_remove_active_session)) = self
            .with_current_room(room_id, |room| async move {
                let user_count_before = room.user_count().await;
                let media_counts_before = room.media_counts().await;
                let did_remove_active_session = room
                    .test_api()
                    .lifecycle()
                    .leave_session_without_transport_cleanup(
                        user_id,
                        connection_id,
                        media_transport,
                    )
                    .await;
                (
                    room,
                    user_count_before,
                    media_counts_before,
                    did_remove_active_session,
                )
            })
            .await
        else {
            return false;
        };
        self.finish_session_mutation(
            room_id,
            &room,
            user_count_before,
            media_counts_before,
            did_remove_active_session,
        )
        .await;
        did_remove_active_session
    }

    pub async fn disconnect_sessions_for_test(
        &self,
        room_id: &str,
        user_ids: &[UserId],
        media_transport: &MediaTransport,
    ) {
        let Some((room, user_count_before, media_counts_before)) = self
            .with_current_room(room_id, |room| async move {
                let user_count_before = room.user_count().await;
                let media_counts_before = room.media_counts().await;
                room.test_api()
                    .lifecycle()
                    .disconnect_users_without_transport_cleanup(user_ids, media_transport)
                    .await;
                (room, user_count_before, media_counts_before)
            })
            .await
        else {
            return;
        };
        self.finish_session_mutation(room_id, &room, user_count_before, media_counts_before, true)
            .await;
    }
}

impl Default for RoomManager {
    fn default() -> Self {
        Self::for_test()
    }
}
