use std::sync::Arc;

use crate::config::{MediaCodecFlags, RuntimeFeatureFlags};
use crate::runtime::diagnostics::DiagnosticsStore;
use crate::runtime::recording::MediaTap;
use crate::runtime::transport_adapter::RuntimeTransportAdapter;
use crate::runtime::{ConnectionId, metrics::RuntimeMetrics};
use o_sfu_protocol::shared::SessionId;

use super::super::{
    ChannelAdmissionPolicy, ChannelRuntimePolicy, rtp_capabilities::router_rtp_capabilities,
};
use super::{ChannelManager, ChannelManagerConfig, ChannelManagerJoinError, JoinSessionRequest};

const DEFAULT_TEST_MAX_SESSIONS: usize = 100;

impl ChannelManager {
    #[must_use]
    pub fn for_test() -> Self {
        Self::for_test_with_media_workers(1)
    }

    #[must_use]
    pub fn for_test_with_media_workers(media_worker_count: usize) -> Self {
        Self::for_test_with_config(ChannelManagerConfig::new(
            media_worker_count,
            ChannelRuntimePolicy::new(
                ChannelAdmissionPolicy::new(DEFAULT_TEST_MAX_SESSIONS),
                RuntimeFeatureFlags::default(),
                router_rtp_capabilities(MediaCodecFlags::default()),
            ),
        ))
    }

    #[must_use]
    pub fn for_test_with_admission_policy(admission_policy: ChannelAdmissionPolicy) -> Self {
        Self::for_test_with_config(ChannelManagerConfig::new(
            1,
            ChannelRuntimePolicy::new(
                admission_policy,
                RuntimeFeatureFlags::default(),
                router_rtp_capabilities(MediaCodecFlags::default()),
            ),
        ))
    }

    #[must_use]
    pub fn for_test_with_config(config: ChannelManagerConfig) -> Self {
        Self::new(
            config,
            Arc::new(MediaTap::default()),
            Arc::new(DiagnosticsStore::default()),
            Arc::new(RuntimeMetrics::default()),
        )
    }

    pub async fn join_session_for_test(
        &self,
        channel_uuid: &str,
        request: JoinSessionRequest,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Result<(Arc<super::super::Channel>, ConnectionId), ChannelManagerJoinError> {
        let Some((channel, session_count_before, join_result)) = self
            .with_current_channel(channel_uuid, |channel| async move {
                let session_count_before = channel.session_count().await;
                let join_result = channel
                    .test_api()
                    .lifecycle()
                    .join_session_without_transport_cleanup(
                        request.session_id,
                        request.label,
                        request.permissions,
                        request.sender,
                        transport_adapter,
                    )
                    .await;
                (channel, session_count_before, join_result)
            })
            .await
        else {
            return Err(ChannelManagerJoinError::MissingChannel);
        };
        let connection_id = join_result.map_err(|error| match error {
            super::super::ChannelJoinError::ChannelFull => ChannelManagerJoinError::ChannelFull,
            super::super::ChannelJoinError::RouterState => ChannelManagerJoinError::RouterState,
        })?;
        self.record_active_session_delta(session_count_before, channel.session_count().await);
        Ok((channel, connection_id))
    }

    pub async fn leave_session_for_test(
        &self,
        channel_uuid: &str,
        session_id: &SessionId,
        connection_id: ConnectionId,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        let Some((channel, session_count_before, did_remove_active_session)) = self
            .with_current_channel(channel_uuid, |channel| async move {
                let session_count_before = channel.session_count().await;
                let did_remove_active_session = channel
                    .test_api()
                    .lifecycle()
                    .leave_session_without_transport_cleanup(
                        session_id,
                        connection_id,
                        transport_adapter,
                    )
                    .await;
                (channel, session_count_before, did_remove_active_session)
            })
            .await
        else {
            return false;
        };
        self.finish_session_mutation(
            channel_uuid,
            &channel,
            session_count_before,
            did_remove_active_session,
        )
        .await;
        did_remove_active_session
    }

    pub async fn disconnect_sessions_for_test(
        &self,
        channel_uuid: &str,
        session_ids: &[SessionId],
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let Some((channel, session_count_before)) = self
            .with_current_channel(channel_uuid, |channel| async move {
                let session_count_before = channel.session_count().await;
                channel
                    .test_api()
                    .lifecycle()
                    .disconnect_sessions_without_transport_cleanup(session_ids, transport_adapter)
                    .await;
                (channel, session_count_before)
            })
            .await
        else {
            return;
        };
        self.finish_session_mutation(channel_uuid, &channel, session_count_before, true)
            .await;
    }
}

impl Default for ChannelManager {
    fn default() -> Self {
        Self::for_test()
    }
}
