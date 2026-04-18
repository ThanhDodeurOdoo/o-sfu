use std::sync::Arc;

use crate::config::{MediaCodecFlags, RuntimeFeatureFlags};
use crate::runtime::metrics::RuntimeMetrics;
use crate::runtime::recording::MediaTap;
use crate::runtime::transport_adapter::RuntimeTransportAdapter;
use o_sfu_protocol::shared::SessionId;

use super::super::{
    ChannelAdmissionPolicy, ChannelRuntimePolicy, SessionCleanupPolicy,
    rtp_capabilities::router_rtp_capabilities,
};
use super::{ChannelManager, ChannelManagerConfig};

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
            Arc::new(RuntimeMetrics::default()),
        )
    }

    pub async fn leave_session_for_test(
        &self,
        channel_uuid: &str,
        session_id: &SessionId,
        connection_id: u64,
        transport_adapter: &RuntimeTransportAdapter,
        cleanup_policy: SessionCleanupPolicy,
    ) -> bool {
        let Some((channel, session_count_before, did_remove_active_session)) = self
            .with_current_channel(channel_uuid, |channel| async move {
                let session_count_before = channel.session_count().await;
                let did_remove_active_session = channel
                    .leave_session_runtime(
                        session_id,
                        connection_id,
                        transport_adapter,
                        cleanup_policy,
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
}

impl Default for ChannelManager {
    fn default() -> Self {
        Self::for_test()
    }
}
