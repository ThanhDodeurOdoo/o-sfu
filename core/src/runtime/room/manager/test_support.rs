use std::sync::Arc;

use super::{
    super::{RoomAdmissionPolicy, RoomRuntimePolicy, rtp_capabilities::router_rtp_capabilities},
    RoomManager, RoomManagerConfig, RoomManagerDeps,
};
use crate::{
    MediaCodecFlags, RuntimeFeatureFlags,
    runtime::{diagnostics::DiagnosticsStore, metrics::RuntimeMetrics, recording::MediaTap},
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
}
