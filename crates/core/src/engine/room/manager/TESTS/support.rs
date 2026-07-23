use std::sync::Arc;

use o_sfu_router::test_support::rtp_samples::sample_client_rtp_capabilities;

#[cfg(any(test, feature = "testing-transport"))]
use super::JoinPlacementTestGate;
use super::{
    super::{RoomAdmissionPolicy, RoomRuntimePolicy},
    RoomManager, RoomManagerConfig,
};
#[cfg(any(test, feature = "testing-transport"))]
use crate::engine::sync::lock_unpoisoned;
use crate::{RoomMediaLimits, RuntimeFeatureFlags, engine::metrics::RuntimeMetrics};

const DEFAULT_TEST_MAX_SESSIONS: usize = 100;

impl RoomManager {
    #[must_use]
    pub fn for_test() -> Self {
        Self::for_test_with_config(RoomManagerConfig::new(
            1,
            test_runtime_policy(RoomAdmissionPolicy::new(DEFAULT_TEST_MAX_SESSIONS)),
        ))
    }

    #[must_use]
    pub fn for_test_with_admission_policy(admission_policy: RoomAdmissionPolicy) -> Self {
        Self::for_test_with_config(RoomManagerConfig::new(
            1,
            test_runtime_policy(admission_policy),
        ))
    }

    #[must_use]
    pub fn for_test_with_media_limits(media_limits: RoomMediaLimits) -> Self {
        Self::for_test_with_config(RoomManagerConfig::new(
            1,
            test_runtime_policy(RoomAdmissionPolicy::new(DEFAULT_TEST_MAX_SESSIONS))
                .with_media_limits(media_limits),
        ))
    }

    #[must_use]
    pub fn for_test_with_config(config: RoomManagerConfig) -> Self {
        Self::new(config, Arc::new(RuntimeMetrics::default()))
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn set_join_placement_gate_for_test(&self, gate: Arc<JoinPlacementTestGate>) {
        *lock_unpoisoned(&self.join_placement_gate) = Some(gate);
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn join_placement_gate_for_test(&self) -> Option<Arc<JoinPlacementTestGate>> {
        lock_unpoisoned(&self.join_placement_gate).clone()
    }
}

fn test_runtime_policy(admission_policy: RoomAdmissionPolicy) -> RoomRuntimePolicy {
    RoomRuntimePolicy::new(
        admission_policy,
        RuntimeFeatureFlags::default(),
        sample_client_rtp_capabilities(),
    )
}
