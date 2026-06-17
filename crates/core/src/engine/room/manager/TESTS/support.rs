use std::sync::Arc;

#[cfg(test)]
use {crate::engine::sync::lock_unpoisoned, std::fmt, tokio::sync::Barrier};

use super::{
    super::{RoomAdmissionPolicy, RoomRuntimePolicy, rtp_capabilities::router_rtp_capabilities},
    RoomManager, RoomManagerConfig, RoomManagerDeps,
};
use crate::{
    MediaCodecFlags, RoomMediaLimits, RuntimeFeatureFlags,
    engine::{diagnostics::DiagnosticsStore, metrics::RuntimeMetrics},
};

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
        Self::new(
            config,
            RoomManagerDeps {
                diagnostics: Arc::new(DiagnosticsStore::default()),
                metrics: Arc::new(RuntimeMetrics::default()),
            },
        )
    }

    #[cfg(test)]
    pub fn set_join_placement_gate_for_test(&self, gate: Arc<JoinPlacementTestGate>) {
        *lock_unpoisoned(&self.join_placement_gate) = Some(gate);
    }

    #[cfg(test)]
    pub(super) async fn wait_after_join_placement_for_test(&self) {
        let gate = lock_unpoisoned(&self.join_placement_gate).as_ref().cloned();
        if let Some(gate) = gate {
            gate.wait_after_planning().await;
        }
    }
}

fn test_runtime_policy(admission_policy: RoomAdmissionPolicy) -> RoomRuntimePolicy {
    RoomRuntimePolicy::new(
        admission_policy,
        RuntimeFeatureFlags::default(),
        router_rtp_capabilities(MediaCodecFlags::default()),
    )
}

#[cfg(test)]
pub struct JoinPlacementTestGate {
    planned: Barrier,
    release: Barrier,
}

#[cfg(test)]
impl JoinPlacementTestGate {
    pub fn new(expected: usize) -> Self {
        Self {
            planned: Barrier::new(expected + 1),
            release: Barrier::new(expected + 1),
        }
    }

    async fn wait_after_planning(&self) {
        self.planned.wait().await;
        self.release.wait().await;
    }

    pub async fn hold_all_planned(&self) {
        self.planned.wait().await;
    }

    pub async fn release_all(&self) {
        self.release.wait().await;
    }
}

#[cfg(test)]
impl fmt::Debug for JoinPlacementTestGate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JoinPlacementTestGate")
            .finish_non_exhaustive()
    }
}
