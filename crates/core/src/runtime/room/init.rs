//! room creation policy and initialization inputs

use std::sync::Arc;

use o_sfu_router::MediaCapabilities;

use super::placement::RoomRuntimeContext;
use crate::{
    RoomMediaLimits, RoomWorkerPolicy, RuntimeFeatureFlags,
    runtime::{
        diagnostics::DiagnosticsStore, metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry,
    },
};

/// admission limits that stay fixed for one room lifetime
///
/// this is kept separate from the wider runtime policy because admission is a
/// narrow concern with its own tests and state checks
///
/// the policy is passed into `RoomState` at construction time and then treated
/// as immutable room configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoomAdmissionPolicy {
    /// maximum number of live users the room accepts at once
    ///
    /// replaced connections still consume this budget until the room transition
    /// finishes and the old live user has been removed
    pub max_sessions: usize,
}

impl RoomAdmissionPolicy {
    #[must_use]
    pub const fn new(max_sessions: usize) -> Self {
        Self { max_sessions }
    }
}

/// stable runtime policy bundle shared by the room and its state model
///
/// this groups the room rules that are fixed for the room lifetime and read by
/// more than one boundary during join, negotiation and observability work
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomRuntimePolicy {
    /// room-level admission limits enforced by room state
    pub admission_policy: RoomAdmissionPolicy,
    /// feature surface the room advertises to clients
    pub feature_flags: RuntimeFeatureFlags,
    /// router-native capability baseline used for negotiation and bootstrap
    pub router_rtp_capabilities: MediaCapabilities,
    /// same-room local worker-placement policy selected at runtime boot
    pub room_worker_policy: RoomWorkerPolicy,
    /// room media activation caps applied by source policy
    pub media_limits: RoomMediaLimits,
}

impl RoomRuntimePolicy {
    #[must_use]
    pub fn new(
        admission_policy: RoomAdmissionPolicy,
        feature_flags: RuntimeFeatureFlags,
        router_rtp_capabilities: MediaCapabilities,
    ) -> Self {
        Self {
            admission_policy,
            feature_flags,
            router_rtp_capabilities,
            room_worker_policy: RoomWorkerPolicy::strict_single_router(),
            media_limits: RoomMediaLimits::default(),
        }
    }

    /// return a room policy that uses the provided same-room worker policy
    #[must_use]
    pub fn with_room_worker_policy(mut self, room_worker_policy: RoomWorkerPolicy) -> Self {
        self.room_worker_policy = room_worker_policy;
        self
    }

    /// return a room policy that uses the provided media activation limits
    #[must_use]
    pub fn with_media_limits(mut self, media_limits: RoomMediaLimits) -> Self {
        self.media_limits = media_limits;
        self
    }
}

/// external room config passed in from the http or runtime edge
///
/// this type keeps room identity separate from operator-facing knobs and
/// compatibility toggles that may be chosen per room at creation time
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomConfig {
    /// whether this room should expose WebRTC to clients at all
    pub web_rtc_enabled: bool,
    /// compatibility recording address from `/v1/channel`
    pub recording_address: Option<String>,
}

impl Default for RoomConfig {
    fn default() -> Self {
        Self {
            web_rtc_enabled: true,
            recording_address: None,
        }
    }
}

/// shared services injected into each room at construction time
///
/// these handles are process-wide services rather than room policy
/// keeping
/// them in one bundle makes `RoomInit` express construction ownership without a
/// long positional argument list
#[derive(Debug, Clone)]
pub(crate) struct RoomServices {
    pub(crate) diagnostics: Arc<DiagnosticsStore>,
    pub(crate) packet_sink_registry: Arc<RoomPacketSinkRegistry>,
    pub(crate) metrics: Arc<RuntimeMetrics>,
}

impl RoomServices {
    pub(crate) fn new(
        diagnostics: Arc<DiagnosticsStore>,
        packet_sink_registry: Arc<RoomPacketSinkRegistry>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            diagnostics,
            packet_sink_registry,
            metrics,
        }
    }
}

pub(crate) struct RoomInit {
    /// runtime-local instance and primary placement for the room
    pub(crate) runtime_context: RoomRuntimeContext,
    /// validated room policy copied from runtime startup
    pub(crate) runtime_policy: RoomRuntimePolicy,
    /// compatibility-facing issuer captured at room creation
    pub(crate) issuer: String,
    /// room key captured from the first create request
    pub(crate) key: String,
    /// room-level compatibility configuration
    pub(crate) config: RoomConfig,
    /// process services needed by the room facade and observers
    pub(crate) services: RoomServices,
}
