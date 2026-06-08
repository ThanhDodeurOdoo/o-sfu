mod catalog;
mod counter;
mod descriptor;
mod labels;
mod rtc;
mod rtp;
mod snapshot;

#[cfg(any(test, feature = "test-support"))]
#[path = "TESTS/test_support.rs"]
pub mod test_support;

#[cfg(test)]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;

pub use self::{
    catalog::RuntimeMetrics,
    descriptor::MetricName,
    labels::{
        BudgetSolverOutcome, HttpRoute, MediaQualityLossDirection, MediaQualitySample,
        RtcDatagramDropReason, RtcDatagramRoutePath, RtcKeyframeRequestOutcome,
        RtcRelayEnqueueResult, RtcRemoteControlDropKind, RtcRemotePacketGateConvergence,
        RtcRouteControlOutcome, RtpDecoderRefreshScope, RtpForwardDestinationKind,
        RtpRelayDropKind, SourceSelectionKind, TransportCleanupFailureKind, TransportHealthState,
        TransportIceState, WsSessionLoopExitReason,
    },
    rtc::{RtcMetricsRecorder, RtcRouteControlMetrics},
    rtp::RtpMetricsRecorder,
    snapshot::{
        MetricFamilySnapshot, MetricHistogramBucketSnapshot, MetricHistogramSnapshot, MetricKind,
        MetricLabel, MetricSample, MetricValue, RuntimeMetricsSnapshot,
    },
};
