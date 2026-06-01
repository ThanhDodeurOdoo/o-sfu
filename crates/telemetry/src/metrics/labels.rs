use std::time::Duration;

use o_sfu_model::WebSocketCloseCode;

use super::counter::{ExportedMetricLabel, HistogramBucketLabel, MetricBucketLabel, MetricLabel};

macro_rules! impl_metric_label {
    ($label:ty { $($variant:ident => $index:expr),+ $(,)? }) => {
        impl MetricLabel for $label {
            const VARIANTS: &'static [Self] = &[$(Self::$variant),+];
            const COUNT: usize = <[()]>::len(&[$(impl_metric_label!(@unit $variant)),+]);

            fn as_index(self) -> usize {
                match self {
                    $(Self::$variant => $index),+
                }
            }
        }
    };
    (@unit $_variant:ident) => {
        ()
    };
}

macro_rules! impl_exported_metric_label {
    ($label:ty { $($variant:ident => ($index:expr, $label_value:literal)),+ $(,)? }) => {
        impl_metric_label!($label {
            $($variant => $index),+
        });

        impl ExportedMetricLabel for $label {
            fn label_value(self) -> &'static str {
                match self {
                    $(Self::$variant => $label_value),+
                }
            }
        }
    };
}

macro_rules! impl_exported_metric_label_pair {
    ($label:ty { $($variant:ident => ($index:expr, [($first_name:literal, $first_value:literal), ($second_name:literal, $second_value:literal)])),+ $(,)? }) => {
        impl_metric_label!($label {
            $($variant => $index),+
        });

        impl ExportedMetricLabelPair for $label {
            fn label_pair(self) -> [(&'static str, &'static str); 2] {
                match self {
                    $(Self::$variant => [($first_name, $first_value), ($second_name, $second_value)]),+
                }
            }
        }
    };
}

pub(super) trait ExportedMetricLabelPair: MetricLabel {
    fn label_pair(self) -> [(&'static str, &'static str); 2];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsSessionLoopExitReason {
    UserClosed,
    ReaderError,
    BusBreak,
    PingTimeout,
    TransportDisconnected,
    OutboundChannelClosed,
    OutboundCloseSignal,
    OutboundMessageSendFailure,
    OutboundQueueOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpRoute {
    Noop,
    Stats,
    Room,
    Disconnect,
    Metrics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HttpRoomResponseStatus {
    Success,
    Unauthorized,
    Forbidden,
    BadRequest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HttpDisconnectResponseStatus {
    Success,
    BadRequest,
    UnprocessableEntity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ControlPlaneDurationBucket {
    Le10Millis,
    Le50Millis,
    Le100Millis,
    Le250Millis,
    Le500Millis,
    Le1Second,
    Le5Seconds,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WsConnectionStage {
    Accepted,
    CredentialsReceived,
    Joined,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WsStartupFailureKind {
    StartupSend,
    SessionInitialize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WsBusDirection {
    Received,
    Sent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WsBusFailureKind {
    InvalidInput,
    UnsupportedFeature,
    Send,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WsBusClientFrameKind {
    Request,
    Message,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RtpFlowDirection {
    Ingress,
    Egress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RtpForwardDestinationKind {
    LocalRtc,
    Recording,
    IntraNodeRelay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RtpDecoderRefreshScope {
    Rid,
    Source,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RtpRelayDropKind {
    IntraNodeRelay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RtcDatagramRoutePath {
    Indexed,
    Scan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RtcDatagramDropReason {
    RecentMissCache,
    SourceRateLimited,
    NoUser,
    Malformed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RtcRouteControlOutcome {
    Absorbed,
    Forwarded,
    RouteGatedRelayDrop,
    LayerAllowed,
    LayerDropped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RtcKeyframeRequestOutcome {
    Forwarded,
    Absorbed,
    Retry,
    Cleared,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RtcRelayEnqueueResult {
    IntraNodeEnqueued,
    IntraNodeOverloaded,
    IntraNodeClosed,
}

impl RtcRelayEnqueueResult {
    #[must_use]
    pub const fn target_label(self) -> &'static str {
        match self {
            Self::IntraNodeEnqueued | Self::IntraNodeOverloaded | Self::IntraNodeClosed => {
                "intra_node_relay"
            }
        }
    }

    #[must_use]
    pub const fn outcome_label(self) -> &'static str {
        match self {
            Self::IntraNodeEnqueued => "enqueued",
            Self::IntraNodeOverloaded => "overloaded",
            Self::IntraNodeClosed => "closed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RtcRemoteControlDropKind {
    Keyframe,
    PacketGate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RtcRemotePacketGateConvergence {
    Retry,
    Flushed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceSelectionKind {
    Open,
    Encoding,
    OperatingPoint,
    RoomPolicyFeatured,
    RoomPolicyThumbnail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetSolverOutcome {
    Degraded,
    Paused,
    Resumed,
    ProtectedOverBudget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportIceState {
    New,
    Checking,
    Connected,
    Completed,
    Disconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportHealthState {
    Connected,
    Disconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TransportHealthTransition {
    UnsetToConnected,
    UnsetToDisconnected,
    ConnectedToDisconnected,
    DisconnectedToConnected,
    ConnectedToUnset,
    DisconnectedToUnset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TransportUserLifetimeBucket {
    Le1Second,
    Le10Seconds,
    Le60Seconds,
    Le300Seconds,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaQualitySample {
    Peer,
    MediaIngress,
    MediaEgress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaQualityLossDirection {
    Ingress,
    Egress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MediaQualityRttBucket {
    Le50Millis,
    Le100Millis,
    Le250Millis,
    Le500Millis,
    Le1Second,
    Le2Seconds,
    Le5Seconds,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportCleanupFailureKind {
    Terminal,
    RetryExhausted,
    QueueFull,
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RecordingActionOutcome {
    StartAccepted,
    StartRejected,
    StopAccepted,
    StopRejected,
}

impl_exported_metric_label!(HttpRoute {
    Noop => (0, "noop"),
    Stats => (1, "stats"),
    Room => (2, "room"),
    Disconnect => (3, "disconnect"),
    Metrics => (4, "metrics"),
});

impl_exported_metric_label!(HttpRoomResponseStatus {
    Success => (0, "success"),
    Unauthorized => (1, "unauthorized"),
    Forbidden => (2, "forbidden"),
    BadRequest => (3, "bad_request"),
});

impl_exported_metric_label!(HttpDisconnectResponseStatus {
    Success => (0, "success"),
    BadRequest => (1, "bad_request"),
    UnprocessableEntity => (2, "unprocessable_entity"),
});

impl MetricLabel for ControlPlaneDurationBucket {
    const VARIANTS: &'static [Self] = &[
        Self::Le10Millis,
        Self::Le50Millis,
        Self::Le100Millis,
        Self::Le250Millis,
        Self::Le500Millis,
        Self::Le1Second,
        Self::Le5Seconds,
    ];
    const COUNT: usize = 7;

    fn as_index(self) -> usize {
        match self {
            Self::Le10Millis => 0,
            Self::Le50Millis => 1,
            Self::Le100Millis => 2,
            Self::Le250Millis => 3,
            Self::Le500Millis => 4,
            Self::Le1Second => 5,
            Self::Le5Seconds => 6,
        }
    }
}

impl MetricBucketLabel for ControlPlaneDurationBucket {
    fn upper_bound(self) -> &'static str {
        match self {
            Self::Le10Millis => "0.01",
            Self::Le50Millis => "0.05",
            Self::Le100Millis => "0.1",
            Self::Le250Millis => "0.25",
            Self::Le500Millis => "0.5",
            Self::Le1Second => "1",
            Self::Le5Seconds => "5",
        }
    }
}

impl HistogramBucketLabel for ControlPlaneDurationBucket {
    fn from_duration(duration: Duration) -> Self {
        if duration <= Duration::from_millis(10) {
            return Self::Le10Millis;
        }
        if duration <= Duration::from_millis(50) {
            return Self::Le50Millis;
        }
        if duration <= Duration::from_millis(100) {
            return Self::Le100Millis;
        }
        if duration <= Duration::from_millis(250) {
            return Self::Le250Millis;
        }
        if duration <= Duration::from_millis(500) {
            return Self::Le500Millis;
        }
        if duration <= Duration::from_secs(1) {
            return Self::Le1Second;
        }
        Self::Le5Seconds
    }
}

impl_exported_metric_label!(WsConnectionStage {
    Accepted => (0, "accepted"),
    CredentialsReceived => (1, "credentials_received"),
    Joined => (2, "joined"),
});

impl_exported_metric_label!(WebSocketCloseCode {
    AuthTimeout => (0, "auth_timeout"),
    AuthFailed => (1, "auth_failed"),
    ProtocolError => (2, "protocol_error"),
    RoomFull => (3, "room_full"),
    Error => (4, "error"),
    Clean => (5, "clean"),
    Leaving => (6, "leaving"),
    Kicked => (7, "kicked"),
});

impl_exported_metric_label!(WsStartupFailureKind {
    StartupSend => (0, "startup_send"),
    SessionInitialize => (1, "user_initialize"),
});

impl_exported_metric_label!(WsSessionLoopExitReason {
    UserClosed => (0, "user_closed"),
    ReaderError => (1, "reader_error"),
    BusBreak => (2, "bus_break"),
    PingTimeout => (3, "ping_timeout"),
    TransportDisconnected => (4, "transport_disconnected"),
    OutboundChannelClosed => (5, "outbound_room_closed"),
    OutboundCloseSignal => (6, "outbound_close_signal"),
    OutboundMessageSendFailure => (7, "outbound_message_send_failure"),
    OutboundQueueOverflow => (8, "outbound_queue_overflow"),
});

impl_exported_metric_label!(WsBusDirection {
    Received => (0, "received"),
    Sent => (1, "sent"),
});

impl_exported_metric_label!(WsBusFailureKind {
    InvalidInput => (0, "invalid_input"),
    UnsupportedFeature => (1, "unsupported_feature"),
    Send => (2, "send"),
});

impl_exported_metric_label!(WsBusClientFrameKind {
    Request => (0, "request"),
    Message => (1, "message"),
});

impl_exported_metric_label!(RtpFlowDirection {
    Ingress => (0, "ingress"),
    Egress => (1, "egress"),
});

impl_exported_metric_label!(RtpForwardDestinationKind {
    LocalRtc => (0, "local_rtc"),
    Recording => (1, "recording"),
    IntraNodeRelay => (2, "intra_node_relay"),
});

impl_exported_metric_label!(RtpDecoderRefreshScope {
    Rid => (0, "rid"),
    Source => (1, "source"),
});

impl_exported_metric_label!(RtpRelayDropKind {
    IntraNodeRelay => (0, "intra_node_relay"),
});

impl_exported_metric_label!(RtcDatagramRoutePath {
    Indexed => (0, "indexed"),
    Scan => (1, "scan"),
});

impl_exported_metric_label!(RtcDatagramDropReason {
    RecentMissCache => (0, "recent_miss_cache"),
    SourceRateLimited => (1, "source_rate_limited"),
    NoUser => (2, "no_user"),
    Malformed => (3, "malformed"),
});

impl_exported_metric_label!(RtcRouteControlOutcome {
    Absorbed => (0, "absorbed"),
    Forwarded => (1, "forwarded"),
    RouteGatedRelayDrop => (2, "route_gated_relay_drop"),
    LayerAllowed => (3, "layer_allowed"),
    LayerDropped => (4, "layer_dropped"),
});

impl_exported_metric_label!(RtcKeyframeRequestOutcome {
    Forwarded => (0, "forwarded"),
    Absorbed => (1, "absorbed"),
    Retry => (2, "retry"),
    Cleared => (3, "cleared"),
});

impl_metric_label!(RtcRelayEnqueueResult {
    IntraNodeEnqueued => 0,
    IntraNodeOverloaded => 1,
    IntraNodeClosed => 2,
});

impl ExportedMetricLabelPair for RtcRelayEnqueueResult {
    fn label_pair(self) -> [(&'static str, &'static str); 2] {
        [
            ("target", self.target_label()),
            ("outcome", self.outcome_label()),
        ]
    }
}

impl_exported_metric_label!(RtcRemoteControlDropKind {
    Keyframe => (0, "keyframe"),
    PacketGate => (1, "packet_gate"),
});

impl_exported_metric_label!(RtcRemotePacketGateConvergence {
    Retry => (0, "retry"),
    Flushed => (1, "flushed"),
});

impl_exported_metric_label!(SourceSelectionKind {
    Open => (0, "open"),
    Encoding => (1, "encoding"),
    OperatingPoint => (2, "operating_point"),
    RoomPolicyFeatured => (3, "room_policy_featured"),
    RoomPolicyThumbnail => (4, "room_policy_thumbnail"),
});

impl_exported_metric_label!(BudgetSolverOutcome {
    Degraded => (0, "degraded"),
    Paused => (1, "paused"),
    Resumed => (2, "resumed"),
    ProtectedOverBudget => (3, "protected_over_budget"),
});

impl_exported_metric_label!(TransportIceState {
    New => (0, "new"),
    Checking => (1, "checking"),
    Connected => (2, "connected"),
    Completed => (3, "completed"),
    Disconnected => (4, "disconnected"),
});

impl_exported_metric_label!(TransportHealthState {
    Connected => (0, "connected"),
    Disconnected => (1, "disconnected"),
});

impl_exported_metric_label_pair!(TransportHealthTransition {
    UnsetToConnected => (0, [("from", "unset"), ("to", "connected")]),
    UnsetToDisconnected => (1, [("from", "unset"), ("to", "disconnected")]),
    ConnectedToDisconnected => (2, [("from", "connected"), ("to", "disconnected")]),
    DisconnectedToConnected => (3, [("from", "disconnected"), ("to", "connected")]),
    ConnectedToUnset => (4, [("from", "connected"), ("to", "unset")]),
    DisconnectedToUnset => (5, [("from", "disconnected"), ("to", "unset")]),
});

impl_metric_label!(TransportUserLifetimeBucket {
    Le1Second => 0,
    Le10Seconds => 1,
    Le60Seconds => 2,
    Le300Seconds => 3,
});

impl MetricBucketLabel for TransportUserLifetimeBucket {
    fn upper_bound(self) -> &'static str {
        match self {
            Self::Le1Second => "1",
            Self::Le10Seconds => "10",
            Self::Le60Seconds => "60",
            Self::Le300Seconds => "300",
        }
    }
}

impl_exported_metric_label!(MediaQualitySample {
    Peer => (0, "peer"),
    MediaIngress => (1, "media_ingress"),
    MediaEgress => (2, "media_egress"),
});

impl_exported_metric_label!(MediaQualityLossDirection {
    Ingress => (0, "ingress"),
    Egress => (1, "egress"),
});

impl_metric_label!(MediaQualityRttBucket {
    Le50Millis => 0,
    Le100Millis => 1,
    Le250Millis => 2,
    Le500Millis => 3,
    Le1Second => 4,
    Le2Seconds => 5,
    Le5Seconds => 6,
});

impl MetricBucketLabel for MediaQualityRttBucket {
    fn upper_bound(self) -> &'static str {
        match self {
            Self::Le50Millis => "0.05",
            Self::Le100Millis => "0.1",
            Self::Le250Millis => "0.25",
            Self::Le500Millis => "0.5",
            Self::Le1Second => "1",
            Self::Le2Seconds => "2",
            Self::Le5Seconds => "5",
        }
    }
}

impl HistogramBucketLabel for MediaQualityRttBucket {
    fn from_duration(duration: Duration) -> Self {
        if duration <= Duration::from_millis(50) {
            return Self::Le50Millis;
        }
        if duration <= Duration::from_millis(100) {
            return Self::Le100Millis;
        }
        if duration <= Duration::from_millis(250) {
            return Self::Le250Millis;
        }
        if duration <= Duration::from_millis(500) {
            return Self::Le500Millis;
        }
        if duration <= Duration::from_secs(1) {
            return Self::Le1Second;
        }
        if duration <= Duration::from_secs(2) {
            return Self::Le2Seconds;
        }
        Self::Le5Seconds
    }
}

impl_exported_metric_label!(TransportCleanupFailureKind {
    Terminal => (0, "terminal"),
    RetryExhausted => (1, "retry_exhausted"),
    QueueFull => (2, "queue_full"),
    Shutdown => (3, "shutdown"),
});

impl_exported_metric_label_pair!(RecordingActionOutcome {
    StartAccepted => (0, [("action", "start"), ("outcome", "accepted")]),
    StartRejected => (1, [("action", "start"), ("outcome", "rejected")]),
    StopAccepted => (2, [("action", "stop"), ("outcome", "accepted")]),
    StopRejected => (3, [("action", "stop"), ("outcome", "rejected")]),
});
