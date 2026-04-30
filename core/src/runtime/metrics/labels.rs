use std::time::Duration;

use super::counter::{HistogramBucketLabel, MetricLabel};
use crate::runtime::{
    WebSocketCloseCode,
    source_model::{SourceRoomPolicySelector, SourceSelector},
};

macro_rules! impl_metric_label {
    ($label:ty { $($variant:ident => $index:expr),+ $(,)? }) => {
        impl MetricLabel for $label {
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
    InterNodeRelay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RtpRelayDropKind {
    IntraNodeRelay,
    InterNodeRelay,
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

impl_metric_label!(HttpRoute {
    Noop => 0,
    Stats => 1,
    Room => 2,
    Disconnect => 3,
    Metrics => 4,
});

impl_metric_label!(HttpRoomResponseStatus {
    Success => 0,
    Unauthorized => 1,
    Forbidden => 2,
    BadRequest => 3,
});

impl_metric_label!(HttpDisconnectResponseStatus {
    Success => 0,
    BadRequest => 1,
    UnprocessableEntity => 2,
});

impl MetricLabel for ControlPlaneDurationBucket {
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

impl_metric_label!(WsConnectionStage {
    Accepted => 0,
    CredentialsReceived => 1,
    Joined => 2,
});

impl_metric_label!(WebSocketCloseCode {
    AuthTimeout => 0,
    AuthFailed => 1,
    ProtocolError => 2,
    RoomFull => 3,
    Error => 4,
    Clean => 5,
    Leaving => 6,
    Kicked => 7,
});

impl_metric_label!(WsStartupFailureKind {
    StartupSend => 0,
    SessionInitialize => 1,
});

impl_metric_label!(WsSessionLoopExitReason {
    UserClosed => 0,
    ReaderError => 1,
    BusBreak => 2,
    PingTimeout => 3,
    TransportDisconnected => 4,
    OutboundChannelClosed => 5,
    OutboundCloseSignal => 6,
    OutboundMessageSendFailure => 7,
});

impl_metric_label!(WsBusDirection {
    Received => 0,
    Sent => 1,
});

impl_metric_label!(WsBusFailureKind {
    InvalidInput => 0,
    UnsupportedFeature => 1,
    Send => 2,
});

impl_metric_label!(WsBusClientFrameKind {
    Request => 0,
    Message => 1,
});

impl_metric_label!(RtpFlowDirection {
    Ingress => 0,
    Egress => 1,
});

impl_metric_label!(RtpForwardDestinationKind {
    LocalRtc => 0,
    Recording => 1,
    IntraNodeRelay => 2,
    InterNodeRelay => 3,
});

impl_metric_label!(RtpRelayDropKind {
    IntraNodeRelay => 0,
    InterNodeRelay => 1,
});

impl_metric_label!(RtcDatagramRoutePath {
    Indexed => 0,
    Scan => 1,
});

impl_metric_label!(RtcDatagramDropReason {
    RecentMissCache => 0,
    SourceRateLimited => 1,
    NoUser => 2,
    Malformed => 3,
});

impl_metric_label!(RtcRouteControlOutcome {
    Absorbed => 0,
    Forwarded => 1,
    RouteGatedRelayDrop => 2,
    LayerAllowed => 3,
    LayerDropped => 4,
});

impl_metric_label!(SourceSelectionKind {
    Open => 0,
    Encoding => 1,
    OperatingPoint => 2,
    RoomPolicyFeatured => 3,
    RoomPolicyThumbnail => 4,
});

impl_metric_label!(BudgetSolverOutcome {
    Degraded => 0,
    Paused => 1,
    Resumed => 2,
    ProtectedOverBudget => 3,
});

impl_metric_label!(TransportIceState {
    New => 0,
    Checking => 1,
    Connected => 2,
    Completed => 3,
    Disconnected => 4,
});

impl_metric_label!(TransportHealthTransition {
    UnsetToConnected => 0,
    UnsetToDisconnected => 1,
    ConnectedToDisconnected => 2,
    DisconnectedToConnected => 3,
    ConnectedToUnset => 4,
    DisconnectedToUnset => 5,
});

impl_metric_label!(TransportUserLifetimeBucket {
    Le1Second => 0,
    Le10Seconds => 1,
    Le60Seconds => 2,
    Le300Seconds => 3,
});

impl_metric_label!(TransportCleanupFailureKind {
    Terminal => 0,
    RetryExhausted => 1,
    QueueFull => 2,
    Shutdown => 3,
});

impl_metric_label!(RecordingActionOutcome {
    StartAccepted => 0,
    StartRejected => 1,
    StopAccepted => 2,
    StopRejected => 3,
});

impl From<SourceSelector> for SourceSelectionKind {
    fn from(value: SourceSelector) -> Self {
        match value {
            SourceSelector::Open => Self::Open,
            SourceSelector::Encoding(_) => Self::Encoding,
            SourceSelector::OperatingPoint(_) => Self::OperatingPoint,
            SourceSelector::RoomPolicy(
                SourceRoomPolicySelector::Pinned
                | SourceRoomPolicySelector::Featured
                | SourceRoomPolicySelector::ScreenShare
                | SourceRoomPolicySelector::ActiveSpeaker,
            ) => Self::RoomPolicyFeatured,
            SourceSelector::RoomPolicy(
                SourceRoomPolicySelector::VisibleThumbnail
                | SourceRoomPolicySelector::Hidden
                | SourceRoomPolicySelector::Overflow,
            ) => Self::RoomPolicyThumbnail,
        }
    }
}
