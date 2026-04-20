use o_sfu_protocol::signaling::WebSocketCloseCode;

use std::time::Duration;

use super::counter::{HistogramBucketLabel, MetricLabel};

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
pub(crate) enum WsSessionLoopExitReason {
    PeerClosed,
    ReaderError,
    BusBreak,
    PingTimeout,
    TransportDisconnected,
    OutboundChannelClosed,
    OutboundCloseSignal,
    OutboundMessageSendFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HttpRoute {
    Noop,
    Stats,
    Channel,
    Disconnect,
    Metrics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HttpChannelResponseStatus {
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
pub(crate) enum RtpForwardDestinationKind {
    LocalRtc,
    Recording,
    IntraNodeRelay,
    InterNodeRelay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RtpRelayDropKind {
    IntraNodeRelay,
    InterNodeRelay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RtcDatagramRoutePath {
    Indexed,
    Scan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RtcDatagramDropReason {
    RecentMissCache,
    SourceRateLimited,
    NoSession,
    Malformed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RtcRouteControlOutcome {
    Absorbed,
    Forwarded,
    RouteGatedRelayDrop,
    LayerAllowed,
    LayerDropped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportIceState {
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
pub(super) enum TransportSessionLifetimeBucket {
    Le1Second,
    Le10Seconds,
    Le60Seconds,
    Le300Seconds,
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
    Channel => 2,
    Disconnect => 3,
    Metrics => 4,
});

impl_metric_label!(HttpChannelResponseStatus {
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
    ChannelFull => 3,
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
    PeerClosed => 0,
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
    NoSession => 2,
    Malformed => 3,
});

impl_metric_label!(RtcRouteControlOutcome {
    Absorbed => 0,
    Forwarded => 1,
    RouteGatedRelayDrop => 2,
    LayerAllowed => 3,
    LayerDropped => 4,
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

impl_metric_label!(TransportSessionLifetimeBucket {
    Le1Second => 0,
    Le10Seconds => 1,
    Le60Seconds => 2,
    Le300Seconds => 3,
});

impl_metric_label!(RecordingActionOutcome {
    StartAccepted => 0,
    StartRejected => 1,
    StopAccepted => 2,
    StopRejected => 3,
});
