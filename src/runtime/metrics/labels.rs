use crate::signaling::protocol::WebSocketCloseCode;

use super::counter::MetricLabel;

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
pub(super) enum HttpRoute {
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

impl MetricLabel for HttpRoute {
    const COUNT: usize = 5;

    fn as_index(self) -> usize {
        match self {
            Self::Noop => 0,
            Self::Stats => 1,
            Self::Channel => 2,
            Self::Disconnect => 3,
            Self::Metrics => 4,
        }
    }
}

impl MetricLabel for HttpChannelResponseStatus {
    const COUNT: usize = 4;

    fn as_index(self) -> usize {
        match self {
            Self::Success => 0,
            Self::Unauthorized => 1,
            Self::Forbidden => 2,
            Self::BadRequest => 3,
        }
    }
}

impl MetricLabel for HttpDisconnectResponseStatus {
    const COUNT: usize = 3;

    fn as_index(self) -> usize {
        match self {
            Self::Success => 0,
            Self::BadRequest => 1,
            Self::UnprocessableEntity => 2,
        }
    }
}

impl MetricLabel for WsConnectionStage {
    const COUNT: usize = 3;

    fn as_index(self) -> usize {
        match self {
            Self::Accepted => 0,
            Self::CredentialsReceived => 1,
            Self::Joined => 2,
        }
    }
}

impl MetricLabel for WebSocketCloseCode {
    const COUNT: usize = 8;

    fn as_index(self) -> usize {
        match self {
            Self::AuthTimeout => 0,
            Self::AuthFailed => 1,
            Self::ProtocolError => 2,
            Self::ChannelFull => 3,
            Self::Error => 4,
            Self::Clean => 5,
            Self::Leaving => 6,
            Self::Kicked => 7,
        }
    }
}

impl MetricLabel for WsStartupFailureKind {
    const COUNT: usize = 2;

    fn as_index(self) -> usize {
        match self {
            Self::StartupSend => 0,
            Self::SessionInitialize => 1,
        }
    }
}

impl MetricLabel for WsSessionLoopExitReason {
    const COUNT: usize = 8;

    fn as_index(self) -> usize {
        match self {
            Self::PeerClosed => 0,
            Self::ReaderError => 1,
            Self::BusBreak => 2,
            Self::PingTimeout => 3,
            Self::TransportDisconnected => 4,
            Self::OutboundChannelClosed => 5,
            Self::OutboundCloseSignal => 6,
            Self::OutboundMessageSendFailure => 7,
        }
    }
}

impl MetricLabel for WsBusDirection {
    const COUNT: usize = 2;

    fn as_index(self) -> usize {
        match self {
            Self::Received => 0,
            Self::Sent => 1,
        }
    }
}

impl MetricLabel for WsBusFailureKind {
    const COUNT: usize = 3;

    fn as_index(self) -> usize {
        match self {
            Self::InvalidInput => 0,
            Self::UnsupportedFeature => 1,
            Self::Send => 2,
        }
    }
}

impl MetricLabel for WsBusClientFrameKind {
    const COUNT: usize = 2;

    fn as_index(self) -> usize {
        match self {
            Self::Request => 0,
            Self::Message => 1,
        }
    }
}

impl MetricLabel for RtpFlowDirection {
    const COUNT: usize = 2;

    fn as_index(self) -> usize {
        match self {
            Self::Ingress => 0,
            Self::Egress => 1,
        }
    }
}

impl MetricLabel for RtpForwardDestinationKind {
    const COUNT: usize = 4;

    fn as_index(self) -> usize {
        match self {
            Self::LocalRtc => 0,
            Self::Recording => 1,
            Self::IntraNodeRelay => 2,
            Self::InterNodeRelay => 3,
        }
    }
}

impl MetricLabel for RtcDatagramRoutePath {
    const COUNT: usize = 2;

    fn as_index(self) -> usize {
        match self {
            Self::Indexed => 0,
            Self::Scan => 1,
        }
    }
}

impl MetricLabel for RtcDatagramDropReason {
    const COUNT: usize = 4;

    fn as_index(self) -> usize {
        match self {
            Self::RecentMissCache => 0,
            Self::SourceRateLimited => 1,
            Self::NoSession => 2,
            Self::Malformed => 3,
        }
    }
}

impl MetricLabel for RtcRouteControlOutcome {
    const COUNT: usize = 5;

    fn as_index(self) -> usize {
        match self {
            Self::Absorbed => 0,
            Self::Forwarded => 1,
            Self::RouteGatedRelayDrop => 2,
            Self::LayerAllowed => 3,
            Self::LayerDropped => 4,
        }
    }
}

impl MetricLabel for TransportIceState {
    const COUNT: usize = 5;

    fn as_index(self) -> usize {
        match self {
            Self::New => 0,
            Self::Checking => 1,
            Self::Connected => 2,
            Self::Completed => 3,
            Self::Disconnected => 4,
        }
    }
}

impl MetricLabel for TransportHealthTransition {
    const COUNT: usize = 6;

    fn as_index(self) -> usize {
        match self {
            Self::UnsetToConnected => 0,
            Self::UnsetToDisconnected => 1,
            Self::ConnectedToDisconnected => 2,
            Self::DisconnectedToConnected => 3,
            Self::ConnectedToUnset => 4,
            Self::DisconnectedToUnset => 5,
        }
    }
}

impl MetricLabel for TransportSessionLifetimeBucket {
    const COUNT: usize = 4;

    fn as_index(self) -> usize {
        match self {
            Self::Le1Second => 0,
            Self::Le10Seconds => 1,
            Self::Le60Seconds => 2,
            Self::Le300Seconds => 3,
        }
    }
}

impl MetricLabel for RecordingActionOutcome {
    const COUNT: usize = 4;

    fn as_index(self) -> usize {
        match self {
            Self::StartAccepted => 0,
            Self::StartRejected => 1,
            Self::StopAccepted => 2,
            Self::StopRejected => 3,
        }
    }
}
