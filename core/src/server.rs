//! Supported in-repository server integration paths.
//!
//! The crate root remains the stable media-core front door. This namespace is
//! narrower than the transitional `runtime` tree and exposes the concrete
//! pieces that the `o-sfu` server crate currently needs while the broad runtime
//! modules are being retired from production imports.

pub mod diagnostics {
    //! Operator diagnostics storage and DTOs consumed by server HTTP routes.

    pub use crate::runtime::diagnostics::{DiagnosticsEventData, DiagnosticsStore, types::*};
}

pub mod metrics {
    //! Runtime metrics catalog and snapshots consumed by `/metrics` and stats routes.

    pub use crate::runtime::metrics::*;
}

pub mod recording {
    //! Recording packet-sink boundary shared by the room engine and server runtime.

    pub use crate::runtime::{
        packet_sink_registry::ActiveRoomRegistry,
        recording::{MediaPacketSink, MediaTap, into_packet_sink},
    };
}

pub mod room {
    //! Room orchestration facade used by HTTP, websocket, and application code.

    pub use crate::runtime::room::{
        ConsumerRouteState, JoinUserRequest, RemoteTrackBootstrap, Room, RoomAdmissionPolicy,
        RoomConfig, RoomEventMessage, RoomEventRequest, RoomJoinError, RoomManager,
        RoomManagerConfig, RoomManagerDeps, RoomManagerJoinError, RoomMediaCounts,
        RoomRuntimeContext, RoomRuntimePolicy, RoomUserPermissions, RoomUserStatsSnapshot,
        RuntimeRoomDirectorySnapshot, RuntimeRoomStatsSnapshot, TrackBindingUpdate,
        UserCloseReason, UserOutbound, rtp_capabilities,
    };
}

pub mod session {
    //! Shared runtime session vocabulary used at the server edge.

    pub use crate::runtime::{
        AvailableFeatures, DownloadStates, JsonPayload, PeerSnapshot, RecordingOptions,
        RecordingState, RecordingStateUpdate, StopCode, StreamType, UserId, UserInfo,
        UserPermissions, VideoLayoutIntent, WebSocketCloseCode,
    };
}

pub mod source_model {
    //! Room-domain source descriptors shared with server-side protocol projection.

    pub use crate::runtime::source_model::*;
}

pub mod transport {
    //! Current media transport construction and extension boundary.
    //!
    //! The construction types remain server-integration API until the typed
    //! media transport boundary replaces them. The concern traits and transport
    //! DTOs are the lasting extension surface.

    #[cfg(any(test, feature = "testing-transport"))]
    pub mod test_support {
        pub use crate::runtime::{
            rtc_adapter::test_support::*,
            transport_adapter::test_support::{FakeWebRtcAdapter, FakeWebRtcEvent},
        };
    }
    #[cfg(any(test, feature = "testing-transport"))]
    pub use crate::runtime::transport_adapter::TestTransport;
    pub use crate::{
        SessionBitrateLimits,
        runtime::{
            rtc_adapter::{
                RelayTargetRegistry, RemoteAddrDemux, WorkerHandleSlot,
                client_rtp_capabilities_from_answer,
            },
            transport_adapter::{
                MediaTransport, MediaTransportDeps, RtcTransport, RtcTransportBuildError,
                RtcTransportBuilder, RtcTransportConfig, RtcTransportShardSetConfig,
                RuntimeTransportAdapter,
            },
        },
        transport::*,
    };
}
