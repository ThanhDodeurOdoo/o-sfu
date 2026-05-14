//! Supported in-repository server integration paths.
//!
//! The crate root remains the stable media-core front door. This namespace is
//! the curated bridge from the private core runtime tree to the `o-sfu` server
//! crate. It exposes the concrete pieces that the server, diagnostics routes,
//! metrics exporter and in-repository tests need without making RTC workers,
//! room-state internals or packet-loop modules part of the public front door.

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

    pub use crate::runtime::recording::MediaPacketSink;
}

pub mod packet_sinks {
    //! Generic room packet-sink registry consumed by transport and room services.

    pub use crate::runtime::packet_sink_registry::{
        PacketSink, RegisteredPacketSink, RoomPacketSinkRegistry, into_packet_sink,
    };
}

pub mod room {
    //! Room orchestration facade used by HTTP, websocket, and application code.

    pub use crate::runtime::room::{
        BroadcastPayload, BroadcastPayloadError, ConsumerRouteState,
        DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
        IncomingBitrateSnapshot, JoinUserRequest, LocalRoomRouterPlacements,
        LocalRoomRouterPlacementsError, LocalRouterRuntimeContext, MAX_BROADCAST_PAYLOAD_BYTES,
        RemoteTrackBootstrap, Room, RoomAdmissionPolicy, RoomConfig, RoomEventMessage,
        RoomEventRequest, RoomJoinError, RoomManager, RoomManagerConfig, RoomManagerDeps,
        RoomManagerJoinError, RoomMediaCounts, RoomRuntimeContext, RoomRuntimePolicy,
        RoomUserPermissions, RoomUserStatsSnapshot, RuntimeRoomDirectorySnapshot,
        RuntimeRoomStatsSnapshot, TrackBindingUpdate, UserCloseReason, UserOutbound,
        UserOutboundEvent, UserOutboundOverflow, UserOutboundOverflowKind, UserOutboundQueueLimits,
        UserOutboundReceiver, UserOutboundSendError, UserOutboundSender, rtp_capabilities,
    };
    #[cfg(any(test, feature = "testing-transport"))]
    pub use crate::runtime::room::{
        NegotiatedPublish, RoomManagerTestApi, RoomTestApi, RoomTestInspect, RoomTestLifecycle,
        RoomTestMedia,
    };
}

pub mod session {
    //! Shared runtime session vocabulary used at the server edge.

    pub use crate::runtime::{
        AvailableFeatures, JsonPayload, PeerSnapshot, RecordingOptions, RecordingState,
        RecordingStateUpdate, StopCode, UserId, UserInfo, UserPermissions, VideoLayoutIntent,
        WebSocketCloseCode,
    };
}

pub mod source_model {
    //! Room-domain source descriptors shared with server-side protocol projection.

    pub use crate::runtime::source_model::*;
}

pub mod transport {
    //! Curated media transport construction and extension boundary.
    //!
    //! Production server code gets the opaque media transport handle, named RTC
    //! construction inputs plus concern-oriented transport ports from here. RTC
    //! worker internals stay below the media transport boundary.

    #[cfg(any(test, feature = "testing-transport"))]
    pub mod test_support {
        pub use crate::runtime::{
            media_transport::test_support::{FakeMediaTransport, FakeMediaTransportEvent},
            rtc_engine::{ForwardedPacket, test_support::*},
        };
    }

    #[cfg(any(test, feature = "fuzzing"))]
    pub mod fuzz_support {
        //! Fuzz-only RTC answer projection seam.

        pub use crate::runtime::rtc_engine::client_rtp_capabilities_from_answer;
    }

    pub use crate::{
        SessionBitrateLimits,
        runtime::media_transport::{
            MediaTransport, MediaTransportDeps, RtcTransport, RtcTransportBuildError,
            RtcTransportBuilder, RtcTransportConfig,
        },
        transport::{
            ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSource,
            ActiveSpeakerSourceDiagnostic, AppliedProducer, AppliedSessionAnswer, ConsumerActivity,
            ConsumerPacketGateUpdate, MediaPort, NegotiationPort, ObservabilityPort,
            ProducerActivity, ReceiverBandwidthSnapshot, SessionOffer, SessionPort,
            SessionUploadEncoding, SessionUploadSlot, SourcePacketGate, SourcePacketOperatingPoint,
            SourcePolicyDirtyState, SourcePolicyPort, SourcePolicySignal,
            SourcePolicyUpdateSubscription, TransportAdapterError, TransportBitrateSnapshot,
            TransportMediaId, TransportPlacementPressureSnapshot, TransportResult,
            TransportSessionHealth, TransportSessionKey,
        },
    };
}
