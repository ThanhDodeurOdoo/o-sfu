//! In-repository server integration paths.
//!
//! Use this namespace for concrete room, diagnostics, metrics, packet sink,
//! recording and transport types that do not belong in [`crate::prelude`].

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

    #[cfg(any(test, feature = "testing-transport"))]
    pub mod test_support {
        //! non-production room harness types for deterministic integration tests

        pub use crate::runtime::{
            room::{
                NegotiatedPublish, RoomManagerTestApi, RoomTestApi, RoomTestInspect,
                RoomTestLifecycle, RoomTestMedia,
            },
            source_model::test_support::TestSourceKind,
        };
    }

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

    pub use crate::runtime::source_model::{
        PublishedSourceDescriptor, PublishedSourceDescriptorParts, PublishedSourceId,
        PublishedSourceOwner, SourceEncodingDescriptor, SourceEncodingDescriptorParts,
        SourceEncodingId, SourceModelError, SourceTemporalLayerId,
    };
}

pub mod transport {
    //! Curated media transport construction and extension boundary.
    //!
    //! Production server code gets the opaque media transport handle, named RTC
    //! construction inputs and transport DTOs from here. RTC
    //! worker internals stay below the media transport boundary.

    #[cfg(test)]
    pub mod test_support {
        //! non-production media transport route inspectors
        //!
        //! this module exists only for deterministic tests. production code
        //! must use the opaque `MediaTransport` facade

        pub use crate::runtime::rtc_engine::{ForwardedPacket, test_support::*};
    }

    #[cfg(feature = "internal-benchmarks")]
    pub mod benchmark_support {
        //! feature-gated packet-loop benchmark fixtures
        //!
        //! this module exists only for deterministic benchmark targets
        //! the
        //! fixtures prepare fixed transport scenarios while the measured calls
        //! still execute production RTC-engine helpers

        pub use crate::runtime::rtc_engine::benchmark_support::*;
    }

    #[cfg(any(test, feature = "fuzzing"))]
    pub mod fuzz_support {
        //! Fuzz-only RTC answer projection seam.

        pub use crate::runtime::rtc_engine::client_rtp_capabilities_from_answer;
    }

    pub use crate::{
        prelude::SessionBitrateLimits,
        runtime::media_transport::{
            ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSource,
            ActiveSpeakerSourceDiagnostic, AppliedProducer, AppliedSessionAnswer, ConsumerActivity,
            ConsumerPacketGateUpdate, MediaTransport, MediaTransportBuildError,
            MediaTransportBuilder, MediaTransportConfig, MediaTransportDeps, ProducerActivity,
            ReceiverBandwidthSnapshot, SessionOffer, SessionUploadEncoding, SessionUploadSlot,
            SourcePacketGate, SourcePacketOperatingPoint, SourcePolicyDirtyState,
            SourcePolicySignal, SourcePolicyUpdateSubscription, TransportAdapterError,
            TransportBitrateSnapshot, TransportConsumerRoute, TransportMediaId,
            TransportPlacementPressureSnapshot, TransportRelayRouteAction,
            TransportRelayRouteEffect, TransportResult, TransportSessionHealth,
            TransportSessionKey, TransportWorkerPressureSnapshot,
        },
    };
}
