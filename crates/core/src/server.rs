//! server-runtime integration surface for `o-sfu-core`
//!
//! this module xposes the pieces the top-level runtime needs to assemble a
//! process: diagnostics stores, metric catalogs, room managers, packet sink
//! registries and the media transport

/// diagnostics store and event payloads used by runtime inspection endpoints
///
/// room and transport code record typed diagnostics here
/// HTTP diagnostics routes read the same store to render operator-facing
/// summaries and room detail views
pub mod diagnostics {
    pub use crate::engine::diagnostics::{DiagnosticsEventData, DiagnosticsStore, types::*};
}

/// process-local metric catalog and typed recorders
///
/// runtime edges record through this facade instead of assembling metric names
/// or label sets manually
/// the Prometheus renderer consumes snapshots produced by these types
pub mod metrics {
    pub use crate::engine::metrics::*;
}

/// recording sink trait used by media routing code
///
/// recording integrations register packet sinks through the packet-sink
/// registry while transport code only depends on this narrow sink contract
pub mod recording {
    pub use crate::engine::recording::MediaPacketSink;
}

/// room packet-sink registry shared by transport workers
///
/// packet sinks are looked up by room when media has to fan out to recording or
/// other non-local destinations
/// the registry keeps that routing concern out of packet-loop callers
pub mod packet_sinks {
    pub use crate::engine::packet_sink_registry::{
        PacketSink, RegisteredPacketSink, RoomPacketSinkRegistry,
    };
}

/// room manager, room runtime and user-session integration types
///
/// this facade is the server crate's entry point for admitting users, sending
/// outbound messages and reading room statistics
/// pure routing and transport details remain behind the room API
pub mod room {
    #[cfg(any(test, feature = "testing-transport"))]
    pub mod test_support {
        pub use crate::engine::{
            room::{
                NegotiatedPublish, RoomManagerTestApi, RoomTestApi, RoomTestInspect,
                RoomTestLifecycle, RoomTestMedia,
            },
            source_model::test_support::{
                TestSourceKind, TestSubscriptionStates, source_kind_for_stream_id,
                source_publish_intent_for_source, stream_id_for_source,
                subscription_intents_from_test_states,
            },
        };
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub use crate::engine::room::ConsumerRouteState;
    pub use crate::{
        MediaWorkerId,
        engine::room::{
            BroadcastPayload, BroadcastPayloadError, DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY,
            DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY, IncomingBitrateSnapshot, JoinUserRequest,
            MAX_BROADCAST_PAYLOAD_BYTES, RemoteSourceProjection, RemoteSourceSnapshot, Room,
            RoomAdmissionPolicy, RoomConfig, RoomEventMessage, RoomJoinError, RoomManager,
            RoomManagerConfig, RoomManagerDeps, RoomManagerJoinError, RoomMediaCounts,
            RoomRuntimeContext, RoomRuntimePolicy, RoomUserAdmission, RoomUserPermissions,
            RoomUserStatsSnapshot, RouterPlacement, RouterPlacements, RouterPlacementsError,
            RuntimeRoomDirectorySnapshot, RuntimeRoomStatsSnapshot, UserCloseReason, UserOutbound,
            UserOutboundEvent, UserOutboundOverflow, UserOutboundOverflowKind,
            UserOutboundQueueLimits, UserOutboundReceiver, UserOutboundSendError,
            UserOutboundSender,
        },
    };
}

/// signaling-domain payloads shared by rooms and WebSocket sessions
///
/// these types describe users, features, permissionsm recording state and
/// host-visible close codes
/// they are re-exported here so the runtime does not import private room or
/// engine modules for protocol payload construction
pub mod session {
    pub use crate::engine::{
        AvailableFeatures, JsonPayload, PeerSnapshot, RecordingOptions, RecordingState,
        RecordingStateUpdate, StopCode, UserId, UserInfo, UserPermissions, VideoLayoutIntent,
        WebSocketCloseCode,
    };
}

/// source descriptor model accepted by publication and subscription flows
///
/// source-model types make published media identity explicit before it reaches
/// room routing or transport negotiation
pub mod source_model {
    pub use crate::engine::source_model::{
        PublishedSourceDescriptor, PublishedSourceDescriptorParts, PublishedSourceId,
        PublishedSourceOwner, SourceEncodingDescriptor, SourceEncodingDescriptorParts,
        SourceEncodingId, SourceModelError,
    };
}

/// media transport construction and extension boundary
///
/// the runtime builds one `MediaTransport` from owner configuration and process
/// services then room code uses the opaque handle for media operations
///
/// code above this module should not branch on RTC worker internals
pub mod transport {

    #[cfg(any(test, feature = "testing-transport"))]
    pub mod test_support {
        //! non-production media transport route inspectors

        pub use crate::engine::media_transport::test_support::*;
    }

    #[cfg(feature = "internal-benchmarks")]
    pub mod benchmark_support {
        //! feature-gated packet-loop benchmark fixtures

        pub use crate::engine::media_transport::benchmark_support::*;
    }

    #[cfg(any(test, feature = "fuzzing"))]
    pub mod fuzz_support {
        pub use crate::engine::media_transport::fuzz_support::{
            client_rtp_capabilities_from_answer, route_packet_loop_ingress_demux,
        };
    }

    pub use crate::{
        MediaWorkerId,
        engine::media_transport::{
            ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSource,
            ActiveSpeakerSourceDiagnostic, AppliedProducer, AppliedSessionAnswer, ConsumerActivity,
            MediaTransport, MediaTransportBuildError, MediaTransportConfig, MediaTransportDeps,
            ProducerActivity, ReceiverBandwidthSnapshot, RelayRouteActivity, SessionOffer,
            SessionUploadEncoding, SessionUploadSlot, SourcePacketGate, SourcePolicySignal,
            SourcePolicyUpdateSubscription, TransportAdapterError, TransportBitrateSnapshot,
            TransportConsumerRoute, TransportMediaId, TransportPlacementPressureSnapshot,
            TransportQualitySample, TransportQualitySnapshot, TransportRelayRouteAction,
            TransportRelayRouteEffect, TransportResult, TransportSessionHealth,
            TransportSessionKey, TransportSourceKey, TransportWorkerPressureSnapshot,
        },
        prelude::SessionBitrateLimits,
    };
}
