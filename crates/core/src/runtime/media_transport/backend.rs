#[cfg(any(test, feature = "testing-transport"))]
use std::sync::Arc;
use std::{collections::BTreeSet, time::Instant};

use o_sfu_router::{MediaCapabilities, MediaKind, MediaStream as RouterRtpParameters};

#[cfg(any(test, feature = "testing-transport"))]
use super::test_support::FakeMediaTransport;
use super::{
    ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, AppliedSessionAnswer,
    ConsumerPacketGateUpdate, ReceiverBandwidthSnapshot, RtcTransport, SessionOffer,
    SourcePacketGate, SourcePolicyUpdateSubscription, TransportBitrateSnapshot, TransportMediaId,
    TransportPlacementPressureSnapshot, TransportRelayRouteEffect, TransportResult,
    TransportSessionHealth, TransportSessionKey, TransportWorkerPressureSnapshot,
    worker_manager::RtcWorkerManager,
};
use crate::runtime::RoomInstanceId;

macro_rules! worker_or_fake_async {
    ($backend:expr, $method:ident($($arg:expr),* $(,)?)) => {
        match $backend {
            Self::Rtc(transport) => transport.worker_manager.$method($($arg),*).await,
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.$method($($arg),*).await,
        }
    };
}

macro_rules! worker_or_fake {
    ($backend:expr, $method:ident($($arg:expr),* $(,)?)) => {
        match $backend {
            Self::Rtc(transport) => transport.worker_manager.$method($($arg),*),
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.$method($($arg),*),
        }
    };
}

macro_rules! rtc_or_fake_async {
    ($backend:expr, $method:ident($($arg:expr),* $(,)?), $fake:expr) => {
        match $backend {
            Self::Rtc(transport) => transport.worker_manager.$method($($arg),*).await,
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(_) => $fake,
        }
    };
}

macro_rules! rtc_or_fake {
    ($backend:expr, $method:ident($($arg:expr),* $(,)?), $fake:expr) => {
        match $backend {
            Self::Rtc(transport) => transport.worker_manager.$method($($arg),*),
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(_) => $fake,
        }
    };
}

macro_rules! backend_async_method {
    (fn $method:ident($($arg:ident: $ty:ty),* $(,)?) -> $result:ty) => {
        pub(super) async fn $method(&self, $($arg: $ty),*) -> $result {
            worker_or_fake_async!(self, $method($($arg),*))
        }
    };
}

macro_rules! backend_method {
    (fn $method:ident($($arg:ident: $ty:ty),* $(,)?) -> $result:ty) => {
        pub(super) fn $method(&self, $($arg: $ty),*) -> $result {
            worker_or_fake!(self, $method($($arg),*))
        }
    };
}

#[derive(Debug, Clone)]
pub(super) enum Backend {
    Rtc(RtcTransport),
    #[cfg(any(test, feature = "testing-transport"))]
    Fake(Arc<FakeMediaTransport>),
}

impl Backend {
    backend_async_method!(
        fn create_initial_session_offer(
            session_key: &TransportSessionKey,
        ) -> TransportResult<SessionOffer>
    );
    backend_async_method!(
        fn create_session_renegotiation_offer(
            session_key: &TransportSessionKey,
        ) -> TransportResult<SessionOffer>
    );
    backend_async_method!(
        fn apply_session_answer(
            session_key: &TransportSessionKey,
            answer_sdp: &str,
        ) -> TransportResult<AppliedSessionAnswer>
    );

    pub(super) fn negotiated_client_rtp_capabilities(
        &self,
        answer_sdp: &str,
        offered_router_capabilities: &MediaCapabilities,
    ) -> TransportResult<MediaCapabilities> {
        match self {
            Self::Rtc(_) => RtcWorkerManager::negotiated_client_rtp_capabilities(
                answer_sdp,
                offered_router_capabilities,
            ),
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(_) => FakeMediaTransport::project_answered_client_rtp_capabilities(
                answer_sdp,
                offered_router_capabilities,
            ),
        }
    }

    backend_async_method!(fn close_session(session_key: &TransportSessionKey) -> TransportResult<()>);
    backend_async_method!(
        fn remove_media(
            session_key: &TransportSessionKey,
            transport_media_id: TransportMediaId,
        ) -> TransportResult<()>
    );
    backend_async_method!(
        fn publish_media(
            session_key: &TransportSessionKey,
            media_kind: MediaKind,
            rtp_parameters: &RouterRtpParameters,
        ) -> TransportResult<TransportMediaId>
    );
    backend_async_method!(
        fn consume_media(
            consumer_session_key: &TransportSessionKey,
            media_kind: MediaKind,
            source_session_key: &TransportSessionKey,
            source_media_id: TransportMediaId,
            consumer_rtp_parameters: &RouterRtpParameters,
        ) -> TransportResult<TransportMediaId>
    );
    backend_async_method!(
        fn apply_relay_route_effect(
            effect: &TransportRelayRouteEffect,
        ) -> TransportResult<()>
    );
    backend_async_method!(
        fn set_producer_active(
            session_key: &TransportSessionKey,
            transport_media_id: TransportMediaId,
            active: bool,
        ) -> TransportResult<()>
    );
    backend_async_method!(
        fn set_consumer_active(
            consumer_session_key: &TransportSessionKey,
            consumer_transport_media_id: TransportMediaId,
            source_session_key: &TransportSessionKey,
            source_transport_media_id: TransportMediaId,
            active: bool,
        ) -> TransportResult<()>
    );
    backend_async_method!(
        fn set_consumer_packet_gate(
            consumer_session_key: &TransportSessionKey,
            consumer_transport_media_id: TransportMediaId,
            source_session_key: &TransportSessionKey,
            source_transport_media_id: TransportMediaId,
            packet_gate: SourcePacketGate,
        ) -> TransportResult<()>
    );
    backend_async_method!(
        fn set_consumer_packet_gates(
            updates: &[ConsumerPacketGateUpdate],
        ) -> Vec<TransportResult<()>>
    );
    backend_async_method!(
        fn request_consumer_keyframe(
            consumer_session_key: &TransportSessionKey,
            consumer_transport_media_id: TransportMediaId,
            source_session_key: &TransportSessionKey,
            source_transport_media_id: TransportMediaId,
        ) -> TransportResult<()>
    );

    pub(super) async fn transport_media_mid(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Option<String> {
        rtc_or_fake_async!(
            self,
            transport_media_mid(session_key, transport_media_id),
            None
        )
    }

    pub(super) fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        rtc_or_fake!(
            self,
            transport_bitrate_snapshot(session_keys),
            TransportBitrateSnapshot::default()
        )
    }

    backend_method!(
        fn receiver_bandwidth_snapshot(
            session_keys: &[TransportSessionKey],
        ) -> ReceiverBandwidthSnapshot
    );
    backend_method!(
        fn placement_pressure_snapshot(
            session_keys: &[TransportSessionKey],
        ) -> TransportPlacementPressureSnapshot
    );
    backend_method!(
        fn worker_pressure_snapshots() -> Vec<TransportWorkerPressureSnapshot>
    );
    backend_async_method!(
        fn active_speaker_source_snapshot() -> Vec<ActiveSpeakerSource>
    );
    backend_async_method!(
        fn active_speaker_diagnostic_snapshot() -> Vec<ActiveSpeakerSourceDiagnostic>
    );

    pub(super) async fn next_active_speaker_deadline(&self) -> Option<Instant> {
        rtc_or_fake_async!(self, next_active_speaker_deadline(), None)
    }

    pub(super) async fn expired_active_speaker_room_instance_ids(
        &self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId> {
        rtc_or_fake_async!(
            self,
            expired_active_speaker_room_instance_ids(now),
            BTreeSet::new()
        )
    }

    pub(super) fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        rtc_or_fake!(self, session_transport_health(session_key), None)
    }

    pub(super) fn source_policy_subscription(&self) -> SourcePolicyUpdateSubscription {
        match self {
            Self::Rtc(transport) => transport.worker_manager.source_policy_subscription(),
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.source_policy_signal().subscribe(),
        }
    }
}
