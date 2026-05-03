//! Test backend selection for [`MediaTransport`](super::MediaTransport).
//!
//! This file is selected only for `cfg(test)` or the `testing-transport`
//! feature. It keeps deterministic fake transport behavior out of the
//! production boundary file while preserving the same concern-port contract that
//! production RTC implements. Tests may still choose real RTC by constructing
//! [`RtcTransport`], which is why this backend enum includes both variants.

use std::{collections::BTreeSet, sync::Arc, time::Instant};

use o_sfu_router::{MediaCapabilities, MediaKind, MediaStream as RouterRtpParameters};

use super::{fake::FakeWebRtcAdapter, runtime_adapter::RtcTransport};
use crate::{
    runtime::RoomInstanceId,
    transport::{
        ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, AppliedSessionAnswer, ConsumerActivity,
        ConsumerPacketGateUpdate, MediaPort, NegotiationPort, ObservabilityPort, ProducerActivity,
        ReceiverBandwidthSnapshot, SessionOffer, SessionPort, SourcePacketGate, SourcePolicyPort,
        SourcePolicyUpdateSubscription, TransportAdapterError, TransportBitrateSnapshot,
        TransportMediaId, TransportSessionHealth, TransportSessionKey,
    },
};

/// Deterministic media transport used by room, websocket and protocol tests.
///
/// `TestTransport` wraps [`FakeWebRtcAdapter`] behind the same port traits as
/// production RTC. Inspection helpers stay on the fake adapter or
/// `test_support`, not on the production trait surface. That keeps test
/// assertions explicit without teaching production code about fake-only state.
#[derive(Debug, Clone)]
pub struct TestTransport {
    pub(super) adapter: Arc<FakeWebRtcAdapter>,
}

impl TestTransport {
    /// Wraps a configured fake adapter.
    ///
    /// Tests use this when they need to pre-install delays, negotiated producer
    /// parameters or source-policy observations before handing the transport to
    /// room orchestration.
    #[must_use]
    pub fn new(adapter: Arc<FakeWebRtcAdapter>) -> Self {
        Self { adapter }
    }

    /// Creates a fake transport with default deterministic behavior.
    #[must_use]
    pub fn default_fake() -> Self {
        Self::new(Arc::new(FakeWebRtcAdapter::default()))
    }

    /// Returns the underlying fake adapter for test-only inspection.
    ///
    /// This method is deliberately on `TestTransport`, not on the shared port
    /// traits. Production callers should not learn about fake adapter events.
    #[must_use]
    pub fn adapter(&self) -> &Arc<FakeWebRtcAdapter> {
        &self.adapter
    }
}

impl NegotiationPort for TestTransport {
    async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.adapter.create_initial_session_offer(session_key).await
    }

    async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.adapter
            .create_session_renegotiation_offer(session_key)
            .await
    }

    async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, TransportAdapterError> {
        self.adapter
            .apply_session_answer(session_key, answer_sdp)
            .await
    }

    fn negotiated_client_rtp_capabilities(
        &self,
        answer_sdp: &str,
        offered_router_capabilities: &MediaCapabilities,
    ) -> Result<MediaCapabilities, TransportAdapterError> {
        self.adapter
            .negotiated_client_rtp_capabilities(answer_sdp, offered_router_capabilities)
    }
}

impl SessionPort for TestTransport {
    async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        self.adapter.close_session(session_key).await
    }
}

impl MediaPort for TestTransport {
    async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        self.adapter
            .remove_media(session_key, transport_media_id)
            .await
    }

    async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.adapter
            .publish_media(session_key, media_kind, rtp_parameters)
            .await
    }

    async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_media_id: TransportMediaId,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.adapter
            .consume_media(
                consumer_session_key,
                media_kind,
                source_session_key,
                source_media_id,
                consumer_rtp_parameters,
            )
            .await
    }

    async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        activity: ProducerActivity,
    ) -> Result<(), TransportAdapterError> {
        self.adapter
            .set_producer_active(session_key, transport_media_id, activity.is_active())
            .await
    }

    async fn set_consumer_active(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        activity: ConsumerActivity,
    ) -> Result<(), TransportAdapterError> {
        self.adapter
            .set_consumer_active(
                consumer_session_key,
                consumer_transport_media_id,
                source_session_key,
                source_transport_media_id,
                activity.is_active(),
            )
            .await
    }

    async fn set_consumer_packet_gate(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: SourcePacketGate,
    ) -> Result<(), TransportAdapterError> {
        self.adapter
            .set_consumer_packet_gate(
                consumer_session_key,
                consumer_transport_media_id,
                source_session_key,
                source_transport_media_id,
                packet_gate,
            )
            .await
    }

    async fn request_consumer_keyframe(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        self.adapter
            .request_consumer_keyframe(
                consumer_session_key,
                consumer_transport_media_id,
                source_session_key,
                source_transport_media_id,
            )
            .await
    }

    async fn transport_media_mid(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Option<String> {
        self.adapter
            .transport_media_mid(session_key, transport_media_id)
            .await
    }
}

impl ObservabilityPort for TestTransport {
    fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        self.adapter.transport_bitrate_snapshot(session_keys)
    }

    fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        self.adapter.receiver_bandwidth_snapshot(session_keys)
    }

    async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        self.adapter.active_speaker_source_snapshot().await
    }

    async fn active_speaker_diagnostic_snapshot(&self) -> Vec<ActiveSpeakerSourceDiagnostic> {
        self.adapter.active_speaker_diagnostic_snapshot().await
    }

    async fn next_active_speaker_deadline(&self) -> Option<Instant> {
        self.adapter.next_active_speaker_deadline().await
    }

    async fn expired_active_speaker_room_instance_ids(
        &self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId> {
        self.adapter
            .expired_active_speaker_room_instance_ids(now)
            .await
    }

    fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        self.adapter.session_transport_health(session_key)
    }
}

impl SourcePolicyPort for TestTransport {
    fn source_policy_subscription(&self) -> SourcePolicyUpdateSubscription {
        self.adapter.source_policy_subscription()
    }
}

/// Test-build backend set hidden behind the opaque media handle.
///
/// The all-features test matrix needs deterministic fake transport and real
/// RTC transport in the same compilation unit. This cfg-selected enum provides
/// that without exposing backend variants outside `media_transport` test
/// support.
#[derive(Debug, Clone)]
pub(super) enum MediaTransportBackend {
    /// Real RTC backend used by integration tests that verify str0m behavior.
    Rtc(RtcTransport),
    /// Deterministic fake backend used by tests that need direct event
    /// inspection or controlled async delays.
    Test(TestTransport),
}

impl NegotiationPort for MediaTransportBackend {
    async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        match self {
            Self::Rtc(transport) => transport.create_initial_session_offer(session_key).await,
            Self::Test(transport) => transport.create_initial_session_offer(session_key).await,
        }
    }

    async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .create_session_renegotiation_offer(session_key)
                    .await
            }
            Self::Test(transport) => {
                transport
                    .create_session_renegotiation_offer(session_key)
                    .await
            }
        }
    }

    async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, TransportAdapterError> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .apply_session_answer(session_key, answer_sdp)
                    .await
            }
            Self::Test(transport) => {
                transport
                    .apply_session_answer(session_key, answer_sdp)
                    .await
            }
        }
    }

    fn negotiated_client_rtp_capabilities(
        &self,
        answer_sdp: &str,
        offered_router_capabilities: &MediaCapabilities,
    ) -> Result<MediaCapabilities, TransportAdapterError> {
        match self {
            Self::Rtc(transport) => transport
                .negotiated_client_rtp_capabilities(answer_sdp, offered_router_capabilities),
            Self::Test(transport) => transport
                .negotiated_client_rtp_capabilities(answer_sdp, offered_router_capabilities),
        }
    }
}

impl SessionPort for MediaTransportBackend {
    async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Rtc(transport) => transport.close_session(session_key).await,
            Self::Test(transport) => transport.close_session(session_key).await,
        }
    }
}

impl MediaPort for MediaTransportBackend {
    async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .remove_media(session_key, transport_media_id)
                    .await
            }
            Self::Test(transport) => {
                transport
                    .remove_media(session_key, transport_media_id)
                    .await
            }
        }
    }

    async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .publish_media(session_key, media_kind, rtp_parameters)
                    .await
            }
            Self::Test(transport) => {
                transport
                    .publish_media(session_key, media_kind, rtp_parameters)
                    .await
            }
        }
    }

    async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_media_id: TransportMediaId,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .consume_media(
                        consumer_session_key,
                        media_kind,
                        source_session_key,
                        source_media_id,
                        consumer_rtp_parameters,
                    )
                    .await
            }
            Self::Test(transport) => {
                transport
                    .consume_media(
                        consumer_session_key,
                        media_kind,
                        source_session_key,
                        source_media_id,
                        consumer_rtp_parameters,
                    )
                    .await
            }
        }
    }

    async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        activity: ProducerActivity,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .set_producer_active(session_key, transport_media_id, activity)
                    .await
            }
            Self::Test(transport) => {
                transport
                    .set_producer_active(session_key, transport_media_id, activity)
                    .await
            }
        }
    }

    async fn set_consumer_active(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        activity: ConsumerActivity,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .set_consumer_active(
                        consumer_session_key,
                        consumer_transport_media_id,
                        source_session_key,
                        source_transport_media_id,
                        activity,
                    )
                    .await
            }
            Self::Test(transport) => {
                transport
                    .set_consumer_active(
                        consumer_session_key,
                        consumer_transport_media_id,
                        source_session_key,
                        source_transport_media_id,
                        activity,
                    )
                    .await
            }
        }
    }

    async fn set_consumer_packet_gate(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: SourcePacketGate,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .set_consumer_packet_gate(
                        consumer_session_key,
                        consumer_transport_media_id,
                        source_session_key,
                        source_transport_media_id,
                        packet_gate,
                    )
                    .await
            }
            Self::Test(transport) => {
                transport
                    .set_consumer_packet_gate(
                        consumer_session_key,
                        consumer_transport_media_id,
                        source_session_key,
                        source_transport_media_id,
                        packet_gate,
                    )
                    .await
            }
        }
    }

    async fn set_consumer_packet_gates(
        &self,
        updates: &[ConsumerPacketGateUpdate],
    ) -> Vec<Result<(), TransportAdapterError>> {
        match self {
            Self::Rtc(transport) => transport.set_consumer_packet_gates(updates).await,
            Self::Test(transport) => transport.set_consumer_packet_gates(updates).await,
        }
    }

    async fn request_consumer_keyframe(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .request_consumer_keyframe(
                        consumer_session_key,
                        consumer_transport_media_id,
                        source_session_key,
                        source_transport_media_id,
                    )
                    .await
            }
            Self::Test(transport) => {
                transport
                    .request_consumer_keyframe(
                        consumer_session_key,
                        consumer_transport_media_id,
                        source_session_key,
                        source_transport_media_id,
                    )
                    .await
            }
        }
    }

    async fn transport_media_mid(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Option<String> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .transport_media_mid(session_key, transport_media_id)
                    .await
            }
            Self::Test(transport) => {
                transport
                    .transport_media_mid(session_key, transport_media_id)
                    .await
            }
        }
    }
}

impl ObservabilityPort for MediaTransportBackend {
    fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        match self {
            Self::Rtc(transport) => transport.transport_bitrate_snapshot(session_keys),
            Self::Test(transport) => transport.transport_bitrate_snapshot(session_keys),
        }
    }

    fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        match self {
            Self::Rtc(transport) => transport.receiver_bandwidth_snapshot(session_keys),
            Self::Test(transport) => transport.receiver_bandwidth_snapshot(session_keys),
        }
    }

    async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        match self {
            Self::Rtc(transport) => transport.active_speaker_source_snapshot().await,
            Self::Test(transport) => transport.active_speaker_source_snapshot().await,
        }
    }

    async fn active_speaker_diagnostic_snapshot(&self) -> Vec<ActiveSpeakerSourceDiagnostic> {
        match self {
            Self::Rtc(transport) => transport.active_speaker_diagnostic_snapshot().await,
            Self::Test(transport) => transport.active_speaker_diagnostic_snapshot().await,
        }
    }

    async fn next_active_speaker_deadline(&self) -> Option<Instant> {
        match self {
            Self::Rtc(transport) => transport.next_active_speaker_deadline().await,
            Self::Test(transport) => transport.next_active_speaker_deadline().await,
        }
    }

    async fn expired_active_speaker_room_instance_ids(
        &self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .expired_active_speaker_room_instance_ids(now)
                    .await
            }
            Self::Test(transport) => {
                transport
                    .expired_active_speaker_room_instance_ids(now)
                    .await
            }
        }
    }

    fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        match self {
            Self::Rtc(transport) => transport.session_transport_health(session_key),
            Self::Test(transport) => transport.session_transport_health(session_key),
        }
    }
}

impl SourcePolicyPort for MediaTransportBackend {
    fn source_policy_subscription(&self) -> SourcePolicyUpdateSubscription {
        match self {
            Self::Rtc(transport) => transport.source_policy_subscription(),
            Self::Test(transport) => transport.source_policy_subscription(),
        }
    }
}
