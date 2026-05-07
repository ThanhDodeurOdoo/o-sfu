//! Test backend selection for [`MediaTransport`](super::MediaTransport).
//!
//! This file is selected only for `cfg(test)` or the `testing-transport`
//! feature. It keeps deterministic fake transport behavior out of the
//! production boundary file while preserving the same concern-port contract that
//! production RTC implements. Tests may still choose real RTC by constructing
//! [`RtcTransport`], which is why this backend enum includes both variants.

use std::{collections::BTreeSet, sync::Arc, time::Instant};

use o_sfu_router::{MediaCapabilities, MediaKind, MediaStream as RouterRtpParameters};

use super::{runtime_adapter::RtcTransport, test_support::FakeMediaTransport};
use crate::{
    runtime::RoomInstanceId,
    transport::{
        ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, AppliedSessionAnswer, ConsumerActivity,
        ConsumerPacketGateUpdate, MediaPort, NegotiationPort, ObservabilityPort, ProducerActivity,
        ReceiverBandwidthSnapshot, SessionOffer, SessionPort, SourcePacketGate, SourcePolicyPort,
        SourcePolicyUpdateSubscription, TransportAdapterError, TransportBitrateSnapshot,
        TransportMediaId, TransportPlacementPressureSnapshot, TransportSessionHealth,
        TransportSessionKey,
    },
};

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
    Fake(Arc<FakeMediaTransport>),
}

macro_rules! delegate_backend {
    ($self:expr, $method:ident($($arg:expr),* $(,)?).await) => {
        match $self {
            Self::Rtc(transport) => transport.$method($($arg),*).await,
            Self::Fake(transport) => transport.$method($($arg),*).await,
        }
    };
    ($self:expr, $method:ident($($arg:expr),* $(,)?)) => {
        match $self {
            Self::Rtc(transport) => transport.$method($($arg),*),
            Self::Fake(transport) => transport.$method($($arg),*),
        }
    };
}

impl NegotiationPort for MediaTransportBackend {
    async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        delegate_backend!(self, create_initial_session_offer(session_key).await)
    }

    async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        delegate_backend!(self, create_session_renegotiation_offer(session_key).await)
    }

    async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, TransportAdapterError> {
        delegate_backend!(self, apply_session_answer(session_key, answer_sdp).await)
    }

    fn negotiated_client_rtp_capabilities(
        &self,
        answer_sdp: &str,
        offered_router_capabilities: &MediaCapabilities,
    ) -> Result<MediaCapabilities, TransportAdapterError> {
        delegate_backend!(
            self,
            negotiated_client_rtp_capabilities(answer_sdp, offered_router_capabilities)
        )
    }
}

impl SessionPort for MediaTransportBackend {
    async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        delegate_backend!(self, close_session(session_key).await)
    }
}

impl MediaPort for MediaTransportBackend {
    async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        delegate_backend!(self, remove_media(session_key, transport_media_id).await)
    }

    async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        delegate_backend!(
            self,
            publish_media(session_key, media_kind, rtp_parameters).await
        )
    }

    async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_media_id: TransportMediaId,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        delegate_backend!(
            self,
            consume_media(
                consumer_session_key,
                media_kind,
                source_session_key,
                source_media_id,
                consumer_rtp_parameters,
            )
            .await
        )
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
            Self::Fake(transport) => {
                transport
                    .set_producer_active(session_key, transport_media_id, activity.is_active())
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
            Self::Fake(transport) => {
                transport
                    .set_consumer_active(
                        consumer_session_key,
                        consumer_transport_media_id,
                        source_session_key,
                        source_transport_media_id,
                        activity.is_active(),
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
        delegate_backend!(
            self,
            set_consumer_packet_gate(
                consumer_session_key,
                consumer_transport_media_id,
                source_session_key,
                source_transport_media_id,
                packet_gate,
            )
            .await
        )
    }

    async fn set_consumer_packet_gates(
        &self,
        updates: &[ConsumerPacketGateUpdate],
    ) -> Vec<Result<(), TransportAdapterError>> {
        delegate_backend!(self, set_consumer_packet_gates(updates).await)
    }

    async fn request_consumer_keyframe(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        delegate_backend!(
            self,
            request_consumer_keyframe(
                consumer_session_key,
                consumer_transport_media_id,
                source_session_key,
                source_transport_media_id,
            )
            .await
        )
    }

    async fn transport_media_mid(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Option<String> {
        delegate_backend!(
            self,
            transport_media_mid(session_key, transport_media_id).await
        )
    }
}

impl ObservabilityPort for MediaTransportBackend {
    fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        delegate_backend!(self, transport_bitrate_snapshot(session_keys))
    }

    fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        delegate_backend!(self, receiver_bandwidth_snapshot(session_keys))
    }

    fn placement_pressure_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportPlacementPressureSnapshot {
        delegate_backend!(self, placement_pressure_snapshot(session_keys))
    }

    async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        delegate_backend!(self, active_speaker_source_snapshot().await)
    }

    async fn active_speaker_diagnostic_snapshot(&self) -> Vec<ActiveSpeakerSourceDiagnostic> {
        delegate_backend!(self, active_speaker_diagnostic_snapshot().await)
    }

    async fn next_active_speaker_deadline(&self) -> Option<Instant> {
        delegate_backend!(self, next_active_speaker_deadline().await)
    }

    async fn expired_active_speaker_room_instance_ids(
        &self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId> {
        delegate_backend!(self, expired_active_speaker_room_instance_ids(now).await)
    }

    fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        delegate_backend!(self, session_transport_health(session_key))
    }
}

impl SourcePolicyPort for MediaTransportBackend {
    fn source_policy_subscription(&self) -> SourcePolicyUpdateSubscription {
        delegate_backend!(self, source_policy_subscription())
    }
}
