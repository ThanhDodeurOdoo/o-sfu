//! Backend selection hidden behind [`MediaTransport`](super::MediaTransport).
//!
//! The opaque media transport handle is the only type above this module that
//! implements the transport concern traits. This backend enum keeps production
//! RTC shard ownership and deterministic fake transport selection below that
//! orchestration boundary.

#[cfg(any(test, feature = "testing-transport"))]
use std::sync::Arc;
use std::{collections::BTreeSet, time::Instant};

use o_sfu_router::{MediaCapabilities, MediaKind, MediaStream as RouterRtpParameters};

use super::{runtime_adapter::RtcTransport, shard_set::RtcTransportShardSet};
#[cfg(any(test, feature = "testing-transport"))]
use crate::runtime::media_transport::test_support::FakeMediaTransport;
use crate::{
    runtime::RoomInstanceId,
    transport::{
        ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, AppliedSessionAnswer, ConsumerActivity,
        ConsumerPacketGateUpdate, ProducerActivity, ReceiverBandwidthSnapshot, SessionOffer,
        SourcePacketGate, SourcePolicyUpdateSubscription, TransportAdapterError,
        TransportBitrateSnapshot, TransportMediaId, TransportPlacementPressureSnapshot,
        TransportRelayRouteEffect, TransportSessionHealth, TransportSessionKey,
    },
};

#[derive(Debug, Clone)]
pub(super) enum MediaTransportBackend {
    Rtc(RtcTransport),
    #[cfg(any(test, feature = "testing-transport"))]
    Fake(Arc<FakeMediaTransport>),
}

impl MediaTransportBackend {
    pub(in crate::runtime::media_transport) const fn from_rtc(transport: RtcTransport) -> Self {
        Self::Rtc(transport)
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::runtime::media_transport) fn from_fake(
        transport: Arc<FakeMediaTransport>,
    ) -> Self {
        Self::Fake(transport)
    }

    pub(in crate::runtime::media_transport) async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .shards
                    .create_initial_session_offer(session_key)
                    .await
            }
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.create_initial_session_offer(session_key).await,
        }
    }

    pub(in crate::runtime::media_transport) async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .shards
                    .create_session_renegotiation_offer(session_key)
                    .await
            }
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => {
                transport
                    .create_session_renegotiation_offer(session_key)
                    .await
            }
        }
    }

    pub(in crate::runtime::media_transport) async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, TransportAdapterError> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .shards
                    .apply_session_answer(session_key, answer_sdp)
                    .await
            }
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => {
                transport
                    .apply_session_answer(session_key, answer_sdp)
                    .await
            }
        }
    }

    pub(in crate::runtime::media_transport) fn negotiated_client_rtp_capabilities(
        &self,
        answer_sdp: &str,
        offered_router_capabilities: &MediaCapabilities,
    ) -> Result<MediaCapabilities, TransportAdapterError> {
        match self {
            Self::Rtc(_) => RtcTransportShardSet::negotiated_client_rtp_capabilities(
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

    pub(in crate::runtime::media_transport) async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Rtc(transport) => transport.shards.close_session(session_key).await,
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.close_session(session_key).await,
        }
    }

    pub(in crate::runtime::media_transport) async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .shards
                    .remove_media(session_key, transport_media_id)
                    .await
            }
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => {
                transport
                    .remove_media(session_key, transport_media_id)
                    .await
            }
        }
    }

    pub(in crate::runtime::media_transport) async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .shards
                    .publish_media(session_key, media_kind, rtp_parameters)
                    .await
            }
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => {
                transport
                    .publish_media(session_key, media_kind, rtp_parameters)
                    .await
            }
        }
    }

    pub(in crate::runtime::media_transport) async fn consume_media(
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
                    .shards
                    .consume_media(
                        consumer_session_key,
                        media_kind,
                        source_session_key,
                        source_media_id,
                        consumer_rtp_parameters,
                    )
                    .await
            }
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => {
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

    pub(in crate::runtime::media_transport) async fn apply_relay_route_effect(
        &self,
        effect: &TransportRelayRouteEffect,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Rtc(transport) => transport.shards.apply_relay_route_effect(effect).await,
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.apply_relay_route_effect(effect).await,
        }
    }

    pub(in crate::runtime::media_transport) async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        activity: ProducerActivity,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .shards
                    .set_producer_active(session_key, transport_media_id, activity.is_active())
                    .await
            }
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => {
                transport
                    .set_producer_active(session_key, transport_media_id, activity.is_active())
                    .await
            }
        }
    }

    pub(in crate::runtime::media_transport) async fn set_consumer_active(
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
                    .shards
                    .set_consumer_active(
                        consumer_session_key,
                        consumer_transport_media_id,
                        source_session_key,
                        source_transport_media_id,
                        activity.is_active(),
                    )
                    .await
            }
            #[cfg(any(test, feature = "testing-transport"))]
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

    pub(in crate::runtime::media_transport) async fn set_consumer_packet_gate(
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
                    .shards
                    .set_consumer_packet_gate(
                        consumer_session_key,
                        consumer_transport_media_id,
                        source_session_key,
                        source_transport_media_id,
                        packet_gate,
                    )
                    .await
            }
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => {
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

    pub(in crate::runtime::media_transport) async fn set_consumer_packet_gates(
        &self,
        updates: &[ConsumerPacketGateUpdate],
    ) -> Vec<Result<(), TransportAdapterError>> {
        match self {
            Self::Rtc(transport) => transport.shards.set_consumer_packet_gates(updates).await,
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.set_consumer_packet_gates(updates).await,
        }
    }

    pub(in crate::runtime::media_transport) async fn request_consumer_keyframe(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .shards
                    .request_consumer_keyframe(
                        consumer_session_key,
                        consumer_transport_media_id,
                        source_session_key,
                        source_transport_media_id,
                    )
                    .await
            }
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => {
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

    pub(in crate::runtime::media_transport) async fn transport_media_mid(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Option<String> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .shards
                    .transport_media_mid(session_key, transport_media_id)
                    .await
            }
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(_) => None,
        }
    }

    pub(in crate::runtime::media_transport) fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        match self {
            Self::Rtc(transport) => transport.shards.transport_bitrate_snapshot(session_keys),
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(_) => TransportBitrateSnapshot::default(),
        }
    }

    pub(in crate::runtime::media_transport) fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        match self {
            Self::Rtc(transport) => transport.shards.receiver_bandwidth_snapshot(session_keys),
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.receiver_bandwidth_snapshot(session_keys),
        }
    }

    pub(in crate::runtime::media_transport) fn placement_pressure_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportPlacementPressureSnapshot {
        match self {
            Self::Rtc(transport) => transport.shards.placement_pressure_snapshot(session_keys),
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.placement_pressure_snapshot(session_keys),
        }
    }

    pub(in crate::runtime::media_transport) async fn active_speaker_source_snapshot(
        &self,
    ) -> Vec<ActiveSpeakerSource> {
        match self {
            Self::Rtc(transport) => transport.shards.active_speaker_source_snapshot().await,
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.active_speaker_source_snapshot().await,
        }
    }

    pub(in crate::runtime::media_transport) async fn active_speaker_diagnostic_snapshot(
        &self,
    ) -> Vec<ActiveSpeakerSourceDiagnostic> {
        match self {
            Self::Rtc(transport) => transport.shards.active_speaker_diagnostic_snapshot().await,
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.active_speaker_diagnostic_snapshot().await,
        }
    }

    pub(in crate::runtime::media_transport) async fn next_active_speaker_deadline(
        &self,
    ) -> Option<Instant> {
        match self {
            Self::Rtc(transport) => transport.shards.next_active_speaker_deadline().await,
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(_) => None,
        }
    }

    pub(in crate::runtime::media_transport) async fn expired_active_speaker_room_instance_ids(
        &self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .shards
                    .expired_active_speaker_room_instance_ids(now)
                    .await
            }
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(_) => BTreeSet::new(),
        }
    }

    pub(in crate::runtime::media_transport) fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        match self {
            Self::Rtc(transport) => transport.shards.session_transport_health(session_key),
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(_) => None,
        }
    }

    pub(in crate::runtime::media_transport) fn source_policy_subscription(
        &self,
    ) -> SourcePolicyUpdateSubscription {
        match self {
            Self::Rtc(transport) => transport.shards.source_policy_subscription(),
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.source_policy_signal().subscribe(),
        }
    }
}
