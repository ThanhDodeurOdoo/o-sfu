use std::{collections::BTreeSet, sync::Arc, time::Instant};

use o_sfu_router::{MediaCapabilities, MediaKind, MediaStream as RouterRtpParameters};
use tracing::warn;

#[cfg(any(test, feature = "testing-transport"))]
use super::fake::FakeWebRtcAdapter;
use super::{
    config::RtcTransportAdapterShardSetConfig,
    ports::{MediaPort, NegotiationPort, ObservabilityPort, SessionPort, SourcePolicyPort},
    shard_set::RtcTransportAdapterShardSet,
    types::{
        ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, AppliedSessionAnswer,
        ConsumerPacketGateUpdate, ReceiverBandwidthSnapshot, SessionOffer, SourcePacketGate,
        TransportAdapterError, TransportBitrateSnapshot, TransportMediaId, TransportSessionKey,
    },
};
use crate::runtime::{
    ChannelInstanceId, rtc_adapter::TransportSessionHealth,
    transport_adapter::SourcePolicyUpdateSubscription,
};

macro_rules! dispatch_transport_backend {
    ($adapter:expr, |$backend:ident| $body:block) => {{
        match $adapter {
            Self::Rtc($backend) => $body,
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Test($backend) => $body,
        }
    }};
}

/// Runtime boundary between signaling/session orchestration and transport-specific behavior.
#[derive(Debug, Clone)]
pub(crate) enum RuntimeTransportAdapter {
    Rtc(Arc<RtcTransportAdapterShardSet>),
    #[cfg(any(test, feature = "testing-transport"))]
    Test(Arc<FakeWebRtcAdapter>),
}

impl RuntimeTransportAdapter {
    #[must_use]
    pub(crate) fn rtc(config: &RtcTransportAdapterShardSetConfig) -> Self {
        Self::Rtc(Arc::new(RtcTransportAdapterShardSet::new(config)))
    }

    #[cfg(test)]
    pub(crate) async fn negotiated_producer_parameters(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError> {
        match self {
            Self::Rtc(shards) => {
                shards
                    .shard_for_session(session_key)
                    .media()
                    .negotiated_producer_parameters(session_key, transport_media_id)
                    .await
            }
            Self::Test(fake) => {
                fake.negotiated_producer_parameters(session_key, transport_media_id)
                    .await
            }
        }
    }
}

impl NegotiationPort for RuntimeTransportAdapter {
    async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        let result = dispatch_transport_backend!(self, |backend| {
            backend.create_initial_session_offer(session_key).await
        });
        if let Err(error) = &result {
            warn!(
                ?session_key,
                ?error,
                "transport adapter failed to create initial session offer"
            );
        }
        result
    }

    async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        let result = dispatch_transport_backend!(self, |backend| {
            backend
                .create_session_renegotiation_offer(session_key)
                .await
        });
        if let Err(error) = &result {
            warn!(
                ?session_key,
                ?error,
                "transport adapter failed to create renegotiation offer"
            );
        }
        result
    }

    async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, TransportAdapterError> {
        let result = dispatch_transport_backend!(self, |backend| {
            backend.apply_session_answer(session_key, answer_sdp).await
        });
        if let Err(error) = &result {
            warn!(
                ?session_key,
                answer_len = answer_sdp.len(),
                ?error,
                "transport adapter failed to apply session answer"
            );
        }
        result
    }

    fn negotiated_client_rtp_capabilities(
        &self,
        answer_sdp: &str,
        offered_router_capabilities: &MediaCapabilities,
    ) -> Result<MediaCapabilities, TransportAdapterError> {
        let result = dispatch_transport_backend!(self, |backend| {
            backend.negotiated_client_rtp_capabilities(answer_sdp, offered_router_capabilities)
        });
        if let Err(error) = &result {
            warn!(
                answer_len = answer_sdp.len(),
                ?error,
                "transport adapter failed to derive client RTP capabilities from answer SDP"
            );
        }
        result
    }
}

impl SessionPort for RuntimeTransportAdapter {
    async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        let result = dispatch_transport_backend!(self, |backend| {
            backend.close_session(session_key).await
        });
        if let Err(error) = &result {
            warn!(
                ?session_key,
                ?error,
                "transport adapter failed to close session"
            );
        }
        result
    }
}

impl MediaPort for RuntimeTransportAdapter {
    async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        let result = dispatch_transport_backend!(self, |backend| {
            backend.remove_media(session_key, transport_media_id).await
        });
        if let Err(error) = &result {
            warn!(
                ?session_key,
                ?transport_media_id,
                ?error,
                "transport adapter failed to remove media"
            );
        }
        result
    }

    async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        let result = dispatch_transport_backend!(self, |backend| {
            backend
                .publish_media(session_key, media_kind, rtp_parameters)
                .await
        });
        if let Err(error) = &result {
            warn!(
                ?session_key,
                ?media_kind,
                mid = rtp_parameters.mid(),
                ?error,
                "transport adapter failed to declare producer media"
            );
        }
        result
    }

    async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_media_id: TransportMediaId,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        let result = dispatch_transport_backend!(self, |backend| {
            backend
                .consume_media(
                    consumer_session_key,
                    media_kind,
                    source_session_key,
                    source_media_id,
                    consumer_rtp_parameters,
                )
                .await
        });
        if let Err(error) = &result {
            warn!(
                ?consumer_session_key,
                ?source_session_key,
                ?source_media_id,
                ?media_kind,
                mid = consumer_rtp_parameters.mid(),
                ?error,
                "transport adapter failed to declare consumer media"
            );
        }
        result
    }

    async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        let result = dispatch_transport_backend!(self, |backend| {
            backend
                .set_producer_active(session_key, transport_media_id, active)
                .await
        });
        if let Err(error) = &result {
            warn!(
                ?session_key,
                ?transport_media_id,
                active,
                ?error,
                "transport adapter failed to update producer activity"
            );
        }
        result
    }

    async fn set_consumer_active(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        let result = dispatch_transport_backend!(self, |backend| {
            backend
                .set_consumer_active(
                    consumer_session_key,
                    consumer_transport_media_id,
                    source_session_key,
                    source_transport_media_id,
                    active,
                )
                .await
        });
        if let Err(error) = &result {
            warn!(
                ?consumer_session_key,
                ?consumer_transport_media_id,
                ?source_session_key,
                ?source_transport_media_id,
                active,
                ?error,
                "transport adapter failed to update consumer activity"
            );
        }
        result
    }

    async fn set_consumer_packet_gate(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: SourcePacketGate,
    ) -> Result<(), TransportAdapterError> {
        let result = dispatch_transport_backend!(self, |backend| {
            backend
                .set_consumer_packet_gate(
                    consumer_session_key,
                    consumer_transport_media_id,
                    source_session_key,
                    source_transport_media_id,
                    packet_gate.clone(),
                )
                .await
        });
        if let Err(error) = &result {
            warn!(
                ?consumer_session_key,
                ?consumer_transport_media_id,
                ?source_session_key,
                ?source_transport_media_id,
                ?packet_gate,
                ?error,
                "transport adapter failed to update consumer packet gate"
            );
        }
        result
    }

    async fn set_consumer_packet_gates(
        &self,
        updates: &[ConsumerPacketGateUpdate],
    ) -> Vec<Result<(), TransportAdapterError>> {
        let results = dispatch_transport_backend!(self, |backend| {
            backend.set_consumer_packet_gates(updates).await
        });
        for (update, result) in updates.iter().zip(results.iter()) {
            if let Err(error) = result {
                warn!(
                    ?error,
                    consumer_session_key = ?update.consumer_session_key(),
                    consumer_transport_media_id = ?update.consumer_transport_media_id(),
                    source_session_key = ?update.source_session_key(),
                    source_transport_media_id = ?update.source_transport_media_id(),
                    packet_gate = ?update.packet_gate(),
                    "transport adapter failed to update a batched consumer packet gate"
                );
            }
        }
        results
    }

    async fn request_consumer_keyframe(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        let result = dispatch_transport_backend!(self, |backend| {
            backend
                .request_consumer_keyframe(
                    consumer_session_key,
                    consumer_transport_media_id,
                    source_session_key,
                    source_transport_media_id,
                )
                .await
        });
        if let Err(error) = &result {
            warn!(
                ?consumer_session_key,
                ?consumer_transport_media_id,
                ?source_session_key,
                ?source_transport_media_id,
                ?error,
                "transport adapter failed to request a consumer keyframe refresh"
            );
        }
        result
    }

    async fn transport_media_mid(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Option<String> {
        dispatch_transport_backend!(self, |backend| {
            backend
                .transport_media_mid(session_key, transport_media_id)
                .await
        })
    }
}

impl ObservabilityPort for RuntimeTransportAdapter {
    fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        dispatch_transport_backend!(self, |backend| {
            backend.transport_bitrate_snapshot(session_keys)
        })
    }

    fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        dispatch_transport_backend!(self, |backend| {
            backend.receiver_bandwidth_snapshot(session_keys)
        })
    }

    async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        dispatch_transport_backend!(self, |backend| {
            backend.active_speaker_source_snapshot().await
        })
    }

    async fn active_speaker_diagnostic_snapshot(&self) -> Vec<ActiveSpeakerSourceDiagnostic> {
        dispatch_transport_backend!(self, |backend| {
            backend.active_speaker_diagnostic_snapshot().await
        })
    }

    async fn next_active_speaker_deadline(&self) -> Option<Instant> {
        dispatch_transport_backend!(self, |backend| {
            backend.next_active_speaker_deadline().await
        })
    }

    async fn expired_active_speaker_channel_instance_ids(
        &self,
        now: Instant,
    ) -> BTreeSet<ChannelInstanceId> {
        dispatch_transport_backend!(self, |backend| {
            backend
                .expired_active_speaker_channel_instance_ids(now)
                .await
        })
    }

    fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        dispatch_transport_backend!(self, |backend| {
            backend.session_transport_health(session_key)
        })
    }
}

impl SourcePolicyPort for RuntimeTransportAdapter {
    fn source_policy_subscription(&self) -> SourcePolicyUpdateSubscription {
        dispatch_transport_backend!(self, |backend| { backend.source_policy_subscription() })
    }
}
