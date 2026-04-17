use std::sync::Arc;

use super::config::RtcTransportAdapterShardSetConfig;
#[cfg(any(test, feature = "testing-transport"))]
use super::fake::FakeWebRtcAdapter;
use super::shard_set::RtcTransportAdapterShardSet;
use super::types::{
    ActiveSpeakerSource, SessionOffer, SourcePacketGate, TransportAdapterError,
    TransportBitrateSnapshot, TransportMediaId, TransportSessionKey,
};
use crate::runtime::rtc_adapter::{TransportSessionHealth, client_rtp_capabilities_from_answer};
use o_sfu_router::{MediaCapabilities, MediaKind, RtpParameters as RouterRtpParameters};
use str0m::media::MediaKind as Str0mMediaKind;
use tracing::warn;

/// Runtime boundary between signaling/session orchestration and transport-specific behavior.
///
/// Implementations provide transport bootstrap payloads and transport connection handling
/// without leaking concrete WebRTC library details into the signaling flow.
#[derive(Debug, Clone)]
pub(crate) enum RuntimeTransportAdapter {
    #[cfg(any(test, feature = "testing-transport"))]
    Fake(Arc<FakeWebRtcAdapter>),
    Rtc(Arc<RtcTransportAdapterShardSet>),
}

impl RuntimeTransportAdapter {
    #[must_use]
    pub(crate) fn rtc(config: &RtcTransportAdapterShardSetConfig) -> Self {
        Self::Rtc(Arc::new(RtcTransportAdapterShardSet::new(config)))
    }

    /// Create the first server-authored SDP offer for the protocol signaling path.
    pub(crate) async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        let result = match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(adapter) => adapter.create_initial_session_offer(session_key).await,
            Self::Rtc(adapter) => {
                adapter
                    .shard_for_session(session_key)
                    .create_initial_session_offer(session_key)
                    .await
            }
        };
        if let Err(error) = &result {
            warn!(
                ?session_key,
                ?error,
                "transport adapter failed to create initial session offer"
            );
        }
        result
    }

    /// Create a follow-up renegotiation offer for the protocol signaling path.
    pub(crate) async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        let result = match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(adapter) => {
                adapter
                    .create_session_renegotiation_offer(session_key)
                    .await
            }
            Self::Rtc(adapter) => {
                adapter
                    .shard_for_session(session_key)
                    .create_session_renegotiation_offer(session_key)
                    .await
            }
        };
        if let Err(error) = &result {
            warn!(
                ?session_key,
                ?error,
                "transport adapter failed to create renegotiation offer"
            );
        }
        result
    }

    /// Apply the remote answer to the outstanding protocol session offer.
    pub(crate) async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<(), TransportAdapterError> {
        let result = match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(adapter) => adapter.apply_session_answer(session_key, answer_sdp).await,
            Self::Rtc(adapter) => {
                adapter
                    .shard_for_session(session_key)
                    .apply_session_answer(session_key, answer_sdp)
                    .await
            }
        };
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

    pub(crate) fn negotiated_client_rtp_capabilities(
        &self,
        answer_sdp: &str,
        offered_router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<MediaCapabilities, TransportAdapterError> {
        #[cfg(not(any(test, feature = "testing-transport")))]
        let _ = offered_router_capabilities;
        let result = match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(_adapter) => FakeWebRtcAdapter::project_answered_client_rtp_capabilities(
                answer_sdp,
                offered_router_capabilities,
            ),
            Self::Rtc(_adapter) => client_rtp_capabilities_from_answer(answer_sdp)
                .ok_or(TransportAdapterError::InvalidInput),
        };
        if let Err(error) = &result {
            warn!(
                answer_len = answer_sdp.len(),
                ?error,
                "transport adapter failed to derive client RTP capabilities from answer SDP"
            );
        }
        result
    }

    /// Release transport-adapter state for a disconnected session.
    pub(crate) async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        let result = match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(adapter) => adapter.close_session(session_key).await,
            Self::Rtc(adapter) => {
                let session_shard = adapter.shard_for_session(session_key);
                let close_outcome = session_shard
                    .close_session_with_outcome(session_key)
                    .await?;
                adapter.release_relay_cleanup(&session_shard, close_outcome.relay_cleanup());
                Ok(())
            }
        };
        if let Err(error) = &result {
            warn!(
                ?session_key,
                ?error,
                "transport adapter failed to close session"
            );
        }
        result
    }

    /// Remove a previously declared media line owned by `session_id`.
    pub(crate) async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        let result = match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(adapter) => adapter.remove_media(session_key, transport_media_id).await,
            Self::Rtc(adapter) => {
                let session_shard = adapter.shard_for_session(session_key);
                let remove_outcome = session_shard
                    .remove_media_with_outcome(session_key, transport_media_id)
                    .await?;
                if let Some(cleanup) = remove_outcome.relay_cleanup() {
                    let relay_cleanup = [cleanup.clone()];
                    adapter.release_relay_cleanup(&session_shard, &relay_cleanup);
                }
                Ok(())
            }
        };
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

    #[allow(
        dead_code,
        reason = "protocol publish commit wiring is staged separately from answered-SDP publication extraction"
    )]
    pub(crate) async fn negotiated_producer_parameters(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError> {
        match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(adapter) => {
                adapter
                    .negotiated_producer_parameters(session_key, transport_media_id)
                    .await
            }
            Self::Rtc(adapter) => {
                adapter
                    .shard_for_session(session_key)
                    .negotiated_producer_parameters(session_key, transport_media_id)
                    .await
            }
        }
    }

    /// Declare a media line for receiving RTP from a producer session.
    pub(crate) async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        let result = match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(adapter) => {
                adapter
                    .publish_media(session_key, media_kind, rtp_parameters)
                    .await
            }
            Self::Rtc(adapter) => {
                adapter
                    .shard_for_session(session_key)
                    .add_recv_media(
                        session_key,
                        signaling_to_str0m_media_kind(media_kind),
                        rtp_parameters,
                    )
                    .await
            }
        };
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

    /// Declare a media line for sending RTP to a consumer session, routed from a producer.
    pub(crate) async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_media_id: TransportMediaId,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        let result = match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(adapter) => {
                adapter
                    .consume_media(
                        consumer_session_key,
                        media_kind,
                        source_session_key,
                        consumer_rtp_parameters,
                    )
                    .await
            }
            Self::Rtc(adapter) => {
                if consumer_session_key.channel_runtime_id()
                    != source_session_key.channel_runtime_id()
                {
                    return Err(TransportAdapterError::InvalidInput);
                }
                let relay_route =
                    adapter.relay_registration_shards(consumer_session_key, source_session_key);
                let remote_source_control = relay_route
                    .as_ref()
                    .map(|(source_shard, consumer_shard)| {
                        source_shard.remote_source_control(consumer_shard)
                    })
                    .transpose()?;
                if let Some((source_shard, consumer_shard)) = &relay_route {
                    source_shard.activate_relay_route(source_media_id, consumer_shard)?;
                }
                let consumer_shard = adapter.shard_for_session(consumer_session_key);
                let add_result = consumer_shard
                    .add_send_media(
                        consumer_session_key,
                        signaling_to_str0m_media_kind(media_kind),
                        source_session_key,
                        source_media_id,
                        remote_source_control,
                        consumer_rtp_parameters,
                    )
                    .await;
                if let Some((source_shard, consumer_shard)) = relay_route {
                    if add_result.is_ok() {
                        source_shard.set_relay_route_active(source_media_id, &consumer_shard, true);
                    } else {
                        source_shard.deactivate_relay_route(source_media_id, &consumer_shard);
                    }
                }
                add_result
            }
        };
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

    pub(crate) fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(_adapter) => TransportBitrateSnapshot::default(),
            Self::Rtc(adapter) => adapter.transport_bitrate_snapshot(session_keys),
        }
    }

    pub(crate) async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(adapter) => adapter.active_speaker_source_snapshot().await,
            Self::Rtc(adapter) => adapter.active_speaker_source_snapshot().await,
        }
    }

    pub(crate) fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(_adapter) => None,
            Self::Rtc(adapter) => adapter
                .shard_for_session(session_key)
                .session_transport_health(session_key),
        }
    }

    /// Update whether a producer media line is allowed to forward packets.
    pub(crate) async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        let result = match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(adapter) => {
                adapter
                    .set_producer_active(session_key, transport_media_id, active)
                    .await
            }
            Self::Rtc(adapter) => {
                adapter
                    .shard_for_session(session_key)
                    .set_producer_active(session_key, transport_media_id, active)
                    .await
            }
        };
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

    /// Update whether one consumer route is allowed to forward packets.
    pub(crate) async fn set_consumer_active(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        let result = match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(adapter) => {
                adapter
                    .set_consumer_active(
                        consumer_session_key,
                        consumer_transport_media_id,
                        source_session_key,
                        source_transport_media_id,
                        active,
                    )
                    .await
            }
            Self::Rtc(adapter) => {
                if consumer_session_key.channel_runtime_id()
                    != source_session_key.channel_runtime_id()
                {
                    return Err(TransportAdapterError::InvalidInput);
                }
                adapter
                    .shard_for_session(consumer_session_key)
                    .set_consumer_active(
                        consumer_session_key,
                        consumer_transport_media_id,
                        source_session_key,
                        source_transport_media_id,
                        active,
                    )
                    .await
            }
        };
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

    pub(crate) async fn transport_media_mid(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Option<String> {
        match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(_adapter) => None,
            Self::Rtc(adapter) => adapter
                .shard_for_session(session_key)
                .transport_media_mid(transport_media_id)
                .await
                .ok()
                .flatten(),
        }
    }

    /// Apply a generic packet-routing gate to one published media source.
    pub(crate) async fn set_source_packet_gate(
        &self,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: Option<SourcePacketGate>,
    ) -> Result<(), TransportAdapterError> {
        let has_packet_gate = packet_gate.is_some();
        let result = match self {
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(adapter) => {
                adapter
                    .set_source_packet_gate(
                        source_session_key,
                        source_transport_media_id,
                        packet_gate,
                    )
                    .await
            }
            Self::Rtc(adapter) => {
                adapter
                    .shard_for_session(source_session_key)
                    .set_source_packet_gate(
                        source_session_key,
                        source_transport_media_id,
                        packet_gate,
                    )
                    .await
            }
        };
        if let Err(error) = &result {
            warn!(
                ?source_session_key,
                ?source_transport_media_id,
                has_packet_gate,
                ?error,
                "transport adapter failed to update source packet gate"
            );
        }
        result
    }
}

fn signaling_to_str0m_media_kind(kind: MediaKind) -> Str0mMediaKind {
    match kind {
        MediaKind::Audio => Str0mMediaKind::Audio,
        MediaKind::Video => Str0mMediaKind::Video,
    }
}
