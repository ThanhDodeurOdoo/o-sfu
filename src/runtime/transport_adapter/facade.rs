use std::sync::Arc;

use super::config::RtcTransportAdapterShardSetConfig;
use super::shard_set::RtcTransportAdapterShardSet;
#[cfg(any(test, feature = "testing-transport"))]
use super::test_support::FakeWebRtcAdapter;
use super::types::{
    ActiveSpeakerSource, SessionOffer, SourcePacketGate, TransportAdapterError,
    TransportBitrateSnapshot, TransportMediaId, TransportSessionKey,
};
use crate::runtime::rtc_adapter::{TransportSessionHealth, client_rtp_capabilities_from_answer};
use o_sfu_router::{MediaCapabilities, MediaKind, RtpParameters as RouterRtpParameters};
use str0m::media::MediaKind as Str0mMediaKind;
use tracing::warn;

macro_rules! dispatch_transport_backend {
    ($adapter:expr, rtc => |$rtc:ident| $rtc_body:block, test => |$test:ident| $test_body:block) => {{
        match $adapter {
            RuntimeTransportAdapter::Rtc($rtc) => $rtc_body,
            #[cfg(any(test, feature = "testing-transport"))]
            RuntimeTransportAdapter::Test($test) => $test_body,
        }
    }};
}

/// Runtime boundary between signaling/session orchestration and transport-specific behavior.
///
/// The enum remains only as the transport selector. Semantic responsibilities are
/// surfaced through `negotiation()`, `media()`, `sessions()`, and `observability()`
/// so the selector itself does not keep growing as a catch-all facade.
#[derive(Debug, Clone)]
pub(crate) enum RuntimeTransportAdapter {
    Rtc(Arc<RtcTransportAdapterShardSet>),
    #[cfg(any(test, feature = "testing-transport"))]
    Test(Arc<FakeWebRtcAdapter>),
}

#[derive(Clone, Copy)]
pub(crate) struct RuntimeTransportNegotiation<'a> {
    adapter: &'a RuntimeTransportAdapter,
}

#[derive(Clone, Copy)]
pub(crate) struct RuntimeTransportMedia<'a> {
    adapter: &'a RuntimeTransportAdapter,
}

#[derive(Clone, Copy)]
pub(crate) struct RuntimeTransportSessions<'a> {
    adapter: &'a RuntimeTransportAdapter,
}

#[derive(Clone, Copy)]
pub(crate) struct RuntimeTransportObservability<'a> {
    adapter: &'a RuntimeTransportAdapter,
}

impl RuntimeTransportAdapter {
    #[must_use]
    pub(crate) fn rtc(config: &RtcTransportAdapterShardSetConfig) -> Self {
        Self::Rtc(Arc::new(RtcTransportAdapterShardSet::new(config)))
    }

    #[must_use]
    pub(crate) const fn negotiation(&self) -> RuntimeTransportNegotiation<'_> {
        RuntimeTransportNegotiation { adapter: self }
    }

    #[must_use]
    pub(crate) const fn media(&self) -> RuntimeTransportMedia<'_> {
        RuntimeTransportMedia { adapter: self }
    }

    #[must_use]
    pub(crate) const fn sessions(&self) -> RuntimeTransportSessions<'_> {
        RuntimeTransportSessions { adapter: self }
    }

    #[must_use]
    pub(crate) const fn observability(&self) -> RuntimeTransportObservability<'_> {
        RuntimeTransportObservability { adapter: self }
    }

    pub(crate) async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.negotiation()
            .create_initial_session_offer(session_key)
            .await
    }

    pub(crate) async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.negotiation()
            .create_session_renegotiation_offer(session_key)
            .await
    }

    pub(crate) async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<(), TransportAdapterError> {
        self.negotiation()
            .apply_session_answer(session_key, answer_sdp)
            .await
    }

    pub(crate) fn negotiated_client_rtp_capabilities(
        &self,
        answer_sdp: &str,
        offered_router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<MediaCapabilities, TransportAdapterError> {
        self.negotiation()
            .negotiated_client_rtp_capabilities(answer_sdp, offered_router_capabilities)
    }

    pub(crate) async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        self.sessions().close_session(session_key).await
    }

    pub(crate) async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        self.media()
            .remove_media(session_key, transport_media_id)
            .await
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
        self.media()
            .negotiated_producer_parameters(session_key, transport_media_id)
            .await
    }

    pub(crate) async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.media()
            .publish_media(session_key, media_kind, rtp_parameters)
            .await
    }

    pub(crate) async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_media_id: TransportMediaId,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.media()
            .consume_media(
                consumer_session_key,
                media_kind,
                source_session_key,
                source_media_id,
                consumer_rtp_parameters,
            )
            .await
    }

    pub(crate) fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        self.observability()
            .transport_bitrate_snapshot(session_keys)
    }

    pub(crate) async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        self.observability().active_speaker_source_snapshot().await
    }

    pub(crate) fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        self.observability().session_transport_health(session_key)
    }

    pub(crate) async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.media()
            .set_producer_active(session_key, transport_media_id, active)
            .await
    }

    pub(crate) async fn set_consumer_active(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.media()
            .set_consumer_active(
                consumer_session_key,
                consumer_transport_media_id,
                source_session_key,
                source_transport_media_id,
                active,
            )
            .await
    }

    pub(crate) async fn transport_media_mid(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Option<String> {
        self.media()
            .transport_media_mid(session_key, transport_media_id)
            .await
    }

    pub(crate) async fn set_source_packet_gate(
        &self,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: Option<SourcePacketGate>,
    ) -> Result<(), TransportAdapterError> {
        self.media()
            .set_source_packet_gate(source_session_key, source_transport_media_id, packet_gate)
            .await
    }
}

impl RuntimeTransportNegotiation<'_> {
    pub(crate) async fn create_initial_session_offer(
        self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        let result = dispatch_transport_backend!(self.adapter,
            rtc => |adapter| {
                adapter
                    .shard_for_session(session_key)
                    .negotiation()
                    .create_initial_session_offer(session_key)
                    .await
            },
            test => |adapter| { adapter.create_initial_session_offer(session_key).await }
        );
        if let Err(error) = &result {
            warn!(
                ?session_key,
                ?error,
                "transport adapter failed to create initial session offer"
            );
        }
        result
    }

    pub(crate) async fn create_session_renegotiation_offer(
        self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        let result = dispatch_transport_backend!(self.adapter,
            rtc => |adapter| {
                adapter
                    .shard_for_session(session_key)
                    .negotiation()
                    .create_session_renegotiation_offer(session_key)
                    .await
            },
            test => |adapter| { adapter.create_session_renegotiation_offer(session_key).await }
        );
        if let Err(error) = &result {
            warn!(
                ?session_key,
                ?error,
                "transport adapter failed to create renegotiation offer"
            );
        }
        result
    }

    pub(crate) async fn apply_session_answer(
        self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<(), TransportAdapterError> {
        let result = dispatch_transport_backend!(self.adapter,
            rtc => |adapter| {
                adapter
                    .shard_for_session(session_key)
                    .negotiation()
                    .apply_session_answer(session_key, answer_sdp)
                    .await
            },
            test => |adapter| { adapter.apply_session_answer(session_key, answer_sdp).await }
        );
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
        self,
        answer_sdp: &str,
        offered_router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<MediaCapabilities, TransportAdapterError> {
        #[cfg(not(any(test, feature = "testing-transport")))]
        let _ = offered_router_capabilities;
        let result = dispatch_transport_backend!(self.adapter,
            rtc => |_adapter| {
                client_rtp_capabilities_from_answer(answer_sdp)
                    .ok_or(TransportAdapterError::InvalidInput)
            },
            test => |_adapter| {
                FakeWebRtcAdapter::project_answered_client_rtp_capabilities(
                    answer_sdp,
                    offered_router_capabilities,
                )
            }
        );
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

impl RuntimeTransportSessions<'_> {
    pub(crate) async fn close_session(
        self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        let result = dispatch_transport_backend!(self.adapter,
            rtc => |adapter| {
                let session_shard = adapter.shard_for_session(session_key);
                let close_outcome = session_shard
                    .sessions()
                    .close_session_with_outcome(session_key)
                    .await?;
                adapter.release_relay_cleanup(&session_shard, close_outcome.relay_cleanup());
                Ok(())
            },
            test => |adapter| { adapter.close_session(session_key).await }
        );
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

impl RuntimeTransportMedia<'_> {
    pub(crate) async fn remove_media(
        self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        let result = dispatch_transport_backend!(self.adapter,
            rtc => |adapter| {
                let session_shard = adapter.shard_for_session(session_key);
                let remove_outcome = session_shard
                    .media()
                    .remove_media_with_outcome(session_key, transport_media_id)
                    .await?;
                if let Some(cleanup) = remove_outcome.relay_cleanup() {
                    let relay_cleanup = [cleanup.clone()];
                    adapter.release_relay_cleanup(&session_shard, &relay_cleanup);
                }
                Ok(())
            },
            test => |adapter| { adapter.remove_media(session_key, transport_media_id).await }
        );
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
        self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError> {
        dispatch_transport_backend!(self.adapter,
            rtc => |adapter| {
                adapter
                    .shard_for_session(session_key)
                    .media()
                    .negotiated_producer_parameters(session_key, transport_media_id)
                    .await
            },
            test => |adapter| {
                adapter
                    .negotiated_producer_parameters(session_key, transport_media_id)
                    .await
            }
        )
    }

    pub(crate) async fn publish_media(
        self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        let result = dispatch_transport_backend!(self.adapter,
            rtc => |adapter| {
                adapter
                    .shard_for_session(session_key)
                    .media()
                    .add_recv_media(
                        session_key,
                        signaling_to_str0m_media_kind(media_kind),
                        rtp_parameters,
                    )
                    .await
            },
            test => |adapter| { adapter.publish_media(session_key, media_kind, rtp_parameters).await }
        );
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

    pub(crate) async fn consume_media(
        self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_media_id: TransportMediaId,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        ensure_same_channel_runtime(consumer_session_key, source_session_key)?;
        let result = dispatch_transport_backend!(self.adapter,
            rtc => |adapter| {
                let relay_route =
                    adapter.relay_registration_shards(consumer_session_key, source_session_key);
                let remote_source_control = relay_route
                    .as_ref()
                    .map(|(source_shard, consumer_shard)| {
                        source_shard
                            .media()
                            .remote_source_control(consumer_shard.as_ref())
                    })
                    .transpose()?;
                if let Some((source_shard, consumer_shard)) = &relay_route {
                    source_shard
                        .media()
                        .activate_relay_route(source_media_id, consumer_shard.as_ref())?;
                }
                let consumer_shard = adapter.shard_for_session(consumer_session_key);
                let add_result = consumer_shard
                    .media()
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
                        source_shard.media().set_relay_route_active(
                            source_media_id,
                            consumer_shard.as_ref(),
                            true,
                        );
                    } else {
                        source_shard
                            .media()
                            .deactivate_relay_route(source_media_id, consumer_shard.as_ref());
                    }
                }
                add_result
            },
            test => |adapter| {
                adapter
                    .consume_media(
                        consumer_session_key,
                        media_kind,
                        source_session_key,
                        consumer_rtp_parameters,
                    )
                    .await
            }
        );
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

    pub(crate) async fn set_producer_active(
        self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        let result = dispatch_transport_backend!(self.adapter,
            rtc => |adapter| {
                adapter
                    .shard_for_session(session_key)
                    .media()
                    .set_producer_active(session_key, transport_media_id, active)
                    .await
            },
            test => |adapter| { adapter.set_producer_active(session_key, transport_media_id, active).await }
        );
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

    pub(crate) async fn set_consumer_active(
        self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        ensure_same_channel_runtime(consumer_session_key, source_session_key)?;
        let result = dispatch_transport_backend!(self.adapter,
            rtc => |adapter| {
                adapter
                    .shard_for_session(consumer_session_key)
                    .media()
                    .set_consumer_active(
                        consumer_session_key,
                        consumer_transport_media_id,
                        source_session_key,
                        source_transport_media_id,
                        active,
                    )
                    .await
            },
            test => |adapter| {
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
        );
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
        self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Option<String> {
        dispatch_transport_backend!(self.adapter,
            rtc => |adapter| {
                adapter
                    .shard_for_session(session_key)
                    .media()
                    .transport_media_mid(transport_media_id)
                    .await
                    .ok()
                    .flatten()
            },
            test => |_adapter| { None }
        )
    }

    pub(crate) async fn set_source_packet_gate(
        self,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: Option<SourcePacketGate>,
    ) -> Result<(), TransportAdapterError> {
        let has_packet_gate = packet_gate.is_some();
        let result = dispatch_transport_backend!(self.adapter,
            rtc => |adapter| {
                adapter
                    .shard_for_session(source_session_key)
                    .media()
                    .set_source_packet_gate(
                        source_session_key,
                        source_transport_media_id,
                        packet_gate,
                    )
                    .await
            },
            test => |adapter| {
                adapter
                    .set_source_packet_gate(
                        source_session_key,
                        source_transport_media_id,
                        packet_gate,
                    )
                    .await
            }
        );
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

impl RuntimeTransportObservability<'_> {
    pub(crate) fn transport_bitrate_snapshot(
        self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        dispatch_transport_backend!(self.adapter,
            rtc => |adapter| { adapter.transport_bitrate_snapshot(session_keys) },
            test => |_adapter| { TransportBitrateSnapshot::default() }
        )
    }

    pub(crate) async fn active_speaker_source_snapshot(self) -> Vec<ActiveSpeakerSource> {
        dispatch_transport_backend!(self.adapter,
            rtc => |adapter| { adapter.active_speaker_source_snapshot().await },
            test => |adapter| { adapter.active_speaker_source_snapshot().await }
        )
    }

    pub(crate) fn session_transport_health(
        self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        dispatch_transport_backend!(self.adapter,
            rtc => |adapter| {
                adapter
                    .shard_for_session(session_key)
                    .observability()
                    .session_transport_health(session_key)
            },
            test => |_adapter| { None }
        )
    }
}

fn ensure_same_channel_runtime(
    consumer_session_key: &TransportSessionKey,
    source_session_key: &TransportSessionKey,
) -> Result<(), TransportAdapterError> {
    if consumer_session_key.channel_runtime_id() == source_session_key.channel_runtime_id() {
        return Ok(());
    }
    Err(TransportAdapterError::InvalidInput)
}

fn signaling_to_str0m_media_kind(kind: MediaKind) -> Str0mMediaKind {
    match kind {
        MediaKind::Audio => Str0mMediaKind::Audio,
        MediaKind::Video => Str0mMediaKind::Video,
    }
}
