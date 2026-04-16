use std::collections::BTreeMap;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[cfg(test)]
use super::fake_bootstrap;
use crate::runtime::transport_adapter::{
    ActiveSpeakerSource, SessionOffer, SourcePacketGate, TransportAdapterError, TransportMediaId,
    TransportSessionKey,
};
#[cfg(test)]
use crate::runtime::transport_bootstrap::SessionTransportBootstrap;
use crate::signaling::shared::SessionId;
use o_sfu_router::{
    MediaFormat as RouterMediaFormat, MediaKind, MediaKind as RouterMediaKind, RtcpFeedback,
    RtcpFeedbackKind, RtpParameters as RouterRtpParameters, StreamBinding,
};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::time::sleep;

const FAKE_SESSION_NEGOTIATION_OFFER_SDP: &str = "v=0\r\ns=o-sfu-fake-offer\r\n";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FakeWebRtcEvent {
    #[cfg(test)]
    BootstrapRequested,
    SessionClosed {
        session_id: SessionId,
    },
    PublishMediaRequested {
        session_id: SessionId,
        media_kind: MediaKind,
    },
    ConsumeMediaRequested {
        consumer_session_id: SessionId,
        source_session_id: SessionId,
        media_kind: MediaKind,
    },
    MediaRemoved {
        session_id: SessionId,
        transport_media_id: TransportMediaId,
    },
    ProducerActivityUpdated {
        session_id: SessionId,
        active: bool,
    },
    ConsumerActivityUpdated {
        consumer_session_id: SessionId,
        source_session_id: SessionId,
        active: bool,
    },
    SourcePacketGateUpdated {
        session_id: SessionId,
        transport_media_id: TransportMediaId,
        packet_gate: Option<SourcePacketGate>,
    },
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FakeWebRtcAdapter {
    events: Arc<Mutex<Vec<FakeWebRtcEvent>>>,
    next_media_id: Arc<AtomicU64>,
    negotiated_producer_parameters: Arc<Mutex<BTreeMap<TransportMediaId, RouterRtpParameters>>>,
    active_speaker_sources: Arc<Mutex<Vec<ActiveSpeakerSource>>>,
    delays: Arc<Mutex<FakeWebRtcAdapterDelays>>,
}

#[derive(Debug, Clone, Copy, Default)]
struct FakeWebRtcAdapterDelays {
    publish_media: Option<Duration>,
    consume_media: Option<Duration>,
    producer_activity: Option<Duration>,
}

#[allow(
    clippy::unused_async,
    reason = "fake adapter keeps the same async boundary as the rtc adapter and runtime call sites"
)]
impl FakeWebRtcAdapter {
    fn record_event(&self, event: FakeWebRtcEvent) {
        match self.events.lock() {
            Ok(mut events) => {
                events.push(event);
            }
            Err(poisoned) => {
                poisoned.into_inner().push(event);
            }
        }
    }

    fn delay_for_publish_media(&self) -> Option<Duration> {
        match self.delays.lock() {
            Ok(delays) => delays.publish_media,
            Err(poisoned) => poisoned.into_inner().publish_media,
        }
    }

    fn delay_for_consume_media(&self) -> Option<Duration> {
        match self.delays.lock() {
            Ok(delays) => delays.consume_media,
            Err(poisoned) => poisoned.into_inner().consume_media,
        }
    }

    fn delay_for_producer_activity(&self) -> Option<Duration> {
        match self.delays.lock() {
            Ok(delays) => delays.producer_activity,
            Err(poisoned) => poisoned.into_inner().producer_activity,
        }
    }

    #[cfg(test)]
    pub(crate) fn snapshot_events(&self) -> Vec<FakeWebRtcEvent> {
        match self.events.lock() {
            Ok(events) => events.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_publish_media_delay(&self, delay: Option<Duration>) {
        match self.delays.lock() {
            Ok(mut delays) => {
                delays.publish_media = delay;
            }
            Err(poisoned) => {
                poisoned.into_inner().publish_media = delay;
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn set_consume_media_delay(&self, delay: Option<Duration>) {
        match self.delays.lock() {
            Ok(mut delays) => {
                delays.consume_media = delay;
            }
            Err(poisoned) => {
                poisoned.into_inner().consume_media = delay;
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn set_producer_active_delay(&self, delay: Option<Duration>) {
        match self.delays.lock() {
            Ok(mut delays) => {
                delays.producer_activity = delay;
            }
            Err(poisoned) => {
                poisoned.into_inner().producer_activity = delay;
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn set_active_speaker_source_snapshot(&self, sources: Vec<ActiveSpeakerSource>) {
        match self.active_speaker_sources.lock() {
            Ok(mut active_speaker_sources) => {
                *active_speaker_sources = sources;
            }
            Err(poisoned) => {
                *poisoned.into_inner() = sources;
            }
        }
    }
}

impl FakeWebRtcAdapter {
    #[allow(
        clippy::unused_async,
        reason = "fake adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(crate) async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        match self.active_speaker_sources.lock() {
            Ok(active_speaker_sources) => active_speaker_sources.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    pub(crate) fn compatibility_client_rtp_capabilities(
        answer_sdp: &str,
        offered_router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<o_sfu_router::RtpCapabilities, TransportAdapterError> {
        if !answer_sdp.trim_start().starts_with("v=0") {
            return Err(TransportAdapterError::InvalidInput);
        }
        Ok(offered_router_capabilities.clone())
    }

    #[allow(
        clippy::unused_async,
        reason = "fake adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(crate) async fn create_initial_session_offer(
        &self,
        _session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        Ok(SessionOffer::new(String::from(
            FAKE_SESSION_NEGOTIATION_OFFER_SDP,
        )))
    }

    #[allow(
        clippy::unused_async,
        reason = "fake adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(crate) async fn create_session_renegotiation_offer(
        &self,
        _session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        Ok(SessionOffer::new(String::from(
            FAKE_SESSION_NEGOTIATION_OFFER_SDP,
        )))
    }

    #[allow(
        clippy::unused_async,
        reason = "fake adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(crate) async fn apply_session_answer(
        &self,
        _session_key: &TransportSessionKey,
        _answer_sdp: &str,
    ) -> Result<(), TransportAdapterError> {
        Ok(())
    }

    #[allow(
        clippy::unused_async,
        reason = "fake adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    #[cfg(test)]
    pub(crate) async fn transport_bootstrap_payload(
        &self,
        _session_key: &TransportSessionKey,
        router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<SessionTransportBootstrap, TransportAdapterError> {
        self.record_event(FakeWebRtcEvent::BootstrapRequested);
        Ok(fake_bootstrap::transport_bootstrap_payload(
            router_capabilities,
        ))
    }

    #[allow(
        clippy::unused_async,
        reason = "fake adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(crate) async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        self.record_event(FakeWebRtcEvent::SessionClosed {
            session_id: session_key.session_id().clone(),
        });
        Ok(())
    }

    #[allow(
        clippy::unused_async,
        reason = "fake adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(crate) async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        match self.negotiated_producer_parameters.lock() {
            Ok(mut parameters) => {
                parameters.remove(&transport_media_id);
            }
            Err(poisoned) => {
                poisoned.into_inner().remove(&transport_media_id);
            }
        }
        self.record_event(FakeWebRtcEvent::MediaRemoved {
            session_id: session_key.session_id().clone(),
            transport_media_id,
        });
        Ok(())
    }

    #[allow(
        clippy::unused_async,
        reason = "fake adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(crate) async fn negotiated_producer_parameters(
        &self,
        _session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError> {
        match self.negotiated_producer_parameters.lock() {
            Ok(parameters) => parameters
                .get(&transport_media_id)
                .cloned()
                .ok_or(TransportAdapterError::UnsupportedFeature),
            Err(poisoned) => poisoned
                .into_inner()
                .get(&transport_media_id)
                .cloned()
                .ok_or(TransportAdapterError::UnsupportedFeature),
        }
    }

    #[allow(
        clippy::unused_async,
        reason = "fake adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(crate) async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        _rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.record_event(FakeWebRtcEvent::PublishMediaRequested {
            session_id: session_key.session_id().clone(),
            media_kind,
        });
        if let Some(delay) = self.delay_for_publish_media() {
            sleep(delay).await;
        }
        let id = self.next_media_id.fetch_add(1, Ordering::Relaxed);
        let transport_media_id = TransportMediaId::new(id);
        let negotiated = synthetic_negotiated_producer_parameters(media_kind, transport_media_id);
        match self.negotiated_producer_parameters.lock() {
            Ok(mut parameters) => {
                parameters.insert(transport_media_id, negotiated);
            }
            Err(poisoned) => {
                poisoned.into_inner().insert(transport_media_id, negotiated);
            }
        }
        Ok(transport_media_id)
    }

    #[allow(
        clippy::unused_async,
        reason = "fake adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(crate) async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        _consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.record_event(FakeWebRtcEvent::ConsumeMediaRequested {
            consumer_session_id: consumer_session_key.session_id().clone(),
            source_session_id: source_session_key.session_id().clone(),
            media_kind,
        });
        if let Some(delay) = self.delay_for_consume_media() {
            sleep(delay).await;
        }
        let id = self.next_media_id.fetch_add(1, Ordering::Relaxed);
        Ok(TransportMediaId::new(id))
    }

    #[allow(
        clippy::unused_async,
        reason = "fake adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(crate) async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        _transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.record_event(FakeWebRtcEvent::ProducerActivityUpdated {
            session_id: session_key.session_id().clone(),
            active,
        });
        if let Some(delay) = self.delay_for_producer_activity() {
            sleep(delay).await;
        }
        Ok(())
    }

    #[allow(
        clippy::unused_async,
        reason = "fake adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(crate) async fn set_consumer_active(
        &self,
        consumer_session_key: &TransportSessionKey,
        _consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        _source_transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.record_event(FakeWebRtcEvent::ConsumerActivityUpdated {
            consumer_session_id: consumer_session_key.session_id().clone(),
            source_session_id: source_session_key.session_id().clone(),
            active,
        });
        Ok(())
    }

    #[allow(
        clippy::unused_async,
        reason = "fake adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(crate) async fn set_source_packet_gate(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        packet_gate: Option<SourcePacketGate>,
    ) -> Result<(), TransportAdapterError> {
        self.record_event(FakeWebRtcEvent::SourcePacketGateUpdated {
            session_id: session_key.session_id().clone(),
            transport_media_id,
            packet_gate,
        });
        Ok(())
    }
}

fn synthetic_negotiated_producer_parameters(
    media_kind: MediaKind,
    transport_media_id: TransportMediaId,
) -> RouterRtpParameters {
    let (router_media_kind, codec_name, payload_type, clock_rate) = match media_kind {
        MediaKind::Audio => (RouterMediaKind::Audio, "opus", 111_u8, 48_000_u32),
        MediaKind::Video => (RouterMediaKind::Video, "VP8", 96_u8, 90_000_u32),
    };
    let mut codec = RouterMediaFormat::new(router_media_kind, codec_name, payload_type, clock_rate);
    if matches!(media_kind, MediaKind::Audio) {
        codec = codec.with_channels(2);
    } else {
        codec = codec.with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None));
    }
    let transport_media_u64 = transport_media_id.as_u64();
    let ssrc_suffix = u32::try_from(transport_media_u64).unwrap_or(u32::MAX.saturating_sub(90_000));
    RouterRtpParameters::new(
        vec![codec],
        vec![],
        vec![
            StreamBinding::new()
                .with_ssrc(90_000_u32.saturating_add(ssrc_suffix))
                .with_payload_type(payload_type),
        ],
    )
    .with_mid(format!("fake-mid-{transport_media_u64}"))
}
