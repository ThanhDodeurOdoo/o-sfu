use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use super::bootstrap;
use crate::runtime::transport_adapter::{
    TransportAdapterError, TransportConnectDirection, TransportMediaId, TransportSessionKey,
};
use crate::signaling::{
    current_protocol::CurrentTransportBootstrapPayload,
    shared::SessionId,
    webrtc::{DtlsParameters, IceParameters, MediaKind},
};
use o_sfu_router::RtpParameters as RouterRtpParameters;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::time::sleep;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StubWebRtcEvent {
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
    TransportConnectRequested {
        session_id: SessionId,
        direction: TransportConnectDirection,
        dtls_parameters: DtlsParameters,
    },
    TransportConnected {
        session_id: SessionId,
        direction: TransportConnectDirection,
    },
    TransportConnectRejected {
        session_id: SessionId,
        direction: TransportConnectDirection,
    },
}

#[derive(Debug, Clone, Default)]
pub(crate) struct StubWebRtcAdapter {
    events: Arc<Mutex<Vec<StubWebRtcEvent>>>,
    next_media_id: Arc<AtomicU64>,
    delays: Arc<Mutex<StubWebRtcAdapterDelays>>,
}

#[derive(Debug, Clone, Copy, Default)]
struct StubWebRtcAdapterDelays {
    publish_media: Option<Duration>,
    consume_media: Option<Duration>,
}

#[allow(
    clippy::unused_async,
    reason = "stub adapter keeps the same async boundary as the rtc adapter and runtime call sites"
)]
impl StubWebRtcAdapter {
    fn record_event(&self, event: StubWebRtcEvent) {
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

    #[cfg(test)]
    pub(crate) fn snapshot_events(&self) -> Vec<StubWebRtcEvent> {
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
}

impl StubWebRtcAdapter {
    #[allow(
        clippy::unused_async,
        reason = "stub adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(crate) async fn transport_bootstrap_payload(
        &self,
        _session_key: &TransportSessionKey,
        router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<CurrentTransportBootstrapPayload, TransportAdapterError> {
        self.record_event(StubWebRtcEvent::BootstrapRequested);
        Ok(bootstrap::transport_bootstrap_payload(router_capabilities))
    }

    #[allow(
        clippy::unused_async,
        reason = "stub adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(crate) async fn connect_transport(
        &self,
        session_key: &TransportSessionKey,
        direction: TransportConnectDirection,
        dtls_parameters: &DtlsParameters,
        _ice_parameters: Option<&IceParameters>,
        _sdp_offer: Option<&str>,
    ) -> Result<(), TransportAdapterError> {
        self.record_event(StubWebRtcEvent::TransportConnectRequested {
            session_id: session_key.session_id().clone(),
            direction,
            dtls_parameters: dtls_parameters.clone(),
        });
        if dtls_parameters.role.is_empty() || dtls_parameters.fingerprints.is_empty() {
            self.record_event(StubWebRtcEvent::TransportConnectRejected {
                session_id: session_key.session_id().clone(),
                direction,
            });
            return Err(TransportAdapterError::TransportUnavailable);
        }
        self.record_event(StubWebRtcEvent::TransportConnected {
            session_id: session_key.session_id().clone(),
            direction,
        });
        Ok(())
    }

    #[allow(
        clippy::unused_async,
        reason = "stub adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(crate) async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        self.record_event(StubWebRtcEvent::SessionClosed {
            session_id: session_key.session_id().clone(),
        });
        Ok(())
    }

    #[allow(
        clippy::unused_async,
        reason = "stub adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(crate) async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        self.record_event(StubWebRtcEvent::MediaRemoved {
            session_id: session_key.session_id().clone(),
            transport_media_id,
        });
        Ok(())
    }

    #[allow(
        clippy::unused_async,
        reason = "stub adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(crate) async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        _rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.record_event(StubWebRtcEvent::PublishMediaRequested {
            session_id: session_key.session_id().clone(),
            media_kind,
        });
        if let Some(delay) = self.delay_for_publish_media() {
            sleep(delay).await;
        }
        let id = self.next_media_id.fetch_add(1, Ordering::Relaxed);
        Ok(TransportMediaId::new(id))
    }

    #[allow(
        clippy::unused_async,
        reason = "stub adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(crate) async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        _consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.record_event(StubWebRtcEvent::ConsumeMediaRequested {
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
        reason = "stub adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(crate) async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        _transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.record_event(StubWebRtcEvent::ProducerActivityUpdated {
            session_id: session_key.session_id().clone(),
            active,
        });
        Ok(())
    }

    #[allow(
        clippy::unused_async,
        reason = "stub adapter keeps the same async boundary as the rtc adapter and runtime call sites"
    )]
    pub(crate) async fn set_consumer_active(
        &self,
        consumer_session_key: &TransportSessionKey,
        _consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        _source_transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.record_event(StubWebRtcEvent::ConsumerActivityUpdated {
            consumer_session_id: consumer_session_key.session_id().clone(),
            source_session_id: source_session_key.session_id().clone(),
            active,
        });
        Ok(())
    }
}
