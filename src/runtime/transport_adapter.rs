use std::{fmt::Debug, net::IpAddr, sync::Arc};

use super::{rtc_adapter::RtcTransportAdapter, stub_bus::StubWebRtcAdapter};

use crate::config::RtcPortRange;
use crate::signaling::{
    current_protocol::CurrentTransportBootstrapPayload,
    shared::SessionId,
    shared::StreamType as SignalingStreamType,
    webrtc::{DtlsParameters, IceParameters, MediaKind as SignalingMediaKind},
};
use o_sfu_router::RtpParameters as RouterRtpParameters;
use str0m::media::MediaKind as Str0mMediaKind;

/// Channel-scoped transport-adapter session identity.
///
/// the transport boundary must distinguish identical session ids that are active in
/// different channels at the same time.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TransportSessionKey {
    channel_runtime: u64,
    connection: u64,
    session: SessionId,
}

impl TransportSessionKey {
    #[must_use]
    pub(crate) fn new(channel_runtime_id: u64, connection_id: u64, session_id: SessionId) -> Self {
        Self {
            channel_runtime: channel_runtime_id,
            connection: connection_id,
            session: session_id,
        }
    }

    #[must_use]
    pub(crate) fn channel_runtime_id(&self) -> u64 {
        self.channel_runtime
    }

    #[must_use]
    pub(crate) fn session_id(&self) -> &SessionId {
        &self.session
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TransportConnectDirection {
    Upload,
    Download,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportAdapterError {
    TransportUnavailable,
    InvalidInput,
    UnsupportedFeature,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct IncomingBitrateSnapshot {
    pub(crate) total: u64,
    pub(crate) audio: u64,
    pub(crate) camera: u64,
    pub(crate) screen: u64,
}

/// Opaque identifier for a media line allocated by the transport adapter.
///
/// Wraps the transport-internal representation (e.g. str0m `Mid`) without
/// exposing WebRTC library types to the signaling/channel layers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TransportMediaId(u64);

impl TransportMediaId {
    pub(super) fn new(raw: u64) -> Self {
        Self(raw)
    }

    pub(crate) fn as_u64(self) -> u64 {
        self.0
    }
}

/// Runtime boundary between signaling/session orchestration and transport-specific behavior.
///
/// Implementations provide transport bootstrap payloads and transport connection handling
/// without leaking concrete WebRTC library details into the signaling flow.
#[derive(Debug, Clone)]
pub(crate) enum RuntimeTransportAdapter {
    Stub(Arc<StubWebRtcAdapter>),
    Rtc(Arc<RtcTransportAdapter>),
}

impl RuntimeTransportAdapter {
    #[must_use]
    pub(crate) fn stub() -> Self {
        Self::Stub(Arc::new(StubWebRtcAdapter::default()))
    }

    #[must_use]
    pub(crate) fn rtc(public_ip: IpAddr, rtc_port_range: RtcPortRange) -> Self {
        Self::Rtc(Arc::new(RtcTransportAdapter::new(
            public_ip,
            rtc_port_range,
        )))
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn from_stub_adapter(adapter: Arc<StubWebRtcAdapter>) -> Self {
        Self::Stub(adapter)
    }

    /// Build the `INIT_TRANSPORTS` payload for a newly authenticated session.
    pub(crate) async fn transport_bootstrap_payload(
        &self,
        session_key: &TransportSessionKey,
        router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<CurrentTransportBootstrapPayload, TransportAdapterError> {
        match self {
            Self::Stub(adapter) => {
                adapter
                    .transport_bootstrap_payload(session_key, router_capabilities)
                    .await
            }
            Self::Rtc(adapter) => {
                adapter
                    .transport_bootstrap_payload(session_key, router_capabilities)
                    .await
            }
        }
    }

    /// Connect one direction transport with client DTLS parameters.
    pub(crate) async fn connect_transport(
        &self,
        session_key: &TransportSessionKey,
        direction: TransportConnectDirection,
        dtls_parameters: &DtlsParameters,
        ice_parameters: Option<&IceParameters>,
        sdp_offer: Option<&str>,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Stub(adapter) => {
                adapter
                    .connect_transport(
                        session_key,
                        direction,
                        dtls_parameters,
                        ice_parameters,
                        sdp_offer,
                    )
                    .await
            }
            Self::Rtc(adapter) => {
                adapter
                    .connect_transport(
                        session_key,
                        direction,
                        dtls_parameters,
                        ice_parameters,
                        sdp_offer,
                    )
                    .await
            }
        }
    }

    /// Release transport-adapter state for a disconnected session.
    pub(crate) async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Stub(adapter) => adapter.close_session(session_key).await,
            Self::Rtc(adapter) => adapter.close_session(session_key).await,
        }
    }

    /// Remove a previously declared media line owned by `session_id`.
    pub(crate) async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Stub(adapter) => adapter.remove_media(session_key, transport_media_id).await,
            Self::Rtc(adapter) => adapter.remove_media(session_key, transport_media_id).await,
        }
    }

    /// Declare a media line for receiving RTP from a producer session.
    pub(crate) async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        stream_type: SignalingStreamType,
        media_kind: SignalingMediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        match self {
            Self::Stub(adapter) => {
                adapter
                    .publish_media(session_key, stream_type, media_kind, rtp_parameters)
                    .await
            }
            Self::Rtc(adapter) => {
                adapter
                    .add_recv_media(
                        session_key,
                        stream_type,
                        signaling_to_str0m_media_kind(media_kind),
                        rtp_parameters,
                    )
                    .await
            }
        }
    }

    /// Declare a media line for sending RTP to a consumer session, routed from a producer.
    pub(crate) async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: SignalingMediaKind,
        source_session_key: &TransportSessionKey,
        source_media_id: TransportMediaId,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        match self {
            Self::Stub(adapter) => {
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
                adapter
                    .add_send_media(
                        consumer_session_key,
                        signaling_to_str0m_media_kind(media_kind),
                        source_session_key,
                        source_media_id,
                        consumer_rtp_parameters,
                    )
                    .await
            }
        }
    }

    pub(crate) fn incoming_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> IncomingBitrateSnapshot {
        match self {
            Self::Stub(_adapter) => IncomingBitrateSnapshot::default(),
            Self::Rtc(adapter) => adapter.incoming_bitrate_snapshot(session_keys),
        }
    }

    /// Update whether a producer media line is allowed to forward packets.
    pub(crate) async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Stub(adapter) => {
                adapter
                    .set_producer_active(session_key, transport_media_id, active)
                    .await
            }
            Self::Rtc(adapter) => {
                adapter
                    .set_producer_active(session_key, transport_media_id, active)
                    .await
            }
        }
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
        match self {
            Self::Stub(adapter) => {
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
        }
    }
}

fn signaling_to_str0m_media_kind(kind: SignalingMediaKind) -> Str0mMediaKind {
    match kind {
        SignalingMediaKind::Audio => Str0mMediaKind::Audio,
        SignalingMediaKind::Video => Str0mMediaKind::Video,
    }
}
