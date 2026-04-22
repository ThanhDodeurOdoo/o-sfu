use std::collections::BTreeSet;
use std::time::Instant;

use crate::runtime::ChannelRuntimeId;
use crate::runtime::rtc_adapter::TransportSessionHealth;
use crate::runtime::transport_adapter::SourcePolicyUpdateSubscription;
use crate::runtime::transport_adapter::types::{
    ActiveSpeakerSource, SessionOffer, SourcePacketGate, TransportAdapterError,
    TransportBitrateSnapshot, TransportMediaId, TransportSessionKey,
};
use o_sfu_router::{MediaCapabilities, MediaKind, MediaStream as RouterRtpParameters};

pub(crate) trait NegotiationPort {
    async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError>;

    async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError>;

    async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<(), TransportAdapterError>;

    fn negotiated_client_rtp_capabilities(
        &self,
        answer_sdp: &str,
        offered_router_capabilities: &MediaCapabilities,
    ) -> Result<MediaCapabilities, TransportAdapterError>;
}

pub(crate) trait SessionPort {
    async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError>;
}

pub(crate) trait MediaPort {
    async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError>;

    async fn negotiated_producer_parameters(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError>;

    async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError>;

    async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_media_id: TransportMediaId,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError>;

    async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError>;

    async fn set_consumer_active(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError>;

    async fn transport_media_mid(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Option<String>;

    async fn set_source_packet_gate(
        &self,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: Option<SourcePacketGate>,
    ) -> Result<(), TransportAdapterError>;
}

pub(crate) trait ObservabilityPort {
    fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot;

    async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource>;

    async fn next_active_speaker_deadline(&self) -> Option<Instant>;

    async fn expired_active_speaker_channel_runtime_ids(
        &self,
        now: Instant,
    ) -> BTreeSet<ChannelRuntimeId>;

    fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth>;
}

pub(crate) trait SourcePolicyPort {
    fn source_policy_subscription(&self) -> SourcePolicyUpdateSubscription;
}
