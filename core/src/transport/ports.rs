//! a "port" is a trait to expose e transport concern:
//!
//! SDP negotiation, user teardown, media wiring, observability, or
//! source-policy updates
//!
//! exemple:
//! ```rust,ignore
//! async fn establish_session(
//!     negotiation: &impl NegotiationPort,
//!     session_key: &TransportSessionKey,
//!     answer_sdp: &str,
//!     offered_capabilities: &MediaCapabilities,
//! ) -> Result<MediaCapabilities, TransportAdapterError> {
//!     let _offer = negotiation
//!         .create_initial_session_offer(session_key)
//!         .await?;
//!     let _applied_answer = negotiation.apply_session_answer(session_key, answer_sdp).await?;
//!     negotiation.negotiated_client_rtp_capabilities(
//!         answer_sdp,
//!         offered_capabilities,
//!     )
//! }
//! ```
//!
//! The caller above knows it is performing negotiation, but it does not know
//! whether the backend is backed by str0m, a fake adapter, or a future
//! transport implementation

use std::{collections::BTreeSet, time::Instant};

use o_sfu_router::{MediaCapabilities, MediaKind, MediaStream as RouterRtpParameters};

use crate::{
    RoomInstanceId,
    transport::{
        ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, AppliedSessionAnswer,
        ConsumerPacketGateUpdate, ReceiverBandwidthSnapshot, SessionOffer, SourcePacketGate,
        SourcePolicyUpdateSubscription, TransportAdapterError, TransportBitrateSnapshot,
        TransportMediaId, TransportSessionHealth, TransportSessionKey,
    },
};

/// Full transport backend contract required by the media-core facade.
///
/// `SfuCore` needs negotiation, media mutation, and read-only transport
/// observability against the same backend. Code that only needs one concern
/// should keep depending on the narrower port trait instead.
pub trait TransportFacade:
    Clone + MediaPort + NegotiationPort + ObservabilityPort + Send + Sync
{
}

impl<T> TransportFacade for T where
    T: Clone + MediaPort + NegotiationPort + ObservabilityPort + Send + Sync
{
}

/// Handles the SDP negotiation lifecycle for a transport user
#[allow(
    async_fn_in_trait,
    reason = "transport ports are intentionally static-dispatch crate-boundary contracts and are not exposed as dyn trait objects"
)]
pub trait NegotiationPort {
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
    ) -> Result<AppliedSessionAnswer, TransportAdapterError>;

    fn negotiated_client_rtp_capabilities(
        &self,
        answer_sdp: &str,
        offered_router_capabilities: &MediaCapabilities,
    ) -> Result<MediaCapabilities, TransportAdapterError>;
}

#[allow(
    async_fn_in_trait,
    reason = "transport ports are intentionally static-dispatch crate-boundary contracts and are not exposed as dyn trait objects"
)]
pub trait SessionPort {
    async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError>;
}

/// Handles transport media state for established transport user
#[allow(
    async_fn_in_trait,
    reason = "transport ports are intentionally static-dispatch crate-boundary contracts and are not exposed as dyn trait objects"
)]
pub trait MediaPort {
    async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError>;

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

    async fn set_consumer_packet_gate(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: SourcePacketGate,
    ) -> Result<(), TransportAdapterError>;

    async fn set_consumer_packet_gates(
        &self,
        updates: &[ConsumerPacketGateUpdate],
    ) -> Vec<Result<(), TransportAdapterError>> {
        let mut results = Vec::with_capacity(updates.len());
        for update in updates {
            results.push(
                self.set_consumer_packet_gate(
                    update.consumer_session_key(),
                    update.consumer_transport_media_id(),
                    update.source_session_key(),
                    update.source_transport_media_id(),
                    update.packet_gate().clone(),
                )
                .await,
            );
        }
        results
    }

    async fn request_consumer_keyframe(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError>;

    async fn transport_media_mid(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Option<String>;
}

/// Exposes read-only transport snapshots for diagnostics and runtime decisions
#[allow(
    async_fn_in_trait,
    reason = "transport ports are intentionally static-dispatch crate-boundary contracts and are not exposed as dyn trait objects"
)]
pub trait ObservabilityPort {
    fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot;

    fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot;

    async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource>;

    async fn active_speaker_diagnostic_snapshot(&self) -> Vec<ActiveSpeakerSourceDiagnostic>;

    async fn next_active_speaker_deadline(&self) -> Option<Instant>;

    async fn expired_active_speaker_room_instance_ids(
        &self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId>;

    fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth>;
}

pub trait SourcePolicyPort {
    fn source_policy_subscription(&self) -> SourcePolicyUpdateSubscription;
}
