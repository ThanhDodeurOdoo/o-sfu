//! Runtime media transport boundary used by core orchestration.
//!
//! This module contains the service surface that room, server plus [`crate::SfuCore`]
//! code use when they need media work to happen outside the pure router. It is
//! intentionally named after the capability it provides rather than the backend
//! that currently implements it: callers ask the media transport to create SDP
//! offers, apply answers, publish or consume media, close sessions, read
//! diagnostics snapshots plus subscribe to source-policy wakeups.
//!
//! The boundary has three responsibilities:
//!
//! - expose the opaque [`MediaTransport`] handle plus the narrow construction
//!   inputs needed by the server runtime
//! - implement the transport port traits from [`crate::transport`] so higher
//!   layers express intent without knowing about RTC workers, Str0m state, UDP
//!   sockets, worker-local relay routing or deterministic test backends
//! - select the active backend for the build. Production builds wrap the real
//!   RTC engine through [`RtcTransport`], while test builds can also select a
//!   deterministic fake transport through the same transport handle without
//!   putting fake-only behavior on the production path.
//!
//! Code above this module should depend on [`MediaTransport`] or one of the
//! concern-oriented port traits. Code below this module, especially the RTC
//! engine, may deal with worker-local state machines and packet-loop details.
//! Keeping that split explicit prevents room and signaling orchestration from
//! growing knowledge of the concrete WebRTC implementation.

mod config;
mod rtc_transport;
#[cfg(any(test, feature = "testing-transport"))]
pub mod test_support;
mod worker_set;

#[cfg(any(test, feature = "testing-transport"))]
use std::sync::Arc;
use std::{collections::BTreeSet, time::Instant};

pub use config::{MediaTransportDeps, RtcTransportConfig};
use o_sfu_router::{MediaCapabilities, MediaKind, MediaStream as RouterRtpParameters};
pub use rtc_transport::{RtcTransport, RtcTransportBuildError, RtcTransportBuilder};
#[cfg(any(test, feature = "testing-transport"))]
use test_support::FakeMediaTransport;
use tracing::warn;
use worker_set::RtcTransportWorkerSet;

pub use crate::transport::{
    ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSource,
    ActiveSpeakerSourceDiagnostic, AppliedProducer, AppliedSessionAnswer, ConsumerActivity,
    ConsumerPacketGateUpdate, MediaPort, ObservabilityPort, ProducerActivity,
    ReceiverBandwidthSnapshot, SessionOffer, SessionPort, SessionUploadEncoding, SessionUploadSlot,
    SourcePacketGate, SourcePacketOperatingPoint, SourcePolicySignal, TransportAdapterError,
    TransportBitrateSnapshot, TransportMediaId, TransportPlacementPressureSnapshot,
    TransportResult, TransportSessionKey, TransportWorkerPressureSnapshot,
};
use crate::{
    CoreOptions,
    runtime::RoomInstanceId,
    transport::{
        NegotiationPort, SourcePolicyPort, SourcePolicyUpdateSubscription,
        TransportRelayRouteEffect, TransportSessionHealth,
    },
};

/// Opaque runtime media transport handle.
///
/// `MediaTransport` is the type server orchestration and `SfuCore` should hold.
/// It hides whether the active backend is production RTC or a deterministic
/// test transport selected by cfg. Callers express intent through the transport
/// port traits and must not branch on concrete backend variants.
///
/// The handle also centralizes warning logs for failed transport effects. Inner
/// backends return typed errors, while this boundary adds stable diagnostic
/// context such as session keys, media ids and SDP lengths.
#[derive(Debug, Clone)]
pub enum MediaTransport {
    /// Production RTC transport selected by normal server builds.
    #[non_exhaustive]
    Rtc(RtcTransport),
    /// Deterministic transport selected by tests or `testing-transport` builds.
    #[cfg(any(test, feature = "testing-transport"))]
    #[non_exhaustive]
    Fake(Arc<FakeMediaTransport>),
}

impl MediaTransport {
    /// Builds the runtime media transport from neutral core options and process
    /// dependencies.
    ///
    /// This is the production server construction path. It intentionally
    /// returns the opaque media transport handle so the orchestration layer does
    /// not import RTC-specific construction types.
    ///
    /// # Errors
    ///
    /// Returns [`RtcTransportBuildError`] when the derived RTC transport cannot
    /// be built from the supplied options and dependencies.
    pub fn from_core_options(
        options: &CoreOptions,
        deps: MediaTransportDeps,
    ) -> Result<Self, RtcTransportBuildError> {
        RtcTransport::builder()
            .core_options(options)
            .deps(deps)
            .build()
            .map(Self::from_rtc_transport)
    }

    /// Wraps a production RTC implementation in the opaque media transport
    /// handle.
    ///
    /// This is useful for tests that need real RTC behavior while still passing
    /// the same type that production orchestration uses.
    #[must_use]
    pub const fn from_rtc_transport(transport: RtcTransport) -> Self {
        Self::Rtc(transport)
    }
}

impl NegotiationPort for MediaTransport {
    async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        let result = match self {
            Self::Rtc(transport) => {
                transport
                    .workers
                    .create_initial_session_offer(session_key)
                    .await
            }
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.create_initial_session_offer(session_key).await,
        };
        if let Err(error) = &result {
            warn!(
                ?session_key,
                ?error,
                "media transport failed to create initial user offer"
            );
        }
        result
    }

    async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        let result = match self {
            Self::Rtc(transport) => {
                transport
                    .workers
                    .create_session_renegotiation_offer(session_key)
                    .await
            }
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => {
                transport
                    .create_session_renegotiation_offer(session_key)
                    .await
            }
        };
        if let Err(error) = &result {
            warn!(
                ?session_key,
                ?error,
                "media transport failed to create renegotiation offer"
            );
        }
        result
    }

    async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, TransportAdapterError> {
        let result = match self {
            Self::Rtc(transport) => {
                transport
                    .workers
                    .apply_session_answer(session_key, answer_sdp)
                    .await
            }
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => {
                transport
                    .apply_session_answer(session_key, answer_sdp)
                    .await
            }
        };
        if let Err(error) = &result {
            warn!(
                ?session_key,
                answer_len = answer_sdp.len(),
                ?error,
                "media transport failed to apply user answer"
            );
        }
        result
    }

    fn negotiated_client_rtp_capabilities(
        &self,
        answer_sdp: &str,
        offered_router_capabilities: &MediaCapabilities,
    ) -> Result<MediaCapabilities, TransportAdapterError> {
        let result = match self {
            Self::Rtc(_) => RtcTransportWorkerSet::negotiated_client_rtp_capabilities(
                answer_sdp,
                offered_router_capabilities,
            ),
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(_) => FakeMediaTransport::project_answered_client_rtp_capabilities(
                answer_sdp,
                offered_router_capabilities,
            ),
        };
        if let Err(error) = &result {
            warn!(
                answer_len = answer_sdp.len(),
                ?error,
                "media transport failed to derive client RTP capabilities from answer SDP"
            );
        }
        result
    }
}

impl SessionPort for MediaTransport {
    async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        let result = match self {
            Self::Rtc(transport) => transport.workers.close_session(session_key).await,
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.close_session(session_key).await,
        };
        if let Err(error) = &result {
            warn!(?session_key, ?error, "media transport failed to close user");
        }
        result
    }
}

impl MediaPort for MediaTransport {
    async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        let result = match self {
            Self::Rtc(transport) => {
                transport
                    .workers
                    .remove_media(session_key, transport_media_id)
                    .await
            }
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => {
                transport
                    .remove_media(session_key, transport_media_id)
                    .await
            }
        };
        if let Err(error) = &result {
            warn!(
                ?session_key,
                ?transport_media_id,
                ?error,
                "media transport failed to remove media"
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
        let result = match self {
            Self::Rtc(transport) => {
                transport
                    .workers
                    .publish_media(session_key, media_kind, rtp_parameters)
                    .await
            }
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => {
                transport
                    .publish_media(session_key, media_kind, rtp_parameters)
                    .await
            }
        };
        if let Err(error) = &result {
            warn!(
                ?session_key,
                ?media_kind,
                mid = rtp_parameters.mid(),
                ?error,
                "media transport failed to declare producer media"
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
        let result = match self {
            Self::Rtc(transport) => {
                transport
                    .workers
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
        };
        if let Err(error) = &result {
            warn!(
                ?consumer_session_key,
                ?source_session_key,
                ?source_media_id,
                ?media_kind,
                mid = consumer_rtp_parameters.mid(),
                ?error,
                "media transport failed to declare consumer media"
            );
        }
        result
    }

    async fn apply_relay_route_effect(
        &self,
        effect: &TransportRelayRouteEffect,
    ) -> Result<(), TransportAdapterError> {
        let result = match self {
            Self::Rtc(transport) => transport.workers.apply_relay_route_effect(effect).await,
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.apply_relay_route_effect(effect).await,
        };
        if let Err(error) = &result {
            warn!(
                source_session_key = ?effect.source_session_key,
                source_transport_media_id = ?effect.source_transport_media_id,
                target_media_worker_id = effect.target_media_worker_id,
                action = ?effect.action,
                ?error,
                "media transport failed to apply relay route effect"
            );
        }
        result
    }

    async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        activity: ProducerActivity,
    ) -> Result<(), TransportAdapterError> {
        let result = match self {
            Self::Rtc(transport) => {
                transport
                    .workers
                    .set_producer_active(session_key, transport_media_id, activity.is_active())
                    .await
            }
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => {
                transport
                    .set_producer_active(session_key, transport_media_id, activity.is_active())
                    .await
            }
        };
        if let Err(error) = &result {
            warn!(
                ?session_key,
                ?transport_media_id,
                active = activity.is_active(),
                ?error,
                "media transport failed to update producer activity"
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
        activity: ConsumerActivity,
    ) -> Result<(), TransportAdapterError> {
        let result = match self {
            Self::Rtc(transport) => {
                transport
                    .workers
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
        };
        if let Err(error) = &result {
            warn!(
                ?consumer_session_key,
                ?consumer_transport_media_id,
                ?source_session_key,
                ?source_transport_media_id,
                active = activity.is_active(),
                ?error,
                "media transport failed to update consumer activity"
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
        let result = match self {
            Self::Rtc(transport) => {
                transport
                    .workers
                    .set_consumer_packet_gate(
                        consumer_session_key,
                        consumer_transport_media_id,
                        source_session_key,
                        source_transport_media_id,
                        packet_gate.clone(),
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
                        packet_gate.clone(),
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
                ?packet_gate,
                ?error,
                "media transport failed to update consumer packet gate"
            );
        }
        result
    }

    async fn set_consumer_packet_gates(
        &self,
        updates: &[ConsumerPacketGateUpdate],
    ) -> Vec<Result<(), TransportAdapterError>> {
        let results = match self {
            Self::Rtc(transport) => transport.workers.set_consumer_packet_gates(updates).await,
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.set_consumer_packet_gates(updates).await,
        };
        for (update, result) in updates.iter().zip(results.iter()) {
            if let Err(error) = result {
                warn!(
                    ?error,
                    consumer_session_key = ?update.consumer_session_key(),
                    consumer_transport_media_id = ?update.consumer_transport_media_id(),
                    source_session_key = ?update.source_session_key(),
                    source_transport_media_id = ?update.source_transport_media_id(),
                    packet_gate = ?update.packet_gate(),
                    "media transport failed to update a batched consumer packet gate"
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
        let result = match self {
            Self::Rtc(transport) => {
                transport
                    .workers
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
        };
        if let Err(error) = &result {
            warn!(
                ?consumer_session_key,
                ?consumer_transport_media_id,
                ?source_session_key,
                ?source_transport_media_id,
                ?error,
                "media transport failed to request a consumer keyframe refresh"
            );
        }
        result
    }

    async fn transport_media_mid(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Option<String> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .workers
                    .transport_media_mid(session_key, transport_media_id)
                    .await
            }
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(_) => None,
        }
    }
}

impl ObservabilityPort for MediaTransport {
    fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        match self {
            Self::Rtc(transport) => transport.workers.transport_bitrate_snapshot(session_keys),
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(_) => TransportBitrateSnapshot::default(),
        }
    }

    fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        match self {
            Self::Rtc(transport) => transport.workers.receiver_bandwidth_snapshot(session_keys),
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.receiver_bandwidth_snapshot(session_keys),
        }
    }

    fn placement_pressure_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportPlacementPressureSnapshot {
        match self {
            Self::Rtc(transport) => transport.workers.placement_pressure_snapshot(session_keys),
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.placement_pressure_snapshot(session_keys),
        }
    }

    fn worker_pressure_snapshots(&self) -> Vec<TransportWorkerPressureSnapshot> {
        match self {
            Self::Rtc(transport) => transport.workers.worker_pressure_snapshots(),
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.worker_pressure_snapshots(),
        }
    }

    async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        match self {
            Self::Rtc(transport) => transport.workers.active_speaker_source_snapshot().await,
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.active_speaker_source_snapshot().await,
        }
    }

    async fn active_speaker_diagnostic_snapshot(&self) -> Vec<ActiveSpeakerSourceDiagnostic> {
        match self {
            Self::Rtc(transport) => transport.workers.active_speaker_diagnostic_snapshot().await,
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.active_speaker_diagnostic_snapshot().await,
        }
    }

    async fn next_active_speaker_deadline(&self) -> Option<Instant> {
        match self {
            Self::Rtc(transport) => transport.workers.next_active_speaker_deadline().await,
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(_) => None,
        }
    }

    async fn expired_active_speaker_room_instance_ids(
        &self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId> {
        match self {
            Self::Rtc(transport) => {
                transport
                    .workers
                    .expired_active_speaker_room_instance_ids(now)
                    .await
            }
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(_) => BTreeSet::new(),
        }
    }

    fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        match self {
            Self::Rtc(transport) => transport.workers.session_transport_health(session_key),
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(_) => None,
        }
    }
}

impl SourcePolicyPort for MediaTransport {
    fn source_policy_subscription(&self) -> SourcePolicyUpdateSubscription {
        match self {
            Self::Rtc(transport) => transport.workers.source_policy_subscription(),
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fake(transport) => transport.source_policy_signal().subscribe(),
        }
    }
}

#[cfg(test)]
mod tests;
