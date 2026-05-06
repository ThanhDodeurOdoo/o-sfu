use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use o_sfu_router::{
    MediaFormat as RouterMediaFormat, MediaKind, MediaKind as RouterMediaKind,
    MediaStream as RouterRtpParameters, RtcpFeedback, RtcpFeedbackKind, StreamBinding,
};
use tokio::time::sleep;

#[cfg(any(test, feature = "testing-transport"))]
use crate::runtime::RoomInstanceId;
use crate::{
    runtime::{
        UserId,
        media_transport::{
            ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSource,
            ActiveSpeakerSourceDiagnostic, AppliedSessionAnswer, ConsumerPacketGateUpdate,
            ReceiverBandwidthSnapshot, SessionOffer, SourcePacketGate, TransportAdapterError,
            TransportMediaId, TransportPlacementPressureSnapshot, TransportSessionKey,
        },
    },
    transport::SourcePolicySignal,
};

const FAKE_SESSION_NEGOTIATION_OFFER_SDP: &str = "v=0\r\ns=o-sfu-fake-offer\r\n";

/// Deterministic fake transport events exposed only through the explicit
/// media-transport test seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FakeMediaTransportEvent {
    SessionClosed {
        user_id: UserId,
    },
    PublishMediaRequested {
        user_id: UserId,
        media_kind: MediaKind,
    },
    ConsumeMediaRequested {
        consumer_user_id: UserId,
        source_user_id: UserId,
        media_kind: MediaKind,
    },
    MediaRemoved {
        user_id: UserId,
        transport_media_id: TransportMediaId,
    },
    ProducerActivityUpdated {
        user_id: UserId,
        active: bool,
    },
    ConsumerActivityUpdated {
        consumer_user_id: UserId,
        source_user_id: UserId,
        active: bool,
    },
    ConsumerPacketGateUpdated {
        consumer_user_id: UserId,
        source_user_id: UserId,
        packet_gate: SourcePacketGate,
    },
    ConsumerKeyframeRequested {
        consumer_user_id: UserId,
        source_user_id: UserId,
    },
}

/// Deterministic transport backend for tests and feature-gated development.
#[derive(Debug, Clone, Default)]
pub struct FakeMediaTransport {
    events: Arc<Mutex<Vec<FakeMediaTransportEvent>>>,
    next_media_id: Arc<AtomicU64>,
    media_owners: Arc<Mutex<BTreeMap<TransportMediaId, TransportSessionKey>>>,
    failed_media_removals: Arc<Mutex<BTreeSet<TransportMediaId>>>,
    blocked_media_removals: Arc<Mutex<BTreeSet<TransportMediaId>>>,
    failed_session_closes: Arc<Mutex<BTreeSet<TransportSessionKey>>>,
    blocked_session_closes: Arc<Mutex<BTreeSet<TransportSessionKey>>>,
    negotiated_producer_parameters: Arc<Mutex<BTreeMap<TransportMediaId, RouterRtpParameters>>>,
    active_speaker_sources: Arc<Mutex<Vec<ActiveSpeakerSource>>>,
    receiver_bandwidth_estimates: Arc<Mutex<BTreeMap<UserId, u64>>>,
    placement_pressure: Arc<Mutex<TransportPlacementPressureSnapshot>>,
    delays: Arc<Mutex<FakeMediaTransportDelays>>,
    source_policy_signal: Arc<SourcePolicySignal>,
}

#[derive(Debug, Clone, Copy, Default)]
struct FakeMediaTransportDelays {
    publish_media: Option<Duration>,
    consume_media: Option<Duration>,
    producer_activity: Option<Duration>,
}

#[allow(
    clippy::unused_async,
    reason = "fake transport keeps the same async boundary as the RTC shard and runtime call sites"
)]
impl FakeMediaTransport {
    fn record_event(&self, event: FakeMediaTransportEvent) {
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

    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub fn snapshot_events(&self) -> Vec<FakeMediaTransportEvent> {
        match self.events.lock() {
            Ok(events) => events.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn set_publish_media_delay(&self, delay: Option<Duration>) {
        match self.delays.lock() {
            Ok(mut delays) => {
                delays.publish_media = delay;
            }
            Err(poisoned) => {
                poisoned.into_inner().publish_media = delay;
            }
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn fail_next_remove_media(&self, transport_media_id: TransportMediaId) {
        match self.failed_media_removals.lock() {
            Ok(mut removals) => {
                removals.insert(transport_media_id);
            }
            Err(poisoned) => {
                poisoned.into_inner().insert(transport_media_id);
            }
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn fail_remove_media_until_allowed(&self, transport_media_id: TransportMediaId) {
        match self.blocked_media_removals.lock() {
            Ok(mut removals) => {
                removals.insert(transport_media_id);
            }
            Err(poisoned) => {
                poisoned.into_inner().insert(transport_media_id);
            }
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn allow_remove_media(&self, transport_media_id: TransportMediaId) {
        match self.blocked_media_removals.lock() {
            Ok(mut removals) => {
                removals.remove(&transport_media_id);
            }
            Err(poisoned) => {
                poisoned.into_inner().remove(&transport_media_id);
            }
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn fail_next_close_session(&self, session_key: TransportSessionKey) {
        match self.failed_session_closes.lock() {
            Ok(mut sessions) => {
                sessions.insert(session_key);
            }
            Err(poisoned) => {
                poisoned.into_inner().insert(session_key);
            }
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn fail_close_session_until_allowed(&self, session_key: TransportSessionKey) {
        match self.blocked_session_closes.lock() {
            Ok(mut sessions) => {
                sessions.insert(session_key);
            }
            Err(poisoned) => {
                poisoned.into_inner().insert(session_key);
            }
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn allow_close_session(&self, session_key: &TransportSessionKey) {
        match self.blocked_session_closes.lock() {
            Ok(mut sessions) => {
                sessions.remove(session_key);
            }
            Err(poisoned) => {
                poisoned.into_inner().remove(session_key);
            }
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn set_consume_media_delay(&self, delay: Option<Duration>) {
        match self.delays.lock() {
            Ok(mut delays) => {
                delays.consume_media = delay;
            }
            Err(poisoned) => {
                poisoned.into_inner().consume_media = delay;
            }
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn set_producer_active_delay(&self, delay: Option<Duration>) {
        match self.delays.lock() {
            Ok(mut delays) => {
                delays.producer_activity = delay;
            }
            Err(poisoned) => {
                poisoned.into_inner().producer_activity = delay;
            }
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn clear_negotiated_producer_parameters(&self, transport_media_id: TransportMediaId) {
        match self.negotiated_producer_parameters.lock() {
            Ok(mut parameters) => {
                parameters.remove(&transport_media_id);
            }
            Err(poisoned) => {
                poisoned.into_inner().remove(&transport_media_id);
            }
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn set_negotiated_producer_parameters(
        &self,
        transport_media_id: TransportMediaId,
        parameters: RouterRtpParameters,
    ) {
        match self.negotiated_producer_parameters.lock() {
            Ok(mut negotiated_parameters) => {
                negotiated_parameters.insert(transport_media_id, parameters);
            }
            Err(poisoned) => {
                poisoned.into_inner().insert(transport_media_id, parameters);
            }
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn set_active_speaker_source_snapshot(&self, sources: Vec<ActiveSpeakerSource>) {
        let dirty_room_instance_ids = match self.media_owners.lock() {
            Ok(media_owners) => sources
                .iter()
                .filter_map(|source| {
                    media_owners
                        .get(&source.transport_media_id())
                        .map(TransportSessionKey::room_instance_id)
                })
                .collect::<Vec<_>>(),
            Err(poisoned) => poisoned
                .into_inner()
                .iter()
                .filter_map(|(transport_media_id, session_key)| {
                    sources
                        .iter()
                        .any(|source| source.transport_media_id() == *transport_media_id)
                        .then_some(session_key.room_instance_id())
                })
                .collect::<Vec<_>>(),
        };
        match self.active_speaker_sources.lock() {
            Ok(mut active_speaker_sources) => {
                *active_speaker_sources = sources;
            }
            Err(poisoned) => {
                *poisoned.into_inner() = sources;
            }
        }
        for room_instance_id in dirty_room_instance_ids {
            self.source_policy_signal.mark_dirty(room_instance_id);
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn set_receiver_bandwidth_estimate(&self, user_id: UserId, estimate_bps: u64) {
        let dirty_room_instance_ids = match self.media_owners.lock() {
            Ok(media_owners) => media_owners
                .values()
                .filter_map(|session_key| {
                    (session_key.user_id() == &user_id).then_some(session_key.room_instance_id())
                })
                .collect::<Vec<_>>(),
            Err(poisoned) => poisoned
                .into_inner()
                .values()
                .filter_map(|session_key| {
                    (session_key.user_id() == &user_id).then_some(session_key.room_instance_id())
                })
                .collect::<Vec<_>>(),
        };
        match self.receiver_bandwidth_estimates.lock() {
            Ok(mut estimates) => {
                estimates.insert(user_id, estimate_bps);
            }
            Err(poisoned) => {
                poisoned.into_inner().insert(user_id, estimate_bps);
            }
        }
        for room_instance_id in dirty_room_instance_ids {
            self.source_policy_signal.mark_dirty(room_instance_id);
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn set_placement_pressure_snapshot(&self, snapshot: TransportPlacementPressureSnapshot) {
        match self.placement_pressure.lock() {
            Ok(mut pressure) => {
                *pressure = snapshot;
            }
            Err(poisoned) => {
                *poisoned.into_inner() = snapshot;
            }
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn mark_source_policy_dirty(&self, room_instance_id: RoomInstanceId) {
        self.source_policy_signal.mark_dirty(room_instance_id);
    }
}

impl FakeMediaTransport {
    #[allow(
        clippy::unused_async,
        reason = "fake transport keeps the same async boundary as the RTC shard and runtime call sites"
    )]
    pub(crate) async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        match self.active_speaker_sources.lock() {
            Ok(active_speaker_sources) => active_speaker_sources.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    pub(crate) async fn active_speaker_diagnostic_snapshot(
        &self,
    ) -> Vec<ActiveSpeakerSourceDiagnostic> {
        self.active_speaker_source_snapshot()
            .await
            .into_iter()
            .map(|source| {
                ActiveSpeakerSourceDiagnostic::new(
                    source.transport_media_id(),
                    ActiveSpeakerActivityState::Active,
                    ActiveSpeakerActivityReason::Vad,
                    None,
                    0,
                    None,
                )
            })
            .collect()
    }

    pub(crate) fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        let estimates = match self.receiver_bandwidth_estimates.lock() {
            Ok(estimates) => estimates.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        ReceiverBandwidthSnapshot {
            per_session: session_keys
                .iter()
                .filter_map(|session_key| {
                    estimates
                        .get(session_key.user_id())
                        .copied()
                        .map(|estimate_bps| (session_key.clone(), estimate_bps))
                })
                .collect(),
        }
    }

    pub(crate) fn placement_pressure_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportPlacementPressureSnapshot {
        if session_keys.is_empty() {
            return TransportPlacementPressureSnapshot::default();
        }
        match self.placement_pressure.lock() {
            Ok(pressure) => *pressure,
            Err(poisoned) => *poisoned.into_inner(),
        }
    }

    pub(crate) fn source_policy_signal(&self) -> Arc<SourcePolicySignal> {
        Arc::clone(&self.source_policy_signal)
    }

    pub(crate) fn project_answered_client_rtp_capabilities(
        answer_sdp: &str,
        offered_router_capabilities: &o_sfu_router::MediaCapabilities,
    ) -> Result<o_sfu_router::MediaCapabilities, TransportAdapterError> {
        if !answer_sdp.trim_start().starts_with("v=0") {
            return Err(TransportAdapterError::InvalidInput);
        }
        Ok(offered_router_capabilities.clone())
    }

    #[allow(
        clippy::unused_async,
        reason = "fake transport keeps the same async boundary as the RTC shard and runtime call sites"
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
        reason = "fake transport keeps the same async boundary as the RTC shard and runtime call sites"
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
        reason = "fake transport keeps the same async boundary as the RTC shard and runtime call sites"
    )]
    pub(crate) async fn apply_session_answer(
        &self,
        _session_key: &TransportSessionKey,
        _answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, TransportAdapterError> {
        let negotiated_producer_parameters = match self.negotiated_producer_parameters.lock() {
            Ok(parameters) => parameters.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        Ok(AppliedSessionAnswer::from_negotiated_producers(
            negotiated_producer_parameters,
        ))
    }

    #[allow(
        clippy::unused_async,
        reason = "fake transport keeps the same async boundary as the RTC shard and runtime call sites"
    )]
    pub(crate) async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        let should_fail_once = match self.failed_session_closes.lock() {
            Ok(mut sessions) => sessions.remove(session_key),
            Err(poisoned) => poisoned.into_inner().remove(session_key),
        };
        let should_fail_until_allowed = match self.blocked_session_closes.lock() {
            Ok(sessions) => sessions.contains(session_key),
            Err(poisoned) => poisoned.into_inner().contains(session_key),
        };
        if should_fail_once || should_fail_until_allowed {
            return Err(TransportAdapterError::TransportUnavailable);
        }
        match self.media_owners.lock() {
            Ok(mut media_owners) => {
                media_owners.retain(|_, owner| owner != session_key);
            }
            Err(poisoned) => {
                poisoned
                    .into_inner()
                    .retain(|_, owner| owner != session_key);
            }
        }
        self.record_event(FakeMediaTransportEvent::SessionClosed {
            user_id: session_key.user_id().clone(),
        });
        Ok(())
    }

    #[allow(
        clippy::unused_async,
        reason = "fake transport keeps the same async boundary as the RTC shard and runtime call sites"
    )]
    pub(crate) async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        let should_fail = match self.failed_media_removals.lock() {
            Ok(mut removals) => removals.remove(&transport_media_id),
            Err(poisoned) => poisoned.into_inner().remove(&transport_media_id),
        };
        let should_fail_until_allowed = match self.blocked_media_removals.lock() {
            Ok(removals) => removals.contains(&transport_media_id),
            Err(poisoned) => poisoned.into_inner().contains(&transport_media_id),
        };
        if should_fail || should_fail_until_allowed {
            return Err(TransportAdapterError::TransportUnavailable);
        }
        match self.negotiated_producer_parameters.lock() {
            Ok(mut parameters) => {
                parameters.remove(&transport_media_id);
            }
            Err(poisoned) => {
                poisoned.into_inner().remove(&transport_media_id);
            }
        }
        match self.media_owners.lock() {
            Ok(mut media_owners) => {
                media_owners.remove(&transport_media_id);
            }
            Err(poisoned) => {
                poisoned.into_inner().remove(&transport_media_id);
            }
        }
        self.record_event(FakeMediaTransportEvent::MediaRemoved {
            user_id: session_key.user_id().clone(),
            transport_media_id,
        });
        Ok(())
    }

    #[cfg(test)]
    #[allow(
        clippy::unused_async,
        reason = "fake transport keeps the same async boundary as the RTC shard and runtime call sites"
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
        reason = "fake transport keeps the same async boundary as the RTC shard and runtime call sites"
    )]
    pub(crate) async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        _rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.record_event(FakeMediaTransportEvent::PublishMediaRequested {
            user_id: session_key.user_id().clone(),
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
        match self.media_owners.lock() {
            Ok(mut media_owners) => {
                media_owners.insert(transport_media_id, session_key.clone());
            }
            Err(poisoned) => {
                poisoned
                    .into_inner()
                    .insert(transport_media_id, session_key.clone());
            }
        }
        Ok(transport_media_id)
    }

    #[allow(
        clippy::unused_async,
        reason = "fake transport keeps the same async boundary as the RTC shard and runtime call sites"
    )]
    pub(crate) async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        _source_media_id: TransportMediaId,
        _consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.record_event(FakeMediaTransportEvent::ConsumeMediaRequested {
            consumer_user_id: consumer_session_key.user_id().clone(),
            source_user_id: source_session_key.user_id().clone(),
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
        reason = "fake transport keeps the same async boundary as the RTC shard and runtime call sites"
    )]
    pub(crate) async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        _transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.record_event(FakeMediaTransportEvent::ProducerActivityUpdated {
            user_id: session_key.user_id().clone(),
            active,
        });
        if let Some(delay) = self.delay_for_producer_activity() {
            sleep(delay).await;
        }
        Ok(())
    }

    #[allow(
        clippy::unused_async,
        reason = "fake transport keeps the same async boundary as the RTC shard and runtime call sites"
    )]
    pub(crate) async fn set_consumer_active(
        &self,
        consumer_session_key: &TransportSessionKey,
        _consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        _source_transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.record_event(FakeMediaTransportEvent::ConsumerActivityUpdated {
            consumer_user_id: consumer_session_key.user_id().clone(),
            source_user_id: source_session_key.user_id().clone(),
            active,
        });
        Ok(())
    }

    #[allow(
        clippy::unused_async,
        reason = "fake transport keeps the same async boundary as the RTC shard and runtime call sites"
    )]
    pub(crate) async fn set_consumer_packet_gate(
        &self,
        consumer_session_key: &TransportSessionKey,
        _consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        _source_transport_media_id: TransportMediaId,
        packet_gate: SourcePacketGate,
    ) -> Result<(), TransportAdapterError> {
        self.record_event(FakeMediaTransportEvent::ConsumerPacketGateUpdated {
            consumer_user_id: consumer_session_key.user_id().clone(),
            source_user_id: source_session_key.user_id().clone(),
            packet_gate,
        });
        Ok(())
    }

    pub(crate) async fn set_consumer_packet_gates(
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

    #[allow(
        clippy::unused_async,
        reason = "fake transport keeps the same async boundary as the RTC shard and runtime call sites"
    )]
    pub(crate) async fn request_consumer_keyframe(
        &self,
        consumer_session_key: &TransportSessionKey,
        _consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        _source_transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        self.record_event(FakeMediaTransportEvent::ConsumerKeyframeRequested {
            consumer_user_id: consumer_session_key.user_id().clone(),
            source_user_id: source_session_key.user_id().clone(),
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
