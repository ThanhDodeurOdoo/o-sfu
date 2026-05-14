use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
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
    Bitrate,
    runtime::{
        UserId,
        media_transport::{
            ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSource,
            ActiveSpeakerSourceDiagnostic, AppliedSessionAnswer, ConsumerPacketGateUpdate,
            ReceiverBandwidthSnapshot, SessionOffer, SourcePacketGate, TransportAdapterError,
            TransportMediaId, TransportPlacementPressureSnapshot, TransportSessionKey,
        },
    },
    transport::{SourcePolicySignal, TransportRelayRouteAction, TransportRelayRouteEffect},
};

const FAKE_SESSION_NEGOTIATION_OFFER_SDP: &str = "v=0\r\ns=o-sfu-fake-offer\r\n";

/// Deterministic fake transport events exposed only through the explicit
/// media-transport test seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FakeMediaTransportEvent {
    SessionClosed {
        user_id: UserId,
        media_worker_id: usize,
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
    RelayRouteEffectApplied {
        source_user_id: UserId,
        source_transport_media_id: TransportMediaId,
        target_media_worker_id: usize,
        action: TransportRelayRouteAction,
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
    state: Arc<Mutex<FakeMediaTransportState>>,
    source_policy_signal: Arc<SourcePolicySignal>,
}

#[derive(Debug, Default)]
struct FakeMediaTransportState {
    events: Vec<FakeMediaTransportEvent>,
    next_media_id: u64,
    media_owners: BTreeMap<TransportMediaId, TransportSessionKey>,
    failed_media_removals: BTreeSet<TransportMediaId>,
    failed_relay_route_releases: BTreeSet<FakeRelayRouteKey>,
    blocked_media_removals: BTreeSet<TransportMediaId>,
    failed_session_closes: BTreeSet<TransportSessionKey>,
    blocked_session_closes: BTreeSet<TransportSessionKey>,
    negotiated_producer_parameters: BTreeMap<TransportMediaId, RouterRtpParameters>,
    active_speaker_sources: Vec<ActiveSpeakerSource>,
    receiver_bandwidth_estimates: BTreeMap<UserId, Bitrate>,
    placement_pressure: TransportPlacementPressureSnapshot,
    delays: FakeMediaTransportDelays,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FakeRelayRouteKey {
    source_user: UserId,
    source_media: TransportMediaId,
    target_worker: usize,
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
    fn inspect_state<T>(&self, inspect: impl FnOnce(&FakeMediaTransportState) -> T) -> T {
        let state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        inspect(&state)
    }

    fn mutate_state<T>(&self, mutate: impl FnOnce(&mut FakeMediaTransportState) -> T) -> T {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        mutate(&mut state)
    }

    fn record_event(&self, event: FakeMediaTransportEvent) {
        self.mutate_state(|state| state.events.push(event));
    }

    fn delay_for_publish_media(&self) -> Option<Duration> {
        self.inspect_state(|state| state.delays.publish_media)
    }

    fn delay_for_consume_media(&self) -> Option<Duration> {
        self.inspect_state(|state| state.delays.consume_media)
    }

    fn delay_for_producer_activity(&self) -> Option<Duration> {
        self.inspect_state(|state| state.delays.producer_activity)
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub fn snapshot_events(&self) -> Vec<FakeMediaTransportEvent> {
        self.inspect_state(|state| state.events.clone())
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn set_publish_media_delay(&self, delay: Option<Duration>) {
        self.mutate_state(|state| state.delays.publish_media = delay);
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn fail_next_remove_media(&self, transport_media_id: TransportMediaId) {
        self.mutate_state(|state| {
            state.failed_media_removals.insert(transport_media_id);
        });
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub fn next_transport_media_id(&self) -> TransportMediaId {
        self.inspect_state(|state| TransportMediaId::new(state.next_media_id))
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn fail_next_relay_release(
        &self,
        source_user_id: UserId,
        source_transport_media_id: TransportMediaId,
        target_media_worker_id: usize,
    ) {
        self.mutate_state(|state| {
            state.failed_relay_route_releases.insert(FakeRelayRouteKey {
                source_user: source_user_id,
                source_media: source_transport_media_id,
                target_worker: target_media_worker_id,
            });
        });
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn fail_remove_media_until_allowed(&self, transport_media_id: TransportMediaId) {
        self.mutate_state(|state| {
            state.blocked_media_removals.insert(transport_media_id);
        });
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn allow_remove_media(&self, transport_media_id: TransportMediaId) {
        self.mutate_state(|state| {
            state.blocked_media_removals.remove(&transport_media_id);
        });
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn fail_next_close_session(&self, session_key: TransportSessionKey) {
        self.mutate_state(|state| {
            state.failed_session_closes.insert(session_key);
        });
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn fail_close_session_until_allowed(&self, session_key: TransportSessionKey) {
        self.mutate_state(|state| {
            state.blocked_session_closes.insert(session_key);
        });
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn allow_close_session(&self, session_key: &TransportSessionKey) {
        self.mutate_state(|state| {
            state.blocked_session_closes.remove(session_key);
        });
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn set_consume_media_delay(&self, delay: Option<Duration>) {
        self.mutate_state(|state| state.delays.consume_media = delay);
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn set_producer_active_delay(&self, delay: Option<Duration>) {
        self.mutate_state(|state| state.delays.producer_activity = delay);
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn clear_negotiated_producer_parameters(&self, transport_media_id: TransportMediaId) {
        self.mutate_state(|state| {
            state
                .negotiated_producer_parameters
                .remove(&transport_media_id);
        });
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn set_negotiated_producer_parameters(
        &self,
        transport_media_id: TransportMediaId,
        parameters: RouterRtpParameters,
    ) {
        self.mutate_state(|state| {
            state
                .negotiated_producer_parameters
                .insert(transport_media_id, parameters);
        });
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn set_active_speaker_source_snapshot(&self, sources: Vec<ActiveSpeakerSource>) {
        let dirty_room_instance_ids = self.mutate_state(|state| {
            let dirty_room_instance_ids = sources
                .iter()
                .filter_map(|source| {
                    state
                        .media_owners
                        .get(&source.transport_media_id())
                        .map(TransportSessionKey::room_instance_id)
                })
                .collect::<Vec<_>>();
            state.active_speaker_sources = sources;
            dirty_room_instance_ids
        });
        for room_instance_id in dirty_room_instance_ids {
            self.source_policy_signal.mark_dirty(room_instance_id);
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn set_receiver_bandwidth_estimate(&self, user_id: UserId, estimate: Bitrate) {
        let dirty_room_instance_ids = self.mutate_state(|state| {
            let dirty_room_instance_ids = state
                .media_owners
                .values()
                .filter_map(|session_key| {
                    (session_key.user_id() == &user_id).then_some(session_key.room_instance_id())
                })
                .collect::<Vec<_>>();
            state.receiver_bandwidth_estimates.insert(user_id, estimate);
            dirty_room_instance_ids
        });
        for room_instance_id in dirty_room_instance_ids {
            self.source_policy_signal.mark_dirty(room_instance_id);
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn set_placement_pressure_snapshot(&self, snapshot: TransportPlacementPressureSnapshot) {
        self.mutate_state(|state| state.placement_pressure = snapshot);
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
        self.inspect_state(|state| state.active_speaker_sources.clone())
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
        self.inspect_state(|state| ReceiverBandwidthSnapshot {
            per_session: session_keys
                .iter()
                .filter_map(|session_key| {
                    state
                        .receiver_bandwidth_estimates
                        .get(session_key.user_id())
                        .copied()
                        .map(|estimate| (session_key.clone(), estimate))
                })
                .collect(),
        })
    }

    pub(crate) fn placement_pressure_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportPlacementPressureSnapshot {
        if session_keys.is_empty() {
            return TransportPlacementPressureSnapshot::default();
        }
        self.inspect_state(|state| state.placement_pressure)
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
        let negotiated_producer_parameters =
            self.inspect_state(|state| state.negotiated_producer_parameters.clone());
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
        let should_fail = self.mutate_state(|state| {
            let should_fail_once = state.failed_session_closes.remove(session_key);
            let should_fail_until_allowed = state.blocked_session_closes.contains(session_key);
            if should_fail_once || should_fail_until_allowed {
                return true;
            }
            state.media_owners.retain(|_, owner| owner != session_key);
            state.events.push(FakeMediaTransportEvent::SessionClosed {
                user_id: session_key.user_id().clone(),
                media_worker_id: session_key.media_worker_id(),
            });
            false
        });
        if should_fail {
            return Err(TransportAdapterError::TransportUnavailable);
        }
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
        let should_fail = self.mutate_state(|state| {
            let should_fail_once = state.failed_media_removals.remove(&transport_media_id);
            let should_fail_until_allowed =
                state.blocked_media_removals.contains(&transport_media_id);
            if should_fail_once || should_fail_until_allowed {
                return true;
            }
            state
                .negotiated_producer_parameters
                .remove(&transport_media_id);
            state.media_owners.remove(&transport_media_id);
            state.events.push(FakeMediaTransportEvent::MediaRemoved {
                user_id: session_key.user_id().clone(),
                transport_media_id,
            });
            false
        });
        if should_fail {
            return Err(TransportAdapterError::TransportUnavailable);
        }
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
        self.inspect_state(|state| {
            state
                .negotiated_producer_parameters
                .get(&transport_media_id)
                .cloned()
                .ok_or(TransportAdapterError::UnsupportedFeature)
        })
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
        let transport_media_id = self.mutate_state(|state| {
            let transport_media_id = TransportMediaId::new(state.next_media_id);
            state.next_media_id = state.next_media_id.wrapping_add(1);
            let negotiated =
                synthetic_negotiated_producer_parameters(media_kind, transport_media_id);
            state
                .negotiated_producer_parameters
                .insert(transport_media_id, negotiated);
            state
                .media_owners
                .insert(transport_media_id, session_key.clone());
            transport_media_id
        });
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
        Ok(self.mutate_state(|state| {
            let transport_media_id = TransportMediaId::new(state.next_media_id);
            state.next_media_id = state.next_media_id.wrapping_add(1);
            transport_media_id
        }))
    }

    #[allow(
        clippy::unused_async,
        reason = "fake transport keeps the same async boundary as the RTC shard and runtime call sites"
    )]
    pub(crate) async fn apply_relay_route_effect(
        &self,
        effect: &TransportRelayRouteEffect,
    ) -> Result<(), TransportAdapterError> {
        if effect.action == TransportRelayRouteAction::Release {
            let release_key = FakeRelayRouteKey {
                source_user: effect.source_session_key.user_id().clone(),
                source_media: effect.source_transport_media_id,
                target_worker: effect.target_media_worker_id,
            };
            let should_fail_once =
                self.mutate_state(|state| state.failed_relay_route_releases.remove(&release_key));
            if should_fail_once {
                return Err(TransportAdapterError::TransportUnavailable);
            }
        }
        self.record_event(FakeMediaTransportEvent::RelayRouteEffectApplied {
            source_user_id: effect.source_session_key.user_id().clone(),
            source_transport_media_id: effect.source_transport_media_id,
            target_media_worker_id: effect.target_media_worker_id,
            action: effect.action,
        });
        Ok(())
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
