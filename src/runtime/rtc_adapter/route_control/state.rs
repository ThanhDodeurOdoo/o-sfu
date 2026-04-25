//! Worker-local route-control state.
//!
//! `RouteControlState` composes source-local packet gates, relay-target gates,
//! active-speaker packet policy, and keyframe coalescing. It remains
//! transport-facing: callers provide transport media ids and packet metadata,
//! and room policy stays outside this module.

use std::{cmp::Reverse, collections::BTreeMap, time::Instant};

use super::{
    active_speaker::SourceAudioPolicyState,
    keyframe::{KeyframeRequestDecision, KeyframeRequestWindow},
    packet_gate::{
        PacketLayerGate, PacketLayerMetadata, PacketRouteDecision, aggregate_packet_gates,
        intersect_packet_gates,
    },
};
use crate::runtime::{
    rtc_adapter::relay_registry::RelayTargetId,
    transport_adapter::{ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, TransportMediaId},
};

#[derive(Debug, Default)]
pub(in crate::runtime::rtc_adapter) struct RouteControlState {
    sources: BTreeMap<TransportMediaId, SourceRouteControl>,
}

impl RouteControlState {
    pub(in crate::runtime::rtc_adapter) fn decide_keyframe_request(
        &mut self,
        source_transport_media_id: TransportMediaId,
        now: Instant,
    ) -> KeyframeRequestDecision {
        let source_control = self.sources.entry(source_transport_media_id).or_default();
        let Some(window) = source_control.keyframe_request else {
            source_control.keyframe_request = Some(KeyframeRequestWindow::new(now));
            return KeyframeRequestDecision::Forward;
        };
        if window.is_open(now) {
            return KeyframeRequestDecision::Absorb;
        }
        source_control.keyframe_request = Some(KeyframeRequestWindow::new(now));
        KeyframeRequestDecision::Forward
    }

    pub(in crate::runtime::rtc_adapter) fn decide_packet_route(
        &self,
        source_transport_media_id: TransportMediaId,
        metadata: PacketLayerMetadata,
    ) -> PacketRouteDecision {
        let Some(source_control) = self.sources.get(&source_transport_media_id) else {
            return PacketRouteDecision::Forward;
        };
        match source_control
            .effective_packet_gate()
            .unwrap_or(PacketLayerGate::Open)
        {
            gate if gate.permits(metadata) => PacketRouteDecision::Forward,
            _ => PacketRouteDecision::Drop,
        }
    }

    pub(in crate::runtime::rtc_adapter) fn set_local_packet_gate(
        &mut self,
        source_transport_media_id: TransportMediaId,
        packet_gate: Option<PacketLayerGate>,
    ) {
        let should_remove =
            if let Some(source_control) = self.sources.get_mut(&source_transport_media_id) {
                source_control.local_packet_gate = packet_gate;
                source_control.is_empty()
            } else {
                let Some(packet_gate) = packet_gate else {
                    return;
                };
                self.sources.insert(
                    source_transport_media_id,
                    SourceRouteControl {
                        local_packet_gate: Some(packet_gate),
                        ..Default::default()
                    },
                );
                return;
            };
        if should_remove {
            self.sources.remove(&source_transport_media_id);
        }
    }

    pub(in crate::runtime::rtc_adapter) fn observe_audio_activity(
        &mut self,
        source_transport_media_id: TransportMediaId,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
    ) -> bool {
        let should_remove =
            if let Some(source_control) = self.sources.get_mut(&source_transport_media_id) {
                if !source_control.observe_audio_activity(voice_activity, audio_level_dbov, now) {
                    return false;
                }
                source_control.is_empty()
            } else {
                if voice_activity.is_none() && audio_level_dbov.is_none() {
                    return false;
                }
                let mut source_control = SourceRouteControl::default();
                if !source_control.observe_audio_activity(voice_activity, audio_level_dbov, now) {
                    return false;
                }
                if source_control.is_empty() {
                    return false;
                }
                self.sources
                    .insert(source_transport_media_id, source_control);
                return true;
            };
        if should_remove {
            self.sources.remove(&source_transport_media_id);
        }
        true
    }

    pub(in crate::runtime::rtc_adapter) fn set_relay_packet_gate(
        &mut self,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        packet_gate: PacketLayerGate,
    ) {
        self.sources
            .entry(source_transport_media_id)
            .or_default()
            .relay_packet_gates
            .insert(target_id, packet_gate);
    }

    pub(in crate::runtime::rtc_adapter) fn relay_packet_gate(
        &self,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
    ) -> Option<&PacketLayerGate> {
        self.sources
            .get(&source_transport_media_id)
            .and_then(|source_control| source_control.relay_packet_gates.get(&target_id))
    }

    pub(in crate::runtime::rtc_adapter) fn forget_source(
        &mut self,
        source_transport_media_id: TransportMediaId,
    ) {
        self.sources.remove(&source_transport_media_id);
    }

    pub(in crate::runtime::rtc_adapter) fn retain_sources<F>(&mut self, mut keep: F)
    where
        F: FnMut(&TransportMediaId) -> bool,
    {
        self.sources
            .retain(|source_transport_media_id, _source_control| keep(source_transport_media_id));
    }

    pub(in crate::runtime::rtc_adapter) fn active_speaker_sources(
        &self,
        now: Instant,
    ) -> Vec<ActiveSpeakerSource> {
        let mut sources = self
            .sources
            .iter()
            .filter_map(|(source_transport_media_id, source_control)| {
                source_control.active_speaker_source(*source_transport_media_id, now)
            })
            .collect::<Vec<_>>();
        sources.sort_by_key(|source| {
            (
                Reverse(source.observed_at()),
                source.transport_media_id().as_u64(),
            )
        });
        sources
    }

    pub(in crate::runtime::rtc_adapter) fn active_speaker_diagnostics(
        &self,
        now: Instant,
    ) -> Vec<ActiveSpeakerSourceDiagnostic> {
        self.sources
            .iter()
            .filter_map(|(source_transport_media_id, source_control)| {
                source_control.active_speaker_diagnostic(*source_transport_media_id, now)
            })
            .collect()
    }

    pub(in crate::runtime::rtc_adapter) fn next_active_speaker_deadline(
        &self,
        now: Instant,
    ) -> Option<Instant> {
        self.sources
            .values()
            .filter_map(|source_control| {
                source_control
                    .source_audio_policy
                    .as_ref()
                    .and_then(|source_audio_policy| source_audio_policy.active_deadline_after(now))
            })
            .min()
    }

    pub(in crate::runtime::rtc_adapter) fn expired_active_speaker_source_ids(
        &self,
        now: Instant,
    ) -> Vec<TransportMediaId> {
        self.sources
            .iter()
            .filter_map(|(source_transport_media_id, source_control)| {
                source_control
                    .source_audio_policy
                    .as_ref()
                    .is_some_and(|source_audio_policy| source_audio_policy.expired_at(now))
                    .then_some(*source_transport_media_id)
            })
            .collect()
    }

    #[cfg(test)]
    pub(in crate::runtime::rtc_adapter) fn set_packet_gate(
        &mut self,
        source_transport_media_id: TransportMediaId,
        packet_gate: PacketLayerGate,
    ) {
        self.set_local_packet_gate(source_transport_media_id, Some(packet_gate));
    }

    #[cfg(test)]
    pub(in crate::runtime::rtc_adapter) fn effective_packet_gate(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<PacketLayerGate> {
        self.sources
            .get(&source_transport_media_id)
            .and_then(SourceRouteControl::effective_packet_gate)
    }
}

#[derive(Debug, Default)]
struct SourceRouteControl {
    keyframe_request: Option<KeyframeRequestWindow>,
    source_audio_policy: Option<SourceAudioPolicyState>,
    local_packet_gate: Option<PacketLayerGate>,
    relay_packet_gates: BTreeMap<RelayTargetId, PacketLayerGate>,
}

impl SourceRouteControl {
    fn active_speaker_source(
        &self,
        source_transport_media_id: TransportMediaId,
        now: Instant,
    ) -> Option<ActiveSpeakerSource> {
        self.source_audio_policy
            .as_ref()
            .and_then(|source_audio_policy| source_audio_policy.active_speaker_observed_at(now))
            .map(|observed_at| ActiveSpeakerSource::new(source_transport_media_id, observed_at))
    }

    fn active_speaker_diagnostic(
        &self,
        source_transport_media_id: TransportMediaId,
        now: Instant,
    ) -> Option<ActiveSpeakerSourceDiagnostic> {
        self.source_audio_policy
            .as_ref()
            .map(|source_audio_policy| {
                source_audio_policy.diagnostic(source_transport_media_id, now)
            })
    }

    fn observe_audio_activity(
        &mut self,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
    ) -> bool {
        let previous = self.source_audio_policy.clone();
        let Some(mut source_policy) = self.source_audio_policy.take().or_else(|| {
            (voice_activity.is_some() || audio_level_dbov.is_some())
                .then(SourceAudioPolicyState::default)
        }) else {
            return false;
        };
        source_policy.observe_packet(voice_activity, audio_level_dbov, now);
        self.source_audio_policy = Some(source_policy);
        self.source_audio_policy != previous
    }

    fn effective_packet_gate(&self) -> Option<PacketLayerGate> {
        intersect_packet_gates(
            aggregate_packet_gates(
                self.local_packet_gate
                    .iter()
                    .chain(self.relay_packet_gates.values()),
            ),
            self.source_audio_policy
                .as_ref()
                .map(SourceAudioPolicyState::packet_gate),
        )
    }

    fn is_empty(&self) -> bool {
        self.keyframe_request.is_none()
            && self.source_audio_policy.is_none()
            && self.local_packet_gate.is_none()
            && self.relay_packet_gates.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{
        KeyframeRequestDecision, PacketLayerGate, PacketLayerMetadata, PacketRouteDecision,
        RouteControlState,
    };
    use crate::runtime::{
        rtc_adapter::relay_registry::RelayTargetId,
        transport_adapter::{
            ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSource,
            TransportMediaId,
        },
    };

    #[test]
    fn route_control_absorbs_repeated_keyframe_requests_within_the_window() {
        let mut state = RouteControlState::default();
        let source_transport_media_id = TransportMediaId::new(17);
        let now = Instant::now();

        assert_eq!(
            state.decide_keyframe_request(source_transport_media_id, now),
            KeyframeRequestDecision::Forward
        );
        assert_eq!(
            state.decide_keyframe_request(source_transport_media_id, now),
            KeyframeRequestDecision::Absorb
        );
    }

    #[test]
    fn route_control_reopens_after_the_coalesce_window() {
        let mut state = RouteControlState::default();
        let source_transport_media_id = TransportMediaId::new(18);
        let now = Instant::now();

        assert_eq!(
            state.decide_keyframe_request(source_transport_media_id, now),
            KeyframeRequestDecision::Forward
        );
        assert_eq!(
            state.decide_keyframe_request(source_transport_media_id, now + Duration::from_secs(1)),
            KeyframeRequestDecision::Forward
        );
    }

    #[test]
    fn route_control_drops_packets_when_the_source_is_blocked() {
        let mut state = RouteControlState::default();
        let source_transport_media_id = TransportMediaId::new(19);
        state.set_packet_gate(source_transport_media_id, PacketLayerGate::Block);

        assert_eq!(
            state.decide_packet_route(source_transport_media_id, PacketLayerMetadata::default()),
            PacketRouteDecision::Drop
        );
    }

    #[test]
    fn route_control_combines_local_and_remote_target_gates() {
        let mut state = RouteControlState::default();
        let source_transport_media_id = TransportMediaId::new(21);

        state.set_local_packet_gate(
            source_transport_media_id,
            Some(PacketLayerGate::Rid("hi".into())),
        );
        state.set_relay_packet_gate(
            source_transport_media_id,
            RelayTargetId::new(1),
            PacketLayerGate::Rid("hi".into()),
        );

        assert_eq!(
            state.effective_packet_gate(source_transport_media_id),
            Some(PacketLayerGate::Rid("hi".into()))
        );

        state.set_relay_packet_gate(
            source_transport_media_id,
            RelayTargetId::new(2),
            PacketLayerGate::Rid("lo".into()),
        );

        assert_eq!(
            state.effective_packet_gate(source_transport_media_id),
            Some(PacketLayerGate::Open)
        );
    }

    #[test]
    fn route_control_transport_audio_policy_blocks_silent_sources() {
        let mut state = RouteControlState::default();
        let source_transport_media_id = TransportMediaId::new(22);
        let now = Instant::now();

        state.set_local_packet_gate(source_transport_media_id, Some(PacketLayerGate::Open));
        state.observe_audio_activity(source_transport_media_id, Some(false), None, now);

        assert_eq!(
            state.effective_packet_gate(source_transport_media_id),
            Some(PacketLayerGate::Block)
        );
    }

    #[test]
    fn route_control_vad_true_promotes_active_speaker_immediately() {
        let mut state = RouteControlState::default();
        let source_transport_media_id = TransportMediaId::new(28);
        let now = Instant::now();

        state.observe_audio_activity(source_transport_media_id, Some(true), Some(-90), now);

        let snapshot = state.active_speaker_sources(now);
        assert_eq!(snapshot.len(), 1);
        assert_eq!(
            snapshot.first().map(|source| source.transport_media_id()),
            Some(source_transport_media_id)
        );

        let diagnostics = state.active_speaker_diagnostics(now);
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics.first().map(|diagnostic| diagnostic.state()),
            Some(ActiveSpeakerActivityState::Active)
        );
        assert_eq!(
            diagnostics.first().map(|diagnostic| diagnostic.reason()),
            Some(ActiveSpeakerActivityReason::Vad)
        );
    }

    #[test]
    fn route_control_vad_false_overrides_loud_audio_level() {
        let mut state = RouteControlState::default();
        let source_transport_media_id = TransportMediaId::new(29);
        let now = Instant::now();

        state.observe_audio_activity(source_transport_media_id, Some(false), Some(-12), now);

        assert!(state.active_speaker_sources(now).is_empty());
        assert_eq!(
            state.effective_packet_gate(source_transport_media_id),
            Some(PacketLayerGate::Block)
        );
        let diagnostics = state.active_speaker_diagnostics(now);
        assert_eq!(
            diagnostics.first().map(|diagnostic| diagnostic.state()),
            Some(ActiveSpeakerActivityState::Blocked)
        );
        assert_eq!(
            diagnostics.first().map(|diagnostic| diagnostic.reason()),
            Some(ActiveSpeakerActivityReason::VadFalse)
        );
    }

    #[test]
    fn route_control_transport_audio_policy_holds_recent_speech_open() {
        let mut state = RouteControlState::default();
        let source_transport_media_id = TransportMediaId::new(23);
        let now = Instant::now();

        state.set_local_packet_gate(
            source_transport_media_id,
            Some(PacketLayerGate::Rid("hi".into())),
        );
        state.observe_audio_activity(source_transport_media_id, Some(true), None, now);
        state.observe_audio_activity(
            source_transport_media_id,
            Some(false),
            None,
            now + Duration::from_millis(100),
        );

        assert_eq!(
            state.effective_packet_gate(source_transport_media_id),
            Some(PacketLayerGate::Rid("hi".into()))
        );
    }

    #[test]
    fn route_control_transport_audio_policy_reblocks_after_the_hold_window() {
        let mut state = RouteControlState::default();
        let source_transport_media_id = TransportMediaId::new(24);
        let now = Instant::now();

        state.set_local_packet_gate(source_transport_media_id, Some(PacketLayerGate::Open));
        state.observe_audio_activity(source_transport_media_id, Some(true), None, now);
        state.observe_audio_activity(
            source_transport_media_id,
            Some(false),
            None,
            now + Duration::from_millis(300),
        );

        assert_eq!(
            state.effective_packet_gate(source_transport_media_id),
            Some(PacketLayerGate::Block)
        );
    }

    #[test]
    fn route_control_transport_audio_policy_uses_repeated_audio_level_fallback() {
        let mut state = RouteControlState::default();
        let source_transport_media_id = TransportMediaId::new(25);
        let now = Instant::now();

        state.set_local_packet_gate(source_transport_media_id, Some(PacketLayerGate::Open));
        state.observe_audio_activity(source_transport_media_id, None, Some(-24), now);

        assert!(state.active_speaker_sources(now).is_empty());
        assert_eq!(
            state.effective_packet_gate(source_transport_media_id),
            Some(PacketLayerGate::Open)
        );

        state.observe_audio_activity(
            source_transport_media_id,
            None,
            Some(-24),
            now + Duration::from_millis(20),
        );

        let snapshot = state.active_speaker_sources(now + Duration::from_millis(20));
        assert_eq!(snapshot.len(), 1);
        assert_eq!(
            snapshot.first().map(|source| source.transport_media_id()),
            Some(source_transport_media_id)
        );
        let diagnostics = state.active_speaker_diagnostics(now + Duration::from_millis(20));
        assert_eq!(
            diagnostics.first().map(|diagnostic| diagnostic.state()),
            Some(ActiveSpeakerActivityState::Active)
        );
        assert_eq!(
            diagnostics.first().map(|diagnostic| diagnostic.reason()),
            Some(ActiveSpeakerActivityReason::AudioLevel)
        );
    }

    #[test]
    fn route_control_transport_audio_policy_rejects_persistent_low_noise() {
        let mut state = RouteControlState::default();
        let source_transport_media_id = TransportMediaId::new(26);
        let now = Instant::now();

        for offset in [0, 20, 40] {
            state.observe_audio_activity(
                source_transport_media_id,
                None,
                Some(-80),
                now + Duration::from_millis(offset),
            );
        }

        assert!(
            state
                .active_speaker_sources(now + Duration::from_millis(40))
                .is_empty()
        );
        assert_eq!(
            state.effective_packet_gate(source_transport_media_id),
            Some(PacketLayerGate::Block)
        );
        let diagnostics = state.active_speaker_diagnostics(now + Duration::from_millis(40));
        assert_eq!(
            diagnostics.first().map(|diagnostic| diagnostic.state()),
            Some(ActiveSpeakerActivityState::Blocked)
        );
        assert_eq!(
            diagnostics.first().map(|diagnostic| diagnostic.reason()),
            Some(ActiveSpeakerActivityReason::LowNoise)
        );
    }

    #[test]
    fn route_control_active_speaker_expiry_is_observable() {
        let mut state = RouteControlState::default();
        let source_transport_media_id = TransportMediaId::new(30);
        let now = Instant::now();

        state.observe_audio_activity(source_transport_media_id, Some(true), None, now);

        let expired_at = now + Duration::from_millis(300);
        assert!(state.active_speaker_sources(expired_at).is_empty());
        let diagnostics = state.active_speaker_diagnostics(expired_at);
        assert_eq!(
            diagnostics.first().map(|diagnostic| diagnostic.state()),
            Some(ActiveSpeakerActivityState::RecentlyExpired)
        );
        assert_eq!(
            diagnostics.first().map(|diagnostic| diagnostic.reason()),
            Some(ActiveSpeakerActivityReason::Expired)
        );
    }

    #[test]
    fn route_control_active_speaker_order_is_deterministic_for_equal_observations() {
        let mut state = RouteControlState::default();
        let first_source_transport_media_id = TransportMediaId::new(31);
        let second_source_transport_media_id = TransportMediaId::new(32);
        let now = Instant::now();

        state.observe_audio_activity(second_source_transport_media_id, Some(true), None, now);
        state.observe_audio_activity(first_source_transport_media_id, Some(true), None, now);

        assert_eq!(
            state
                .active_speaker_sources(now)
                .into_iter()
                .map(ActiveSpeakerSource::transport_media_id)
                .collect::<Vec<_>>(),
            vec![
                first_source_transport_media_id,
                second_source_transport_media_id
            ]
        );
    }

    #[test]
    fn route_control_local_packet_gate_composes_with_transport_audio_policy() {
        let mut state = RouteControlState::default();
        let source_transport_media_id = TransportMediaId::new(27);
        let now = Instant::now();

        state.set_local_packet_gate(
            source_transport_media_id,
            Some(PacketLayerGate::Rid("hi".into())),
        );
        state.observe_audio_activity(source_transport_media_id, Some(true), None, now);

        assert_eq!(
            state.effective_packet_gate(source_transport_media_id),
            Some(PacketLayerGate::Rid("hi".into()))
        );

        state.observe_audio_activity(
            source_transport_media_id,
            Some(false),
            None,
            now + Duration::from_millis(300),
        );

        assert_eq!(
            state.effective_packet_gate(source_transport_media_id),
            Some(PacketLayerGate::Block)
        );
    }
}
