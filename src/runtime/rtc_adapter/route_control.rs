use std::{
    cmp::Reverse,
    collections::BTreeMap,
    time::{Duration, Instant},
};

use str0m::media::{KeyframeRequestKind, Rid};

use crate::runtime::transport_adapter::{ActiveSpeakerSource, TransportMediaId};

use super::relay_registry::RelayTargetId;

const ACTIVE_SPEAKER_HOLD_WINDOW: Duration = Duration::from_millis(250);
const ACTIVE_SPEAKER_AUDIO_LEVEL_THRESHOLD_DBOV: i8 = -48;
const KEYFRAME_REQUEST_COALESCE_WINDOW: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum KeyframeRequestDecision {
    Forward,
    Absorb,
}

#[allow(
    dead_code,
    reason = "route-level packet gating is intentionally wired before broader orchestration uses it, so only tests construct non-default gates in this slice"
)]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) enum PacketLayerGate {
    #[default]
    Open,
    Block,
    Rid(Rid),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PacketRouteDecision {
    Forward,
    Drop,
}

#[derive(Debug, Default)]
pub(super) struct RouteControlState {
    sources: BTreeMap<TransportMediaId, SourceRouteControl>,
}

impl RouteControlState {
    pub(super) fn decide_keyframe_request(
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

    pub(super) fn decide_packet_route(
        &self,
        source_transport_media_id: TransportMediaId,
        rid: Option<Rid>,
    ) -> PacketRouteDecision {
        let Some(source_control) = self.sources.get(&source_transport_media_id) else {
            return PacketRouteDecision::Forward;
        };
        match source_control
            .effective_packet_gate()
            .unwrap_or(PacketLayerGate::Open)
        {
            PacketLayerGate::Open => PacketRouteDecision::Forward,
            PacketLayerGate::Block => PacketRouteDecision::Drop,
            PacketLayerGate::Rid(selected_rid) => rid
                .as_ref()
                .filter(|packet_rid| *packet_rid == &selected_rid)
                .map_or(PacketRouteDecision::Drop, |_rid| {
                    PacketRouteDecision::Forward
                }),
        }
    }

    pub(super) fn set_local_packet_gate(
        &mut self,
        source_transport_media_id: TransportMediaId,
        packet_gate: Option<PacketLayerGate>,
    ) {
        let source_control = self.sources.entry(source_transport_media_id).or_default();
        source_control.local_packet_gate = packet_gate;
        if source_control.is_empty() {
            self.sources.remove(&source_transport_media_id);
        }
    }

    pub(super) fn set_source_packet_gate(
        &mut self,
        source_transport_media_id: TransportMediaId,
        packet_gate: Option<PacketLayerGate>,
    ) {
        let source_control = self.sources.entry(source_transport_media_id).or_default();
        source_control.source_packet_gate = packet_gate;
        if source_control.is_empty() {
            self.sources.remove(&source_transport_media_id);
        }
    }

    pub(super) fn observe_audio_activity(
        &mut self,
        source_transport_media_id: TransportMediaId,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
    ) {
        let Some(mut source_control) = self.sources.remove(&source_transport_media_id) else {
            if voice_activity.is_none() && audio_level_dbov.is_none() {
                return;
            }
            let mut source_control = SourceRouteControl::default();
            source_control.observe_audio_activity(voice_activity, audio_level_dbov, now);
            self.sources
                .insert(source_transport_media_id, source_control);
            return;
        };
        source_control.observe_audio_activity(voice_activity, audio_level_dbov, now);
        if source_control.is_empty() {
            return;
        }
        self.sources
            .insert(source_transport_media_id, source_control);
    }

    pub(super) fn set_relay_packet_gate(
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

    pub(super) fn forget_source(&mut self, source_transport_media_id: TransportMediaId) {
        self.sources.remove(&source_transport_media_id);
    }

    pub(super) fn retain_sources<F>(&mut self, mut keep: F)
    where
        F: FnMut(&TransportMediaId) -> bool,
    {
        self.sources
            .retain(|source_transport_media_id, _source_control| keep(source_transport_media_id));
    }

    pub(super) fn active_speaker_sources(&self, now: Instant) -> Vec<ActiveSpeakerSource> {
        let mut sources = self
            .sources
            .iter()
            .filter_map(|(source_transport_media_id, source_control)| {
                source_control.active_speaker_source(*source_transport_media_id, now)
            })
            .collect::<Vec<_>>();
        sources.sort_by_key(|source| Reverse(source.observed_at()));
        sources
    }

    #[cfg(test)]
    pub(super) fn set_packet_gate(
        &mut self,
        source_transport_media_id: TransportMediaId,
        packet_gate: PacketLayerGate,
    ) {
        self.set_local_packet_gate(source_transport_media_id, Some(packet_gate));
    }

    #[cfg(test)]
    pub(super) fn effective_packet_gate(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<PacketLayerGate> {
        self.sources
            .get(&source_transport_media_id)
            .and_then(SourceRouteControl::effective_packet_gate)
    }
}

pub(super) fn aggregate_packet_gates<'a>(
    packet_gates: impl IntoIterator<Item = &'a PacketLayerGate>,
) -> Option<PacketLayerGate> {
    let mut saw_block = false;
    let mut selected_rid: Option<&Rid> = None;
    for packet_gate in packet_gates {
        match packet_gate {
            PacketLayerGate::Open => return Some(PacketLayerGate::Open),
            PacketLayerGate::Block => {
                saw_block = true;
            }
            PacketLayerGate::Rid(rid) => {
                if let Some(current_rid) = selected_rid {
                    if current_rid != rid {
                        return Some(PacketLayerGate::Open);
                    }
                } else {
                    selected_rid = Some(rid);
                }
            }
        }
    }
    selected_rid
        .copied()
        .map(PacketLayerGate::Rid)
        .or_else(|| saw_block.then_some(PacketLayerGate::Block))
}

fn intersect_packet_gates(
    first: Option<PacketLayerGate>,
    second: Option<PacketLayerGate>,
) -> Option<PacketLayerGate> {
    match (first, second) {
        (None, None) => None,
        (Some(gate), None) | (None, Some(gate)) => Some(gate),
        (Some(PacketLayerGate::Block), _) | (_, Some(PacketLayerGate::Block)) => {
            Some(PacketLayerGate::Block)
        }
        (Some(PacketLayerGate::Open), Some(gate)) | (Some(gate), Some(PacketLayerGate::Open)) => {
            Some(gate)
        }
        (Some(PacketLayerGate::Rid(first_rid)), Some(PacketLayerGate::Rid(second_rid))) => {
            if first_rid == second_rid {
                Some(PacketLayerGate::Rid(first_rid))
            } else {
                Some(PacketLayerGate::Block)
            }
        }
    }
}

pub(super) fn coalesce_keyframe_kind(
    current: KeyframeRequestKind,
    incoming: KeyframeRequestKind,
) -> KeyframeRequestKind {
    match (current, incoming) {
        (KeyframeRequestKind::Fir, _) | (_, KeyframeRequestKind::Fir) => KeyframeRequestKind::Fir,
        _ => current,
    }
}

#[derive(Debug, Clone, Copy)]
struct KeyframeRequestWindow {
    blocked_until: Instant,
}

#[derive(Debug, Default)]
struct SourceRouteControl {
    keyframe_request: Option<KeyframeRequestWindow>,
    source_audio_policy: Option<SourceAudioPolicyState>,
    source_packet_gate: Option<PacketLayerGate>,
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

    fn observe_audio_activity(
        &mut self,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
    ) {
        let Some(mut source_policy) = self.source_audio_policy.take().or_else(|| {
            (voice_activity.is_some() || audio_level_dbov.is_some())
                .then(SourceAudioPolicyState::default)
        }) else {
            return;
        };
        source_policy.observe_packet(voice_activity, audio_level_dbov, now);
        self.source_audio_policy = Some(source_policy);
    }

    fn effective_packet_gate(&self) -> Option<PacketLayerGate> {
        intersect_packet_gates(
            aggregate_packet_gates(
                self.local_packet_gate
                    .iter()
                    .chain(self.relay_packet_gates.values()),
            ),
            intersect_packet_gates(
                self.source_packet_gate.clone(),
                self.source_audio_policy
                    .as_ref()
                    .map(SourceAudioPolicyState::packet_gate),
            ),
        )
    }

    fn is_empty(&self) -> bool {
        self.keyframe_request.is_none()
            && self.source_audio_policy.is_none()
            && self.source_packet_gate.is_none()
            && self.local_packet_gate.is_none()
            && self.relay_packet_gates.is_empty()
    }
}

#[derive(Debug, Clone, Default)]
struct SourceAudioPolicyState {
    active_until: Option<Instant>,
    last_spoke_at: Option<Instant>,
    packet_gate: PacketLayerGate,
}

impl SourceAudioPolicyState {
    fn observe_packet(
        &mut self,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
    ) {
        let speech_detected = match voice_activity {
            Some(true) => true,
            Some(false) => false,
            None => audio_level_dbov
                .is_some_and(|level| level >= ACTIVE_SPEAKER_AUDIO_LEVEL_THRESHOLD_DBOV),
        };
        if speech_detected {
            self.active_until = Some(now + ACTIVE_SPEAKER_HOLD_WINDOW);
            self.last_spoke_at = Some(now);
            self.packet_gate = PacketLayerGate::Open;
            return;
        }
        if self.active_until.is_some_and(|deadline| now < deadline) {
            self.packet_gate = PacketLayerGate::Open;
            return;
        }
        self.active_until = None;
        self.packet_gate = PacketLayerGate::Block;
    }

    fn packet_gate(&self) -> PacketLayerGate {
        self.packet_gate.clone()
    }

    fn active_speaker_observed_at(&self, now: Instant) -> Option<Instant> {
        self.last_spoke_at
            .filter(|_| self.active_until.is_some_and(|deadline| now < deadline))
    }
}

impl KeyframeRequestWindow {
    fn new(now: Instant) -> Self {
        Self {
            blocked_until: now + KEYFRAME_REQUEST_COALESCE_WINDOW,
        }
    }

    fn is_open(self, now: Instant) -> bool {
        now < self.blocked_until
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use str0m::media::KeyframeRequestKind;

    use super::{
        KeyframeRequestDecision, PacketLayerGate, PacketRouteDecision, RouteControlState,
        aggregate_packet_gates, coalesce_keyframe_kind,
    };
    use crate::runtime::rtc_adapter::relay_registry::RelayTargetId;
    use crate::runtime::transport_adapter::TransportMediaId;

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
    fn route_control_prefers_fir_when_coalescing_batch_kinds() {
        assert_eq!(
            coalesce_keyframe_kind(KeyframeRequestKind::Pli, KeyframeRequestKind::Fir),
            KeyframeRequestKind::Fir
        );
        assert_eq!(
            coalesce_keyframe_kind(KeyframeRequestKind::Fir, KeyframeRequestKind::Pli),
            KeyframeRequestKind::Fir
        );
    }

    #[test]
    fn route_control_drops_packets_when_the_source_is_blocked() {
        let mut state = RouteControlState::default();
        let source_transport_media_id = TransportMediaId::new(19);
        state.set_packet_gate(source_transport_media_id, PacketLayerGate::Block);

        assert_eq!(
            state.decide_packet_route(source_transport_media_id, None),
            PacketRouteDecision::Drop
        );
    }

    #[test]
    fn route_control_only_forwards_the_selected_rid() {
        let mut state = RouteControlState::default();
        let source_transport_media_id = TransportMediaId::new(20);
        state.set_packet_gate(source_transport_media_id, PacketLayerGate::Rid("hi".into()));

        assert_eq!(
            state.decide_packet_route(source_transport_media_id, Some("hi".into())),
            PacketRouteDecision::Forward
        );
        assert_eq!(
            state.decide_packet_route(source_transport_media_id, Some("lo".into())),
            PacketRouteDecision::Drop
        );
        assert_eq!(
            state.decide_packet_route(source_transport_media_id, None),
            PacketRouteDecision::Drop
        );
    }

    #[test]
    fn aggregate_packet_gates_prefers_a_shared_selected_rid() {
        assert_eq!(
            aggregate_packet_gates([
                &PacketLayerGate::Rid("hi".into()),
                &PacketLayerGate::Rid("hi".into()),
                &PacketLayerGate::Block,
            ]),
            Some(PacketLayerGate::Rid("hi".into()))
        );
    }

    #[test]
    fn aggregate_packet_gates_reopens_when_routes_disagree() {
        assert_eq!(
            aggregate_packet_gates([
                &PacketLayerGate::Rid("hi".into()),
                &PacketLayerGate::Rid("lo".into()),
            ]),
            Some(PacketLayerGate::Open)
        );
        assert_eq!(
            aggregate_packet_gates([&PacketLayerGate::Rid("hi".into()), &PacketLayerGate::Open]),
            Some(PacketLayerGate::Open)
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
    fn route_control_transport_audio_policy_uses_audio_level_when_voice_activity_is_absent() {
        let mut state = RouteControlState::default();
        let source_transport_media_id = TransportMediaId::new(25);
        let now = Instant::now();

        state.set_local_packet_gate(source_transport_media_id, Some(PacketLayerGate::Open));
        state.observe_audio_activity(source_transport_media_id, None, Some(-24), now);

        assert_eq!(
            state.effective_packet_gate(source_transport_media_id),
            Some(PacketLayerGate::Open)
        );
    }

    #[test]
    fn route_control_source_packet_gate_intersects_with_local_consumer_policy() {
        let mut state = RouteControlState::default();
        let source_transport_media_id = TransportMediaId::new(26);

        state.set_local_packet_gate(
            source_transport_media_id,
            Some(PacketLayerGate::Rid("hi".into())),
        );
        state.set_source_packet_gate(
            source_transport_media_id,
            Some(PacketLayerGate::Rid("hi".into())),
        );

        assert_eq!(
            state.effective_packet_gate(source_transport_media_id),
            Some(PacketLayerGate::Rid("hi".into()))
        );

        state.set_source_packet_gate(
            source_transport_media_id,
            Some(PacketLayerGate::Rid("lo".into())),
        );

        assert_eq!(
            state.effective_packet_gate(source_transport_media_id),
            Some(PacketLayerGate::Block)
        );
    }

    #[test]
    fn route_control_source_packet_gate_composes_with_transport_audio_policy() {
        let mut state = RouteControlState::default();
        let source_transport_media_id = TransportMediaId::new(27);
        let now = Instant::now();

        state.set_local_packet_gate(source_transport_media_id, Some(PacketLayerGate::Open));
        state.set_source_packet_gate(
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
