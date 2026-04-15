use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use str0m::media::{KeyframeRequestKind, Rid};

use crate::runtime::transport_adapter::TransportMediaId;

const KEYFRAME_REQUEST_COALESCE_WINDOW: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum KeyframeRequestDecision {
    Forward,
    Absorb,
}

#[allow(
    dead_code,
    reason = "route-level packet gating is intentionally wired before its production policy caller lands, so only tests construct non-default gates in this slice"
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
        match &source_control.packet_gate {
            PacketLayerGate::Open => PacketRouteDecision::Forward,
            PacketLayerGate::Block => PacketRouteDecision::Drop,
            PacketLayerGate::Rid(selected_rid) => rid
                .as_ref()
                .filter(|packet_rid| *packet_rid == selected_rid)
                .map_or(PacketRouteDecision::Drop, |_rid| {
                    PacketRouteDecision::Forward
                }),
        }
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

    #[cfg(test)]
    pub(super) fn set_packet_gate(
        &mut self,
        source_transport_media_id: TransportMediaId,
        packet_gate: PacketLayerGate,
    ) {
        self.sources
            .entry(source_transport_media_id)
            .or_default()
            .packet_gate = packet_gate;
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
    packet_gate: PacketLayerGate,
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
        coalesce_keyframe_kind,
    };
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
}
