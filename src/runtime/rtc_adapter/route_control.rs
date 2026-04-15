use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use str0m::media::KeyframeRequestKind;

use crate::runtime::transport_adapter::TransportMediaId;

const KEYFRAME_REQUEST_COALESCE_WINDOW: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum KeyframeRequestDecision {
    Forward,
    Absorb,
}

#[derive(Debug, Default)]
pub(super) struct RouteControlState {
    keyframe_requests: BTreeMap<TransportMediaId, KeyframeRequestWindow>,
}

impl RouteControlState {
    pub(super) fn decide_keyframe_request(
        &mut self,
        source_transport_media_id: TransportMediaId,
        now: Instant,
    ) -> KeyframeRequestDecision {
        let Some(window) = self.keyframe_requests.get(&source_transport_media_id) else {
            self.keyframe_requests
                .insert(source_transport_media_id, KeyframeRequestWindow::new(now));
            return KeyframeRequestDecision::Forward;
        };
        if window.is_open(now) {
            return KeyframeRequestDecision::Absorb;
        }
        self.keyframe_requests
            .insert(source_transport_media_id, KeyframeRequestWindow::new(now));
        KeyframeRequestDecision::Forward
    }

    pub(super) fn forget_source(&mut self, source_transport_media_id: TransportMediaId) {
        self.keyframe_requests.remove(&source_transport_media_id);
    }

    pub(super) fn retain_sources<F>(&mut self, mut keep: F)
    where
        F: FnMut(&TransportMediaId) -> bool,
    {
        self.keyframe_requests
            .retain(|source_transport_media_id, _window| keep(source_transport_media_id));
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

    use super::{KeyframeRequestDecision, RouteControlState, coalesce_keyframe_kind};
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
}
