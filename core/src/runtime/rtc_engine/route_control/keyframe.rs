//! Keyframe request coalescing for source routes.
//!
//! Receiver-driven upswitches, route resumes, and relayed feedback may all ask
//! for a fresh decodable frame. This module contain the packet-route side of that
//! debounce so repeated requests inside a short window do not fan out
//! redundant PLI/FIR traffic.

use std::time::{Duration, Instant};

use str0m::media::KeyframeRequestKind;

const KEYFRAME_REQUEST_COALESCE_WINDOW: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::rtc_engine) enum KeyframeRequestDecision {
    Forward,
    Absorb,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct KeyframeRequestWindow {
    blocked_until: Instant,
}

impl KeyframeRequestWindow {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            blocked_until: now + KEYFRAME_REQUEST_COALESCE_WINDOW,
        }
    }

    pub(super) fn is_open(self, now: Instant) -> bool {
        now < self.blocked_until
    }
}

pub(in crate::runtime::rtc_engine) fn coalesce_keyframe_kind(
    current: KeyframeRequestKind,
    incoming: KeyframeRequestKind,
) -> KeyframeRequestKind {
    match (current, incoming) {
        (KeyframeRequestKind::Fir, _) | (_, KeyframeRequestKind::Fir) => KeyframeRequestKind::Fir,
        _ => current,
    }
}

#[cfg(test)]
mod tests {
    use str0m::media::KeyframeRequestKind;

    use super::coalesce_keyframe_kind;

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
