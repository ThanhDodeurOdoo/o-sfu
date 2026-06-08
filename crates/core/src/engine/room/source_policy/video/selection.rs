use crate::engine::source_model::{ConsumerSourceSelection, PolicyPauseReason, SourceSelector};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct AdaptationCounts {
    pub(super) pressure: u8,
    pub(super) upgrade: u8,
}

impl AdaptationCounts {
    pub(super) const fn reset() -> Self {
        Self {
            pressure: 0,
            upgrade: 0,
        }
    }

    pub(super) fn from_current(selection: ConsumerSourceSelection) -> Self {
        Self {
            pressure: selection.pressure_observations(),
            upgrade: selection.upgrade_observations(),
        }
    }

    pub(super) fn next_pressure(selection: ConsumerSourceSelection, limit: u8) -> Self {
        Self {
            pressure: selection
                .pressure_observations()
                .saturating_add(1)
                .min(limit),
            upgrade: 0,
        }
    }

    pub(super) fn next_upgrade(selection: ConsumerSourceSelection, limit: u8) -> Self {
        Self {
            pressure: 0,
            upgrade: selection
                .upgrade_observations()
                .saturating_add(1)
                .min(limit),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ReceiverRouteSelection {
    pub(super) selector: SourceSelector,
    pub(super) policy_pause_reason: Option<PolicyPauseReason>,
    pub(super) counts: AdaptationCounts,
    pub(super) request_keyframe: bool,
}

impl ReceiverRouteSelection {
    pub(super) const fn send(
        selector: SourceSelector,
        counts: AdaptationCounts,
        request_keyframe: bool,
    ) -> Self {
        Self {
            selector,
            policy_pause_reason: None,
            counts,
            request_keyframe,
        }
    }

    pub(super) const fn pause(
        current: ConsumerSourceSelection,
        reason: PolicyPauseReason,
        counts: AdaptationCounts,
    ) -> Self {
        Self {
            selector: current.selector(),
            policy_pause_reason: Some(reason),
            counts,
            request_keyframe: false,
        }
    }

    pub(super) const fn hold(
        current: ConsumerSourceSelection,
        policy_pause_reason: Option<PolicyPauseReason>,
        counts: AdaptationCounts,
    ) -> Self {
        Self {
            selector: current.selector(),
            policy_pause_reason,
            counts,
            request_keyframe: false,
        }
    }
}
