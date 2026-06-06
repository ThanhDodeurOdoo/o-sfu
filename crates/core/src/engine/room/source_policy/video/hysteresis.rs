//! receiver video adaptation stability rules

use super::planner::{PlannedReceiverRoute, ReceiverRouteDecision};
use crate::engine::source_model::PolicyPauseReason;

pub(super) const DOWNSWITCH_PRESSURE_OBSERVATIONS: u8 = 2;
pub(super) const UPSWITCH_STABLE_OBSERVATIONS: u8 = 3;

pub(super) fn resolve(route: &PlannedReceiverRoute<'_>) -> ReceiverRouteDecision {
    let current = route.input().current_selection;
    let current_pause_reason = current.policy_pause_reason();
    match (route.decision(), current_pause_reason) {
        (
            ReceiverRouteDecision::Send { selector, .. },
            Some(PolicyPauseReason::VideoDownloadLimit),
        ) => ReceiverRouteDecision::Send {
            selector,
            pressure_observations: 0,
            upgrade_observations: 0,
            request_keyframe: true,
        },
        (ReceiverRouteDecision::Send { selector, .. }, Some(reason)) => {
            let upgrade_observations = current
                .upgrade_observations()
                .saturating_add(1)
                .min(UPSWITCH_STABLE_OBSERVATIONS);
            if upgrade_observations >= UPSWITCH_STABLE_OBSERVATIONS {
                ReceiverRouteDecision::Send {
                    selector,
                    pressure_observations: 0,
                    upgrade_observations: 0,
                    request_keyframe: true,
                }
            } else {
                ReceiverRouteDecision::Hold {
                    policy_pause_reason: Some(reason),
                    selector: current.selector(),
                    pressure_observations: 0,
                    upgrade_observations,
                }
            }
        }
        (ReceiverRouteDecision::Pause { reason, .. }, pause_reason)
            if pause_reason != Some(reason) =>
        {
            if reason == PolicyPauseReason::VideoDownloadLimit {
                return ReceiverRouteDecision::Pause {
                    reason,
                    pressure_observations: 0,
                    upgrade_observations: 0,
                };
            }
            let pressure_observations = current
                .pressure_observations()
                .saturating_add(1)
                .min(DOWNSWITCH_PRESSURE_OBSERVATIONS);
            if pressure_observations >= DOWNSWITCH_PRESSURE_OBSERVATIONS {
                ReceiverRouteDecision::Pause {
                    reason,
                    pressure_observations: 0,
                    upgrade_observations: 0,
                }
            } else {
                ReceiverRouteDecision::Hold {
                    policy_pause_reason: None,
                    selector: current.selector(),
                    pressure_observations,
                    upgrade_observations: 0,
                }
            }
        }
        _ => route.decision(),
    }
}
