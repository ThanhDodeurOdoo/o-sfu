use super::{
    receiver::PlannedReceiverRoute,
    selection::{AdaptationCounts, ReceiverRouteSelection},
};
use crate::engine::source_model::PolicyPauseReason;

pub(super) const DOWNSWITCH_PRESSURE_OBSERVATIONS: u8 = 2;
pub(super) const UPSWITCH_STABLE_OBSERVATIONS: u8 = 3;

pub(super) fn resolve(route: &PlannedReceiverRoute<'_>) -> ReceiverRouteSelection {
    let current = route.route.current_selection;
    let current_pause_reason = current.policy_pause_reason();
    let selection = route.selection;
    match (selection.policy_pause_reason, current_pause_reason) {
        (None, Some(PolicyPauseReason::VideoDownloadLimit)) => {
            ReceiverRouteSelection::send(selection.selector, AdaptationCounts::reset(), true)
        }
        (None, Some(reason)) => {
            let counts = AdaptationCounts::next_upgrade(current, UPSWITCH_STABLE_OBSERVATIONS);
            if counts.upgrade >= UPSWITCH_STABLE_OBSERVATIONS {
                ReceiverRouteSelection::send(selection.selector, AdaptationCounts::reset(), true)
            } else {
                ReceiverRouteSelection::hold(current, Some(reason), counts)
            }
        }
        (Some(reason), pause_reason) if pause_reason != Some(reason) => {
            if matches!(
                reason,
                PolicyPauseReason::VideoDownloadLimit | PolicyPauseReason::SourceBitrateLimit
            ) {
                return ReceiverRouteSelection::pause(current, reason, AdaptationCounts::reset());
            }
            let counts = AdaptationCounts::next_pressure(current, DOWNSWITCH_PRESSURE_OBSERVATIONS);
            if counts.pressure >= DOWNSWITCH_PRESSURE_OBSERVATIONS {
                ReceiverRouteSelection::pause(current, reason, AdaptationCounts::reset())
            } else {
                ReceiverRouteSelection::hold(current, None, counts)
            }
        }
        _ => selection,
    }
}
