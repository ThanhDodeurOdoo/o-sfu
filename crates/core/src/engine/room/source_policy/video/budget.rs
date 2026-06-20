use super::{
    adaptation::cheapest_useful_selector,
    admission::active_route_count,
    receiver::{PlannedReceiverRoute, RouteOutcome},
};
use crate::{
    Bitrate,
    engine::source_model::{
        OverBudgetExceptionReason, PolicyPauseReason, ReceiverVideoBudgetDiagnostics,
        SourceAdaptationPolicy, SourceRoomPolicySelector, SourceRoutePriority,
    },
};

pub(super) fn apply_overload_policy(
    routes: &mut [PlannedReceiverRoute<'_>],
    receiver_bandwidth: Bitrate,
) {
    let mut total_bitrate = selected_receiver_bitrate(routes);
    if total_bitrate <= receiver_bandwidth {
        return;
    }
    for route in routes.iter_mut().filter(|route| route_can_downgrade(route)) {
        let Some((selector, bitrate)) = cheapest_useful_selector(route.input) else {
            let selected_bitrate = route.selected_bitrate;
            route.pause(PolicyPauseReason::MissingUsableLayer, RouteOutcome::Neutral);
            total_bitrate = total_bitrate.saturating_sub(selected_bitrate);
            continue;
        };
        if bitrate < route.selected_bitrate {
            let selected_bitrate = route.selected_bitrate;
            total_bitrate = total_bitrate
                .saturating_sub(selected_bitrate)
                .saturating_add(bitrate);
            route.send(selector, bitrate, RouteOutcome::Degraded);
        }
    }
    if total_bitrate <= receiver_bandwidth {
        return;
    }
    let mut pause_order = Vec::with_capacity(routes.len());
    for route in routes.iter_mut() {
        if route.selection.policy_pause_reason.is_none() && !route_is_protected(route) {
            pause_order.push((pause_rank(route), route));
        }
    }
    pause_order.sort_by_key(|(rank, _)| *rank);
    for (_rank, route) in pause_order {
        if total_bitrate <= receiver_bandwidth {
            break;
        }
        let selected_bitrate = route.selected_bitrate;
        let pause_reason = pause_reason_for_route(route);
        route.pause(pause_reason, RouteOutcome::Paused);
        total_bitrate = total_bitrate.saturating_sub(selected_bitrate);
    }
}

pub(super) fn diagnostics(
    routes: &[PlannedReceiverRoute<'_>],
    receiver_bandwidth: Option<Bitrate>,
) -> ReceiverVideoBudgetDiagnostics {
    let selected_video_bitrate = selected_receiver_bitrate(routes);
    let over_budget_exception_reason = receiver_bandwidth
        .filter(|budget| selected_video_bitrate > *budget)
        .map(|_budget| OverBudgetExceptionReason::ProtectedRoute);
    ReceiverVideoBudgetDiagnostics::new(
        receiver_bandwidth,
        receiver_bandwidth,
        active_route_count(routes),
        selected_video_bitrate,
        over_budget_exception_reason,
    )
}

fn selected_receiver_bitrate(routes: &[PlannedReceiverRoute<'_>]) -> Bitrate {
    routes
        .iter()
        .filter(|route| route.selection.policy_pause_reason.is_none())
        .fold(Bitrate::zero(), |total, route| {
            total.saturating_add(route.selected_bitrate)
        })
}

fn route_can_downgrade(route: &PlannedReceiverRoute<'_>) -> bool {
    let input = route.input;
    route.selection.policy_pause_reason.is_none()
        && input.adaptation_policy() == SourceAdaptationPolicy::ScalableVideo
        && matches!(
            input.layout_intent.priority(),
            SourceRoutePriority::VisibleThumbnail | SourceRoutePriority::HiddenOrOverflow
        )
}

fn route_is_protected(route: &PlannedReceiverRoute<'_>) -> bool {
    matches!(
        route.input.layout_intent.priority(),
        SourceRoutePriority::PinnedOrFeatured
            | SourceRoutePriority::ReadableDetail
            | SourceRoutePriority::ActiveSpeaker
    )
}

fn pause_rank(route: &PlannedReceiverRoute<'_>) -> u8 {
    match route.input.layout_intent.priority() {
        SourceRoutePriority::HiddenOrOverflow => 0,
        SourceRoutePriority::VisibleThumbnail => 1,
        SourceRoutePriority::ActiveSpeaker => 2,
        SourceRoutePriority::ReadableDetail => 3,
        SourceRoutePriority::PinnedOrFeatured => 4,
    }
}

fn pause_reason_for_route(route: &PlannedReceiverRoute<'_>) -> PolicyPauseReason {
    let intent = route.input.layout_intent;
    match intent.priority() {
        SourceRoutePriority::HiddenOrOverflow => match intent.role() {
            SourceRoomPolicySelector::Hidden => PolicyPauseReason::HiddenTile,
            SourceRoomPolicySelector::Overflow => PolicyPauseReason::OverflowTile,
            _ => PolicyPauseReason::BudgetPressure,
        },
        _ => PolicyPauseReason::BudgetPressure,
    }
}
