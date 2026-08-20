use o_sfu_router::MediaKind;

use super::{
    super::action::{ConsumerPacketSelectionUpdate, RouteBudgetOutcome},
    solver::{AdaptationCounts, PlannedReceiverRoute, ReceiverRouteSelection, RouteOutcome},
};
use crate::engine::{
    media_transport::SourcePacketGate,
    source_model::{PublishedSourceDescriptor, ReceiverVideoBudgetDiagnostics, SourceSelector},
};

/// Projects a room selector into a source-worker packet gate.
///
/// Returns `None` when the selected encoding is unknown or has no negotiated RID.
pub(super) fn source_packet_gate_for_selector(
    source: &PublishedSourceDescriptor,
    selector: SourceSelector,
) -> Option<SourcePacketGate> {
    match selector {
        SourceSelector::Open => Some(SourcePacketGate::Open),
        SourceSelector::Encoding(encoding_id) => {
            // Never fall back to `Open`: it would forward every encoding while
            // room state records one selected encoding.
            let encoding = source.encoding(encoding_id)?;
            let rid = encoding.rid()?;
            Some(SourcePacketGate::Rid(rid.as_str().to_owned()))
        }
    }
}

pub(super) fn consumer_packet_selection_update(
    planned_route: &PlannedReceiverRoute<'_>,
    selection: ReceiverRouteSelection,
    budget: ReceiverVideoBudgetDiagnostics,
) -> Option<ConsumerPacketSelectionUpdate> {
    let input = planned_route.input;
    let current_selection = input.current_selection;
    if selection.selector == current_selection.selector()
        && selection.policy_pause_reason == current_selection.policy_pause_reason()
        && budget == current_selection.budget()
        && selection.counts == AdaptationCounts::from_current(current_selection)
    {
        return None;
    }
    let packet_gate = if selection.selector == current_selection.selector() {
        None
    } else {
        Some(source_packet_gate_for_selector(
            input.source,
            selection.selector,
        )?)
    };
    let route_activity_changed =
        selection.policy_pause_reason != current_selection.policy_pause_reason();
    // A newly selected RID or resumed route may not share the receiver's last
    // decodable reference chain, so request a keyframe for either transition.
    let request_keyframe = selection.policy_pause_reason.is_none()
        && (selection.request_keyframe
            || selection.selector != current_selection.selector()
            || !current_selection.policy_allows_delivery());
    let outcome = route_outcome(planned_route, selection);
    Some(ConsumerPacketSelectionUpdate {
        key: input.key.clone(),
        source_id: input.source.source_id(),
        route: input.route.clone(),
        selector: selection.selector,
        policy_pause_reason: selection.policy_pause_reason,
        budget,
        outcome,
        pressure_observations: selection.counts.pressure,
        upgrade_observations: selection.counts.upgrade,
        packet_gate,
        route_activity_changed,
        request_keyframe: request_keyframe && input.source.media_kind() == MediaKind::Video,
    })
}

fn route_outcome(
    route: &PlannedReceiverRoute<'_>,
    selection: ReceiverRouteSelection,
) -> Option<RouteBudgetOutcome> {
    match (
        selection.policy_pause_reason,
        route.input.current_selection.policy_pause_reason(),
    ) {
        (None, Some(_reason)) => Some(RouteBudgetOutcome::Resumed),
        (Some(reason), current_reason) if current_reason != Some(reason) => {
            Some(RouteBudgetOutcome::Paused)
        }
        _ => match route.outcome {
            RouteOutcome::Neutral => None,
            RouteOutcome::Degraded => Some(RouteBudgetOutcome::Degraded),
            RouteOutcome::Paused => Some(RouteBudgetOutcome::Paused),
        },
    }
}
