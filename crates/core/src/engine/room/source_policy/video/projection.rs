use o_sfu_router::MediaKind;

use super::{
    super::action::{BudgetSolverOutcomes, ConsumerPacketSelectionUpdate},
    receiver::{PlannedReceiverRoute, RouteOutcome},
    selection::{AdaptationCounts, ReceiverRouteSelection},
};
use crate::engine::{
    media_transport::{SourcePacketGate, SourcePacketOperatingPoint},
    source_model::{PublishedSourceDescriptor, ReceiverVideoBudgetDiagnostics, SourceSelector},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SourcePacketGateProjectionError {
    MissingEncoding,
    MissingRid,
    MissingTemporalMetadata,
    TemporalLayerExceedsAdvertised,
}

pub(super) fn source_packet_gate_for_selector(
    source: &PublishedSourceDescriptor,
    selector: SourceSelector,
) -> Result<SourcePacketGate, SourcePacketGateProjectionError> {
    match selector {
        SourceSelector::Open => Ok(SourcePacketGate::Open),
        SourceSelector::Encoding(encoding_id) => {
            let encoding = source
                .encoding(encoding_id)
                .ok_or(SourcePacketGateProjectionError::MissingEncoding)?;
            let rid = encoding
                .rid()
                .ok_or(SourcePacketGateProjectionError::MissingRid)?;
            Ok(SourcePacketGate::Rid(rid.as_str().to_owned()))
        }
        SourceSelector::OperatingPoint(operating_point) => {
            let encoding = source
                .encoding(operating_point.encoding_id())
                .ok_or(SourcePacketGateProjectionError::MissingEncoding)?;
            let max_temporal_layer_id = encoding
                .max_temporal_layer_id()
                .ok_or(SourcePacketGateProjectionError::MissingTemporalMetadata)?;
            if operating_point.max_temporal_layer_id() > max_temporal_layer_id {
                return Err(SourcePacketGateProjectionError::TemporalLayerExceedsAdvertised);
            }
            Ok(SourcePacketGate::OperatingPoint(
                SourcePacketOperatingPoint::new(
                    encoding.rid().map(|rid| rid.as_str().to_owned()),
                    operating_point.max_temporal_layer_id().as_u8(),
                ),
            ))
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
        Some(source_packet_gate_for_selector(input.source, selection.selector).ok()?)
    };
    let route_activity_changed =
        selection.policy_pause_reason != current_selection.policy_pause_reason();
    let request_keyframe = selection.policy_pause_reason.is_none()
        && (selection.request_keyframe || !current_selection.policy_allows_delivery());
    let mut outcomes = route_outcomes(planned_route, selection);
    if budget.over_budget_exception_reason().is_some() {
        outcomes = outcomes.with_protected_over_budget();
    }
    Some(ConsumerPacketSelectionUpdate {
        transport_ref: input.transport_ref.clone(),
        source_id: input.source.source_id(),
        selector: selection.selector,
        policy_pause_reason: selection.policy_pause_reason,
        budget,
        outcomes,
        pressure_observations: selection.counts.pressure,
        upgrade_observations: selection.counts.upgrade,
        packet_gate,
        route_activity_changed,
        request_keyframe: request_keyframe && input.source.media_kind() == MediaKind::Video,
    })
}

fn route_outcomes(
    route: &PlannedReceiverRoute<'_>,
    selection: ReceiverRouteSelection,
) -> BudgetSolverOutcomes {
    match (
        selection.policy_pause_reason,
        route.input.current_selection.policy_pause_reason(),
    ) {
        (None, Some(_reason)) => BudgetSolverOutcomes::resumed(),
        (Some(reason), current_reason) if current_reason != Some(reason) => {
            BudgetSolverOutcomes::paused()
        }
        _ => route.outcome.into(),
    }
}

impl From<RouteOutcome> for BudgetSolverOutcomes {
    fn from(outcome: RouteOutcome) -> Self {
        match outcome {
            RouteOutcome::Neutral => Self::default(),
            RouteOutcome::Degraded => Self::degraded(),
            RouteOutcome::Paused => Self::paused(),
        }
    }
}

#[cfg(test)]
#[path = "TESTS/projection.rs"]
mod tests;
