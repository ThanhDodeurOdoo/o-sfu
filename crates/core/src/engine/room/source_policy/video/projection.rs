//! Projection from source-domain route decisions to effect updates.
//!
//! The video planner speaks in `SourceSelector` values. This module is the
//! only video-policy boundary that translates that room intent into the
//! packet-facing gate vocabulary consumed by the media transport.

use super::{
    super::action::{BudgetSolverOutcomes, ConsumerPacketSelectionUpdate},
    planner::{PlannedReceiverRoute, ReceiverRouteDecision, RouteOutcome},
};
use crate::engine::{
    media_transport::{SourcePacketGate, SourcePacketOperatingPoint},
    source_model::{
        PolicyPauseReason, PublishedSourceDescriptor, ReceiverVideoBudgetDiagnostics,
        SourceSelector,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SourcePacketGateProjectionError {
    MissingEncoding,
    MissingRid,
    MissingTemporalMetadata,
    TemporalLayerExceedsAdvertised,
    UnsupportedRoomPolicy,
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
        SourceSelector::RoomPolicy(_) => {
            Err(SourcePacketGateProjectionError::UnsupportedRoomPolicy)
        }
    }
}

pub(super) fn consumer_packet_selection_update(
    route: PlannedReceiverRoute<'_>,
    decision: ReceiverRouteDecision,
    budget: ReceiverVideoBudgetDiagnostics,
) -> Option<ConsumerPacketSelectionUpdate> {
    let input = route.input();
    let current_selection = input.current_selection;
    let fields = selection_fields(decision, current_selection.selector())?;
    let decision = if fields.selector == current_selection.selector()
        && fields.policy_pause_reason == current_selection.policy_pause_reason()
        && budget == current_selection.budget()
        && fields.pressure_observations == current_selection.pressure_observations()
        && fields.upgrade_observations == current_selection.upgrade_observations()
    {
        ReceiverRouteDecision::Noop
    } else {
        decision
    };
    if matches!(decision, ReceiverRouteDecision::Noop) {
        return None;
    }
    let packet_gate = if fields.selector == current_selection.selector() {
        None
    } else {
        Some(source_packet_gate_for_selector(input.source, fields.selector).ok()?)
    };
    let route_activity_update =
        fields.policy_pause_reason != current_selection.policy_pause_reason();
    let request_keyframe = match decision {
        ReceiverRouteDecision::Send {
            request_keyframe, ..
        } => request_keyframe || !current_selection.policy_allows_delivery(),
        ReceiverRouteDecision::Pause { .. }
        | ReceiverRouteDecision::Hold { .. }
        | ReceiverRouteDecision::Noop => false,
    };
    let mut outcomes = route_outcomes(&route, decision);
    if budget.over_budget_exception_reason().is_some() {
        outcomes = outcomes.with_protected_over_budget();
    }
    Some(ConsumerPacketSelectionUpdate {
        route: input.transport_ref.clone(),
        source_id: input.source_id(),
        selector: fields.selector,
        policy_pause_reason: fields.policy_pause_reason,
        budget,
        outcomes,
        pressure_observations: fields.pressure_observations,
        upgrade_observations: fields.upgrade_observations,
        packet_gate,
        route_activity_update,
        request_keyframe,
    })
}

struct SelectionFields {
    selector: SourceSelector,
    policy_pause_reason: Option<PolicyPauseReason>,
    pressure_observations: u8,
    upgrade_observations: u8,
}

fn selection_fields(
    decision: ReceiverRouteDecision,
    current_selector: SourceSelector,
) -> Option<SelectionFields> {
    match decision {
        ReceiverRouteDecision::Send {
            selector,
            pressure_observations,
            upgrade_observations,
            ..
        } => Some(SelectionFields {
            selector,
            policy_pause_reason: None,
            pressure_observations,
            upgrade_observations,
        }),
        ReceiverRouteDecision::Pause {
            reason,
            pressure_observations,
            upgrade_observations,
        } => Some(SelectionFields {
            selector: current_selector,
            policy_pause_reason: Some(reason),
            pressure_observations,
            upgrade_observations,
        }),
        ReceiverRouteDecision::Hold {
            policy_pause_reason,
            selector,
            pressure_observations,
            upgrade_observations,
        } => Some(SelectionFields {
            selector,
            policy_pause_reason,
            pressure_observations,
            upgrade_observations,
        }),
        ReceiverRouteDecision::Noop => None,
    }
}

fn route_outcomes(
    route: &PlannedReceiverRoute<'_>,
    decision: ReceiverRouteDecision,
) -> BudgetSolverOutcomes {
    match decision {
        ReceiverRouteDecision::Send { .. }
            if route
                .input()
                .current_selection
                .policy_pause_reason()
                .is_some() =>
        {
            BudgetSolverOutcomes::resumed()
        }
        ReceiverRouteDecision::Pause { reason, .. }
            if route.input().current_selection.policy_pause_reason() != Some(reason) =>
        {
            BudgetSolverOutcomes::paused()
        }
        ReceiverRouteDecision::Send { .. }
        | ReceiverRouteDecision::Pause { .. }
        | ReceiverRouteDecision::Hold { .. } => route.outcome().into(),
        ReceiverRouteDecision::Noop => BudgetSolverOutcomes::default(),
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
mod tests {
    #![allow(
        clippy::expect_used,
        reason = "test fixtures should fail loudly when they build invalid source graphs"
    )]

    use o_sfu_router::{MediaKind, Rid};

    use super::*;
    use crate::{
        Bitrate,
        engine::{
            UserId,
            source_model::{
                PublishedSourceDescriptorParts, PublishedSourceId, PublishedSourceOwner,
                SourceEncodingDescriptor, SourceEncodingDescriptorParts, SourceEncodingId,
                SourceOperatingPoint, SourcePolicy, SourceTemporalLayerId, UserStreamId,
            },
        },
    };

    fn source_with_encodings(
        encodings: Vec<SourceEncodingDescriptor>,
    ) -> PublishedSourceDescriptor {
        PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
            source_id: PublishedSourceId::from_raw(7),
            owner: PublishedSourceOwner::new(UserId::Integer(42)),
            stream_id: UserStreamId::new("main-video"),
            media_kind: MediaKind::Video,
            policy: SourcePolicy::hidden(),
            mid: None,
            encodings,
        })
        .expect("test source graph should be valid")
    }

    fn encoding(
        source_id: PublishedSourceId,
        encoding_id: SourceEncodingId,
        rid: Option<&str>,
        max_bitrate: Option<Bitrate>,
    ) -> SourceEncodingDescriptor {
        SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
            encoding_id,
            source_id,
            rid: rid.map(Rid::new),
            primary_ssrc: None,
            repair_ssrc: None,
            max_bitrate,
            resolution_scale: None,
            max_framerate: None,
            policy_role: None,
            max_temporal_layer_id: None,
            negotiated_format: None,
        })
    }

    fn layered_encoding(
        source_id: PublishedSourceId,
        encoding_id: SourceEncodingId,
        rid: Option<&str>,
        max_temporal_layer_id: u8,
    ) -> SourceEncodingDescriptor {
        SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
            encoding_id,
            source_id,
            rid: rid.map(Rid::new),
            primary_ssrc: None,
            repair_ssrc: None,
            max_bitrate: None,
            resolution_scale: None,
            max_framerate: None,
            policy_role: None,
            max_temporal_layer_id: SourceTemporalLayerId::new(max_temporal_layer_id),
            negotiated_format: None,
        })
    }

    #[test]
    fn projects_selected_encoding_to_rid_gate() {
        let source_id = PublishedSourceId::from_raw(7);
        let high_encoding_id = SourceEncodingId::from_raw(1);
        let low_encoding_id = SourceEncodingId::from_raw(2);
        let source = source_with_encodings(vec![
            encoding(
                source_id,
                high_encoding_id,
                Some("hi"),
                Some(Bitrate::from_kbps(750)),
            ),
            encoding(
                source_id,
                low_encoding_id,
                Some("lo"),
                Some(Bitrate::from_kbps(150)),
            ),
        ]);

        let selector = SourceSelector::Encoding(low_encoding_id);

        assert_eq!(
            source_packet_gate_for_selector(&source, selector),
            Ok(SourcePacketGate::Rid(String::from("lo")))
        );
    }

    #[test]
    fn keeps_open_as_an_explicit_transport_gate() {
        let source_id = PublishedSourceId::from_raw(7);
        let source = source_with_encodings(vec![encoding(
            source_id,
            SourceEncodingId::from_raw(1),
            None,
            None,
        )]);

        assert_eq!(
            source_packet_gate_for_selector(&source, SourceSelector::Open),
            Ok(SourcePacketGate::Open)
        );
    }

    #[test]
    fn rejects_ridless_selected_encoding() {
        let source_id = PublishedSourceId::from_raw(7);
        let encoding_id = SourceEncodingId::from_raw(1);
        let source = source_with_encodings(vec![encoding(source_id, encoding_id, None, None)]);

        assert_eq!(
            source_packet_gate_for_selector(&source, SourceSelector::Encoding(encoding_id)),
            Err(SourcePacketGateProjectionError::MissingRid)
        );
    }

    #[test]
    fn projects_operating_point_to_transport_layer_gate() {
        let source_id = PublishedSourceId::from_raw(7);
        let encoding_id = SourceEncodingId::from_raw(1);
        let source = source_with_encodings(vec![layered_encoding(
            source_id,
            encoding_id,
            Some("hi"),
            2,
        )]);
        let temporal_layer = SourceTemporalLayerId::new(1)
            .expect("test temporal layer should fit the RFC 9626 TID range");

        assert_eq!(
            source_packet_gate_for_selector(
                &source,
                SourceSelector::OperatingPoint(SourceOperatingPoint::new(
                    encoding_id,
                    temporal_layer,
                ))
            ),
            Ok(SourcePacketGate::OperatingPoint(
                SourcePacketOperatingPoint::new(Some(String::from("hi")), 1)
            ))
        );
    }

    #[test]
    fn rejects_operating_points_without_advertised_temporal_metadata() {
        let source_id = PublishedSourceId::from_raw(7);
        let encoding_id = SourceEncodingId::from_raw(1);
        let source =
            source_with_encodings(vec![encoding(source_id, encoding_id, Some("hi"), None)]);
        let temporal_layer = SourceTemporalLayerId::base();

        assert_eq!(
            source_packet_gate_for_selector(
                &source,
                SourceSelector::OperatingPoint(SourceOperatingPoint::new(
                    encoding_id,
                    temporal_layer,
                ))
            ),
            Err(SourcePacketGateProjectionError::MissingTemporalMetadata)
        );
    }

    #[test]
    fn projects_advertised_base_layer_operating_point() {
        let source_id = PublishedSourceId::from_raw(7);
        let encoding_id = SourceEncodingId::from_raw(1);
        let source = source_with_encodings(vec![layered_encoding(
            source_id,
            encoding_id,
            Some("hi"),
            0,
        )]);
        let temporal_layer = SourceTemporalLayerId::base();

        assert_eq!(
            source_packet_gate_for_selector(
                &source,
                SourceSelector::OperatingPoint(SourceOperatingPoint::new(
                    encoding_id,
                    temporal_layer,
                ))
            ),
            Ok(SourcePacketGate::OperatingPoint(
                SourcePacketOperatingPoint::new(Some(String::from("hi")), 0)
            ))
        );
    }

    #[test]
    fn rejects_operating_points_above_advertised_layer() {
        let source_id = PublishedSourceId::from_raw(7);
        let encoding_id = SourceEncodingId::from_raw(1);
        let source = source_with_encodings(vec![layered_encoding(source_id, encoding_id, None, 1)]);
        let temporal_layer = SourceTemporalLayerId::new(2)
            .expect("test temporal layer should fit the RFC 9626 TID range");

        assert_eq!(
            source_packet_gate_for_selector(
                &source,
                SourceSelector::OperatingPoint(SourceOperatingPoint::new(
                    encoding_id,
                    temporal_layer,
                ))
            ),
            Err(SourcePacketGateProjectionError::TemporalLayerExceedsAdvertised)
        );
    }
}
