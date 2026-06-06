//! receiver video policy orchestration
//!
//! the planner groups receiver routes and delegates each policy stage to a
//! small pure module before returning effect-bound source-policy updates

use super::{
    super::action::{ConsumerPacketSelectionUpdate, ReceiverVideoPolicyPlan},
    adaptation::{self, ConsumerAdaptationPlan},
    admission, budget, hysteresis,
    input::{ReceiverVideoPolicyInput, ReceiverVideoRouteInput},
    projection,
};
use crate::{
    Bitrate,
    engine::{
        media_transport::{ActiveSpeakerSource, ReceiverBandwidthSnapshot},
        room::state::RoomState,
        source_model::{PolicyPauseReason, SourceSelector},
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReceiverRouteDecision {
    Send {
        selector: SourceSelector,
        pressure_observations: u8,
        upgrade_observations: u8,
        request_keyframe: bool,
    },
    Pause {
        reason: PolicyPauseReason,
        pressure_observations: u8,
        upgrade_observations: u8,
    },
    Hold {
        policy_pause_reason: Option<PolicyPauseReason>,
        selector: SourceSelector,
        pressure_observations: u8,
        upgrade_observations: u8,
    },
    Noop,
}

impl ReceiverRouteDecision {
    pub(super) const fn sends_media(self) -> bool {
        matches!(self, Self::Send { .. })
    }

    const fn request_keyframe(self) -> bool {
        match self {
            Self::Send {
                request_keyframe, ..
            } => request_keyframe,
            Self::Pause { .. } | Self::Hold { .. } | Self::Noop => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RouteOutcome {
    Neutral,
    Degraded,
    Paused,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PlannedReceiverRoute<'a> {
    route: &'a ReceiverVideoRouteInput<'a>,
    selected_bitrate: Bitrate,
    decision: ReceiverRouteDecision,
    outcome: RouteOutcome,
}

impl<'a> PlannedReceiverRoute<'a> {
    fn new(route: &'a ReceiverVideoRouteInput<'a>, adaptation: ConsumerAdaptationPlan) -> Self {
        let selected_bitrate = adaptation::selector_bitrate(route.encodings(), adaptation.selector);
        let current_bitrate =
            adaptation::selector_bitrate(route.encodings(), route.current_selection.selector());
        let outcome = if selected_bitrate < current_bitrate {
            RouteOutcome::Degraded
        } else {
            RouteOutcome::Neutral
        };
        Self {
            route,
            selected_bitrate,
            decision: ReceiverRouteDecision::Send {
                selector: adaptation.selector,
                pressure_observations: adaptation.pressure_observations,
                upgrade_observations: adaptation.upgrade_observations,
                request_keyframe: adaptation.request_keyframe,
            },
            outcome,
        }
    }

    pub(super) const fn input(&self) -> &'a ReceiverVideoRouteInput<'a> {
        self.route
    }

    pub(super) const fn selected_bitrate(&self) -> Bitrate {
        self.selected_bitrate
    }

    pub(super) const fn decision(&self) -> ReceiverRouteDecision {
        self.decision
    }

    pub(super) const fn outcome(&self) -> RouteOutcome {
        self.outcome
    }

    pub(super) fn send(
        &mut self,
        selector: SourceSelector,
        selected_bitrate: Bitrate,
        outcome: RouteOutcome,
    ) {
        self.selected_bitrate = selected_bitrate;
        self.decision = ReceiverRouteDecision::Send {
            selector,
            pressure_observations: self.pressure_observations(),
            upgrade_observations: self.upgrade_observations(),
            request_keyframe: self.decision.request_keyframe(),
        };
        self.outcome = outcome;
    }

    pub(super) fn pause(&mut self, reason: PolicyPauseReason, outcome: RouteOutcome) {
        self.selected_bitrate = Bitrate::zero();
        self.decision = ReceiverRouteDecision::Pause {
            reason,
            pressure_observations: self.pressure_observations(),
            upgrade_observations: self.upgrade_observations(),
        };
        self.outcome = outcome;
    }

    const fn pressure_observations(self) -> u8 {
        match self.decision {
            ReceiverRouteDecision::Send {
                pressure_observations,
                ..
            }
            | ReceiverRouteDecision::Pause {
                pressure_observations,
                ..
            }
            | ReceiverRouteDecision::Hold {
                pressure_observations,
                ..
            } => pressure_observations,
            ReceiverRouteDecision::Noop => 0,
        }
    }

    const fn upgrade_observations(self) -> u8 {
        match self.decision {
            ReceiverRouteDecision::Send {
                upgrade_observations,
                ..
            }
            | ReceiverRouteDecision::Pause {
                upgrade_observations,
                ..
            }
            | ReceiverRouteDecision::Hold {
                upgrade_observations,
                ..
            } => upgrade_observations,
            ReceiverRouteDecision::Noop => 0,
        }
    }
}

#[derive(Debug)]
pub(super) struct PlannedReceiverRoutes {
    pub selection_updates: Vec<ConsumerPacketSelectionUpdate>,
    pub receiver_bwe_target: Bitrate,
}

impl RoomState {
    /// Plans deterministic per-consumer source selectors for live video routes.
    ///
    /// The snapshot inputs are best-effort transport observations. They do not
    /// change room authority on their own. This method combines them with
    /// committed source descriptors, subscription state and active-speaker
    /// layout state to build staged updates for the effect executor.
    pub fn receiver_video_policy_plan(
        &self,
        ranked_active_speaker_sources: &[ActiveSpeakerSource],
        receiver_bandwidth_snapshot: &ReceiverBandwidthSnapshot,
    ) -> ReceiverVideoPolicyPlan {
        let input = ReceiverVideoPolicyInput::from_state(
            self,
            ranked_active_speaker_sources,
            receiver_bandwidth_snapshot,
        );
        receiver_video_selection_plan(input)
    }
}

fn receiver_video_selection_plan(input: ReceiverVideoPolicyInput<'_>) -> ReceiverVideoPolicyPlan {
    let ReceiverVideoPolicyInput {
        routes,
        mut receiver_bwe_targets,
        max_video_downloads_per_receiver,
    } = input;
    let mut selection_updates = Vec::with_capacity(routes.len());
    for receiver_routes in
        routes.chunk_by(|left, right| left.consumer_user_id() == right.consumer_user_id())
    {
        let Some(first_route) = receiver_routes.first() else {
            continue;
        };
        let consumer_user_id = first_route.consumer_user_id();
        let plan = plan_receiver_routes(receiver_routes, max_video_downloads_per_receiver);
        if let Some(target) = receiver_bwe_targets.get_mut(consumer_user_id) {
            target.set_target(plan.receiver_bwe_target);
        }
        selection_updates.extend(plan.selection_updates);
    }
    ReceiverVideoPolicyPlan {
        consumer_packet_updates: selection_updates,
        receiver_bwe_targets: receiver_bwe_targets.into_values().collect(),
    }
}

fn plan_receiver_routes<'a>(
    routes: &'a [ReceiverVideoRouteInput<'a>],
    max_video_downloads_per_receiver: usize,
) -> PlannedReceiverRoutes {
    let receiver_bandwidth = routes.iter().find_map(|route| route.receiver_bandwidth);
    let mut planned_routes = routes
        .iter()
        .filter_map(|route| {
            let adaptation = adaptation::route_plan(route)?;
            Some(PlannedReceiverRoute::new(route, adaptation))
        })
        .collect::<Vec<_>>();
    admission::apply_video_download_limit(&mut planned_routes, max_video_downloads_per_receiver);
    if let Some(receiver_bandwidth) = receiver_bandwidth {
        budget::apply_overload_policy(&mut planned_routes, receiver_bandwidth);
    }
    let diagnostics = budget::diagnostics(&planned_routes, receiver_bandwidth);
    let receiver_bwe_target = diagnostics.selected_video_bitrate();
    let selection_updates = planned_routes
        .into_iter()
        .filter_map(|route| {
            let decision = hysteresis::resolve(&route);
            projection::consumer_packet_selection_update(route, decision, diagnostics)
        })
        .collect();
    PlannedReceiverRoutes {
        selection_updates,
        receiver_bwe_target,
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        reason = "test fixtures should fail loudly when they build invalid source graphs"
    )]

    use std::slice;

    use super::{
        super::{fixtures, hysteresis},
        *,
    };
    use crate::{
        Bitrate,
        engine::{
            ConnectionId, UserId,
            media_transport::SourcePacketGate,
            room::source_policy::ReceiverBweTargetPlan,
            source_model::{
                ConsumerSourceSelection, PolicyPauseReason, PublishedSourceDescriptor,
                PublishedSourceId, SourceEncodingId, SourceModelError, SourceRoomPolicySelector,
                SourceSelector, UploadLayerPolicyRole,
            },
        },
    };

    fn two_layer_source() -> Result<PublishedSourceDescriptor, SourceModelError> {
        let source_id = PublishedSourceId::from_raw(7);
        fixtures::scalable_source(vec![
            fixtures::bitrate_encoding(
                source_id,
                SourceEncodingId::from_raw(1),
                "lo",
                Bitrate::from_kbps(150),
                UploadLayerPolicyRole::Thumbnail,
            ),
            fixtures::bitrate_encoding(
                source_id,
                SourceEncodingId::from_raw(2),
                "hi",
                Bitrate::from_kbps(900),
                UploadLayerPolicyRole::Featured,
            ),
        ])
    }

    fn single_layer_source() -> Result<PublishedSourceDescriptor, SourceModelError> {
        let source_id = PublishedSourceId::from_raw(7);
        fixtures::scalable_source(vec![fixtures::bitrate_encoding(
            source_id,
            SourceEncodingId::from_raw(1),
            "hi",
            Bitrate::from_kbps(900),
            UploadLayerPolicyRole::Featured,
        )])
    }

    fn first_update(plan: &PlannedReceiverRoutes) -> &ConsumerPacketSelectionUpdate {
        plan.selection_updates
            .first()
            .expect("policy plan should emit an update")
    }

    #[test]
    fn receiver_budget_uses_policy_role_order_when_bitrates_are_absent()
    -> Result<(), SourceModelError> {
        let source_id = PublishedSourceId::from_raw(7);
        let high_encoding_id = SourceEncodingId::from_raw(1);
        let low_encoding_id = SourceEncodingId::from_raw(2);
        let source = fixtures::scalable_source(vec![
            fixtures::role_encoding(
                source_id,
                high_encoding_id,
                "hi",
                UploadLayerPolicyRole::Featured,
            ),
            fixtures::role_encoding(
                source_id,
                low_encoding_id,
                "lo",
                UploadLayerPolicyRole::Thumbnail,
            ),
        ])?;
        let mut selection = ConsumerSourceSelection::open(true);
        selection.set_selector(SourceSelector::Encoding(high_encoding_id));
        let route = fixtures::route(&source, selection);

        let plan = plan_receiver_routes(&[route], usize::MAX);

        assert_eq!(plan.selection_updates.len(), 1);
        let update = first_update(&plan);
        assert_eq!(update.selector, SourceSelector::Encoding(low_encoding_id));
        assert_eq!(
            update.packet_gate.as_ref(),
            Some(&SourcePacketGate::Rid("lo".into()))
        );
        assert!(update.request_keyframe);
        Ok(())
    }

    #[test]
    fn video_download_limit_pause_is_immediate() -> Result<(), SourceModelError> {
        let visible_source_id = PublishedSourceId::from_raw(7);
        let hidden_source_id = PublishedSourceId::from_raw(8);
        let visible_source = fixtures::scalable_source_with(
            visible_source_id,
            UserId::Integer(41),
            SourceRoomPolicySelector::VisibleThumbnail,
            vec![fixtures::ridless_encoding(
                visible_source_id,
                SourceEncodingId::from_raw(1),
            )],
        )?;
        let hidden_source = fixtures::scalable_source_with(
            hidden_source_id,
            UserId::Integer(43),
            SourceRoomPolicySelector::Hidden,
            vec![fixtures::ridless_encoding(
                hidden_source_id,
                SourceEncodingId::from_raw(1),
            )],
        )?;
        let routes = [
            fixtures::route_with_layout(
                &visible_source,
                ConsumerSourceSelection::open(true),
                SourceRoomPolicySelector::VisibleThumbnail,
            ),
            fixtures::route_with_layout(
                &hidden_source,
                ConsumerSourceSelection::open(true),
                SourceRoomPolicySelector::Hidden,
            ),
        ];

        let plan = plan_receiver_routes(&routes, 1);
        let hidden_update = plan
            .selection_updates
            .iter()
            .find(|update| update.source_id == hidden_source_id)
            .expect("download cap should pause the RID-less overflow route");

        assert_eq!(
            hidden_update.policy_pause_reason,
            Some(PolicyPauseReason::VideoDownloadLimit)
        );
        assert!(hidden_update.route_activity_update);
        assert!(hidden_update.outcomes.is_paused());
        assert_eq!(hidden_update.pressure_observations, 0);
        assert_eq!(hidden_update.upgrade_observations, 0);
        assert_eq!(hidden_update.budget.active_video_route_count(), 1);
        Ok(())
    }

    #[test]
    fn receiver_bwe_targets_follow_room_size_quality_policy() -> Result<(), SourceModelError> {
        let source = two_layer_source()?;
        let mut route = fixtures::route(&source, ConsumerSourceSelection::open(true));
        route.user_count = 2;

        let plan = plan_receiver_routes(&[route], usize::MAX);

        assert_eq!(plan.receiver_bwe_target, Bitrate::from_kbps(900));
        let route = fixtures::route(&source, ConsumerSourceSelection::open(true));

        let plan = plan_receiver_routes(&[route], usize::MAX);

        assert_eq!(plan.receiver_bwe_target, Bitrate::from_kbps(150));
        Ok(())
    }

    #[test]
    fn receiver_without_video_routes_gets_zero_bwe_target() {
        let input = ReceiverVideoPolicyInput {
            routes: Vec::new(),
            receiver_bwe_targets: [(
                UserId::Integer(42),
                ReceiverBweTargetPlan::new(
                    UserId::Integer(42),
                    ConnectionId::from_raw(10),
                    Bitrate::zero(),
                ),
            )]
            .into(),
            max_video_downloads_per_receiver: usize::MAX,
        };

        let plan = receiver_video_selection_plan(input);

        assert_eq!(plan.receiver_bwe_targets.len(), 1);
        let target = plan
            .receiver_bwe_targets
            .first()
            .expect("plan should keep the seeded receiver BWE target");
        assert_eq!(target.target(), Bitrate::zero());
    }

    #[test]
    fn protected_over_budget_route_keeps_selected_bwe_target() -> Result<(), SourceModelError> {
        let source = two_layer_source()?;
        let mut route = fixtures::route_with_layout(
            &source,
            ConsumerSourceSelection::open(true),
            SourceRoomPolicySelector::Pinned,
        );
        route.receiver_bandwidth = Some(Bitrate::from_kbps(100));

        let plan = plan_receiver_routes(&[route], usize::MAX);

        assert_eq!(plan.receiver_bwe_target, Bitrate::from_kbps(900));
        assert!(first_update(&plan).outcomes.is_protected_over_budget());
        Ok(())
    }

    #[test]
    fn budget_pressure_pause_needs_two_pressure_observations() -> Result<(), SourceModelError> {
        let source = single_layer_source()?;
        let mut route = fixtures::route(&source, ConsumerSourceSelection::open(true));
        route.receiver_bandwidth = Some(Bitrate::from_kbps(100));

        let first_plan = plan_receiver_routes(slice::from_ref(&route), usize::MAX);
        let first = first_update(&first_plan);

        assert_eq!(first.policy_pause_reason, None);
        assert!(!first.route_activity_update);
        assert_eq!(first.pressure_observations, 1);

        let mut pressured_selection = ConsumerSourceSelection::open(true);
        pressured_selection.set_adaptation_observations(
            hysteresis::DOWNSWITCH_PRESSURE_OBSERVATIONS.saturating_sub(1),
            0,
        );
        route.current_selection = pressured_selection;

        let second_plan = plan_receiver_routes(&[route], usize::MAX);
        let second = first_update(&second_plan);

        assert_eq!(
            second.policy_pause_reason,
            Some(PolicyPauseReason::BudgetPressure)
        );
        assert!(second.route_activity_update);
        assert_eq!(second.pressure_observations, 0);
        Ok(())
    }

    #[test]
    fn downswitch_needs_two_pressure_observations() -> Result<(), SourceModelError> {
        let low_encoding_id = SourceEncodingId::from_raw(1);
        let high_encoding_id = SourceEncodingId::from_raw(2);
        let source = two_layer_source()?;
        let mut selection = ConsumerSourceSelection::open(true);
        selection.set_selector(SourceSelector::Encoding(high_encoding_id));
        let mut route = fixtures::route(&source, selection);
        route.visible_scalable_route_count = 4;
        route.receiver_bandwidth = Some(Bitrate::from_kbps(1000));

        let first_plan = plan_receiver_routes(slice::from_ref(&route), usize::MAX);
        let first = first_update(&first_plan);

        assert_eq!(first.selector, SourceSelector::Encoding(high_encoding_id));
        assert_eq!(first.packet_gate, None);
        assert_eq!(first.pressure_observations, 1);

        let mut pressured_selection = selection;
        pressured_selection.set_adaptation_observations(
            hysteresis::DOWNSWITCH_PRESSURE_OBSERVATIONS.saturating_sub(1),
            0,
        );
        route.current_selection = pressured_selection;

        let second_plan = plan_receiver_routes(&[route], usize::MAX);
        let second_update = first_update(&second_plan);

        assert_eq!(
            second_update.selector,
            SourceSelector::Encoding(low_encoding_id)
        );
        assert_eq!(
            second_update.packet_gate.as_ref(),
            Some(&SourcePacketGate::Rid("lo".into()))
        );
        assert!(second_update.request_keyframe);
        assert_eq!(second_update.pressure_observations, 0);
        Ok(())
    }

    #[test]
    fn upswitch_needs_three_stable_observations() -> Result<(), SourceModelError> {
        let low_encoding_id = SourceEncodingId::from_raw(1);
        let high_encoding_id = SourceEncodingId::from_raw(2);
        let source = two_layer_source()?;
        let mut selection = ConsumerSourceSelection::open(true);
        selection.set_selector(SourceSelector::Encoding(low_encoding_id));
        let mut route = fixtures::route(&source, selection);
        route.user_count = 2;
        route.receiver_bandwidth = Some(Bitrate::from_kbps(1000));

        let first_plan = plan_receiver_routes(slice::from_ref(&route), usize::MAX);
        let first = first_update(&first_plan);

        assert_eq!(first.selector, SourceSelector::Encoding(low_encoding_id));
        assert_eq!(first.packet_gate, None);
        assert_eq!(first.upgrade_observations, 1);

        let mut stable_selection = selection;
        stable_selection.set_adaptation_observations(
            0,
            hysteresis::UPSWITCH_STABLE_OBSERVATIONS.saturating_sub(1),
        );
        route.current_selection = stable_selection;

        let final_plan = plan_receiver_routes(&[route], usize::MAX);
        let final_update = first_update(&final_plan);

        assert_eq!(
            final_update.selector,
            SourceSelector::Encoding(high_encoding_id)
        );
        assert_eq!(
            final_update.packet_gate.as_ref(),
            Some(&SourcePacketGate::Rid("hi".into()))
        );
        assert!(final_update.request_keyframe);
        assert_eq!(final_update.upgrade_observations, 0);
        Ok(())
    }

    #[test]
    fn paused_route_resume_needs_three_stable_observations() -> Result<(), SourceModelError> {
        let source = single_layer_source()?;
        let mut selection = ConsumerSourceSelection::open(true);
        selection.set_policy_pause_reason(Some(PolicyPauseReason::BudgetPressure));
        let mut route = fixtures::route(&source, selection);

        let first_plan = plan_receiver_routes(slice::from_ref(&route), usize::MAX);
        let first = first_update(&first_plan);

        assert_eq!(
            first.policy_pause_reason,
            Some(PolicyPauseReason::BudgetPressure)
        );
        assert!(!first.route_activity_update);
        assert!(!first.request_keyframe);
        assert_eq!(first.upgrade_observations, 1);

        let mut stable_selection = selection;
        stable_selection.set_adaptation_observations(
            0,
            hysteresis::UPSWITCH_STABLE_OBSERVATIONS.saturating_sub(1),
        );
        route.current_selection = stable_selection;

        let final_plan = plan_receiver_routes(&[route], usize::MAX);
        let final_update = first_update(&final_plan);

        assert_eq!(final_update.policy_pause_reason, None);
        assert!(final_update.route_activity_update);
        assert!(final_update.request_keyframe);
        assert!(final_update.outcomes.is_resumed());
        assert_eq!(final_update.upgrade_observations, 0);
        Ok(())
    }

    #[test]
    fn budget_update_emits_once_then_suppresses_noop_route() -> Result<(), SourceModelError> {
        let source = single_layer_source()?;
        let mut route = fixtures::route(&source, ConsumerSourceSelection::open(true));
        route.receiver_bandwidth = Some(Bitrate::from_kbps(1000));
        let plan = plan_receiver_routes(slice::from_ref(&route), usize::MAX);
        let update = first_update(&plan);

        assert_eq!(update.selector, SourceSelector::Open);
        assert_eq!(update.packet_gate, None);
        assert!(!update.route_activity_update);
        assert_eq!(
            update.budget.selected_video_bitrate(),
            Bitrate::from_kbps(900)
        );

        let mut selection = ConsumerSourceSelection::open(true);
        selection.set_budget(update.budget);
        route.current_selection = selection;

        let next_plan = plan_receiver_routes(&[route], usize::MAX);

        assert!(next_plan.selection_updates.is_empty());
        Ok(())
    }
}
