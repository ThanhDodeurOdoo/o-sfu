//! Pure receiver video budget planner.
//!
//! The planner first chooses the useful encoding for each receiver/source route,
//! then solves the receiver's selected video set against the live bandwidth
//! estimate. Overload is expressed as semantic route pauses so the transport
//! withholds whole routes instead of randomly dropping packets.
//!
//! # Source policy
//!
//! The planner does not know product stream names. A source participates in
//! receiver BWE control because its descriptor carries
//! [`SourceAdaptationPolicy::ScalableVideo`], and a readable-detail source is
//! protected because its descriptor carries
//! [`SourceAdaptationPolicy::ReadableDetail`]. Orchestration changes those
//! decisions before publish by building a different source policy.

use super::{
    action::{
        BudgetSolverOutcomes, ConsumerPacketSelectionUpdate, ReceiverVideoRouteAction,
        VideoRouteAction,
    },
    input::{ReceiverVideoPolicyInput, ReceiverVideoRouteInput, SelectableRouteEncodings},
};
use crate::{
    Bitrate,
    runtime::{
        media_transport::{ActiveSpeakerSource, ReceiverBandwidthSnapshot},
        source_model::{
            ConsumerSourceSelection, OverBudgetExceptionReason, PolicyPauseReason,
            ReceiverVideoBudgetDiagnostics, SourceAdaptationPolicy, SourceEncodingDescriptor,
            SourceRoomPolicySelector, SourceRoutePriority, SourceSelector, UploadLayerPolicyRole,
        },
    },
};

/// Minimum room size where scalable-video adaptation starts constraining receivers.
const MULTIPARTY_SCALABLE_VIDEO_SELECTION_THRESHOLD: usize = 3;

/// Number of policy refreshes that must agree before a lower encoding is committed.
const DOWNSWITCH_PRESSURE_OBSERVATIONS: u8 = 2;

/// Number of policy refreshes that must agree before a higher encoding is committed.
const UPSWITCH_STABLE_OBSERVATIONS: u8 = 3;

/// Extra conservatism applied after thumbnail budget is split across visible videos.
const THUMBNAIL_BUDGET_DIVISOR: u64 = 2;

/// Pure output of one receiver video policy refresh.
#[derive(Debug)]
pub(in crate::runtime::room) struct ReceiverVideoBudgetPlan<'a> {
    route_actions: Vec<ReceiverVideoRouteAction<'a>>,
}

impl<'a> ReceiverVideoBudgetPlan<'a> {
    #[must_use]
    pub(in crate::runtime::room) fn from_input(input: &'a ReceiverVideoPolicyInput<'a>) -> Self {
        let routes = input.routes();
        let mut route_actions = Vec::with_capacity(routes.len());
        let mut remaining_routes = routes;
        while let Some((first_route, rest)) = remaining_routes.split_first() {
            let consumer_user_id = first_route.consumer_user_id();
            let group_len = 1 + rest
                .iter()
                .take_while(|route| route.consumer_user_id() == consumer_user_id)
                .count();
            let Some((receiver_routes, next_routes)) = remaining_routes.split_at_checked(group_len)
            else {
                break;
            };
            route_actions.extend(plan_receiver_routes(receiver_routes));
            remaining_routes = next_routes;
        }
        Self { route_actions }
    }

    #[must_use]
    pub(in crate::runtime::room) fn into_selection_updates(
        self,
    ) -> Vec<ConsumerPacketSelectionUpdate> {
        self.route_actions
            .into_iter()
            .filter_map(ReceiverVideoRouteAction::into_selection_update)
            .collect()
    }
}

/// Selector decision plus refresh-count hysteresis for one route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConsumerAdaptationPlan {
    selector: SourceSelector,
    pressure_observations: u8,
    upgrade_observations: u8,
    request_keyframe: bool,
}

#[derive(Debug, Clone, Copy)]
struct PlannedReceiverRoute<'a> {
    route: &'a ReceiverVideoRouteInput<'a>,
    adaptation: ConsumerAdaptationPlan,
    selected_bitrate: Bitrate,
    action: VideoRouteAction,
    outcomes: BudgetSolverOutcomes,
}

impl super::super::shared::RoomState {
    /// Plans deterministic per-consumer source selectors for live video routes.
    ///
    /// The snapshot inputs are best-effort transport observations. They do not
    /// change room authority on their own. This method combines them with
    /// committed source descriptors, subscription state and active-speaker
    /// layout state to build staged updates for the effect executor.
    pub(in crate::runtime::room) fn consumer_packet_selection_updates(
        &self,
        active_speaker_sources: &[ActiveSpeakerSource],
        receiver_bandwidth_snapshot: &ReceiverBandwidthSnapshot,
    ) -> Vec<ConsumerPacketSelectionUpdate> {
        let input = ReceiverVideoPolicyInput::from_state(
            self,
            active_speaker_sources,
            receiver_bandwidth_snapshot,
        );
        ReceiverVideoBudgetPlan::from_input(&input).into_selection_updates()
    }
}

fn plan_receiver_routes<'a>(
    routes: &'a [ReceiverVideoRouteInput<'a>],
) -> Vec<ReceiverVideoRouteAction<'a>> {
    let Some(receiver_bandwidth) = routes
        .iter()
        .find_map(ReceiverVideoRouteInput::receiver_bandwidth)
    else {
        let planned_routes = routes
            .iter()
            .filter_map(|route| {
                let adaptation = consumer_route_adaptation_plan(route)?;
                Some(PlannedReceiverRoute {
                    route,
                    selected_bitrate: selector_bitrate(route.encodings(), adaptation.selector),
                    action: VideoRouteAction::Send(adaptation.selector),
                    adaptation,
                    outcomes: adaptation_outcomes(
                        route.encodings(),
                        route.current_selection(),
                        adaptation.selector,
                    ),
                })
            })
            .collect::<Vec<_>>();
        let budget = receiver_budget_diagnostics(&planned_routes, None);
        return planned_routes
            .into_iter()
            .filter_map(|route| planned_route_action(route, budget))
            .collect();
    };
    let mut planned_routes = routes
        .iter()
        .filter_map(|route| {
            let adaptation = consumer_route_adaptation_plan(route)?;
            let selected_bitrate = selector_bitrate(route.encodings(), adaptation.selector);
            let outcomes = adaptation_outcomes(
                route.encodings(),
                route.current_selection(),
                adaptation.selector,
            );
            Some(PlannedReceiverRoute {
                route,
                adaptation,
                selected_bitrate,
                action: VideoRouteAction::Send(adaptation.selector),
                outcomes,
            })
        })
        .collect::<Vec<_>>();
    apply_receiver_overload_policy(&mut planned_routes, receiver_bandwidth);
    let budget = receiver_budget_diagnostics(&planned_routes, Some(receiver_bandwidth));
    planned_routes
        .into_iter()
        .filter_map(|route| planned_route_action(route, budget))
        .collect()
}

fn consumer_route_adaptation_plan(
    route: &ReceiverVideoRouteInput<'_>,
) -> Option<ConsumerAdaptationPlan> {
    consumer_adaptation_plan(
        route.user_count(),
        route.adaptation_policy(),
        route.encodings(),
        route.current_selection(),
        route.layout_intent().uses_featured_quality(),
        route.visible_scalable_route_count(),
        route.receiver_bandwidth(),
    )
}

fn apply_receiver_overload_policy(
    planned_routes: &mut [PlannedReceiverRoute<'_>],
    receiver_bandwidth: Bitrate,
) {
    let mut selected_bitrate = selected_receiver_bitrate(planned_routes);
    if selected_bitrate <= receiver_bandwidth {
        return;
    }
    for route in planned_routes
        .iter_mut()
        .filter(|route| route_can_downgrade(route))
    {
        let Some((selector, bitrate)) = cheapest_useful_encoding(route.route.encodings()) else {
            route.action = VideoRouteAction::Pause(PolicyPauseReason::MissingUsableLayer);
            selected_bitrate = selected_bitrate.saturating_sub(route.selected_bitrate);
            route.selected_bitrate = Bitrate::zero();
            continue;
        };
        if bitrate < route.selected_bitrate {
            selected_bitrate = selected_bitrate
                .saturating_sub(route.selected_bitrate)
                .saturating_add(bitrate);
            route.selected_bitrate = bitrate;
            route.action = VideoRouteAction::Send(selector);
            route.outcomes = BudgetSolverOutcomes::degraded();
        }
    }
    if selected_bitrate <= receiver_bandwidth {
        return;
    }
    let mut pause_order = planned_routes
        .iter()
        .enumerate()
        .filter(|(_index, route)| !route_is_protected(route))
        .map(|(index, route)| (pause_rank(route), index))
        .collect::<Vec<_>>();
    pause_order.sort_by_key(|(rank, _index)| *rank);
    for (_rank, index) in pause_order {
        let Some(route) = planned_routes.get_mut(index) else {
            continue;
        };
        if selected_bitrate <= receiver_bandwidth {
            break;
        }
        let pause_reason = pause_reason_for_route(route);
        route.action = VideoRouteAction::Pause(pause_reason);
        route.outcomes = BudgetSolverOutcomes::paused();
        selected_bitrate = selected_bitrate.saturating_sub(route.selected_bitrate);
        route.selected_bitrate = Bitrate::zero();
    }
}

fn planned_route_action(
    route: PlannedReceiverRoute<'_>,
    budget: ReceiverVideoBudgetDiagnostics,
) -> Option<ReceiverVideoRouteAction<'_>> {
    let current = route.route.current_selection();
    let outcomes = budget_outcomes(route.outcomes, budget);
    match route.action {
        VideoRouteAction::Send(selector) if current.policy_pause_reason().is_some() => {
            let upgrade_observations = current
                .upgrade_observations()
                .saturating_add(1)
                .min(UPSWITCH_STABLE_OBSERVATIONS);
            if upgrade_observations >= UPSWITCH_STABLE_OBSERVATIONS {
                Some(ReceiverVideoRouteAction::new(
                    route.route,
                    VideoRouteAction::Send(selector),
                    budget,
                    budget_outcomes(BudgetSolverOutcomes::resumed(), budget),
                    0,
                    0,
                    true,
                ))
            } else {
                Some(ReceiverVideoRouteAction::new(
                    route.route,
                    VideoRouteAction::Pause(current.policy_pause_reason()?),
                    budget,
                    outcomes,
                    0,
                    upgrade_observations,
                    false,
                ))
            }
        }
        VideoRouteAction::Pause(reason) if current.policy_pause_reason() != Some(reason) => {
            let pressure_observations = current
                .pressure_observations()
                .saturating_add(1)
                .min(DOWNSWITCH_PRESSURE_OBSERVATIONS);
            if pressure_observations >= DOWNSWITCH_PRESSURE_OBSERVATIONS {
                Some(ReceiverVideoRouteAction::new(
                    route.route,
                    VideoRouteAction::Pause(reason),
                    budget,
                    budget_outcomes(BudgetSolverOutcomes::paused(), budget),
                    0,
                    0,
                    false,
                ))
            } else {
                Some(ReceiverVideoRouteAction::new(
                    route.route,
                    VideoRouteAction::Send(current.selector()),
                    budget,
                    outcomes,
                    pressure_observations,
                    0,
                    false,
                ))
            }
        }
        _ => Some(ReceiverVideoRouteAction::new(
            route.route,
            route.action,
            budget,
            outcomes,
            route.adaptation.pressure_observations,
            route.adaptation.upgrade_observations,
            route.adaptation.request_keyframe,
        )),
    }
}

fn budget_outcomes(
    outcomes: BudgetSolverOutcomes,
    budget: ReceiverVideoBudgetDiagnostics,
) -> BudgetSolverOutcomes {
    if budget.over_budget_exception_reason().is_some() {
        outcomes.with_protected_over_budget()
    } else {
        outcomes
    }
}

fn receiver_budget_diagnostics(
    planned_routes: &[PlannedReceiverRoute<'_>],
    receiver_bandwidth: Option<Bitrate>,
) -> ReceiverVideoBudgetDiagnostics {
    let selected_video_bitrate = selected_receiver_bitrate(planned_routes);
    let over_budget_exception_reason = receiver_bandwidth
        .filter(|budget| selected_video_bitrate > *budget)
        .map(|_budget| OverBudgetExceptionReason::ProtectedRoute);
    ReceiverVideoBudgetDiagnostics::new(
        receiver_bandwidth,
        receiver_bandwidth,
        active_video_route_count(planned_routes),
        selected_video_bitrate,
        over_budget_exception_reason,
    )
}

fn active_video_route_count(planned_routes: &[PlannedReceiverRoute<'_>]) -> usize {
    planned_routes
        .iter()
        .filter(|route| matches!(route.action, VideoRouteAction::Send(_)))
        .count()
}

fn adaptation_outcomes(
    encodings: SelectableRouteEncodings<'_>,
    current: ConsumerSourceSelection,
    selector: SourceSelector,
) -> BudgetSolverOutcomes {
    let current_bitrate = selector_bitrate(encodings, current.selector());
    let selected_bitrate = selector_bitrate(encodings, selector);
    if selected_bitrate < current_bitrate {
        BudgetSolverOutcomes::degraded()
    } else {
        BudgetSolverOutcomes::default()
    }
}

fn consumer_adaptation_plan(
    user_count: usize,
    adaptation_policy: SourceAdaptationPolicy,
    encodings: SelectableRouteEncodings<'_>,
    current: ConsumerSourceSelection,
    featured: bool,
    visible_scalable_route_count: usize,
    receiver_bandwidth: Option<Bitrate>,
) -> Option<ConsumerAdaptationPlan> {
    match adaptation_policy {
        SourceAdaptationPolicy::ReadableDetail => {
            return readable_detail_adaptation_plan(encodings, current);
        }
        SourceAdaptationPolicy::None => return None,
        SourceAdaptationPolicy::ScalableVideo => {}
    }
    if encodings.len() < 2 {
        return None;
    }
    let current_index = selector_index(current.selector(), encodings);
    let target_index = desired_encoding_index(
        user_count,
        featured,
        visible_scalable_route_count,
        receiver_bandwidth,
        encodings,
    );
    let target_selector = SourceSelector::Encoding(encodings.get(target_index)?.encoding_id());
    let selector_changed = target_selector != current.selector();
    if target_index == current_index {
        return Some(ConsumerAdaptationPlan {
            selector: target_selector,
            pressure_observations: 0,
            upgrade_observations: 0,
            request_keyframe: selector_changed,
        });
    }
    if receiver_bandwidth.is_none() {
        return Some(ConsumerAdaptationPlan {
            selector: target_selector,
            pressure_observations: 0,
            upgrade_observations: 0,
            request_keyframe: true,
        });
    }
    if target_index < current_index {
        let pressure_observations = current
            .pressure_observations()
            .saturating_add(1)
            .min(DOWNSWITCH_PRESSURE_OBSERVATIONS);
        if pressure_observations >= DOWNSWITCH_PRESSURE_OBSERVATIONS {
            return Some(ConsumerAdaptationPlan {
                selector: target_selector,
                pressure_observations: 0,
                upgrade_observations: 0,
                request_keyframe: true,
            });
        }
        return Some(ConsumerAdaptationPlan {
            selector: current.selector(),
            pressure_observations,
            upgrade_observations: 0,
            request_keyframe: false,
        });
    }
    if target_index > current_index {
        let upgrade_observations = current
            .upgrade_observations()
            .saturating_add(1)
            .min(UPSWITCH_STABLE_OBSERVATIONS);
        if upgrade_observations >= UPSWITCH_STABLE_OBSERVATIONS {
            return Some(ConsumerAdaptationPlan {
                selector: target_selector,
                pressure_observations: 0,
                upgrade_observations: 0,
                request_keyframe: true,
            });
        }
        return Some(ConsumerAdaptationPlan {
            selector: current.selector(),
            pressure_observations: 0,
            upgrade_observations,
            request_keyframe: false,
        });
    }
    Some(ConsumerAdaptationPlan {
        selector: current.selector(),
        pressure_observations: 0,
        upgrade_observations: 0,
        request_keyframe: false,
    })
}

fn selected_receiver_bitrate(planned_routes: &[PlannedReceiverRoute<'_>]) -> Bitrate {
    planned_routes
        .iter()
        .filter(|route| matches!(route.action, VideoRouteAction::Send(_)))
        .fold(Bitrate::zero(), |total, route| {
            total.saturating_add(route.selected_bitrate)
        })
}

fn route_can_downgrade(route: &PlannedReceiverRoute<'_>) -> bool {
    route.route.adaptation_policy() == SourceAdaptationPolicy::ScalableVideo
        && matches!(
            route.route.layout_intent().priority(),
            SourceRoutePriority::VisibleThumbnail | SourceRoutePriority::HiddenOrOverflow
        )
}

fn route_is_protected(route: &PlannedReceiverRoute<'_>) -> bool {
    matches!(
        route.route.layout_intent().priority(),
        SourceRoutePriority::PinnedOrFeatured
            | SourceRoutePriority::ReadableDetail
            | SourceRoutePriority::ActiveSpeaker
    )
}

fn pause_rank(route: &PlannedReceiverRoute<'_>) -> u8 {
    match route.route.layout_intent().priority() {
        SourceRoutePriority::HiddenOrOverflow => 0,
        SourceRoutePriority::VisibleThumbnail => 1,
        SourceRoutePriority::ActiveSpeaker => 2,
        SourceRoutePriority::ReadableDetail => 3,
        SourceRoutePriority::PinnedOrFeatured => 4,
    }
}

fn pause_reason_for_route(route: &PlannedReceiverRoute<'_>) -> PolicyPauseReason {
    match route.route.layout_intent().priority() {
        SourceRoutePriority::HiddenOrOverflow => match route.route.layout_intent().role() {
            SourceRoomPolicySelector::Hidden => PolicyPauseReason::HiddenTile,
            SourceRoomPolicySelector::Overflow => PolicyPauseReason::OverflowTile,
            _ => PolicyPauseReason::BudgetPressure,
        },
        _ => PolicyPauseReason::BudgetPressure,
    }
}

fn cheapest_useful_encoding(
    encodings: SelectableRouteEncodings<'_>,
) -> Option<(SourceSelector, Bitrate)> {
    encodings
        .iter()
        .filter(|encoding| {
            !matches!(
                encoding.policy_role(),
                Some(UploadLayerPolicyRole::Featured)
            )
        })
        .chain(encodings.iter())
        .find_map(|encoding| {
            Some((
                SourceSelector::Encoding(encoding.encoding_id()),
                encoding.max_bitrate()?,
            ))
        })
}

fn selector_bitrate(encodings: SelectableRouteEncodings<'_>, selector: SourceSelector) -> Bitrate {
    selector
        .selected_encoding()
        .and_then(|encoding_id| {
            encodings
                .iter()
                .find(|encoding| encoding.encoding_id() == encoding_id)
                .and_then(SourceEncodingDescriptor::max_bitrate)
        })
        .or_else(|| {
            encodings
                .iter()
                .filter_map(SourceEncodingDescriptor::max_bitrate)
                .max()
        })
        .unwrap_or_default()
}

fn readable_detail_adaptation_plan(
    encodings: SelectableRouteEncodings<'_>,
    current: ConsumerSourceSelection,
) -> Option<ConsumerAdaptationPlan> {
    if encodings.len() < 2 {
        return None;
    }
    let target_index = encodings.len().saturating_sub(1);
    let target_selector = SourceSelector::Encoding(encodings.get(target_index)?.encoding_id());
    Some(ConsumerAdaptationPlan {
        selector: target_selector,
        pressure_observations: 0,
        upgrade_observations: 0,
        request_keyframe: target_selector != current.selector(),
    })
}

fn desired_encoding_index(
    user_count: usize,
    featured: bool,
    visible_scalable_route_count: usize,
    receiver_bandwidth: Option<Bitrate>,
    encodings: SelectableRouteEncodings<'_>,
) -> usize {
    let highest_index = encodings.len().saturating_sub(1);
    if user_count < MULTIPARTY_SCALABLE_VIDEO_SELECTION_THRESHOLD {
        return highest_index;
    }
    let Some(receiver_bandwidth) = receiver_bandwidth else {
        return if featured { highest_index } else { 0 };
    };
    let budget = if featured {
        receiver_bandwidth
    } else {
        let divisor = u64::try_from(visible_scalable_route_count)
            .unwrap_or(u64::MAX)
            .saturating_mul(THUMBNAIL_BUDGET_DIVISOR)
            .max(1);
        receiver_bandwidth.divided_by(divisor)
    };
    highest_affordable_encoding_index(encodings, budget, featured)
}

fn highest_affordable_encoding_index(
    encodings: SelectableRouteEncodings<'_>,
    budget: Bitrate,
    featured: bool,
) -> usize {
    if encodings
        .iter()
        .all(|encoding| encoding.max_bitrate().is_none())
    {
        return if featured {
            encodings.len().saturating_sub(1)
        } else {
            0
        };
    }
    encodings
        .iter()
        .enumerate()
        .filter(|(_index, encoding)| {
            encoding
                .max_bitrate()
                .is_some_and(|bitrate| bitrate <= budget)
        })
        .last()
        .map_or(0, |(index, _encoding)| index)
}

fn selector_index(selector: SourceSelector, encodings: SelectableRouteEncodings<'_>) -> usize {
    selector
        .selected_encoding()
        .and_then(|encoding_id| {
            encodings
                .iter()
                .position(|encoding| encoding.encoding_id() == encoding_id)
        })
        .unwrap_or_else(|| encodings.len().saturating_sub(1))
}
