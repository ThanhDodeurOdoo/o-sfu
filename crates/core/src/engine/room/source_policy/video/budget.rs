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

use std::collections::BTreeSet;

use super::{
    super::{
        VideoAdmissionRank,
        action::{BudgetSolverOutcomes, ConsumerPacketSelectionUpdate, ReceiverVideoPolicyPlan},
    },
    input::{ReceiverVideoPolicyInput, ReceiverVideoRouteInput, SelectableRouteEncodings},
    projection::source_packet_gate_for_selector,
};
use crate::{
    Bitrate,
    engine::{
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

/// Semantic decision for one receiver/source video route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VideoRouteAction {
    Send(SourceSelector),
    Pause(PolicyPauseReason),
}

/// Selector decision plus refresh-count hysteresis for one route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConsumerAdaptationPlan {
    selector: SourceSelector,
    pressure_observations: u8,
    upgrade_observations: u8,
    request_keyframe: bool,
}

impl ConsumerAdaptationPlan {
    const fn new(
        selector: SourceSelector,
        pressure_observations: u8,
        upgrade_observations: u8,
        request_keyframe: bool,
    ) -> Self {
        Self {
            selector,
            pressure_observations,
            upgrade_observations,
            request_keyframe,
        }
    }

    const fn confirmed(selector: SourceSelector, request_keyframe: bool) -> Self {
        Self::new(selector, 0, 0, request_keyframe)
    }

    const fn hold(
        selector: SourceSelector,
        pressure_observations: u8,
        upgrade_observations: u8,
    ) -> Self {
        Self::new(selector, pressure_observations, upgrade_observations, false)
    }

    const fn from_current(current: ConsumerSourceSelection) -> Self {
        Self::new(
            current.selector(),
            current.pressure_observations(),
            current.upgrade_observations(),
            false,
        )
    }
}

#[derive(Debug, Clone, Copy)]
struct PlannedReceiverRoute<'a> {
    route: &'a ReceiverVideoRouteInput<'a>,
    adaptation: ConsumerAdaptationPlan,
    selected_bitrate: Bitrate,
    action: VideoRouteAction,
    outcomes: BudgetSolverOutcomes,
}

#[derive(Debug, Clone, Copy)]
struct RouteUpdatePlan {
    action: VideoRouteAction,
    outcomes: BudgetSolverOutcomes,
    pressure_observations: u8,
    upgrade_observations: u8,
    request_keyframe: bool,
}

impl RouteUpdatePlan {
    const fn send(
        selector: SourceSelector,
        outcomes: BudgetSolverOutcomes,
        request_keyframe: bool,
    ) -> Self {
        Self::new(
            VideoRouteAction::Send(selector),
            outcomes,
            0,
            0,
            request_keyframe,
        )
    }

    const fn pause(reason: PolicyPauseReason, outcomes: BudgetSolverOutcomes) -> Self {
        Self::new(VideoRouteAction::Pause(reason), outcomes, 0, 0, false)
    }

    const fn hold(
        action: VideoRouteAction,
        outcomes: BudgetSolverOutcomes,
        pressure_observations: u8,
        upgrade_observations: u8,
    ) -> Self {
        Self::new(
            action,
            outcomes,
            pressure_observations,
            upgrade_observations,
            false,
        )
    }

    const fn from_route(route: &PlannedReceiverRoute<'_>, outcomes: BudgetSolverOutcomes) -> Self {
        Self::new(
            route.action,
            outcomes,
            route.adaptation.pressure_observations,
            route.adaptation.upgrade_observations,
            route.adaptation.request_keyframe,
        )
    }

    const fn new(
        action: VideoRouteAction,
        outcomes: BudgetSolverOutcomes,
        pressure_observations: u8,
        upgrade_observations: u8,
        request_keyframe: bool,
    ) -> Self {
        Self {
            action,
            outcomes,
            pressure_observations,
            upgrade_observations,
            request_keyframe,
        }
    }
}

impl super::super::super::state::RoomState {
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
    let mut remaining_routes = routes.as_slice();
    while let Some((first_route, rest)) = remaining_routes.split_first() {
        let consumer_user_id = first_route.consumer_user_id();
        let group_len = 1 + rest
            .iter()
            .take_while(|route| route.consumer_user_id() == consumer_user_id)
            .count();
        let (receiver_routes, next_routes) = remaining_routes.split_at(group_len);
        let receiver_plan = plan_receiver_routes(receiver_routes, max_video_downloads_per_receiver);
        if let Some(target) = receiver_bwe_targets.get_mut(consumer_user_id) {
            target.set_target(receiver_plan.receiver_bwe_target);
        }
        selection_updates.extend(receiver_plan.selection_updates);
        remaining_routes = next_routes;
    }
    ReceiverVideoPolicyPlan {
        consumer_packet_updates: selection_updates,
        receiver_bwe_targets: receiver_bwe_targets.into_values().collect(),
    }
}

#[derive(Debug)]
struct PlannedReceiverRoutes {
    selection_updates: Vec<ConsumerPacketSelectionUpdate>,
    receiver_bwe_target: Bitrate,
}

fn plan_receiver_routes<'a>(
    routes: &'a [ReceiverVideoRouteInput<'a>],
    max_video_downloads_per_receiver: usize,
) -> PlannedReceiverRoutes {
    let receiver_bandwidth = routes.iter().find_map(|route| route.receiver_bandwidth);
    let mut planned_routes = routes
        .iter()
        .filter_map(planned_receiver_route)
        .collect::<Vec<_>>();
    apply_receiver_video_download_limit(&mut planned_routes, max_video_downloads_per_receiver);
    if let Some(receiver_bandwidth) = receiver_bandwidth {
        apply_receiver_overload_policy(&mut planned_routes, receiver_bandwidth);
    }
    let budget = receiver_budget_diagnostics(&planned_routes, receiver_bandwidth);
    let receiver_bwe_target = budget.selected_video_bitrate();
    let selection_updates = planned_routes
        .into_iter()
        .filter_map(|route| planned_route_update(route, budget))
        .collect();
    PlannedReceiverRoutes {
        selection_updates,
        receiver_bwe_target,
    }
}

fn planned_receiver_route<'a>(
    route: &'a ReceiverVideoRouteInput<'a>,
) -> Option<PlannedReceiverRoute<'a>> {
    let adaptation =
        consumer_route_adaptation_plan(route).or_else(|| current_route_admission_plan(route))?;
    let selected_bitrate = selector_bitrate(route.encodings(), adaptation.selector);
    let outcomes = adaptation_outcomes(
        route.encodings(),
        route.current_selection,
        adaptation.selector,
    );
    Some(PlannedReceiverRoute {
        route,
        adaptation,
        selected_bitrate,
        action: VideoRouteAction::Send(adaptation.selector),
        outcomes,
    })
}

fn consumer_route_adaptation_plan(
    route: &ReceiverVideoRouteInput<'_>,
) -> Option<ConsumerAdaptationPlan> {
    consumer_adaptation_plan(
        route.user_count,
        route.adaptation_policy(),
        route.encodings(),
        route.current_selection,
        route.layout_intent.uses_featured_quality(),
        route.visible_scalable_route_count,
        route.receiver_bandwidth,
    )
}

fn current_route_admission_plan(
    route: &ReceiverVideoRouteInput<'_>,
) -> Option<ConsumerAdaptationPlan> {
    if route.adaptation_policy() == SourceAdaptationPolicy::None {
        return None;
    }
    Some(ConsumerAdaptationPlan::from_current(
        route.current_selection,
    ))
}

fn apply_receiver_video_download_limit(
    planned_routes: &mut [PlannedReceiverRoute<'_>],
    max_video_downloads_per_receiver: usize,
) {
    if active_video_route_count(planned_routes) <= max_video_downloads_per_receiver {
        return;
    }
    let admitted_indexes = {
        let mut ranked = planned_routes
            .iter()
            .enumerate()
            .filter(|(_index, route)| matches!(route.action, VideoRouteAction::Send(_)))
            .map(|(index, route)| (video_download_rank(route), index))
            .collect::<Vec<_>>();
        ranked.sort_by_key(|(rank, _index)| *rank);
        ranked
            .into_iter()
            .take(max_video_downloads_per_receiver)
            .map(|(_rank, index)| index)
            .collect::<BTreeSet<_>>()
    };
    for (index, route) in planned_routes.iter_mut().enumerate() {
        if admitted_indexes.contains(&index) {
            continue;
        }
        route.action = VideoRouteAction::Pause(PolicyPauseReason::VideoDownloadLimit);
        route.outcomes = BudgetSolverOutcomes::paused();
        route.selected_bitrate = Bitrate::zero();
    }
}

fn apply_receiver_overload_policy(
    planned_routes: &mut [PlannedReceiverRoute<'_>],
    receiver_bandwidth: Bitrate,
) {
    let mut total_selected_bitrate = selected_receiver_bitrate(planned_routes);
    if total_selected_bitrate <= receiver_bandwidth {
        return;
    }
    for route in planned_routes
        .iter_mut()
        .filter(|route| route_can_downgrade(route))
    {
        let Some((selector, bitrate)) = cheapest_useful_selector(route.route.encodings()) else {
            route.action = VideoRouteAction::Pause(PolicyPauseReason::MissingUsableLayer);
            total_selected_bitrate = total_selected_bitrate.saturating_sub(route.selected_bitrate);
            route.selected_bitrate = Bitrate::zero();
            continue;
        };
        if bitrate < route.selected_bitrate {
            total_selected_bitrate = total_selected_bitrate
                .saturating_sub(route.selected_bitrate)
                .saturating_add(bitrate);
            route.selected_bitrate = bitrate;
            route.action = VideoRouteAction::Send(selector);
            route.outcomes = BudgetSolverOutcomes::degraded();
        }
    }
    if total_selected_bitrate <= receiver_bandwidth {
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
        if total_selected_bitrate <= receiver_bandwidth {
            break;
        }
        let pause_reason = pause_reason_for_route(route);
        route.action = VideoRouteAction::Pause(pause_reason);
        route.outcomes = BudgetSolverOutcomes::paused();
        total_selected_bitrate = total_selected_bitrate.saturating_sub(route.selected_bitrate);
        route.selected_bitrate = Bitrate::zero();
    }
}

fn planned_route_update(
    route: PlannedReceiverRoute<'_>,
    budget: ReceiverVideoBudgetDiagnostics,
) -> Option<ConsumerPacketSelectionUpdate> {
    let current = route.route.current_selection;
    let current_pause_reason = current.policy_pause_reason();
    let outcomes = budget_outcomes(route.outcomes, budget);
    let update = match (route.action, current_pause_reason) {
        (VideoRouteAction::Send(selector), Some(PolicyPauseReason::VideoDownloadLimit)) => {
            RouteUpdatePlan::send(
                selector,
                budget_outcomes(BudgetSolverOutcomes::resumed(), budget),
                true,
            )
        }
        (VideoRouteAction::Send(selector), Some(reason)) => {
            let upgrade_observations = current
                .upgrade_observations()
                .saturating_add(1)
                .min(UPSWITCH_STABLE_OBSERVATIONS);
            if upgrade_observations >= UPSWITCH_STABLE_OBSERVATIONS {
                RouteUpdatePlan::send(
                    selector,
                    budget_outcomes(BudgetSolverOutcomes::resumed(), budget),
                    true,
                )
            } else {
                RouteUpdatePlan::hold(
                    VideoRouteAction::Pause(reason),
                    outcomes,
                    0,
                    upgrade_observations,
                )
            }
        }
        (VideoRouteAction::Pause(reason), pause_reason) if pause_reason != Some(reason) => {
            if reason == PolicyPauseReason::VideoDownloadLimit {
                RouteUpdatePlan::pause(
                    reason,
                    budget_outcomes(BudgetSolverOutcomes::paused(), budget),
                )
            } else {
                let pressure_observations = current
                    .pressure_observations()
                    .saturating_add(1)
                    .min(DOWNSWITCH_PRESSURE_OBSERVATIONS);
                if pressure_observations >= DOWNSWITCH_PRESSURE_OBSERVATIONS {
                    RouteUpdatePlan::pause(
                        reason,
                        budget_outcomes(BudgetSolverOutcomes::paused(), budget),
                    )
                } else {
                    RouteUpdatePlan::hold(
                        VideoRouteAction::Send(current.selector()),
                        outcomes,
                        pressure_observations,
                        0,
                    )
                }
            }
        }
        _ => RouteUpdatePlan::from_route(&route, outcomes),
    };
    consumer_packet_selection_update(route.route, update, budget)
}

fn consumer_packet_selection_update(
    route: &ReceiverVideoRouteInput<'_>,
    update: RouteUpdatePlan,
    budget: ReceiverVideoBudgetDiagnostics,
) -> Option<ConsumerPacketSelectionUpdate> {
    let current_selection = route.current_selection;
    let (selector, policy_pause_reason, request_keyframe) = match update.action {
        VideoRouteAction::Send(selector) => (
            selector,
            None,
            update.request_keyframe || !current_selection.policy_allows_delivery(),
        ),
        VideoRouteAction::Pause(reason) => (current_selection.selector(), Some(reason), false),
    };
    let packet_gate = if selector == current_selection.selector() {
        None
    } else {
        Some(source_packet_gate_for_selector(route.source, selector).ok()?)
    };
    let route_activity_update = policy_pause_reason != current_selection.policy_pause_reason();
    if packet_gate.is_none()
        && !route_activity_update
        && budget == current_selection.budget()
        && update.pressure_observations == current_selection.pressure_observations()
        && update.upgrade_observations == current_selection.upgrade_observations()
    {
        return None;
    }
    Some(ConsumerPacketSelectionUpdate {
        route: route.transport_ref.clone(),
        source_id: route.source_id(),
        selector,
        policy_pause_reason,
        budget,
        outcomes: update.outcomes,
        pressure_observations: update.pressure_observations,
        upgrade_observations: update.upgrade_observations,
        packet_gate,
        route_activity_update,
        request_keyframe,
    })
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
        return Some(ConsumerAdaptationPlan::confirmed(
            target_selector,
            selector_changed,
        ));
    }
    if receiver_bandwidth.is_none() {
        return Some(ConsumerAdaptationPlan::confirmed(target_selector, true));
    }
    if target_index < current_index {
        let pressure_observations = current
            .pressure_observations()
            .saturating_add(1)
            .min(DOWNSWITCH_PRESSURE_OBSERVATIONS);
        if pressure_observations >= DOWNSWITCH_PRESSURE_OBSERVATIONS {
            return Some(ConsumerAdaptationPlan::confirmed(target_selector, true));
        }
        return Some(ConsumerAdaptationPlan::hold(
            current.selector(),
            pressure_observations,
            0,
        ));
    }
    let upgrade_observations = current
        .upgrade_observations()
        .saturating_add(1)
        .min(UPSWITCH_STABLE_OBSERVATIONS);
    if upgrade_observations >= UPSWITCH_STABLE_OBSERVATIONS {
        return Some(ConsumerAdaptationPlan::confirmed(target_selector, true));
    }
    Some(ConsumerAdaptationPlan::hold(
        current.selector(),
        0,
        upgrade_observations,
    ))
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
            route.route.layout_intent.priority(),
            SourceRoutePriority::VisibleThumbnail | SourceRoutePriority::HiddenOrOverflow
        )
}

fn route_is_protected(route: &PlannedReceiverRoute<'_>) -> bool {
    matches!(
        route.route.layout_intent.priority(),
        SourceRoutePriority::PinnedOrFeatured
            | SourceRoutePriority::ReadableDetail
            | SourceRoutePriority::ActiveSpeaker
    )
}

fn pause_rank(route: &PlannedReceiverRoute<'_>) -> u8 {
    match route.route.layout_intent.priority() {
        SourceRoutePriority::HiddenOrOverflow => 0,
        SourceRoutePriority::VisibleThumbnail => 1,
        SourceRoutePriority::ActiveSpeaker => 2,
        SourceRoutePriority::ReadableDetail => 3,
        SourceRoutePriority::PinnedOrFeatured => 4,
    }
}

fn video_download_rank(route: &PlannedReceiverRoute<'_>) -> VideoAdmissionRank {
    VideoAdmissionRank::new(
        route.route.layout_intent.priority(),
        route.route.active_speaker_rank,
        route.route.source_id(),
    )
}

fn pause_reason_for_route(route: &PlannedReceiverRoute<'_>) -> PolicyPauseReason {
    match route.route.layout_intent.priority() {
        SourceRoutePriority::HiddenOrOverflow => match route.route.layout_intent.role() {
            SourceRoomPolicySelector::Hidden => PolicyPauseReason::HiddenTile,
            SourceRoomPolicySelector::Overflow => PolicyPauseReason::OverflowTile,
            _ => PolicyPauseReason::BudgetPressure,
        },
        _ => PolicyPauseReason::BudgetPressure,
    }
}

fn cheapest_useful_selector(
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
    Some(ConsumerAdaptationPlan::confirmed(
        target_selector,
        target_selector != current.selector(),
    ))
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

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        reason = "test fixtures should fail loudly when they build invalid source graphs"
    )]

    use o_sfu_router::{MediaKind, Rid};

    use super::*;
    use crate::engine::{
        ConnectionId, UserId,
        media_transport::{SourcePacketGate, TransportMediaId},
        room::{
            media_graph::ConsumerRouteTransportRef,
            source_policy::{ReceiverBweTargetPlan, video::layout::ReceiverVideoLayoutIntent},
        },
        source_model::{
            PublishedSourceDescriptor, PublishedSourceDescriptorParts, PublishedSourceId,
            PublishedSourceOwner, SourceEncodingDescriptorParts, SourceEncodingId,
            SourceLayoutPolicy, SourceModelError, SourcePolicy, SourceRoomPolicySelector,
            UserStreamId,
        },
    };

    fn role_encoding(
        source_id: PublishedSourceId,
        encoding_id: SourceEncodingId,
        rid: &str,
        role: UploadLayerPolicyRole,
    ) -> SourceEncodingDescriptor {
        SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
            encoding_id,
            source_id,
            rid: Some(Rid::new(rid)),
            primary_ssrc: None,
            repair_ssrc: None,
            max_bitrate: None,
            resolution_scale: None,
            max_framerate: None,
            policy_role: Some(role),
            max_temporal_layer_id: None,
            negotiated_format: None,
        })
    }

    fn bitrate_encoding(
        source_id: PublishedSourceId,
        encoding_id: SourceEncodingId,
        rid: &str,
        max_bitrate: Bitrate,
        role: UploadLayerPolicyRole,
    ) -> SourceEncodingDescriptor {
        SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
            encoding_id,
            source_id,
            rid: Some(Rid::new(rid)),
            primary_ssrc: None,
            repair_ssrc: None,
            max_bitrate: Some(max_bitrate),
            resolution_scale: None,
            max_framerate: None,
            policy_role: Some(role),
            max_temporal_layer_id: None,
            negotiated_format: None,
        })
    }

    fn ridless_encoding(
        source_id: PublishedSourceId,
        encoding_id: SourceEncodingId,
    ) -> SourceEncodingDescriptor {
        SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
            encoding_id,
            source_id,
            rid: None,
            primary_ssrc: None,
            repair_ssrc: None,
            max_bitrate: Some(Bitrate::from_kbps(900)),
            resolution_scale: None,
            max_framerate: None,
            policy_role: None,
            max_temporal_layer_id: None,
            negotiated_format: None,
        })
    }

    fn scalable_source(
        encodings: Vec<SourceEncodingDescriptor>,
    ) -> Result<PublishedSourceDescriptor, SourceModelError> {
        scalable_source_with(
            PublishedSourceId::from_raw(7),
            UserId::Integer(41),
            SourceRoomPolicySelector::VisibleThumbnail,
            encodings,
        )
    }

    fn scalable_source_with(
        source_id: PublishedSourceId,
        owner_user_id: UserId,
        visible_selector: SourceRoomPolicySelector,
        encodings: Vec<SourceEncodingDescriptor>,
    ) -> Result<PublishedSourceDescriptor, SourceModelError> {
        PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
            source_id,
            owner: PublishedSourceOwner::new(owner_user_id),
            stream_id: UserStreamId::new("camera"),
            media_kind: MediaKind::Video,
            policy: SourcePolicy::new(
                Some(SourceLayoutPolicy::new(
                    visible_selector,
                    Some(SourceRoomPolicySelector::ActiveSpeaker),
                )),
                SourceAdaptationPolicy::ScalableVideo,
                None,
            ),
            mid: None,
            encodings,
        })
    }

    fn route(
        source: &PublishedSourceDescriptor,
        current_selection: ConsumerSourceSelection,
    ) -> ReceiverVideoRouteInput<'_> {
        route_with_layout(
            source,
            current_selection,
            SourceRoomPolicySelector::VisibleThumbnail,
        )
    }

    fn route_with_layout(
        source: &PublishedSourceDescriptor,
        current_selection: ConsumerSourceSelection,
        layout_selector: SourceRoomPolicySelector,
    ) -> ReceiverVideoRouteInput<'_> {
        ReceiverVideoRouteInput {
            user_count: 3,
            source,
            transport_ref: ConsumerRouteTransportRef::from_parts(
                UserId::Integer(42),
                ConnectionId::from_raw(10),
                TransportMediaId::new(20),
                source.owner().user_id().clone(),
                ConnectionId::from_raw(11),
                TransportMediaId::new(source.source_id().as_u64().saturating_add(20)),
            ),
            current_selection,
            layout_intent: ReceiverVideoLayoutIntent::new(layout_selector),
            visible_scalable_route_count: 2,
            active_speaker_rank: None,
            receiver_bandwidth: Some(Bitrate::from_kbps(21)),
        }
    }

    #[test]
    fn receiver_budget_uses_policy_role_order_when_bitrates_are_absent()
    -> Result<(), SourceModelError> {
        let source_id = PublishedSourceId::from_raw(7);
        let high_encoding_id = SourceEncodingId::from_raw(1);
        let low_encoding_id = SourceEncodingId::from_raw(2);
        let source = scalable_source(vec![
            role_encoding(
                source_id,
                high_encoding_id,
                "hi",
                UploadLayerPolicyRole::Featured,
            ),
            role_encoding(
                source_id,
                low_encoding_id,
                "lo",
                UploadLayerPolicyRole::Thumbnail,
            ),
        ])?;
        let mut selection = ConsumerSourceSelection::open(true);
        selection.set_selector(SourceSelector::Encoding(high_encoding_id));
        selection.set_adaptation_observations(1, 0);
        let route = route(&source, selection);

        let plan = plan_receiver_routes(&[route], usize::MAX);
        let updates = plan.selection_updates;

        assert_eq!(updates.len(), 1);
        let update = updates
            .first()
            .expect("budget plan should select the role-ranked thumbnail layer");
        assert_eq!(update.selector, SourceSelector::Encoding(low_encoding_id));
        assert_eq!(
            update.packet_gate.as_ref(),
            Some(&SourcePacketGate::Rid("lo".into()))
        );
        Ok(())
    }

    #[test]
    fn video_download_limit_counts_ridless_single_encoding_routes() -> Result<(), SourceModelError>
    {
        let visible_source_id = PublishedSourceId::from_raw(7);
        let hidden_source_id = PublishedSourceId::from_raw(8);
        let visible_source = scalable_source_with(
            visible_source_id,
            UserId::Integer(41),
            SourceRoomPolicySelector::VisibleThumbnail,
            vec![ridless_encoding(
                visible_source_id,
                SourceEncodingId::from_raw(1),
            )],
        )?;
        let hidden_source = scalable_source_with(
            hidden_source_id,
            UserId::Integer(43),
            SourceRoomPolicySelector::Hidden,
            vec![ridless_encoding(
                hidden_source_id,
                SourceEncodingId::from_raw(1),
            )],
        )?;
        let routes = [
            route_with_layout(
                &visible_source,
                ConsumerSourceSelection::open(true),
                SourceRoomPolicySelector::VisibleThumbnail,
            ),
            route_with_layout(
                &hidden_source,
                ConsumerSourceSelection::open(true),
                SourceRoomPolicySelector::Hidden,
            ),
        ];

        let plan = plan_receiver_routes(&routes, 1);
        let updates = plan.selection_updates;
        let hidden_update = updates
            .iter()
            .find(|update| update.source_id == hidden_source_id)
            .expect("download cap should pause the RID-less overflow route");

        assert_eq!(
            hidden_update.policy_pause_reason,
            Some(PolicyPauseReason::VideoDownloadLimit)
        );
        assert!(hidden_update.route_activity_update);
        assert!(hidden_update.outcomes.is_paused());
        assert_eq!(hidden_update.budget.active_video_route_count(), 1);
        Ok(())
    }

    #[test]
    fn two_party_receiver_bwe_target_uses_high_layer() -> Result<(), SourceModelError> {
        let source_id = PublishedSourceId::from_raw(7);
        let low_encoding_id = SourceEncodingId::from_raw(1);
        let high_encoding_id = SourceEncodingId::from_raw(2);
        let source = scalable_source(vec![
            bitrate_encoding(
                source_id,
                low_encoding_id,
                "lo",
                Bitrate::from_kbps(150),
                UploadLayerPolicyRole::Thumbnail,
            ),
            bitrate_encoding(
                source_id,
                high_encoding_id,
                "hi",
                Bitrate::from_kbps(900),
                UploadLayerPolicyRole::Featured,
            ),
        ])?;
        let mut route = route(&source, ConsumerSourceSelection::open(true));
        route.user_count = 2;
        route.receiver_bandwidth = None;

        let plan = plan_receiver_routes(&[route], usize::MAX);

        assert_eq!(plan.receiver_bwe_target, Bitrate::from_kbps(900));
        Ok(())
    }

    #[test]
    fn multiparty_receiver_bwe_target_uses_thumbnail_layer() -> Result<(), SourceModelError> {
        let source_id = PublishedSourceId::from_raw(7);
        let low_encoding_id = SourceEncodingId::from_raw(1);
        let high_encoding_id = SourceEncodingId::from_raw(2);
        let source = scalable_source(vec![
            bitrate_encoding(
                source_id,
                low_encoding_id,
                "lo",
                Bitrate::from_kbps(150),
                UploadLayerPolicyRole::Thumbnail,
            ),
            bitrate_encoding(
                source_id,
                high_encoding_id,
                "hi",
                Bitrate::from_kbps(900),
                UploadLayerPolicyRole::Featured,
            ),
        ])?;
        let mut route = route(&source, ConsumerSourceSelection::open(true));
        route.receiver_bandwidth = None;

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
        let source_id = PublishedSourceId::from_raw(7);
        let low_encoding_id = SourceEncodingId::from_raw(1);
        let high_encoding_id = SourceEncodingId::from_raw(2);
        let source = scalable_source(vec![
            bitrate_encoding(
                source_id,
                low_encoding_id,
                "lo",
                Bitrate::from_kbps(150),
                UploadLayerPolicyRole::Thumbnail,
            ),
            bitrate_encoding(
                source_id,
                high_encoding_id,
                "hi",
                Bitrate::from_kbps(900),
                UploadLayerPolicyRole::Featured,
            ),
        ])?;
        let mut route = route_with_layout(
            &source,
            ConsumerSourceSelection::open(true),
            SourceRoomPolicySelector::Pinned,
        );
        route.receiver_bandwidth = Some(Bitrate::from_kbps(100));

        let plan = plan_receiver_routes(&[route], usize::MAX);

        assert_eq!(plan.receiver_bwe_target, Bitrate::from_kbps(900));
        Ok(())
    }
}
