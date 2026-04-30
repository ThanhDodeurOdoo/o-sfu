//! Pure receiver video budget planner.
//!
//! The planner first chooses the useful encoding for each receiver/source route,
//! then solves the receiver's selected video set against the live bandwidth
//! estimate. Overload is expressed as semantic route pauses so the transport
//! withholds whole routes instead of randomly dropping packets.

use std::collections::BTreeMap;

use super::{
    action::{ConsumerPacketSelectionUpdate, ReceiverVideoRouteAction, VideoRouteAction},
    input::{ReceiverVideoPolicyInput, ReceiverVideoRouteInput},
};
use crate::runtime::{
    StreamType, UserId,
    source_model::{
        ConsumerSourceSelection, PolicyPauseReason, SourceEncodingDescriptor,
        SourceRoomPolicySelector, SourceRoutePriority, SourceSelector, UploadLayerPolicyRole,
    },
    transport_adapter::{ActiveSpeakerSource, ReceiverBandwidthSnapshot},
};

/// Minimum room size where camera simulcast adaptation starts constraining receivers.
const MULTIPARTY_CAMERA_SIMULCAST_SELECTION_THRESHOLD: usize = 3;

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
        let mut routes_by_receiver: BTreeMap<&UserId, Vec<&ReceiverVideoRouteInput<'a>>> =
            BTreeMap::new();
        for route in input.routes() {
            routes_by_receiver
                .entry(route.consumer_user_id())
                .or_default()
                .push(route);
        }
        let route_actions = routes_by_receiver
            .into_values()
            .flat_map(plan_receiver_routes)
            .collect();
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

#[derive(Debug, Clone)]
struct PlannedReceiverRoute<'a> {
    route: ReceiverVideoRouteInput<'a>,
    adaptation: ConsumerAdaptationPlan,
    selected_bitrate_bps: u64,
    action: VideoRouteAction,
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
    routes: Vec<&'a ReceiverVideoRouteInput<'a>>,
) -> Vec<ReceiverVideoRouteAction<'a>> {
    let Some(receiver_bandwidth_bps) = routes
        .iter()
        .find_map(|route| route.receiver_bandwidth_bps())
    else {
        return routes
            .into_iter()
            .filter_map(|route| planned_send_action(route, consumer_route_adaptation_plan(route)?))
            .collect();
    };
    let mut planned_routes = routes
        .into_iter()
        .filter_map(|route| {
            let adaptation = consumer_route_adaptation_plan(route)?;
            let selected_bitrate_bps = selector_bitrate_bps(route.encodings(), adaptation.selector);
            Some(PlannedReceiverRoute {
                route: (*route).clone(),
                adaptation,
                selected_bitrate_bps,
                action: VideoRouteAction::Send(adaptation.selector),
            })
        })
        .collect::<Vec<_>>();
    apply_receiver_overload_policy(&mut planned_routes, receiver_bandwidth_bps);
    planned_routes
        .into_iter()
        .filter_map(planned_route_action)
        .collect()
}

fn consumer_route_adaptation_plan(
    route: &ReceiverVideoRouteInput<'_>,
) -> Option<ConsumerAdaptationPlan> {
    consumer_adaptation_plan(
        route.user_count(),
        route.stream_type(),
        route.encodings(),
        route.current_selection(),
        route.layout_intent().uses_featured_quality(),
        route.visible_camera_route_count(),
        route.receiver_bandwidth_bps(),
    )
}

fn planned_send_action<'a>(
    route: &'a ReceiverVideoRouteInput<'a>,
    adaptation: ConsumerAdaptationPlan,
) -> Option<ReceiverVideoRouteAction<'a>> {
    planned_route_action(PlannedReceiverRoute {
        route: (*route).clone(),
        selected_bitrate_bps: selector_bitrate_bps(route.encodings(), adaptation.selector),
        action: VideoRouteAction::Send(adaptation.selector),
        adaptation,
    })
}

fn apply_receiver_overload_policy(
    planned_routes: &mut [PlannedReceiverRoute<'_>],
    receiver_bandwidth_bps: u64,
) {
    let mut selected_bitrate_bps = selected_receiver_bitrate_bps(planned_routes);
    if selected_bitrate_bps <= receiver_bandwidth_bps {
        return;
    }
    for route in planned_routes
        .iter_mut()
        .filter(|route| route_can_downgrade(route))
    {
        let Some((selector, bitrate_bps)) = cheapest_useful_encoding(route.route.encodings())
        else {
            route.action = VideoRouteAction::Pause(PolicyPauseReason::MissingUsableLayer);
            selected_bitrate_bps = selected_bitrate_bps.saturating_sub(route.selected_bitrate_bps);
            route.selected_bitrate_bps = 0;
            continue;
        };
        if bitrate_bps < route.selected_bitrate_bps {
            selected_bitrate_bps = selected_bitrate_bps
                .saturating_sub(route.selected_bitrate_bps)
                .saturating_add(bitrate_bps);
            route.selected_bitrate_bps = bitrate_bps;
            route.action = VideoRouteAction::Send(selector);
        }
    }
    if selected_bitrate_bps <= receiver_bandwidth_bps {
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
        if selected_bitrate_bps <= receiver_bandwidth_bps {
            break;
        }
        let pause_reason = pause_reason_for_route(route);
        route.action = VideoRouteAction::Pause(pause_reason);
        selected_bitrate_bps = selected_bitrate_bps.saturating_sub(route.selected_bitrate_bps);
        route.selected_bitrate_bps = 0;
    }
}

fn planned_route_action(route: PlannedReceiverRoute<'_>) -> Option<ReceiverVideoRouteAction<'_>> {
    let current = route.route.current_selection();
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
                    0,
                    0,
                    true,
                ))
            } else {
                Some(ReceiverVideoRouteAction::new(
                    route.route,
                    VideoRouteAction::Pause(current.policy_pause_reason()?),
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
                    0,
                    0,
                    false,
                ))
            } else {
                Some(ReceiverVideoRouteAction::new(
                    route.route,
                    VideoRouteAction::Send(current.selector()),
                    pressure_observations,
                    0,
                    false,
                ))
            }
        }
        _ => Some(ReceiverVideoRouteAction::new(
            route.route,
            route.action,
            route.adaptation.pressure_observations,
            route.adaptation.upgrade_observations,
            route.adaptation.request_keyframe,
        )),
    }
}

fn consumer_adaptation_plan(
    user_count: usize,
    stream_type: StreamType,
    encodings: &[&SourceEncodingDescriptor],
    current: ConsumerSourceSelection,
    featured: bool,
    visible_camera_route_count: usize,
    receiver_bandwidth_bps: Option<u64>,
) -> Option<ConsumerAdaptationPlan> {
    if stream_type == StreamType::Screen {
        return screen_share_adaptation_plan(encodings, current);
    }
    if stream_type != StreamType::Camera {
        return None;
    }
    if encodings.len() < 2 {
        return None;
    }
    let current_index = selector_index(current.selector(), encodings);
    let target_index = desired_encoding_index(
        user_count,
        featured,
        visible_camera_route_count,
        receiver_bandwidth_bps,
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
    if receiver_bandwidth_bps.is_none() {
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

fn selected_receiver_bitrate_bps(planned_routes: &[PlannedReceiverRoute<'_>]) -> u64 {
    planned_routes
        .iter()
        .filter(|route| matches!(route.action, VideoRouteAction::Send(_)))
        .fold(0_u64, |total, route| {
            total.saturating_add(route.selected_bitrate_bps)
        })
}

fn route_can_downgrade(route: &PlannedReceiverRoute<'_>) -> bool {
    route.route.stream_type() == StreamType::Camera
        && matches!(
            route.route.layout_intent().priority(),
            SourceRoutePriority::VisibleThumbnail | SourceRoutePriority::HiddenOrOverflow
        )
}

fn route_is_protected(route: &PlannedReceiverRoute<'_>) -> bool {
    matches!(
        route.route.layout_intent().priority(),
        SourceRoutePriority::PinnedOrFeatured
            | SourceRoutePriority::ScreenShare
            | SourceRoutePriority::ActiveSpeaker
    )
}

fn pause_rank(route: &PlannedReceiverRoute<'_>) -> u8 {
    match route.route.layout_intent().priority() {
        SourceRoutePriority::HiddenOrOverflow => 0,
        SourceRoutePriority::VisibleThumbnail => 1,
        SourceRoutePriority::ActiveSpeaker => 2,
        SourceRoutePriority::ScreenShare => 3,
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
    encodings: &[&SourceEncodingDescriptor],
) -> Option<(SourceSelector, u64)> {
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

fn selector_bitrate_bps(encodings: &[&SourceEncodingDescriptor], selector: SourceSelector) -> u64 {
    selector
        .selected_encoding()
        .and_then(|encoding_id| {
            encodings
                .iter()
                .find(|encoding| encoding.encoding_id() == encoding_id)
                .and_then(|encoding| encoding.max_bitrate())
        })
        .or_else(|| {
            encodings
                .iter()
                .filter_map(|encoding| encoding.max_bitrate())
                .max()
        })
        .unwrap_or_default()
}

fn screen_share_adaptation_plan(
    encodings: &[&SourceEncodingDescriptor],
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
    visible_camera_route_count: usize,
    receiver_bandwidth_bps: Option<u64>,
    encodings: &[&SourceEncodingDescriptor],
) -> usize {
    let highest_index = encodings.len().saturating_sub(1);
    if user_count < MULTIPARTY_CAMERA_SIMULCAST_SELECTION_THRESHOLD {
        return highest_index;
    }
    let Some(receiver_bandwidth_bps) = receiver_bandwidth_bps else {
        return if featured { highest_index } else { 0 };
    };
    let budget_bps = if featured {
        receiver_bandwidth_bps
    } else {
        let divisor = u64::try_from(visible_camera_route_count)
            .unwrap_or(u64::MAX)
            .saturating_mul(THUMBNAIL_BUDGET_DIVISOR)
            .max(1);
        receiver_bandwidth_bps / divisor
    };
    highest_affordable_encoding_index(encodings, budget_bps, featured)
}

fn highest_affordable_encoding_index(
    encodings: &[&SourceEncodingDescriptor],
    budget_bps: u64,
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
        .rev()
        .find(|(_index, encoding)| {
            encoding
                .max_bitrate()
                .is_some_and(|bitrate| bitrate <= budget_bps)
        })
        .map_or(0, |(index, _encoding)| index)
}

fn selector_index(selector: SourceSelector, encodings: &[&SourceEncodingDescriptor]) -> usize {
    selector
        .selected_encoding()
        .and_then(|encoding_id| {
            encodings
                .iter()
                .position(|encoding| encoding.encoding_id() == encoding_id)
        })
        .unwrap_or_else(|| encodings.len().saturating_sub(1))
}
