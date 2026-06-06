//! per-route selector choice for receiver video policy

use super::input::{ReceiverVideoRouteInput, SelectableRouteEncodings};
use crate::{
    Bitrate,
    engine::source_model::{
        ConsumerSourceSelection, SourceAdaptationPolicy, SourceEncodingDescriptor, SourceSelector,
        UploadLayerPolicyRole,
    },
};

const MULTIPARTY_SCALABLE_VIDEO_SELECTION_THRESHOLD: usize = 3;
const THUMBNAIL_BUDGET_DIVISOR: u64 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ConsumerAdaptationPlan {
    pub selector: SourceSelector,
    pub pressure_observations: u8,
    pub upgrade_observations: u8,
    pub request_keyframe: bool,
}

impl ConsumerAdaptationPlan {
    pub(super) const fn confirmed(selector: SourceSelector, request_keyframe: bool) -> Self {
        Self {
            selector,
            pressure_observations: 0,
            upgrade_observations: 0,
            request_keyframe,
        }
    }

    const fn hold(
        selector: SourceSelector,
        pressure_observations: u8,
        upgrade_observations: u8,
    ) -> Self {
        Self {
            selector,
            pressure_observations,
            upgrade_observations,
            request_keyframe: false,
        }
    }

    pub(super) const fn from_current(current: ConsumerSourceSelection) -> Self {
        Self {
            selector: current.selector(),
            pressure_observations: current.pressure_observations(),
            upgrade_observations: current.upgrade_observations(),
            request_keyframe: false,
        }
    }
}

pub(super) fn route_plan(route: &ReceiverVideoRouteInput<'_>) -> Option<ConsumerAdaptationPlan> {
    match route.adaptation_policy() {
        SourceAdaptationPolicy::ReadableDetail => {
            readable_detail_plan(route.encodings(), route.current_selection)
        }
        SourceAdaptationPolicy::ScalableVideo => scalable_plan(route),
        SourceAdaptationPolicy::None => None,
    }
    .or_else(|| {
        (route.adaptation_policy() != SourceAdaptationPolicy::None)
            .then(|| ConsumerAdaptationPlan::from_current(route.current_selection))
    })
}

pub(super) fn selector_bitrate(
    encodings: SelectableRouteEncodings<'_>,
    selector: SourceSelector,
) -> Bitrate {
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

pub(super) fn cheapest_useful_selector(
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

fn scalable_plan(route: &ReceiverVideoRouteInput<'_>) -> Option<ConsumerAdaptationPlan> {
    let encodings = route.encodings();
    if encodings.len() < 2 {
        return None;
    }
    let current = route.current_selection;
    let current_index = selector_index(current.selector(), encodings);
    let target_index = desired_encoding_index(
        route.user_count,
        route.layout_intent.uses_featured_quality(),
        route.visible_scalable_route_count,
        route.receiver_bandwidth,
        encodings,
    );
    let target_selector = SourceSelector::Encoding(encodings.get(target_index)?.encoding_id());
    let selector_changed = target_selector != current.selector();
    let request_keyframe = selector_changed;
    if target_index == current_index || route.receiver_bandwidth.is_none() {
        return Some(ConsumerAdaptationPlan::confirmed(
            target_selector,
            request_keyframe,
        ));
    }
    if target_index < current_index {
        let pressure_observations = current
            .pressure_observations()
            .saturating_add(1)
            .min(super::hysteresis::DOWNSWITCH_PRESSURE_OBSERVATIONS);
        if pressure_observations >= super::hysteresis::DOWNSWITCH_PRESSURE_OBSERVATIONS {
            return Some(ConsumerAdaptationPlan::confirmed(
                target_selector,
                request_keyframe,
            ));
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
        .min(super::hysteresis::UPSWITCH_STABLE_OBSERVATIONS);
    if upgrade_observations >= super::hysteresis::UPSWITCH_STABLE_OBSERVATIONS {
        return Some(ConsumerAdaptationPlan::confirmed(target_selector, true));
    }
    Some(ConsumerAdaptationPlan::hold(
        current.selector(),
        0,
        upgrade_observations,
    ))
}

fn readable_detail_plan(
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
