use super::{
    input::{ReceiverVideoRouteInput, SelectableRouteEncodings},
    selection::{AdaptationCounts, ReceiverRouteSelection},
};
use crate::{
    Bitrate,
    engine::source_model::{
        ConsumerSourceSelection, PolicyPauseReason, SourceAdaptationPolicy,
        SourceEncodingDescriptor, SourceSelector, UploadLayerPolicyRole,
    },
};

const MULTIPARTY_SCALABLE_VIDEO_SELECTION_THRESHOLD: usize = 3;
const THUMBNAIL_BUDGET_DIVISOR: u64 = 2;

pub(super) fn route_plan(route: &ReceiverVideoRouteInput<'_>) -> Option<ReceiverRouteSelection> {
    let policy = route.adaptation_policy();
    let cap = route.source.policy().video_bitrate_cap();
    match policy {
        SourceAdaptationPolicy::ReadableDetail if route.encodings().len() < 2 && cap.is_none() => {
            None
        }
        SourceAdaptationPolicy::ReadableDetail => highest_allowed_plan(route),
        SourceAdaptationPolicy::ScalableVideo => scalable_plan(route),
        SourceAdaptationPolicy::None if cap.is_some() => highest_allowed_plan(route),
        SourceAdaptationPolicy::None => None,
    }
    .or_else(|| match policy {
        SourceAdaptationPolicy::None if cap.is_none() => None,
        _ if cap.is_some() => Some(ReceiverRouteSelection::pause(
            route.current_selection,
            PolicyPauseReason::SourceBitrateLimit,
            AdaptationCounts::reset(),
        )),
        _ => Some(adaptation_hold(
            route.current_selection,
            AdaptationCounts::from_current(route.current_selection),
        )),
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
    route: &ReceiverVideoRouteInput<'_>,
) -> Option<(SourceSelector, Bitrate)> {
    let encodings = route.encodings();
    let cap = route.source.policy().video_bitrate_cap();
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
            let bitrate = encoding.max_bitrate()?;
            cap.is_none_or(|cap| bitrate <= cap)
                .then_some((SourceSelector::Encoding(encoding.encoding_id()), bitrate))
        })
}

fn highest_allowed_plan(route: &ReceiverVideoRouteInput<'_>) -> Option<ReceiverRouteSelection> {
    let encodings = route.encodings();
    let target_index =
        allowed_encoding_indices(encodings, route.source.policy().video_bitrate_cap())
            .next_back()?;
    let target_selector = SourceSelector::Encoding(encodings.get(target_index)?.encoding_id());
    Some(adaptation_send(
        target_selector,
        target_selector != route.current_selection.selector(),
    ))
}

fn scalable_plan(route: &ReceiverVideoRouteInput<'_>) -> Option<ReceiverRouteSelection> {
    let encodings = route.encodings();
    let source_cap = route.source.policy().video_bitrate_cap();
    if encodings.len() < 2 && source_cap.is_none() {
        return None;
    }
    let current = route.current_selection;
    let current_index = selector_index(current.selector(), encodings);
    let target_index = desired_encoding_index(route, source_cap, encodings)?;
    let target_selector = SourceSelector::Encoding(encodings.get(target_index)?.encoding_id());
    let request_keyframe = target_selector != current.selector();
    if target_index == current_index || route.receiver_bandwidth.is_none() {
        return Some(adaptation_send(target_selector, request_keyframe));
    }
    if target_index < current_index {
        if source_cap.is_some_and(|cap| selector_bitrate(encodings, current.selector()) > cap) {
            return Some(adaptation_send(target_selector, request_keyframe));
        }
        let counts = AdaptationCounts::next_pressure(
            current,
            super::hysteresis::DOWNSWITCH_PRESSURE_OBSERVATIONS,
        );
        if counts.pressure >= super::hysteresis::DOWNSWITCH_PRESSURE_OBSERVATIONS {
            return Some(adaptation_send(target_selector, request_keyframe));
        }
        return Some(adaptation_hold(current, counts));
    }
    let counts =
        AdaptationCounts::next_upgrade(current, super::hysteresis::UPSWITCH_STABLE_OBSERVATIONS);
    if counts.upgrade >= super::hysteresis::UPSWITCH_STABLE_OBSERVATIONS {
        return Some(adaptation_send(target_selector, true));
    }
    Some(adaptation_hold(current, counts))
}

const fn adaptation_send(
    selector: SourceSelector,
    request_keyframe: bool,
) -> ReceiverRouteSelection {
    ReceiverRouteSelection::send(selector, AdaptationCounts::reset(), request_keyframe)
}

const fn adaptation_hold(
    current: ConsumerSourceSelection,
    counts: AdaptationCounts,
) -> ReceiverRouteSelection {
    ReceiverRouteSelection::send(current.selector(), counts, false)
}

fn desired_encoding_index(
    route: &ReceiverVideoRouteInput<'_>,
    source_cap: Option<Bitrate>,
    encodings: SelectableRouteEncodings<'_>,
) -> Option<usize> {
    if route.user_count < MULTIPARTY_SCALABLE_VIDEO_SELECTION_THRESHOLD {
        return allowed_encoding_indices(encodings, source_cap).next_back();
    }
    let featured = route.layout_intent.uses_featured_quality();
    let Some(receiver_bandwidth) = route.receiver_bandwidth else {
        return if featured {
            allowed_encoding_indices(encodings, source_cap).next_back()
        } else {
            allowed_encoding_indices(encodings, source_cap).next()
        };
    };
    let budget = if featured {
        receiver_bandwidth
    } else {
        let divisor = u64::try_from(route.visible_scalable_route_count)
            .unwrap_or(u64::MAX)
            .saturating_mul(THUMBNAIL_BUDGET_DIVISOR)
            .max(1);
        receiver_bandwidth.divided_by(divisor)
    };
    highest_affordable_encoding_index(encodings, budget, featured, source_cap)
}

fn highest_affordable_encoding_index(
    encodings: SelectableRouteEncodings<'_>,
    budget: Bitrate,
    featured: bool,
    source_cap: Option<Bitrate>,
) -> Option<usize> {
    if encodings
        .iter()
        .all(|encoding| encoding.max_bitrate().is_none())
    {
        return source_cap.is_none().then_some(if featured {
            encodings.len().saturating_sub(1)
        } else {
            0
        });
    }
    encodings
        .iter()
        .enumerate()
        .filter(|(_index, encoding)| {
            encoding.max_bitrate().is_some_and(|bitrate| {
                bitrate <= budget && source_cap.is_none_or(|cap| bitrate <= cap)
            })
        })
        .last()
        .map(|(index, _encoding)| index)
        .or_else(|| allowed_encoding_indices(encodings, source_cap).next())
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

fn allowed_encoding_indices(
    encodings: SelectableRouteEncodings<'_>,
    cap: Option<Bitrate>,
) -> impl DoubleEndedIterator<Item = usize> + '_ {
    (0..encodings.len()).filter(move |index| {
        cap.is_none_or(|cap| {
            encodings
                .get(*index)
                .and_then(SourceEncodingDescriptor::max_bitrate)
                .is_some_and(|bitrate| bitrate <= cap)
        })
    })
}
