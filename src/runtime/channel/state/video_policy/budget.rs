//! Pure receiver video budget planner.
//!
//! The current planner preserves the existing RID-selection behavior: camera
//! routes in small rooms stay on the highest encoding, multiparty featured
//! cameras keep a higher floor, and thumbnails use receiver bandwidth plus
//! refresh-count hysteresis to decide when to downswitch or upswitch. Future
//! overload work should extend this module by selecting a whole receiver video
//! set before emitting pause actions.

use o_sfu_protocol::shared::StreamType;

use super::{
    action::{ConsumerPacketSelectionUpdate, ReceiverVideoRouteAction, VideoRouteAction},
    input::ReceiverVideoPolicyInput,
};
use crate::runtime::{
    source_model::{ConsumerSourceSelection, SourceEncodingDescriptor, SourceSelector},
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
pub(in crate::runtime::channel) struct ReceiverVideoBudgetPlan<'a> {
    route_actions: Vec<ReceiverVideoRouteAction<'a>>,
}

impl<'a> ReceiverVideoBudgetPlan<'a> {
    #[must_use]
    pub(in crate::runtime::channel) fn from_input(input: &'a ReceiverVideoPolicyInput<'a>) -> Self {
        let route_actions = input
            .routes()
            .iter()
            .filter_map(|route| {
                let adaptation = consumer_adaptation_plan(
                    route.session_count(),
                    route.stream_type(),
                    route.encodings(),
                    route.current_selection(),
                    route.layout_intent().is_featured(),
                    route.active_video_route_count(),
                    route.receiver_bandwidth_bps(),
                )?;
                Some(ReceiverVideoRouteAction::new(
                    (*route).clone(),
                    VideoRouteAction::Send(adaptation.selector),
                    adaptation.pressure_observations,
                    adaptation.upgrade_observations,
                    adaptation.request_keyframe,
                ))
            })
            .collect();
        Self { route_actions }
    }

    #[must_use]
    pub(in crate::runtime::channel) fn into_selection_updates(
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

impl super::super::shared::ChannelState {
    /// Plans deterministic per-consumer source selectors for live video routes.
    ///
    /// The snapshot inputs are best-effort transport observations. They do not
    /// change room authority on their own. This method combines them with
    /// committed source descriptors, subscription state and active-speaker
    /// layout state to build staged updates for the effect executor.
    pub(in crate::runtime::channel) fn consumer_packet_selection_updates(
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

fn consumer_adaptation_plan(
    session_count: usize,
    stream_type: StreamType,
    encodings: &[&SourceEncodingDescriptor],
    current: ConsumerSourceSelection,
    featured: bool,
    active_video_route_count: usize,
    receiver_bandwidth_bps: Option<u64>,
) -> Option<ConsumerAdaptationPlan> {
    if stream_type != StreamType::Camera {
        return None;
    }
    if encodings.len() < 2 {
        return None;
    }
    let current_index = selector_index(current.selector(), encodings);
    let target_index = desired_encoding_index(
        session_count,
        featured,
        active_video_route_count,
        receiver_bandwidth_bps,
        encodings,
    );
    let target_selector = SourceSelector::Encoding(encodings.get(target_index)?.encoding_id());
    if target_index == current_index {
        return Some(ConsumerAdaptationPlan {
            selector: target_selector,
            pressure_observations: 0,
            upgrade_observations: 0,
            request_keyframe: false,
        });
    }
    if receiver_bandwidth_bps.is_none() {
        return Some(ConsumerAdaptationPlan {
            selector: target_selector,
            pressure_observations: 0,
            upgrade_observations: 0,
            request_keyframe: target_index > current_index,
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
                request_keyframe: false,
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

fn desired_encoding_index(
    session_count: usize,
    featured: bool,
    active_video_route_count: usize,
    receiver_bandwidth_bps: Option<u64>,
    encodings: &[&SourceEncodingDescriptor],
) -> usize {
    let highest_index = encodings.len().saturating_sub(1);
    if session_count < MULTIPARTY_CAMERA_SIMULCAST_SELECTION_THRESHOLD {
        return highest_index;
    }
    let Some(receiver_bandwidth_bps) = receiver_bandwidth_bps else {
        return if featured { highest_index } else { 0 };
    };
    let budget_bps = if featured {
        receiver_bandwidth_bps
    } else {
        let divisor = u64::try_from(active_video_route_count)
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
