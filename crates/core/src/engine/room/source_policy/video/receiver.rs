use super::{
    super::action::ConsumerPacketSelectionUpdate, adaptation, admission, budget, hysteresis,
    input::ReceiverVideoRouteInput, projection, selection::ReceiverRouteSelection,
};
use crate::{
    Bitrate,
    engine::source_model::{PolicyPauseReason, SourceSelector},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RouteOutcome {
    Neutral,
    Degraded,
    Paused,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PlannedReceiverRoute<'a> {
    pub(super) route: &'a ReceiverVideoRouteInput<'a>,
    pub(super) selected_bitrate: Bitrate,
    pub(super) selection: ReceiverRouteSelection,
    pub(super) outcome: RouteOutcome,
}

impl<'a> PlannedReceiverRoute<'a> {
    fn new(route: &'a ReceiverVideoRouteInput<'a>, selection: ReceiverRouteSelection) -> Self {
        let selected_bitrate = adaptation::selector_bitrate(route.encodings(), selection.selector);
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
            selection,
            outcome,
        }
    }

    pub(super) fn send(
        &mut self,
        selector: SourceSelector,
        selected_bitrate: Bitrate,
        outcome: RouteOutcome,
    ) {
        self.selected_bitrate = selected_bitrate;
        self.selection = ReceiverRouteSelection::send(
            selector,
            self.selection.counts,
            self.selection.request_keyframe,
        );
        self.outcome = outcome;
    }

    pub(super) fn pause(&mut self, reason: PolicyPauseReason, outcome: RouteOutcome) {
        self.selected_bitrate = Bitrate::zero();
        self.selection = ReceiverRouteSelection::pause(
            self.route.current_selection,
            reason,
            self.selection.counts,
        );
        self.outcome = outcome;
    }
}

#[derive(Debug)]
pub(super) struct ReceiverRoutesPlan {
    pub(super) selection_updates: Vec<ConsumerPacketSelectionUpdate>,
    pub(super) receiver_bwe_target: Bitrate,
}

pub(super) fn plan<'a>(
    routes: &'a [ReceiverVideoRouteInput<'a>],
    max_video_downloads_per_receiver: usize,
) -> ReceiverRoutesPlan {
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
            let selection = hysteresis::resolve(&route);
            projection::consumer_packet_selection_update(route, selection, diagnostics)
        })
        .collect();
    ReceiverRoutesPlan {
        selection_updates,
        receiver_bwe_target,
    }
}
