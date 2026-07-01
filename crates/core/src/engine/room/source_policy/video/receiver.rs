use super::{
    super::turn::SourcePolicyTransaction, adaptation, admission, budget, hysteresis,
    input::ReceiverVideoRouteInput, projection, selection::ReceiverRouteSelection,
};
use crate::{
    Bitrate,
    engine::{
        room::state::RoomState,
        source_model::{PolicyPauseReason, SourceSelector},
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RouteOutcome {
    Neutral,
    Degraded,
    Paused,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PlannedReceiverRoute<'a> {
    pub(super) input: &'a ReceiverVideoRouteInput<'a>,
    pub(super) selected_bitrate: Bitrate,
    pub(super) selection: ReceiverRouteSelection,
    pub(super) outcome: RouteOutcome,
}

impl<'a> PlannedReceiverRoute<'a> {
    fn new(input: &'a ReceiverVideoRouteInput<'a>, selection: ReceiverRouteSelection) -> Self {
        let selected_bitrate = adaptation::selector_bitrate(input.encodings(), selection.selector);
        let current_bitrate =
            adaptation::selector_bitrate(input.encodings(), input.current_selection.selector());
        let outcome = if selected_bitrate < current_bitrate {
            RouteOutcome::Degraded
        } else {
            RouteOutcome::Neutral
        };
        Self {
            input,
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
            self.input.current_selection,
            reason,
            self.selection.counts,
        );
        self.outcome = outcome;
    }
}

pub(super) fn append_policy_updates<'a>(
    tx: &mut SourcePolicyTransaction,
    state: &RoomState,
    routes: &'a [ReceiverVideoRouteInput<'a>],
    max_video_downloads_per_receiver: usize,
) -> Bitrate {
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
    for planned in planned_routes {
        let selection = hysteresis::resolve(&planned);
        let Some(update) =
            projection::consumer_packet_selection_update(&planned, selection, diagnostics)
        else {
            continue;
        };
        if update.requires_media_transport_effect() {
            let target = state.topology.consumer_route_target_for_source(
                update.transport_ref.clone(),
                planned.input.source,
            );
            tx.push_route_update(update, &target);
        } else {
            tx.push_state_update(update);
        }
    }
    diagnostics.selected_video_bitrate()
}
