use super::{
    VideoAdmissionRank,
    receiver::{PlannedReceiverRoute, RouteOutcome},
};
use crate::engine::source_model::PolicyPauseReason;

pub(super) fn apply_video_download_limit(
    routes: &mut [PlannedReceiverRoute<'_>],
    max_video_downloads_per_receiver: usize,
) {
    if active_route_count(routes) <= max_video_downloads_per_receiver {
        return;
    }
    let mut ranked = routes
        .iter_mut()
        .filter(|route| route.selection.policy_pause_reason.is_none())
        .map(|route| (video_download_rank(route), route))
        .collect::<Vec<_>>();
    ranked.sort_by_key(|(rank, _)| *rank);
    for (_rank, route) in ranked.into_iter().skip(max_video_downloads_per_receiver) {
        route.pause(PolicyPauseReason::VideoDownloadLimit, RouteOutcome::Paused);
    }
}

pub(super) fn active_route_count(routes: &[PlannedReceiverRoute<'_>]) -> usize {
    routes
        .iter()
        .filter(|route| route.selection.policy_pause_reason.is_none())
        .count()
}

fn video_download_rank(route: &PlannedReceiverRoute<'_>) -> VideoAdmissionRank {
    let input = route.input;
    VideoAdmissionRank::new(
        input.layout_intent.priority(),
        input.active_speaker_rank,
        input.source_id(),
    )
}
