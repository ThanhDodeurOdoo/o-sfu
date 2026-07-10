//! receiver-video policy turn
//!
//! [`SourcePolicyTransaction`]: input -> adapt -> admit -> budget -> hysteresis -> projection
//! packet-gate changes stay behind [`projection`] so planning never builds transport gates directly

use std::collections::BTreeMap;

use super::{
    super::{input::SourcePolicySnapshot, turn::SourcePolicyTransaction},
    input::{ReceiverVideoRouteInput, receiver_video_routes},
    projection,
};
use crate::{
    Bitrate,
    engine::{
        UserId,
        media_transport::ReceiverBweTargetUpdate,
        room::state::RoomState,
        source_model::{
            ConsumerSourceSelection, OverBudgetExceptionReason, PolicyPauseReason,
            PublishedSourceDescriptor, PublishedSourceId, ReceiverVideoBudgetDiagnostics,
            SourceAdaptationPolicy, SourceEncodingDescriptor, SourceRoomPolicySelector,
            SourceRoutePriority, SourceSelector, UploadLayerPolicyRole,
        },
    },
};

/// room size where scalable video starts using receiver-bandwidth share
const MULTIPARTY_SCALABLE_VIDEO_SELECTION_THRESHOLD: usize = 3;
/// multiplier that halves each thumbnail share before layer selection
const THUMBNAIL_BUDGET_DIVISOR: u64 = 2;
/// consecutive pressure observations before a soft downswitch or budget pause
const DOWNSWITCH_PRESSURE_OBSERVATIONS: u8 = 2;
/// consecutive stable observations before a soft resume or upswitch
const UPSWITCH_STABLE_OBSERVATIONS: u8 = 3;

/// ordering key shared by policy refresh and pre-setup receiver admission
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct VideoAdmissionRank {
    /// lower values admit more important layout roles first
    priority: u8,
    /// active-speaker order, or `usize::MAX` for non-speakers
    active_speaker_rank: usize,
    /// deterministic tie-breaker after priority and speaker rank
    source_id: u64,
}

impl VideoAdmissionRank {
    pub const fn new(
        priority: SourceRoutePriority,
        active_speaker_rank: Option<usize>,
        source_id: PublishedSourceId,
    ) -> Self {
        Self {
            priority: match priority {
                SourceRoutePriority::PinnedOrFeatured => 0,
                SourceRoutePriority::ReadableDetail => 1,
                SourceRoutePriority::ActiveSpeaker => 2,
                SourceRoutePriority::VisibleThumbnail => 3,
                SourceRoutePriority::HiddenOrOverflow => 4,
            },
            active_speaker_rank: match active_speaker_rank {
                Some(rank) => rank,
                None => usize::MAX,
            },
            source_id: source_id.as_u64(),
        }
    }
}

/// hysteresis counters persisted in [`ConsumerSourceSelection`] between policy turns
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct AdaptationCounts {
    /// consecutive pressure observations toward a soft downswitch or pause
    pub(super) pressure: u8,
    /// consecutive stable observations toward a soft resume or upswitch
    pub(super) upgrade: u8,
}

impl AdaptationCounts {
    const fn reset() -> Self {
        Self {
            pressure: 0,
            upgrade: 0,
        }
    }

    pub(super) fn from_current(selection: ConsumerSourceSelection) -> Self {
        Self {
            pressure: selection.pressure_observations(),
            upgrade: selection.upgrade_observations(),
        }
    }

    fn next_pressure(selection: ConsumerSourceSelection, limit: u8) -> Self {
        Self {
            pressure: selection
                .pressure_observations()
                .saturating_add(1)
                .min(limit),
            upgrade: 0,
        }
    }

    fn next_upgrade(selection: ConsumerSourceSelection, limit: u8) -> Self {
        Self {
            pressure: 0,
            upgrade: selection
                .upgrade_observations()
                .saturating_add(1)
                .min(limit),
        }
    }
}

/// candidate selector and policy pause state before projection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ReceiverRouteSelection {
    /// packet selector to commit if the route stays deliverable
    pub(super) selector: SourceSelector,
    /// policy reason that keeps receiver intent while blocking delivery
    pub(super) policy_pause_reason: Option<PolicyPauseReason>,
    /// hysteresis state carried into the next policy turn
    pub(super) counts: AdaptationCounts,
    /// decoder refresh requested after selector changes or delivery resumes
    pub(super) request_keyframe: bool,
}

impl ReceiverRouteSelection {
    const fn send(
        selector: SourceSelector,
        counts: AdaptationCounts,
        request_keyframe: bool,
    ) -> Self {
        Self {
            selector,
            policy_pause_reason: None,
            counts,
            request_keyframe,
        }
    }

    const fn pause(
        current: ConsumerSourceSelection,
        reason: PolicyPauseReason,
        counts: AdaptationCounts,
    ) -> Self {
        Self {
            selector: current.selector(),
            policy_pause_reason: Some(reason),
            counts,
            request_keyframe: false,
        }
    }

    const fn hold(
        current: ConsumerSourceSelection,
        policy_pause_reason: Option<PolicyPauseReason>,
        counts: AdaptationCounts,
    ) -> Self {
        Self {
            selector: current.selector(),
            policy_pause_reason,
            counts,
            request_keyframe: false,
        }
    }
}

/// metric outcome produced by budget and admission decisions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RouteOutcome {
    /// no downgrade or pause recorded by the video solver
    Neutral,
    /// selected layer is lower than the previous committed selector
    Degraded,
    /// route is paused by video policy
    Paused,
}

/// planned receiver route passed to [`projection`] after admission and budget pressure
///
/// `selected_bitrate`, `selection` and `outcome` must change together through
/// [`Self::send`] or [`Self::pause`]
#[derive(Debug, Clone, Copy)]
pub(super) struct PlannedReceiverRoute<'a> {
    /// immutable route facts captured before policy mutation
    pub(super) input: &'a ReceiverVideoRouteInput<'a>,
    /// bitrate counted against receiver budget after this policy step
    pub(super) selected_bitrate: Bitrate,
    /// candidate selector, pause state and hysteresis state
    pub(super) selection: ReceiverRouteSelection,
    /// diagnostic outcome attached to projection
    pub(super) outcome: RouteOutcome,
}

impl<'a> PlannedReceiverRoute<'a> {
    fn new(input: &'a ReceiverVideoRouteInput<'a>, selection: ReceiverRouteSelection) -> Self {
        let selected_bitrate = selector_bitrate(input.source, selection.selector);
        let current_bitrate = selector_bitrate(input.source, input.current_selection.selector());
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

    fn send(&mut self, selector: SourceSelector, selected_bitrate: Bitrate, outcome: RouteOutcome) {
        self.selected_bitrate = selected_bitrate;
        self.selection = ReceiverRouteSelection::send(
            selector,
            self.selection.counts,
            self.selection.request_keyframe,
        );
        self.outcome = outcome;
    }

    fn pause(&mut self, reason: PolicyPauseReason, outcome: RouteOutcome) {
        self.selected_bitrate = Bitrate::zero();
        self.selection = ReceiverRouteSelection::pause(
            self.input.current_selection,
            reason,
            self.selection.counts,
        );
        self.outcome = outcome;
    }
}

pub(in crate::engine::room::source_policy) fn append_receiver_video_policy(
    tx: &mut SourcePolicyTransaction,
    state: &RoomState,
    input: &SourcePolicySnapshot<'_>,
    mut receiver_bwe_targets: BTreeMap<UserId, ReceiverBweTargetUpdate>,
) {
    let routes = receiver_video_routes(state, input);
    let max_video_downloads_per_receiver = input.media_limits.max_video_downloads_per_receiver();
    for receiver_routes in routes.chunk_by(|left, right| {
        left.transport_ref.consumer_user_id == right.transport_ref.consumer_user_id
    }) {
        let Some(first_route) = receiver_routes.first() else {
            continue;
        };
        let consumer_user_id = &first_route.transport_ref.consumer_user_id;
        let receiver_bwe_target = append_receiver_policy_updates(
            tx,
            state,
            receiver_routes,
            max_video_downloads_per_receiver,
        );
        if let Some(update) = receiver_bwe_targets.get_mut(consumer_user_id) {
            update.set_target(receiver_bwe_target);
        }
    }
    tx.set_receiver_bwe_targets(receiver_bwe_targets.into_values().collect());
}

fn append_receiver_policy_updates<'a>(
    tx: &mut SourcePolicyTransaction,
    state: &RoomState,
    receiver_routes: &'a [ReceiverVideoRouteInput<'a>],
    max_video_downloads_per_receiver: usize,
) -> Bitrate {
    let receiver_bandwidth = receiver_routes
        .iter()
        .find_map(|route| route.receiver_bandwidth);
    let mut planned_routes = receiver_routes
        .iter()
        .filter_map(|route| {
            route_plan(route).map(|selection| PlannedReceiverRoute::new(route, selection))
        })
        .collect::<Vec<_>>();
    apply_video_download_limit(&mut planned_routes, max_video_downloads_per_receiver);
    if let Some(receiver_bandwidth) = receiver_bandwidth {
        apply_overload_policy(&mut planned_routes, receiver_bandwidth);
    }
    let budget_diagnostics = receiver_video_budget_diagnostics(&planned_routes, receiver_bandwidth);
    for planned_route in planned_routes {
        let selection = resolve_hysteresis(&planned_route);
        let Some(update) = projection::consumer_packet_selection_update(
            &planned_route,
            selection,
            budget_diagnostics,
        ) else {
            continue;
        };
        if update.requires_media_transport_effect() {
            let target = state.topology.consumer_route_target_for_source(
                &update.transport_ref,
                planned_route.input.source,
            );
            tx.push_route_update(update, &target);
        } else {
            tx.push_state_update(update);
        }
    }
    budget_diagnostics.selected_video_bitrate()
}

fn route_plan(route: &ReceiverVideoRouteInput<'_>) -> Option<ReceiverRouteSelection> {
    let policy = route.source.policy().adaptation();
    let source_cap = route.source.policy().video_bitrate_cap();
    match policy {
        SourceAdaptationPolicy::ReadableDetail
            if route.source.selectable_encoding_count() < 2 && source_cap.is_none() =>
        {
            None
        }
        SourceAdaptationPolicy::ReadableDetail => highest_allowed_plan(route),
        SourceAdaptationPolicy::ScalableVideo => scalable_plan(route),
        SourceAdaptationPolicy::None if source_cap.is_some() => highest_allowed_plan(route),
        SourceAdaptationPolicy::None => None,
    }
    .or_else(|| match policy {
        SourceAdaptationPolicy::None if source_cap.is_none() => None,
        _ if source_cap.is_some() => Some(ReceiverRouteSelection::pause(
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

fn selector_bitrate(source: &PublishedSourceDescriptor, selector: SourceSelector) -> Bitrate {
    selector
        .selected_encoding()
        .and_then(|encoding_id| {
            source
                .selectable_encodings()
                .find(|encoding| encoding.encoding_id() == encoding_id)
                .and_then(SourceEncodingDescriptor::max_bitrate)
        })
        .or_else(|| {
            source
                .selectable_encodings()
                .filter_map(SourceEncodingDescriptor::max_bitrate)
                .max()
        })
        .unwrap_or_default()
}

fn cheapest_useful_selector(
    route: &ReceiverVideoRouteInput<'_>,
) -> Option<(SourceSelector, Bitrate)> {
    let source_cap = route.source.policy().video_bitrate_cap();
    route
        .source
        .selectable_encodings()
        .filter(|encoding| {
            !matches!(
                encoding.policy_role(),
                Some(UploadLayerPolicyRole::Featured)
            )
        })
        .chain(route.source.selectable_encodings())
        .find_map(|encoding| {
            let bitrate = encoding.max_bitrate()?;
            source_cap
                .is_none_or(|source_cap| bitrate <= source_cap)
                .then_some((SourceSelector::Encoding(encoding.encoding_id()), bitrate))
        })
}

fn highest_allowed_plan(route: &ReceiverVideoRouteInput<'_>) -> Option<ReceiverRouteSelection> {
    let target_index =
        allowed_encoding_indices(route.source, route.source.policy().video_bitrate_cap())
            .next_back()?;
    let target_selector = SourceSelector::Encoding(
        route
            .source
            .selectable_encoding_by_rank(target_index)?
            .encoding_id(),
    );
    Some(adaptation_send(
        target_selector,
        target_selector != route.current_selection.selector(),
    ))
}

fn scalable_plan(route: &ReceiverVideoRouteInput<'_>) -> Option<ReceiverRouteSelection> {
    let source_cap = route.source.policy().video_bitrate_cap();
    if route.source.selectable_encoding_count() < 2 && source_cap.is_none() {
        return None;
    }
    let current = route.current_selection;
    let current_index = selector_index(route.source, current.selector());
    let target_index = desired_encoding_index(route, source_cap)?;
    let target_selector = SourceSelector::Encoding(
        route
            .source
            .selectable_encoding_by_rank(target_index)?
            .encoding_id(),
    );
    let request_keyframe = target_selector != current.selector();
    if target_index == current_index || route.receiver_bandwidth.is_none() {
        return Some(adaptation_send(target_selector, request_keyframe));
    }
    if target_index < current_index {
        if source_cap.is_some_and(|cap| selector_bitrate(route.source, current.selector()) > cap) {
            return Some(adaptation_send(target_selector, request_keyframe));
        }
        let counts = AdaptationCounts::next_pressure(current, DOWNSWITCH_PRESSURE_OBSERVATIONS);
        if counts.pressure >= DOWNSWITCH_PRESSURE_OBSERVATIONS {
            return Some(adaptation_send(target_selector, request_keyframe));
        }
        return Some(adaptation_hold(current, counts));
    }
    let counts = AdaptationCounts::next_upgrade(current, UPSWITCH_STABLE_OBSERVATIONS);
    if counts.upgrade >= UPSWITCH_STABLE_OBSERVATIONS {
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
) -> Option<usize> {
    if route.user_count < MULTIPARTY_SCALABLE_VIDEO_SELECTION_THRESHOLD {
        return allowed_encoding_indices(route.source, source_cap).next_back();
    }
    let uses_featured_quality = route.layout_intent.uses_featured_quality();
    let Some(receiver_bandwidth) = route.receiver_bandwidth else {
        return if uses_featured_quality {
            allowed_encoding_indices(route.source, source_cap).next_back()
        } else {
            allowed_encoding_indices(route.source, source_cap).next()
        };
    };
    let budget = if uses_featured_quality || route.visible_scalable_route_count <= 1 {
        receiver_bandwidth
    } else {
        let divisor = u64::try_from(route.visible_scalable_route_count)
            .unwrap_or(u64::MAX)
            .saturating_mul(THUMBNAIL_BUDGET_DIVISOR);
        receiver_bandwidth.divided_by(divisor)
    };
    highest_affordable_encoding_index(route.source, budget, uses_featured_quality, source_cap)
}

fn highest_affordable_encoding_index(
    source: &PublishedSourceDescriptor,
    budget: Bitrate,
    uses_featured_quality: bool,
    source_cap: Option<Bitrate>,
) -> Option<usize> {
    if source
        .selectable_encodings()
        .all(|encoding| encoding.max_bitrate().is_none())
    {
        return source_cap.is_none().then_some(if uses_featured_quality {
            source.selectable_encoding_count().saturating_sub(1)
        } else {
            0
        });
    }
    source
        .selectable_encodings()
        .enumerate()
        .filter(|(_index, encoding)| {
            encoding.max_bitrate().is_some_and(|bitrate| {
                bitrate <= budget && source_cap.is_none_or(|cap| bitrate <= cap)
            })
        })
        .last()
        .map(|(index, _encoding)| index)
        .or_else(|| allowed_encoding_indices(source, source_cap).next())
}

fn selector_index(source: &PublishedSourceDescriptor, selector: SourceSelector) -> usize {
    selector
        .selected_encoding()
        .and_then(|encoding_id| {
            source
                .selectable_encodings()
                .position(|encoding| encoding.encoding_id() == encoding_id)
        })
        .unwrap_or_else(|| source.selectable_encoding_count().saturating_sub(1))
}

fn allowed_encoding_indices(
    source: &PublishedSourceDescriptor,
    source_cap: Option<Bitrate>,
) -> impl DoubleEndedIterator<Item = usize> + '_ {
    (0..source.selectable_encoding_count()).filter(move |index| {
        source_cap.is_none_or(|source_cap| {
            source
                .selectable_encoding_by_rank(*index)
                .and_then(SourceEncodingDescriptor::max_bitrate)
                .is_some_and(|bitrate| bitrate <= source_cap)
        })
    })
}

fn apply_video_download_limit(
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

fn active_route_count(routes: &[PlannedReceiverRoute<'_>]) -> usize {
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
        input.source.source_id(),
    )
}

fn apply_overload_policy(routes: &mut [PlannedReceiverRoute<'_>], receiver_bandwidth: Bitrate) {
    let mut total_bitrate = selected_active_video_bitrate(routes);
    if total_bitrate <= receiver_bandwidth {
        return;
    }
    for route in routes.iter_mut().filter(|route| route_can_downgrade(route)) {
        let Some((selector, bitrate)) = cheapest_useful_selector(route.input) else {
            let selected_bitrate = route.selected_bitrate;
            route.pause(PolicyPauseReason::MissingUsableLayer, RouteOutcome::Neutral);
            total_bitrate = total_bitrate.saturating_sub(selected_bitrate);
            continue;
        };
        if bitrate < route.selected_bitrate {
            let selected_bitrate = route.selected_bitrate;
            total_bitrate = total_bitrate
                .saturating_sub(selected_bitrate)
                .saturating_add(bitrate);
            route.send(selector, bitrate, RouteOutcome::Degraded);
        }
    }
    if total_bitrate <= receiver_bandwidth {
        return;
    }
    let mut pause_order = Vec::with_capacity(routes.len());
    for route in routes.iter_mut() {
        if route.selection.policy_pause_reason.is_some() {
            continue;
        }
        let Some(rank) = pausable_route_rank(route) else {
            continue;
        };
        pause_order.push((rank, route));
    }
    pause_order.sort_by_key(|(rank, _)| *rank);
    for (_rank, route) in pause_order {
        if total_bitrate <= receiver_bandwidth {
            break;
        }
        let selected_bitrate = route.selected_bitrate;
        let pause_reason = pause_reason_for_route(route);
        route.pause(pause_reason, RouteOutcome::Paused);
        total_bitrate = total_bitrate.saturating_sub(selected_bitrate);
    }
}

fn receiver_video_budget_diagnostics(
    routes: &[PlannedReceiverRoute<'_>],
    receiver_bandwidth: Option<Bitrate>,
) -> ReceiverVideoBudgetDiagnostics {
    let selected_video_bitrate = selected_active_video_bitrate(routes);
    let over_budget_exception_reason = receiver_bandwidth
        .filter(|budget| selected_video_bitrate > *budget)
        .map(|_budget| OverBudgetExceptionReason::ProtectedRoute);
    ReceiverVideoBudgetDiagnostics::new(
        receiver_bandwidth,
        receiver_bandwidth,
        active_route_count(routes),
        selected_video_bitrate,
        over_budget_exception_reason,
    )
}

fn selected_active_video_bitrate(routes: &[PlannedReceiverRoute<'_>]) -> Bitrate {
    routes
        .iter()
        .filter(|route| route.selection.policy_pause_reason.is_none())
        .fold(Bitrate::zero(), |total, route| {
            total.saturating_add(route.selected_bitrate)
        })
}

fn route_can_downgrade(route: &PlannedReceiverRoute<'_>) -> bool {
    let input = route.input;
    route.selection.policy_pause_reason.is_none()
        && input.source.policy().adaptation() == SourceAdaptationPolicy::ScalableVideo
        && matches!(
            input.layout_intent.priority(),
            SourceRoutePriority::VisibleThumbnail | SourceRoutePriority::HiddenOrOverflow
        )
}

fn pausable_route_rank(route: &PlannedReceiverRoute<'_>) -> Option<u8> {
    match route.input.layout_intent.priority() {
        SourceRoutePriority::HiddenOrOverflow => Some(0),
        SourceRoutePriority::VisibleThumbnail => Some(1),
        SourceRoutePriority::ActiveSpeaker
        | SourceRoutePriority::ReadableDetail
        | SourceRoutePriority::PinnedOrFeatured => None,
    }
}

fn pause_reason_for_route(route: &PlannedReceiverRoute<'_>) -> PolicyPauseReason {
    let intent = route.input.layout_intent;
    match intent.priority() {
        SourceRoutePriority::HiddenOrOverflow => match intent.role() {
            SourceRoomPolicySelector::Hidden => PolicyPauseReason::HiddenTile,
            SourceRoomPolicySelector::Overflow => PolicyPauseReason::OverflowTile,
            _ => PolicyPauseReason::BudgetPressure,
        },
        _ => PolicyPauseReason::BudgetPressure,
    }
}

fn resolve_hysteresis(route: &PlannedReceiverRoute<'_>) -> ReceiverRouteSelection {
    let current = route.input.current_selection;
    let current_pause_reason = current.policy_pause_reason();
    let selection = route.selection;
    match (selection.policy_pause_reason, current_pause_reason) {
        (None, Some(PolicyPauseReason::VideoDownloadLimit)) => {
            ReceiverRouteSelection::send(selection.selector, AdaptationCounts::reset(), true)
        }
        (None, Some(reason)) => {
            let counts = AdaptationCounts::next_upgrade(current, UPSWITCH_STABLE_OBSERVATIONS);
            if counts.upgrade >= UPSWITCH_STABLE_OBSERVATIONS {
                ReceiverRouteSelection::send(selection.selector, AdaptationCounts::reset(), true)
            } else {
                ReceiverRouteSelection::hold(current, Some(reason), counts)
            }
        }
        (Some(reason), pause_reason) if pause_reason != Some(reason) => {
            if matches!(
                reason,
                PolicyPauseReason::VideoDownloadLimit | PolicyPauseReason::SourceBitrateLimit
            ) {
                return ReceiverRouteSelection::pause(current, reason, AdaptationCounts::reset());
            }
            let counts = AdaptationCounts::next_pressure(current, DOWNSWITCH_PRESSURE_OBSERVATIONS);
            if counts.pressure >= DOWNSWITCH_PRESSURE_OBSERVATIONS {
                ReceiverRouteSelection::pause(current, reason, AdaptationCounts::reset())
            } else {
                ReceiverRouteSelection::hold(current, None, counts)
            }
        }
        _ => selection,
    }
}
