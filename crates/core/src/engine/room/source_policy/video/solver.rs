//! receiver-video policy turn
//!
//! [`SourcePolicyTransaction`]: filter -> budget -> plan -> admit -> fit -> hysteresis -> projection
//! packet-gate changes stay behind [`projection`] so planning never builds transport gates directly

use std::{cmp::Reverse, collections::BTreeMap};

use itertools::Itertools;

use super::{
    super::{
        action::{ReceiverVideoBudgetPlan, VideoRouteAllocation, VideoRouteAllocationState},
        input::SourcePolicySnapshot,
        turn::SourcePolicyTransaction,
    },
    input::{ReceiverVideoRouteInput, receiver_video_routes},
    projection,
};
use crate::{
    Bitrate, VideoAdaptationTuning,
    engine::{
        UserId,
        media_transport::ReceiverBweTargetUpdate,
        room::state::RoomState,
        source_model::{
            ConsumerSourceSelection, PolicyPauseReason, PublishedSourceDescriptor,
            PublishedSourceId, ReceiverVideoBudgetDiagnostics, SourceAdaptationPolicy,
            SourceEncodingDescriptor, SourceRoomPolicySelector, SourceRoutePriority,
            SourceSelector, UploadLayerPolicyRole,
        },
    },
};

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

/// planned receiver route passed to [`projection`] after admission and budget pressure
///
/// `selected_bitrate` and `selection` must change together through
/// [`Self::apply_resolved_selection`], [`Self::send`] or [`Self::pause`]
#[derive(Debug, Clone, Copy)]
pub(super) struct PlannedReceiverRoute<'a> {
    /// immutable route facts captured before policy mutation
    pub(super) input: &'a ReceiverVideoRouteInput<'a>,
    /// bitrate counted against receiver budget after this policy step
    pub(super) selected_bitrate: Bitrate,
    /// candidate selector, pause state and hysteresis state
    pub(super) selection: ReceiverRouteSelection,
}

impl<'a> PlannedReceiverRoute<'a> {
    fn new(input: &'a ReceiverVideoRouteInput<'a>, selection: ReceiverRouteSelection) -> Self {
        let selected_bitrate = if selection.policy_pause_reason.is_some() {
            Bitrate::zero()
        } else {
            selector_bitrate(input, selection.selector).unwrap_or_default()
        };
        Self {
            input,
            selected_bitrate,
            selection,
        }
    }

    fn apply_resolved_selection(&mut self, selection: ReceiverRouteSelection) {
        *self = Self::new(self.input, selection);
    }

    fn send(&mut self, selector: SourceSelector, selected_bitrate: Bitrate) {
        self.selected_bitrate = selected_bitrate;
        self.selection = ReceiverRouteSelection::send(
            selector,
            self.selection.counts,
            self.selection.request_keyframe,
        );
    }

    fn pause(&mut self, reason: PolicyPauseReason) {
        self.selected_bitrate = Bitrate::zero();
        self.selection = ReceiverRouteSelection::pause(
            self.input.current_selection,
            reason,
            self.selection.counts,
        );
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
    let tuning = input.video_adaptation_tuning;
    // `committed_consumer_routes` is ordered by `SubscriptionKey` with
    // `receiver` first. Preserve that order so `chunk_by` sees each complete
    // receiver allocation.
    for receiver_routes in routes.chunk_by(|left, right| left.key.receiver == right.key.receiver) {
        let Some(first_route) = receiver_routes.first() else {
            continue;
        };
        let consumer_user_id = &first_route.key.receiver;
        let video_demand = append_receiver_policy_updates(
            tx,
            receiver_routes,
            max_video_downloads_per_receiver,
            tuning,
        );
        // The target is seeded with this receiver's audio reserve. Add eventual
        // admitted video demand so str0m can probe while overload pauses routes.
        if let Some(update) = receiver_bwe_targets.get_mut(consumer_user_id) {
            update.set_target(update.target().saturating_add(video_demand));
        }
    }
    tx.set_receiver_bwe_targets(receiver_bwe_targets.into_values().collect());
}

/// Solves video subscription quality for a receiver based on layout intent and network capacity.
///
/// [`receiver_video_routes`] first retains video routes with an adaptation policy or source bitrate
/// cap. This function computes the optional video budget before calling `route_plan` for each
/// retained route.
///
/// Balances visual experience against available bandwidth through a multi-pass allocation:
///
/// 1. **Desired Quality Target**: Builds each route's selection from its policy-specific facts.
///    An unpaused selector with no bitrate is paused as
///    [`PolicyPauseReason::MissingUsableLayer`].
/// 2. **Stream Count Capping**: Enforces the receiver download limit across planned routes. Lowest-priority
///    streams are paused before bandwidth sharing. Equal ranks use active-route position.
/// 3. **Bandwidth Fitting**: When aggregate bitrate exceeds optional downlink headroom after the
///    audio reserve, secondary streams are stepped down layer-by-layer. If demand still exceeds
///    budget after eligible step-downs, the lowest-priority streams are paused. Each pass stops once
///    demand fits.
/// 4. **Hysteresis Smoothing**: `scalable_plan` applies selector hysteresis. `resolve_hysteresis`
///    delays soft pauses and most resumes. New download-limit and source-cap pauses apply
///    immediately. A route resuming from [`PolicyPauseReason::VideoDownloadLimit`] also resumes
///    immediately.
/// 5. **Decision Emission**: Projection returns an optional packet-selection update. The caller
///    stages it as a transport effect or state-only update.
///
/// ```text
///      Policy-Managed Video Routes                Downlink Bandwidth Estimate
///                    \                                   /
///                     v                                 v
///          +-------------------------------------------------------+
///          | 1. Desired Quality Target                             |
///          |    - Multiparty featured --> featured-quality layer   |
///          |    - Multiparty secondary --> thumbnail-biased layer  |
///          +-------------------------------------------------------+
///                                     |
///                                     v
///          +-------------------------------------------------------+
///          | 2. Stream Count Capping (Top-N)                       |
///          |    - Active planned streams > receiver limit?         |
///          |      yes -> pause lowest-ranked excess positions      |
///          |      no  -> keep planned streams active               |
///          +-------------------------------------------------------+
///                                     |
///                                     v
///          +-------------------------------------------------------+
///          | 3. Bandwidth Fitting & Degradation                    |
///          |    - Total bitrate <= bandwidth budget?               |
///          |      yes -> fit as-is                                 |
///          |      no  -> step down eligible layers one-by-one      |
///          |             pause lowest-priority tiles if congested  |
///          +-------------------------------------------------------+
///                                     |
///                                     v
///          +-------------------------------------------------------+
///          | 4. Hysteresis Smoothing                               |
///          |    - Smooth selector changes and route oscillation    |
///          |    - Delay soft pauses and most resumes               |
///          +-------------------------------------------------------+
///                                     |
///                                     v
///              Selection Updates + Receiver BWE Demand
/// ```
fn append_receiver_policy_updates<'a>(
    tx: &mut SourcePolicyTransaction,
    receiver_routes: &'a [ReceiverVideoRouteInput<'a>],
    max_video_downloads_per_receiver: usize,
    tuning: VideoAdaptationTuning,
) -> Bitrate {
    let Some(first_route) = receiver_routes.first() else {
        return Bitrate::zero();
    };
    let receiver_bandwidth = receiver_routes
        .iter()
        .find_map(|route| route.receiver_bandwidth);
    let audio_reserve = first_route.audio_budget_reserve;
    let video_budget = receiver_bandwidth
        .map(|bandwidth| effective_video_budget(bandwidth, tuning, audio_reserve));
    let mut planned_routes = Vec::with_capacity(receiver_routes.len());
    for route in receiver_routes {
        if let Some(selection) = route_plan(route, tuning) {
            planned_routes.push(PlannedReceiverRoute::new(route, selection));
        }
    }
    // Apply the hard route count before sharing bandwidth. Rejected routes must
    // not consume receiver budget or force admitted routes down.
    apply_video_download_limit(&mut planned_routes, max_video_downloads_per_receiver);
    // str0m caps probes from the desired target. Use each hard-admitted route's
    // highest allowed layer so current BWE cannot bound future discovery.
    let eventual_admitted_video_bitrate = admitted_video_bwe_demand(&planned_routes);
    if let Some(video_budget) = video_budget {
        apply_overload_policy(&mut planned_routes, video_budget);
    }
    for route in &mut planned_routes {
        let selection = resolve_hysteresis(route, tuning);
        route.apply_resolved_selection(selection);
    }
    let planned_budget =
        receiver_video_budget_diagnostics(&planned_routes, receiver_bandwidth, video_budget);
    if requires_budget_reconciliation(&planned_routes, planned_budget) {
        let mut planned_index = 0;
        let routes = receiver_routes
            .iter()
            .map(|input| {
                let resolved = planned_routes
                    .get(planned_index)
                    .filter(|route| route.input.key == input.key);
                if resolved.is_some() {
                    planned_index += 1;
                }
                video_route_allocation(input, resolved)
            })
            .collect();
        debug_assert_eq!(planned_index, planned_routes.len());
        tx.push_receiver_video_budget_plan(ReceiverVideoBudgetPlan {
            receiver: first_route.key.receiver.clone(),
            planned_budget,
            routes,
        });
    }
    for planned_route in planned_routes {
        let Some(update) =
            projection::consumer_packet_selection_update(&planned_route, planned_budget)
        else {
            continue;
        };
        if update.requires_media_transport_effect() {
            tx.push_route_update(update);
        } else {
            tx.push_state_update(update);
        }
    }
    eventual_admitted_video_bitrate.max(planned_budget.selected_video_bitrate())
}

fn requires_budget_reconciliation(
    routes: &[PlannedReceiverRoute<'_>],
    planned_budget: ReceiverVideoBudgetDiagnostics,
) -> bool {
    routes.iter().any(|route| {
        let current = route.input.current_selection;
        current.budget() != planned_budget
            || current.selector() != route.selection.selector
            || current.policy_pause_reason() != route.selection.policy_pause_reason
    })
}

fn video_route_allocation(
    input: &ReceiverVideoRouteInput<'_>,
    resolved: Option<&PlannedReceiverRoute<'_>>,
) -> VideoRouteAllocation {
    let captured = input.current_selection;
    let captured_selected_bitrate = if captured.policy_pause_reason().is_some() {
        Bitrate::zero()
    } else {
        selector_bitrate(input, captured.selector()).unwrap_or_default()
    };
    VideoRouteAllocation {
        key: input.key.clone(),
        source_id: input.source.source_id(),
        route: input.route.clone(),
        captured: VideoRouteAllocationState {
            selector: captured.selector(),
            policy_pause_reason: captured.policy_pause_reason(),
            selected_bitrate: captured_selected_bitrate,
        },
        planned: resolved.map(|route| VideoRouteAllocationState {
            selector: route.selection.selector,
            policy_pause_reason: route.selection.policy_pause_reason,
            selected_bitrate: route.selected_bitrate,
        }),
    }
}

fn route_plan(
    route: &ReceiverVideoRouteInput<'_>,
    tuning: VideoAdaptationTuning,
) -> Option<ReceiverRouteSelection> {
    let policy = route.source.policy().adaptation();
    let source_cap = route.source.policy().video_bitrate_cap();
    if source_cap.is_some_and(|cap| source_exceeds_bitrate_cap(route, cap)) {
        return Some(ReceiverRouteSelection::pause(
            route.current_selection,
            PolicyPauseReason::SourceBitrateLimit,
            AdaptationCounts::reset(),
        ));
    }
    let selection = match policy {
        SourceAdaptationPolicy::ReadableDetail
            if route.source.selectable_encoding_count() < 2 && source_cap.is_none() =>
        {
            None
        }
        SourceAdaptationPolicy::ReadableDetail => highest_allowed_plan(route),
        SourceAdaptationPolicy::ScalableVideo => scalable_plan(route, tuning),
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
    })?;
    if selection.policy_pause_reason.is_none()
        && selector_bitrate(route, selection.selector).is_none()
    {
        return Some(ReceiverRouteSelection::pause(
            route.current_selection,
            PolicyPauseReason::MissingUsableLayer,
            AdaptationCounts::reset(),
        ));
    }
    Some(selection)
}

fn source_exceeds_bitrate_cap(route: &ReceiverVideoRouteInput<'_>, cap: Bitrate) -> bool {
    if route.source.selectable_encoding_count() > 1 {
        return false;
    }
    // Missing data cannot prove recovery from a cap violation. Keep the route
    // paused until a measured source rate is at or below the cap.
    route.source_bitrate.map_or_else(
        || {
            route.current_selection.policy_pause_reason()
                == Some(PolicyPauseReason::SourceBitrateLimit)
        },
        |observed| observed > cap,
    )
}

pub(super) fn selector_bitrate(
    route: &ReceiverVideoRouteInput<'_>,
    selector: SourceSelector,
) -> Option<Bitrate> {
    let source = route.source;
    let declared = selector.selected_encoding().map_or_else(
        || {
            source
                .selectable_encodings()
                .filter_map(SourceEncodingDescriptor::max_bitrate)
                .max()
        },
        |encoding_id| {
            source
                .selectable_encodings()
                .find(|encoding| encoding.encoding_id() == encoding_id)
                .and_then(SourceEncodingDescriptor::max_bitrate)
        },
    );
    // A per-media observation aggregates every RID. Use it only when policy has
    // at most one selectable encoding and needs no per-RID estimate.
    let observed = route
        .source_bitrate
        .filter(|_| source.selectable_encoding_count() <= 1);
    declared.max(observed)
}

fn highest_allowed_plan(route: &ReceiverVideoRouteInput<'_>) -> Option<ReceiverRouteSelection> {
    let target_selector = highest_allowed_selector(route)?;
    Some(adaptation_send(
        target_selector,
        target_selector != route.current_selection.selector(),
    ))
}

fn highest_allowed_selector(route: &ReceiverVideoRouteInput<'_>) -> Option<SourceSelector> {
    let source_cap = route.source.policy().video_bitrate_cap();
    if route.source.selectable_encoding_count() == 0 {
        return source_cap
            .is_none_or(|cap| route.source_bitrate.is_some_and(|rate| rate <= cap))
            .then_some(SourceSelector::Open);
    }
    let target_index = allowed_encoding_indices(route.source, source_cap)
        .next_back()
        .or_else(|| {
            (route.source.selectable_encoding_count() == 1
                && source_cap
                    .is_some_and(|cap| route.source_bitrate.is_some_and(|rate| rate <= cap)))
            .then_some(0)
        })?;
    Some(SourceSelector::Encoding(
        route
            .source
            .selectable_encoding_by_rank(target_index)?
            .encoding_id(),
    ))
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

/// Separates configured downlink headroom from audio consumption so receivers
/// reserve audio only for admitted routes they can hear.
fn effective_video_budget(
    receiver_bandwidth: Bitrate,
    tuning: VideoAdaptationTuning,
    audio_reserve: Bitrate,
) -> Bitrate {
    let usable_percent = 100u64.saturating_sub(u64::from(tuning.receiver_budget_headroom_percent));
    let after_overhead =
        Bitrate::from_bps(receiver_bandwidth.as_bps().saturating_mul(usable_percent) / 100);
    after_overhead.saturating_sub(audio_reserve)
}

/// next allowed, non-featured layer one step below `current`, with its bitrate
///
/// returns `None` when the route is already at its lowest usable layer, letting
/// the overload pass stop degrading a route before it bottoms out
fn step_down_selector(
    route: &ReceiverVideoRouteInput<'_>,
    current: SourceSelector,
) -> Option<(SourceSelector, Bitrate)> {
    let source = route.source;
    let source_cap = source.policy().video_bitrate_cap();
    let current_index = selector_index(source, current);
    (0..current_index).rev().find_map(|index| {
        let encoding = source.selectable_encoding_by_rank(index)?;
        if matches!(
            encoding.policy_role(),
            Some(UploadLayerPolicyRole::Featured)
        ) {
            return None;
        }
        let bitrate = encoding.max_bitrate()?;
        source_cap
            .is_none_or(|cap| bitrate <= cap)
            .then_some((SourceSelector::Encoding(encoding.encoding_id()), bitrate))
    })
}

fn scalable_plan(
    route: &ReceiverVideoRouteInput<'_>,
    tuning: VideoAdaptationTuning,
) -> Option<ReceiverRouteSelection> {
    let source_cap = route.source.policy().video_bitrate_cap();
    if route.source.selectable_encoding_count() < 2 && source_cap.is_none() {
        return None;
    }
    let current = route.current_selection;
    let current_index = selector_index(route.source, current.selector());
    let target_index = desired_encoding_index(route, source_cap, tuning)?;
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
        // A policy-paused route has no delivered quality transition to smooth.
        // Downswitch hysteresis would reset the observations needed to resume it.
        if !current.policy_allows_delivery()
            || source_cap.is_some_and(|cap| {
                selector_bitrate(route, current.selector()).is_some_and(|bitrate| bitrate > cap)
            })
        {
            return Some(adaptation_send(target_selector, request_keyframe));
        }
        let pressure_limit = tuning.downswitch_pressure_observations;
        let counts = AdaptationCounts::next_pressure(current, pressure_limit);
        if counts.pressure >= pressure_limit {
            return Some(adaptation_send(target_selector, request_keyframe));
        }
        return Some(adaptation_hold(current, counts));
    }
    let stable_limit = tuning.upswitch_stable_observations;
    let counts = AdaptationCounts::next_upgrade(current, stable_limit);
    if counts.upgrade >= stable_limit {
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
    tuning: VideoAdaptationTuning,
) -> Option<usize> {
    if route.user_count < tuning.multiparty_scalable_video_threshold {
        return allowed_encoding_indices(route.source, source_cap).next_back();
    }
    let uses_featured_quality = route.layout_role.uses_featured_quality();
    let Some(receiver_bandwidth) = route.receiver_bandwidth else {
        return if uses_featured_quality {
            allowed_encoding_indices(route.source, source_cap).next_back()
        } else {
            allowed_encoding_indices(route.source, source_cap).next()
        };
    };
    let receiver_bandwidth =
        effective_video_budget(receiver_bandwidth, tuning, route.audio_budget_reserve);
    let budget = if uses_featured_quality || route.visible_scalable_route_count <= 1 {
        receiver_bandwidth
    } else {
        let divisor = u64::try_from(route.visible_scalable_route_count)
            .unwrap_or(u64::MAX)
            // Bias secondary routes toward thumbnail quality before aggregate overload handling.
            .saturating_mul(tuning.thumbnail_budget_divisor);
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
    allowed_encoding_indices(source, source_cap)
        .rev()
        .find_or_last(|index| {
            source
                .selectable_encoding_by_rank(*index)
                .and_then(SourceEncodingDescriptor::max_bitrate)
                .is_some_and(|bitrate| bitrate <= budget)
        })
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

/// enforces receiver download limits by pausing the lowest-ranked routes using top-k selection
///
/// ```text
/// all active routes (N)
///   [ R0: rank 2, R1: rank 5, R2: rank 1, R3: rank 8, R4: rank 4 ]
///   limit = 3  ==>  routes_to_pause (K) = 5 - 3 = 2
///                              |
///                              v  .k_largest_by_key(K = 2)
///   +-------------------------------------------------------------+
///   | min-heap of size K=2:                                       |
///   | retains only the 2 highest ranks: [ R1: rank 5, R3: rank 8 ]|
///   +-------------------------------------------------------------+
///                              |
///                              v  .rev()
///   route.pause(VideoDownloadLimit) applied only to [ R3, R1 ]
/// ```
fn apply_video_download_limit(
    routes: &mut [PlannedReceiverRoute<'_>],
    max_video_downloads_per_receiver: usize,
) {
    let routes_to_pause =
        active_route_count(routes).saturating_sub(max_video_downloads_per_receiver);
    if routes_to_pause == 0 {
        return;
    }
    for (_rank_key, route) in routes
        .iter_mut()
        .filter(|route| route.selection.policy_pause_reason.is_none())
        .enumerate()
        .map(|(position, route)| ((video_download_rank(route), position), route))
        .k_largest_by_key(routes_to_pause, |(rank_key, _route)| *rank_key)
        .rev()
    {
        route.pause(PolicyPauseReason::VideoDownloadLimit);
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
        input.layout_role.priority(),
        input.active_speaker_rank,
        input.source.source_id(),
    )
}

fn apply_overload_policy(routes: &mut [PlannedReceiverRoute<'_>], video_budget: Bitrate) {
    let mut total_bitrate = selected_active_video_bitrate(routes);
    if total_bitrate <= video_budget {
        return;
    }
    // Drop downgradable routes that have no usable layer to fall back to.
    let mut missing_ladders = routes
        .iter_mut()
        .filter(|route| {
            route_can_downgrade(route) && cheapest_useful_selector(route.input).is_none()
        })
        .map(|route| (video_download_rank(route), route))
        .collect::<Vec<_>>();
    missing_ladders.sort_by_key(|(rank, _route)| Reverse(*rank));
    for (_rank, route) in missing_ladders {
        if total_bitrate <= video_budget {
            break;
        }
        let selected_bitrate = route.selected_bitrate;
        route.pause(PolicyPauseReason::MissingUsableLayer);
        total_bitrate = total_bitrate.saturating_sub(selected_bitrate);
    }
    // Step the least important downgradable route down one layer at a time.
    // Preserve intermediate layers and stop as soon as aggregate demand fits.
    while total_bitrate > video_budget {
        let Some((route, selector, bitrate)) = routes
            .iter_mut()
            .filter(|route| route_can_downgrade(route))
            .filter_map(|route| {
                step_down_selector(route.input, route.selection.selector)
                    .map(|(selector, bitrate)| (route, selector, bitrate))
            })
            .max_by_key(|(route, _selector, _bitrate)| {
                (video_download_rank(route), route.selected_bitrate)
            })
        else {
            break;
        };
        let selected_bitrate = route.selected_bitrate;
        total_bitrate = total_bitrate
            .saturating_sub(selected_bitrate)
            .saturating_add(bitrate);
        route.send(selector, bitrate);
    }
    if total_bitrate <= video_budget {
        return;
    }
    let mut pause_order = Vec::with_capacity(routes.len());
    for route in routes.iter_mut() {
        if route.selection.policy_pause_reason.is_some() {
            continue;
        }
        pause_order.push((video_download_rank(route), route));
    }
    pause_order.sort_by_key(|(rank, _)| Reverse(*rank));
    for (_rank, route) in pause_order {
        if total_bitrate <= video_budget {
            break;
        }
        let selected_bitrate = route.selected_bitrate;
        let pause_reason = pause_reason_for_route(route);
        route.pause(pause_reason);
        total_bitrate = total_bitrate.saturating_sub(selected_bitrate);
    }
}

fn receiver_video_budget_diagnostics(
    routes: &[PlannedReceiverRoute<'_>],
    receiver_bandwidth: Option<Bitrate>,
    video_budget: Option<Bitrate>,
) -> ReceiverVideoBudgetDiagnostics {
    let selected_video_bitrate = selected_active_video_bitrate(routes);
    ReceiverVideoBudgetDiagnostics::new(
        receiver_bandwidth,
        video_budget,
        active_route_count(routes),
        selected_video_bitrate,
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

fn admitted_video_bwe_demand(routes: &[PlannedReceiverRoute<'_>]) -> Bitrate {
    routes
        .iter()
        .filter(|route| route.selection.policy_pause_reason.is_none())
        .fold(Bitrate::zero(), |total, route| {
            let route_demand = highest_allowed_selector(route.input)
                .and_then(|selector| selector_bitrate(route.input, selector))
                .unwrap_or(route.selected_bitrate);
            total.saturating_add(route_demand.max(route.selected_bitrate))
        })
}

fn route_can_downgrade(route: &PlannedReceiverRoute<'_>) -> bool {
    let input = route.input;
    route.selection.policy_pause_reason.is_none()
        && input.source.policy().adaptation() == SourceAdaptationPolicy::ScalableVideo
        && matches!(
            input.layout_role.priority(),
            SourceRoutePriority::VisibleThumbnail | SourceRoutePriority::HiddenOrOverflow
        )
}

fn pause_reason_for_route(route: &PlannedReceiverRoute<'_>) -> PolicyPauseReason {
    let role = route.input.layout_role;
    match role.priority() {
        SourceRoutePriority::HiddenOrOverflow => match role {
            SourceRoomPolicySelector::Hidden => PolicyPauseReason::HiddenTile,
            SourceRoomPolicySelector::Overflow => PolicyPauseReason::OverflowTile,
            _ => PolicyPauseReason::BudgetPressure,
        },
        _ => PolicyPauseReason::BudgetPressure,
    }
}

fn resolve_hysteresis(
    route: &PlannedReceiverRoute<'_>,
    tuning: VideoAdaptationTuning,
) -> ReceiverRouteSelection {
    let current = route.input.current_selection;
    let current_pause_reason = current.policy_pause_reason();
    let selection = route.selection;
    match (selection.policy_pause_reason, current_pause_reason) {
        // Resume a route newly admitted under the download limit immediately
        // because recovery hysteresis would leave an available slot unused.
        (None, Some(PolicyPauseReason::VideoDownloadLimit)) => {
            ReceiverRouteSelection::send(selection.selector, AdaptationCounts::reset(), true)
        }
        (None, Some(reason)) => {
            let stable_limit = tuning.upswitch_stable_observations;
            let counts = AdaptationCounts::next_upgrade(current, stable_limit);
            if counts.upgrade >= stable_limit {
                ReceiverRouteSelection::send(selection.selector, AdaptationCounts::reset(), true)
            } else {
                ReceiverRouteSelection::hold(current, Some(reason), counts)
            }
        }
        (Some(reason), pause_reason) if pause_reason != Some(reason) => {
            // Per-receiver download count and source bitrate caps are hard
            // constraints. Holding current state would exceed the admission
            // limit or source ceiling.
            if matches!(
                reason,
                PolicyPauseReason::VideoDownloadLimit | PolicyPauseReason::SourceBitrateLimit
            ) {
                return ReceiverRouteSelection::pause(current, reason, AdaptationCounts::reset());
            }
            let pressure_limit = tuning.downswitch_pressure_observations;
            let counts = AdaptationCounts::next_pressure(current, pressure_limit);
            if counts.pressure >= pressure_limit {
                ReceiverRouteSelection::pause(current, reason, AdaptationCounts::reset())
            } else {
                ReceiverRouteSelection::hold(current, None, counts)
            }
        }
        _ => selection,
    }
}

#[cfg(test)]
#[path = "TESTS/solver.rs"]
mod tests;
