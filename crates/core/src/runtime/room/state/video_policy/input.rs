//! immutable input snapshot for pure receiver video policy
//!
//! this module is the only place where the receiver video policy path reads
//! the `RoomState` indexes and transport observation snapshots together
//! those mutable room facts are normalized into route-shaped facts so the budget
//! planner can stay pure, deterministic and testable without a `Room`, media
//! transport, websocket user or `str0m` state
//!
//! source behavior enters this path through the committed
//! [`PublishedSourceDescriptor`]
//! that keeps product vocabulary at the
//! orchestration edge while letting the planner consume media kind, layout
//! policy, adaptation policy and active-speaker role as generic route facts
//!
//! the snapshot is intentionally lossy because it is an action boundary, not a
//! room-state mirror
//! routes that cannot be changed during this refresh are filtered out before
//! planning, so the planner never spends receiver budget on stale producers,
//! inactive consumer selections or sources without selectable encodings
//! the planner receives only the facts it can turn into media effects:
//! live producer routes, active consumer selections, usable source encodings,
//! receiver layout intent and best-effort bandwidth observations

use std::collections::{BTreeMap, BTreeSet};

use super::{
    super::{media::ConsumerRouteTransportRef, shared::RoomState},
    layout::{ReceiverVideoLayoutIntent, featured_source_user_ids_for_active_speakers},
};
use crate::{
    Bitrate,
    runtime::{
        UserId,
        media_transport::{ActiveSpeakerSource, ReceiverBandwidthSnapshot, TransportMediaId},
        source_model::{
            ActiveSpeakerSourceRole, ConsumerSourceSelection, PublishedSourceDescriptor,
            PublishedSourceId, SourceAdaptationPolicy, SourceEncodingDescriptor,
        },
    },
};

/// policy input for one refresh across all live receiver/source video routes
///
/// routes retain `consumer_index` order so the budget planner can process
/// contiguous receiver groups without rebuilding a map
/// this matters because
/// the budget solver allocates bandwidth per receiver, while the room index is
/// already keyed by consumer/source route
#[derive(Debug)]
pub(in crate::runtime::room) struct ReceiverVideoPolicyInput<'a> {
    /// normalized live routes in `RoomState::consumer_index` order
    routes: Vec<ReceiverVideoRouteInput<'a>>,
}

impl<'a> ReceiverVideoPolicyInput<'a> {
    /// builds a policy input from an already-normalized route list
    ///
    /// this constructor is private so production callers cannot bypass the
    /// room-state filtering in `from_state`
    /// tests can still construct focused route inputs directly when they need
    /// budget-planner fixtures
    #[must_use]
    fn new(routes: Vec<ReceiverVideoRouteInput<'a>>) -> Self {
        Self { routes }
    }

    /// build the pure policy input from committed room state and transport observations
    ///
    /// this method is the boundary between orchestration state and pure video
    /// planning
    /// it filters stale or unusable routes before they reach the
    /// budget solver:
    ///
    /// * source descriptors must still exist and expose at least one selectable encoding
    /// * producer media must still be registered and active
    /// * consumer selections must still be active
    /// * layout intent is resolved with the current active-speaker snapshot
    /// * receiver bandwidth is attached by user id as a best-effort observation
    ///
    /// the returned snapshot borrows source descriptors from `state`
    /// callers must consume it before mutating the room graph again
    #[must_use]
    pub fn from_state(
        state: &'a RoomState,
        active_speaker_sources: &[ActiveSpeakerSource],
        receiver_bandwidth_snapshot: &ReceiverBandwidthSnapshot,
    ) -> Self {
        let featured_source_user_ids =
            featured_source_user_ids_for_active_speakers(state, active_speaker_sources);
        let receiver_bandwidth_by_user = receiver_bandwidth_by_user(receiver_bandwidth_snapshot);
        let visible_scalable_route_counts =
            visible_scalable_route_counts_by_consumer(state, &featured_source_user_ids);
        let routes = state
            .current_live_consumer_routes()
            .filter_map(|route| {
                let source = route.source;
                if source.selectable_encoding_count() == 0 {
                    return None;
                }
                if !route.producer.active {
                    return None;
                }
                let current_selection = route.selection_or_open(true);
                if !current_selection.active() {
                    return None;
                }
                let layout_intent = state.receiver_video_layout_intent(
                    &route.consumer_user_id,
                    source,
                    &featured_source_user_ids,
                );
                Some(ReceiverVideoRouteInput::new(ReceiverVideoRouteInputParts {
                    user_count: state.user_count(),
                    source,
                    transport_ref: route.transport_ref(),
                    current_selection,
                    layout_intent,
                    visible_scalable_route_count: visible_scalable_route_counts
                        .get(&route.consumer_user_id)
                        .copied()
                        .unwrap_or(1),
                    receiver_bandwidth: receiver_bandwidth_by_user
                        .get(&route.consumer_user_id)
                        .copied(),
                }))
            })
            .collect();
        Self::new(routes)
    }

    /// returns the normalized route list for pure planning
    ///
    /// routes are grouped by receiver because they preserve `consumer_index`
    /// ordering from the room state
    pub fn routes(&self) -> &[ReceiverVideoRouteInput<'a>] {
        &self.routes
    }
}

/// immutable facts the planner needs for one receiver/source route
///
/// this is a route snapshot, not authority
/// applying the planner result later
/// must still go through stale-update checks because room state can change while
/// async media effects are in flight
#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct ReceiverVideoRouteInput<'a> {
    /// live user count at snapshot time
    user_count: usize,
    /// committed source descriptor that owns policy and encoding metadata
    source: &'a PublishedSourceDescriptor,
    /// stable transport identity for this receiver/source route
    transport_ref: ConsumerRouteTransportRef,
    /// last committed consumer source selection before this refresh
    current_selection: ConsumerSourceSelection,
    /// receiver-specific layout role resolved for this source
    layout_intent: ReceiverVideoLayoutIntent,
    /// visible scalable routes sharing the same receiver budget
    visible_scalable_route_count: usize,
    /// latest receiver bandwidth estimate, if transport has reported one
    receiver_bandwidth: Option<Bitrate>,
}

/// construction input for [`ReceiverVideoRouteInput`]
///
/// the route input is intentionally explicit so pure policy tests can build the
/// planner input without constructing a full room or media transport
/// production callers should normally use `ReceiverVideoPolicyInput::from_state`
/// so stale route filtering stays centralized
#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct ReceiverVideoRouteInputParts<'a> {
    /// live user count at snapshot time
    pub user_count: usize,
    /// committed source descriptor for the route
    pub source: &'a PublishedSourceDescriptor,
    /// stable transport identity for this route
    pub transport_ref: ConsumerRouteTransportRef,
    /// current committed transport selection for this route
    pub current_selection: ConsumerSourceSelection,
    /// layout importance resolved for this receiver/source pair
    pub layout_intent: ReceiverVideoLayoutIntent,
    /// visible scalable route count for this receiver
    pub visible_scalable_route_count: usize,
    /// latest receiver bandwidth estimate, if known
    pub receiver_bandwidth: Option<Bitrate>,
}

impl<'a> ReceiverVideoRouteInput<'a> {
    /// builds one route input from explicit snapshot facts
    #[must_use]
    pub fn new(parts: ReceiverVideoRouteInputParts<'a>) -> Self {
        Self {
            user_count: parts.user_count,
            source: parts.source,
            transport_ref: parts.transport_ref,
            current_selection: parts.current_selection,
            layout_intent: parts.layout_intent,
            visible_scalable_route_count: parts.visible_scalable_route_count,
            receiver_bandwidth: parts.receiver_bandwidth,
        }
    }

    /// returns the committed source descriptor for policy and encoding lookup
    pub fn source(&self) -> &'a PublishedSourceDescriptor {
        self.source
    }

    /// returns the stable source id for stale-update checks
    pub const fn source_id(&self) -> PublishedSourceId {
        self.source.source_id()
    }

    /// returns the source adaptation policy consumed by the budget planner
    pub const fn adaptation_policy(&self) -> SourceAdaptationPolicy {
        self.source.policy().adaptation()
    }

    /// returns the receiver user id for grouping and effect routing
    pub fn consumer_user_id(&self) -> &UserId {
        self.transport_ref.consumer_user_id()
    }

    /// returns the stable transport identity for effect execution
    pub fn transport_ref(&self) -> &ConsumerRouteTransportRef {
        &self.transport_ref
    }

    /// returns the selection currently committed for this consumer route
    pub const fn current_selection(&self) -> ConsumerSourceSelection {
        self.current_selection
    }

    /// returns the receiver-specific layout role for this source
    pub const fn layout_intent(&self) -> ReceiverVideoLayoutIntent {
        self.layout_intent
    }

    /// returns the live user count captured for this policy refresh
    pub const fn user_count(&self) -> usize {
        self.user_count
    }

    /// returns how many visible scalable routes share this receiver's budget
    ///
    /// hidden or overflow routes do not count toward this divisor because they
    /// should not force visible thumbnails to downscale
    pub const fn visible_scalable_route_count(&self) -> usize {
        self.visible_scalable_route_count
    }

    /// returns the latest receiver bandwidth estimate when one is available
    ///
    /// absence is treated as "no transport observation yet" by the planner
    /// it is not a zero-bandwidth signal
    pub const fn receiver_bandwidth(&self) -> Option<Bitrate> {
        self.receiver_bandwidth
    }

    /// returns selectable encodings in the descriptor-owned policy order
    pub fn encodings(&self) -> SelectableRouteEncodings<'a> {
        SelectableRouteEncodings::new(self.source)
    }
}

/// borrowed view over encodings that the receiver policy may select
///
/// this wrapper keeps the planner from depending on the entire source model
/// surface
/// ordering is the source model's selectable order, usually from cheapest
/// useful encoding to highest quality
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime::room) struct SelectableRouteEncodings<'a> {
    /// source descriptor that owns encoding metadata
    source: &'a PublishedSourceDescriptor,
}

impl<'a> SelectableRouteEncodings<'a> {
    /// builds an encoding view for one source descriptor
    #[must_use]
    fn new(source: &'a PublishedSourceDescriptor) -> Self {
        Self { source }
    }

    /// returns the number of selectable encodings exposed to policy
    #[must_use]
    pub fn len(self) -> usize {
        self.source.selectable_encoding_count()
    }

    /// returns one selectable encoding by policy rank
    #[must_use]
    pub fn get(self, rank: usize) -> Option<&'a SourceEncodingDescriptor> {
        self.source.selectable_encoding_by_rank(rank)
    }

    /// iterates selectable encodings in policy rank order
    pub fn iter(self) -> impl Iterator<Item = &'a SourceEncodingDescriptor> {
        self.source.selectable_encodings()
    }
}

/// count visible scalable-video routes per receiver for budget splitting
///
/// only currently valid routes are counted
/// stale consumer connections,
/// inactive producers, inactive selections and hidden layout roles are ignored
/// so a receiver's thumbnail budget is divided only across visible routes that
/// can actually receive scalable-video adaptation
fn visible_scalable_route_counts_by_consumer(
    state: &RoomState,
    featured_source_user_ids: &BTreeSet<UserId>,
) -> BTreeMap<UserId, usize> {
    let mut counts = BTreeMap::new();
    for route in state.current_live_consumer_routes() {
        let source = route.source;
        if source.policy().adaptation() != SourceAdaptationPolicy::ScalableVideo {
            continue;
        }
        if !route.producer.active || !route.selection_or_open(true).active() {
            continue;
        }
        let layout_intent = state.receiver_video_layout_intent(
            &route.consumer_user_id,
            source,
            featured_source_user_ids,
        );
        if !layout_intent.counts_toward_visible_budget() {
            continue;
        }
        *counts.entry(route.consumer_user_id.clone()).or_default() += 1;
    }
    counts
}

/// collapse transport receiver bandwidth by user id
///
/// transport observations are keyed by session because the media layer tracks
/// concrete rtc sessions
/// receiver video policy is user-scoped, so the latest
/// entry for a user becomes the budget hint for every live route of that user
fn receiver_bandwidth_by_user(snapshot: &ReceiverBandwidthSnapshot) -> BTreeMap<UserId, Bitrate> {
    snapshot
        .per_session
        .iter()
        .map(|(session_key, estimate_bps)| (session_key.user_id().clone(), *estimate_bps))
        .collect()
}

/// resolve the featured user that corresponds to one active-speaker source
///
/// active-speaker detection can be driven by a detector source that is not the
/// video source to feature
/// this helper maps that detector back to its owner
/// only when the same owner also publishes a promotable source in the detector's
/// active-speaker group
pub(super) fn featured_source_owner_for_active_speaker_source(
    state: &RoomState,
    transport_media_id: TransportMediaId,
) -> Option<UserId> {
    let entry = state.source_transport_media_entry(transport_media_id)?;
    let detector_source = state.media.source(entry.source_id())?;
    let detector_policy = detector_source.policy().active_speaker()?;
    if detector_policy.role() != ActiveSpeakerSourceRole::Detector {
        return None;
    }
    let owner_user_id = entry.owner_user_id().clone();
    state
        .media
        .owner_has_promotable_source_in_group(&owner_user_id, detector_policy.group())
        .then_some(owner_user_id)
}

/// return the first promotable featured user from the active-speaker snapshot
///
/// snapshot order is preserved so the caller gets the highest-priority active
/// speaker that can actually influence a promotable video source
pub(super) fn first_featured_source_user_for_active_speakers(
    state: &RoomState,
    active_speaker_sources: &[ActiveSpeakerSource],
) -> Option<UserId> {
    active_speaker_sources.iter().find_map(|source| {
        featured_source_owner_for_active_speaker_source(state, source.transport_media_id())
    })
}

/// return the first promotable featured users from the active-speaker snapshot
///
/// the limit protects layout updates from clearing too many server-derived
/// featured states at once when active-speaker observations are noisy
pub(super) fn first_featured_source_users_for_active_speakers(
    state: &RoomState,
    active_speaker_sources: &[ActiveSpeakerSource],
    limit: usize,
) -> BTreeSet<UserId> {
    active_speaker_sources
        .iter()
        .filter_map(|source| {
            featured_source_owner_for_active_speaker_source(state, source.transport_media_id())
        })
        .take(limit)
        .collect()
}

#[cfg(test)]
mod tests {
    use o_sfu_router::{MediaKind, Rid};

    use super::*;
    use crate::runtime::{
        ConnectionId,
        source_model::{
            PublishedSourceDescriptorParts, PublishedSourceOwner, SourceEncodingDescriptorParts,
            SourceEncodingId, SourceModelError, SourcePolicy, SourceRoomPolicySelector,
            UserStreamId,
        },
    };

    fn source_encoding(
        source_id: PublishedSourceId,
        encoding_id: SourceEncodingId,
        rid: &str,
        max_bitrate: Bitrate,
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
            policy_role: None,
            max_temporal_layer_id: None,
            negotiated_format: None,
        })
    }

    #[test]
    fn route_input_uses_descriptor_owned_selectable_encoding_order() -> Result<(), SourceModelError>
    {
        let source_id = PublishedSourceId::from_raw(19);
        let low_encoding_id = SourceEncodingId::from_raw(1);
        let middle_encoding_id = SourceEncodingId::from_raw(2);
        let high_encoding_id = SourceEncodingId::from_raw(3);
        let source_user_id = UserId::Integer(4);
        let consumer_user_id = UserId::Integer(5);
        let source = PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
            source_id,
            owner: PublishedSourceOwner::new(source_user_id.clone()),
            stream_id: UserStreamId::new("camera"),
            media_kind: MediaKind::Video,
            policy: SourcePolicy::hidden(),
            mid: None,
            encodings: vec![
                source_encoding(source_id, high_encoding_id, "hi", Bitrate::from_kbps(900)),
                source_encoding(source_id, low_encoding_id, "lo", Bitrate::from_kbps(150)),
                source_encoding(
                    source_id,
                    middle_encoding_id,
                    "mid",
                    Bitrate::from_kbps(450),
                ),
            ],
        })?;
        let route = ReceiverVideoRouteInput::new(ReceiverVideoRouteInputParts {
            user_count: 2,
            source: &source,
            transport_ref: ConsumerRouteTransportRef::from_parts(
                consumer_user_id,
                ConnectionId::from_raw(10),
                TransportMediaId::new(12),
                source_user_id,
                ConnectionId::from_raw(9),
                TransportMediaId::new(11),
            ),
            current_selection: ConsumerSourceSelection::open(true),
            layout_intent: ReceiverVideoLayoutIntent::new(
                SourceRoomPolicySelector::VisibleThumbnail,
            ),
            visible_scalable_route_count: 1,
            receiver_bandwidth: None,
        });

        let selectable_encoding_ids = route
            .encodings()
            .iter()
            .map(SourceEncodingDescriptor::encoding_id)
            .collect::<Vec<_>>();

        assert_eq!(
            selectable_encoding_ids,
            vec![low_encoding_id, middle_encoding_id, high_encoding_id]
        );
        assert_eq!(
            route
                .encodings()
                .get(1)
                .map(SourceEncodingDescriptor::encoding_id),
            Some(middle_encoding_id)
        );
        Ok(())
    }
}
