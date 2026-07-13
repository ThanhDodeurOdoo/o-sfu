use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet},
};

use super::action::FeaturedUserUpdate;
use crate::{
    Bitrate, RoomMediaLimits,
    engine::{
        ConnectionId, UserId,
        media_transport::{
            ActiveSpeakerSource, ReceiverBandwidthSnapshot, ReceiverBweTargetUpdate,
            TransportMediaId,
        },
        room::{media_graph::ConsumerRouteTransportRef, state::RoomState},
        source_model::{ConsumerSourceSelection, PublishedSourceDescriptor},
    },
};

const ACTIVE_SPEAKER_FEATURED_CLEAR_LIMIT: usize = 5;

#[derive(Debug)]
pub(super) struct SourcePolicySnapshot<'a> {
    pub(super) routes: Vec<SourcePolicyRoute<'a>>,
    pub(super) receiver_bwe_targets: BTreeMap<UserId, ReceiverBweTargetUpdate>,
    pub(super) receiver_bandwidth_by_connection: BTreeMap<ConnectionId, Bitrate>,
    pub(super) active_speaker_media_ids: BTreeSet<TransportMediaId>,
    pub(super) admitted_audio_media_ids: BTreeSet<TransportMediaId>,
    pub(super) featured_source_user_ids: BTreeSet<UserId>,
    pub(super) active_speaker_rank_by_user: BTreeMap<UserId, usize>,
    pub(super) featured_user_updates: Vec<FeaturedUserUpdate>,
    pub(super) user_count: usize,
    pub(super) media_limits: RoomMediaLimits,
}

#[derive(Debug)]
pub(super) struct SourcePolicyRoute<'a> {
    pub(super) source: &'a PublishedSourceDescriptor,
    pub(super) transport_ref: ConsumerRouteTransportRef,
    pub(super) current_selection: ConsumerSourceSelection,
}

impl<'a> SourcePolicySnapshot<'a> {
    pub(super) fn from_state(
        state: &'a RoomState,
        active_speaker_sources: &[ActiveSpeakerSource],
        receiver_bandwidth_snapshot: &ReceiverBandwidthSnapshot,
    ) -> Self {
        let ranked_sources = rank_active_speaker_sources(active_speaker_sources);
        let media_limits = state.media_limits;
        let active_speakers = active_speaker_media_ids(&ranked_sources);
        let admitted_audio_speakers =
            admitted_audio_media_ids(&ranked_sources, media_limits.max_active_audio_speakers());
        let featured_source_user_ids = featured_source_user_ids(state, &ranked_sources);
        let active_speaker_rank_by_user = active_speaker_rank_by_user(state, &ranked_sources);
        let desired_featured_user_id = ranked_sources.iter().find_map(|source| {
            featured_source_owner_for_active_speaker_source(state, source.transport_media_id())
        });
        let featured_user_updates = featured_user_updates(state, desired_featured_user_id.as_ref());
        let routes = source_policy_routes(state);
        Self {
            routes,
            receiver_bwe_targets: receiver_bwe_targets(state),
            receiver_bandwidth_by_connection: receiver_bandwidth_by_connection(
                receiver_bandwidth_snapshot,
            ),
            active_speaker_media_ids: active_speakers,
            admitted_audio_media_ids: admitted_audio_speakers,
            featured_source_user_ids,
            active_speaker_rank_by_user,
            featured_user_updates,
            user_count: state.user_count(),
            media_limits,
        }
    }
}

fn source_policy_routes(state: &RoomState) -> Vec<SourcePolicyRoute<'_>> {
    let live_routes = state.live_consumer_routes();
    let mut routes = Vec::with_capacity(live_routes.size_hint().1.unwrap_or_default());
    for route in live_routes {
        if !route.source.active {
            continue;
        }
        let source = &route.source.descriptor;
        let desired_active = state.desired_source_active(
            &route.consumer_user_id,
            source.owner().user_id(),
            source.stream_id(),
        );
        let current_selection = route.selection_or_open(desired_active);
        if !current_selection.active() {
            continue;
        }
        routes.push(SourcePolicyRoute {
            source,
            transport_ref: route.transport_ref(),
            current_selection,
        });
    }
    routes
}

fn receiver_bwe_targets(state: &RoomState) -> BTreeMap<UserId, ReceiverBweTargetUpdate> {
    state
        .transport_user_entries()
        .into_iter()
        .map(|(user_id, connection_id)| {
            let session = state.transport_user_key(&user_id, connection_id);
            (
                user_id,
                ReceiverBweTargetUpdate::new(session, Bitrate::zero()),
            )
        })
        .collect()
}

fn receiver_bandwidth_by_connection(
    snapshot: &ReceiverBandwidthSnapshot,
) -> BTreeMap<ConnectionId, Bitrate> {
    snapshot
        .per_session
        .iter()
        .map(|(session, estimate)| (session.connection_id(), *estimate))
        .collect()
}

fn rank_active_speaker_sources(sources: &[ActiveSpeakerSource]) -> Vec<ActiveSpeakerSource> {
    let mut sources = sources.to_vec();
    sources.sort_by_key(|source| {
        (
            Reverse(source.observed_at()),
            Reverse(source.last_audio_level_dbov().unwrap_or(i8::MIN)),
            source.transport_media_id().as_u64(),
        )
    });
    sources
}

fn active_speaker_media_ids(sources: &[ActiveSpeakerSource]) -> BTreeSet<TransportMediaId> {
    sources
        .iter()
        .map(|source| source.transport_media_id())
        .collect()
}

fn admitted_audio_media_ids(
    sources: &[ActiveSpeakerSource],
    limit: usize,
) -> BTreeSet<TransportMediaId> {
    sources
        .iter()
        .take(limit)
        .map(|source| source.transport_media_id())
        .collect()
}

fn featured_source_user_ids(
    state: &RoomState,
    sources: &[ActiveSpeakerSource],
) -> BTreeSet<UserId> {
    sources
        .iter()
        .filter_map(|source| {
            featured_source_owner_for_active_speaker_source(state, source.transport_media_id())
        })
        .take(ACTIVE_SPEAKER_FEATURED_CLEAR_LIMIT)
        .collect()
}

fn active_speaker_rank_by_user(
    state: &RoomState,
    sources: &[ActiveSpeakerSource],
) -> BTreeMap<UserId, usize> {
    let mut ranks = BTreeMap::new();
    for source in sources {
        let Some(user_id) =
            featured_source_owner_for_active_speaker_source(state, source.transport_media_id())
        else {
            continue;
        };
        let next_rank = ranks.len();
        ranks.entry(user_id).or_insert(next_rank);
    }
    ranks
}

fn featured_source_owner_for_active_speaker_source(
    state: &RoomState,
    transport_media_id: TransportMediaId,
) -> Option<UserId> {
    state
        .topology
        .active_speaker_detector_owner(transport_media_id)
}

fn featured_user_updates(
    state: &RoomState,
    desired_featured_user_id: Option<&UserId>,
) -> Vec<FeaturedUserUpdate> {
    if desired_featured_user_id.is_none()
        && !state.users.values().any(|user| user.featured().is_some())
    {
        return Vec::new();
    }
    state
        .users
        .iter()
        .filter_map(|(user_id, user)| {
            let current_featured = user.featured();
            let desired_featured = match desired_featured_user_id {
                Some(featured_user_id) => Some(featured_user_id == user_id),
                None if current_featured.is_some() => Some(false),
                None => None,
            };
            (desired_featured != current_featured).then(|| {
                FeaturedUserUpdate::new(user_id.clone(), user.connection_id, desired_featured)
            })
        })
        .collect()
}
