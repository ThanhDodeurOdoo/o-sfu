use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet},
};

use super::action::FeaturedUserUpdate;
use crate::{
    Bitrate, RoomMediaLimits,
    engine::{
        UserId,
        media_transport::{
            ActiveSpeakerSource, ReceiverBandwidthSnapshot, ReceiverBweTargetUpdate,
            TransportMediaId,
        },
        room::state::RoomState,
        source_model::{
            ActiveSpeakerSourceRole, ConsumerSourceSelection, PublishedSourceDescriptor,
        },
    },
};

const ACTIVE_SPEAKER_FEATURED_CLEAR_LIMIT: usize = 5;

#[derive(Debug)]
pub struct SourcePolicyInput<'a> {
    pub(super) routes: Vec<SourcePolicyRouteInput<'a>>,
    pub(super) receiver_bwe_targets: BTreeMap<UserId, ReceiverBweTargetUpdate>,
    pub(super) receiver_bandwidth_by_user: BTreeMap<UserId, Bitrate>,
    pub(super) active_speaker_media_ids: BTreeSet<TransportMediaId>,
    pub(super) admitted_audio_media_ids: BTreeSet<TransportMediaId>,
    pub(super) featured_source_user_ids: BTreeSet<UserId>,
    pub(super) active_speaker_rank_by_user: BTreeMap<UserId, usize>,
    pub(super) featured_user_updates: Vec<FeaturedUserUpdate>,
    pub(super) user_count: usize,
    pub(super) media_limits: RoomMediaLimits,
}

#[derive(Debug, Clone)]
pub struct SourcePolicyRouteInput<'a> {
    pub(super) source: &'a PublishedSourceDescriptor,
    pub(super) route: super::super::media_graph::ConsumerRouteTransportRef,
    pub(super) current_selection: ConsumerSourceSelection,
}

impl<'a> SourcePolicyInput<'a> {
    pub(super) fn from_state(
        state: &'a RoomState,
        active_speaker_sources: &[ActiveSpeakerSource],
        receiver_bandwidth_snapshot: &ReceiverBandwidthSnapshot,
    ) -> Self {
        let ranked_active_speaker_sources = rank_active_speaker_sources(active_speaker_sources);
        let media_limits = state.source_policy_media_limits();
        let active_speakers = active_speaker_media_ids(&ranked_active_speaker_sources);
        let admitted_audio_speakers = admitted_audio_media_ids(
            &ranked_active_speaker_sources,
            media_limits.max_active_audio_speakers(),
        );
        let featured_source_user_ids =
            featured_source_user_ids(state, &ranked_active_speaker_sources);
        let active_speaker_rank_by_user =
            active_speaker_rank_by_user(state, &ranked_active_speaker_sources);
        let desired_featured_user_id = ranked_active_speaker_sources.iter().find_map(|source| {
            featured_source_owner_for_active_speaker_source(state, source.transport_media_id())
        });
        let featured_user_updates = featured_user_updates(state, desired_featured_user_id.as_ref());
        let routes = source_policy_routes(state);
        Self {
            routes,
            receiver_bwe_targets: receiver_bwe_targets(state),
            receiver_bandwidth_by_user: receiver_bandwidth_by_user(receiver_bandwidth_snapshot),
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

fn source_policy_routes(state: &RoomState) -> Vec<SourcePolicyRouteInput<'_>> {
    let live_routes = state.current_live_consumer_routes();
    let mut routes = Vec::with_capacity(live_routes.size_hint().1.unwrap_or_default());
    for route in live_routes {
        if !route.producer.active {
            continue;
        }
        let source = route.source;
        let desired_active = state.desired_source_active(
            &route.consumer_user_id,
            source.owner().user_id(),
            source.stream_id(),
        );
        let current_selection = route.selection_or_open(desired_active);
        if !current_selection.active() {
            continue;
        }
        routes.push(SourcePolicyRouteInput {
            source,
            route: route.transport_ref(),
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

fn receiver_bandwidth_by_user(snapshot: &ReceiverBandwidthSnapshot) -> BTreeMap<UserId, Bitrate> {
    snapshot
        .per_session
        .iter()
        .map(|(session_key, estimate_bps)| (session_key.user_id().clone(), *estimate_bps))
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
    let entry = state.source_transport_media_entry(transport_media_id)?;
    let detector_source = state.source_policy_source(entry.source)?;
    let detector_policy = detector_source.policy().active_speaker()?;
    if detector_policy.role() != ActiveSpeakerSourceRole::Detector {
        return None;
    }
    state
        .source_policy_owner_has_promotable_source_in_group(&entry.owner, detector_policy.group())
        .then(|| entry.owner.clone())
}

fn featured_user_updates(
    state: &RoomState,
    desired_featured_user_id: Option<&UserId>,
) -> Vec<FeaturedUserUpdate> {
    if desired_featured_user_id.is_none()
        && !state
            .source_policy_user_featured_states()
            .any(|(_user_id, featured)| featured.is_some())
    {
        return Vec::new();
    }
    state
        .source_policy_user_featured_states()
        .filter_map(|(user_id, current_featured)| {
            let desired_featured = match desired_featured_user_id {
                Some(featured_user_id) => Some(featured_user_id == user_id),
                None if current_featured.is_some() => Some(false),
                None => None,
            };
            (desired_featured != current_featured)
                .then(|| FeaturedUserUpdate::new(user_id.clone(), desired_featured))
        })
        .collect()
}
