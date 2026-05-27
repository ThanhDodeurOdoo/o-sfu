//! immutable input snapshot for pure receiver video policy
//!
//! this boundary normalizes room indexes and transport observations into
//! route-shaped facts so the budget planner stays pure and deterministic

use std::collections::{BTreeMap, BTreeSet};

use o_sfu_router::MediaKind;

use super::{
    super::super::{media::ConsumerRouteTransportRef, shared::RoomState},
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

#[derive(Debug)]
pub(in crate::runtime::room) struct ReceiverVideoPolicyInput<'a> {
    pub routes: Vec<ReceiverVideoRouteInput<'a>>,
    pub max_video_downloads_per_receiver: usize,
}

impl<'a> ReceiverVideoPolicyInput<'a> {
    #[must_use]
    pub fn from_state(
        state: &'a RoomState,
        ranked_active_speaker_sources: &[ActiveSpeakerSource],
        receiver_bandwidth_snapshot: &ReceiverBandwidthSnapshot,
    ) -> Self {
        let featured_source_user_ids =
            featured_source_user_ids_for_active_speakers(state, ranked_active_speaker_sources);
        let active_speaker_rank_by_user =
            active_speaker_rank_by_user(state, ranked_active_speaker_sources);
        let receiver_bandwidth_by_user = receiver_bandwidth_by_user(receiver_bandwidth_snapshot);
        let visible_scalable_route_counts =
            visible_scalable_route_counts_by_consumer(state, &featured_source_user_ids);
        let routes = state
            .current_live_consumer_routes()
            .filter_map(|route| {
                let source = route.source;
                if source.media_kind() != MediaKind::Video
                    || source.policy().adaptation() == SourceAdaptationPolicy::None
                {
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
                Some(ReceiverVideoRouteInput {
                    user_count: state.user_count(),
                    source,
                    transport_ref: route.transport_ref(),
                    current_selection,
                    layout_intent,
                    visible_scalable_route_count: visible_scalable_route_counts
                        .get(&route.consumer_user_id)
                        .copied()
                        .unwrap_or(1),
                    active_speaker_rank: active_speaker_rank_by_user
                        .get(source.owner().user_id())
                        .copied(),
                    receiver_bandwidth: receiver_bandwidth_by_user
                        .get(&route.consumer_user_id)
                        .copied(),
                })
            })
            .collect();
        Self {
            routes,
            max_video_downloads_per_receiver: state.media_limits.max_video_downloads_per_receiver(),
        }
    }
}

#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct ReceiverVideoRouteInput<'a> {
    pub user_count: usize,
    pub source: &'a PublishedSourceDescriptor,
    pub transport_ref: ConsumerRouteTransportRef,
    pub current_selection: ConsumerSourceSelection,
    pub layout_intent: ReceiverVideoLayoutIntent,
    pub visible_scalable_route_count: usize,
    pub active_speaker_rank: Option<usize>,
    pub receiver_bandwidth: Option<Bitrate>,
}

impl ReceiverVideoRouteInput<'_> {
    pub const fn source_id(&self) -> PublishedSourceId {
        self.source.source_id()
    }

    pub const fn adaptation_policy(&self) -> SourceAdaptationPolicy {
        self.source.policy().adaptation()
    }

    pub fn consumer_user_id(&self) -> &UserId {
        self.transport_ref.consumer_user_id()
    }

    pub fn encodings(&self) -> SelectableRouteEncodings<'_> {
        SelectableRouteEncodings::new(self.source)
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime::room) struct SelectableRouteEncodings<'a> {
    source: &'a PublishedSourceDescriptor,
}

impl<'a> SelectableRouteEncodings<'a> {
    #[must_use]
    fn new(source: &'a PublishedSourceDescriptor) -> Self {
        Self { source }
    }

    #[must_use]
    pub fn len(self) -> usize {
        self.source.selectable_encoding_count()
    }

    #[must_use]
    pub fn get(self, rank: usize) -> Option<&'a SourceEncodingDescriptor> {
        self.source.selectable_encoding_by_rank(rank)
    }

    pub fn iter(self) -> impl Iterator<Item = &'a SourceEncodingDescriptor> {
        self.source.selectable_encodings()
    }
}

fn visible_scalable_route_counts_by_consumer(
    state: &RoomState,
    featured_source_user_ids: &BTreeSet<UserId>,
) -> BTreeMap<UserId, usize> {
    let mut counts = BTreeMap::new();
    for route in state.current_live_consumer_routes() {
        let source = route.source;
        if source.media_kind() != MediaKind::Video
            || source.policy().adaptation() != SourceAdaptationPolicy::ScalableVideo
        {
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

fn receiver_bandwidth_by_user(snapshot: &ReceiverBandwidthSnapshot) -> BTreeMap<UserId, Bitrate> {
    snapshot
        .per_session
        .iter()
        .map(|(session_key, estimate_bps)| (session_key.user_id().clone(), *estimate_bps))
        .collect()
}

fn active_speaker_rank_by_user(
    state: &RoomState,
    ranked_active_speaker_sources: &[ActiveSpeakerSource],
) -> BTreeMap<UserId, usize> {
    let mut ranks = BTreeMap::new();
    for source in ranked_active_speaker_sources {
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

pub(super) fn first_featured_source_user_for_active_speakers(
    state: &RoomState,
    ranked_active_speaker_sources: &[ActiveSpeakerSource],
) -> Option<UserId> {
    ranked_active_speaker_sources.iter().find_map(|source| {
        featured_source_owner_for_active_speaker_source(state, source.transport_media_id())
    })
}

pub(super) fn first_featured_source_users_for_active_speakers(
    state: &RoomState,
    ranked_active_speaker_sources: &[ActiveSpeakerSource],
    limit: usize,
) -> BTreeSet<UserId> {
    ranked_active_speaker_sources
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
        let route = ReceiverVideoRouteInput {
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
            active_speaker_rank: None,
            receiver_bandwidth: None,
        };

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
