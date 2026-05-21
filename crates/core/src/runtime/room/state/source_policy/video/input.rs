//! immutable input snapshot for pure receiver video policy
//!
//! this boundary normalizes room indexes and transport observations into
//! route-shaped facts so the budget planner stays pure and deterministic

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet},
};

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
    routes: Vec<ReceiverVideoRouteInput<'a>>,
    max_video_downloads_per_receiver: usize,
}

impl<'a> ReceiverVideoPolicyInput<'a> {
    #[must_use]
    fn new(
        routes: Vec<ReceiverVideoRouteInput<'a>>,
        max_video_downloads_per_receiver: usize,
    ) -> Self {
        Self {
            routes,
            max_video_downloads_per_receiver,
        }
    }

    #[must_use]
    pub fn from_state(
        state: &'a RoomState,
        active_speaker_sources: &[ActiveSpeakerSource],
        receiver_bandwidth_snapshot: &ReceiverBandwidthSnapshot,
    ) -> Self {
        let featured_source_user_ids =
            featured_source_user_ids_for_active_speakers(state, active_speaker_sources);
        let active_speaker_rank_by_user =
            active_speaker_rank_by_user(state, active_speaker_sources);
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
                    active_speaker_rank: active_speaker_rank_by_user
                        .get(source.owner().user_id())
                        .copied(),
                    receiver_bandwidth: receiver_bandwidth_by_user
                        .get(&route.consumer_user_id)
                        .copied(),
                }))
            })
            .collect();
        Self::new(
            routes,
            state.media_limits.max_video_downloads_per_receiver(),
        )
    }

    pub fn routes(&self) -> &[ReceiverVideoRouteInput<'a>] {
        &self.routes
    }

    pub const fn max_video_downloads_per_receiver(&self) -> usize {
        self.max_video_downloads_per_receiver
    }
}

#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct ReceiverVideoRouteInput<'a> {
    user_count: usize,
    source: &'a PublishedSourceDescriptor,
    transport_ref: ConsumerRouteTransportRef,
    current_selection: ConsumerSourceSelection,
    layout_intent: ReceiverVideoLayoutIntent,
    visible_scalable_route_count: usize,
    active_speaker_rank: Option<usize>,
    receiver_bandwidth: Option<Bitrate>,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct ReceiverVideoRouteInputParts<'a> {
    pub user_count: usize,
    pub source: &'a PublishedSourceDescriptor,
    pub transport_ref: ConsumerRouteTransportRef,
    pub current_selection: ConsumerSourceSelection,
    pub layout_intent: ReceiverVideoLayoutIntent,
    pub visible_scalable_route_count: usize,
    pub active_speaker_rank: Option<usize>,
    pub receiver_bandwidth: Option<Bitrate>,
}

impl<'a> ReceiverVideoRouteInput<'a> {
    #[must_use]
    pub fn new(parts: ReceiverVideoRouteInputParts<'a>) -> Self {
        Self {
            user_count: parts.user_count,
            source: parts.source,
            transport_ref: parts.transport_ref,
            current_selection: parts.current_selection,
            layout_intent: parts.layout_intent,
            visible_scalable_route_count: parts.visible_scalable_route_count,
            active_speaker_rank: parts.active_speaker_rank,
            receiver_bandwidth: parts.receiver_bandwidth,
        }
    }

    pub fn source(&self) -> &'a PublishedSourceDescriptor {
        self.source
    }

    pub const fn source_id(&self) -> PublishedSourceId {
        self.source.source_id()
    }

    pub const fn adaptation_policy(&self) -> SourceAdaptationPolicy {
        self.source.policy().adaptation()
    }

    pub fn consumer_user_id(&self) -> &UserId {
        self.transport_ref.consumer_user_id()
    }

    pub fn transport_ref(&self) -> &ConsumerRouteTransportRef {
        &self.transport_ref
    }

    pub const fn current_selection(&self) -> ConsumerSourceSelection {
        self.current_selection
    }

    pub const fn layout_intent(&self) -> ReceiverVideoLayoutIntent {
        self.layout_intent
    }

    pub const fn user_count(&self) -> usize {
        self.user_count
    }

    pub const fn visible_scalable_route_count(&self) -> usize {
        self.visible_scalable_route_count
    }

    pub const fn active_speaker_rank(&self) -> Option<usize> {
        self.active_speaker_rank
    }

    pub const fn receiver_bandwidth(&self) -> Option<Bitrate> {
        self.receiver_bandwidth
    }

    pub fn encodings(&self) -> SelectableRouteEncodings<'a> {
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
    active_speaker_sources: &[ActiveSpeakerSource],
) -> BTreeMap<UserId, usize> {
    let mut ranked_sources = active_speaker_sources.to_vec();
    ranked_sources.sort_by_key(|source| {
        (
            Reverse(source.observed_at()),
            Reverse(source.last_audio_level_dbov().unwrap_or(i8::MIN)),
            source.transport_media_id().as_u64(),
        )
    });
    let mut ranks = BTreeMap::new();
    for source in ranked_sources {
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
    active_speaker_sources: &[ActiveSpeakerSource],
) -> Option<UserId> {
    active_speaker_sources.iter().find_map(|source| {
        featured_source_owner_for_active_speaker_source(state, source.transport_media_id())
    })
}

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
            active_speaker_rank: None,
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
