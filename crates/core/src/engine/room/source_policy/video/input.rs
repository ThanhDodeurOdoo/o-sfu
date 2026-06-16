use std::collections::{BTreeMap, BTreeSet};

use o_sfu_router::MediaKind;

use super::{
    super::input::{SourcePolicyInput, SourcePolicyRouteInput},
    layout::ReceiverVideoLayoutIntent,
};
use crate::{
    Bitrate,
    engine::{
        UserId,
        media_transport::TransportConsumerRoute,
        room::{media_graph::ConsumerRouteTransportRef, state::RoomState},
        source_model::{
            ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceId,
            SourceAdaptationPolicy, SourceEncodingDescriptor,
        },
    },
};

pub(super) fn receiver_video_routes<'a>(
    state: &RoomState,
    input: &SourcePolicyInput<'a>,
) -> Vec<ReceiverVideoRouteInput<'a>> {
    let visible_scalable_route_counts = visible_scalable_route_counts_by_consumer(
        state,
        &input.routes,
        &input.featured_source_user_ids,
    );
    input
        .routes
        .iter()
        .filter_map(|route| {
            let source = route.source;
            if source.media_kind() != MediaKind::Video
                || (source.policy().adaptation() == SourceAdaptationPolicy::None
                    && source.policy().video_bitrate_cap().is_none())
            {
                return None;
            }
            let layout_intent = state.receiver_video_layout_intent(
                &route.route.consumer_user_id,
                source,
                &input.featured_source_user_ids,
            );
            Some(ReceiverVideoRouteInput {
                user_count: input.user_count,
                source,
                route: route.route.clone(),
                transport_route: route.transport_route.clone(),
                current_selection: route.current_selection,
                layout_intent,
                visible_scalable_route_count: visible_scalable_route_counts
                    .get(&route.route.consumer_user_id)
                    .copied()
                    .unwrap_or(1),
                active_speaker_rank: input
                    .active_speaker_rank_by_user
                    .get(source.owner().user_id())
                    .copied(),
                receiver_bandwidth: input
                    .receiver_bandwidth_by_user
                    .get(&route.route.consumer_user_id)
                    .copied(),
            })
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct ReceiverVideoRouteInput<'a> {
    pub user_count: usize,
    pub source: &'a PublishedSourceDescriptor,
    pub route: ConsumerRouteTransportRef,
    pub transport_route: TransportConsumerRoute,
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
        &self.route.consumer_user_id
    }

    pub fn encodings(&self) -> SelectableRouteEncodings<'_> {
        SelectableRouteEncodings::new(self.source)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SelectableRouteEncodings<'a> {
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
    routes: &[SourcePolicyRouteInput<'_>],
    featured_source_user_ids: &BTreeSet<UserId>,
) -> BTreeMap<UserId, usize> {
    let mut counts = BTreeMap::new();
    for route in routes {
        let source = route.source;
        if source.media_kind() != MediaKind::Video
            || source.policy().adaptation() != SourceAdaptationPolicy::ScalableVideo
        {
            continue;
        }
        let layout_intent = state.receiver_video_layout_intent(
            &route.route.consumer_user_id,
            source,
            featured_source_user_ids,
        );
        if !layout_intent.counts_toward_visible_budget() {
            continue;
        }
        *counts
            .entry(route.route.consumer_user_id.clone())
            .or_default() += 1;
    }
    counts
}
