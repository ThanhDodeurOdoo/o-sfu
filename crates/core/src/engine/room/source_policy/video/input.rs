use std::collections::{BTreeMap, BTreeSet};

use o_sfu_router::MediaKind;

use super::{
    super::input::{SourcePolicyRoute, SourcePolicySnapshot},
    layout::ReceiverVideoLayoutIntent,
};
use crate::{
    Bitrate,
    engine::{
        UserId,
        room::{media_graph::ConsumerRouteTransportRef, state::RoomState},
        source_model::{
            ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceId,
            SourceAdaptationPolicy, SourceEncodingDescriptor,
        },
    },
};

pub(super) fn receiver_video_routes<'a>(
    state: &RoomState,
    input: &SourcePolicySnapshot<'a>,
) -> Vec<ReceiverVideoRouteInput<'a>> {
    let visible_scalable_route_counts = visible_scalable_route_counts_by_consumer(
        state,
        &input.routes,
        &input.featured_source_user_ids,
    );
    let mut routes = Vec::with_capacity(input.routes.len());
    for route in &input.routes {
        let source = route.source;
        if source.media_kind() != MediaKind::Video
            || (source.policy().adaptation() == SourceAdaptationPolicy::None
                && source.policy().video_bitrate_cap().is_none())
        {
            continue;
        }
        let layout_intent = state.receiver_video_layout_intent(
            &route.transport_ref.consumer_user_id,
            source,
            &input.featured_source_user_ids,
        );
        routes.push(ReceiverVideoRouteInput {
            user_count: input.user_count,
            source,
            transport_ref: route.transport_ref.clone(),
            current_selection: route.current_selection,
            layout_intent,
            visible_scalable_route_count: visible_scalable_route_counts
                .get(&route.transport_ref.consumer_user_id)
                .copied()
                .unwrap_or(1),
            active_speaker_rank: input
                .active_speaker_rank_by_user
                .get(source.owner().user_id())
                .copied(),
            receiver_bandwidth: input
                .receiver_bandwidth_by_connection
                .get(&route.transport_ref.consumer_connection_id)
                .copied(),
        });
    }
    routes
}

#[derive(Debug)]
pub struct ReceiverVideoRouteInput<'a> {
    pub user_count: usize,
    pub source: &'a PublishedSourceDescriptor,
    pub(super) transport_ref: ConsumerRouteTransportRef,
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
    routes: &[SourcePolicyRoute<'_>],
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
            &route.transport_ref.consumer_user_id,
            source,
            featured_source_user_ids,
        );
        if !layout_intent.counts_toward_visible_budget() {
            continue;
        }
        *counts
            .entry(route.transport_ref.consumer_user_id.clone())
            .or_default() += 1;
    }
    counts
}
