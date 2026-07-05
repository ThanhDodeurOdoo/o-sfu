use std::collections::{BTreeMap, BTreeSet};

use o_sfu_router::MediaKind;

use super::super::input::SourcePolicySnapshot;
use crate::{
    Bitrate,
    engine::{
        UserId, VideoLayoutIntent,
        room::{media_graph::ConsumerRouteTransportRef, state::RoomState},
        source_model::{
            ConsumerSourceSelection, PublishedSourceDescriptor, SourceAdaptationPolicy,
            SourceRoomPolicySelector, SourceRoutePriority,
        },
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::room) struct ReceiverVideoLayoutIntent {
    role: SourceRoomPolicySelector,
}

impl ReceiverVideoLayoutIntent {
    #[must_use]
    pub(in crate::engine::room) const fn role(self) -> SourceRoomPolicySelector {
        self.role
    }

    #[must_use]
    pub(in crate::engine::room) const fn priority(self) -> SourceRoutePriority {
        self.role.priority()
    }

    #[must_use]
    pub(super) const fn uses_featured_quality(self) -> bool {
        self.role.uses_featured_quality()
    }

    #[must_use]
    const fn counts_toward_visible_budget(self) -> bool {
        self.role.counts_toward_visible_budget()
    }

    #[must_use]
    fn resolve(
        source: &PublishedSourceDescriptor,
        preference: Option<VideoLayoutIntent>,
        active_speaker: bool,
    ) -> Self {
        let role = source
            .policy()
            .layout()
            .map_or(SourceRoomPolicySelector::Hidden, |policy| {
                policy.resolve(preference, active_speaker)
            });
        Self { role }
    }
}

pub(super) fn receiver_video_routes<'a>(
    state: &RoomState,
    input: &SourcePolicySnapshot<'a>,
) -> Vec<ReceiverVideoRouteInput<'a>> {
    let mut visible_scalable_route_counts = BTreeMap::new();
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
        if source.policy().adaptation() == SourceAdaptationPolicy::ScalableVideo
            && layout_intent.counts_toward_visible_budget()
        {
            *visible_scalable_route_counts
                .entry(route.transport_ref.consumer_user_id.clone())
                .or_default() += 1;
        }
        routes.push(ReceiverVideoRouteInput {
            user_count: input.user_count,
            source,
            transport_ref: route.transport_ref.clone(),
            current_selection: route.current_selection,
            layout_intent,
            visible_scalable_route_count: 1,
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
    for route in &mut routes {
        route.visible_scalable_route_count = visible_scalable_route_counts
            .get(&route.transport_ref.consumer_user_id)
            .copied()
            .unwrap_or(1);
    }
    routes
}

#[derive(Debug)]
pub(super) struct ReceiverVideoRouteInput<'a> {
    pub(super) user_count: usize,
    pub(super) source: &'a PublishedSourceDescriptor,
    pub(super) transport_ref: ConsumerRouteTransportRef,
    pub(super) current_selection: ConsumerSourceSelection,
    pub(super) layout_intent: ReceiverVideoLayoutIntent,
    pub(super) visible_scalable_route_count: usize,
    pub(super) active_speaker_rank: Option<usize>,
    pub(super) receiver_bandwidth: Option<Bitrate>,
}

impl RoomState {
    #[must_use]
    pub(in crate::engine::room) fn receiver_video_layout_intent(
        &self,
        consumer_user_id: &UserId,
        source: &PublishedSourceDescriptor,
        active_speaker_source_user_ids: &BTreeSet<UserId>,
    ) -> ReceiverVideoLayoutIntent {
        let preference = layout_preference(self, consumer_user_id, source);
        ReceiverVideoLayoutIntent::resolve(
            source,
            preference,
            active_speaker_source_user_ids.contains(source.owner().user_id()),
        )
    }

    #[must_use]
    pub(in crate::engine::room) fn diagnostics_video_layout_intent(
        &self,
        consumer_user_id: &UserId,
        source: &PublishedSourceDescriptor,
    ) -> Option<ReceiverVideoLayoutIntent> {
        source.policy().layout()?;
        let preference = layout_preference(self, consumer_user_id, source);
        let active_speaker = self
            .users
            .get(source.owner().user_id())
            .is_some_and(|user| user.featured() == Some(true));
        Some(ReceiverVideoLayoutIntent::resolve(
            source,
            preference,
            active_speaker,
        ))
    }
}

fn layout_preference(
    state: &RoomState,
    consumer_user_id: &UserId,
    source: &PublishedSourceDescriptor,
) -> Option<VideoLayoutIntent> {
    state
        .users
        .get(consumer_user_id)
        .and_then(|user| {
            user.desired_source_subscriptions
                .get(source.owner().user_id())
        })
        .and_then(|states| states.get(source.stream_id()))
        .and_then(|intent| intent.layout())
}
