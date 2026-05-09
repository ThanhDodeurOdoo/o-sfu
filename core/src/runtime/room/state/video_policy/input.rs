//! Immutable input for pure receiver video policy.
//!
//! This module is the only place where the policy path reads `RoomState`
//! indexes and transport observation snapshots together. It normalizes that
//! state into route-shaped facts so the budget planner can be tested without a
//! `Room`, media transport, websocket user, or `str0m` state.
//!
//! Source behavior enters this path through the committed
//! [`PublishedSourceDescriptor`]. That keeps product vocabulary at the
//! orchestration edge while letting the planner consume media kind, layout
//! policy, adaptation policy and active-speaker role as generic route facts.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    super::shared::RoomState,
    layout::{ReceiverVideoLayoutIntent, featured_source_user_ids_for_active_speakers},
};
use crate::runtime::{
    ConnectionId, UserId,
    media_transport::{ActiveSpeakerSource, ReceiverBandwidthSnapshot, TransportMediaId},
    source_model::{
        ActiveSpeakerSourceRole, ConsumerSourceSelection, PublishedSourceDescriptor,
        PublishedSourceId, SourceAdaptationPolicy, SourceEncodingDescriptor,
    },
};

/// Policy input for one refresh across all live receiver/source video routes.
///
/// Routes retain `consumer_index` order so the budget planner can process
/// contiguous receiver groups without rebuilding a map.
#[derive(Debug)]
pub(in crate::runtime::room) struct ReceiverVideoPolicyInput<'a> {
    routes: Vec<ReceiverVideoRouteInput<'a>>,
}

impl<'a> ReceiverVideoPolicyInput<'a> {
    #[must_use]
    fn new(routes: Vec<ReceiverVideoRouteInput<'a>>) -> Self {
        Self { routes }
    }

    #[must_use]
    pub(in crate::runtime::room) fn from_state(
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
            .consumer_index
            .iter()
            .filter_map(|(consumer_key, consumer_state)| {
                let source = state.sources.get(&consumer_key.source_id)?;
                if source.selectable_encoding_count() == 0 {
                    return None;
                }
                let producer_id = state
                    .producer_id_by_source_id
                    .get(&consumer_key.source_id)?;
                let producer = state.producers.get(producer_id)?;
                if !producer.active {
                    return None;
                }
                let current_selection = state
                    .consumer_source_selections
                    .get(consumer_key)
                    .copied()
                    .unwrap_or_else(|| ConsumerSourceSelection::open(true));
                if !current_selection.active() {
                    return None;
                }
                let layout_intent = state.receiver_video_layout_intent(
                    &consumer_key.consumer_user_id,
                    source,
                    &featured_source_user_ids,
                );
                Some(ReceiverVideoRouteInput::new(ReceiverVideoRouteInputParts {
                    user_count: state.user_count(),
                    source,
                    consumer_user_id: &consumer_key.consumer_user_id,
                    consumer_connection_id: consumer_state.consumer_connection_id,
                    source_user_id: source.owner().user_id(),
                    source_connection_id: consumer_state.source_connection_id,
                    source_transport_media_id: consumer_state.source_media,
                    consumer_transport_media_id: consumer_state.consumer_media,
                    current_selection,
                    layout_intent,
                    visible_scalable_route_count: visible_scalable_route_counts
                        .get(&consumer_key.consumer_user_id)
                        .copied()
                        .unwrap_or(1),
                    receiver_bandwidth_bps: receiver_bandwidth_by_user
                        .get(&consumer_key.consumer_user_id)
                        .copied(),
                }))
            })
            .collect();
        Self::new(routes)
    }

    pub(in crate::runtime::room) fn routes(&self) -> &[ReceiverVideoRouteInput<'a>] {
        &self.routes
    }
}

/// Immutable facts the planner needs for one receiver/source route.
#[derive(Debug, Clone)]
pub(in crate::runtime::room) struct ReceiverVideoRouteInput<'a> {
    user_count: usize,
    source: &'a PublishedSourceDescriptor,
    consumer_user_id: &'a UserId,
    consumer_connection_id: ConnectionId,
    source_user_id: &'a UserId,
    source_connection_id: ConnectionId,
    source_transport_media_id: TransportMediaId,
    consumer_transport_media_id: TransportMediaId,
    current_selection: ConsumerSourceSelection,
    layout_intent: ReceiverVideoLayoutIntent,
    visible_scalable_route_count: usize,
    receiver_bandwidth_bps: Option<u64>,
}

/// Construction input for [`ReceiverVideoRouteInput`].
///
/// The route input is intentionally explicit so pure policy tests can build the
/// planner input without constructing a full room or media transport.
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime::room) struct ReceiverVideoRouteInputParts<'a> {
    pub(in crate::runtime::room) user_count: usize,
    pub(in crate::runtime::room) source: &'a PublishedSourceDescriptor,
    pub(in crate::runtime::room) consumer_user_id: &'a UserId,
    pub(in crate::runtime::room) consumer_connection_id: ConnectionId,
    pub(in crate::runtime::room) source_user_id: &'a UserId,
    pub(in crate::runtime::room) source_connection_id: ConnectionId,
    pub(in crate::runtime::room) source_transport_media_id: TransportMediaId,
    pub(in crate::runtime::room) consumer_transport_media_id: TransportMediaId,
    pub(in crate::runtime::room) current_selection: ConsumerSourceSelection,
    pub(in crate::runtime::room) layout_intent: ReceiverVideoLayoutIntent,
    pub(in crate::runtime::room) visible_scalable_route_count: usize,
    pub(in crate::runtime::room) receiver_bandwidth_bps: Option<u64>,
}

impl<'a> ReceiverVideoRouteInput<'a> {
    #[must_use]
    pub(in crate::runtime::room) fn new(parts: ReceiverVideoRouteInputParts<'a>) -> Self {
        Self {
            user_count: parts.user_count,
            source: parts.source,
            consumer_user_id: parts.consumer_user_id,
            consumer_connection_id: parts.consumer_connection_id,
            source_user_id: parts.source_user_id,
            source_connection_id: parts.source_connection_id,
            source_transport_media_id: parts.source_transport_media_id,
            consumer_transport_media_id: parts.consumer_transport_media_id,
            current_selection: parts.current_selection,
            layout_intent: parts.layout_intent,
            visible_scalable_route_count: parts.visible_scalable_route_count,
            receiver_bandwidth_bps: parts.receiver_bandwidth_bps,
        }
    }

    pub(in crate::runtime::room) fn source(&self) -> &'a PublishedSourceDescriptor {
        self.source
    }

    pub(in crate::runtime::room) const fn source_id(&self) -> PublishedSourceId {
        self.source.source_id()
    }

    pub(in crate::runtime::room) const fn adaptation_policy(&self) -> SourceAdaptationPolicy {
        self.source.policy().adaptation()
    }

    pub(in crate::runtime::room) fn consumer_user_id(&self) -> &'a UserId {
        self.consumer_user_id
    }

    pub(in crate::runtime::room) const fn consumer_connection_id(&self) -> ConnectionId {
        self.consumer_connection_id
    }

    pub(in crate::runtime::room) fn source_user_id(&self) -> &'a UserId {
        self.source_user_id
    }

    pub(in crate::runtime::room) const fn source_connection_id(&self) -> ConnectionId {
        self.source_connection_id
    }

    pub(in crate::runtime::room) const fn source_transport_media_id(&self) -> TransportMediaId {
        self.source_transport_media_id
    }

    pub(in crate::runtime::room) const fn consumer_transport_media_id(&self) -> TransportMediaId {
        self.consumer_transport_media_id
    }

    pub(in crate::runtime::room) const fn current_selection(&self) -> ConsumerSourceSelection {
        self.current_selection
    }

    pub(in crate::runtime::room) const fn layout_intent(&self) -> ReceiverVideoLayoutIntent {
        self.layout_intent
    }

    pub(in crate::runtime::room) const fn user_count(&self) -> usize {
        self.user_count
    }

    pub(in crate::runtime::room) const fn visible_scalable_route_count(&self) -> usize {
        self.visible_scalable_route_count
    }

    pub(in crate::runtime::room) const fn receiver_bandwidth_bps(&self) -> Option<u64> {
        self.receiver_bandwidth_bps
    }

    pub(in crate::runtime::room) fn encodings(&self) -> SelectableRouteEncodings<'a> {
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
    pub(in crate::runtime::room) fn len(self) -> usize {
        self.source.selectable_encoding_count()
    }

    #[must_use]
    pub(in crate::runtime::room) fn get(self, rank: usize) -> Option<&'a SourceEncodingDescriptor> {
        self.source.selectable_encoding_by_rank(rank)
    }

    pub(in crate::runtime::room) fn iter(
        self,
    ) -> impl Iterator<Item = &'a SourceEncodingDescriptor> {
        self.source.selectable_encodings()
    }
}

fn visible_scalable_route_counts_by_consumer(
    state: &RoomState,
    featured_source_user_ids: &BTreeSet<UserId>,
) -> BTreeMap<UserId, usize> {
    let mut counts = BTreeMap::new();
    for (consumer_key, consumer_state) in &state.consumer_index {
        let Some(source) = state.sources.get(&consumer_key.source_id) else {
            continue;
        };
        if source.policy().adaptation() != SourceAdaptationPolicy::ScalableVideo {
            continue;
        }
        let Some(producer_id) = state.producer_id_by_source_id.get(&consumer_key.source_id) else {
            continue;
        };
        let Some(producer) = state.producers.get(producer_id) else {
            continue;
        };
        let Some(consumer_connection_id) = state.user_connection_id(&consumer_key.consumer_user_id)
        else {
            continue;
        };
        if !producer.active
            || consumer_state.consumer_connection_id != consumer_connection_id
            || !state
                .consumer_source_selections
                .get(consumer_key)
                .is_none_or(|selection| selection.active())
        {
            continue;
        }
        let layout_intent = state.receiver_video_layout_intent(
            &consumer_key.consumer_user_id,
            source,
            featured_source_user_ids,
        );
        if !layout_intent.counts_toward_visible_budget() {
            continue;
        }
        *counts
            .entry(consumer_key.consumer_user_id.clone())
            .or_default() += 1;
    }
    counts
}

fn receiver_bandwidth_by_user(snapshot: &ReceiverBandwidthSnapshot) -> BTreeMap<UserId, u64> {
    snapshot
        .per_session
        .iter()
        .map(|(session_key, estimate_bps)| (session_key.user_id().clone(), *estimate_bps))
        .collect()
}

pub(super) fn featured_source_owner_for_active_speaker_source(
    state: &RoomState,
    transport_media_id: TransportMediaId,
) -> Option<UserId> {
    let entry = state.source_transport_media_entry(transport_media_id)?;
    let detector_source = state.sources.get(&entry.source_id())?;
    let detector_policy = detector_source.policy().active_speaker()?;
    if detector_policy.role() != ActiveSpeakerSourceRole::Detector {
        return None;
    }
    let owner_user_id = entry.owner_user_id().clone();
    state
        .source_ids_by_owner
        .get(&owner_user_id)?
        .iter()
        .filter_map(|source_id| state.sources.get(source_id))
        .any(|source| {
            source.policy().active_speaker().is_some_and(|policy| {
                policy.group() == detector_policy.group()
                    && policy.role() == ActiveSpeakerSourceRole::Promotable
            })
        })
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
    use crate::runtime::source_model::{
        PublishedSourceDescriptorParts, PublishedSourceOwner, SourceEncodingDescriptorParts,
        SourceEncodingId, SourceModelError, SourcePolicy, SourceRoomPolicySelector, UserStreamId,
    };

    fn source_encoding(
        source_id: PublishedSourceId,
        encoding_id: SourceEncodingId,
        rid: &str,
        max_bitrate: u64,
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
                source_encoding(source_id, high_encoding_id, "hi", 900_000),
                source_encoding(source_id, low_encoding_id, "lo", 150_000),
                source_encoding(source_id, middle_encoding_id, "mid", 450_000),
            ],
        })?;
        let route = ReceiverVideoRouteInput::new(ReceiverVideoRouteInputParts {
            user_count: 2,
            source: &source,
            consumer_user_id: &consumer_user_id,
            consumer_connection_id: ConnectionId::from_raw(10),
            source_user_id: &source_user_id,
            source_connection_id: ConnectionId::from_raw(9),
            source_transport_media_id: TransportMediaId::new(11),
            consumer_transport_media_id: TransportMediaId::new(12),
            current_selection: ConsumerSourceSelection::open(true),
            layout_intent: ReceiverVideoLayoutIntent::new(
                SourceRoomPolicySelector::VisibleThumbnail,
            ),
            visible_scalable_route_count: 1,
            receiver_bandwidth_bps: None,
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
