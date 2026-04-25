//! Immutable input for pure receiver video policy.
//!
//! This module is the only place where the policy path reads `ChannelState`
//! indexes and transport observation snapshots together. It normalizes that
//! state into route-shaped facts so the budget planner can be tested without a
//! `Channel`, transport adapter, websocket session, or `str0m` state.

use std::collections::{BTreeMap, BTreeSet};

use o_sfu_protocol::shared::{SessionId, StreamType};

use super::{
    super::shared::{ChannelState, SourceKey},
    layout::{ReceiverVideoLayoutIntent, featured_camera_session_ids_for_active_speakers},
};
use crate::runtime::{
    ConnectionId,
    source_model::{
        ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceId,
        SourceEncodingDescriptor,
    },
    transport_adapter::{ActiveSpeakerSource, ReceiverBandwidthSnapshot, TransportMediaId},
};

/// Policy input for one refresh across all live receiver/source video routes.
#[derive(Debug)]
pub(in crate::runtime::channel) struct ReceiverVideoPolicyInput<'a> {
    routes: Vec<ReceiverVideoRouteInput<'a>>,
}

impl<'a> ReceiverVideoPolicyInput<'a> {
    #[must_use]
    pub(in crate::runtime::channel) fn new(routes: Vec<ReceiverVideoRouteInput<'a>>) -> Self {
        Self { routes }
    }

    #[must_use]
    pub(in crate::runtime::channel) fn from_state(
        state: &'a ChannelState,
        active_speaker_sources: &[ActiveSpeakerSource],
        receiver_bandwidth_snapshot: &ReceiverBandwidthSnapshot,
    ) -> Self {
        let featured_camera_session_ids =
            featured_camera_session_ids_for_active_speakers(state, active_speaker_sources);
        let receiver_bandwidth_by_session =
            receiver_bandwidth_by_session(receiver_bandwidth_snapshot);
        let visible_camera_route_counts =
            visible_camera_route_counts_by_consumer(state, &featured_camera_session_ids);
        let selectable_encoding_ladders = selectable_encoding_ladders_by_source(&state.sources);
        let routes = state
            .consumer_index
            .iter()
            .filter_map(|(consumer_key, consumer_state)| {
                let source = state.sources.get(&consumer_key.source_id)?;
                let encodings = selectable_encoding_ladders.get(&consumer_key.source_id)?;
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
                    &consumer_key.consumer_session_id,
                    source,
                    &featured_camera_session_ids,
                );
                Some(ReceiverVideoRouteInput::new(ReceiverVideoRouteInputParts {
                    session_count: state.session_count(),
                    source,
                    consumer_session_id: &consumer_key.consumer_session_id,
                    consumer_connection_id: consumer_state.consumer_connection_id,
                    source_session_id: source.owner().session_id(),
                    source_connection_id: consumer_state.source_connection_id,
                    source_transport_media_id: consumer_state.source_media,
                    consumer_transport_media_id: consumer_state.consumer_media,
                    current_selection,
                    layout_intent,
                    visible_camera_route_count: visible_camera_route_counts
                        .get(&consumer_key.consumer_session_id)
                        .copied()
                        .unwrap_or(1),
                    receiver_bandwidth_bps: receiver_bandwidth_by_session
                        .get(&consumer_key.consumer_session_id)
                        .copied(),
                    encodings: encodings.clone(),
                }))
            })
            .collect();
        Self::new(routes)
    }

    pub(in crate::runtime::channel) fn routes(&self) -> &[ReceiverVideoRouteInput<'a>] {
        &self.routes
    }
}

/// Immutable facts the planner needs for one receiver/source route.
#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct ReceiverVideoRouteInput<'a> {
    session_count: usize,
    source: &'a PublishedSourceDescriptor,
    consumer_session_id: &'a SessionId,
    consumer_connection_id: ConnectionId,
    source_session_id: &'a SessionId,
    source_connection_id: ConnectionId,
    source_transport_media_id: TransportMediaId,
    consumer_transport_media_id: TransportMediaId,
    current_selection: ConsumerSourceSelection,
    layout_intent: ReceiverVideoLayoutIntent,
    visible_camera_route_count: usize,
    receiver_bandwidth_bps: Option<u64>,
    encodings: Vec<&'a SourceEncodingDescriptor>,
}

/// Construction input for [`ReceiverVideoRouteInput`].
///
/// The route input is intentionally explicit so pure policy tests can build the
/// planner input without constructing a full channel or transport adapter.
#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct ReceiverVideoRouteInputParts<'a> {
    pub(in crate::runtime::channel) session_count: usize,
    pub(in crate::runtime::channel) source: &'a PublishedSourceDescriptor,
    pub(in crate::runtime::channel) consumer_session_id: &'a SessionId,
    pub(in crate::runtime::channel) consumer_connection_id: ConnectionId,
    pub(in crate::runtime::channel) source_session_id: &'a SessionId,
    pub(in crate::runtime::channel) source_connection_id: ConnectionId,
    pub(in crate::runtime::channel) source_transport_media_id: TransportMediaId,
    pub(in crate::runtime::channel) consumer_transport_media_id: TransportMediaId,
    pub(in crate::runtime::channel) current_selection: ConsumerSourceSelection,
    pub(in crate::runtime::channel) layout_intent: ReceiverVideoLayoutIntent,
    pub(in crate::runtime::channel) visible_camera_route_count: usize,
    pub(in crate::runtime::channel) receiver_bandwidth_bps: Option<u64>,
    pub(in crate::runtime::channel) encodings: Vec<&'a SourceEncodingDescriptor>,
}

impl<'a> ReceiverVideoRouteInput<'a> {
    #[must_use]
    pub(in crate::runtime::channel) fn new(parts: ReceiverVideoRouteInputParts<'a>) -> Self {
        Self {
            session_count: parts.session_count,
            source: parts.source,
            consumer_session_id: parts.consumer_session_id,
            consumer_connection_id: parts.consumer_connection_id,
            source_session_id: parts.source_session_id,
            source_connection_id: parts.source_connection_id,
            source_transport_media_id: parts.source_transport_media_id,
            consumer_transport_media_id: parts.consumer_transport_media_id,
            current_selection: parts.current_selection,
            layout_intent: parts.layout_intent,
            visible_camera_route_count: parts.visible_camera_route_count,
            receiver_bandwidth_bps: parts.receiver_bandwidth_bps,
            encodings: parts.encodings,
        }
    }

    pub(in crate::runtime::channel) fn source(&self) -> &'a PublishedSourceDescriptor {
        self.source
    }

    pub(in crate::runtime::channel) const fn source_id(&self) -> PublishedSourceId {
        self.source.source_id()
    }

    pub(in crate::runtime::channel) const fn stream_type(&self) -> StreamType {
        self.source.stream_type()
    }

    pub(in crate::runtime::channel) fn consumer_session_id(&self) -> &'a SessionId {
        self.consumer_session_id
    }

    pub(in crate::runtime::channel) const fn consumer_connection_id(&self) -> ConnectionId {
        self.consumer_connection_id
    }

    pub(in crate::runtime::channel) fn source_session_id(&self) -> &'a SessionId {
        self.source_session_id
    }

    pub(in crate::runtime::channel) const fn source_connection_id(&self) -> ConnectionId {
        self.source_connection_id
    }

    pub(in crate::runtime::channel) const fn source_transport_media_id(&self) -> TransportMediaId {
        self.source_transport_media_id
    }

    pub(in crate::runtime::channel) const fn consumer_transport_media_id(
        &self,
    ) -> TransportMediaId {
        self.consumer_transport_media_id
    }

    pub(in crate::runtime::channel) const fn current_selection(&self) -> ConsumerSourceSelection {
        self.current_selection
    }

    pub(in crate::runtime::channel) const fn layout_intent(&self) -> ReceiverVideoLayoutIntent {
        self.layout_intent
    }

    pub(in crate::runtime::channel) const fn session_count(&self) -> usize {
        self.session_count
    }

    pub(in crate::runtime::channel) const fn visible_camera_route_count(&self) -> usize {
        self.visible_camera_route_count
    }

    pub(in crate::runtime::channel) const fn receiver_bandwidth_bps(&self) -> Option<u64> {
        self.receiver_bandwidth_bps
    }

    pub(in crate::runtime::channel) fn encodings(&self) -> &[&'a SourceEncodingDescriptor] {
        &self.encodings
    }
}

fn visible_camera_route_counts_by_consumer(
    state: &ChannelState,
    featured_camera_session_ids: &BTreeSet<SessionId>,
) -> BTreeMap<SessionId, usize> {
    let mut counts = BTreeMap::new();
    for (consumer_key, consumer_state) in &state.consumer_index {
        let Some(source) = state.sources.get(&consumer_key.source_id) else {
            continue;
        };
        if source.stream_type() != StreamType::Camera {
            continue;
        }
        let Some(producer_id) = state.producer_id_by_source_id.get(&consumer_key.source_id) else {
            continue;
        };
        let Some(producer) = state.producers.get(producer_id) else {
            continue;
        };
        let Some(consumer_connection_id) =
            state.session_connection_id(&consumer_key.consumer_session_id)
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
            &consumer_key.consumer_session_id,
            source,
            featured_camera_session_ids,
        );
        if !layout_intent.counts_toward_visible_budget() {
            continue;
        }
        *counts
            .entry(consumer_key.consumer_session_id.clone())
            .or_default() += 1;
    }
    counts
}

fn receiver_bandwidth_by_session(snapshot: &ReceiverBandwidthSnapshot) -> BTreeMap<SessionId, u64> {
    snapshot
        .per_session
        .iter()
        .map(|(session_key, estimate_bps)| (session_key.session_id().clone(), *estimate_bps))
        .collect()
}

fn selectable_encoding_ladders_by_source(
    sources: &BTreeMap<PublishedSourceId, PublishedSourceDescriptor>,
) -> BTreeMap<PublishedSourceId, Vec<&SourceEncodingDescriptor>> {
    sources
        .iter()
        .filter_map(|(source_id, source)| {
            let encodings = selectable_encodings(source);
            (!encodings.is_empty()).then_some((*source_id, encodings))
        })
        .collect()
}

pub(super) fn selectable_encodings(
    source: &PublishedSourceDescriptor,
) -> Vec<&SourceEncodingDescriptor> {
    let mut encodings = source.encodings().collect::<Vec<_>>();
    if encodings.iter().any(|encoding| encoding.rid().is_none()) {
        return Vec::new();
    }
    let use_declared_order = encodings
        .iter()
        .all(|encoding| encoding.max_bitrate().is_none());
    if !use_declared_order {
        encodings.sort_by_key(|encoding| encoding.max_bitrate().unwrap_or(u64::MAX));
    }
    encodings
}

pub(super) fn camera_owner_for_active_audio_source(
    state: &ChannelState,
    transport_media_id: TransportMediaId,
) -> Option<SessionId> {
    let owner_session_id = state
        .source_transport_media_entry(transport_media_id)
        .filter(|entry| entry.stream_type() == StreamType::Audio)
        .map(|entry| entry.owner_session_id().clone())?;
    state
        .source_ids_by_owner_stream
        .contains_key(&SourceKey::new(&owner_session_id, StreamType::Camera))
        .then_some(owner_session_id)
}

pub(super) fn first_featured_camera_session_for_active_speakers(
    state: &ChannelState,
    active_speaker_sources: &[ActiveSpeakerSource],
) -> Option<SessionId> {
    active_speaker_sources
        .iter()
        .find_map(|source| camera_owner_for_active_audio_source(state, source.transport_media_id()))
}

pub(super) fn first_featured_camera_sessions_for_active_speakers(
    state: &ChannelState,
    active_speaker_sources: &[ActiveSpeakerSource],
    limit: usize,
) -> BTreeSet<SessionId> {
    active_speaker_sources
        .iter()
        .filter_map(|source| {
            camera_owner_for_active_audio_source(state, source.transport_media_id())
        })
        .take(limit)
        .collect()
}
