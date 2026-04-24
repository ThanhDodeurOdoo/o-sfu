use std::collections::BTreeSet;

use o_sfu_protocol::shared::{SessionId, StreamType};

use super::{
    super::{ChannelEventMessage, outbound::MessageFanout},
    ids::ProducerRuntimeId,
    shared::{ChannelState, SourceKey},
};
use crate::runtime::{
    ConnectionId,
    source_model::{
        PublishedSourceDescriptor, SourceEncodingDescriptor, SourceEncodingId, SourceSelector,
    },
    transport_adapter::{ActiveSpeakerSource, SourcePacketGate, TransportMediaId},
};

const MULTIPARTY_CAMERA_SIMULCAST_SELECTION_THRESHOLD: usize = 3;
const ACTIVE_SPEAKER_CAMERA_CLEAR_LIMIT: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::channel) struct SourcePacketSelectionUpdate {
    producer_id: ProducerRuntimeId,
    owner_session_id: SessionId,
    owner_connection_id: ConnectionId,
    transport_media_id: TransportMediaId,
    selector: SourceSelector,
    packet_gate: SourcePacketGate,
}

impl SourcePacketSelectionUpdate {
    pub(in crate::runtime::channel) const fn producer_id(&self) -> ProducerRuntimeId {
        self.producer_id
    }

    pub(in crate::runtime::channel) fn owner_session_id(&self) -> &SessionId {
        &self.owner_session_id
    }

    pub(in crate::runtime::channel) const fn owner_connection_id(&self) -> ConnectionId {
        self.owner_connection_id
    }

    pub(in crate::runtime::channel) const fn transport_media_id(&self) -> TransportMediaId {
        self.transport_media_id
    }

    pub(in crate::runtime::channel) const fn selector(&self) -> SourceSelector {
        self.selector
    }

    pub(in crate::runtime::channel) fn packet_gate(&self) -> &SourcePacketGate {
        &self.packet_gate
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::channel) struct FeaturedSessionUpdate {
    session_id: SessionId,
    featured: Option<bool>,
}

impl FeaturedSessionUpdate {
    pub(in crate::runtime::channel) fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub(in crate::runtime::channel) const fn featured(&self) -> Option<bool> {
        self.featured
    }
}

impl ChannelState {
    pub(in crate::runtime::channel) fn source_packet_selection_updates(
        &self,
        active_speaker_sources: &[ActiveSpeakerSource],
    ) -> Vec<SourcePacketSelectionUpdate> {
        let session_count = self.session_count();
        let cleared_camera_session_ids =
            self.cleared_camera_session_ids_for_active_speakers(active_speaker_sources);
        self.producers
            .iter()
            .filter_map(|(producer_id, producer)| {
                let transport_media_id = producer.transport_media_id?;
                let source = self.sources.get(&producer.source_id)?;
                let desired_selector = desired_source_packet_selector(
                    session_count,
                    producer.stream_type,
                    source,
                    &cleared_camera_session_ids,
                    &producer.owner_session_id,
                );
                if desired_selector == producer.source_packet_selector {
                    return None;
                }
                let packet_gate = source_packet_gate_for_selector(source, desired_selector)?;
                Some(SourcePacketSelectionUpdate {
                    producer_id: *producer_id,
                    owner_session_id: producer.owner_session_id.clone(),
                    owner_connection_id: producer.owner_connection_id,
                    transport_media_id,
                    selector: desired_selector,
                    packet_gate,
                })
            })
            .collect()
    }

    fn cleared_camera_session_ids_for_active_speakers(
        &self,
        active_speaker_sources: &[ActiveSpeakerSource],
    ) -> BTreeSet<SessionId> {
        active_speaker_sources
            .iter()
            .filter_map(|source| {
                let owner_session_id = self.producer_owner_session_id_for_transport_media_id(
                    source.transport_media_id(),
                    StreamType::Audio,
                )?;
                self.source_ids_by_owner_stream
                    .contains_key(&SourceKey::new(&owner_session_id, StreamType::Camera))
                    .then_some(owner_session_id)
            })
            .take(ACTIVE_SPEAKER_CAMERA_CLEAR_LIMIT)
            .collect()
    }

    pub(in crate::runtime::channel) fn featured_session_updates(
        &self,
        active_speaker_sources: &[ActiveSpeakerSource],
    ) -> Vec<FeaturedSessionUpdate> {
        let desired_featured_session_id =
            self.featured_session_id_for_active_speakers(active_speaker_sources);
        let should_clear_featured_state = desired_featured_session_id.is_none()
            && self
                .sessions
                .values()
                .any(|session| session.layout.featured().is_some());
        if desired_featured_session_id.is_none() && !should_clear_featured_state {
            return Vec::new();
        }
        self.sessions
            .iter()
            .filter_map(|(session_id, session)| {
                let desired_featured = desired_featured_session_id.as_ref().map_or_else(
                    || session.layout.featured().is_some().then_some(false),
                    |featured_session_id| Some(featured_session_id == session_id),
                );
                (desired_featured != session.layout.featured()).then(|| FeaturedSessionUpdate {
                    session_id: session_id.clone(),
                    featured: desired_featured,
                })
            })
            .collect()
    }

    fn featured_session_id_for_active_speakers(
        &self,
        active_speaker_sources: &[ActiveSpeakerSource],
    ) -> Option<SessionId> {
        active_speaker_sources.iter().find_map(|source| {
            let owner_session_id = self.producer_owner_session_id_for_transport_media_id(
                source.transport_media_id(),
                StreamType::Audio,
            )?;
            self.source_ids_by_owner_stream
                .contains_key(&SourceKey::new(&owner_session_id, StreamType::Camera))
                .then_some(owner_session_id)
        })
    }

    fn producer_owner_session_id_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
        stream_type: StreamType,
    ) -> Option<SessionId> {
        self.source_transport_media_entry(transport_media_id)
            .filter(|entry| entry.stream_type() == stream_type)
            .map(|entry| entry.owner_session_id().clone())
    }

    pub(in crate::runtime::channel) fn commit_source_packet_selection_updates(
        &mut self,
        updates: &[SourcePacketSelectionUpdate],
    ) {
        for update in updates {
            let Some(producer) = self.producers.get_mut(&update.producer_id()) else {
                continue;
            };
            if producer.owner_session_id != *update.owner_session_id()
                || producer.owner_connection_id != update.owner_connection_id()
                || producer.transport_media_id != Some(update.transport_media_id())
            {
                continue;
            }
            producer.source_packet_selector = update.selector();
        }
    }

    pub(in crate::runtime::channel) fn commit_featured_session_updates(
        &mut self,
        updates: &[FeaturedSessionUpdate],
    ) -> Option<MessageFanout> {
        let changed_session_ids = updates
            .iter()
            .filter_map(|update| {
                let session = self.sessions.get_mut(update.session_id())?;
                if session.layout.featured() == update.featured() {
                    return None;
                }
                session.layout.set_featured(update.featured());
                Some(update.session_id().clone())
            })
            .collect::<Vec<_>>();
        if changed_session_ids.is_empty() {
            return None;
        }
        let snapshot = changed_session_ids
            .into_iter()
            .filter_map(|session_id| self.session_info_snapshot(&session_id))
            .collect();
        Some(self.fanout_all(&ChannelEventMessage::SessionInfoChanged(snapshot)))
    }
}

fn desired_source_packet_selector(
    session_count: usize,
    stream_type: StreamType,
    source: &PublishedSourceDescriptor,
    cleared_camera_session_ids: &BTreeSet<SessionId>,
    owner_session_id: &SessionId,
) -> SourceSelector {
    if stream_type != StreamType::Camera
        || session_count < MULTIPARTY_CAMERA_SIMULCAST_SELECTION_THRESHOLD
    {
        return SourceSelector::Open;
    }
    if cleared_camera_session_ids.contains(owner_session_id) {
        return SourceSelector::Open;
    }
    lowest_declared_encoding(source).map_or(SourceSelector::Open, SourceSelector::Encoding)
}

fn source_packet_gate_for_selector(
    source: &PublishedSourceDescriptor,
    selector: SourceSelector,
) -> Option<SourcePacketGate> {
    match selector {
        SourceSelector::Open => Some(SourcePacketGate::Open),
        SourceSelector::Encoding(encoding_id) => source
            .encoding(encoding_id)
            .and_then(SourceEncodingDescriptor::rid)
            .map(|rid| SourcePacketGate::Rid(rid.as_str().to_owned())),
        SourceSelector::RoomPolicy(_) => None,
    }
}

fn lowest_declared_encoding(source: &PublishedSourceDescriptor) -> Option<SourceEncodingId> {
    let encodings = source.encodings().collect::<Vec<_>>();
    if encodings.len() < 2 || encodings.iter().any(|encoding| encoding.rid().is_none()) {
        return None;
    }
    let use_declared_order = encodings
        .iter()
        .all(|encoding| encoding.max_bitrate().is_none());
    encodings
        .into_iter()
        .enumerate()
        .min_by_key(|(index, encoding)| {
            if use_declared_order {
                (0_u64, *index)
            } else {
                (encoding.max_bitrate().unwrap_or(u64::MAX), *index)
            }
        })
        .map(|(_index, encoding)| encoding.encoding_id())
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        reason = "test fixtures should fail loudly when they build invalid source graphs"
    )]

    use o_sfu_router::{MediaKind, Rid};

    use super::*;
    use crate::runtime::source_model::{
        PublishedSourceDescriptorParts, PublishedSourceId, PublishedSourceOwner,
        SourceEncodingDescriptorParts,
    };

    fn source_with_encodings(
        encodings: Vec<SourceEncodingDescriptor>,
    ) -> PublishedSourceDescriptor {
        PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
            source_id: PublishedSourceId::from_raw(7),
            owner: PublishedSourceOwner::new(SessionId::Integer(42), ConnectionId::from_raw(9)),
            stream_type: StreamType::Camera,
            media_kind: MediaKind::Video,
            mid: None,
            encodings,
        })
        .expect("test source graph should be valid")
    }

    fn encoding(
        source_id: PublishedSourceId,
        encoding_id: SourceEncodingId,
        rid: Option<&str>,
        max_bitrate: Option<u64>,
    ) -> SourceEncodingDescriptor {
        SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
            encoding_id,
            source_id,
            rid: rid.map(Rid::new),
            primary_ssrc: None,
            repair_ssrc: None,
            max_bitrate,
            negotiated_format: None,
            transport_binding: None,
        })
    }

    #[test]
    fn source_selector_bridge_projects_selected_encoding_to_transport_rid() {
        let source_id = PublishedSourceId::from_raw(7);
        let high_encoding_id = SourceEncodingId::from_raw(1);
        let low_encoding_id = SourceEncodingId::from_raw(2);
        let source = source_with_encodings(vec![
            encoding(source_id, high_encoding_id, Some("hi"), Some(750_000)),
            encoding(source_id, low_encoding_id, Some("lo"), Some(150_000)),
        ]);

        let selector = lowest_declared_encoding(&source)
            .map_or(SourceSelector::Open, SourceSelector::Encoding);

        assert_eq!(selector, SourceSelector::Encoding(low_encoding_id));
        assert_eq!(
            source_packet_gate_for_selector(&source, selector),
            Some(SourcePacketGate::Rid(String::from("lo")))
        );
    }

    #[test]
    fn source_selector_bridge_keeps_open_as_an_explicit_transport_gate() {
        let source_id = PublishedSourceId::from_raw(7);
        let source = source_with_encodings(vec![encoding(
            source_id,
            SourceEncodingId::from_raw(1),
            None,
            None,
        )]);

        assert_eq!(
            source_packet_gate_for_selector(&source, SourceSelector::Open),
            Some(SourcePacketGate::Open)
        );
    }

    #[test]
    fn source_selector_bridge_requires_rid_for_rid_packet_selection() {
        let source_id = PublishedSourceId::from_raw(7);
        let encoding_id = SourceEncodingId::from_raw(1);
        let source = source_with_encodings(vec![encoding(source_id, encoding_id, None, None)]);

        assert_eq!(
            source_packet_gate_for_selector(&source, SourceSelector::Encoding(encoding_id)),
            None
        );
    }
}
