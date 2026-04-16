use crate::runtime::transport_adapter::{ActiveSpeakerSource, TransportMediaId};
use crate::signaling::shared::{SessionId, StreamType};
use std::collections::BTreeSet;

use super::{
    ids::ProducerRuntimeId,
    shared::{ChannelState, ProducerKey, SourcePacketSelection},
};

const MULTIPARTY_CAMERA_SIMULCAST_SELECTION_THRESHOLD: usize = 3;
const ACTIVE_SPEAKER_CAMERA_CLEAR_LIMIT: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime::channel) struct SourcePacketSelectionUpdate {
    producer_id: ProducerRuntimeId,
    owner_session_id: SessionId,
    owner_connection_id: u64,
    transport_media_id: TransportMediaId,
    selection: Option<SourcePacketSelection>,
}

impl SourcePacketSelectionUpdate {
    pub(in crate::runtime::channel) const fn producer_id(&self) -> ProducerRuntimeId {
        self.producer_id
    }

    pub(in crate::runtime::channel) fn owner_session_id(&self) -> &SessionId {
        &self.owner_session_id
    }

    pub(in crate::runtime::channel) const fn owner_connection_id(&self) -> u64 {
        self.owner_connection_id
    }

    pub(in crate::runtime::channel) const fn transport_media_id(&self) -> TransportMediaId {
        self.transport_media_id
    }

    pub(in crate::runtime::channel) fn selection(&self) -> Option<&SourcePacketSelection> {
        self.selection.as_ref()
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
                let desired_selection = desired_source_packet_selection(
                    session_count,
                    producer.stream_type,
                    &producer.consumable_rtp_parameters,
                    &cleared_camera_session_ids,
                    &producer.owner_session_id,
                );
                if desired_selection == producer.source_packet_selection {
                    return None;
                }
                Some(SourcePacketSelectionUpdate {
                    producer_id: *producer_id,
                    owner_session_id: producer.owner_session_id.clone(),
                    owner_connection_id: producer.owner_connection_id,
                    transport_media_id,
                    selection: desired_selection,
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
                self.producer_ids_by_owner_stream
                    .contains_key(&ProducerKey::new(&owner_session_id, StreamType::Camera))
                    .then_some(owner_session_id)
            })
            .take(ACTIVE_SPEAKER_CAMERA_CLEAR_LIMIT)
            .collect()
    }

    fn producer_owner_session_id_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
        stream_type: StreamType,
    ) -> Option<SessionId> {
        self.producers.values().find_map(|producer| {
            (producer.transport_media_id == Some(transport_media_id)
                && producer.stream_type == stream_type)
                .then(|| producer.owner_session_id.clone())
        })
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
            producer
                .source_packet_selection
                .clone_from(&update.selection);
        }
    }
}

fn desired_source_packet_selection(
    session_count: usize,
    stream_type: StreamType,
    producer_rtp_parameters: &o_sfu_router::RtpParameters,
    cleared_camera_session_ids: &BTreeSet<SessionId>,
    owner_session_id: &SessionId,
) -> Option<SourcePacketSelection> {
    if stream_type != StreamType::Camera
        || session_count < MULTIPARTY_CAMERA_SIMULCAST_SELECTION_THRESHOLD
    {
        return None;
    }
    let shared_selection =
        lowest_declared_rid(producer_rtp_parameters).map(SourcePacketSelection::Rid)?;
    if cleared_camera_session_ids.contains(owner_session_id) {
        return None;
    }
    Some(shared_selection)
}

fn lowest_declared_rid(producer_rtp_parameters: &o_sfu_router::RtpParameters) -> Option<String> {
    let encodings = producer_rtp_parameters.encodings().collect::<Vec<_>>();
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
        .and_then(|(_index, encoding)| encoding.rid().map(str::to_owned))
}
