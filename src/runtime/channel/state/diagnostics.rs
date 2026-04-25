use std::collections::{BTreeMap, BTreeSet};

use o_sfu_protocol::shared::{SessionId, StreamType};

use super::shared::{ChannelState, ConsumerKey};
use crate::runtime::{
    ConnectionId,
    diagnostics::{
        DiagnosticsIncomingBitrate, DiagnosticsMediaKind, DiagnosticsPublication,
        DiagnosticsQualitySummary, DiagnosticsRouteState, DiagnosticsSessionTransport,
        DiagnosticsSessionView, DiagnosticsSource, DiagnosticsSourceEncoding,
        DiagnosticsSourceSelection, DiagnosticsSubscription, DiagnosticsTemporalLayerMetadata,
        DiagnosticsTemporalLayerSelection,
    },
    source_model::{
        ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceId,
        SourceEncodingDescriptor, SourceEncodingId, SourceTemporalLayerId,
    },
    transport_adapter::TransportMediaId,
};

impl ChannelState {
    pub(in crate::runtime) fn diagnostics_incoming_bitrate_by_session(
        &self,
        per_media: &[(TransportMediaId, u64)],
    ) -> BTreeMap<SessionId, DiagnosticsIncomingBitrate> {
        let mut incoming_bitrate = BTreeMap::new();
        for (transport_media_id, bits) in per_media {
            let Some(entry) = self.source_transport_media_entry(*transport_media_id) else {
                continue;
            };
            let session_bitrate: &mut DiagnosticsIncomingBitrate = incoming_bitrate
                .entry(entry.owner_session_id().clone())
                .or_default();
            session_bitrate.total = session_bitrate.total.saturating_add(*bits);
            match entry.stream_type() {
                StreamType::Audio => {
                    session_bitrate.audio = session_bitrate.audio.saturating_add(*bits);
                }
                StreamType::Camera => {
                    session_bitrate.camera = session_bitrate.camera.saturating_add(*bits);
                }
                StreamType::Screen => {
                    session_bitrate.screen = session_bitrate.screen.saturating_add(*bits);
                }
            }
        }
        incoming_bitrate
    }

    pub(in crate::runtime) fn diagnostics_incoming_bitrate_by_source(
        &self,
        per_media: &[(TransportMediaId, u64)],
    ) -> BTreeMap<PublishedSourceId, u64> {
        let mut incoming_bitrate: BTreeMap<PublishedSourceId, u64> = BTreeMap::new();
        for (transport_media_id, bits) in per_media {
            let Some(entry) = self.source_transport_media_entry(*transport_media_id) else {
                continue;
            };
            let source_bitrate = incoming_bitrate.entry(entry.source_id).or_default();
            *source_bitrate = (*source_bitrate).saturating_add(*bits);
        }
        incoming_bitrate
    }

    pub(in crate::runtime) fn diagnostics_sources(
        &self,
        incoming_bitrate_by_source: &BTreeMap<PublishedSourceId, u64>,
    ) -> Vec<DiagnosticsSource> {
        self.sources
            .values()
            .map(|source| {
                let producer = self
                    .producer_id_by_source_id
                    .get(&source.source_id())
                    .and_then(|producer_id| self.producers.get(producer_id));
                let encodings = source
                    .encodings()
                    .map(diagnostics_source_encoding)
                    .collect();
                DiagnosticsSource {
                    active: producer.is_some_and(|producer| producer.active),
                    current_incoming_bitrate_bps: incoming_bitrate_by_source
                        .get(&source.source_id())
                        .copied()
                        .unwrap_or_default(),
                    encodings,
                    media_kind: DiagnosticsMediaKind::from(source.media_kind()),
                    mid: source.mid().map(|mid| mid.as_str().to_owned()),
                    owner_session_id: source.owner().session_id().clone(),
                    source_id: source.source_id().as_u64(),
                    stream_type: source.stream_type(),
                    transport_media_id: producer
                        .and_then(|producer| producer.transport_media_id)
                        .map(TransportMediaId::as_u64),
                }
            })
            .collect()
    }

    pub(in crate::runtime) fn diagnostics_session_views(
        &self,
        media_worker_id: usize,
        transport_by_session: &BTreeMap<SessionId, DiagnosticsSessionTransport>,
    ) -> Vec<DiagnosticsSessionView> {
        self.sessions
            .iter()
            .filter_map(|(session_id, session)| {
                let session_info = self.session_info_snapshot(session_id)?.1;
                let transport = transport_by_session
                    .get(session_id)
                    .cloned()
                    .unwrap_or_else(|| DiagnosticsSessionTransport {
                        connection_id: session.connection_id.as_u64(),
                        health: None,
                        media_worker_id,
                        quality_summary: DiagnosticsQualitySummary {
                            current_incoming_bitrate: DiagnosticsIncomingBitrate::default(),
                            sampled_metrics_available: false,
                        },
                    });
                Some(DiagnosticsSessionView {
                    publications: self.diagnostics_publications(session_id, session.connection_id),
                    session_id: session_id.clone(),
                    session_info,
                    subscriptions: self
                        .diagnostics_subscriptions(session_id, session.connection_id),
                    transport,
                })
            })
            .collect()
    }

    fn diagnostics_publications(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
    ) -> Vec<DiagnosticsPublication> {
        self.producers
            .values()
            .filter(|producer| {
                producer.owner_session_id == *session_id
                    && producer.owner_connection_id == connection_id
            })
            .filter_map(|producer| {
                let source = self.sources.get(&producer.source_id)?;
                Some(DiagnosticsPublication {
                    active: producer.active,
                    encoding_ids: source
                        .encodings()
                        .map(|encoding| encoding.encoding_id().as_u64())
                        .collect(),
                    media_kind: DiagnosticsMediaKind::from(producer.media_kind),
                    source_id: producer.source_id.as_u64(),
                    stream_type: producer.stream_type,
                    transport_media_id: producer.transport_media_id.map(TransportMediaId::as_u64),
                })
            })
            .collect()
    }

    fn diagnostics_subscriptions(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
    ) -> Vec<DiagnosticsSubscription> {
        let mut subscriptions = self
            .consumer_index
            .iter()
            .filter_map(|(key, consumer_state)| {
                if key.consumer_session_id != *session_id
                    || consumer_state.consumer_connection_id != connection_id
                {
                    return None;
                }
                let source = self.sources.get(&key.source_id)?;
                let selection = self
                    .consumer_source_selections
                    .get(key)
                    .copied()
                    .unwrap_or_else(|| ConsumerSourceSelection::open(true));
                let route_state = self.consumer_route_state(
                    session_id,
                    source.owner().session_id(),
                    source.stream_type(),
                )?;
                Some(DiagnosticsSubscription {
                    consumer_transport_media_id: Some(consumer_state.consumer_media.as_u64()),
                    producer_session_id: source.owner().session_id().clone(),
                    selection: diagnostics_source_selection(source, selection),
                    source_id: source.source_id().as_u64(),
                    source_transport_media_id: Some(consumer_state.source_media.as_u64()),
                    state: match route_state {
                        super::ConsumerRouteState::Active => DiagnosticsRouteState::Active,
                        super::ConsumerRouteState::Inactive => DiagnosticsRouteState::Inactive,
                        super::ConsumerRouteState::Absent => return None,
                    },
                    stream_type: source.stream_type(),
                })
            })
            .collect::<Vec<_>>();

        let pending_keys = self
            .pending_consumer_bootstraps
            .iter()
            .filter(|key| key.consumer_session_id == *session_id)
            .cloned()
            .collect::<BTreeSet<ConsumerKey>>();
        let existing_keys = self
            .consumer_index
            .keys()
            .filter(|key| key.consumer_session_id == *session_id)
            .cloned()
            .collect::<BTreeSet<ConsumerKey>>();
        subscriptions.extend(pending_keys.difference(&existing_keys).filter_map(|key| {
            let source = self.sources.get(&key.source_id)?;
            let selection = self
                .consumer_source_selections
                .get(key)
                .copied()
                .unwrap_or_else(|| ConsumerSourceSelection::open(true));
            Some(DiagnosticsSubscription {
                consumer_transport_media_id: None,
                producer_session_id: source.owner().session_id().clone(),
                selection: diagnostics_source_selection(source, selection),
                source_id: source.source_id().as_u64(),
                source_transport_media_id: self
                    .producer_id_by_source_id
                    .get(&key.source_id)
                    .and_then(|producer_id| self.producers.get(producer_id))
                    .and_then(|producer| producer.transport_media_id)
                    .map(TransportMediaId::as_u64),
                state: DiagnosticsRouteState::Pending,
                stream_type: source.stream_type(),
            })
        }));
        subscriptions
    }
}

fn diagnostics_source_encoding(encoding: &SourceEncodingDescriptor) -> DiagnosticsSourceEncoding {
    let negotiated_format = encoding.negotiated_format();
    let max_temporal_layer_id = encoding
        .max_temporal_layer_id()
        .map(SourceTemporalLayerId::as_u8);
    DiagnosticsSourceEncoding {
        codec: negotiated_format.map(|format| format.codec_name().to_owned()),
        encoding_id: encoding.encoding_id().as_u64(),
        max_bitrate_bps: encoding.max_bitrate(),
        max_temporal_layer_id,
        payload_type: negotiated_format.map(o_sfu_router::MediaFormat::payload_type),
        primary_ssrc: encoding.primary_ssrc().map(o_sfu_router::Ssrc::value),
        repair_ssrc: encoding.repair_ssrc().map(o_sfu_router::Ssrc::value),
        rid: encoding.rid().map(|rid| rid.as_str().to_owned()),
        temporal_layer_metadata: if max_temporal_layer_id.is_some() {
            DiagnosticsTemporalLayerMetadata::Advertised
        } else {
            DiagnosticsTemporalLayerMetadata::Absent
        },
    }
}

fn diagnostics_source_selection(
    source: &PublishedSourceDescriptor,
    selection: ConsumerSourceSelection,
) -> DiagnosticsSourceSelection {
    let selected_encoding_id = selection.selector().selected_encoding();
    let selected_temporal_layer_id = selection
        .selector()
        .selected_operating_point()
        .map(|operating_point| operating_point.max_temporal_layer_id().as_u8());
    let temporal_layer_selection = if selected_temporal_layer_id.is_some() {
        DiagnosticsTemporalLayerSelection::Selected
    } else {
        DiagnosticsTemporalLayerSelection::NotSelected
    };
    DiagnosticsSourceSelection {
        active: selection.active(),
        pressure_observations: selection.pressure_observations(),
        selection_reason: selection.selector().into(),
        selector: selection.selector().into(),
        selected_encoding_id: selected_encoding_id.map(SourceEncodingId::as_u64),
        selected_rid: selected_encoding_id
            .and_then(|encoding_id| source.encoding(encoding_id))
            .and_then(SourceEncodingDescriptor::rid)
            .map(|rid| rid.as_str().to_owned()),
        selected_temporal_layer_id,
        temporal_layer_selection,
        upgrade_observations: selection.upgrade_observations(),
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        reason = "diagnostics fixtures should fail loudly when they build invalid source graphs"
    )]

    use o_sfu_router::{MediaKind, Rid};

    use super::*;
    use crate::runtime::source_model::{
        PublishedSourceDescriptorParts, PublishedSourceOwner, SourceEncodingDescriptorParts,
        SourceOperatingPoint, SourceSelector,
    };

    fn encoding(
        source_id: PublishedSourceId,
        encoding_id: SourceEncodingId,
        max_temporal_layer_id: Option<u8>,
    ) -> SourceEncodingDescriptor {
        SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
            encoding_id,
            source_id,
            rid: Some(Rid::new("hi")),
            primary_ssrc: None,
            repair_ssrc: None,
            max_bitrate: None,
            max_temporal_layer_id: max_temporal_layer_id.and_then(SourceTemporalLayerId::new),
            negotiated_format: None,
        })
    }

    fn source_with_encoding(encoding: SourceEncodingDescriptor) -> PublishedSourceDescriptor {
        PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
            source_id: encoding.source_id(),
            owner: PublishedSourceOwner::new(SessionId::Integer(1)),
            stream_type: StreamType::Camera,
            media_kind: MediaKind::Video,
            mid: None,
            encodings: vec![encoding],
        })
        .expect("test source graph should be valid")
    }

    #[test]
    fn source_encoding_diagnostics_distinguish_absent_temporal_metadata_from_base_layer() {
        let source_id = PublishedSourceId::from_raw(7);
        let absent_encoding =
            diagnostics_source_encoding(&encoding(source_id, SourceEncodingId::from_raw(1), None));
        let base_layer_encoding = diagnostics_source_encoding(&encoding(
            source_id,
            SourceEncodingId::from_raw(2),
            Some(SourceTemporalLayerId::base().as_u8()),
        ));

        assert_eq!(
            absent_encoding.temporal_layer_metadata,
            DiagnosticsTemporalLayerMetadata::Absent
        );
        assert_eq!(absent_encoding.max_temporal_layer_id, None);
        assert_eq!(
            base_layer_encoding.temporal_layer_metadata,
            DiagnosticsTemporalLayerMetadata::Advertised
        );
        assert_eq!(base_layer_encoding.max_temporal_layer_id, Some(0));
    }

    #[test]
    fn source_selection_diagnostics_distinguish_unselected_temporal_layer_from_base_layer() {
        let source_id = PublishedSourceId::from_raw(7);
        let encoding_id = SourceEncodingId::from_raw(1);
        let source = source_with_encoding(encoding(
            source_id,
            encoding_id,
            Some(SourceTemporalLayerId::base().as_u8()),
        ));

        let mut rid_selection = ConsumerSourceSelection::open(true);
        rid_selection.set_selector(SourceSelector::Encoding(encoding_id));
        let rid_diagnostics = diagnostics_source_selection(&source, rid_selection);
        assert_eq!(
            rid_diagnostics.temporal_layer_selection,
            DiagnosticsTemporalLayerSelection::NotSelected
        );
        assert_eq!(rid_diagnostics.selected_temporal_layer_id, None);

        let mut base_layer_selection = ConsumerSourceSelection::open(true);
        base_layer_selection.set_selector(SourceSelector::OperatingPoint(
            SourceOperatingPoint::new(encoding_id, SourceTemporalLayerId::base()),
        ));
        let base_layer_diagnostics = diagnostics_source_selection(&source, base_layer_selection);
        assert_eq!(
            base_layer_diagnostics.temporal_layer_selection,
            DiagnosticsTemporalLayerSelection::Selected
        );
        assert_eq!(
            base_layer_diagnostics.selected_temporal_layer_id,
            Some(SourceTemporalLayerId::base().as_u8())
        );
    }
}
