use std::collections::{BTreeMap, BTreeSet};

use crate::runtime::ConnectionId;
use crate::runtime::diagnostics::{
    DiagnosticsIncomingBitrate, DiagnosticsMediaKind, DiagnosticsPublication,
    DiagnosticsQualitySummary, DiagnosticsRouteState, DiagnosticsSessionTransport,
    DiagnosticsSessionView, DiagnosticsSubscription,
};
use crate::runtime::transport_adapter::TransportMediaId;
use o_sfu_protocol::shared::{SessionId, StreamType};

use super::shared::{ChannelState, ConsumerKey};

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
                let route_state = self.consumer_route_state(
                    session_id,
                    source.owner().session_id(),
                    source.stream_type(),
                )?;
                Some(DiagnosticsSubscription {
                    consumer_transport_media_id: Some(consumer_state.consumer_media.as_u64()),
                    producer_session_id: source.owner().session_id().clone(),
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
            Some(DiagnosticsSubscription {
                consumer_transport_media_id: None,
                producer_session_id: source.owner().session_id().clone(),
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
