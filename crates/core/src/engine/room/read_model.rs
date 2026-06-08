use std::collections::BTreeMap;

use super::{Room, state::RoomState};
use crate::engine::{
    MediaWorkerId, UserId,
    diagnostics::{
        self, DiagnosticsIncomingBitrate, DiagnosticsQualitySummary, DiagnosticsSource,
        DiagnosticsUserTransport, DiagnosticsUserView,
    },
    media_transport::{
        ActiveSpeakerSourceDiagnostic, MediaTransport, TransportMediaId, TransportQualitySample,
        TransportSessionKey, TransportSourceKey,
    },
    source_model::UserStreamId,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IncomingBitrateSnapshot {
    pub total: u64,
    pub by_stream: BTreeMap<UserStreamId, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomUserStatsSnapshot {
    pub incoming_bitrate: IncomingBitrateSnapshot,
    pub count: u64,
    pub active_stream_counts: BTreeMap<UserStreamId, u64>,
}

impl Room {
    pub(crate) async fn session_stats_snapshot(
        &self,
        transport: &MediaTransport,
    ) -> RoomUserStatsSnapshot {
        let state = self.state.read().await;
        let session_keys = transport_session_keys(&state);
        let transport_snapshot = transport.transport_bitrate_snapshot(&session_keys);
        let mut incoming_bitrate = IncomingBitrateSnapshot {
            total: transport_snapshot.total.as_bps(),
            ..Default::default()
        };
        for (transport_media_id, bits) in transport_snapshot.per_media {
            let Some(stream_id) =
                state.producer_stream_id_for_transport_media_id(transport_media_id)
            else {
                continue;
            };
            let entry = incoming_bitrate.by_stream.entry(stream_id).or_default();
            *entry = entry.saturating_add(bits.as_bps());
        }
        let (count, active_stream_counts) = state.user_stats_counts();
        drop(state);
        RoomUserStatsSnapshot {
            incoming_bitrate,
            count,
            active_stream_counts,
        }
    }

    pub async fn diagnostics_user_views(
        &self,
        transport: &MediaTransport,
    ) -> Vec<DiagnosticsUserView> {
        let state = self.state.read().await;
        let session_entries = state.transport_user_entries();
        let session_keys = session_entries
            .iter()
            .map(|(user_id, connection_id)| state.transport_user_key(user_id, *connection_id))
            .collect::<Vec<_>>();
        let transport_snapshot = transport.transport_bitrate_snapshot(&session_keys);
        let quality_by_session = transport
            .transport_quality_snapshot(&session_keys)
            .per_session
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        let incoming_bitrate_by_session =
            state.diagnostics_incoming_bitrate_by_session(&transport_snapshot.per_media);
        let transport_by_session = session_entries
            .into_iter()
            .map(|(user_id, connection_id)| {
                let session_key = state.transport_user_key(&user_id, connection_id);
                let transport = DiagnosticsUserTransport {
                    connection_id: connection_id.as_u64(),
                    health: transport
                        .session_transport_health(&session_key)
                        .map(diagnostics::diagnostics_transport_health),
                    media_worker_id: session_key.media_worker_id().as_usize(),
                    quality_summary: diagnostics_quality_summary(
                        incoming_bitrate_by_session
                            .get(&user_id)
                            .cloned()
                            .unwrap_or_default(),
                        quality_by_session.get(&session_key).copied(),
                    ),
                };
                (user_id, transport)
            })
            .collect();
        state.diagnostics_user_views(
            state
                .assigned_primary_media_worker_id()
                .map_or(0, MediaWorkerId::as_usize),
            &transport_by_session,
        )
    }

    pub async fn diagnostics_sources(&self, transport: &MediaTransport) -> Vec<DiagnosticsSource> {
        let active_speaker_diagnostics = active_speaker_diagnostics_by_media(
            transport.active_speaker_diagnostic_snapshot().await,
        );
        let state = self.state.read().await;
        let session_keys = transport_session_keys(&state);
        let transport_snapshot = transport.transport_bitrate_snapshot(&session_keys);
        let incoming_bitrate_by_source =
            state.diagnostics_incoming_bitrate_by_source(&transport_snapshot.per_media);
        let sources = transport_source_keys(&state);
        drop(state);
        let source_activity_by_media = transport
            .source_activity_snapshot(&sources)
            .await
            .per_media
            .into_iter()
            .map(|activity| (activity.transport_media_id(), activity))
            .collect::<BTreeMap<_, _>>();
        let state = self.state.read().await;
        state.diagnostics_sources(
            &incoming_bitrate_by_source,
            &active_speaker_diagnostics,
            &source_activity_by_media,
        )
    }

    pub async fn diagnostics_matching_user(
        &self,
        requested_user_id: &str,
        transport: &MediaTransport,
    ) -> Option<(DiagnosticsUserView, UserId)> {
        self.diagnostics_user_views(transport)
            .await
            .into_iter()
            .find(|user| user_id_matches(&user.user_id, requested_user_id))
            .map(|user| {
                let user_id = user.user_id.clone();
                (user, user_id)
            })
    }
}

fn transport_session_keys(state: &RoomState) -> Vec<TransportSessionKey> {
    state
        .transport_user_entries()
        .into_iter()
        .map(|(user_id, connection_id)| state.transport_user_key(&user_id, connection_id))
        .collect()
}

fn transport_source_keys(state: &RoomState) -> Vec<TransportSourceKey> {
    state
        .diagnostics_source_media()
        .into_iter()
        .map(|media| {
            TransportSourceKey::new(
                state.transport_user_key(&media.owner, media.connection),
                media.media,
            )
        })
        .collect()
}

fn active_speaker_diagnostics_by_media(
    diagnostics: Vec<ActiveSpeakerSourceDiagnostic>,
) -> BTreeMap<TransportMediaId, ActiveSpeakerSourceDiagnostic> {
    diagnostics
        .into_iter()
        .map(|diagnostic| (diagnostic.transport_media_id(), diagnostic))
        .collect()
}

fn diagnostics_quality_summary(
    current_incoming_bitrate: DiagnosticsIncomingBitrate,
    quality_sample: Option<TransportQualitySample>,
) -> DiagnosticsQualitySummary {
    let Some(quality_sample) = quality_sample else {
        return DiagnosticsQualitySummary {
            current_incoming_bitrate,
            sampled_metrics_available: false,
            latest_bwe_bps: None,
            rtt_ms: None,
            ingress_loss_ppm: None,
            egress_loss_ppm: None,
            egress_jitter_rtp_timestamp_units: None,
            sample_count: 0,
        };
    };
    DiagnosticsQualitySummary {
        current_incoming_bitrate,
        sampled_metrics_available: quality_sample.sample_count > 0,
        latest_bwe_bps: quality_sample.latest_bwe_bps,
        rtt_ms: quality_sample.rtt_ms,
        ingress_loss_ppm: quality_sample.ingress_loss_ppm,
        egress_loss_ppm: quality_sample.egress_loss_ppm,
        egress_jitter_rtp_timestamp_units: quality_sample.egress_jitter_rtp_timestamp_units,
        sample_count: quality_sample.sample_count,
    }
}

fn user_id_matches(user_id: &UserId, requested_user_id: &str) -> bool {
    match user_id {
        UserId::Integer(value) => value.to_string() == requested_user_id,
        UserId::String(value) => value == requested_user_id,
    }
}
