//! Querys the diagnostics operator surface
//!
//! This module turns live room state plus bounded event history into the
//! summary, channel, and session views served by `/internal/diagnostics/...`.
//! It depend on `ObservabilityPort` rather than the full transport adapter so
//!  diagnostics stays a consumer of transport snapshots,
//! not a peer that can mutate transport state.
//!
//! The functions here are cold-path (no diagnostics for the hot routing loop) helpers used by the HTTP.
//! They gather channel snapshots, merge in transport health and bitrate
//! data, and attach the relevant recent-event history from `DiagnosticsStore`.

use crate::runtime::channel::{ChannelManager, RuntimeChannelDirectorySnapshot};
use crate::runtime::transport_adapter::ObservabilityPort;

use super::store::DiagnosticsStore;
use super::types::{
    DiagnosticsChannelDetail, DiagnosticsChannelSummary, DiagnosticsSessionDetail,
    DiagnosticsSessionLookup, DiagnosticsSessionLookupConflict, DiagnosticsSessionView,
    DiagnosticsSummaryResponse, DiagnosticsTransportCounts, DiagnosticsTransportHealth,
};

#[derive(Debug, Clone)]
struct DiagnosticsChannelSnapshot {
    detail: DiagnosticsChannelDetail,
}

/// aggregate all live channel snapshots into one process-wide view for
/// operators.:
/// - per-channel counts derived from live channel/session state
/// - transport health totals derived from `ObservabilityPort`
/// - bounded recent global events from `DiagnosticsStore`
///
/// Callers should use this when they need an overview of current runtime activity.
/// The function is cold-path only and recomputes the response from current snapshots.
pub(crate) async fn summary_response(
    channels: &ChannelManager,
    observability_port: &impl ObservabilityPort,
    diagnostics: &DiagnosticsStore,
) -> DiagnosticsSummaryResponse {
    let channel_snapshots = channel_snapshots(channels, observability_port, diagnostics).await;
    let mut transport = DiagnosticsTransportCounts::default();
    let mut recording_channels_active = 0_usize;
    let mut publications_active = 0_usize;
    let mut sessions_active = 0_usize;
    let mut subscriptions_active = 0_usize;
    for snapshot in &channel_snapshots {
        let summary = &snapshot.detail.summary;
        sessions_active = sessions_active.saturating_add(summary.session_count);
        publications_active = publications_active.saturating_add(summary.publication_count);
        subscriptions_active = subscriptions_active.saturating_add(summary.subscription_count);
        if summary.recording_state.recording == Some(true) {
            recording_channels_active = recording_channels_active.saturating_add(1);
        }
        transport.connected = transport
            .connected
            .saturating_add(summary.transport.connected);
        transport.disconnected = transport
            .disconnected
            .saturating_add(summary.transport.disconnected);
        transport.unknown = transport.unknown.saturating_add(summary.transport.unknown);
        transport.total = transport.total.saturating_add(summary.transport.total);
    }
    DiagnosticsSummaryResponse {
        channels_active: channel_snapshots.len(),
        publications_active,
        recent_events: diagnostics.global_recent_events(),
        recording_channels_active,
        sessions_active,
        subscriptions_active,
        transport,
    }
}

/// Each item is a summary for one live channel
pub(crate) async fn channels_response(
    channels: &ChannelManager,
    observability_port: &impl ObservabilityPort,
    diagnostics: &DiagnosticsStore,
) -> Vec<DiagnosticsChannelSummary> {
    channel_snapshots(channels, observability_port, diagnostics)
        .await
        .into_iter()
        .map(|snapshot| snapshot.detail.summary)
        .collect()
}

pub(crate) async fn channel_detail_response(
    channels: &ChannelManager,
    observability_port: &impl ObservabilityPort,
    diagnostics: &DiagnosticsStore,
    channel_uuid: &str,
) -> Option<DiagnosticsChannelDetail> {
    let entry = channels.directory_snapshot(channel_uuid).await?;
    Some(
        channel_snapshot(&entry, observability_port, diagnostics)
            .await
            .detail,
    )
}

/// Resolves a session-focused diagnostics query across all live channels.
///
/// Session ids are only unique within a room, so this query must scan the live
/// channels and classify the result explicitly:
/// - `Missing` when no live channel contains the requested session id
/// - `Found` when exactly one channel matches
/// - `Conflict` when multiple live channels contain the same requested session
///   id and the operator must disambiguate by chanel
///
/// if found it return the matched session view, the room's recording
/// state, and the bounded recent event history for that exact
/// `(channel_uuid, session_id)` scope.
pub(crate) async fn session_detail_response(
    channels: &ChannelManager,
    observability_port: &impl ObservabilityPort,
    diagnostics: &DiagnosticsStore,
    requested_session_id: &str,
) -> DiagnosticsSessionLookup {
    let mut matches = Vec::new();
    for entry in channels.directory_snapshots().await {
        let Some((session_view, session_id)) = entry
            .channel()
            .diagnostics_matching_session(requested_session_id, observability_port)
            .await
        else {
            continue;
        };
        matches.push(DiagnosticsSessionDetail {
            channel_uuid: entry.channel().uuid().to_owned(),
            recent_events: diagnostics.session_recent_events(entry.channel().uuid(), &session_id),
            recording_state: entry.channel().recording_state().await,
            session: session_view,
        });
    }
    match matches.len() {
        0 => DiagnosticsSessionLookup::Missing,
        1 => DiagnosticsSessionLookup::Found(matches.remove(0)),
        _ => DiagnosticsSessionLookup::Conflict(DiagnosticsSessionLookupConflict {
            matching_channel_uuids: matches
                .into_iter()
                .map(|detail| detail.channel_uuid)
                .collect(),
            requested_session_id: requested_session_id.to_owned(),
        }),
    }
}

async fn channel_snapshots(
    channels: &ChannelManager,
    observability_port: &impl ObservabilityPort,
    diagnostics: &DiagnosticsStore,
) -> Vec<DiagnosticsChannelSnapshot> {
    let entries = channels.directory_snapshots().await;
    let mut snapshots = Vec::with_capacity(entries.len());
    for entry in entries {
        snapshots.push(channel_snapshot(&entry, observability_port, diagnostics).await);
    }
    snapshots
}

async fn channel_snapshot(
    entry: &RuntimeChannelDirectorySnapshot,
    observability_port: &impl ObservabilityPort,
    diagnostics: &DiagnosticsStore,
) -> DiagnosticsChannelSnapshot {
    let sessions = entry
        .channel()
        .diagnostics_session_views(observability_port)
        .await;
    let transport = transport_counts(&sessions);
    let publication_count = sessions
        .iter()
        .map(|session| session.publications.len())
        .sum();
    let subscription_count = sessions
        .iter()
        .map(|session| session.subscriptions.len())
        .sum();
    DiagnosticsChannelSnapshot {
        detail: DiagnosticsChannelDetail {
            recent_events: diagnostics.channel_recent_events(entry.channel().uuid()),
            sessions: sessions.clone(),
            summary: DiagnosticsChannelSummary {
                create_date: entry.create_date().to_owned(),
                media_worker_id: entry.channel().media_worker_id(),
                publication_count,
                recording_state: entry.channel().recording_state().await,
                remote_address: entry.remote_address().to_owned(),
                session_count: sessions.len(),
                subscription_count,
                transport,
                uuid: entry.channel().uuid().to_owned(),
                web_rtc_enabled: entry.channel().web_rtc_enabled(),
            },
        },
    }
}

fn transport_counts(sessions: &[DiagnosticsSessionView]) -> DiagnosticsTransportCounts {
    let mut counts = DiagnosticsTransportCounts::default();
    for session in sessions {
        match session.transport.health {
            Some(DiagnosticsTransportHealth::Connected) => {
                counts.connected = counts.connected.saturating_add(1);
            }
            Some(DiagnosticsTransportHealth::Disconnected) => {
                counts.disconnected = counts.disconnected.saturating_add(1);
            }
            None => {
                counts.unknown = counts.unknown.saturating_add(1);
            }
        }
    }
    counts.total = sessions.len();
    counts
}
