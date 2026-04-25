//! Querys the diagnostics operator surface
//!
//! This module turns live room state plus bounded event history into the
//! summary, room, and user views served by `/internal/diagnostics/...`.
//! It depend on `ObservabilityPort` rather than the full transport adapter so
//!  diagnostics stays a consumer of transport snapshots,
//! not a peer that can mutate transport state.
//!
//! The functions here are cold-path (no diagnostics for the hot routing loop) helpers used by the HTTP.
//! They gather room snapshots, merge in transport health and bitrate
//! data, and attach the relevant recent-event history from `DiagnosticsStore`.

use super::{
    store::DiagnosticsStore,
    types::{
        DiagnosticsRoomDetail, DiagnosticsRoomSummary, DiagnosticsSummaryResponse,
        DiagnosticsTransportCounts, DiagnosticsTransportHealth, DiagnosticsUserDetail,
        DiagnosticsUserLookup, DiagnosticsUserLookupConflict, DiagnosticsUserView,
    },
};
use crate::runtime::{
    room::{RoomManager, RuntimeRoomDirectorySnapshot},
    transport_adapter::ObservabilityPort,
};

#[derive(Debug, Clone)]
struct DiagnosticsRoomSnapshot {
    detail: DiagnosticsRoomDetail,
}

/// aggregate all live room snapshots into one process-wide view for
/// operators.:
/// - per-room counts derived from live room/user state
/// - transport health totals derived from `ObservabilityPort`
/// - bounded recent global events from `DiagnosticsStore`
///
/// Callers should use this when they need an overview of current runtime activity.
/// The function is cold-path only and recomputes the response from current snapshots.
pub(crate) async fn summary_response(
    rooms: &RoomManager,
    observability_port: &impl ObservabilityPort,
    diagnostics: &DiagnosticsStore,
) -> DiagnosticsSummaryResponse {
    let room_snapshots = room_snapshots(rooms, observability_port, diagnostics).await;
    let mut transport = DiagnosticsTransportCounts::default();
    let mut recording_rooms_active = 0_usize;
    let mut publications_active = 0_usize;
    let mut users_active = 0_usize;
    let mut subscriptions_active = 0_usize;
    for snapshot in &room_snapshots {
        let summary = &snapshot.detail.summary;
        users_active = users_active.saturating_add(summary.user_count);
        publications_active = publications_active.saturating_add(summary.publication_count);
        subscriptions_active = subscriptions_active.saturating_add(summary.subscription_count);
        if summary.recording_state.recording == Some(true) {
            recording_rooms_active = recording_rooms_active.saturating_add(1);
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
        rooms_active: room_snapshots.len(),
        publications_active,
        recent_events: diagnostics.global_recent_events(),
        recording_rooms_active,
        users_active,
        subscriptions_active,
        transport,
    }
}

/// Each item is a summary for one live room
pub(crate) async fn rooms_response(
    rooms: &RoomManager,
    observability_port: &impl ObservabilityPort,
    diagnostics: &DiagnosticsStore,
) -> Vec<DiagnosticsRoomSummary> {
    room_snapshots(rooms, observability_port, diagnostics)
        .await
        .into_iter()
        .map(|snapshot| snapshot.detail.summary)
        .collect()
}

pub(crate) async fn room_detail_response(
    rooms: &RoomManager,
    observability_port: &impl ObservabilityPort,
    diagnostics: &DiagnosticsStore,
    room_id: &str,
) -> Option<DiagnosticsRoomDetail> {
    let entry = rooms.directory_snapshot(room_id).await?;
    Some(
        room_snapshot(&entry, observability_port, diagnostics)
            .await
            .detail,
    )
}

/// Resolves a user-focused diagnostics query across all live rooms.
///
/// User ids are only unique within a room, so this query must scan the live
/// rooms and classify the result explicitly:
/// - `Missing` when no live room contains the requested user id
/// - `Found` when exactly one room matches
/// - `Conflict` when multiple live rooms contain the same requested user
///   id and the operator must disambiguate by room
///
/// if found it return the matched user view, the room's recording
/// state, and the bounded recent event history for that exact
/// `(room_id, user_id)` scope.
pub(crate) async fn user_detail_response(
    rooms: &RoomManager,
    observability_port: &impl ObservabilityPort,
    diagnostics: &DiagnosticsStore,
    requested_user_id: &str,
) -> DiagnosticsUserLookup {
    let mut matches = Vec::new();
    for entry in rooms.directory_snapshots().await {
        let Some((user_view, user_id)) = entry
            .room()
            .diagnostics_matching_user(requested_user_id, observability_port)
            .await
        else {
            continue;
        };
        matches.push(DiagnosticsUserDetail {
            room_id: entry.room().uuid().to_owned(),
            recent_events: diagnostics.user_recent_events(entry.room().uuid(), &user_id),
            recording_state: entry.room().recording_state().await,
            user: user_view,
        });
    }
    match matches.len() {
        0 => DiagnosticsUserLookup::Missing,
        1 => DiagnosticsUserLookup::Found(matches.remove(0)),
        _ => DiagnosticsUserLookup::Conflict(DiagnosticsUserLookupConflict {
            matching_room_ids: matches.into_iter().map(|detail| detail.room_id).collect(),
            requested_user_id: requested_user_id.to_owned(),
        }),
    }
}

async fn room_snapshots(
    rooms: &RoomManager,
    observability_port: &impl ObservabilityPort,
    diagnostics: &DiagnosticsStore,
) -> Vec<DiagnosticsRoomSnapshot> {
    let entries = rooms.directory_snapshots().await;
    let mut snapshots = Vec::with_capacity(entries.len());
    for entry in entries {
        snapshots.push(room_snapshot(&entry, observability_port, diagnostics).await);
    }
    snapshots
}

async fn room_snapshot(
    entry: &RuntimeRoomDirectorySnapshot,
    observability_port: &impl ObservabilityPort,
    diagnostics: &DiagnosticsStore,
) -> DiagnosticsRoomSnapshot {
    let users = entry
        .room()
        .diagnostics_user_views(observability_port)
        .await;
    let sources = entry.room().diagnostics_sources(observability_port).await;
    let transport = transport_counts(&users);
    let publication_count = users.iter().map(|user| user.publications.len()).sum();
    let subscription_count = users.iter().map(|user| user.subscriptions.len()).sum();
    DiagnosticsRoomSnapshot {
        detail: DiagnosticsRoomDetail {
            recent_events: diagnostics.room_recent_events(entry.room().uuid()),
            users: users.clone(),
            sources,
            summary: DiagnosticsRoomSummary {
                create_date: entry.create_date().to_owned(),
                media_worker_id: entry.room().media_worker_id(),
                publication_count,
                recording_state: entry.room().recording_state().await,
                remote_address: entry.remote_address().to_owned(),
                user_count: users.len(),
                subscription_count,
                transport,
                uuid: entry.room().uuid().to_owned(),
                web_rtc_enabled: entry.room().web_rtc_enabled(),
            },
        },
    }
}

fn transport_counts(users: &[DiagnosticsUserView]) -> DiagnosticsTransportCounts {
    let mut counts = DiagnosticsTransportCounts::default();
    for user in users {
        match user.transport.health {
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
    counts.total = users.len();
    counts
}
