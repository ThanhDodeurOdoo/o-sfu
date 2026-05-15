//! Querys the diagnostics operator surface
//!
//! This module turns live room state plus bounded event history into the
//! summary, room, and user views served by `/internal/diagnostics/...`.
//! It depend on `ObservabilityPort` rather than the full media transport so
//!  diagnostics stays a consumer of transport snapshots,
//! not a peer that can mutate transport state.
//!
//! The functions here are cold-path (no diagnostics for the hot routing loop) helpers used by the HTTP.
//! They gather room snapshots, merge in transport health and bitrate
//! data, and attach the relevant recent-event history from `DiagnosticsStore`.

use std::collections::{BTreeMap, BTreeSet};

use o_sfu_core::server::{session::UserId, transport::TransportPlacementPressureSnapshot};

use super::{
    DiagnosticsStore,
    types::{
        DiagnosticsRoomDetail, DiagnosticsRoomSummary, DiagnosticsSummaryResponse,
        DiagnosticsTransportCounts, DiagnosticsTransportHealth, DiagnosticsUserDetail,
        DiagnosticsUserLookup, DiagnosticsUserLookupConflict, DiagnosticsUserSummary,
        DiagnosticsUserView, DiagnosticsWorkerPressure, DiagnosticsWorkerSummary,
    },
};
use crate::{
    application::stream_catalog::{
        AUDIO_STREAM_LABEL, CAMERA_STREAM_LABEL, SCREEN_STREAM_LABEL,
        diagnostics_bitrate_for_stream_id,
    },
    runtime::{
        media_transport::ObservabilityPort,
        room::{RoomManager, RuntimeRoomDirectorySnapshot},
    },
};

#[derive(Debug, Clone)]
struct DiagnosticsRoomSnapshot {
    detail: DiagnosticsRoomDetail,
}

#[derive(Debug, Clone)]
struct DiagnosticsWorkerAccumulator {
    connected_user_count: usize,
    disconnected_user_count: usize,
    media_worker_id: usize,
    pressure: DiagnosticsWorkerPressure,
    publication_count: usize,
    room_ids: BTreeSet<String>,
    subscription_count: usize,
    unknown_user_count: usize,
    user_count: usize,
}

impl DiagnosticsWorkerAccumulator {
    fn new(media_worker_id: usize) -> Self {
        Self {
            connected_user_count: 0,
            disconnected_user_count: 0,
            media_worker_id,
            pressure: DiagnosticsWorkerPressure::default(),
            publication_count: 0,
            room_ids: BTreeSet::new(),
            subscription_count: 0,
            unknown_user_count: 0,
            user_count: 0,
        }
    }

    fn record_user(&mut self, room_id: &str, user: &DiagnosticsUserView) {
        self.room_ids.insert(room_id.to_owned());
        self.user_count = self.user_count.saturating_add(1);
        self.publication_count = self
            .publication_count
            .saturating_add(user.publications.len());
        self.subscription_count = self
            .subscription_count
            .saturating_add(user.subscriptions.len());
        match user.transport.health {
            Some(DiagnosticsTransportHealth::Connected) => {
                self.connected_user_count = self.connected_user_count.saturating_add(1);
            }
            Some(DiagnosticsTransportHealth::Disconnected) => {
                self.disconnected_user_count = self.disconnected_user_count.saturating_add(1);
            }
            None => {
                self.unknown_user_count = self.unknown_user_count.saturating_add(1);
            }
        }
    }

    fn record_empty_room(&mut self, room_id: &str) {
        self.room_ids.insert(room_id.to_owned());
    }

    fn set_pressure(&mut self, pressure: DiagnosticsWorkerPressure) {
        self.pressure = pressure;
    }

    fn into_summary(self) -> DiagnosticsWorkerSummary {
        DiagnosticsWorkerSummary {
            connected_user_count: self.connected_user_count,
            disconnected_user_count: self.disconnected_user_count,
            media_worker_id: self.media_worker_id,
            pressure: self.pressure,
            publication_count: self.publication_count,
            room_count: self.room_ids.len(),
            subscription_count: self.subscription_count,
            unknown_user_count: self.unknown_user_count,
            user_count: self.user_count,
        }
    }
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

pub(crate) async fn room_users_response(
    rooms: &RoomManager,
    observability_port: &impl ObservabilityPort,
    diagnostics: &DiagnosticsStore,
    room_id: &str,
) -> Option<Vec<DiagnosticsUserSummary>> {
    let entry = rooms.directory_snapshot(room_id).await?;
    let snapshot = room_snapshot(&entry, observability_port, diagnostics).await;
    Some(user_summaries(&snapshot.detail))
}

pub(crate) async fn workers_response(
    rooms: &RoomManager,
    observability_port: &impl ObservabilityPort,
) -> Vec<DiagnosticsWorkerSummary> {
    let mut workers = (0..rooms.media_worker_count())
        .map(|media_worker_id| {
            (
                media_worker_id,
                DiagnosticsWorkerAccumulator::new(media_worker_id),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for pressure in observability_port.worker_pressure_snapshots() {
        workers
            .entry(pressure.media_worker_id)
            .or_insert_with(|| DiagnosticsWorkerAccumulator::new(pressure.media_worker_id))
            .set_pressure(diagnostics_worker_pressure(pressure.pressure));
    }
    for entry in rooms.directory_snapshots().await {
        let room = entry.room();
        let room_id = room.uuid();
        let users = room.diagnostics_user_views(observability_port).await;
        if users.is_empty() {
            workers
                .entry(room.media_worker_id())
                .or_insert_with(|| DiagnosticsWorkerAccumulator::new(room.media_worker_id()))
                .record_empty_room(room_id);
            continue;
        }
        for user in &users {
            workers
                .entry(user.transport.media_worker_id)
                .or_insert_with(|| {
                    DiagnosticsWorkerAccumulator::new(user.transport.media_worker_id)
                })
                .record_user(room_id, user);
        }
    }
    workers
        .into_values()
        .map(DiagnosticsWorkerAccumulator::into_summary)
        .collect()
}

/// Resolves a user-focused diagnostics query from the diagnostics user index.
///
/// User ids are only unique within a room, so this query classifies indexed
/// room matches explicitly:
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
    let room_ids = diagnostics.user_lookup_room_ids(requested_user_id);
    if room_ids.is_empty() {
        return DiagnosticsUserLookup::Missing;
    }
    let mut matches = Vec::new();
    for entry in rooms.directory_snapshots_for_room_ids(&room_ids).await {
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
    let source_count = sources.len();
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
                source_count,
                user_count: users.len(),
                subscription_count,
                transport,
                uuid: entry.room().uuid().to_owned(),
                web_rtc_enabled: entry.room().web_rtc_enabled(),
            },
        },
    }
}

fn user_summaries(detail: &DiagnosticsRoomDetail) -> Vec<DiagnosticsUserSummary> {
    detail
        .users
        .iter()
        .map(|user| {
            let bitrate = &user.transport.quality_summary.current_incoming_bitrate;
            DiagnosticsUserSummary {
                audio_incoming_bitrate_bps: diagnostics_bitrate_for_stream_id(
                    &bitrate.by_stream_bps,
                    AUDIO_STREAM_LABEL,
                ),
                camera_incoming_bitrate_bps: diagnostics_bitrate_for_stream_id(
                    &bitrate.by_stream_bps,
                    CAMERA_STREAM_LABEL,
                ),
                connection_id: user.transport.connection_id,
                health: user.transport.health.clone(),
                incoming_bitrate_bps: bitrate.total,
                media_worker_id: user.transport.media_worker_id,
                publication_count: user.publications.len(),
                room_id: detail.summary.uuid.clone(),
                screen_incoming_bitrate_bps: diagnostics_bitrate_for_stream_id(
                    &bitrate.by_stream_bps,
                    SCREEN_STREAM_LABEL,
                ),
                subscription_count: user.subscriptions.len(),
                user_id: user.user_id.clone(),
                user_key: user_id_to_path_segment(&user.user_id),
            }
        })
        .collect()
}

fn user_id_to_path_segment(user_id: &UserId) -> String {
    match user_id {
        UserId::Integer(value) => value.to_string(),
        UserId::String(value) => value.clone(),
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

fn diagnostics_worker_pressure(
    pressure: TransportPlacementPressureSnapshot,
) -> DiagnosticsWorkerPressure {
    DiagnosticsWorkerPressure {
        command_backlog_depth: pressure.command_backlog_depth,
        egress_bitrate_bps: pressure.egress_bitrate.as_bps(),
        packet_loop_lag_ms: pressure.packet_loop_lag_ms,
        relay_mailbox_depth: pressure.relay_mailbox_depth,
        worker_pressure_score: pressure.worker_pressure_score,
    }
}
