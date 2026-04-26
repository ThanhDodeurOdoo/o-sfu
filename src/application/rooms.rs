//! Room-level business use cases consumed by non-streaming server edges.
//!
//! `Room` is the application facade for HTTP control-plane flows. The
//! Axum layer remains responsible for request extraction, authentication, and
//! response rendering; this facade owns the room behavior those routes ask for.

use std::{collections::BTreeMap, sync::Arc};

use o_sfu_protocol::shared::UserId;

use crate::runtime::{
    DiagnosticsStore, RuntimeTransportAdapter,
    diagnostics::{
        self, DiagnosticsUserLookup,
        types::{DiagnosticsRoomDetail, DiagnosticsRoomSummary, DiagnosticsSummaryResponse},
    },
    room::{RoomConfig, RoomManager, RuntimeRoomStatsSnapshot},
};

#[derive(Debug, Clone)]
pub(crate) struct Room {
    rooms: Arc<RoomManager>,
    diagnostics: Arc<DiagnosticsStore>,
    transport_adapter: RuntimeTransportAdapter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateRoomRequest<'a> {
    pub(crate) issuer: &'a str,
    pub(crate) key: Option<&'a str>,
    pub(crate) web_rtc_enabled: bool,
    pub(crate) recording_address: Option<String>,
    pub(crate) remote_address: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreatedRoom {
    pub(crate) uuid: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoomStats {
    pub(crate) create_date: String,
    pub(crate) uuid: String,
    pub(crate) remote_address: String,
    pub(crate) users_stats: UsersStats,
    pub(crate) web_rtc_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UsersStats {
    pub(crate) incoming_bitrate: IncomingBitrateStats,
    pub(crate) count: u64,
    pub(crate) camera_count: u64,
    pub(crate) screen_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IncomingBitrateStats {
    pub(crate) total: u64,
    pub(crate) audio: u64,
    pub(crate) camera: u64,
    pub(crate) screen: u64,
}

impl Room {
    #[must_use]
    pub(crate) fn new(
        rooms: Arc<RoomManager>,
        diagnostics: Arc<DiagnosticsStore>,
        transport_adapter: RuntimeTransportAdapter,
    ) -> Self {
        Self {
            rooms,
            diagnostics,
            transport_adapter,
        }
    }

    pub(crate) async fn create_or_get(&self, request: CreateRoomRequest<'_>) -> CreatedRoom {
        let config = RoomConfig {
            web_rtc_enabled: request.web_rtc_enabled,
            recording_address: request.recording_address,
        };
        let room = self
            .rooms
            .serve_room(
                request.issuer,
                request.key,
                &config,
                request.remote_address.as_deref(),
            )
            .await;
        CreatedRoom {
            uuid: room.uuid().to_owned(),
        }
    }

    pub(crate) async fn disconnect_users(&self, user_ids_by_room: &BTreeMap<String, Vec<UserId>>) {
        for (room_id, user_ids) in user_ids_by_room {
            self.rooms
                .disconnect_users(room_id, user_ids, &self.transport_adapter)
                .await;
        }
    }

    pub(crate) async fn stats(&self) -> Vec<RoomStats> {
        self.rooms
            .stats_snapshots(&self.transport_adapter)
            .await
            .into_iter()
            .map(RoomStats::from)
            .collect()
    }

    pub(crate) async fn diagnostics_summary(&self) -> DiagnosticsSummaryResponse {
        diagnostics::summary_response(&self.rooms, &self.transport_adapter, &self.diagnostics).await
    }

    pub(crate) async fn diagnostics_rooms(&self) -> Vec<DiagnosticsRoomSummary> {
        diagnostics::rooms_response(&self.rooms, &self.transport_adapter, &self.diagnostics).await
    }

    pub(crate) async fn diagnostics_room_detail(
        &self,
        room_id: &str,
    ) -> Option<DiagnosticsRoomDetail> {
        diagnostics::room_detail_response(
            &self.rooms,
            &self.transport_adapter,
            &self.diagnostics,
            room_id,
        )
        .await
    }

    pub(crate) async fn diagnostics_user_detail(&self, user_id: &str) -> DiagnosticsUserLookup {
        diagnostics::user_detail_response(
            &self.rooms,
            &self.transport_adapter,
            &self.diagnostics,
            user_id,
        )
        .await
    }
}

impl From<RuntimeRoomStatsSnapshot> for RoomStats {
    fn from(snapshot: RuntimeRoomStatsSnapshot) -> Self {
        Self {
            create_date: snapshot.create_date,
            uuid: snapshot.uuid,
            remote_address: snapshot.remote_address,
            users_stats: UsersStats {
                incoming_bitrate: IncomingBitrateStats {
                    total: snapshot.users_stats.incoming_bitrate.total,
                    audio: snapshot.users_stats.incoming_bitrate.audio,
                    camera: snapshot.users_stats.incoming_bitrate.camera,
                    screen: snapshot.users_stats.incoming_bitrate.screen,
                },
                count: snapshot.users_stats.count,
                camera_count: snapshot.users_stats.camera_count,
                screen_count: snapshot.users_stats.screen_count,
            },
            web_rtc_enabled: snapshot.web_rtc_enabled,
        }
    }
}
