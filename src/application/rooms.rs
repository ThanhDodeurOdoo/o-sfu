//! Room-level business use cases consumed by non-streaming server edges.
//!
//! `CallRooms` is the application facade for HTTP control-plane flows. The
//! Axum layer remains responsible for request extraction, authentication, and
//! response rendering; this facade owns the room behavior those routes ask for.

use std::{collections::BTreeMap, sync::Arc};

use o_sfu_protocol::{
    shared::{UserId, UserPermissions},
    signaling::RecordingOptions,
};
use room::{
    JoinUserRequest, Room as CoreRoom, RoomConfig, RoomEventMessage, RoomEventRequest, RoomManager,
    RoomManagerJoinError, RuntimeRoomStatsSnapshot, UserCloseReason, UserOutbound,
};
use tokio::sync::mpsc;

use crate::{
    core::{RuntimeSfuCore, RuntimeTransportAdapter, User, runtime::room},
    runtime::{
        ConnectionId, DiagnosticsStore,
        diagnostics::{
            self, DiagnosticsUserLookup,
            types::{DiagnosticsRoomDetail, DiagnosticsRoomSummary, DiagnosticsSummaryResponse},
        },
    },
};

#[derive(Debug, Clone)]
pub(crate) struct CallRooms {
    manager: Arc<RoomManager>,
    diagnostics: Arc<DiagnosticsStore>,
    transport_adapter: RuntimeTransportAdapter,
    media_core: RuntimeSfuCore,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServeRoomRequest<'a> {
    pub(crate) issuer: &'a str,
    pub(crate) key: Option<&'a str>,
    pub(crate) web_rtc_enabled: bool,
    pub(crate) recording_address: Option<String>,
    pub(crate) remote_address: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServedRoom {
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

#[derive(Debug, Clone)]
pub(crate) struct RoomHandle {
    room: Arc<CoreRoom>,
}

pub(crate) type UserOutboundEvent = UserOutbound;
pub(crate) type UserCloseReasonEvent = UserCloseReason;
pub(crate) type RoomMessageEvent = RoomEventMessage;
pub(crate) type RoomRequestEvent = RoomEventRequest;

pub(crate) struct JoinRoomUserRequest {
    pub(crate) user_id: UserId,
    pub(crate) label: Option<String>,
    pub(crate) permissions: UserPermissions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinRoomUserError {
    MissingRoom,
    RoomFull,
    RouterState,
}

pub(crate) struct JoinedRoomUser {
    pub(crate) room: RoomHandle,
    pub(crate) user_id: UserId,
    pub(crate) connection_id: ConnectionId,
    pub(crate) outbound_rx: mpsc::UnboundedReceiver<UserOutboundEvent>,
    pub(crate) user: User,
}

impl CallRooms {
    #[must_use]
    pub(crate) fn new(
        manager: Arc<RoomManager>,
        diagnostics: Arc<DiagnosticsStore>,
        transport_adapter: RuntimeTransportAdapter,
        media_core: RuntimeSfuCore,
    ) -> Self {
        Self {
            manager,
            diagnostics,
            transport_adapter,
            media_core,
        }
    }

    pub(crate) async fn serve(&self, request: ServeRoomRequest<'_>) -> ServedRoom {
        let config = RoomConfig {
            web_rtc_enabled: request.web_rtc_enabled,
            recording_address: request.recording_address,
        };
        let room = self
            .manager
            .serve_room(
                request.issuer,
                request.key,
                &config,
                request.remote_address.as_deref(),
            )
            .await;
        ServedRoom {
            uuid: room.uuid().to_owned(),
        }
    }

    pub(crate) async fn disconnect_users(&self, user_ids_by_room: &BTreeMap<String, Vec<UserId>>) {
        for (room_id, user_ids) in user_ids_by_room {
            self.manager
                .disconnect_users(room_id, user_ids, &self.transport_adapter)
                .await;
        }
    }

    pub(crate) async fn by_uuid(&self, room_id: &str) -> Option<RoomHandle> {
        Some(RoomHandle {
            room: self.manager.get_by_uuid(room_id).await?,
        })
    }

    pub(crate) async fn join_user(
        &self,
        room: &RoomHandle,
        request: JoinRoomUserRequest,
        remote_address: Arc<str>,
    ) -> Result<JoinedRoomUser, JoinRoomUserError> {
        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();
        let user_id = request.user_id.clone();
        let (room, connection_id) = self
            .manager
            .join_user(
                room.uuid(),
                JoinUserRequest {
                    user_id: request.user_id,
                    label: request.label,
                    permissions: request.permissions,
                    sender: outbound_tx,
                },
                &self.transport_adapter,
            )
            .await
            .map_err(JoinRoomUserError::from)?;
        let user = User::new(
            user_id.clone(),
            connection_id,
            remote_address,
            Arc::clone(&room),
            self.media_core.clone(),
        );
        Ok(JoinedRoomUser {
            room: RoomHandle { room },
            user_id,
            connection_id,
            outbound_rx,
            user,
        })
    }

    pub(crate) async fn remove_user(
        &self,
        room_id: &str,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> bool {
        self.manager
            .close_session(room_id, user_id, connection_id, &self.transport_adapter)
            .await
    }

    pub(crate) async fn stats(&self) -> Vec<RoomStats> {
        self.manager
            .stats_snapshots(&self.transport_adapter)
            .await
            .into_iter()
            .map(RoomStats::from)
            .collect()
    }

    pub(crate) async fn diagnostics_summary(&self) -> DiagnosticsSummaryResponse {
        diagnostics::summary_response(&self.manager, &self.transport_adapter, &self.diagnostics)
            .await
    }

    pub(crate) async fn diagnostics_rooms(&self) -> Vec<DiagnosticsRoomSummary> {
        diagnostics::rooms_response(&self.manager, &self.transport_adapter, &self.diagnostics).await
    }

    pub(crate) async fn diagnostics_room_detail(
        &self,
        room_id: &str,
    ) -> Option<DiagnosticsRoomDetail> {
        diagnostics::room_detail_response(
            &self.manager,
            &self.transport_adapter,
            &self.diagnostics,
            room_id,
        )
        .await
    }

    pub(crate) async fn diagnostics_user_detail(&self, user_id: &str) -> DiagnosticsUserLookup {
        diagnostics::user_detail_response(
            &self.manager,
            &self.transport_adapter,
            &self.diagnostics,
            user_id,
        )
        .await
    }
}

impl RoomHandle {
    pub(crate) fn uuid(&self) -> &str {
        self.room.uuid()
    }

    pub(crate) fn key(&self) -> Option<&str> {
        self.room.key()
    }

    pub(crate) async fn start_recording(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        options: RecordingOptions,
    ) -> bool {
        self.room
            .start_recording_runtime(user_id, connection_id, options)
            .await
    }

    pub(crate) async fn stop_recording(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> bool {
        self.room
            .stop_recording_runtime(user_id, connection_id)
            .await
    }
}

impl From<RoomManagerJoinError> for JoinRoomUserError {
    fn from(error: RoomManagerJoinError) -> Self {
        match error {
            RoomManagerJoinError::MissingRoom => Self::MissingRoom,
            RoomManagerJoinError::RoomFull => Self::RoomFull,
            RoomManagerJoinError::RouterState => Self::RouterState,
        }
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
