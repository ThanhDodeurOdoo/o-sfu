//! HTTP API Contracts
//!
//! This module defines the paths and JSON payloads for the SFU's HTTP
//! These endpoints are primarily used by the Odoo server to manage rooms, disconnect
//! users and get metrics.

use serde::{Deserialize, Serialize};
pub const METRICS_PATH: &str = "/metrics";
pub const NOOP_PATH: &str = "/v1/noop";
pub const STATS_PATH: &str = "/v1/stats";
pub const CHANNEL_PATH: &str = "/v1/channel";
pub const DISCONNECT_PATH: &str = "/v1/disconnect";
pub const DIAGNOSTICS_SUMMARY_PATH: &str = "/internal/diagnostics/summary";
pub const DIAGNOSTICS_ROOMS_PATH: &str = "/internal/diagnostics/rooms";
pub const DIAGNOSTICS_WORKERS_PATH: &str = "/internal/diagnostics/workers";

/// Response payload for the `/v1/noop` health-check endpoint.
/// used by the operators (infra team) to check if the SFU is up
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoopResponse {
    /// Always returns "ok"
    pub result: String,
}

impl NoopResponse {
    #[must_use]
    pub fn ok() -> Self {
        Self {
            result: "ok".to_owned(),
        }
    }
}

/// Query parameters for the `/v1/channel` creation endpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateRoomQuery {
    /// Whether the room supports WebRTC features. Defaults to `true`.
    #[serde(rename = "webRTC", skip_serializing_if = "Option::is_none")]
    pub web_rtc: Option<bool>,
    /// Optional compatibility recording address from Odoo.
    ///
    /// The current runtime preserves this field for the room contract but does
    /// not send recording output until persistent recording finalization lands.
    #[serde(rename = "recordingAddress", skip_serializing_if = "Option::is_none")]
    pub recording_address: Option<String>,
}

impl CreateRoomQuery {
    #[must_use]
    pub fn web_rtc_enabled(&self) -> bool {
        self.web_rtc.unwrap_or(true)
    }
}

/// Response payload for a successfully created room.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomResponse {
    /// The unique identifier allocated for the newly created room.
    pub uuid: String,
    /// The base URL (e.g., `https://sfu.example.com`) where clients should connect via WebSocket.
    pub url: String,
}

/// Incoming bitrate statistics broken down by media type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IncomingBitRateStatsResponse {
    /// Total incoming bitrate across all streams (in bps).
    pub total: u64,
    /// Incoming bitrate from screen sharing streams (in bps).
    pub screen: u64,
    /// Incoming bitrate from audio streams (in bps).
    pub audio: u64,
    /// Incoming bitrate from camera video streams (in bps).
    pub camera: u64,
}

/// Aggregated statistics for all active users within a room.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsersStatsResponse {
    /// Breakdown of incoming bitrates for the room.
    pub incoming_bit_rate: IncomingBitRateStatsResponse,
    /// Total number of connected users in this room.
    pub count: u64,
    /// Number of users currently publishing a camera stream.
    pub camera_count: u64,
    /// Number of users currently publishing a screen share stream.
    pub screen_count: u64,
}

/// Statistics payload for an individual active room.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoomStatsResponse {
    /// ISO 8601 formatted timestamp of when the room was created.
    pub create_date: String,
    /// The room's unique identifier.
    pub uuid: String,
    /// The remote IP address that requested the room creation.
    pub remote_address: String,
    /// Aggregated user statistics for the room.
    #[serde(rename = "sessionsStats")]
    pub users_stats: UsersStatsResponse,
    /// Whether WebRTC is enabled for this room.
    pub web_rtc_enabled: bool,
}

/// Response payload for the `/v1/stats` endpoint, containing statistics for all active rooms.
pub type StatsResponse = Vec<RoomStatsResponse>;

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        CreateRoomQuery, IncomingBitRateStatsResponse, NoopResponse, RoomResponse,
        RoomStatsResponse, StatsResponse, UsersStatsResponse,
    };

    #[test]
    fn route_types_round_trip() -> serde_json::Result<()> {
        let query = CreateRoomQuery {
            web_rtc: Some(false),
            recording_address: Some("https://record.example.com".to_owned()),
        };
        let expected_query = json!({
            "webRTC": false,
            "recordingAddress": "https://record.example.com"
        });
        assert_eq!(serde_json::to_value(&query)?, expected_query);
        assert_eq!(
            serde_json::from_value::<CreateRoomQuery>(expected_query)?,
            query
        );
        assert!(!query.web_rtc_enabled());

        let noop = NoopResponse::ok();
        let expected_noop = json!({ "result": "ok" });
        assert_eq!(serde_json::to_value(&noop)?, expected_noop);
        assert_eq!(serde_json::from_value::<NoopResponse>(expected_noop)?, noop);

        let room = RoomResponse {
            uuid: "31dcc5dc-4d26-453e-9bca-ab1f5d268303".to_owned(),
            url: "https://sfu.example.com".to_owned(),
        };
        let expected_room = json!({
            "uuid": "31dcc5dc-4d26-453e-9bca-ab1f5d268303",
            "url": "https://sfu.example.com"
        });
        assert_eq!(serde_json::to_value(&room)?, expected_room);
        assert_eq!(serde_json::from_value::<RoomResponse>(expected_room)?, room);

        let stats: StatsResponse = vec![RoomStatsResponse {
            create_date: "2026-04-02T01:02:03.000Z".to_owned(),
            uuid: "31dcc5dc-4d26-453e-9bca-ab1f5d268303".to_owned(),
            remote_address: "203.0.113.10".to_owned(),
            users_stats: UsersStatsResponse {
                incoming_bit_rate: IncomingBitRateStatsResponse {
                    total: 1200,
                    screen: 400,
                    audio: 300,
                    camera: 500,
                },
                count: 2,
                camera_count: 1,
                screen_count: 1,
            },
            web_rtc_enabled: true,
        }];
        let expected_stats = json!([{
            "createDate": "2026-04-02T01:02:03.000Z",
            "uuid": "31dcc5dc-4d26-453e-9bca-ab1f5d268303",
            "remoteAddress": "203.0.113.10",
            "sessionsStats": {
                "incomingBitRate": {
                    "total": 1200,
                    "screen": 400,
                    "audio": 300,
                    "camera": 500
                },
                "count": 2,
                "cameraCount": 1,
                "screenCount": 1
            },
            "webRtcEnabled": true
        }]);
        assert_eq!(serde_json::to_value(&stats)?, expected_stats);
        assert_eq!(
            serde_json::from_value::<StatsResponse>(expected_stats)?,
            stats
        );
        Ok(())
    }
}
