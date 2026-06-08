//! HTTP control-plane contracts
//!
//! Odoo uses these paths and payloads to create rooms, disconnect users and read
//! runtime stats

use serde::{Deserialize, Serialize};
pub const METRICS_PATH: &str = "/metrics";
pub const NOOP_PATH: &str = "/v1/noop";
pub const STATS_PATH: &str = "/v1/stats";
pub const CHANNEL_PATH: &str = "/v1/channel";
pub const DISCONNECT_PATH: &str = "/v1/disconnect";
pub const DIAGNOSTICS_SUMMARY_PATH: &str = "/internal/diagnostics/summary";
pub const DIAGNOSTICS_ROOMS_PATH: &str = "/internal/diagnostics/rooms";
pub const DIAGNOSTICS_WORKERS_PATH: &str = "/internal/diagnostics/workers";

/// `/v1/noop` response payload
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoopResponse {
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

/// `/v1/channel` query parameters
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateRoomQuery {
    #[serde(rename = "webRTC", skip_serializing_if = "Option::is_none")]
    pub web_rtc: Option<bool>,
    /// compatibility field preserved until persistent recording output lands
    #[serde(rename = "recordingAddress", skip_serializing_if = "Option::is_none")]
    pub recording_address: Option<String>,
}

impl CreateRoomQuery {
    #[must_use]
    pub fn web_rtc_enabled(&self) -> bool {
        self.web_rtc.unwrap_or(true)
    }
}

/// created-room response payload
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomResponse {
    pub uuid: String,
    pub url: String,
}

/// incoming bitrate stats by compatibility stream type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IncomingBitRateStatsResponse {
    pub total: u64,
    pub screen: u64,
    pub audio: u64,
    pub camera: u64,
}

/// active-user stats for one room
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsersStatsResponse {
    pub incoming_bit_rate: IncomingBitRateStatsResponse,
    pub count: u64,
    pub camera_count: u64,
    pub screen_count: u64,
}

/// `/v1/stats` entry for one active room
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoomStatsResponse {
    pub create_date: String,
    pub uuid: String,
    pub remote_address: String,
    #[serde(rename = "sessionsStats")]
    pub users_stats: UsersStatsResponse,
    pub web_rtc_enabled: bool,
}

/// `/v1/stats` response payload
pub type StatsResponse = Vec<RoomStatsResponse>;

#[cfg(test)]
#[path = "TESTS/contract.rs"]
mod tests;
