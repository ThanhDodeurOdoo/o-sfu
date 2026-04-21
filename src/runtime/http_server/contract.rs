//! HTTP API Contracts
//!
//! This module defines the paths and JSON payloads for the SFU's HTTP
//! These endpoints are primarily used by the Odoo server to manage channels, disconnect
//! sessions, and get metrics.

use serde::{Deserialize, Serialize};
pub const METRICS_PATH: &str = "/metrics";
pub const NOOP_PATH: &str = "/v1/noop";
pub const STATS_PATH: &str = "/v1/stats";
pub const CHANNEL_PATH: &str = "/v1/channel";
pub const DISCONNECT_PATH: &str = "/v1/disconnect";

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
pub struct CreateChannelQuery {
    /// Whether the channel supports WebRTC features. Defaults to `true`.
    #[serde(rename = "webRTC", skip_serializing_if = "Option::is_none")]
    pub web_rtc: Option<bool>,
    /// Optional webhook address to send recordings to when a recording session finishes.
    #[serde(rename = "recordingAddress", skip_serializing_if = "Option::is_none")]
    pub recording_address: Option<String>,
}

impl CreateChannelQuery {
    #[must_use]
    pub fn web_rtc_enabled(&self) -> bool {
        self.web_rtc.unwrap_or(true)
    }
}

/// Response payload for a successfully created channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelResponse {
    /// The unique identifier allocated for the newly created channel.
    pub uuid: String,
    /// The base URL (e.g., `https://sfu.example.com`) where clients should connect via WebSocket.
    pub url: String,
}

/// Incoming bitrate statistics broken down by media type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IncomingBitRateStats {
    /// Total incoming bitrate across all streams (in bps).
    pub total: u64,
    /// Incoming bitrate from screen sharing streams (in bps).
    pub screen: u64,
    /// Incoming bitrate from audio streams (in bps).
    pub audio: u64,
    /// Incoming bitrate from camera video streams (in bps).
    pub camera: u64,
}

/// Aggregated statistics for all active sessions within a channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionsStats {
    /// Breakdown of incoming bitrates for the channel.
    pub incoming_bit_rate: IncomingBitRateStats,
    /// Total number of connected sessions in this channel.
    pub count: u64,
    /// Number of sessions currently publishing a camera stream.
    pub camera_count: u64,
    /// Number of sessions currently publishing a screen share stream.
    pub screen_count: u64,
}

/// Statistics payload for an individual active channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelStats {
    /// ISO 8601 formatted timestamp of when the channel was created.
    pub create_date: String,
    /// The channel's unique identifier.
    pub uuid: String,
    /// The remote IP address that requested the channel creation.
    pub remote_address: String,
    /// Aggregated session statistics for the channel.
    pub sessions_stats: SessionsStats,
    /// Whether WebRTC is enabled for this channel.
    pub web_rtc_enabled: bool,
}

/// Response payload for the `/v1/stats` endpoint, containing statistics for all active channels.
pub type StatsResponse = Vec<ChannelStats>;

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        ChannelResponse, ChannelStats, CreateChannelQuery, IncomingBitRateStats, NoopResponse,
        SessionsStats, StatsResponse,
    };

    #[test]
    fn route_types_round_trip() -> serde_json::Result<()> {
        let query = CreateChannelQuery {
            web_rtc: Some(false),
            recording_address: Some("https://record.example.com".to_owned()),
        };
        let expected_query = json!({
            "webRTC": false,
            "recordingAddress": "https://record.example.com"
        });
        assert_eq!(serde_json::to_value(&query)?, expected_query);
        assert_eq!(
            serde_json::from_value::<CreateChannelQuery>(expected_query)?,
            query
        );
        assert!(!query.web_rtc_enabled());

        let noop = NoopResponse::ok();
        let expected_noop = json!({ "result": "ok" });
        assert_eq!(serde_json::to_value(&noop)?, expected_noop);
        assert_eq!(serde_json::from_value::<NoopResponse>(expected_noop)?, noop);

        let channel = ChannelResponse {
            uuid: "31dcc5dc-4d26-453e-9bca-ab1f5d268303".to_owned(),
            url: "https://sfu.example.com".to_owned(),
        };
        let expected_channel = json!({
            "uuid": "31dcc5dc-4d26-453e-9bca-ab1f5d268303",
            "url": "https://sfu.example.com"
        });
        assert_eq!(serde_json::to_value(&channel)?, expected_channel);
        assert_eq!(
            serde_json::from_value::<ChannelResponse>(expected_channel)?,
            channel
        );

        let stats: StatsResponse = vec![ChannelStats {
            create_date: "2026-04-02T01:02:03.000Z".to_owned(),
            uuid: "31dcc5dc-4d26-453e-9bca-ab1f5d268303".to_owned(),
            remote_address: "203.0.113.10".to_owned(),
            sessions_stats: SessionsStats {
                incoming_bit_rate: IncomingBitRateStats {
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
