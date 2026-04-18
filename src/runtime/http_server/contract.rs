use serde::{Deserialize, Serialize};

pub const API_VERSION: u16 = 1;
pub const METRICS_PATH: &str = "/metrics";
pub const NOOP_PATH: &str = "/v1/noop";
pub const STATS_PATH: &str = "/v1/stats";
pub const CHANNEL_PATH: &str = "/v1/channel";
pub const DISCONNECT_PATH: &str = "/v1/disconnect";

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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateChannelQuery {
    #[serde(rename = "webRTC", skip_serializing_if = "Option::is_none")]
    pub web_rtc: Option<bool>,
    #[serde(rename = "recordingAddress", skip_serializing_if = "Option::is_none")]
    pub recording_address: Option<String>,
}

impl CreateChannelQuery {
    #[must_use]
    pub fn web_rtc_enabled(&self) -> bool {
        self.web_rtc.unwrap_or(true)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelResponse {
    pub uuid: String,
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IncomingBitRateStats {
    pub total: u64,
    pub screen: u64,
    pub audio: u64,
    pub camera: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionsStats {
    pub incoming_bit_rate: IncomingBitRateStats,
    pub count: u64,
    pub camera_count: u64,
    pub screen_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelStats {
    pub create_date: String,
    pub uuid: String,
    pub remote_address: String,
    pub sessions_stats: SessionsStats,
    pub web_rtc_enabled: bool,
}

pub type StatsResponse = Vec<ChannelStats>;

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        API_VERSION, ChannelResponse, ChannelStats, CreateChannelQuery, IncomingBitRateStats,
        NoopResponse, SessionsStats, StatsResponse,
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

        assert_eq!(API_VERSION, 1);

        Ok(())
    }
}
