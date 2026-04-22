//! Serialized diagnostics response types and small format helpers.
//!
//!structs and enums shape returned by the diagnostics tools

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::runtime::rtc_adapter::TransportSessionHealth;
use o_sfu_protocol::shared::{RecordingState, SessionId, SessionInfo, StreamType};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DiagnosticsTransportHealth {
    Connected,
    Disconnected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DiagnosticsMediaKind {
    Audio,
    Video,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DiagnosticsRouteState {
    Active,
    Inactive,
    Pending,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiagnosticsIncomingBitrate {
    #[serde(rename = "audioBps")]
    pub(crate) audio: u64,
    #[serde(rename = "cameraBps")]
    pub(crate) camera: u64,
    #[serde(rename = "screenBps")]
    pub(crate) screen: u64,
    #[serde(rename = "totalBps")]
    pub(crate) total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiagnosticsQualitySummary {
    pub(crate) current_incoming_bitrate: DiagnosticsIncomingBitrate,
    pub(crate) sampled_metrics_available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiagnosticsSessionTransport {
    pub(crate) connection_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) health: Option<DiagnosticsTransportHealth>,
    pub(crate) media_worker_id: usize,
    pub(crate) quality_summary: DiagnosticsQualitySummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiagnosticsPublication {
    pub(crate) active: bool,
    pub(crate) media_kind: DiagnosticsMediaKind,
    pub(crate) stream_type: StreamType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) transport_media_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiagnosticsSubscription {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) consumer_transport_media_id: Option<u64>,
    pub(crate) producer_session_id: SessionId,
    pub(crate) state: DiagnosticsRouteState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) source_transport_media_id: Option<u64>,
    pub(crate) stream_type: StreamType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiagnosticsSessionView {
    pub(crate) publications: Vec<DiagnosticsPublication>,
    pub(crate) session_id: SessionId,
    pub(crate) session_info: SessionInfo,
    pub(crate) subscriptions: Vec<DiagnosticsSubscription>,
    pub(crate) transport: DiagnosticsSessionTransport,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiagnosticsTransportCounts {
    #[serde(rename = "connectedSessions")]
    pub(crate) connected: usize,
    #[serde(rename = "disconnectedSessions")]
    pub(crate) disconnected: usize,
    #[serde(rename = "totalSessions")]
    pub(crate) total: usize,
    #[serde(rename = "unknownSessions")]
    pub(crate) unknown: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiagnosticsChannelSummary {
    pub(crate) create_date: String,
    pub(crate) media_worker_id: usize,
    pub(crate) publication_count: usize,
    pub(crate) recording_state: RecordingState,
    pub(crate) remote_address: String,
    pub(crate) session_count: usize,
    pub(crate) subscription_count: usize,
    pub(crate) transport: DiagnosticsTransportCounts,
    pub(crate) uuid: String,
    pub(crate) web_rtc_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiagnosticsChannelDetail {
    pub(crate) recent_events: Vec<DiagnosticsEvent>,
    pub(crate) sessions: Vec<DiagnosticsSessionView>,
    pub(crate) summary: DiagnosticsChannelSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiagnosticsSessionDetail {
    pub(crate) channel_uuid: String,
    pub(crate) recent_events: Vec<DiagnosticsEvent>,
    pub(crate) recording_state: RecordingState,
    pub(crate) session: DiagnosticsSessionView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiagnosticsSummaryResponse {
    pub(crate) channels_active: usize,
    pub(crate) publications_active: usize,
    pub(crate) recent_events: Vec<DiagnosticsEvent>,
    pub(crate) recording_channels_active: usize,
    pub(crate) sessions_active: usize,
    pub(crate) subscriptions_active: usize,
    pub(crate) transport: DiagnosticsTransportCounts,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiagnosticsSessionLookupConflict {
    pub(crate) matching_channel_uuids: Vec<String>,
    pub(crate) requested_session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiagnosticsEvent {
    pub(crate) channel_uuid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) connection_id: Option<u64>,
    pub(crate) event: String,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub(crate) fields: Map<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) media_worker_id: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) session_id: Option<SessionId>,
    pub(crate) timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) transport_media_id: Option<u64>,
}

#[derive(Debug)]
pub(crate) enum DiagnosticsSessionLookup {
    Missing,
    Found(DiagnosticsSessionDetail),
    Conflict(DiagnosticsSessionLookupConflict),
}

impl From<TransportSessionHealth> for DiagnosticsTransportHealth {
    fn from(value: TransportSessionHealth) -> Self {
        match value {
            TransportSessionHealth::Connected => Self::Connected,
            TransportSessionHealth::Disconnected => Self::Disconnected,
        }
    }
}

impl From<o_sfu_router::MediaKind> for DiagnosticsMediaKind {
    fn from(value: o_sfu_router::MediaKind) -> Self {
        match value {
            o_sfu_router::MediaKind::Audio => Self::Audio,
            o_sfu_router::MediaKind::Video => Self::Video,
        }
    }
}

pub(crate) fn health_json_value(health: TransportSessionHealth) -> Value {
    json!(DiagnosticsTransportHealth::from(health))
}

pub(crate) fn maybe_health_json_value(health: Option<TransportSessionHealth>) -> Value {
    health.map_or(Value::Null, health_json_value)
}
