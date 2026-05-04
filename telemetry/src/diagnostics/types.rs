//! Serialized diagnostics response types and small format helpers.
//!
//!structs and enums shape returned by the diagnostics tools

use std::collections::BTreeMap;

use o_sfu_model::{RecordingState, UserId, UserInfo};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticsTransportHealth {
    Connected,
    Disconnected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticsMediaKind {
    Audio,
    Video,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticsRouteState {
    Active,
    Inactive,
    Pending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticsSourceSelector {
    Open,
    Encoding,
    OperatingPoint,
    RoomPolicyPinned,
    RoomPolicyFeatured,
    RoomPolicyReadableDetail,
    RoomPolicyActiveSpeaker,
    RoomPolicyVisibleThumbnail,
    RoomPolicyHidden,
    RoomPolicyOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticsSourceSelectionReason {
    Open,
    ReceiverAdaptation,
    RoomPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticsPolicyPauseReason {
    BudgetPressure,
    HiddenTile,
    OverflowTile,
    MissingUsableLayer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DiagnosticsOverBudgetExceptionReason {
    ProtectedRoute,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticsTemporalLayerMetadata {
    Absent,
    Advertised,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticsTemporalLayerSelection {
    NotSelected,
    Selected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticsVideoLayoutRole {
    Pinned,
    Featured,
    ReadableDetail,
    ActiveSpeaker,
    VisibleThumbnail,
    Hidden,
    Overflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticsVideoRoutePriority {
    PinnedOrFeatured,
    ReadableDetail,
    ActiveSpeaker,
    VisibleThumbnail,
    HiddenOrOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticsActiveSpeakerState {
    Active,
    Idle,
    Blocked,
    RecentlyExpired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticsActiveSpeakerReason {
    Vad,
    AudioLevel,
    AudioLevelWarmup,
    VadFalse,
    LowNoise,
    BelowSpeechThreshold,
    MissingAudioMetadata,
    Expired,
    NoMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticsActiveSpeaker {
    pub state: DiagnosticsActiveSpeakerState,
    pub reason: DiagnosticsActiveSpeakerReason,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_audio_level_dbov: Option<i8>,
    pub confidence_observations: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hold_remaining_ms: Option<u64>,
}

impl DiagnosticsActiveSpeaker {
    #[must_use]
    pub const fn idle() -> Self {
        Self {
            state: DiagnosticsActiveSpeakerState::Idle,
            reason: DiagnosticsActiveSpeakerReason::NoMetadata,
            last_audio_level_dbov: None,
            confidence_observations: 0,
            hold_remaining_ms: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticsIncomingBitrate {
    #[serde(default)]
    pub by_stream_bps: BTreeMap<String, u64>,
    #[serde(rename = "totalBps")]
    pub total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticsQualitySummary {
    pub current_incoming_bitrate: DiagnosticsIncomingBitrate,
    pub sampled_metrics_available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticsUserTransport {
    pub connection_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health: Option<DiagnosticsTransportHealth>,
    pub media_worker_id: usize,
    pub quality_summary: DiagnosticsQualitySummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticsPublication {
    pub active: bool,
    pub encoding_ids: Vec<u64>,
    pub media_kind: DiagnosticsMediaKind,
    pub source_id: u64,
    pub stream_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport_media_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticsSourceEncoding {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    pub encoding_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bitrate_bps: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolution_scale: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_framerate: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_temporal_layer_id: Option<u8>,
    pub temporal_layer_metadata: DiagnosticsTemporalLayerMetadata,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_type: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_ssrc: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repair_ssrc: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rid: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticsSource {
    pub active: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_speaker: Option<DiagnosticsActiveSpeaker>,
    pub current_incoming_bitrate_bps: u64,
    pub encodings: Vec<DiagnosticsSourceEncoding>,
    pub media_kind: DiagnosticsMediaKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mid: Option<String>,
    pub owner_user_id: UserId,
    pub source_id: u64,
    pub stream_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport_media_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticsSourceSelection {
    pub active: bool,
    pub active_video_route_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_receiver_bandwidth_estimate_bps: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub over_budget_exception_reason: Option<DiagnosticsOverBudgetExceptionReason>,
    pub policy_allows_delivery: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_pause_reason: Option<DiagnosticsPolicyPauseReason>,
    pub pressure_observations: u8,
    pub selection_reason: DiagnosticsSourceSelectionReason,
    pub selector: DiagnosticsSourceSelector,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_estimated_bitrate_bps: Option<u64>,
    pub selected_video_bitrate_bps: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_video_budget_bps: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_encoding_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_rid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_temporal_layer_id: Option<u8>,
    pub temporal_layer_selection: DiagnosticsTemporalLayerSelection,
    pub upgrade_observations: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticsSubscription {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_transport_media_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub layout_priority: Option<DiagnosticsVideoRoutePriority>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub layout_role: Option<DiagnosticsVideoLayoutRole>,
    pub producer_user_id: UserId,
    pub selection: DiagnosticsSourceSelection,
    pub source_id: u64,
    pub state: DiagnosticsRouteState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_transport_media_id: Option<u64>,
    pub stream_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticsUserView {
    pub publications: Vec<DiagnosticsPublication>,
    pub user_id: UserId,
    pub user_info: UserInfo,
    pub subscriptions: Vec<DiagnosticsSubscription>,
    pub transport: DiagnosticsUserTransport,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticsTransportCounts {
    #[serde(rename = "connectedUsers")]
    pub connected: usize,
    #[serde(rename = "disconnectedUsers")]
    pub disconnected: usize,
    #[serde(rename = "totalUsers")]
    pub total: usize,
    #[serde(rename = "unknownUsers")]
    pub unknown: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticsRoomSummary {
    pub create_date: String,
    pub media_worker_id: usize,
    pub publication_count: usize,
    pub recording_state: RecordingState,
    pub remote_address: String,
    pub source_count: usize,
    pub user_count: usize,
    pub subscription_count: usize,
    pub transport: DiagnosticsTransportCounts,
    pub uuid: String,
    pub web_rtc_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticsUserSummary {
    pub audio_incoming_bitrate_bps: u64,
    pub camera_incoming_bitrate_bps: u64,
    pub connection_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health: Option<DiagnosticsTransportHealth>,
    pub incoming_bitrate_bps: u64,
    pub media_worker_id: usize,
    pub publication_count: usize,
    pub room_id: String,
    pub screen_incoming_bitrate_bps: u64,
    pub subscription_count: usize,
    pub user_id: UserId,
    pub user_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticsRoomDetail {
    pub recent_events: Vec<DiagnosticsEvent>,
    pub users: Vec<DiagnosticsUserView>,
    pub sources: Vec<DiagnosticsSource>,
    pub summary: DiagnosticsRoomSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticsUserDetail {
    pub room_id: String,
    pub recent_events: Vec<DiagnosticsEvent>,
    pub recording_state: RecordingState,
    pub user: DiagnosticsUserView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticsSummaryResponse {
    pub rooms_active: usize,
    pub publications_active: usize,
    pub recent_events: Vec<DiagnosticsEvent>,
    pub recording_rooms_active: usize,
    pub users_active: usize,
    pub subscriptions_active: usize,
    pub transport: DiagnosticsTransportCounts,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticsUserLookupConflict {
    pub matching_room_ids: Vec<String>,
    pub requested_user_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiagnosticsEvent {
    pub room_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_id: Option<u64>,
    pub event: String,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub fields: Map<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_worker_id: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<UserId>,
    pub timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport_media_id: Option<u64>,
}

#[derive(Debug)]
pub enum DiagnosticsUserLookup {
    Missing,
    Found(DiagnosticsUserDetail),
    Conflict(DiagnosticsUserLookupConflict),
}
