//! Serialized diagnostics response types and small format helpers.
//!
//!structs and enums shape returned by the diagnostics tools

use std::time::Duration;

use o_sfu_protocol::shared::{RecordingState, SessionId, SessionInfo, StreamType};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::runtime::{
    rtc_adapter::TransportSessionHealth,
    source_model::{SourceRoomPolicySelector, SourceRoutePriority, SourceSelector},
    transport_adapter::{
        ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSourceDiagnostic,
    },
};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DiagnosticsSourceSelector {
    Open,
    Encoding,
    OperatingPoint,
    RoomPolicyPinned,
    RoomPolicyFeatured,
    RoomPolicyScreenShare,
    RoomPolicyActiveSpeaker,
    RoomPolicyVisibleThumbnail,
    RoomPolicyHidden,
    RoomPolicyOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DiagnosticsSourceSelectionReason {
    Open,
    ReceiverAdaptation,
    RoomPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DiagnosticsTemporalLayerMetadata {
    Absent,
    Advertised,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DiagnosticsTemporalLayerSelection {
    NotSelected,
    Selected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DiagnosticsVideoLayoutRole {
    Pinned,
    Featured,
    ScreenShare,
    ActiveSpeaker,
    VisibleThumbnail,
    Hidden,
    Overflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DiagnosticsVideoRoutePriority {
    PinnedOrFeatured,
    ScreenShare,
    ActiveSpeaker,
    VisibleThumbnail,
    HiddenOrOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DiagnosticsActiveSpeakerState {
    Active,
    Idle,
    Blocked,
    RecentlyExpired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DiagnosticsActiveSpeakerReason {
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
pub(crate) struct DiagnosticsActiveSpeaker {
    pub(crate) state: DiagnosticsActiveSpeakerState,
    pub(crate) reason: DiagnosticsActiveSpeakerReason,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_audio_level_dbov: Option<i8>,
    pub(crate) confidence_observations: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) hold_remaining_ms: Option<u64>,
}

impl DiagnosticsActiveSpeaker {
    #[must_use]
    pub(crate) const fn idle() -> Self {
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
    pub(crate) encoding_ids: Vec<u64>,
    pub(crate) media_kind: DiagnosticsMediaKind,
    pub(crate) source_id: u64,
    pub(crate) stream_type: StreamType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) transport_media_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiagnosticsSourceEncoding {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) codec: Option<String>,
    pub(crate) encoding_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_bitrate_bps: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_temporal_layer_id: Option<u8>,
    pub(crate) temporal_layer_metadata: DiagnosticsTemporalLayerMetadata,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) payload_type: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) primary_ssrc: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) repair_ssrc: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rid: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiagnosticsSource {
    pub(crate) active: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) active_speaker: Option<DiagnosticsActiveSpeaker>,
    pub(crate) current_incoming_bitrate_bps: u64,
    pub(crate) encodings: Vec<DiagnosticsSourceEncoding>,
    pub(crate) media_kind: DiagnosticsMediaKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) mid: Option<String>,
    pub(crate) owner_session_id: SessionId,
    pub(crate) source_id: u64,
    pub(crate) stream_type: StreamType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) transport_media_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiagnosticsSourceSelection {
    pub(crate) active: bool,
    pub(crate) pressure_observations: u8,
    pub(crate) selection_reason: DiagnosticsSourceSelectionReason,
    pub(crate) selector: DiagnosticsSourceSelector,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) selected_encoding_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) selected_rid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) selected_temporal_layer_id: Option<u8>,
    pub(crate) temporal_layer_selection: DiagnosticsTemporalLayerSelection,
    pub(crate) upgrade_observations: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiagnosticsSubscription {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) consumer_transport_media_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) layout_priority: Option<DiagnosticsVideoRoutePriority>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) layout_role: Option<DiagnosticsVideoLayoutRole>,
    pub(crate) producer_session_id: SessionId,
    pub(crate) selection: DiagnosticsSourceSelection,
    pub(crate) source_id: u64,
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
    pub(crate) sources: Vec<DiagnosticsSource>,
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

impl From<ActiveSpeakerActivityState> for DiagnosticsActiveSpeakerState {
    fn from(state: ActiveSpeakerActivityState) -> Self {
        match state {
            ActiveSpeakerActivityState::Active => Self::Active,
            ActiveSpeakerActivityState::Idle => Self::Idle,
            ActiveSpeakerActivityState::Blocked => Self::Blocked,
            ActiveSpeakerActivityState::RecentlyExpired => Self::RecentlyExpired,
        }
    }
}

impl From<ActiveSpeakerActivityReason> for DiagnosticsActiveSpeakerReason {
    fn from(reason: ActiveSpeakerActivityReason) -> Self {
        match reason {
            ActiveSpeakerActivityReason::Vad => Self::Vad,
            ActiveSpeakerActivityReason::AudioLevel => Self::AudioLevel,
            ActiveSpeakerActivityReason::AudioLevelWarmup => Self::AudioLevelWarmup,
            ActiveSpeakerActivityReason::VadFalse => Self::VadFalse,
            ActiveSpeakerActivityReason::LowNoise => Self::LowNoise,
            ActiveSpeakerActivityReason::BelowSpeechThreshold => Self::BelowSpeechThreshold,
            ActiveSpeakerActivityReason::MissingAudioMetadata => Self::MissingAudioMetadata,
            ActiveSpeakerActivityReason::Expired => Self::Expired,
            ActiveSpeakerActivityReason::NoMetadata => Self::NoMetadata,
        }
    }
}

impl From<ActiveSpeakerSourceDiagnostic> for DiagnosticsActiveSpeaker {
    fn from(diagnostic: ActiveSpeakerSourceDiagnostic) -> Self {
        Self {
            state: diagnostic.state().into(),
            reason: diagnostic.reason().into(),
            last_audio_level_dbov: diagnostic.last_audio_level_dbov(),
            confidence_observations: diagnostic.confidence_observations(),
            hold_remaining_ms: diagnostic.hold_remaining().map(duration_millis),
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

impl From<SourceSelector> for DiagnosticsSourceSelector {
    fn from(value: SourceSelector) -> Self {
        match value {
            SourceSelector::Open => Self::Open,
            SourceSelector::Encoding(_) => Self::Encoding,
            SourceSelector::OperatingPoint(_) => Self::OperatingPoint,
            SourceSelector::RoomPolicy(SourceRoomPolicySelector::Pinned) => Self::RoomPolicyPinned,
            SourceSelector::RoomPolicy(SourceRoomPolicySelector::Featured) => {
                Self::RoomPolicyFeatured
            }
            SourceSelector::RoomPolicy(SourceRoomPolicySelector::ScreenShare) => {
                Self::RoomPolicyScreenShare
            }
            SourceSelector::RoomPolicy(SourceRoomPolicySelector::ActiveSpeaker) => {
                Self::RoomPolicyActiveSpeaker
            }
            SourceSelector::RoomPolicy(SourceRoomPolicySelector::VisibleThumbnail) => {
                Self::RoomPolicyVisibleThumbnail
            }
            SourceSelector::RoomPolicy(SourceRoomPolicySelector::Hidden) => Self::RoomPolicyHidden,
            SourceSelector::RoomPolicy(SourceRoomPolicySelector::Overflow) => {
                Self::RoomPolicyOverflow
            }
        }
    }
}

impl From<SourceSelector> for DiagnosticsSourceSelectionReason {
    fn from(value: SourceSelector) -> Self {
        match value {
            SourceSelector::Open => Self::Open,
            SourceSelector::Encoding(_) | SourceSelector::OperatingPoint(_) => {
                Self::ReceiverAdaptation
            }
            SourceSelector::RoomPolicy(_) => Self::RoomPolicy,
        }
    }
}

impl From<SourceRoomPolicySelector> for DiagnosticsVideoLayoutRole {
    fn from(value: SourceRoomPolicySelector) -> Self {
        match value {
            SourceRoomPolicySelector::Pinned => Self::Pinned,
            SourceRoomPolicySelector::Featured => Self::Featured,
            SourceRoomPolicySelector::ScreenShare => Self::ScreenShare,
            SourceRoomPolicySelector::ActiveSpeaker => Self::ActiveSpeaker,
            SourceRoomPolicySelector::VisibleThumbnail => Self::VisibleThumbnail,
            SourceRoomPolicySelector::Hidden => Self::Hidden,
            SourceRoomPolicySelector::Overflow => Self::Overflow,
        }
    }
}

impl From<SourceRoutePriority> for DiagnosticsVideoRoutePriority {
    fn from(value: SourceRoutePriority) -> Self {
        match value {
            SourceRoutePriority::PinnedOrFeatured => Self::PinnedOrFeatured,
            SourceRoutePriority::ScreenShare => Self::ScreenShare,
            SourceRoutePriority::ActiveSpeaker => Self::ActiveSpeaker,
            SourceRoutePriority::VisibleThumbnail => Self::VisibleThumbnail,
            SourceRoutePriority::HiddenOrOverflow => Self::HiddenOrOverflow,
        }
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub(crate) fn health_json_value(health: TransportSessionHealth) -> Value {
    json!(DiagnosticsTransportHealth::from(health))
}

pub(crate) fn maybe_health_json_value(health: Option<TransportSessionHealth>) -> Value {
    health.map_or(Value::Null, health_json_value)
}
