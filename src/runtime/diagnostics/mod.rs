use std::collections::{BTreeMap, VecDeque};
use std::sync::{Mutex, PoisonError};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::runtime::ChannelRuntimeId;
use crate::runtime::channel::{ChannelManager, RuntimeChannelDirectorySnapshot};
use crate::runtime::rtc_adapter::TransportSessionHealth;
use crate::runtime::transport_adapter::RuntimeTransportAdapter;
use o_sfu_protocol::shared::{RecordingState, SessionId, SessionInfo, StreamType};

const GLOBAL_RECENT_EVENT_LIMIT: usize = 64;
const SCOPE_RECENT_EVENT_LIMIT: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SessionScopeKey {
    channel_uuid: String,
    session_id: SessionId,
}

#[derive(Debug, Default)]
struct DiagnosticsStoreState {
    channel_uuid_by_runtime_id: BTreeMap<ChannelRuntimeId, String>,
    global_recent_events: VecDeque<DiagnosticsEvent>,
    channel_recent_events: BTreeMap<String, VecDeque<DiagnosticsEvent>>,
    session_recent_events: BTreeMap<SessionScopeKey, VecDeque<DiagnosticsEvent>>,
}

#[derive(Debug, Default)]
pub(crate) struct DiagnosticsStore {
    state: Mutex<DiagnosticsStoreState>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DiagnosticsEventData {
    fields: Map<String, Value>,
    pub(crate) channel_uuid: String,
    pub(crate) connection_id: Option<u64>,
    pub(crate) event: &'static str,
    pub(crate) media_worker_id: Option<usize>,
    pub(crate) session_id: Option<SessionId>,
    pub(crate) transport_media_id: Option<u64>,
}

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone)]
struct DiagnosticsChannelSnapshot {
    detail: DiagnosticsChannelDetail,
}

#[derive(Debug)]
pub(crate) enum DiagnosticsSessionLookup {
    Missing,
    Found(DiagnosticsSessionDetail),
    Conflict(DiagnosticsSessionLookupConflict),
}

impl DiagnosticsEventData {
    #[must_use]
    pub(crate) fn for_channel(channel_uuid: &str, event: &'static str) -> Self {
        Self {
            channel_uuid: channel_uuid.to_owned(),
            event,
            ..Self::default()
        }
    }

    #[must_use]
    pub(crate) fn for_session(
        channel_uuid: &str,
        session_id: &SessionId,
        event: &'static str,
    ) -> Self {
        Self {
            channel_uuid: channel_uuid.to_owned(),
            event,
            session_id: Some(session_id.clone()),
            ..Self::default()
        }
    }

    #[must_use]
    pub(crate) fn with_connection_id(mut self, connection_id: u64) -> Self {
        self.connection_id = Some(connection_id);
        self
    }

    #[must_use]
    pub(crate) fn with_media_worker_id(mut self, media_worker_id: usize) -> Self {
        self.media_worker_id = Some(media_worker_id);
        self
    }

    #[must_use]
    pub(crate) fn with_transport_media_id(mut self, transport_media_id: u64) -> Self {
        self.transport_media_id = Some(transport_media_id);
        self
    }

    pub(crate) fn insert_field(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.fields.insert(key.to_owned(), value.into());
        self
    }

    pub(crate) fn insert_fields(mut self, fields: Map<String, Value>) -> Self {
        self.fields.extend(fields);
        self
    }
}

impl DiagnosticsStore {
    pub(crate) fn register_channel_runtime(
        &self,
        channel_runtime_id: ChannelRuntimeId,
        channel_uuid: &str,
    ) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .channel_uuid_by_runtime_id
            .insert(channel_runtime_id, channel_uuid.to_owned());
    }

    pub(crate) fn record(&self, data: DiagnosticsEventData) {
        let event = DiagnosticsEvent {
            channel_uuid: data.channel_uuid.clone(),
            connection_id: data.connection_id,
            event: data.event.to_owned(),
            fields: data.fields,
            media_worker_id: data.media_worker_id,
            session_id: data.session_id.clone(),
            timestamp: diagnostics_timestamp_now(),
            transport_media_id: data.transport_media_id,
        };
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        push_bounded_event(
            &mut state.global_recent_events,
            event.clone(),
            GLOBAL_RECENT_EVENT_LIMIT,
        );
        push_bounded_event(
            state
                .channel_recent_events
                .entry(data.channel_uuid)
                .or_default(),
            event.clone(),
            SCOPE_RECENT_EVENT_LIMIT,
        );
        if let Some(session_id) = data.session_id {
            push_bounded_event(
                state
                    .session_recent_events
                    .entry(SessionScopeKey {
                        channel_uuid: event.channel_uuid.clone(),
                        session_id,
                    })
                    .or_default(),
                event,
                SCOPE_RECENT_EVENT_LIMIT,
            );
        }
    }

    pub(crate) fn forget_channel(&self, channel_uuid: &str) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .channel_uuid_by_runtime_id
            .retain(|_, known_channel_uuid| known_channel_uuid != channel_uuid);
        state.channel_recent_events.remove(channel_uuid);
        state
            .session_recent_events
            .retain(|scope, _| scope.channel_uuid != channel_uuid);
    }

    pub(crate) fn forget_session(&self, channel_uuid: &str, session_id: &SessionId) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.session_recent_events.remove(&SessionScopeKey {
            channel_uuid: channel_uuid.to_owned(),
            session_id: session_id.clone(),
        });
    }

    pub(crate) fn global_recent_events(&self) -> Vec<DiagnosticsEvent> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        reversed_events(&state.global_recent_events)
    }

    pub(crate) fn channel_recent_events(&self, channel_uuid: &str) -> Vec<DiagnosticsEvent> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .channel_recent_events
            .get(channel_uuid)
            .map_or_else(Vec::new, reversed_events)
    }

    pub(crate) fn session_recent_events(
        &self,
        channel_uuid: &str,
        session_id: &SessionId,
    ) -> Vec<DiagnosticsEvent> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .session_recent_events
            .get(&SessionScopeKey {
                channel_uuid: channel_uuid.to_owned(),
                session_id: session_id.clone(),
            })
            .map_or_else(Vec::new, reversed_events)
    }

    pub(crate) fn record_transport_session_event(
        &self,
        channel_runtime_id: ChannelRuntimeId,
        session_id: &SessionId,
        event: &'static str,
        media_worker_id: usize,
        fields: Map<String, Value>,
    ) {
        let channel_uuid = {
            let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state
                .channel_uuid_by_runtime_id
                .get(&channel_runtime_id)
                .cloned()
        };
        let Some(channel_uuid) = channel_uuid else {
            return;
        };
        self.record(
            DiagnosticsEventData::for_session(&channel_uuid, session_id, event)
                .with_media_worker_id(media_worker_id)
                .insert_fields(fields),
        );
    }
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

pub(crate) async fn summary_response(
    channels: &ChannelManager,
    transport_adapter: &RuntimeTransportAdapter,
    diagnostics: &DiagnosticsStore,
) -> DiagnosticsSummaryResponse {
    let channel_snapshots = channel_snapshots(channels, transport_adapter, diagnostics).await;
    let mut transport = DiagnosticsTransportCounts {
        connected: 0,
        disconnected: 0,
        total: 0,
        unknown: 0,
    };
    let mut recording_channels_active = 0_usize;
    let mut publications_active = 0_usize;
    let mut sessions_active = 0_usize;
    let mut subscriptions_active = 0_usize;
    for snapshot in &channel_snapshots {
        let summary = &snapshot.detail.summary;
        sessions_active = sessions_active.saturating_add(summary.session_count);
        publications_active = publications_active.saturating_add(summary.publication_count);
        subscriptions_active = subscriptions_active.saturating_add(summary.subscription_count);
        if summary.recording_state.recording == Some(true) {
            recording_channels_active = recording_channels_active.saturating_add(1);
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
        channels_active: channel_snapshots.len(),
        publications_active,
        recent_events: diagnostics.global_recent_events(),
        recording_channels_active,
        sessions_active,
        subscriptions_active,
        transport,
    }
}

pub(crate) async fn channels_response(
    channels: &ChannelManager,
    transport_adapter: &RuntimeTransportAdapter,
    diagnostics: &DiagnosticsStore,
) -> Vec<DiagnosticsChannelSummary> {
    channel_snapshots(channels, transport_adapter, diagnostics)
        .await
        .into_iter()
        .map(|snapshot| snapshot.detail.summary)
        .collect()
}

pub(crate) async fn channel_detail_response(
    channels: &ChannelManager,
    transport_adapter: &RuntimeTransportAdapter,
    diagnostics: &DiagnosticsStore,
    channel_uuid: &str,
) -> Option<DiagnosticsChannelDetail> {
    let entry = channels.directory_snapshot(channel_uuid).await?;
    Some(
        channel_snapshot(&entry, transport_adapter, diagnostics)
            .await
            .detail,
    )
}

pub(crate) async fn session_detail_response(
    channels: &ChannelManager,
    transport_adapter: &RuntimeTransportAdapter,
    diagnostics: &DiagnosticsStore,
    requested_session_id: &str,
) -> DiagnosticsSessionLookup {
    let mut matches = Vec::new();
    for entry in channels.directory_snapshots().await {
        let Some((session_view, session_id)) = entry
            .channel()
            .diagnostics_matching_session(requested_session_id, transport_adapter)
            .await
        else {
            continue;
        };
        matches.push(DiagnosticsSessionDetail {
            channel_uuid: entry.channel().uuid().to_owned(),
            recent_events: diagnostics.session_recent_events(entry.channel().uuid(), &session_id),
            recording_state: entry.channel().recording_state().await,
            session: session_view,
        });
    }
    match matches.len() {
        0 => DiagnosticsSessionLookup::Missing,
        1 => DiagnosticsSessionLookup::Found(matches.remove(0)),
        _ => DiagnosticsSessionLookup::Conflict(DiagnosticsSessionLookupConflict {
            matching_channel_uuids: matches
                .into_iter()
                .map(|detail| detail.channel_uuid)
                .collect(),
            requested_session_id: requested_session_id.to_owned(),
        }),
    }
}

fn diagnostics_timestamp_now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| String::from("1970-01-01T00:00:00Z"))
}

fn push_bounded_event(
    events: &mut VecDeque<DiagnosticsEvent>,
    event: DiagnosticsEvent,
    limit: usize,
) {
    if events.len() >= limit {
        let _ = events.pop_front();
    }
    events.push_back(event);
}

fn reversed_events(events: &VecDeque<DiagnosticsEvent>) -> Vec<DiagnosticsEvent> {
    events.iter().rev().cloned().collect()
}

async fn channel_snapshots(
    channels: &ChannelManager,
    transport_adapter: &RuntimeTransportAdapter,
    diagnostics: &DiagnosticsStore,
) -> Vec<DiagnosticsChannelSnapshot> {
    let entries = channels.directory_snapshots().await;
    let mut snapshots = Vec::with_capacity(entries.len());
    for entry in entries {
        snapshots.push(channel_snapshot(&entry, transport_adapter, diagnostics).await);
    }
    snapshots
}

async fn channel_snapshot(
    entry: &RuntimeChannelDirectorySnapshot,
    transport_adapter: &RuntimeTransportAdapter,
    diagnostics: &DiagnosticsStore,
) -> DiagnosticsChannelSnapshot {
    let sessions = entry
        .channel()
        .diagnostics_session_views(transport_adapter)
        .await;
    let transport = transport_counts(&sessions);
    let publication_count = sessions
        .iter()
        .map(|session| session.publications.len())
        .sum();
    let subscription_count = sessions
        .iter()
        .map(|session| session.subscriptions.len())
        .sum();
    DiagnosticsChannelSnapshot {
        detail: DiagnosticsChannelDetail {
            recent_events: diagnostics.channel_recent_events(entry.channel().uuid()),
            sessions: sessions.clone(),
            summary: DiagnosticsChannelSummary {
                create_date: entry.create_date().to_owned(),
                media_worker_id: entry.channel().media_worker_id(),
                publication_count,
                recording_state: entry.channel().recording_state().await,
                remote_address: entry.remote_address().to_owned(),
                session_count: sessions.len(),
                subscription_count,
                transport,
                uuid: entry.channel().uuid().to_owned(),
                web_rtc_enabled: entry.channel().web_rtc_enabled(),
            },
        },
    }
}

fn transport_counts(sessions: &[DiagnosticsSessionView]) -> DiagnosticsTransportCounts {
    let mut connected = 0_usize;
    let mut disconnected = 0_usize;
    let mut unknown = 0_usize;
    for session in sessions {
        match session.transport.health {
            Some(DiagnosticsTransportHealth::Connected) => {
                connected = connected.saturating_add(1);
            }
            Some(DiagnosticsTransportHealth::Disconnected) => {
                disconnected = disconnected.saturating_add(1);
            }
            None => {
                unknown = unknown.saturating_add(1);
            }
        }
    }
    DiagnosticsTransportCounts {
        connected,
        disconnected,
        total: sessions.len(),
        unknown,
    }
}

pub(crate) fn health_json_value(health: TransportSessionHealth) -> Value {
    json!(DiagnosticsTransportHealth::from(health))
}

pub(crate) fn maybe_health_json_value(health: Option<TransportSessionHealth>) -> Value {
    health.map_or(Value::Null, health_json_value)
}
