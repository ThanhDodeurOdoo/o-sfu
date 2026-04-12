use std::collections::BTreeMap;
use std::sync::Arc;

use o_sfu_router::RouterId;
use tokio::sync::{Mutex, RwLock, mpsc};

use super::{
    Channel, ChannelConfig, ChannelJoinError, ChannelManagerJoinError, ChannelSessionStatsSnapshot,
    SessionOutbound,
};
use crate::runtime::recording::MediaTap;
use crate::runtime::transport_adapter::RuntimeTransportAdapter;
use crate::signaling::shared::{SessionId, SessionPermissions};
use crate::utils::rfc3339_now;

const UNKNOWN_REMOTE_ADDRESS: &str = "unknown";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeChannelStatsSnapshot {
    pub(crate) create_date: String,
    pub(crate) uuid: String,
    pub(crate) remote_address: String,
    pub(crate) sessions_stats: ChannelSessionStatsSnapshot,
    pub(crate) web_rtc_enabled: bool,
}

pub(crate) struct JoinSessionRequest {
    pub(crate) session_id: SessionId,
    pub(crate) label: Option<String>,
    pub(crate) permissions: SessionPermissions,
    pub(crate) sender: mpsc::UnboundedSender<SessionOutbound>,
    pub(crate) max_sessions: usize,
}

#[derive(Debug)]
pub struct ChannelManager {
    state: RwLock<ChannelManagerState>,
    media_worker_count: usize,
    recording_media_tap: Arc<MediaTap>,
}

#[derive(Debug, Default)]
struct ChannelManagerState {
    channels_by_uuid: BTreeMap<String, ChannelEntry>,
    next_channel_runtime_id: u64,
    next_router_id: u64,
    uuids_by_issuer: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct ChannelEntry {
    channel: Arc<Channel>,
    op_lock: Arc<Mutex<()>>,
    create_date: String,
    remote_address: String,
}

impl ChannelManager {
    #[cfg(test)]
    #[must_use]
    pub fn new() -> Self {
        Self::with_media_workers(1)
    }

    #[must_use]
    pub fn with_media_workers(media_worker_count: usize) -> Self {
        Self::with_media_workers_and_recording_tap(
            media_worker_count,
            Arc::new(MediaTap::default()),
        )
    }

    #[must_use]
    pub fn with_media_workers_and_recording_tap(
        media_worker_count: usize,
        recording_media_tap: Arc<MediaTap>,
    ) -> Self {
        Self {
            state: RwLock::new(ChannelManagerState::default()),
            media_worker_count: media_worker_count.max(1),
            recording_media_tap,
        }
    }

    /// Create a channel for the given issuer, or return the existing one.
    /// Channel creation is idempotent: repeated calls with the same issuer
    /// return the same channel regardless of key, config, or remote-address differences
    /// similar to odoo's sfu
    ///
    /// `remote_address` is observability metadata used for stats like traceability, lggging,...
    pub async fn create_or_get(
        &self,
        issuer: &str,
        key: Option<&str>,
        config: &ChannelConfig,
        remote_address: Option<&str>,
    ) -> Arc<Channel> {
        {
            let state = self.state.read().await;
            if let Some(uuid) = state.uuids_by_issuer.get(issuer)
                && let Some(entry) = state.channels_by_uuid.get(uuid)
            {
                return Arc::clone(&entry.channel);
            }
        }
        let mut state = self.state.write().await;
        if let Some(uuid) = state.uuids_by_issuer.get(issuer)
            && let Some(entry) = state.channels_by_uuid.get(uuid)
        {
            return Arc::clone(&entry.channel);
        }
        let channel_runtime_id = state.next_channel_runtime_id;
        state.next_channel_runtime_id = state.next_channel_runtime_id.saturating_add(1);
        let media_worker_id = self.media_worker_id_for_channel_runtime(channel_runtime_id);
        let router_id = RouterId(state.next_router_id);
        state.next_router_id = state.next_router_id.saturating_add(1);
        let channel = Arc::new(Channel::new(
            channel_runtime_id,
            media_worker_id,
            router_id,
            issuer.to_owned(),
            key.map(str::to_owned),
            config.clone(),
            Arc::clone(&self.recording_media_tap),
        ));
        let channel_uuid = channel.uuid().to_owned();
        state
            .uuids_by_issuer
            .insert(issuer.to_owned(), channel_uuid.clone());
        state.channels_by_uuid.insert(
            channel_uuid,
            ChannelEntry {
                channel: Arc::clone(&channel),
                op_lock: Arc::new(Mutex::new(())),
                create_date: rfc3339_now(),
                remote_address: remote_address.unwrap_or(UNKNOWN_REMOTE_ADDRESS).to_owned(),
            },
        );
        channel
    }

    pub async fn get_by_uuid(&self, uuid: &str) -> Option<Arc<Channel>> {
        let state = self.state.read().await;
        state
            .channels_by_uuid
            .get(uuid)
            .map(|entry| Arc::clone(&entry.channel))
    }

    #[cfg(test)]
    pub async fn has_session(&self, channel_uuid: &str, session_id: &SessionId) -> bool {
        let Some(entry) = self.entry(channel_uuid).await else {
            return false;
        };
        entry.channel.has_session(session_id).await
    }

    pub async fn stats_snapshots(
        &self,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Vec<RuntimeChannelStatsSnapshot> {
        let entries = {
            let state = self.state.read().await;
            state.channels_by_uuid.values().cloned().collect::<Vec<_>>()
        };
        let mut snapshots = Vec::with_capacity(entries.len());
        for entry in entries {
            snapshots.push(self.entry_stats_snapshot(entry, transport_adapter).await);
        }
        snapshots
    }

    pub async fn join_session(
        &self,
        channel_uuid: &str,
        request: JoinSessionRequest,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Result<(Arc<Channel>, u64), ChannelManagerJoinError> {
        let Some(entry) = self.entry(channel_uuid).await else {
            return Err(ChannelManagerJoinError::MissingChannel);
        };
        let _op_guard = entry.op_lock.lock().await;
        if !self.is_current_entry(channel_uuid, &entry.channel).await {
            return Err(ChannelManagerJoinError::MissingChannel);
        }
        let connection_id = entry
            .channel
            .join_session_runtime(
                request.session_id,
                request.label,
                request.permissions,
                request.sender,
                request.max_sessions,
                transport_adapter,
            )
            .await
            .map_err(|error| match error {
                ChannelJoinError::ChannelFull => ChannelManagerJoinError::ChannelFull,
                ChannelJoinError::RouterState => ChannelManagerJoinError::RouterState,
            })?;
        Ok((Arc::clone(&entry.channel), connection_id))
    }

    pub async fn leave_session(
        &self,
        channel_uuid: &str,
        session_id: &SessionId,
        connection_id: u64,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        let Some(entry) = self.entry(channel_uuid).await else {
            return false;
        };
        let _op_guard = entry.op_lock.lock().await;
        if !self.is_current_entry(channel_uuid, &entry.channel).await {
            return false;
        }
        let did_remove_active_session = entry
            .channel
            .leave_session_runtime(session_id, connection_id, transport_adapter)
            .await;
        if did_remove_active_session && entry.channel.is_empty().await {
            self.remove_entry_if_current(channel_uuid, &entry.channel)
                .await;
        }
        did_remove_active_session
    }

    pub async fn disconnect_sessions(
        &self,
        channel_uuid: &str,
        session_ids: &[SessionId],
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let Some(entry) = self.entry(channel_uuid).await else {
            return;
        };
        let _op_guard = entry.op_lock.lock().await;
        if !self.is_current_entry(channel_uuid, &entry.channel).await {
            return;
        }
        entry
            .channel
            .disconnect_sessions_runtime(session_ids, transport_adapter)
            .await;
        if entry.channel.is_empty().await {
            self.remove_entry_if_current(channel_uuid, &entry.channel)
                .await;
        }
    }

    async fn entry(&self, channel_uuid: &str) -> Option<ChannelEntry> {
        let state = self.state.read().await;
        state.channels_by_uuid.get(channel_uuid).cloned()
    }

    async fn entry_stats_snapshot(
        &self,
        entry: ChannelEntry,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> RuntimeChannelStatsSnapshot {
        let sessions_stats = entry
            .channel
            .session_stats_snapshot(transport_adapter)
            .await;
        RuntimeChannelStatsSnapshot {
            create_date: entry.create_date,
            uuid: entry.channel.uuid().to_owned(),
            remote_address: entry.remote_address,
            sessions_stats,
            web_rtc_enabled: entry.channel.web_rtc_enabled(),
        }
    }

    async fn is_current_entry(&self, channel_uuid: &str, channel: &Arc<Channel>) -> bool {
        let state = self.state.read().await;
        state
            .channels_by_uuid
            .get(channel_uuid)
            .is_some_and(|entry| Arc::ptr_eq(&entry.channel, channel))
    }

    async fn remove_entry_if_current(&self, channel_uuid: &str, channel: &Arc<Channel>) {
        let mut state = self.state.write().await;
        let Some(entry) = state.channels_by_uuid.get(channel_uuid) else {
            return;
        };
        if !Arc::ptr_eq(&entry.channel, channel) {
            return;
        }
        state.channels_by_uuid.remove(channel_uuid);
        state.uuids_by_issuer.remove(channel.issuer());
    }

    fn media_worker_id_for_channel_runtime(&self, channel_runtime_id: u64) -> usize {
        let media_worker_count_u64 = u64::try_from(self.media_worker_count).unwrap_or(1);
        usize::try_from(channel_runtime_id % media_worker_count_u64).unwrap_or(0)
    }
}

impl Default for ChannelManager {
    fn default() -> Self {
        Self::with_media_workers(1)
    }
}
