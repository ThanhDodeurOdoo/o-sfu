use std::collections::BTreeMap;
use std::sync::Arc;

use o_sfu_router::RouterId;
use tokio::sync::{Mutex, RwLock, mpsc};

use super::{Channel, ChannelJoinError, ChannelManagerJoinError, SessionOutbound};
use crate::runtime::transport_adapter::RuntimeTransportAdapter;
use crate::signaling::{
    http::{CreateChannelQuery, StatsResponse},
    shared::{SessionId, SessionPermissions},
};

const UNKNOWN_REMOTE_ADDRESS: &str = "unknown";

/// Manages all active channels with idempotent creation by issuer.
#[derive(Debug, Default)]
pub struct ChannelManager {
    state: RwLock<ChannelManagerState>,
}

#[derive(Debug, Default)]
struct ChannelManagerState {
    channels_by_uuid: BTreeMap<String, ChannelEntry>,
    next_router_id: u64,
    uuids_by_issuer: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct ChannelEntry {
    channel: Arc<Channel>,
    op_lock: Arc<Mutex<()>>,
}

impl ChannelManager {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a channel for the given issuer, or return the existing one.
    /// Channel creation is idempotent: repeated calls with the same issuer
    /// return the same channel regardless of key or query differences.
    pub async fn create_or_get(
        &self,
        issuer: &str,
        key: Option<&str>,
        query: &CreateChannelQuery,
    ) -> Arc<Channel> {
        self.create_or_get_with_remote_address(issuer, key, UNKNOWN_REMOTE_ADDRESS, query)
            .await
    }

    /// Create a channel for the given issuer, preserving the remote address that
    /// originally provisioned it for observability surfaces such as `/v1/stats`.
    pub async fn create_or_get_with_remote_address(
        &self,
        issuer: &str,
        key: Option<&str>,
        remote_address: &str,
        query: &CreateChannelQuery,
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
        let router_id = RouterId(state.next_router_id);
        state.next_router_id = state.next_router_id.saturating_add(1);
        let channel = Arc::new(Channel::new(
            router_id,
            issuer.to_owned(),
            key.map(str::to_owned),
            remote_address.to_owned(),
            query,
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

    pub async fn has_session(&self, channel_uuid: &str, session_id: &SessionId) -> bool {
        let Some(entry) = self.entry(channel_uuid).await else {
            return false;
        };
        entry.channel.has_session(session_id).await
    }

    pub async fn stats(&self, transport_adapter: &RuntimeTransportAdapter) -> StatsResponse {
        let channels = {
            let state = self.state.read().await;
            state
                .channels_by_uuid
                .values()
                .map(|entry| Arc::clone(&entry.channel))
                .collect::<Vec<_>>()
        };
        let mut stats = Vec::with_capacity(channels.len());
        for channel in channels {
            stats.push(channel.stats(transport_adapter).await);
        }
        stats
    }

    pub async fn join_session(
        &self,
        channel_uuid: &str,
        session_id: SessionId,
        label: Option<String>,
        permissions: SessionPermissions,
        sender: mpsc::UnboundedSender<SessionOutbound>,
        max_sessions: usize,
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
            .join_session(session_id, label, permissions, sender, max_sessions)
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
    ) -> bool {
        let Some(entry) = self.entry(channel_uuid).await else {
            return false;
        };
        let _op_guard = entry.op_lock.lock().await;
        if !self.is_current_entry(channel_uuid, &entry.channel).await {
            return false;
        }
        let did_remove_active_session =
            entry.channel.leave_session(session_id, connection_id).await;
        if did_remove_active_session && entry.channel.is_empty().await {
            self.remove_entry_if_current(channel_uuid, &entry.channel)
                .await;
        }
        did_remove_active_session
    }

    pub async fn disconnect_sessions(&self, channel_uuid: &str, session_ids: &[SessionId]) {
        let Some(entry) = self.entry(channel_uuid).await else {
            return;
        };
        let _op_guard = entry.op_lock.lock().await;
        if !self.is_current_entry(channel_uuid, &entry.channel).await {
            return;
        }
        entry.channel.disconnect_sessions(session_ids).await;
        if entry.channel.is_empty().await {
            self.remove_entry_if_current(channel_uuid, &entry.channel)
                .await;
        }
    }

    async fn entry(&self, channel_uuid: &str) -> Option<ChannelEntry> {
        let state = self.state.read().await;
        state.channels_by_uuid.get(channel_uuid).cloned()
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
}
