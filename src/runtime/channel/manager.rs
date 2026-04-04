use std::collections::BTreeMap;
use std::sync::Arc;

use o_sfu_router::RouterId;
use tokio::sync::{RwLock, mpsc};

use super::{Channel, ChannelJoinError, ChannelManagerJoinError, SessionOutbound};
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
    channels_by_uuid: BTreeMap<String, Arc<Channel>>,
    next_router_id: u64,
    uuids_by_issuer: BTreeMap<String, String>,
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
                && let Some(channel) = state.channels_by_uuid.get(uuid)
            {
                return Arc::clone(channel);
            }
        }
        let mut state = self.state.write().await;
        if let Some(uuid) = state.uuids_by_issuer.get(issuer)
            && let Some(channel) = state.channels_by_uuid.get(uuid)
        {
            return Arc::clone(channel);
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
        state
            .channels_by_uuid
            .insert(channel_uuid, Arc::clone(&channel));
        channel
    }

    pub async fn get_by_uuid(&self, uuid: &str) -> Option<Arc<Channel>> {
        let state = self.state.read().await;
        state.channels_by_uuid.get(uuid).map(Arc::clone)
    }

    pub async fn has_session(&self, channel_uuid: &str, session_id: &SessionId) -> bool {
        let channel = {
            let state = self.state.read().await;
            state.channels_by_uuid.get(channel_uuid).map(Arc::clone)
        };
        let Some(channel) = channel else {
            return false;
        };
        channel.has_session(session_id).await
    }

    pub async fn stats(&self) -> StatsResponse {
        let channels = {
            let state = self.state.read().await;
            state
                .channels_by_uuid
                .values()
                .map(Arc::clone)
                .collect::<Vec<_>>()
        };
        let mut stats = Vec::with_capacity(channels.len());
        for channel in channels {
            stats.push(channel.stats().await);
        }
        stats
    }

    #[allow(
        clippy::significant_drop_tightening,
        reason = "join must stay serialized with manager cleanup so an empty channel cannot be removed while a concurrent join is being committed"
    )]
    pub async fn join_session(
        &self,
        channel_uuid: &str,
        session_id: SessionId,
        label: Option<String>,
        permissions: SessionPermissions,
        sender: mpsc::UnboundedSender<SessionOutbound>,
        max_sessions: usize,
    ) -> Result<(Arc<Channel>, u64), ChannelManagerJoinError> {
        let state = self.state.write().await;
        let Some(channel) = state.channels_by_uuid.get(channel_uuid).map(Arc::clone) else {
            return Err(ChannelManagerJoinError::MissingChannel);
        };
        let connection_id = channel
            .join_session(session_id, label, permissions, sender, max_sessions)
            .await
            .map_err(|error| match error {
                ChannelJoinError::ChannelFull => ChannelManagerJoinError::ChannelFull,
                ChannelJoinError::RouterState => ChannelManagerJoinError::RouterState,
            })?;
        Ok((channel, connection_id))
    }

    #[allow(
        clippy::significant_drop_tightening,
        reason = "teardown must keep the manager lock until cleanup decides whether the channel entry should be pruned"
    )]
    pub async fn leave_session(
        &self,
        channel_uuid: &str,
        session_id: &SessionId,
        connection_id: u64,
    ) -> bool {
        let mut state = self.state.write().await;
        let Some(channel) = state.channels_by_uuid.get(channel_uuid).map(Arc::clone) else {
            return false;
        };
        let did_remove_active_session = channel.leave_session(session_id, connection_id).await;
        remove_channel_if_empty(&mut state, &channel).await;
        did_remove_active_session
    }

    #[allow(
        clippy::significant_drop_tightening,
        reason = "bulk disconnect and empty-channel pruning must observe one manager-coordinated critical section"
    )]
    pub async fn disconnect_sessions(&self, channel_uuid: &str, session_ids: &[SessionId]) {
        let mut state = self.state.write().await;
        let Some(channel) = state.channels_by_uuid.get(channel_uuid).map(Arc::clone) else {
            return;
        };
        channel.disconnect_sessions(session_ids).await;
        remove_channel_if_empty(&mut state, &channel).await;
    }
}

async fn remove_channel_if_empty(state: &mut ChannelManagerState, channel: &Channel) {
    if !channel.is_empty().await {
        return;
    }
    state.channels_by_uuid.remove(channel.uuid());
    state.uuids_by_issuer.remove(channel.issuer());
}
