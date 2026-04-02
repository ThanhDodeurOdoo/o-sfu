use std::collections::BTreeMap;

use tokio::sync::RwLock;
use uuid::Uuid;

use crate::signaling::{http::CreateChannelQuery, shared::SessionId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StubChannel {
    pub issuer: String,
    pub key: Option<String>,
    pub uuid: String,
    pub web_rtc_enabled: bool,
    pub recording_address: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StubChannelJoinError {
    MissingChannel,
    ChannelFull,
}

#[derive(Debug, Default)]
struct StubChannelRegistryState {
    channels_by_uuid: BTreeMap<String, StubChannelRecord>,
    uuids_by_issuer: BTreeMap<String, String>,
}

#[derive(Debug)]
struct StubChannelRecord {
    channel: StubChannel,
    active_sessions: BTreeMap<SessionId, usize>,
}

#[derive(Debug, Default)]
pub struct StubChannelRegistry {
    state: RwLock<StubChannelRegistryState>,
}

impl StubChannelRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn create_or_get(
        &self,
        issuer: &str,
        key: Option<&str>,
        query: &CreateChannelQuery,
    ) -> StubChannel {
        {
            let state = self.state.read().await;
            if let Some(uuid) = state.uuids_by_issuer.get(issuer)
                && let Some(record) = state.channels_by_uuid.get(uuid)
            {
                return record.channel.clone();
            }
        }
        let mut state = self.state.write().await;
        if let Some(uuid) = state.uuids_by_issuer.get(issuer)
            && let Some(record) = state.channels_by_uuid.get(uuid)
        {
            return record.channel.clone();
        }
        let channel = StubChannel {
            issuer: issuer.to_owned(),
            key: key.map(str::to_owned),
            uuid: Uuid::new_v4().to_string(),
            web_rtc_enabled: query.web_rtc_enabled(),
            recording_address: query.recording_address.clone(),
        };
        state
            .uuids_by_issuer
            .insert(issuer.to_owned(), channel.uuid.clone());
        state.channels_by_uuid.insert(
            channel.uuid.clone(),
            StubChannelRecord {
                channel: channel.clone(),
                active_sessions: BTreeMap::new(),
            },
        );
        channel
    }

    pub async fn get_by_uuid(&self, uuid: &str) -> Option<StubChannel> {
        let state = self.state.read().await;
        state
            .channels_by_uuid
            .get(uuid)
            .map(|record| record.channel.clone())
    }

    pub async fn join_session(
        &self,
        channel_uuid: &str,
        session_id: &SessionId,
        channel_size: usize,
    ) -> Result<StubChannel, StubChannelJoinError> {
        let mut state = self.state.write().await;
        let Some(record) = state.channels_by_uuid.get_mut(channel_uuid) else {
            return Err(StubChannelJoinError::MissingChannel);
        };
        let is_new_session = !record.active_sessions.contains_key(session_id);
        if is_new_session && record.active_sessions.len() >= channel_size {
            return Err(StubChannelJoinError::ChannelFull);
        }
        record
            .active_sessions
            .entry(session_id.clone())
            .and_modify(|connection_count| *connection_count += 1)
            .or_insert(1);
        let channel = record.channel.clone();
        drop(state);
        Ok(channel)
    }

    pub async fn leave_session(&self, channel_uuid: &str, session_id: &SessionId) {
        let mut state = self.state.write().await;
        let Some(record) = state.channels_by_uuid.get_mut(channel_uuid) else {
            return;
        };
        let Some(connection_count) = record.active_sessions.get_mut(session_id) else {
            return;
        };
        if *connection_count > 1 {
            *connection_count -= 1;
            return;
        }
        record.active_sessions.remove(session_id);
        drop(state);
    }
}

#[cfg(test)]
mod tests {
    use super::{StubChannelJoinError, StubChannelRegistry};
    use crate::signaling::{http::CreateChannelQuery, shared::SessionId};

    #[tokio::test]
    async fn create_or_get_is_idempotent_by_issuer() {
        let registry = StubChannelRegistry::new();
        let query = CreateChannelQuery::default();
        let first = registry.create_or_get("issuer-a", None, &query).await;
        let second = registry
            .create_or_get("issuer-a", Some("ignored"), &query)
            .await;
        let third = registry.create_or_get("issuer-b", None, &query).await;
        assert_eq!(first.uuid, second.uuid);
        assert_ne!(first.uuid, third.uuid);
    }

    #[tokio::test]
    async fn get_by_uuid_returns_created_channel() {
        let registry = StubChannelRegistry::new();
        let channel = registry
            .create_or_get(
                "issuer-a",
                Some("channel-key"),
                &CreateChannelQuery::default(),
            )
            .await;
        let fetched = registry.get_by_uuid(&channel.uuid).await;
        assert_eq!(fetched, Some(channel));
    }

    #[tokio::test]
    async fn join_session_tracks_connections_per_session_id() {
        let registry = StubChannelRegistry::new();
        let channel = registry
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let session_id = SessionId::Integer(7);
        let first = registry.join_session(&channel.uuid, &session_id, 1).await;
        assert!(first.is_ok());
        let second = registry.join_session(&channel.uuid, &session_id, 1).await;
        assert!(second.is_ok());

        registry.leave_session(&channel.uuid, &session_id).await;
        let third = registry
            .join_session(&channel.uuid, &SessionId::Integer(8), 1)
            .await;
        assert!(
            matches!(third, Err(StubChannelJoinError::ChannelFull)),
            "a duplicate session id should not free the channel slot early: {third:?}"
        );

        registry.leave_session(&channel.uuid, &session_id).await;
        let fourth = registry
            .join_session(&channel.uuid, &SessionId::Integer(8), 1)
            .await;
        assert!(fourth.is_ok());
    }
}
