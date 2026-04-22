use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;

use tokio::sync::{RwLock, mpsc};

use super::{
    Channel, ChannelConfig, ChannelJoinError, ChannelManagerJoinError, ChannelMediaCounts,
    ChannelRuntimePolicy, ChannelSessionStatsSnapshot, SessionOutbound,
    directory::{ChannelDirectory, ChannelDirectoryEntry},
    factory::{ChannelCreationIntent, ChannelFactory},
};
use crate::runtime::diagnostics::{DiagnosticsEventData, DiagnosticsStore};
use crate::runtime::metrics::RuntimeMetrics;
use crate::runtime::recording::MediaTap;
use crate::runtime::telemetry::schema::event as telemetry_event;
use crate::runtime::transport_adapter::{MediaPort, ObservabilityPort, RuntimeTransportAdapter};
use crate::runtime::{ChannelRuntimeId, ConnectionId};
use o_sfu_protocol::shared::{SessionId, SessionPermissions};

#[cfg(test)]
mod test_support;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChannelManagerConfig {
    pub(crate) media_worker_count: usize,
    pub(crate) runtime_policy: ChannelRuntimePolicy,
}

impl ChannelManagerConfig {
    #[must_use]
    pub(crate) fn new(media_worker_count: usize, runtime_policy: ChannelRuntimePolicy) -> Self {
        Self {
            media_worker_count,
            runtime_policy,
        }
    }
}

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
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeChannelDirectorySnapshot {
    channel: Arc<Channel>,
    create_date: String,
    remote_address: String,
}

impl RuntimeChannelDirectorySnapshot {
    #[must_use]
    pub(crate) fn channel(&self) -> &Arc<Channel> {
        &self.channel
    }

    #[must_use]
    pub(crate) fn create_date(&self) -> &str {
        &self.create_date
    }

    #[must_use]
    pub(crate) fn remote_address(&self) -> &str {
        &self.remote_address
    }
}

/// global owner of live channels keyed by issuer and UUID.
///
/// `ChannelManager` keeps channel creation idempotent by issuer and centralizes per-room
/// lifecycle serialization so concurrent HTTP and WebSocket tasks cannot overlap join,
/// leave, disconnect, and empty-room cleanup on the same channel. Runtime entrypoints
/// should go through this type instead of cordinating room lookup and teardown
/// themselve
#[derive(Debug)]
pub struct ChannelManager {
    directory: RwLock<ChannelDirectory>,
    diagnostics: Arc<DiagnosticsStore>,
    factory: ChannelFactory,
    metrics: Arc<RuntimeMetrics>,
}

impl ChannelManager {
    #[must_use]
    pub fn new(
        config: ChannelManagerConfig,
        recording_media_tap: Arc<MediaTap>,
        diagnostics: Arc<DiagnosticsStore>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        let factory = ChannelFactory::new(
            config.media_worker_count,
            config.runtime_policy,
            recording_media_tap,
            Arc::clone(&diagnostics),
            Arc::clone(&metrics),
        );
        Self {
            directory: RwLock::new(ChannelDirectory::default()),
            diagnostics,
            factory,
            metrics,
        }
    }

    /// Serve the channel for the given issuer, creating it on first request.
    /// Channel creation remains idempotent by issuer so repeated requests keep
    /// the existing runtime placement and metadata entry.
    pub async fn serve_channel(
        &self,
        issuer: &str,
        key: Option<&str>,
        config: &ChannelConfig,
        remote_address: Option<&str>,
    ) -> Arc<Channel> {
        {
            let directory = self.directory.read().await;
            if let Some(channel) = directory.get_by_issuer(issuer) {
                return channel;
            }
        }
        let mut directory = self.directory.write().await;
        if let Some(channel) = directory.get_by_issuer(issuer) {
            return channel;
        }
        let channel = self
            .factory
            .create(ChannelCreationIntent::new(issuer, key, config));
        directory.insert(Arc::clone(&channel), remote_address);
        drop(directory);
        self.metrics.add_active_channels(1);
        self.diagnostics
            .register_channel_runtime(channel.runtime_id(), channel.uuid());
        self.diagnostics.record(
            DiagnosticsEventData::for_channel(channel.uuid(), telemetry_event::CHANNEL_CREATED)
                .with_media_worker_id(channel.media_worker_id())
                .insert_field("remote_address", remote_address.unwrap_or("unknown"))
                .insert_field("web_rtc_enabled", config.web_rtc_enabled),
        );
        channel
    }

    pub async fn get_by_uuid(&self, uuid: &str) -> Option<Arc<Channel>> {
        let directory = self.directory.read().await;
        directory.get_by_uuid(uuid)
    }

    pub async fn stats_snapshots(
        &self,
        observability_port: &impl ObservabilityPort,
    ) -> Vec<RuntimeChannelStatsSnapshot> {
        let entries = self.directory_entries().await;
        let mut snapshots = Vec::with_capacity(entries.len());
        for entry in entries {
            snapshots.push(self.entry_stats_snapshot(entry, observability_port).await);
        }
        snapshots
    }

    pub(crate) async fn directory_snapshots(&self) -> Vec<RuntimeChannelDirectorySnapshot> {
        self.directory_entries()
            .await
            .into_iter()
            .map(|entry| RuntimeChannelDirectorySnapshot {
                channel: entry.channel(),
                create_date: entry.create_date().to_owned(),
                remote_address: entry.remote_address().to_owned(),
            })
            .collect()
    }

    pub(crate) async fn directory_snapshot(
        &self,
        channel_uuid: &str,
    ) -> Option<RuntimeChannelDirectorySnapshot> {
        let entry = self.entry(channel_uuid).await?;
        Some(RuntimeChannelDirectorySnapshot {
            channel: entry.channel(),
            create_date: entry.create_date().to_owned(),
            remote_address: entry.remote_address().to_owned(),
        })
    }

    pub(crate) async fn sync_source_packet_selection_policies_for_runtime_ids(
        &self,
        channel_runtime_ids: &BTreeSet<ChannelRuntimeId>,
        observability_port: &impl ObservabilityPort,
        media_port: &impl MediaPort,
    ) {
        if channel_runtime_ids.is_empty() {
            return;
        }
        let channels = self
            .directory_entries_for_runtime_ids(channel_runtime_ids)
            .await;
        if channels.is_empty() {
            return;
        }
        let active_speaker_sources = observability_port.active_speaker_source_snapshot().await;
        for channel in channels {
            channel
                .sync_source_packet_selection_policy_from_active_speakers(
                    &active_speaker_sources,
                    media_port,
                )
                .await;
        }
    }

    pub async fn join_session(
        &self,
        channel_uuid: &str,
        request: JoinSessionRequest,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Result<(Arc<Channel>, ConnectionId), ChannelManagerJoinError> {
        let Some((channel, session_count_before, media_counts_before, join_result)) = self
            .with_current_channel(channel_uuid, |channel| async move {
                let session_count_before = channel.session_count().await;
                let media_counts_before = channel.media_counts().await;
                let join_result = channel
                    .join_session_runtime(
                        request.session_id,
                        request.label,
                        request.permissions,
                        request.sender,
                        transport_adapter,
                    )
                    .await;
                (
                    channel,
                    session_count_before,
                    media_counts_before,
                    join_result,
                )
            })
            .await
        else {
            return Err(ChannelManagerJoinError::MissingChannel);
        };
        let connection_id = join_result.map_err(|error| match error {
            ChannelJoinError::ChannelFull => ChannelManagerJoinError::ChannelFull,
            ChannelJoinError::RouterState => ChannelManagerJoinError::RouterState,
        })?;
        self.record_live_count_deltas(
            session_count_before,
            media_counts_before,
            channel.session_count().await,
            channel.media_counts().await,
        );
        Ok((channel, connection_id))
    }

    pub async fn close_session(
        &self,
        channel_uuid: &str,
        session_id: &SessionId,
        connection_id: ConnectionId,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        let Some((channel, session_count_before, media_counts_before, did_remove_active_session)) =
            self.with_current_channel(channel_uuid, |channel| async move {
                let session_count_before = channel.session_count().await;
                let media_counts_before = channel.media_counts().await;
                let did_remove_active_session = channel
                    .close_session_runtime(session_id, connection_id, transport_adapter)
                    .await;
                (
                    channel,
                    session_count_before,
                    media_counts_before,
                    did_remove_active_session,
                )
            })
            .await
        else {
            return false;
        };
        self.finish_session_mutation(
            channel_uuid,
            &channel,
            session_count_before,
            media_counts_before,
            did_remove_active_session,
        )
        .await;
        did_remove_active_session
    }

    pub async fn disconnect_sessions(
        &self,
        channel_uuid: &str,
        session_ids: &[SessionId],
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let Some((channel, session_count_before, media_counts_before)) = self
            .with_current_channel(channel_uuid, |channel| async move {
                let session_count_before = channel.session_count().await;
                let media_counts_before = channel.media_counts().await;
                channel
                    .disconnect_sessions_runtime(session_ids, transport_adapter)
                    .await;
                (channel, session_count_before, media_counts_before)
            })
            .await
        else {
            return;
        };
        self.finish_session_mutation(
            channel_uuid,
            &channel,
            session_count_before,
            media_counts_before,
            true,
        )
        .await;
    }

    pub(super) async fn with_current_channel<T, F, Fut>(
        &self,
        channel_uuid: &str,
        action: F,
    ) -> Option<T>
    where
        F: FnOnce(Arc<Channel>) -> Fut,
        Fut: Future<Output = T>,
    {
        let entry = self.entry(channel_uuid).await?;
        let channel = entry.channel();
        let lifecycle_lock = entry.lifecycle_lock();
        let _lifecycle_guard = lifecycle_lock.lock().await;
        if !self.is_current_entry(channel_uuid, &channel).await {
            return None;
        }
        Some(action(channel).await)
    }

    async fn finish_session_mutation(
        &self,
        channel_uuid: &str,
        channel: &Arc<Channel>,
        session_count_before: usize,
        media_counts_before: ChannelMediaCounts,
        remove_if_empty: bool,
    ) {
        self.record_live_count_deltas(
            session_count_before,
            media_counts_before,
            channel.session_count().await,
            channel.media_counts().await,
        );
        if remove_if_empty && channel.is_empty().await {
            self.remove_entry_if_current(channel_uuid, channel).await;
        }
    }

    async fn entry(&self, channel_uuid: &str) -> Option<ChannelDirectoryEntry> {
        let directory = self.directory.read().await;
        directory.entry(channel_uuid)
    }

    async fn directory_entries(&self) -> Vec<ChannelDirectoryEntry> {
        let directory = self.directory.read().await;
        directory.entries()
    }

    async fn directory_entries_for_runtime_ids(
        &self,
        channel_runtime_ids: &BTreeSet<ChannelRuntimeId>,
    ) -> Vec<Arc<Channel>> {
        let directory = self.directory.read().await;
        channel_runtime_ids
            .iter()
            .filter_map(|channel_runtime_id| directory.entry_by_runtime_id(*channel_runtime_id))
            .map(|entry| entry.channel())
            .collect()
    }

    async fn entry_stats_snapshot(
        &self,
        entry: ChannelDirectoryEntry,
        observability_port: &impl ObservabilityPort,
    ) -> RuntimeChannelStatsSnapshot {
        let channel = entry.channel();
        let sessions_stats = channel.session_stats_snapshot(observability_port).await;
        RuntimeChannelStatsSnapshot {
            create_date: entry.create_date().to_owned(),
            uuid: channel.uuid().to_owned(),
            remote_address: entry.remote_address().to_owned(),
            sessions_stats,
            web_rtc_enabled: channel.web_rtc_enabled(),
        }
    }

    async fn is_current_entry(&self, channel_uuid: &str, channel: &Arc<Channel>) -> bool {
        let directory = self.directory.read().await;
        directory.contains_current(channel_uuid, channel)
    }

    async fn remove_entry_if_current(&self, channel_uuid: &str, channel: &Arc<Channel>) {
        let mut directory = self.directory.write().await;
        let removed = directory.remove_if_current(channel_uuid, channel);
        drop(directory);
        if removed {
            self.metrics.add_active_channels(-1);
            self.diagnostics.forget_channel(channel_uuid);
        }
    }

    fn record_live_count_deltas(
        &self,
        session_count_before: usize,
        media_counts_before: ChannelMediaCounts,
        session_count_after: usize,
        media_counts_after: ChannelMediaCounts,
    ) {
        let before = i64::try_from(session_count_before).unwrap_or(i64::MAX);
        let after = i64::try_from(session_count_after).unwrap_or(i64::MAX);
        self.metrics
            .add_active_sessions(after.saturating_sub(before));

        let before = i64::try_from(media_counts_before.publications).unwrap_or(i64::MAX);
        let after = i64::try_from(media_counts_after.publications).unwrap_or(i64::MAX);
        self.metrics
            .add_active_publications(after.saturating_sub(before));

        let before = i64::try_from(media_counts_before.subscriptions).unwrap_or(i64::MAX);
        let after = i64::try_from(media_counts_after.subscriptions).unwrap_or(i64::MAX);
        self.metrics
            .add_active_subscriptions(after.saturating_sub(before));
    }
}
