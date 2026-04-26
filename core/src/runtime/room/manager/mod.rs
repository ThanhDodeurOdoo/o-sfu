//! Process-global room lookup and per-room lifecycle serialization.
//!
//! This module owns the boundary between "find or create the right room" and
//! "run one room-level mutation at a time". It keeps the directory keyed by
//! issuer, UUID, and instance id, and it re-checks pointer identity after
//! taking each room's lifecycle lock so stale directory handles become no-ops.

use std::{collections::BTreeSet, future::Future, sync::Arc};

use tokio::sync::{RwLock, mpsc};

use super::{
    Room, RoomConfig, RoomJoinError, RoomManagerJoinError, RoomMediaCounts, RoomRuntimePolicy,
    RoomUserStatsSnapshot, UserOutbound,
    directory::{RoomDirectory, RoomDirectoryEntry},
    factory::{RoomCreationIntent, RoomFactory},
};
use crate::runtime::{
    ConnectionId, RoomInstanceId, UserId, UserPermissions,
    diagnostics::{DiagnosticsEventData, DiagnosticsStore},
    metrics::RuntimeMetrics,
    recording::MediaTap,
    telemetry::schema::event as telemetry_event,
    transport_adapter::{MediaPort, ObservabilityPort, RuntimeTransportAdapter},
};

#[cfg(any(test, feature = "testing-transport"))]
mod test_support;

/// Runtime-wide params
///
/// stay fixed for the manager lifetime
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomManagerConfig {
    pub media_worker_count: usize,
    pub runtime_policy: RoomRuntimePolicy,
}

impl RoomManagerConfig {
    #[must_use]
    pub fn new(media_worker_count: usize, runtime_policy: RoomRuntimePolicy) -> Self {
        Self {
            media_worker_count,
            runtime_policy,
        }
    }
}

/// Observability view for one live room
///
/// This merge immutable directory metadata with the current per-room
/// user stats gathered from the observability port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeRoomStatsSnapshot {
    pub create_date: String,
    pub uuid: String,
    pub remote_address: String,
    pub users_stats: RoomUserStatsSnapshot,
    pub web_rtc_enabled: bool,
}

/// Prevalidated join input handed from signaling into the runtime room layer.
///
/// Bundling these fields keeps the join entrypoint stable as user admission
/// data grows and makes it explicit that the outbound sender belongs to the
/// requested user lifecycle
pub struct JoinUserRequest {
    pub user_id: UserId,
    pub label: Option<String>,
    pub permissions: UserPermissions,
    pub sender: mpsc::UnboundedSender<UserOutbound>,
}

/// Read-only directory snapshot for runtime-facing listing and inspection
///
/// The snapshot keeps an `Arc<Room>` so callers can inspect live room state
/// after listing metadata without accesing back into the directory.
#[derive(Debug, Clone)]
pub struct RuntimeRoomDirectorySnapshot {
    room: Arc<Room>,
    create_date: String,
    remote_address: String,
}

impl RuntimeRoomDirectorySnapshot {
    #[must_use]
    pub fn room(&self) -> &Arc<Room> {
        &self.room
    }

    #[must_use]
    pub fn create_date(&self) -> &str {
        &self.create_date
    }

    #[must_use]
    pub fn remote_address(&self) -> &str {
        &self.remote_address
    }
}

/// Process-global owner of live rooms keyed by issuer and UUID.
///
/// `RoomManager` keeps room creation idepotent by issuer and centralises
/// room-level lifecycle serialization so concurrent HTTP and WebSocket tasks
/// cannot overlap join, leave, disconnect and empty-room cleanup on the same
/// room. Runtime entrypoints should go through this type instead of
/// coordonating directory lookup and teardown themselves
#[derive(Debug)]
pub struct RoomManager {
    directory: RwLock<RoomDirectory>,
    diagnostics: Arc<DiagnosticsStore>,
    factory: RoomFactory,
    metrics: Arc<RuntimeMetrics>,
}

impl RoomManager {
    #[must_use]
    pub fn new(
        config: RoomManagerConfig,
        recording_media_tap: Arc<MediaTap>,
        diagnostics: Arc<DiagnosticsStore>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        let factory = RoomFactory::new(
            config.media_worker_count,
            config.runtime_policy,
            recording_media_tap,
            Arc::clone(&diagnostics),
            Arc::clone(&metrics),
        );
        Self {
            directory: RwLock::new(RoomDirectory::default()),
            diagnostics,
            factory,
            metrics,
        }
    }

    /// Returns the live room for `issuer`, creating it on first request.
    ///
    /// Creation is idempotent by issuer, so repeated calls keep the existing
    /// runtime placement and directory metadata for the room lifetime. Later
    /// calls do not replace the original room definition.
    pub async fn serve_room(
        &self,
        issuer: &str,
        key: Option<&str>,
        config: &RoomConfig,
        remote_address: Option<&str>,
    ) -> Arc<Room> {
        {
            let directory = self.directory.read().await;
            if let Some(room) = directory.get_by_issuer(issuer) {
                return room;
            }
        }
        let mut directory = self.directory.write().await;
        if let Some(room) = directory.get_by_issuer(issuer) {
            return room;
        }
        let room = self
            .factory
            .create(RoomCreationIntent::new(issuer, key, config));
        directory.insert(Arc::clone(&room), remote_address);
        drop(directory);
        self.metrics.add_active_rooms(1);
        self.diagnostics
            .register_room_instance(room.instance_id(), room.uuid());
        self.diagnostics.record(
            DiagnosticsEventData::for_room(room.uuid(), telemetry_event::ROOM_CREATED)
                .with_media_worker_id(room.media_worker_id())
                .insert_field("remote_address", remote_address.unwrap_or("unknown"))
                .insert_field("web_rtc_enabled", config.web_rtc_enabled),
        );
        room
    }

    /// Returns the current live room for a UUID without taking its lifecycle
    /// lock.
    pub async fn get_by_uuid(&self, uuid: &str) -> Option<Arc<Room>> {
        let directory = self.directory.read().await;
        directory.get_by_uuid(uuid)
    }

    /// Collects live observability snapshots for every room in the directory.
    ///
    /// This first snapshots the directory, then queries each room, so the
    /// result is best-effort, not like one atomic process-wide instant.
    pub async fn stats_snapshots(
        &self,
        observability_port: &impl ObservabilityPort,
    ) -> Vec<RuntimeRoomStatsSnapshot> {
        let entries = self.directory_entries().await;
        let mut snapshots = Vec::with_capacity(entries.len());
        for entry in entries {
            snapshots.push(self.entry_stats_snapshot(entry, observability_port).await);
        }
        snapshots
    }

    pub async fn directory_snapshots(&self) -> Vec<RuntimeRoomDirectorySnapshot> {
        self.directory_entries()
            .await
            .into_iter()
            .map(|entry| RuntimeRoomDirectorySnapshot {
                room: entry.room(),
                create_date: entry.create_date().to_owned(),
                remote_address: entry.remote_address().to_owned(),
            })
            .collect()
    }

    pub async fn directory_snapshot(&self, room_id: &str) -> Option<RuntimeRoomDirectorySnapshot> {
        let entry = self.entry(room_id).await?;
        Some(RuntimeRoomDirectorySnapshot {
            room: entry.room(),
            create_date: entry.create_date().to_owned(),
            remote_address: entry.remote_address().to_owned(),
        })
    }

    /// Re-applies source packet selection policy for the targeted room instances.
    ///
    /// Missing or already-removed instance ids are skipped. Active-speaker data
    /// is fetched once per call so every targeted room reacts to the same
    /// observability snapshot.
    pub async fn sync_source_packet_selection_policies_for_runtime_ids(
        &self,
        room_instance_ids: &BTreeSet<RoomInstanceId>,
        observability_port: &impl ObservabilityPort,
        media_port: &impl MediaPort,
    ) {
        if room_instance_ids.is_empty() {
            return;
        }
        let rooms = self
            .directory_entries_for_instance_ids(room_instance_ids)
            .await;
        if rooms.is_empty() {
            return;
        }
        let active_speaker_sources = observability_port.active_speaker_source_snapshot().await;
        for room in rooms {
            room.sync_source_packet_selection_policy_from_observations(
                &active_speaker_sources,
                observability_port,
                media_port,
            )
            .await;
        }
    }

    /// Joins a user through the current live room entry for `room_id`.
    ///
    /// The join runs under the room lifecycle lock so it cannot overlap another
    /// room-level mutation. On success this returns the locked room and its
    /// new runtime connection id.
    pub async fn join_user(
        &self,
        room_id: &str,
        request: JoinUserRequest,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Result<(Arc<Room>, ConnectionId), RoomManagerJoinError> {
        let Some((room, user_count_before, media_counts_before, join_result)) = self
            .with_current_room(room_id, |room| async move {
                let user_count_before = room.user_count().await;
                let media_counts_before = room.media_counts().await;
                let join_result = room
                    .join_session_runtime(
                        request.user_id,
                        request.label,
                        request.permissions,
                        request.sender,
                        transport_adapter,
                    )
                    .await;
                (room, user_count_before, media_counts_before, join_result)
            })
            .await
        else {
            return Err(RoomManagerJoinError::MissingRoom);
        };
        let connection_id = join_result.map_err(|error| match error {
            RoomJoinError::RoomFull => RoomManagerJoinError::RoomFull,
            RoomJoinError::RouterState => RoomManagerJoinError::RouterState,
        })?;
        self.record_live_count_deltas(
            user_count_before,
            media_counts_before,
            room.user_count().await,
            room.media_counts().await,
        );
        Ok((room, connection_id))
    }

    /// Closes one runtime connection if the room is still current.
    ///
    /// Returns `true` only when an active user was removed.
    pub async fn close_session(
        &self,
        room_id: &str,
        user_id: &UserId,
        connection_id: ConnectionId,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> bool {
        let Some((room, user_count_before, media_counts_before, did_remove_active_session)) = self
            .with_current_room(room_id, |room| async move {
                let user_count_before = room.user_count().await;
                let media_counts_before = room.media_counts().await;
                let did_remove_active_session = room
                    .close_session_runtime(user_id, connection_id, transport_adapter)
                    .await;
                (
                    room,
                    user_count_before,
                    media_counts_before,
                    did_remove_active_session,
                )
            })
            .await
        else {
            return false;
        };
        self.finish_session_mutation(
            room_id,
            &room,
            user_count_before,
            media_counts_before,
            did_remove_active_session,
        )
        .await;
        did_remove_active_session
    }

    /// Disconnects a batch of users under one lifecycle lock acquisition.
    ///
    /// This is the bulk teardown path for room-level disconnects. The directory
    /// entry is removed afterward if the batch leaves the room empty.
    pub async fn disconnect_users(
        &self,
        room_id: &str,
        user_ids: &[UserId],
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let Some((room, user_count_before, media_counts_before)) = self
            .with_current_room(room_id, |room| async move {
                let user_count_before = room.user_count().await;
                let media_counts_before = room.media_counts().await;
                room.disconnect_sessions_runtime(user_ids, transport_adapter)
                    .await;
                (room, user_count_before, media_counts_before)
            })
            .await
        else {
            return;
        };
        self.finish_session_mutation(room_id, &room, user_count_before, media_counts_before, true)
            .await;
    }

    /// Runs `action` under the current entry's lifecycle lock.
    ///
    /// The helper snapshots the directory entry before locking, then checks
    /// again after the lock is acquired that the same `Arc<Room>` is still
    /// the current directory entry. That turns stale handles into `None`
    /// instead of mutating a room that has already been removed or replaced.
    pub(super) async fn with_current_room<T, F, Fut>(&self, room_id: &str, action: F) -> Option<T>
    where
        F: FnOnce(Arc<Room>) -> Fut,
        Fut: Future<Output = T>,
    {
        let entry = self.entry(room_id).await?;
        let room = entry.room();
        let lifecycle_lock = entry.lifecycle_lock();
        let _lifecycle_guard = lifecycle_lock.lock().await;
        if !self.is_current_entry(room_id, &room).await {
            return None;
        }
        Some(action(room).await)
    }

    async fn finish_session_mutation(
        &self,
        room_id: &str,
        room: &Arc<Room>,
        user_count_before: usize,
        media_counts_before: RoomMediaCounts,
        remove_if_empty: bool,
    ) {
        self.record_live_count_deltas(
            user_count_before,
            media_counts_before,
            room.user_count().await,
            room.media_counts().await,
        );
        if remove_if_empty && room.is_empty().await {
            self.remove_entry_if_current(room_id, room).await;
        }
    }

    async fn entry(&self, room_id: &str) -> Option<RoomDirectoryEntry> {
        let directory = self.directory.read().await;
        directory.entry(room_id)
    }

    async fn directory_entries(&self) -> Vec<RoomDirectoryEntry> {
        let directory = self.directory.read().await;
        directory.entries()
    }

    async fn directory_entries_for_instance_ids(
        &self,
        room_instance_ids: &BTreeSet<RoomInstanceId>,
    ) -> Vec<Arc<Room>> {
        let directory = self.directory.read().await;
        room_instance_ids
            .iter()
            .filter_map(|room_instance_id| directory.entry_by_instance_id(*room_instance_id))
            .map(|entry| entry.room())
            .collect()
    }

    async fn entry_stats_snapshot(
        &self,
        entry: RoomDirectoryEntry,
        observability_port: &impl ObservabilityPort,
    ) -> RuntimeRoomStatsSnapshot {
        let room = entry.room();
        let users_stats = room.session_stats_snapshot(observability_port).await;
        RuntimeRoomStatsSnapshot {
            create_date: entry.create_date().to_owned(),
            uuid: room.uuid().to_owned(),
            remote_address: entry.remote_address().to_owned(),
            users_stats,
            web_rtc_enabled: room.web_rtc_enabled(),
        }
    }

    async fn is_current_entry(&self, room_id: &str, room: &Arc<Room>) -> bool {
        let directory = self.directory.read().await;
        directory.contains_current(room_id, room)
    }

    async fn remove_entry_if_current(&self, room_id: &str, room: &Arc<Room>) {
        let mut directory = self.directory.write().await;
        let removed = directory.remove_if_current(room_id, room);
        drop(directory);
        if removed {
            self.metrics.add_active_rooms(-1);
            self.diagnostics.forget_room(room_id);
        }
    }

    fn record_live_count_deltas(
        &self,
        user_count_before: usize,
        media_counts_before: RoomMediaCounts,
        user_count_after: usize,
        media_counts_after: RoomMediaCounts,
    ) {
        let before = i64::try_from(user_count_before).unwrap_or(i64::MAX);
        let after = i64::try_from(user_count_after).unwrap_or(i64::MAX);
        self.metrics.add_active_users(after.saturating_sub(before));

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
