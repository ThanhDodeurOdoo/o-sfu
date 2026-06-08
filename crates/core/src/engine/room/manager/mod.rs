#[cfg(not(test))]
use std::future::ready;
#[cfg(test)]
use std::sync::Mutex;
use std::{collections::BTreeSet, future::Future, sync::Arc};

use o_sfu_telemetry::schema::event as telemetry_event;
use tokio::sync::RwLock;

#[cfg(test)]
pub use self::test_support::JoinPlacementTestGate;
use super::{
    Room, RoomConfig, RoomJoinError, RoomManagerJoinError, RoomRuntimePolicy,
    RoomUserStatsSnapshot,
    directory::{RoomDirectory, RoomDirectoryEntry, RoomLifecycleLease},
    effects::batch::RoomEffectContext,
    factory::RoomFactory,
    membership::JoinUserRequest,
    placement::WorkerLoadIndex,
};
use crate::engine::{
    ConnectionId, RoomInstanceId, UserId,
    diagnostics::{self, DiagnosticsEventData, DiagnosticsStore},
    media_transport::{MediaTransport, TransportSessionKey},
    metrics::RuntimeMetrics,
    packet_sink_registry::RoomPacketSinkRegistry,
};

#[cfg(any(test, feature = "testing-transport"))]
#[path = "TESTS/support.rs"]
mod test_support;

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

#[derive(Debug, Clone)]
pub struct RoomManagerDeps {
    pub packet_sink_registry: Arc<RoomPacketSinkRegistry>,
    pub diagnostics: Arc<DiagnosticsStore>,
    pub metrics: Arc<RuntimeMetrics>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeRoomStatsSnapshot {
    pub create_date: String,
    pub uuid: String,
    pub remote_address: String,
    pub users_stats: RoomUserStatsSnapshot,
    pub web_rtc_enabled: bool,
}

#[derive(Debug, Clone)]
pub struct RoomUserAdmission {
    pub room: Arc<Room>,
    pub connection_id: ConnectionId,
    pub transport_session_key: TransportSessionKey,
}

#[derive(Debug, Clone)]
pub struct RuntimeRoomDirectorySnapshot {
    pub room: Arc<Room>,
    pub create_date: String,
    pub remote_address: String,
}

#[derive(Debug)]
pub struct RoomManager {
    directory: RwLock<RoomDirectory>,
    diagnostics: Arc<DiagnosticsStore>,
    factory: RoomFactory,
    #[cfg(test)]
    join_placement_gate: Mutex<Option<Arc<JoinPlacementTestGate>>>,
    media_worker_count: usize,
    metrics: Arc<RuntimeMetrics>,
}

impl RoomManager {
    #[must_use]
    pub fn new(config: RoomManagerConfig, deps: RoomManagerDeps) -> Self {
        let factory = RoomFactory::new(
            config.runtime_policy,
            deps.packet_sink_registry,
            Arc::clone(&deps.diagnostics),
            Arc::clone(&deps.metrics),
        );
        Self {
            directory: RwLock::new(RoomDirectory::default()),
            diagnostics: deps.diagnostics,
            factory,
            #[cfg(test)]
            join_placement_gate: Mutex::new(None),
            media_worker_count: config.media_worker_count.max(1),
            metrics: deps.metrics,
        }
    }

    pub async fn serve_room(
        &self,
        issuer: &str,
        key: &str,
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
        let room = self.factory.create(issuer, key, config);
        directory.insert(Arc::clone(&room), remote_address);
        drop(directory);
        self.metrics.add_active_rooms(1);
        self.diagnostics.register_room_instance(
            diagnostics::diagnostics_room_instance_id(room.instance_id()),
            room.uuid(),
        );
        self.diagnostics.record(
            DiagnosticsEventData::for_room(room.uuid(), telemetry_event::ROOM_CREATED)
                .insert_field("remote_address", remote_address.unwrap_or("unknown"))
                .insert_field("web_rtc_enabled", config.web_rtc_enabled),
        );
        room
    }

    pub async fn get_by_uuid(&self, uuid: &str) -> Option<Arc<Room>> {
        let directory = self.directory.read().await;
        directory.get_by_uuid(uuid)
    }

    pub async fn stats_snapshots(
        &self,
        media_transport: &MediaTransport,
    ) -> Vec<RuntimeRoomStatsSnapshot> {
        let entries = self.directory_entries().await;
        let mut snapshots = Vec::with_capacity(entries.len());
        for entry in entries {
            snapshots.push(self.entry_stats_snapshot(entry, media_transport).await);
        }
        snapshots
    }

    pub async fn directory_snapshots(&self) -> Vec<RuntimeRoomDirectorySnapshot> {
        self.directory_entries()
            .await
            .into_iter()
            .map(|entry| RuntimeRoomDirectorySnapshot {
                room: entry.room,
                create_date: entry.create_date,
                remote_address: entry.remote_address,
            })
            .collect()
    }

    pub async fn directory_snapshot(&self, room_id: &str) -> Option<RuntimeRoomDirectorySnapshot> {
        let entry = self.entry(room_id).await?;
        Some(RuntimeRoomDirectorySnapshot {
            room: entry.room,
            create_date: entry.create_date,
            remote_address: entry.remote_address,
        })
    }

    #[must_use]
    pub const fn media_worker_count(&self) -> usize {
        self.media_worker_count
    }

    pub async fn directory_snapshots_for_room_ids(
        &self,
        room_ids: &[String],
    ) -> Vec<RuntimeRoomDirectorySnapshot> {
        let directory = self.directory.read().await;
        room_ids
            .iter()
            .filter_map(|room_id| directory.entry(room_id))
            .map(|entry| RuntimeRoomDirectorySnapshot {
                room: entry.room,
                create_date: entry.create_date,
                remote_address: entry.remote_address,
            })
            .collect()
    }

    pub async fn sync_source_packet_selection_policies_for_runtime_ids(
        &self,
        room_instance_ids: &BTreeSet<RoomInstanceId>,
        media_transport: &MediaTransport,
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
        let active_speaker_sources = media_transport.active_speaker_source_snapshot().await;
        for room in rooms {
            room.sync_source_packet_selection_policy_from_observations(
                &active_speaker_sources,
                media_transport,
            )
            .await;
        }
    }

    pub async fn drain_cleanup_retries(&self, media_transport: &MediaTransport) {
        for entry in self.directory_entries().await {
            let room = entry.room;
            let room_id = room.uuid().to_owned();
            self.run_current_room_mutation(
                &room_id,
                |room| async move {
                    let had_pending_cleanup_retries = room.has_pending_cleanup_retries();
                    if had_pending_cleanup_retries {
                        room.drain_cleanup_retries(media_transport).await;
                    }
                    room.reconcile_spillover_routers().await;
                    had_pending_cleanup_retries
                        && room.is_empty().await
                        && !room.has_pending_cleanup_retries()
                },
                |should_remove| *should_remove,
            )
            .await;
        }
    }

    /// # Errors
    ///
    /// returns [`RoomManagerJoinError`] when the room is missing or admission
    /// rejects the user
    pub async fn join_user(
        &self,
        room_id: &str,
        request: JoinUserRequest,
        media_transport: &MediaTransport,
    ) -> Result<RoomUserAdmission, RoomManagerJoinError> {
        let Some((room, join_result)) = self
            .run_current_room_mutation(
                room_id,
                |room| async move {
                    #[cfg(test)]
                    let after_planning = self.wait_after_join_placement_for_test();
                    #[cfg(not(test))]
                    let after_planning = ready(());
                    let worker_loads = self.worker_load_index(media_transport).await;
                    room.admit_session(
                        request,
                        worker_loads,
                        RoomEffectContext::runtime(media_transport),
                        after_planning,
                        || self.factory.allocate_spillover_router(),
                    )
                    .await
                },
                |_| false,
            )
            .await
        else {
            return Err(RoomManagerJoinError::MissingRoom);
        };
        let routing_receipt = join_result.map_err(|error| match error {
            RoomJoinError::RoomFull => RoomManagerJoinError::RoomFull,
            RoomJoinError::RouterState => RoomManagerJoinError::RouterState,
        })?;
        Ok(RoomUserAdmission {
            room,
            connection_id: routing_receipt.connection_id,
            transport_session_key: routing_receipt.transport_session_key,
        })
    }

    async fn worker_load_index(&self, media_transport: &MediaTransport) -> WorkerLoadIndex {
        let mut load_index = WorkerLoadIndex::new(
            self.media_worker_count,
            media_transport.worker_pressure_snapshots(),
        );
        for entry in self.directory_entries().await {
            entry.room.record_worker_load(&mut load_index).await;
        }
        load_index
    }

    pub async fn close_session(
        &self,
        room_id: &str,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_transport: &MediaTransport,
    ) -> bool {
        let Some((_room, did_remove_active_session)) = self
            .run_current_room_mutation(
                room_id,
                |room| async move {
                    room.remove_user(user_id, connection_id, media_transport)
                        .await
                },
                |did_remove_active_session| *did_remove_active_session,
            )
            .await
        else {
            return false;
        };
        did_remove_active_session
    }

    pub async fn disconnect_users(
        &self,
        room_id: &str,
        user_ids: &[UserId],
        media_transport: &MediaTransport,
    ) {
        let Some((_room, ())) = self
            .run_current_room_mutation(
                room_id,
                |room| async move {
                    room.disconnect_users(user_ids, media_transport).await;
                },
                |()| true,
            )
            .await
        else {
            return;
        };
    }

    #[cfg(test)]
    pub(super) async fn with_current_room<T, F, Fut>(&self, room_id: &str, action: F) -> Option<T>
    where
        F: FnOnce(Arc<Room>) -> Fut,
        Fut: Future<Output = T>,
    {
        let mutation = self.begin_current_room_mutation(room_id).await?;
        let room = Arc::clone(&mutation.room);
        let output = action(room).await;
        self.finish_session_mutation(room_id, mutation, false).await;
        Some(output)
    }

    async fn run_current_room_mutation<T, F, Fut, ShouldRemove>(
        &self,
        room_id: &str,
        action: F,
        should_remove_if_empty: ShouldRemove,
    ) -> Option<(Arc<Room>, T)>
    where
        F: FnOnce(Arc<Room>) -> Fut,
        Fut: Future<Output = T>,
        ShouldRemove: FnOnce(&T) -> bool,
    {
        let mutation = self.begin_current_room_mutation(room_id).await?;
        let room = Arc::clone(&mutation.room);
        let output = action(Arc::clone(&room)).await;
        self.finish_session_mutation(room_id, mutation, should_remove_if_empty(&output))
            .await;
        Some((room, output))
    }

    async fn finish_session_mutation(
        &self,
        room_id: &str,
        mutation: CurrentRoomMutation,
        remove_if_empty: bool,
    ) {
        let room = Arc::clone(&mutation.room);
        let room_can_be_removed = self.room_can_be_removed(&room).await;
        if mutation.lease.finish(remove_if_empty, room_can_be_removed) {
            self.remove_entry_if_current(room_id, &room).await;
        }
    }

    async fn begin_current_room_mutation(&self, room_id: &str) -> Option<CurrentRoomMutation> {
        let entry = self.entry(room_id).await?;
        let lease = entry.lifecycle.begin()?;
        let room = entry.room;
        if self.is_current_entry(room_id, &room).await {
            return Some(CurrentRoomMutation { room, lease });
        }
        lease.cancel();
        None
    }

    async fn room_can_be_removed(&self, room: &Arc<Room>) -> bool {
        room.is_empty().await && !room.has_pending_cleanup_retries()
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
            .map(|entry| entry.room)
            .collect()
    }

    async fn entry_stats_snapshot(
        &self,
        entry: RoomDirectoryEntry,
        media_transport: &MediaTransport,
    ) -> RuntimeRoomStatsSnapshot {
        let room = entry.room;
        let users_stats = room.session_stats_snapshot(media_transport).await;
        RuntimeRoomStatsSnapshot {
            create_date: entry.create_date,
            uuid: room.uuid().to_owned(),
            remote_address: entry.remote_address,
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
            room.abandon_cleanup_retries_for_shutdown();
            self.metrics.add_active_rooms(-1);
            self.diagnostics.forget_room(room_id);
        }
    }
}

#[derive(Debug)]
struct CurrentRoomMutation {
    room: Arc<Room>,
    lease: RoomLifecycleLease,
}
