//! [`RoomManager`] is the live-room registry used by HTTP, websocket and
//! background runtime tasks. it manage room publication, current-room
//! lifecycle leases, worker-load snapshots and room removal
//!
//! ```text
//! /v1/channel -> serve_room -> directory
//! websocket   -> join_user  -> RoomEffects
//! background  -> source policy
//! ```
//!
//! callers receive cloned [`std::sync::Arc`] handles to [`Room`], but only
//! directory-current rooms accept manager mutations. empty-room removal waits
//! for accepted leases to finish before the directory row is forgotten

#[cfg(test)]
use std::sync::Mutex;
use std::{collections::BTreeSet, future::Future, sync::Arc};

use o_sfu_telemetry::schema::event as telemetry_event;
use tokio::sync::RwLock;

#[cfg(test)]
pub use super::placement::JoinPlacementTestGate;
use super::{
    Room, RoomConfig, RoomJoinError, RoomManagerJoinError, RoomRuntimePolicy,
    RoomUserStatsSnapshot,
    directory::{RoomDirectory, RoomDirectoryEntry, RoomLifecycleLease},
    effects::batch::RoomEffectContext,
    factory::RoomFactory,
    membership::JoinUserRequest,
    placement::{JoinAdmissionTurn, WorkerLoadIndex},
    source_policy::SourcePolicyTurn,
};
use crate::engine::{
    ConnectionId, RoomInstanceId, UserId,
    diagnostics::{self, DiagnosticsEventData, DiagnosticsStore},
    media_transport::{MediaTransport, TransportSessionKey},
    metrics::RuntimeMetrics,
};

#[cfg(any(test, feature = "testing-transport"))]
#[path = "TESTS/support.rs"]
mod test_support;

/// runtime configuration shared by one room manager
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomManagerConfig {
    /// upper bound used when building worker-load snapshots
    ///
    /// values below one are normalized by [`RoomManager::new`]
    pub media_worker_count: usize,
    /// room policy cloned into each newly published room
    pub runtime_policy: RoomRuntimePolicy,
}

impl RoomManagerConfig {
    /// builds manager configuration from validated runtime policy
    #[must_use]
    pub fn new(media_worker_count: usize, runtime_policy: RoomRuntimePolicy) -> Self {
        Self {
            media_worker_count,
            runtime_policy,
        }
    }
}

/// process services shared by room manager and room instances
#[derive(Debug, Clone)]
pub struct RoomManagerDeps {
    /// event store used by room creation and removal diagnostics
    pub diagnostics: Arc<DiagnosticsStore>,
    /// metric catalog updated by room publication and teardown
    pub metrics: Arc<RuntimeMetrics>,
}

/// operator-facing room stats assembled from directory and transport snapshots
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeRoomStatsSnapshot {
    /// room publication timestamp in RFC 3339 UTC format
    pub create_date: String,
    /// room uuid returned by `/v1/channel`
    pub uuid: String,
    /// first create request address or `unknown` when unavailable
    pub remote_address: String,
    /// live user, stream and bitrate stats read after the directory snapshot
    pub users_stats: RoomUserStatsSnapshot,
    /// room creation flag exposed by `/v1/stats`
    pub web_rtc_enabled: bool,
}

/// committed admission result returned after join-side effects run
#[derive(Debug, Clone)]
pub struct RoomUserAdmission {
    /// current room that accepted the session
    pub room: Arc<Room>,
    /// room-local connection id assigned to the admitted user
    pub connection_id: ConnectionId,
    /// transport key used by [`crate::prelude::SfuCore`] to build
    /// [`crate::prelude::MediaSession`]
    pub transport_session_key: TransportSessionKey,
}

/// current room directory row used by diagnostics views
#[derive(Debug, Clone)]
pub struct RuntimeRoomDirectorySnapshot {
    /// current room for this directory row
    pub room: Arc<Room>,
    /// room publication timestamp in RFC 3339 UTC format
    pub create_date: String,
    /// first create request address or `unknown` when unavailable
    pub remote_address: String,
}

/// current-room registry and lifecycle coordinator
///
/// `RoomManager` exposes only current directory rows. mutating entrypoints
/// accept a lifecycle lease before awaiting room work, then remove empty rooms
/// only after the accepted mutation finishes
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
    /// builds a room manager with an empty directory
    ///
    /// `media_worker_count` is normalized to at least one so diagnostics and
    /// placement snapshots always have a worker range to inspect
    #[must_use]
    pub fn new(config: RoomManagerConfig, deps: RoomManagerDeps) -> Self {
        let factory = RoomFactory::new(
            config.runtime_policy,
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

    /// returns the current room for `issuer`, creating it on the first miss
    ///
    /// only the first create request publishes `key`, `config` and
    /// `remote_address` into the room. concurrent calls for the same issuer
    /// return the same current room and do not emit duplicate creation metrics
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

    /// returns the current room for a public room uuid
    pub async fn get_by_uuid(&self, uuid: &str) -> Option<Arc<Room>> {
        let directory = self.directory.read().await;
        directory.get_by_uuid(uuid)
    }

    /// builds `/v1/stats` rows from one directory snapshot
    ///
    /// the directory lock is released before transport stats are read, so the
    /// returned rows are best-effort runtime observations rather than a global
    /// transaction across room and media state
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

    /// returns current directory rows for room diagnostics
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

    /// returns one current directory row for room diagnostics
    pub async fn directory_snapshot(&self, room_id: &str) -> Option<RuntimeRoomDirectorySnapshot> {
        let entry = self.entry(room_id).await?;
        Some(RuntimeRoomDirectorySnapshot {
            room: entry.room,
            create_date: entry.create_date,
            remote_address: entry.remote_address,
        })
    }

    /// returns the normalized worker count used by diagnostics worker views
    #[must_use]
    pub const fn media_worker_count(&self) -> usize {
        self.media_worker_count
    }

    /// returns current directory rows for requested room ids in request order
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

    /// recalculates packet-selection policy for rooms dirtied by media activity
    ///
    /// empty input is a no-op. rooms that left the current directory before the
    /// drain are skipped. committed route work is executed after each policy
    /// plan so transport routing and accepted selector state stay in sync
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
            SourcePolicyTurn::packet_selection()
                .execute(&room, Some(media_transport), Some(&active_speaker_sources))
                .await;
        }
    }

    /// admits one websocket connection into a current room
    ///
    /// placement uses worker pressure plus existing room load at call time. a
    /// successful admission has already executed join-side room effects before
    /// the returned transport key reaches [`crate::prelude::SfuCore`]
    ///
    /// # Errors
    ///
    /// returns:
    ///
    /// - [`RoomManagerJoinError::MissingRoom`] when `room_id` is not current
    /// - [`RoomManagerJoinError::RoomFull`] when room admission rejects capacity
    /// - [`RoomManagerJoinError::RouterState`] when routing placement fails
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
                    let worker_loads = self.worker_load_index(media_transport).await;
                    let admission =
                        JoinAdmissionTurn::from_factory(request, worker_loads, &self.factory);
                    #[cfg(test)]
                    let admission = admission.with_gate(self.join_placement_gate_for_test());
                    room.admit_session(admission, RoomEffectContext::runtime(media_transport))
                        .await
                },
                false,
            )
            .await
        else {
            return Err(RoomManagerJoinError::MissingRoom);
        };
        let receipt = join_result.map_err(|error| match error {
            RoomJoinError::RoomFull => RoomManagerJoinError::RoomFull,
            RoomJoinError::RouterState => RoomManagerJoinError::RouterState,
        })?;
        Ok(RoomUserAdmission {
            room,
            connection_id: receipt.connection_id,
            transport_session_key: receipt.transport_session_key,
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

    /// closes one room connection and then re-checks empty-room removal
    ///
    /// returns `false` when the room is missing or the connection was not
    /// removed by this call. the empty current room can still be removed after
    /// stale or already-completed teardown
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
                true,
            )
            .await
        else {
            return false;
        };
        did_remove_active_session
    }

    /// disconnects selected users from a current room and removes it if empty
    ///
    /// missing rooms are ignored because the caller's disconnect intent is
    /// already satisfied
    pub async fn disconnect_users(
        &self,
        room_id: &str,
        user_ids: &[UserId],
        media_transport: &MediaTransport,
    ) {
        let _ = self
            .run_current_room_mutation(
                room_id,
                |room| async move {
                    room.disconnect_users(user_ids, media_transport).await;
                },
                true,
            )
            .await;
    }

    #[cfg(test)]
    pub(super) async fn with_current_room<T, F, Fut>(&self, room_id: &str, action: F) -> Option<T>
    where
        F: FnOnce(Arc<Room>) -> Fut,
        Fut: Future<Output = T>,
    {
        self.run_current_room_mutation(room_id, action, false)
            .await
            .map(|(_, output)| output)
    }

    /// runs awaited room work under a cancellation-safe lifecycle lease
    ///
    /// the directory lock is not held while `action` runs. concurrent teardown
    /// can finish while another accepted mutation is parked, but directory
    /// removal waits until all leases drain
    async fn run_current_room_mutation<T, F, Fut>(
        &self,
        room_id: &str,
        action: F,
        remove_if_empty: bool,
    ) -> Option<(Arc<Room>, T)>
    where
        F: FnOnce(Arc<Room>) -> Fut,
        Fut: Future<Output = T>,
    {
        let mutation = self.begin_current_room_mutation(room_id).await?;
        let room = Arc::clone(&mutation.room);
        let output = action(Arc::clone(&room)).await;
        self.finish_session_mutation(room_id, mutation, remove_if_empty)
            .await;
        Some((room, output))
    }

    /// releases a current-room mutation and removes the row if this finisher won
    async fn finish_session_mutation(
        &self,
        room_id: &str,
        mutation: CurrentRoomMutation,
        remove_if_empty: bool,
    ) {
        let CurrentRoomMutation { room, lease } = mutation;
        if lease.finish(remove_if_empty, room.is_empty().await) {
            self.remove_entry_if_current(room_id, &room).await;
        }
    }

    /// accepts work only against the directory-current room row
    ///
    /// the returned mutation holds no directory lock. if the row is replaced
    /// between snapshot and current check, the lease is cancelled and the caller
    /// sees `None`
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

    /// forgets a directory row only if it still points at the same room
    async fn remove_entry_if_current(&self, room_id: &str, room: &Arc<Room>) {
        let mut directory = self.directory.write().await;
        let removed = directory.remove_if_current(room_id, room);
        drop(directory);
        if removed {
            self.metrics.add_active_rooms(-1);
            self.diagnostics.forget_room(room_id);
        }
    }
}

/// lease plus room pointer accepted from the current directory row
///
/// dropping the lease without [`RoomManager::finish_session_mutation`] releases
/// admission but never removes the room
#[derive(Debug)]
struct CurrentRoomMutation {
    room: Arc<Room>,
    lease: RoomLifecycleLease,
}
