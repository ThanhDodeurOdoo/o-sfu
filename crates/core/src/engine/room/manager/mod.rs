//! Process-global room lookup and per-room lifecycle liveness.
//!
//! This module contains the boundary between "find or create the right room" and
//! "run work only against a room that is still current". It keeps the directory
//! keyed by issuer, UUID and instance id, and turns stale directory handles into
//! no-ops before caller work runs.

#[cfg(test)]
use std::sync::Mutex as StdMutex;
use std::{collections::BTreeSet, future::Future, sync::Arc};

use o_sfu_telemetry::schema::event as telemetry_event;
use tokio::sync::RwLock;

#[cfg(test)]
pub(in crate::engine::room) use self::test_support::JoinPlacementTestGate;
use super::{
    Room, RoomConfig, RoomJoinError, RoomManagerJoinError, RoomRuntimePolicy,
    RoomUserStatsSnapshot, SourcePolicyEvent, UserOutboundSender,
    directory::{RoomDirectory, RoomDirectoryEntry, RoomLifecycleLease},
    effects::RoomEffectContext,
    factory::RoomFactory,
    membership::JoinSessionIntent,
    placement::{JoinPlacementPlan, RoomPlacementPlanner, WorkerLoadIndex},
};
use crate::{
    RoomSpilloverMode,
    engine::{
        ConnectionId, RoomInstanceId, UserId, UserPermissions,
        diagnostics::{self, DiagnosticsEventData, DiagnosticsStore},
        media_transport::{MediaTransport, TransportSessionKey},
        metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry,
        sync::lock_unpoisoned,
    },
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

#[derive(Debug, Clone)]
pub struct RoomManagerDeps {
    pub packet_sink_registry: Arc<RoomPacketSinkRegistry>,
    pub diagnostics: Arc<DiagnosticsStore>,
    pub metrics: Arc<RuntimeMetrics>,
}

/// Observability view for one live room
///
/// This merge immutable directory metadata with the current per-room
/// user stats gathered from `MediaTransport`.
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
    pub sender: UserOutboundSender,
}

/// Room admission result with the committed transport routing key.
///
/// The room manager returns this after the room state has accepted the join and
/// the routing boundary has committed the connection placement.
#[derive(Debug, Clone)]
pub struct JoinedRoomSession {
    room: Arc<Room>,
    connection_id: ConnectionId,
    transport_session_key: TransportSessionKey,
}

impl JoinedRoomSession {
    #[must_use]
    pub fn room(&self) -> &Arc<Room> {
        &self.room
    }

    #[must_use]
    pub const fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    #[must_use]
    pub const fn transport_session_key(&self) -> &TransportSessionKey {
        &self.transport_session_key
    }
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

/// process-global live-room directory keyed by issuer and UUID
///
/// [`RoomManager`] keeps room creation idempotent by issuer and stores the current
/// room directory
/// lifecycle leases prevent empty-room removal from racing
/// accepted work, while room state locks and transition methods own the actual
/// membership ordering
/// runtime entrypoints should go through this type instead
/// of coordinating directory lookup and teardown themselves
#[derive(Debug)]
pub struct RoomManager {
    directory: RwLock<RoomDirectory>,
    diagnostics: Arc<DiagnosticsStore>,
    factory: RoomFactory,
    #[cfg(test)]
    join_placement_gate: StdMutex<Option<Arc<JoinPlacementTestGate>>>,
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
            join_placement_gate: StdMutex::new(None),
            media_worker_count: config.media_worker_count.max(1),
            metrics: deps.metrics,
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

    #[must_use]
    pub const fn media_worker_count(&self) -> usize {
        self.media_worker_count
    }

    /// Returns live directory snapshots for known room ids.
    ///
    /// Diagnostics user lookup uses this after resolving candidate rooms from
    /// the diagnostics user index. Missing rooms are skipped because the
    /// index and directory can be observed at slightly different instants while
    /// room teardown is in progress.
    pub async fn directory_snapshots_for_room_ids(
        &self,
        room_ids: &[String],
    ) -> Vec<RuntimeRoomDirectorySnapshot> {
        let directory = self.directory.read().await;
        room_ids
            .iter()
            .filter_map(|room_id| directory.entry(room_id))
            .map(|entry| RuntimeRoomDirectorySnapshot {
                room: entry.room(),
                create_date: entry.create_date().to_owned(),
                remote_address: entry.remote_address().to_owned(),
            })
            .collect()
    }

    /// Re-applies source packet selection policy for the targeted room instances.
    ///
    /// Missing or already-removed instance ids are skipped. Active-speaker data
    /// is fetched once per call so every targeted room reacts to the same
    /// observability snapshot.
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

    /// drains due cleanup retries for rooms retained after teardown
    ///
    /// this is the runtime maintenance entrypoint for cleanup work captured
    /// after room state has already forgotten users or media. callers should
    /// pass the same [`MediaTransport`] used by normal room teardown so queued
    /// operations keep targeting the resolved transport identities stored by
    /// the room
    ///
    /// each room remains the owner of retry classification, backoff, metrics
    /// and last-resort escalation. the manager only serializes the sweep with
    /// the room lifecycle lock and removes a current room after it is empty
    /// with no pending retry state
    ///
    /// the sweep is best effort. rooms removed or replaced while the directory
    /// snapshot is being processed are skipped by `run_current_room_mutation`
    ///
    /// this is cold-path lifecycle work. it must not be called from packet
    /// forwarding or transport hot loops
    pub async fn drain_cleanup_retries(&self, media_transport: &MediaTransport) {
        for entry in self.directory_entries().await {
            let room = entry.room();
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

    /// Joins a user through the current live room entry for `room_id`.
    ///
    /// On success this returns the current room and its new runtime connection
    /// id plus the committed transport routing key. The room state transition
    /// records live user and media deltas before async transport effects run.
    ///
    /// # Errors
    ///
    /// Returns [`RoomManagerJoinError`] when the room is missing or when room
    /// admission rejects the user.
    pub async fn join_user(
        &self,
        room_id: &str,
        request: JoinUserRequest,
        media_transport: &MediaTransport,
    ) -> Result<JoinedRoomSession, RoomManagerJoinError> {
        let Some((room, join_result)) = self
            .run_current_room_mutation(
                room_id,
                |room| async move {
                    let placement = self.prepare_join_placement(&room, media_transport).await;
                    #[cfg(test)]
                    self.wait_after_join_placement_for_test().await;
                    room.join_session_with_cleanup(
                        JoinSessionIntent {
                            user_id: request.user_id,
                            label: request.label,
                            permissions: request.permissions,
                            sender: request.sender,
                            emit_joined_fanout: true,
                            placement,
                        },
                        RoomEffectContext::runtime(media_transport),
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
        Ok(JoinedRoomSession {
            room,
            connection_id: routing_receipt.connection_id(),
            transport_session_key: routing_receipt.transport_session_key().clone(),
        })
    }

    async fn prepare_join_placement(
        &self,
        room: &Arc<Room>,
        media_transport: &MediaTransport,
    ) -> JoinPlacementPlan {
        let room_snapshot = room.placement_usage_snapshot().await;
        let worker_loads = self.worker_load_index(media_transport).await;
        let policy = room.room_worker_policy();
        let planner = RoomPlacementPlanner::new(self.media_worker_count, policy);
        let decision = match policy.spillover() {
            RoomSpilloverMode::LoadTriggeredLocalSpillover(_) => {
                room.handle_source_policy_event(
                    SourcePolicyEvent::FanoutPressureChanged,
                    Some(media_transport),
                )
                .await;
                let mut load_state = lock_unpoisoned(&room.load_triggered_placement);
                planner.choose_with_load_state(&room_snapshot, &worker_loads, &mut load_state)
            }
            RoomSpilloverMode::StrictSingleRouter | RoomSpilloverMode::BoundedLocalSpillover => {
                planner.choose(&room_snapshot, &worker_loads)
            }
        };
        JoinPlacementPlan::planned(decision, worker_loads, policy)
    }

    async fn worker_load_index(&self, media_transport: &MediaTransport) -> WorkerLoadIndex {
        let mut load_index = WorkerLoadIndex::new(
            self.media_worker_count,
            media_transport.worker_pressure_snapshots(),
        );
        for entry in self.directory_entries().await {
            let contribution = entry.room().worker_load_contribution().await;
            for media_worker_id in contribution.session_worker_ids {
                load_index.record_session(media_worker_id);
            }
            for media_worker_id in contribution.consumer_worker_ids {
                load_index.record_consumer(media_worker_id);
            }
        }
        load_index
    }

    /// Closes one runtime connection if the room is still current.
    ///
    /// Returns `true` only when an active user was removed.
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

    /// Disconnects a batch of users through the current room entry.
    ///
    /// This is the bulk teardown path for room-level disconnects. The directory
    /// entry is removed afterward if the batch leaves the room empty.
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

    /// Runs `action` only after acquiring a current-room lifecycle lease.
    ///
    /// The lease keeps empty-room removal from racing ahead of accepted work,
    /// but no caller future runs while the lifecycle state is locked.
    #[cfg(test)]
    pub(super) async fn with_current_room<T, F, Fut>(&self, room_id: &str, action: F) -> Option<T>
    where
        F: FnOnce(Arc<Room>) -> Fut,
        Fut: Future<Output = T>,
    {
        let mutation = self.begin_current_room_mutation(room_id).await?;
        let room = Arc::clone(&mutation.room);
        let output = action(Arc::clone(&room)).await;
        let room_can_be_removed = self.room_can_be_removed(&room).await;
        if mutation.finish(false, room_can_be_removed) {
            self.remove_entry_if_current(room_id, &room).await;
        }
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
        if mutation.finish(remove_if_empty, room_can_be_removed) {
            self.remove_entry_if_current(room_id, &room).await;
        }
    }

    async fn begin_current_room_mutation(&self, room_id: &str) -> Option<CurrentRoomMutation> {
        let entry = self.entry(room_id).await?;
        let lifecycle = entry.lifecycle();
        let lease = lifecycle.begin()?;
        let room = entry.room();
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
            .map(|entry| entry.room())
            .collect()
    }

    async fn entry_stats_snapshot(
        &self,
        entry: RoomDirectoryEntry,
        media_transport: &MediaTransport,
    ) -> RuntimeRoomStatsSnapshot {
        let room = entry.room();
        let users_stats = room.session_stats_snapshot(media_transport).await;
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
            room.abandon_cleanup_retries_for_shutdown();
            self.metrics.add_active_rooms(-1);
            self.diagnostics.forget_room(room_id);
        }
    }
}

/// accepted manager work against one current room entry
///
/// this bundles the room with the lease that must be finished after caller
/// work runs, so removal coordination cannot be skipped by accident
#[derive(Debug)]
struct CurrentRoomMutation {
    /// room pointer that was current when the lifecycle lease was accepted
    room: Arc<Room>,
    /// current-room lease that keeps empty-room removal waiting for this work
    lease: RoomLifecycleLease,
}

impl CurrentRoomMutation {
    /// finish accepted room work and report whether this caller owns removal
    ///
    /// the caller computes `room_can_be_removed` after async effects finish so
    /// cleanup retry state is observed at the final decision point
    fn finish(self, remove_if_empty: bool, room_can_be_removed: bool) -> bool {
        self.lease.finish(remove_if_empty, room_can_be_removed)
    }
}
