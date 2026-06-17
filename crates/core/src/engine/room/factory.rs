//! Room construction for rooms that are new to the runtime directory.
//!
//! `RoomManager` owns idempotent lookup, directory publication, metrics and
//! creation diagnostics. This module contains the cold-path allocation step used
//! after lookup misses, before the new room is visible to other runtime
//! entrypoints.
//!
//! A factory-created room receives fresh process-local placement, the immutable
//! runtime policy selected at boot plus shared observability, recording and
//! metrics services. It does not register the room or emit creation events.
//!
//! Same-room worker placement is not decided here. The factory
//! gives a room its stable instance id and primary router id, while
//! `RoomManager::join_user` assigns workers from live load snapshots when
//! sessions arrive.

use std::sync::{Arc, Mutex};

use o_sfu_router::RouterId;

use super::{
    Room, RoomConfig, RoomRuntimeContext, RoomRuntimePolicy,
    init::{RoomInit, RoomServices},
};
use crate::engine::{
    RoomInstanceId, diagnostics::DiagnosticsStore, metrics::RuntimeMetrics, sync::lock_unpoisoned,
};

/// Monotonic placement counters assigned by the current process.
///
/// Room instance ids and router ids are allocated under one lock so every
/// new room receives one coherent runtime placement. The counters are not a
/// distributed identity source and must not leak into the Odoo-facing room
/// contract.
#[derive(Debug)]
struct RoomRuntimeAllocator {
    next_room_instance_id: u64,
    /// Next router id to allocate for room-local topology.
    ///
    /// Room creation consumes one primary router id. Dynamic spillover
    /// placement consumes additional router ids when sessions join.
    next_router_id: u64,
}

/// Cold-path constructor for rooms that are new to the directory.
///
/// `RoomFactory` keeps runtime-wide creation dependencies behind the manager
/// so `RoomManager::serve_room` can focus on idempotent lookup and
/// publication. Each call to [`Self::create`] returns an unpublished
/// [`Room`] with fresh process-local placement. The caller must insert it in
/// the directory before exposing it to other runtime entrypoints.
#[derive(Debug)]
pub(crate) struct RoomFactory {
    /// Runtime-wide room rules cloned into each room.
    ///
    /// Keeping the policy here makes every room start from the validated
    /// boot-time policy while still letting the room own its copy.
    runtime_policy: RoomRuntimePolicy,
    /// Shared room services cloned into each room.
    services: RoomServices,
    /// Serialized allocator for process-local placement ids.
    ///
    /// This keeps concurrent create requests from receiving the same runtime
    /// placement.
    allocator: Mutex<RoomRuntimeAllocator>,
}

impl RoomFactory {
    /// Builds the factory for one [`RoomManager`](super::RoomManager) lifetime.
    #[must_use]
    pub(crate) fn new(
        runtime_policy: RoomRuntimePolicy,
        diagnostics: Arc<DiagnosticsStore>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            runtime_policy,
            services: RoomServices::new(diagnostics, metrics),
            allocator: Mutex::new(RoomRuntimeAllocator {
                next_room_instance_id: 0,
                next_router_id: 0,
            }),
        }
    }

    /// Creates an unpublished room from one manager lookup miss.
    ///
    /// The returned `Arc` is not registered in the process directory, does not
    /// increment active-room metrics and does not emit creation diagnostics.
    /// `RoomManager` performs those steps after the directory write, which
    /// keeps publication and observability in one place.
    #[must_use]
    pub(crate) fn create(&self, issuer: &str, key: &str, config: &RoomConfig) -> Arc<Room> {
        Arc::new(Room::new(RoomInit {
            runtime_context: self.allocate_runtime_context(),
            runtime_policy: self.runtime_policy.clone(),
            issuer: issuer.to_owned(),
            key: key.to_owned(),
            config: config.clone(),
            services: self.services.clone(),
        }))
    }

    /// Reserves runtime-local placement for one new room.
    ///
    /// The primary router id is allocated here, but worker placement remains
    /// unset until the first session join assigns the room from live load data.
    ///
    /// The mutex is poisoned-tolerant because placement allocation has no
    /// partial side effect beyond the counters themselves. Recovering the inner
    /// value keeps later room creation possible after an unrelated panic.
    fn allocate_runtime_context(&self) -> RoomRuntimeContext {
        let (room_instance_id, primary_router_id) = {
            let mut allocator = lock_unpoisoned(&self.allocator);
            let room_instance_id = RoomInstanceId::allocate(&mut allocator.next_room_instance_id);
            let primary_router_id = RouterId(allocator.next_router_id);
            allocator.next_router_id = allocator.next_router_id.saturating_add(1);
            drop(allocator);
            (room_instance_id, primary_router_id)
        };
        RoomRuntimeContext::new_unassigned(room_instance_id, primary_router_id)
    }

    /// reserve a new process-unique identifier for a spillover router
    ///
    /// this provides the room engine with a thread-safe way to allocate new router
    /// identities on the fly when media load exceeds the primary worker capacity.
    /// the allocator lock is held only long enough to increment the counter, keeping
    /// the cold-path creation from blocking active request loops
    pub(super) fn allocate_spillover_router(&self) -> RouterId {
        let mut allocator = lock_unpoisoned(&self.allocator);
        let router_id = RouterId(allocator.next_router_id);
        allocator.next_router_id = allocator.next_router_id.saturating_add(1);
        router_id
    }
}
