//! Room construction for rooms that are new to the runtime directory.
//!
//! `RoomManager` owns idempotent lookup, directory publication, metrics and
//! creation diagnostics. This module contain the cold-path allocation step used
//! after lookup misses, before the new room is visible to other runtime
//! entrypoints.
//!
//! A factory-created room receives fresh process-local placement, the immutable
//! runtime policy selected at boot plus shared observability, recording and
//! metrics services. It does not register the room or emit creation events.
//!
//! Same-room worker placement is intentionally not decided here. The factory
//! gives a room its stable instance id and primary router id, while
//! `RoomManager::join_user` assigns workers from live load snapshots when
//! sessions arrive.

use std::sync::{Arc, Mutex};

use o_sfu_router::RouterId;

use super::{LocalRouterRuntimeContext, Room, RoomConfig, RoomRuntimeContext, RoomRuntimePolicy};
use crate::runtime::{
    RoomInstanceId, diagnostics::DiagnosticsStore, metrics::RuntimeMetrics,
    packet_sink_registry::RoomPacketSinkRegistry, sync::lock_unpoisoned,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoomCreationIntent {
    /// Compatibility-facing room identity used by manager lookup and the
    /// room definition.
    issuer: String,
    /// Room key captured from the first create request.
    ///
    /// Later calls for the same issuer reuse the already-created room, so
    /// this value is immutable for the room lifetime.
    key: String,
    /// Per-room compatibility knobs attached to the created room.
    ///
    /// This is copied into the room definition once. Repeated create calls
    /// for the same issuer do not replace it.
    config: RoomConfig,
}

impl RoomCreationIntent {
    /// Captures one room creation request as owned runtime input.
    ///
    /// This is cold-path only. Cloning the small create parameters keeps the
    /// factory independent from HTTP or websocket request lifetimes.
    #[must_use]
    pub(crate) fn new(issuer: &str, key: &str, config: &RoomConfig) -> Self {
        Self {
            issuer: issuer.to_owned(),
            key: key.to_owned(),
            config: config.clone(),
        }
    }
}

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
///
/// # Concurrency
///
/// The factory is shared by async request tasks, but creation does not await. A
/// small mutex protects the placement counters. The lock is held only while ids
/// are reserved, then room construction continues without it.
///
/// # Performance
///
/// Room creation is a cold-path operation. It may clone small request
/// strings and `Arc` service handles, but it must not participate in media
/// packet forwarding.
#[derive(Debug)]
pub(crate) struct RoomFactory {
    /// Runtime-wide room rules cloned into each room.
    ///
    /// Keeping the policy here makes every room start from the validated
    /// boot-time policy while still letting the room own its copy.
    runtime_policy: RoomRuntimePolicy,
    /// Shared diagnostics sink passed into every room.
    ///
    /// Room creation events are emitted by the manager after directory
    /// publication, not by this factory.
    diagnostics: Arc<DiagnosticsStore>,
    /// Shared packet-sink registry used by room-owned recording services.
    ///
    /// The factory wires the service dependency, while each room decides
    /// when recording state should subscribe to its instance id.
    packet_sink_registry: Arc<RoomPacketSinkRegistry>,
    /// Process metrics handle passed into room-owned services.
    ///
    /// Keeping this as an injected dependency avoids global metric lookup
    /// during room construction.
    metrics: Arc<RuntimeMetrics>,
    /// Serialized allocator for process-local placement ids.
    ///
    /// This keeps concurrent create requests from receiving the same runtime
    /// placement.
    allocator: Mutex<RoomRuntimeAllocator>,
}

impl RoomFactory {
    /// Builds the factory for one [`RoomManager`](super::RoomManager)
    /// lifetime.
    ///
    #[must_use]
    pub(crate) fn new(
        runtime_policy: RoomRuntimePolicy,
        packet_sink_registry: Arc<RoomPacketSinkRegistry>,
        diagnostics: Arc<DiagnosticsStore>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            runtime_policy,
            diagnostics,
            packet_sink_registry,
            metrics,
            allocator: Mutex::new(RoomRuntimeAllocator {
                next_room_instance_id: 0,
                next_router_id: 0,
            }),
        }
    }

    /// Creates an unpublished room from a manager intent.
    ///
    /// The returned `Arc` is not registered in the process directory, does not
    /// increment active-room metrics and does not emit creation diagnostics.
    /// `RoomManager` performs those steps after the directory write, which
    /// keeps publication and observability in one place.
    #[must_use]
    pub(crate) fn create(&self, intent: RoomCreationIntent) -> Arc<Room> {
        let runtime_context = self.allocate_runtime_context();
        Arc::new(Room::new(
            &runtime_context,
            self.runtime_policy.clone(),
            intent.issuer,
            intent.key,
            intent.config,
            Arc::clone(&self.diagnostics),
            Arc::clone(&self.packet_sink_registry),
            Arc::clone(&self.metrics),
        ))
    }

    /// Reserves runtime-local placement for one new room.
    ///
    /// The primary router id is allocated here, but its media worker is a
    /// placeholder until the first session join assigns the room to a live
    /// worker from load data.
    ///
    /// The mutex is poisoned-tolerant because placement allocation has no
    /// partial side effect beyond the counters themselves. Recovering the inner
    /// value keeps later room creation possible after an unrelated panic.
    fn allocate_runtime_context(&self) -> RoomRuntimeContext {
        let (room_instance_id, primary) = {
            let mut allocator = lock_unpoisoned(&self.allocator);
            let room_instance_id = RoomInstanceId::allocate(&mut allocator.next_room_instance_id);
            let primary = LocalRouterRuntimeContext {
                router: RouterId(allocator.next_router_id),
                media_worker: 0,
            };
            allocator.next_router_id = allocator.next_router_id.saturating_add(1);
            drop(allocator);
            (room_instance_id, primary)
        };
        RoomRuntimeContext::new(room_instance_id, primary, Vec::new())
    }

    pub(super) fn allocate_spillover_router(&self) -> RouterId {
        let mut allocator = lock_unpoisoned(&self.allocator);
        let router = RouterId(allocator.next_router_id);
        allocator.next_router_id = allocator.next_router_id.saturating_add(1);
        router
    }
}
