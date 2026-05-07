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
//! Same-room spillover is intentionally decided here. The factory reserves the
//! complete local router placement set before the room is visible, while
//! `RoomTopology` later decides which reserved spillover routers need live
//! router state.

use std::sync::{Arc, Mutex, PoisonError};

use o_sfu_router::RouterId;

use super::{LocalRouterRuntimeContext, Room, RoomConfig, RoomRuntimeContext, RoomRuntimePolicy};
use crate::runtime::{
    RoomInstanceId, diagnostics::DiagnosticsStore, metrics::RuntimeMetrics,
    packet_sink_registry::RoomPacketSinkRegistry,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoomCreationIntent {
    /// Compatibility-facing room identity used by manager lookup and the
    /// room definition.
    issuer: String,
    /// Optional room key captured from the first create request.
    ///
    /// Later calls for the same issuer reuse the already-created room, so
    /// this value is immutable for the room lifetime.
    key: Option<String>,
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
    pub(crate) fn new(issuer: &str, key: Option<&str>, config: &RoomConfig) -> Self {
        Self {
            issuer: issuer.to_owned(),
            key: key.map(str::to_owned),
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
    /// Next router id to reserve for room-local topology.
    ///
    /// Spillover rooms advance this counter by the number of reserved local
    /// router placements, not by the number of routers attached at creation.
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
    /// Worker shard count used for deterministic room transport placement.
    ///
    /// This is normalized to at least one at construction so placement never
    /// needs a zero-worker branch. A room's local router cap is also bounded
    /// by this value so each reserved router has one local transport owner.
    media_worker_count: usize,
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
    /// `media_worker_count` is clamped to one so a runtime with missing or
    /// invalid worker configuration still produces addressable transport
    /// placement.
    #[must_use]
    pub(crate) fn new(
        media_worker_count: usize,
        runtime_policy: RoomRuntimePolicy,
        packet_sink_registry: Arc<RoomPacketSinkRegistry>,
        diagnostics: Arc<DiagnosticsStore>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            media_worker_count: media_worker_count.max(1),
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
    /// The primary placement is always first. Additional placements are local
    /// spillover candidates, distributed across consecutive media workers from
    /// the primary worker. `RoomTopology` may attach those routers lazily, but
    /// their ids and worker ownership are fixed here so transport key
    /// derivation can stay lock-free with respect to room state.
    ///
    /// The mutex is poisoned-tolerant because placement allocation has no
    /// partial side effect beyond the counters themselves. Recovering the inner
    /// value keeps later room creation possible after an unrelated panic.
    fn allocate_runtime_context(&self) -> RoomRuntimeContext {
        let (room_instance_id, primary, spillover_routers) = {
            let mut allocator = self
                .allocator
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let room_instance_id = RoomInstanceId::allocate(&mut allocator.next_room_instance_id);
            let local_router_count = self
                .runtime_policy
                .room_sharding_policy
                .max_local_routers()
                .min(self.media_worker_count)
                .max(1);
            let primary_media_worker = self.media_worker_id_for_room_instance(room_instance_id);
            let primary = LocalRouterRuntimeContext {
                router: RouterId(allocator.next_router_id),
                media_worker: primary_media_worker,
            };
            allocator.next_router_id = allocator.next_router_id.saturating_add(1);
            let mut spillover_routers = Vec::with_capacity(local_router_count.saturating_sub(1));
            for offset in 1..local_router_count {
                spillover_routers.push(LocalRouterRuntimeContext {
                    router: RouterId(allocator.next_router_id),
                    media_worker: (primary_media_worker + offset) % self.media_worker_count,
                });
                allocator.next_router_id = allocator.next_router_id.saturating_add(1);
            }
            drop(allocator);
            (room_instance_id, primary, spillover_routers)
        };
        RoomRuntimeContext::new(room_instance_id, primary, spillover_routers)
    }

    /// Maps room instance ids onto media workers with stable modulo
    /// placement.
    ///
    /// The mapping is intentionally simple because room creation is
    /// process-local and cold-path. Keeping it deterministic lets diagnostics
    /// and tests infer the worker from the instance id while leaving future
    /// topology-aware placement behind this factory boundary.
    fn media_worker_id_for_room_instance(&self, room_instance_id: RoomInstanceId) -> usize {
        let media_worker_count_u64 = u64::try_from(self.media_worker_count).unwrap_or(1);
        usize::try_from(room_instance_id.as_u64() % media_worker_count_u64).unwrap_or(0)
    }
}
