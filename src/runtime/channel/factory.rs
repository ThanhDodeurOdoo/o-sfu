//! Channel construction for rooms that are new to the runtime directory.
//!
//! `ChannelManager` owns idempotent lookup, directory publication, metrics and
//! creation diagnostics. This module owns the cold-path allocation step used
//! after lookup misses, before the new room is visible to other runtime
//! entrypoints.
//!
//! A factory-created channel receives fresh process-local placement, the
//! immutable runtime policy selected at boot and shared observability,
//! recording and metrics services. It does not register the channel or emit
//! creation events.

use std::sync::{Arc, Mutex, PoisonError};

use o_sfu_router::RouterId;

use super::{Channel, ChannelConfig, ChannelRuntimeContext, ChannelRuntimePolicy};
use crate::runtime::{
    ChannelInstanceId, diagnostics::DiagnosticsStore, metrics::RuntimeMetrics, recording::MediaTap,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChannelCreationIntent {
    /// Compatibility-facing room identity used by manager lookup and the
    /// channel definition.
    issuer: String,
    /// Optional room key captured from the first create request.
    ///
    /// Later calls for the same issuer reuse the already-created channel, so
    /// this value is immutable for the room lifetime.
    key: Option<String>,
    /// Per-room compatibility knobs attached to the created channel.
    ///
    /// This is copied into the channel definition once. Repeated create calls
    /// for the same issuer do not replace it.
    config: ChannelConfig,
}

impl ChannelCreationIntent {
    /// Captures one channel creation request as owned runtime input.
    ///
    /// This is cold-path only. Cloning the small create parameters keeps the
    /// factory independent from HTTP or websocket request lifetimes.
    #[must_use]
    pub(crate) fn new(issuer: &str, key: Option<&str>, config: &ChannelConfig) -> Self {
        Self {
            issuer: issuer.to_owned(),
            key: key.map(str::to_owned),
            config: config.clone(),
        }
    }
}

/// Monotonic placement counters assigned by the current process.
///
/// Channel instance ids and router ids are allocated under one lock so every
/// new channel receives one coherent runtime placement. The counters are not a
/// distributed identity source and must not leak into the Odoo-facing room
/// contract.
#[derive(Debug)]
struct ChannelRuntimeAllocator {
    next_channel_instance_id: u64,
    next_router_id: u64,
}

/// Cold-path constructor for channels that are new to the directory.
///
/// `ChannelFactory` keeps runtime-wide creation dependencies behind the manager
/// so `ChannelManager::serve_channel` can focus on idempotent lookup and
/// publication. Each call to [`Self::create`] returns an unpublished
/// [`Channel`] with fresh process-local placement. The caller must insert it in
/// the directory before exposing it to other runtime entrypoints.
///
/// # Concurrency model
///
/// The factory is shared by async request tasks, but creation does not await. A
/// small mutex protects the placement counters. The lock is held only while ids
/// are reserved, then channel construction continues without it.
///
/// # Performance
///
/// Channel creation is a cold-path operation. It may clone small request
/// strings and `Arc` service handles, but it must not participate in media
/// packet forwarding.
#[derive(Debug)]
pub(crate) struct ChannelFactory {
    /// Worker shard count used for deterministic per-channel transport
    /// placement.
    ///
    /// This is normalized to at least one at construction so placement never
    /// needs a zero-worker branch.
    media_worker_count: usize,
    /// Runtime-wide room rules cloned into each channel.
    ///
    /// Keeping the policy here makes every room start from the validated
    /// boot-time policy while still letting the channel own its copy.
    runtime_policy: ChannelRuntimePolicy,
    /// Shared diagnostics sink passed into every channel.
    ///
    /// Channel creation events are emitted by the manager after directory
    /// publication, not by this factory.
    diagnostics: Arc<DiagnosticsStore>,
    /// Shared packet tap used by channel-owned recording services.
    ///
    /// The factory wires the service dependency, while each channel decides
    /// when recording state should subscribe to its instance id.
    recording_media_tap: Arc<MediaTap>,
    /// Process metrics handle passed into channel-owned services.
    ///
    /// Keeping this as an injected dependency avoids global metric lookup
    /// during room construction.
    metrics: Arc<RuntimeMetrics>,
    /// Serialized allocator for process-local placement ids.
    ///
    /// This keeps concurrent create requests from receiving the same runtime
    /// placement.
    allocator: Mutex<ChannelRuntimeAllocator>,
}

impl ChannelFactory {
    /// Builds the factory for one [`ChannelManager`](super::ChannelManager)
    /// lifetime.
    ///
    /// `media_worker_count` is clamped to one so a runtime with missing or
    /// invalid worker configuration still produces addressable transport
    /// placement.
    #[must_use]
    pub(crate) fn new(
        media_worker_count: usize,
        runtime_policy: ChannelRuntimePolicy,
        recording_media_tap: Arc<MediaTap>,
        diagnostics: Arc<DiagnosticsStore>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            media_worker_count: media_worker_count.max(1),
            runtime_policy,
            diagnostics,
            recording_media_tap,
            metrics,
            allocator: Mutex::new(ChannelRuntimeAllocator {
                next_channel_instance_id: 0,
                next_router_id: 0,
            }),
        }
    }

    /// Creates an unpublished channel from a manager intent.
    ///
    /// The returned `Arc` is not registered in the process directory, does not
    /// increment active-channel metrics and does not emit creation diagnostics.
    /// `ChannelManager` performs those steps after the directory write, which
    /// keeps publication and observability in one place.
    #[must_use]
    pub(crate) fn create(&self, intent: ChannelCreationIntent) -> Arc<Channel> {
        let runtime_context = self.allocate_runtime_context();
        Arc::new(Channel::new(
            runtime_context,
            self.runtime_policy.clone(),
            intent.issuer,
            intent.key,
            intent.config,
            Arc::clone(&self.diagnostics),
            Arc::clone(&self.recording_media_tap),
            Arc::clone(&self.metrics),
        ))
    }

    /// Reserves runtime-local placement for one new channel.
    ///
    /// The mutex is poisoned-tolerant because placement allocation has no
    /// partial side effect beyond the counters themselves. Recovering the inner
    /// value keeps later channel creation possible after an unrelated panic.
    fn allocate_runtime_context(&self) -> ChannelRuntimeContext {
        let (channel_instance_id, router_id) = {
            let mut allocator = self
                .allocator
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let channel_instance_id =
                ChannelInstanceId::allocate(&mut allocator.next_channel_instance_id);
            let router_id = RouterId(allocator.next_router_id);
            allocator.next_router_id = allocator.next_router_id.saturating_add(1);
            drop(allocator);
            (channel_instance_id, router_id)
        };
        ChannelRuntimeContext {
            instance: channel_instance_id,
            media_worker: self.media_worker_id_for_channel_instance(channel_instance_id),
            router: router_id,
        }
    }

    /// Maps channel instance ids onto media workers with stable modulo
    /// placement.
    ///
    /// The mapping is intentionally simple because channel creation is
    /// process-local and cold-path. Keeping it deterministic lets diagnostics
    /// and tests infer the worker from the instance id while leaving future
    /// topology-aware placement behind this factory boundary.
    fn media_worker_id_for_channel_instance(
        &self,
        channel_instance_id: ChannelInstanceId,
    ) -> usize {
        let media_worker_count_u64 = u64::try_from(self.media_worker_count).unwrap_or(1);
        usize::try_from(channel_instance_id.as_u64() % media_worker_count_u64).unwrap_or(0)
    }
}
