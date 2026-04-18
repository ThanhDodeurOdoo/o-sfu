use std::sync::{Arc, Mutex, PoisonError};

use o_sfu_router::RouterId;

use crate::runtime::metrics::RuntimeMetrics;
use crate::runtime::recording::MediaTap;

use super::{Channel, ChannelConfig, ChannelRuntimeContext, ChannelRuntimePolicy};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChannelCreationIntent {
    issuer: String,
    key: Option<String>,
    config: ChannelConfig,
}

impl ChannelCreationIntent {
    #[must_use]
    pub(crate) fn new(issuer: &str, key: Option<&str>, config: &ChannelConfig) -> Self {
        Self {
            issuer: issuer.to_owned(),
            key: key.map(str::to_owned),
            config: config.clone(),
        }
    }
}

#[derive(Debug)]
struct ChannelRuntimeAllocator {
    next_channel_runtime_id: u64,
    next_router_id: u64,
}

#[derive(Debug)]
pub(crate) struct ChannelFactory {
    media_worker_count: usize,
    runtime_policy: ChannelRuntimePolicy,
    recording_media_tap: Arc<MediaTap>,
    metrics: Arc<RuntimeMetrics>,
    allocator: Mutex<ChannelRuntimeAllocator>,
}

impl ChannelFactory {
    #[must_use]
    pub(crate) fn new(
        media_worker_count: usize,
        runtime_policy: ChannelRuntimePolicy,
        recording_media_tap: Arc<MediaTap>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            media_worker_count: media_worker_count.max(1),
            runtime_policy,
            recording_media_tap,
            metrics,
            allocator: Mutex::new(ChannelRuntimeAllocator {
                next_channel_runtime_id: 0,
                next_router_id: 0,
            }),
        }
    }

    #[must_use]
    pub(crate) fn create(&self, intent: ChannelCreationIntent) -> Arc<Channel> {
        let runtime_context = self.allocate_runtime_context();
        Arc::new(Channel::new(
            runtime_context,
            self.runtime_policy.clone(),
            intent.issuer,
            intent.key,
            intent.config,
            Arc::clone(&self.recording_media_tap),
            Arc::clone(&self.metrics),
        ))
    }

    fn allocate_runtime_context(&self) -> ChannelRuntimeContext {
        let (channel_runtime_id, router_id) = {
            let mut allocator = self
                .allocator
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let channel_runtime_id = allocator.next_channel_runtime_id;
            allocator.next_channel_runtime_id = allocator.next_channel_runtime_id.saturating_add(1);
            let router_id = RouterId(allocator.next_router_id);
            allocator.next_router_id = allocator.next_router_id.saturating_add(1);
            drop(allocator);
            (channel_runtime_id, router_id)
        };
        ChannelRuntimeContext {
            runtime: channel_runtime_id,
            media_worker: self.media_worker_id_for_channel_runtime(channel_runtime_id),
            router: router_id,
        }
    }

    fn media_worker_id_for_channel_runtime(&self, channel_runtime_id: u64) -> usize {
        let media_worker_count_u64 = u64::try_from(self.media_worker_count).unwrap_or(1);
        usize::try_from(channel_runtime_id % media_worker_count_u64).unwrap_or(0)
    }
}
