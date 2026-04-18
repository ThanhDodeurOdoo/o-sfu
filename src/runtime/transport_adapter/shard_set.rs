#[cfg(test)]
use std::iter;
use std::{cmp::Reverse, collections::BTreeMap, sync::Arc};

use crate::runtime::rtc_adapter::{RelayCleanup, RtcTransportAdapter};
use crate::runtime::transport_adapter::config::RtcTransportAdapterShardSetConfig;
use crate::runtime::transport_adapter::types::{
    ActiveSpeakerSource, TransportBitrateSnapshot, TransportSessionKey,
};

#[derive(Debug)]
/// Process-local collection of RTC transport shards keyed by media-worker id.
///
/// The runtime-facing transport selector stays above this type; `ShardSet`
/// only owns shard assignment plus cross-shard relay cleanup fan-out.
pub(crate) struct RtcTransportAdapterShardSet {
    primary_shard: Arc<RtcTransportAdapter>,
    extra_shards: Vec<Arc<RtcTransportAdapter>>,
}

impl RtcTransportAdapterShardSet {
    pub(super) fn new(config: &RtcTransportAdapterShardSetConfig) -> Self {
        let Some(shard_ranges) = config
            .adapter_config()
            .rtc_port_range()
            .split_for_workers(config.worker_count())
        else {
            return Self {
                primary_shard: Arc::new(RtcTransportAdapter::new(config.adapter_config())),
                extra_shards: Vec::new(),
            };
        };
        let mut shard_ranges = shard_ranges.into_iter();
        let Some(primary_range) = shard_ranges.next() else {
            return Self {
                primary_shard: Arc::new(RtcTransportAdapter::new(config.adapter_config())),
                extra_shards: Vec::new(),
            };
        };
        Self {
            primary_shard: Arc::new(RtcTransportAdapter::new(
                &config.shard_config_with_port_range(primary_range),
            )),
            extra_shards: shard_ranges
                .map(|range| {
                    Arc::new(RtcTransportAdapter::new(
                        &config.shard_config_with_port_range(range),
                    ))
                })
                .collect(),
        }
    }

    pub(super) fn shard_for_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Arc<RtcTransportAdapter> {
        self.shard_for_media_worker_id(session_key.media_worker_id())
    }

    pub(super) fn relay_registration_shards(
        &self,
        consumer_session_key: &TransportSessionKey,
        source_session_key: &TransportSessionKey,
    ) -> Option<(Arc<RtcTransportAdapter>, Arc<RtcTransportAdapter>)> {
        let consumer_shard = self.shard_for_session(consumer_session_key);
        let source_shard = self.shard_for_session(source_session_key);
        if Arc::ptr_eq(&consumer_shard, &source_shard) {
            return None;
        }
        Some((source_shard, consumer_shard))
    }

    pub(super) fn release_relay_cleanup(
        &self,
        target_shard: &Arc<RtcTransportAdapter>,
        relay_cleanup: &[RelayCleanup],
    ) {
        for cleanup in relay_cleanup {
            let source_shard = self.shard_for_session(cleanup.source_session_key());
            if Arc::ptr_eq(&source_shard, target_shard) {
                continue;
            }
            source_shard
                .media()
                .deactivate_relay_route(cleanup.source_transport_media_id(), target_shard.as_ref());
        }
    }

    pub(super) fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        let mut keys_by_shard = BTreeMap::<usize, Vec<TransportSessionKey>>::new();
        for session_key in session_keys {
            keys_by_shard
                .entry(self.shard_index_for_session(session_key))
                .or_default()
                .push(session_key.clone());
        }
        let mut snapshot = TransportBitrateSnapshot::default();
        for (shard_index, shard_session_keys) in keys_by_shard {
            let shard = self.shard_for_index(shard_index);
            let shard_snapshot = shard.transport_bitrate_snapshot(&shard_session_keys);
            snapshot.total = snapshot.total.saturating_add(shard_snapshot.total);
            snapshot.per_media.extend(shard_snapshot.per_media);
        }
        snapshot
    }

    pub(super) async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        let mut snapshot = self.primary_shard.active_speaker_source_snapshot().await;
        for shard in &self.extra_shards {
            snapshot.extend(shard.active_speaker_source_snapshot().await);
        }
        snapshot.sort_by_key(|source| Reverse(source.observed_at()));
        snapshot.dedup_by_key(|source| source.transport_media_id());
        snapshot
    }

    fn shard_index_for_session(&self, session_key: &TransportSessionKey) -> usize {
        self.shard_index_for_media_worker_id(session_key.media_worker_id())
    }

    fn shard_for_media_worker_id(&self, media_worker_id: usize) -> Arc<RtcTransportAdapter> {
        self.shard_for_index(self.shard_index_for_media_worker_id(media_worker_id))
    }

    fn shard_index_for_media_worker_id(&self, media_worker_id: usize) -> usize {
        let shard_count = self.extra_shards.len().saturating_add(1);
        media_worker_id % shard_count
    }

    fn shard_for_index(&self, shard_index: usize) -> Arc<RtcTransportAdapter> {
        if shard_index == 0 {
            return Arc::clone(&self.primary_shard);
        }
        self.extra_shards
            .get(shard_index.saturating_sub(1))
            .cloned()
            .unwrap_or_else(|| Arc::clone(&self.primary_shard))
    }

    #[cfg(test)]
    pub(super) fn all_shards(&self) -> impl Iterator<Item = &Arc<RtcTransportAdapter>> {
        iter::once(&self.primary_shard).chain(self.extra_shards.iter())
    }
}
