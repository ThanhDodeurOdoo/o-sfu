#[cfg(any(test, feature = "testing-transport"))]
use std::iter;
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Instant,
};

use o_sfu_router::{MediaCapabilities, MediaKind, MediaStream as RouterRtpParameters};
use str0m::media::MediaKind as Str0mMediaKind;

use crate::{
    runtime::{
        RoomInstanceId,
        rtc_adapter::{RelayCleanup, RtcTransportAdapter, client_rtp_capabilities_from_answer},
        transport_adapter::config::RtcTransportAdapterShardSetConfig,
    },
    transport::{
        ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, AppliedSessionAnswer,
        ConsumerPacketGateUpdate, ReceiverBandwidthSnapshot, SessionOffer, SourcePacketGate,
        SourcePolicySignal, SourcePolicyUpdateSubscription, TransportAdapterError,
        TransportBitrateSnapshot, TransportMediaId, TransportSessionHealth, TransportSessionKey,
    },
};

#[derive(Debug)]
/// Process-local collection of RTC transport shards keyed by media-worker id.
///
/// The runtime-facing transport selector stays above this type: `ShardSet`
/// only owns shard assignment plus cross-shard relay cleanup fan-out.
pub struct RtcTransportAdapterShardSet {
    primary_shard: Arc<RtcTransportAdapter>,
    extra_shards: Vec<Arc<RtcTransportAdapter>>,
    source_policy_signal: Arc<SourcePolicySignal>,
}

impl RtcTransportAdapterShardSet {
    pub(super) fn new(config: &RtcTransportAdapterShardSetConfig) -> Self {
        let source_policy_signal = Arc::new(SourcePolicySignal::default());
        let Some(shard_ranges) = config
            .adapter_config()
            .rtc_port_range()
            .split_for_workers(config.worker_count())
        else {
            return Self {
                primary_shard: Arc::new(RtcTransportAdapter::new(
                    config.adapter_config(),
                    config.adapter_deps(),
                    Arc::clone(&source_policy_signal),
                )),
                extra_shards: Vec::new(),
                source_policy_signal,
            };
        };
        let mut shard_ranges = shard_ranges.into_iter();
        let Some(primary_range) = shard_ranges.next() else {
            return Self {
                primary_shard: Arc::new(RtcTransportAdapter::new(
                    config.adapter_config(),
                    config.adapter_deps(),
                    Arc::clone(&source_policy_signal),
                )),
                extra_shards: Vec::new(),
                source_policy_signal,
            };
        };
        Self {
            primary_shard: Arc::new(RtcTransportAdapter::new(
                &config.shard_config_with_port_range(primary_range),
                config.adapter_deps(),
                Arc::clone(&source_policy_signal),
            )),
            extra_shards: shard_ranges
                .map(|range| {
                    Arc::new(RtcTransportAdapter::new(
                        &config.shard_config_with_port_range(range),
                        config.adapter_deps(),
                        Arc::clone(&source_policy_signal),
                    ))
                })
                .collect(),
            source_policy_signal,
        }
    }

    pub(super) fn shard_for_user(
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
        let consumer_shard = self.shard_for_user(consumer_session_key);
        let source_shard = self.shard_for_user(source_session_key);
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
            let source_shard = self.shard_for_user(cleanup.source_session_key());
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
                .entry(self.shard_index_for_user(session_key))
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

    pub(super) fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        let mut keys_by_shard = BTreeMap::<usize, Vec<TransportSessionKey>>::new();
        for session_key in session_keys {
            keys_by_shard
                .entry(self.shard_index_for_user(session_key))
                .or_default()
                .push(session_key.clone());
        }
        let mut snapshot = ReceiverBandwidthSnapshot::default();
        for (shard_index, shard_session_keys) in keys_by_shard {
            let shard = self.shard_for_index(shard_index);
            let shard_snapshot = shard.receiver_bandwidth_snapshot(&shard_session_keys);
            snapshot.per_session.extend(shard_snapshot.per_session);
        }
        snapshot
    }

    pub(super) async fn set_consumer_packet_gates(
        &self,
        updates: &[ConsumerPacketGateUpdate],
    ) -> Vec<Result<(), TransportAdapterError>> {
        let mut results = vec![Err(TransportAdapterError::TransportUnavailable); updates.len()];
        let mut batches = BTreeMap::<ConsumerPacketGateBatchKey, Vec<usize>>::new();
        for (index, update) in updates.iter().enumerate() {
            if update.consumer_session_key().room_instance_id()
                != update.source_session_key().room_instance_id()
            {
                if let Some(result) = results.get_mut(index) {
                    *result = Err(TransportAdapterError::InvalidInput);
                }
                continue;
            }
            let key = ConsumerPacketGateBatchKey {
                shard_index: self.shard_index_for_user(update.consumer_session_key()),
                source_session_key: update.source_session_key().clone(),
                source_transport_media_id: update.source_transport_media_id(),
            };
            batches.entry(key).or_default().push(index);
        }
        for (key, batch) in batches {
            let shard = self.shard_for_index(key.shard_index);
            let update_count = batch.len();
            let batch_results = shard
                .media()
                .set_consumer_packet_gates(
                    &key.source_session_key,
                    key.source_transport_media_id,
                    batch.iter().filter_map(|index| updates.get(*index)),
                )
                .await
                .unwrap_or_else(|error| vec![Err(error); update_count]);
            for (index, result) in batch.into_iter().zip(batch_results) {
                if let Some(stored_result) = results.get_mut(index) {
                    *stored_result = result;
                }
            }
        }
        results
    }

    pub(super) async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        let mut snapshot = self.primary_shard.active_speaker_source_snapshot().await;
        for shard in &self.extra_shards {
            snapshot.extend(shard.active_speaker_source_snapshot().await);
        }
        snapshot.sort_by_key(|source| {
            (
                Reverse(source.observed_at()),
                source.transport_media_id().as_u64(),
            )
        });
        snapshot.dedup_by_key(|source| source.transport_media_id());
        snapshot
    }

    pub(super) async fn active_speaker_diagnostic_snapshot(
        &self,
    ) -> Vec<ActiveSpeakerSourceDiagnostic> {
        let mut snapshot = self
            .primary_shard
            .active_speaker_diagnostic_snapshot()
            .await;
        for shard in &self.extra_shards {
            snapshot.extend(shard.active_speaker_diagnostic_snapshot().await);
        }
        snapshot.sort_by_key(|source| source.transport_media_id().as_u64());
        snapshot.dedup_by_key(|source| source.transport_media_id());
        snapshot
    }

    pub(super) async fn next_active_speaker_deadline(&self) -> Option<Instant> {
        let mut next_deadline = self.primary_shard.next_active_speaker_deadline().await;
        for shard in &self.extra_shards {
            let shard_deadline = shard.next_active_speaker_deadline().await;
            next_deadline = match (next_deadline, shard_deadline) {
                (Some(current), Some(candidate)) => Some(current.min(candidate)),
                (Some(current), None) => Some(current),
                (None, Some(candidate)) => Some(candidate),
                (None, None) => None,
            };
        }
        next_deadline
    }

    pub(super) async fn expired_active_speaker_room_instance_ids(
        &self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId> {
        let mut room_instance_ids = self
            .primary_shard
            .expired_active_speaker_room_instance_ids(now)
            .await;
        for shard in &self.extra_shards {
            room_instance_ids.extend(shard.expired_active_speaker_room_instance_ids(now).await);
        }
        room_instance_ids
    }

    pub(super) fn source_policy_subscription(&self) -> SourcePolicyUpdateSubscription {
        self.source_policy_signal.subscribe()
    }

    pub(super) async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.shard_for_user(session_key)
            .negotiation()
            .create_initial_session_offer(session_key)
            .await
    }

    pub(super) async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.shard_for_user(session_key)
            .negotiation()
            .create_session_renegotiation_offer(session_key)
            .await
    }

    pub(super) async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, TransportAdapterError> {
        self.shard_for_user(session_key)
            .negotiation()
            .apply_session_answer(session_key, answer_sdp)
            .await
    }

    pub(super) fn negotiated_client_rtp_capabilities(
        answer_sdp: &str,
        _offered_router_capabilities: &MediaCapabilities,
    ) -> Result<MediaCapabilities, TransportAdapterError> {
        client_rtp_capabilities_from_answer(answer_sdp).ok_or(TransportAdapterError::InvalidInput)
    }

    pub(super) async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        let session_shard = self.shard_for_user(session_key);
        let close_outcome = session_shard
            .users()
            .close_session_with_outcome(session_key)
            .await?;
        self.release_relay_cleanup(&session_shard, close_outcome.relay_cleanup());
        Ok(())
    }

    pub(super) async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        let session_shard = self.shard_for_user(session_key);
        let remove_outcome = session_shard
            .media()
            .remove_media_with_outcome(session_key, transport_media_id)
            .await?;
        if let Some(cleanup) = remove_outcome.relay_cleanup() {
            let relay_cleanup = [cleanup.clone()];
            self.release_relay_cleanup(&session_shard, &relay_cleanup);
        }
        Ok(())
    }

    pub(super) async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.shard_for_user(session_key)
            .media()
            .add_recv_media(
                session_key,
                signaling_to_str0m_media_kind(media_kind),
                rtp_parameters,
            )
            .await
    }

    pub(super) async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_media_id: TransportMediaId,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        ensure_same_room_instance(consumer_session_key, source_session_key)?;
        let relay_route = self.relay_registration_shards(consumer_session_key, source_session_key);
        let remote_source_control = relay_route
            .as_ref()
            .map(|(source_shard, consumer_shard)| {
                source_shard
                    .media()
                    .remote_source_control(consumer_shard.as_ref())
            })
            .transpose()?;
        if let Some((source_shard, consumer_shard)) = &relay_route {
            source_shard
                .media()
                .activate_relay_route(source_media_id, consumer_shard.as_ref())?;
        }
        let consumer_shard = self.shard_for_user(consumer_session_key);
        let add_result = consumer_shard
            .media()
            .add_send_media(
                consumer_session_key,
                signaling_to_str0m_media_kind(media_kind),
                source_session_key,
                source_media_id,
                remote_source_control,
                consumer_rtp_parameters,
            )
            .await;
        if let Some((source_shard, consumer_shard)) = relay_route {
            if add_result.is_ok() {
                source_shard.media().set_relay_route_active(
                    source_media_id,
                    consumer_shard.as_ref(),
                    true,
                );
            } else {
                source_shard
                    .media()
                    .deactivate_relay_route(source_media_id, consumer_shard.as_ref());
            }
        }
        add_result
    }

    pub(super) async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.shard_for_user(session_key)
            .media()
            .set_producer_active(session_key, transport_media_id, active)
            .await
    }

    pub(super) async fn set_consumer_active(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        ensure_same_room_instance(consumer_session_key, source_session_key)?;
        self.shard_for_user(consumer_session_key)
            .media()
            .set_consumer_active(
                consumer_session_key,
                consumer_transport_media_id,
                source_session_key,
                source_transport_media_id,
                active,
            )
            .await
    }

    pub(super) async fn set_consumer_packet_gate(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: SourcePacketGate,
    ) -> Result<(), TransportAdapterError> {
        ensure_same_room_instance(consumer_session_key, source_session_key)?;
        self.shard_for_user(consumer_session_key)
            .media()
            .set_consumer_packet_gate(
                consumer_session_key,
                consumer_transport_media_id,
                source_session_key,
                source_transport_media_id,
                packet_gate,
            )
            .await
    }

    pub(super) async fn request_consumer_keyframe(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        ensure_same_room_instance(consumer_session_key, source_session_key)?;
        self.shard_for_user(consumer_session_key)
            .media()
            .request_consumer_keyframe(
                consumer_session_key,
                consumer_transport_media_id,
                source_session_key,
                source_transport_media_id,
            )
            .await
    }

    pub(super) async fn transport_media_mid(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Option<String> {
        self.shard_for_user(session_key)
            .media()
            .transport_media_mid(transport_media_id)
            .await
            .ok()
            .flatten()
    }

    pub(super) fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        self.shard_for_user(session_key)
            .observability()
            .session_transport_health(session_key)
    }

    fn shard_index_for_user(&self, session_key: &TransportSessionKey) -> usize {
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

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn all_shards(&self) -> impl Iterator<Item = &Arc<RtcTransportAdapter>> {
        iter::once(&self.primary_shard).chain(self.extra_shards.iter())
    }
}

fn ensure_same_room_instance(
    consumer_session_key: &TransportSessionKey,
    source_session_key: &TransportSessionKey,
) -> Result<(), TransportAdapterError> {
    if consumer_session_key.room_instance_id() == source_session_key.room_instance_id() {
        return Ok(());
    }
    Err(TransportAdapterError::InvalidInput)
}

fn signaling_to_str0m_media_kind(kind: MediaKind) -> Str0mMediaKind {
    match kind {
        MediaKind::Audio => Str0mMediaKind::Audio,
        MediaKind::Video => Str0mMediaKind::Video,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ConsumerPacketGateBatchKey {
    shard_index: usize,
    source_session_key: TransportSessionKey,
    source_transport_media_id: TransportMediaId,
}
