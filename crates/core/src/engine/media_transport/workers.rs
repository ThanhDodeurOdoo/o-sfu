//! RTC worker ownership below the media transport boundary.
//!
//! `MediaTransport` owns the process-local RTC worker topology, maps each
//! transport session to the worker selected by its media-worker id and
//! coordinates cross-worker relay cleanup. Packet-loop hot paths live inside
//! `engine::media_transport::rtc`.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Instant,
};

use str0m::media::MediaKind as Str0mMediaKind;

use super::rtc::{ConsumerPacketGateCommand, RtcMediaControlCommand, RtcWorker, RtcWorkerCommand};
use crate::engine::{
    MediaWorkerId, RoomInstanceId,
    media_transport::{
        ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, ConsumerPacketGateUpdate,
        MediaTransport, MediaTransportConfig, MediaTransportDeps, ReceiverBandwidthSnapshot,
        ReceiverBweTargetUpdate, SourcePolicySignal, SourcePolicyUpdateSubscription,
        TransportAdapterError, TransportBitrateSnapshot, TransportConsumerRoute, TransportMediaId,
        TransportPlacementPressureSnapshot, TransportQualitySnapshot, TransportRelayRouteAction,
        TransportRelayRouteEffect, TransportSessionHealth, TransportSessionKey,
        TransportSourceActivitySnapshot, TransportSourceKey, TransportWorkerPressureSnapshot,
    },
};

/// Distance between two worker media-id allocation ranges.
///
/// The value only needs to be large enough that one worker cannot exhaust its
/// range during a process lifetime under realistic load. Keeping the gap fixed
/// lets cross-worker route maps keep using `TransportMediaId` as their source
/// key while still avoiding collisions between spillover workers.
const MEDIA_ID_STRIDE: u64 = 1_000_000_000;

type RelayRegistrationWorkers = Option<(Arc<RtcWorker>, Arc<RtcWorker>)>;

impl MediaTransport {
    #[must_use]
    pub fn new(
        transport: &MediaTransportConfig,
        deps: &MediaTransportDeps,
        worker_ranges: Vec<crate::RtcPortRange>,
    ) -> Self {
        let source_policy_signal = Arc::new(SourcePolicySignal::default());
        let workers = worker_ranges
            .into_iter()
            .enumerate()
            .map(|(worker_index, range)| {
                let media_worker_id = MediaWorkerId::from_raw(worker_index);
                Arc::new(RtcWorker::new(
                    &transport.with_rtc_port_range(range),
                    deps,
                    Arc::clone(&source_policy_signal),
                    media_id_base_for_worker_index(worker_index),
                    media_worker_id,
                ))
            })
            .collect::<Vec<_>>()
            .into();
        Self {
            workers,
            source_policy_signal,
        }
    }

    /// Selects the worker that owns a transport session.
    ///
    /// The mapping is deterministic and depends only on the runtime-assigned
    /// media-worker id in the session key. Room and signaling code must not
    /// infer topology from user identity.
    pub(super) fn worker_for_user(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<Arc<RtcWorker>> {
        self.worker_for_index(session_key.media_worker_id().as_usize())
    }

    /// Returns the source and consumer workers needed for cross-worker relay.
    ///
    /// `None` means both sessions are on the same worker and local routing is
    /// enough. A returned pair means the source worker must activate relay
    /// forwarding toward the consumer worker before the consumer route can be
    /// fully installed.
    pub(super) fn relay_registration_workers(
        &self,
        consumer_session_key: &TransportSessionKey,
        source_session_key: &TransportSessionKey,
    ) -> Result<RelayRegistrationWorkers, TransportAdapterError> {
        let consumer_worker = self.require_worker_for_user(consumer_session_key)?;
        let source_worker = self.require_worker_for_user(source_session_key)?;
        if Arc::ptr_eq(&consumer_worker, &source_worker) {
            return Ok(None);
        }
        Ok(Some((source_worker, consumer_worker)))
    }

    /// Builds a best-effort bitrate snapshot across the workers that own the
    /// requested sessions.
    ///
    /// The snapshot is observability data, not authoritative room state. It may
    /// race with packet-loop updates and session cleanup.
    pub(super) fn transport_bitrate_snapshot_from_workers(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        let mut snapshot = TransportBitrateSnapshot::default();
        self.for_session_workers(session_keys, |worker, worker_session_keys| {
            let worker_snapshot = worker.transport_bitrate_snapshot(worker_session_keys);
            snapshot.total = snapshot.total.saturating_add(worker_snapshot.total);
            snapshot.per_media.extend(worker_snapshot.per_media);
        });
        snapshot
    }

    /// Builds a best-effort receiver bandwidth snapshot across RTC workers.
    ///
    /// Room policy uses this as an input to source selection. Missing entries
    /// mean the transport has no current estimate for that session.
    pub(super) fn receiver_bandwidth_snapshot_from_workers(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        let mut snapshot = ReceiverBandwidthSnapshot::default();
        self.for_session_workers(session_keys, |worker, worker_session_keys| {
            let worker_snapshot = worker.receiver_bandwidth_snapshot(worker_session_keys);
            snapshot.per_session.extend(worker_snapshot.per_session);
        });
        snapshot
    }

    pub(super) fn transport_quality_snapshot_from_workers(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportQualitySnapshot {
        let mut snapshot = TransportQualitySnapshot::default();
        self.for_session_workers(session_keys, |worker, worker_session_keys| {
            let worker_snapshot = worker.transport_quality_snapshot(worker_session_keys);
            snapshot.per_session.extend(worker_snapshot.per_session);
        });
        snapshot
    }

    pub(super) async fn source_activity_snapshot_from_workers(
        &self,
        sources: &[TransportSourceKey],
    ) -> TransportSourceActivitySnapshot {
        let mut snapshot = TransportSourceActivitySnapshot::default();
        let mut source_media_by_worker = BTreeMap::<usize, Vec<TransportMediaId>>::new();
        for source in sources {
            let Some(worker_index) = self.worker_index_for_user(source.session_key()) else {
                continue;
            };
            source_media_by_worker
                .entry(worker_index)
                .or_default()
                .push(source.transport_media_id());
        }
        for (worker_index, transport_media_ids) in source_media_by_worker {
            let Some(worker) = self.worker_for_index(worker_index) else {
                continue;
            };
            snapshot.per_media.extend(
                worker
                    .source_activity_snapshot(&transport_media_ids)
                    .await
                    .per_media,
            );
        }
        snapshot
    }

    /// Builds a best-effort placement-pressure snapshot across RTC workers.
    ///
    /// Egress bitrate is additive across selected sessions. Saturation signals
    /// use the hottest owning worker so one overloaded worker can activate
    /// spillover.
    pub(super) fn placement_pressure_snapshot_from_workers(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportPlacementPressureSnapshot {
        let mut snapshot = TransportPlacementPressureSnapshot::default();
        self.for_session_workers(session_keys, |worker, worker_session_keys| {
            snapshot =
                snapshot.merged_with(worker.placement_pressure_snapshot(worker_session_keys));
        });
        snapshot
    }

    pub(super) fn worker_pressure_snapshots_from_workers(
        &self,
    ) -> Vec<TransportWorkerPressureSnapshot> {
        self.workers
            .iter()
            .enumerate()
            .map(|(worker_index, worker)| {
                worker.worker_pressure_snapshot(MediaWorkerId::from_raw(worker_index))
            })
            .collect()
    }

    /// Applies packet-gate updates in worker-local batches.
    ///
    /// The result vector preserves the input order so callers can correlate
    /// failures with planned policy updates. Cross-room updates are rejected
    /// before reaching worker state because a consumer route must never target
    /// media owned by another room instance.
    pub(super) async fn apply_consumer_pkt_gate_batch(
        &self,
        updates: &[ConsumerPacketGateUpdate],
    ) -> Vec<Result<(), TransportAdapterError>> {
        let mut results = vec![Err(TransportAdapterError::TransportUnavailable); updates.len()];
        let mut batches = BTreeMap::<ConsumerPacketGateBatchKey, Vec<usize>>::new();
        for (index, update) in updates.iter().enumerate() {
            let route = update.route();
            if !route.is_single_room() {
                if let Some(result) = results.get_mut(index) {
                    *result = Err(TransportAdapterError::InvalidInput);
                }
                continue;
            }
            let Some(worker_index) = self.worker_index_for_user(route.consumer_session_key())
            else {
                continue;
            };
            let key = ConsumerPacketGateBatchKey {
                worker_index,
                source: route.source().clone(),
            };
            batches.entry(key).or_default().push(index);
        }
        for (key, batch) in batches {
            let Some(worker) = self.worker_for_index(key.worker_index) else {
                continue;
            };
            let update_count = batch.len();
            let batch_updates = batch
                .iter()
                .filter_map(|index| updates.get(*index))
                .map(ConsumerPacketGateCommand::from_update)
                .collect();
            let batch_results = worker
                .request_worker(|response| {
                    RtcWorkerCommand::MediaControl(
                        RtcMediaControlCommand::SetConsumerPacketGateBatch {
                            source: key.source,
                            updates: batch_updates,
                            response,
                        },
                    )
                })
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

    pub(super) async fn execute_receiver_bwe_target_batch(
        &self,
        updates: &[ReceiverBweTargetUpdate],
    ) -> Vec<Result<(), TransportAdapterError>> {
        let mut results = vec![Err(TransportAdapterError::TransportUnavailable); updates.len()];
        let mut batches = BTreeMap::<usize, Vec<usize>>::new();
        for (index, update) in updates.iter().enumerate() {
            let Some(worker_index) = self.worker_index_for_user(update.session_key()) else {
                continue;
            };
            batches.entry(worker_index).or_default().push(index);
        }
        for (worker_index, batch) in batches {
            let Some(worker) = self.worker_for_index(worker_index) else {
                continue;
            };
            let update_count = batch.len();
            let batch_updates = batch
                .iter()
                .filter_map(|index| updates.get(*index))
                .cloned()
                .collect();
            let batch_results = worker
                .request_worker(|response| RtcWorkerCommand::SetReceiverBweTargetBatch {
                    updates: batch_updates,
                    response,
                })
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

    /// Returns the latest active-speaker observations across all workers.
    ///
    /// This is a transport-observed snapshot. It is sorted newest first and
    /// deduplicated by transport media id so room policy can consume it without
    /// learning which worker observed the packet.
    pub(super) async fn active_speaker_source_snapshot_from_workers(
        &self,
    ) -> Vec<ActiveSpeakerSource> {
        let mut snapshot = Vec::new();
        for worker in self.workers.iter() {
            snapshot.extend(worker.active_speaker_source_snapshot().await);
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

    /// The ordering is stable for operator output. Like other diagnostics this
    /// is best-effort and can race with packet processing.
    pub(super) async fn active_speaker_diagnostic_snapshot_from_workers(
        &self,
    ) -> Vec<ActiveSpeakerSourceDiagnostic> {
        let mut snapshot = Vec::new();
        for worker in self.workers.iter() {
            snapshot.extend(worker.active_speaker_diagnostic_snapshot().await);
        }
        snapshot.sort_by_key(|source| source.transport_media_id().as_u64());
        snapshot.dedup_by_key(|source| source.transport_media_id());
        snapshot
    }

    /// Returns the next active-speaker expiry deadline known by any worker.
    ///
    /// The runtime uses this to wake room source-policy sync without polling
    /// every room on a fixed interval.
    pub(super) async fn next_active_speaker_deadline_from_workers(&self) -> Option<Instant> {
        let mut next_deadline: Option<Instant> = None;
        for worker in self.workers.iter() {
            let worker_deadline = worker.next_active_speaker_deadline().await;
            next_deadline = match (next_deadline, worker_deadline) {
                (Some(current), Some(candidate)) => Some(current.min(candidate)),
                (Some(current), None) => Some(current),
                (None, Some(candidate)) => Some(candidate),
                (None, None) => None,
            };
        }
        next_deadline
    }

    /// Returns room instances whose transport-observed active-speaker state has
    /// expired by `now`.
    ///
    /// This bridges packet-loop observations back into room policy. The
    /// room remains authoritative for layout and subscription decisions.
    pub(super) async fn expired_active_speaker_rooms_from_workers(
        &self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId> {
        let mut room_instance_ids = BTreeSet::new();
        for worker in self.workers.iter() {
            room_instance_ids.extend(worker.expired_active_speaker_rooms(now).await);
        }
        room_instance_ids
    }

    pub(super) fn source_policy_subscription_from_workers(&self) -> SourcePolicyUpdateSubscription {
        self.source_policy_signal.subscribe()
    }

    /// Applies one cross-worker relay-route effect.
    ///
    /// Relay installation starts the target worker to obtain its relay mailbox.
    /// Release and activity updates address existing source-worker relay state
    /// without booting the target worker.
    pub(super) async fn execute_relay_route_effect(
        &self,
        effect: &TransportRelayRouteEffect,
    ) -> Result<(), TransportAdapterError> {
        let source_worker = self.require_worker_for_user(effect.source.session_key())?;
        let target_worker =
            self.require_worker_for_media_worker_id(effect.target_media_worker_id)?;
        if Arc::ptr_eq(&source_worker, &target_worker) {
            return Ok(());
        }
        let request = match effect.action {
            TransportRelayRouteAction::Install => {
                target_worker.relay_install_request(effect.source.clone())?
            }
            TransportRelayRouteAction::Release => {
                target_worker.relay_release_request(effect.source.transport_media_id())
            }
            TransportRelayRouteAction::SetActivity(activity) => {
                target_worker.relay_activity_request(effect.source.clone(), activity.is_active())
            }
        };
        source_worker
            .request_worker(|response| RtcWorkerCommand::media_control(request, response))
            .await
    }

    /// Looks up the negotiated MID for a transport media handle.
    ///
    /// This is a best-effort observation used by diagnostics and compatibility
    /// projections. `None` can mean the media no longer exists or that the
    /// worker has not negotiated a MID for it.
    pub(super) async fn transport_media_mid_from_worker(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Option<String> {
        self.worker_for_user(session_key)?
            .request_worker(|response| RtcWorkerCommand::ResolveMediaMid {
                transport_media_id,
                response,
            })
            .await
            .ok()
            .flatten()
    }

    /// Returns the latest known health for one transport session.
    ///
    /// Health is transport-observed and may lag room cleanup. Callers should use
    /// it to decide whether media connectivity appears alive, not as membership
    /// authority.
    pub(super) fn session_transport_health_from_worker(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        self.worker_for_user(session_key)?
            .session_transport_health(session_key)
    }

    fn worker_index_for_user(&self, session_key: &TransportSessionKey) -> Option<usize> {
        let worker_index = session_key.media_worker_id().as_usize();
        (worker_index < self.workers.len()).then_some(worker_index)
    }

    pub(super) fn require_worker_for_user(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<Arc<RtcWorker>, TransportAdapterError> {
        self.worker_for_user(session_key)
            .ok_or(TransportAdapterError::TransportUnavailable)
    }

    fn require_worker_for_media_worker_id(
        &self,
        media_worker_id: MediaWorkerId,
    ) -> Result<Arc<RtcWorker>, TransportAdapterError> {
        self.worker_for_index(media_worker_id.as_usize())
            .ok_or(TransportAdapterError::TransportUnavailable)
    }

    fn session_keys_by_worker(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> BTreeMap<usize, Vec<TransportSessionKey>> {
        let mut keys_by_worker = BTreeMap::<usize, Vec<TransportSessionKey>>::new();
        for session_key in session_keys {
            if let Some(worker_index) = self.worker_index_for_user(session_key) {
                keys_by_worker
                    .entry(worker_index)
                    .or_default()
                    .push(session_key.clone());
            }
        }
        keys_by_worker
    }

    fn for_session_workers(
        &self,
        session_keys: &[TransportSessionKey],
        mut visit: impl FnMut(&RtcWorker, &[TransportSessionKey]),
    ) {
        for (worker_index, worker_session_keys) in self.session_keys_by_worker(session_keys) {
            if let Some(worker) = self.worker_for_index(worker_index) {
                visit(worker.as_ref(), &worker_session_keys);
            }
        }
    }

    pub(super) fn ensure_same_room(
        consumer_session_key: &TransportSessionKey,
        source_session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        if consumer_session_key.room_instance_id() == source_session_key.room_instance_id() {
            return Ok(());
        }
        Err(TransportAdapterError::InvalidInput)
    }

    pub(super) fn require_consumer_worker_for_route(
        &self,
        route: &TransportConsumerRoute,
    ) -> Result<Arc<RtcWorker>, TransportAdapterError> {
        Self::ensure_same_room(route.consumer_session_key(), route.source_session_key())?;
        self.require_worker_for_user(route.consumer_session_key())
    }

    fn worker_for_index(&self, worker_index: usize) -> Option<Arc<RtcWorker>> {
        self.workers.get(worker_index).map(Arc::clone)
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn all_workers(&self) -> impl Iterator<Item = &Arc<RtcWorker>> {
        self.workers.iter()
    }
}

/// first media id reserved for one worker index
///
/// The fallback clamps extreme indexes to the highest representable stride
/// base. Normal startup validates worker counts long before this point, so the
/// saturating behavior is only a defensive guard for transitional callers that
/// bypass the builder.
fn media_id_base_for_worker_index(worker_index: usize) -> u64 {
    u64::try_from(worker_index)
        .unwrap_or(u64::MAX / MEDIA_ID_STRIDE)
        .saturating_mul(MEDIA_ID_STRIDE)
}

pub(super) fn signaling_to_str0m_media_kind(kind: o_sfu_router::MediaKind) -> Str0mMediaKind {
    match kind {
        o_sfu_router::MediaKind::Audio => Str0mMediaKind::Audio,
        o_sfu_router::MediaKind::Video => Str0mMediaKind::Video,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ConsumerPacketGateBatchKey {
    worker_index: usize,
    source: TransportSourceKey,
}
