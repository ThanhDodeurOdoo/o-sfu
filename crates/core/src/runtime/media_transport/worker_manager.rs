//! RTC worker management below the media transport boundary.
//!
//! `MediaTransport` hides this module from server orchestration. The worker manager
//! owns the process-local RTC worker topology, maps each transport session to
//! the worker selected by its media-worker id and coordinates cross-worker
//! relay cleanup. It is still cold-path orchestration around worker services.
//! Packet-loop hot paths live inside `runtime::rtc_engine`.

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
        media_transport::config::RtcWorkerManagerConfig,
        rtc_engine::{RtcTransportWorker, client_rtp_capabilities_from_answer},
    },
    transport::{
        ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, AppliedSessionAnswer,
        ConsumerPacketGateUpdate, ReceiverBandwidthSnapshot, SessionOffer, SourcePacketGate,
        SourcePolicySignal, SourcePolicyUpdateSubscription, TransportAdapterError,
        TransportBitrateSnapshot, TransportMediaId, TransportPlacementPressureSnapshot,
        TransportRelayRouteAction, TransportRelayRouteEffect, TransportSessionHealth,
        TransportSessionKey, TransportWorkerPressureSnapshot,
    },
};

/// Distance between two worker media-id allocation ranges.
///
/// The value only needs to be large enough that one worker cannot exhaust its
/// range during a process lifetime under realistic load. Keeping the gap fixed
/// lets cross-worker route maps keep using `TransportMediaId` as their source
/// key while still avoiding collisions between spillover workers.
const MEDIA_ID_STRIDE: u64 = 1_000_000_000;

type RelayRegistrationWorkers = Option<(Arc<RtcTransportWorker>, Arc<RtcTransportWorker>)>;

/// Process-local manager for RTC transport workers keyed by media-worker id.
///
/// The worker manager is the last transport layer that knows about worker
/// distribution. Above it, callers express media intent through transport
/// ports. Below it, each [`RtcTransportWorker`] owns one worker-local RTC
/// state machine and packet loop.
///
/// # Concurrency
///
/// Cloning worker handles is cheap because each worker is stored in an `Arc`.
/// Operations route to the selected worker and await the worker-owned async work.
/// The worker manager does not hold a global lock across those awaits.
///
/// Cross-worker subscriptions use source-to-consumer relay. The pure room
/// router decides that a consumer route exists; this worker manager installs the
/// worker-local packet bridge that makes the route deliver media:
///
/// ```text
/// Same worker:
///
///   producer session W0
///          |
///          v
///   W0 packet loop -> consumer session W0
///
/// Cross worker:
///
///   producer session W0
///          |
///          v
///   W0 packet loop -> worker-local relay targets -> W1 RelayPacketMailbox
///          |                                      |
///          |                                      v
///          +---------------------------> W1 packet loop -> consumer session W1
/// ```
#[derive(Debug)]
pub struct RtcWorkerManager {
    /// RTC workers addressed by media-worker id modulo worker count.
    workers: Vec<Arc<RtcTransportWorker>>,
    /// Shared wakeup signal used by every worker to notify room-level source
    /// policy tasks about transport-observed changes without polling every room
    source_policy_signal: Arc<SourcePolicySignal>,
}

impl RtcWorkerManager {
    /// Builds RTC workers and installs one shared source-policy signal.
    ///
    /// Invalid worker or port-range combinations should already be rejected by
    /// `RtcTransportBuilder`. If a transitional caller bypasses that builder
    /// this constructor falls back to a single worker, preserving availability
    /// rather than panicking during startup.
    pub(super) fn new(config: &RtcWorkerManagerConfig) -> Self {
        let source_policy_signal = Arc::new(SourcePolicySignal::default());
        let workers = config
            .transport_config()
            .rtc_port_range()
            .split_for_workers(config.worker_count())
            .filter(|worker_ranges| !worker_ranges.is_empty())
            .map_or_else(
                || {
                    vec![Arc::new(RtcTransportWorker::new(
                        config.transport_config(),
                        config.transport_deps(),
                        Arc::clone(&source_policy_signal),
                        media_id_base_for_worker_index(0),
                        0,
                    ))]
                },
                |worker_ranges| {
                    worker_ranges
                        .into_iter()
                        .enumerate()
                        .map(|(media_worker_id, range)| {
                            Arc::new(RtcTransportWorker::new(
                                &config.worker_config_with_port_range(range),
                                config.transport_deps(),
                                Arc::clone(&source_policy_signal),
                                media_id_base_for_worker_index(media_worker_id),
                                media_worker_id,
                            ))
                        })
                        .collect()
                },
            );
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
    ) -> Option<Arc<RtcTransportWorker>> {
        self.worker_for_media_worker_id(session_key.media_worker_id())
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
        let consumer_worker = self
            .worker_for_user(consumer_session_key)
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        let source_worker = self
            .worker_for_user(source_session_key)
            .ok_or(TransportAdapterError::TransportUnavailable)?;
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
    pub(super) fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        let mut keys_by_worker = BTreeMap::<usize, Vec<TransportSessionKey>>::new();
        for session_key in session_keys {
            if let Some(worker_index) = self.worker_index_for_user(session_key) {
                keys_by_worker
                    .entry(worker_index)
                    .or_default()
                    .push(session_key.clone());
            }
        }
        let mut snapshot = TransportBitrateSnapshot::default();
        for (worker_index, worker_session_keys) in keys_by_worker {
            if let Some(worker) = self.worker_for_index(worker_index) {
                let worker_snapshot = worker.transport_bitrate_snapshot(&worker_session_keys);
                snapshot.total = snapshot.total.saturating_add(worker_snapshot.total);
                snapshot.per_media.extend(worker_snapshot.per_media);
            }
        }
        snapshot
    }

    /// Builds a best-effort receiver bandwidth snapshot across RTC workers.
    ///
    /// Room policy uses this as an input to source selection. Missing entries
    /// mean the transport has no current estimate for that session.
    pub(super) fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        let mut keys_by_worker = BTreeMap::<usize, Vec<TransportSessionKey>>::new();
        for session_key in session_keys {
            if let Some(worker_index) = self.worker_index_for_user(session_key) {
                keys_by_worker
                    .entry(worker_index)
                    .or_default()
                    .push(session_key.clone());
            }
        }
        let mut snapshot = ReceiverBandwidthSnapshot::default();
        for (worker_index, worker_session_keys) in keys_by_worker {
            if let Some(worker) = self.worker_for_index(worker_index) {
                let worker_snapshot = worker.receiver_bandwidth_snapshot(&worker_session_keys);
                snapshot.per_session.extend(worker_snapshot.per_session);
            }
        }
        snapshot
    }

    /// Builds a best-effort placement-pressure snapshot across RTC workers.
    ///
    /// Egress bitrate is additive across selected sessions. Saturation signals
    /// use the hottest owning worker so one overloaded worker can activate
    /// spillover.
    pub(super) fn placement_pressure_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportPlacementPressureSnapshot {
        let mut keys_by_worker = BTreeMap::<usize, Vec<TransportSessionKey>>::new();
        for session_key in session_keys {
            if let Some(worker_index) = self.worker_index_for_user(session_key) {
                keys_by_worker
                    .entry(worker_index)
                    .or_default()
                    .push(session_key.clone());
            }
        }
        let mut snapshot = TransportPlacementPressureSnapshot::default();
        for (worker_index, worker_session_keys) in keys_by_worker {
            if let Some(worker) = self.worker_for_index(worker_index) {
                snapshot =
                    snapshot.merged_with(worker.placement_pressure_snapshot(&worker_session_keys));
            }
        }
        snapshot
    }

    /// Builds best-effort pressure snapshots for every local RTC worker.
    pub(super) fn worker_pressure_snapshots(&self) -> Vec<TransportWorkerPressureSnapshot> {
        self.workers
            .iter()
            .enumerate()
            .map(|(media_worker_id, worker)| worker.worker_pressure_snapshot(media_worker_id))
            .collect()
    }

    /// Applies packet-gate updates in worker-local batches.
    ///
    /// The result vector preserves the input order so callers can correlate
    /// failures with planned policy updates. Cross-room updates are rejected
    /// before reaching worker state because a consumer route must never target
    /// media owned by another room instance.
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
            let Some(worker_index) = self.worker_index_for_user(update.consumer_session_key())
            else {
                continue;
            };
            let key = ConsumerPacketGateBatchKey {
                worker_index,
                source_session_key: update.source_session_key().clone(),
                source_transport_media_id: update.source_transport_media_id(),
            };
            batches.entry(key).or_default().push(index);
        }
        for (key, batch) in batches {
            let Some(worker) = self.worker_for_index(key.worker_index) else {
                continue;
            };
            let update_count = batch.len();
            let batch_results = worker
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

    /// Returns the latest active-speaker observations across all workers.
    ///
    /// This is a transport-observed snapshot. It is sorted newest first and
    /// deduplicated by transport media id so room policy can consume it without
    /// learning which worker observed the packet.
    pub(super) async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        let mut snapshot = Vec::new();
        for worker in &self.workers {
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

    /// Returns diagnostic active-speaker state across all workers.
    ///
    /// The ordering is stable for operator output. Like other diagnostics this
    /// is best-effort and can race with packet processing.
    pub(super) async fn active_speaker_diagnostic_snapshot(
        &self,
    ) -> Vec<ActiveSpeakerSourceDiagnostic> {
        let mut snapshot = Vec::new();
        for worker in &self.workers {
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
    pub(super) async fn next_active_speaker_deadline(&self) -> Option<Instant> {
        let mut next_deadline: Option<Instant> = None;
        for worker in &self.workers {
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
    /// This bridges packet-loop observations back into room-owned policy. The
    /// room remains authoritative for layout and subscription decisions.
    pub(super) async fn expired_active_speaker_room_instance_ids(
        &self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId> {
        let mut room_instance_ids = BTreeSet::new();
        for worker in &self.workers {
            room_instance_ids.extend(worker.expired_active_speaker_room_instance_ids(now).await);
        }
        room_instance_ids
    }

    pub(super) fn source_policy_subscription(&self) -> SourcePolicyUpdateSubscription {
        self.source_policy_signal.subscribe()
    }

    /// Creates the first offer on the worker that owns the session.
    ///
    /// Offer state remains worker-local. The worker manager only applies the
    /// deterministic session-to-worker mapping.
    pub(super) async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.worker_for_user(session_key)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .negotiation()
            .create_initial_session_offer(session_key)
            .await
    }

    /// Creates a renegotiation offer on the session's owning worker.
    ///
    /// The worker enforces whether renegotiation is legal for the current RTC
    /// state.
    pub(super) async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.worker_for_user(session_key)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .negotiation()
            .create_session_renegotiation_offer(session_key)
            .await
    }

    /// Applies an SDP answer on the session's owning worker.
    ///
    /// The returned producer parameters are transport-derived facts used by the
    /// room to commit staged publications.
    pub(super) async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, TransportAdapterError> {
        self.worker_for_user(session_key)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .negotiation()
            .apply_session_answer(session_key, answer_sdp)
            .await
    }

    /// Projects answered client RTP capabilities from an SDP answer.
    ///
    /// This is a pure parsing/projection helper. It does not depend on worker
    /// state and returns `InvalidInput` when the answer cannot be projected.
    pub(super) fn negotiated_client_rtp_capabilities(
        answer_sdp: &str,
        _offered_router_capabilities: &MediaCapabilities,
    ) -> Result<MediaCapabilities, TransportAdapterError> {
        client_rtp_capabilities_from_answer(answer_sdp).ok_or(TransportAdapterError::InvalidInput)
    }

    /// Closes all transport state for a session and releases cross-worker relay
    /// registrations.
    ///
    /// Session cleanup starts on the owning worker. Any relay cleanup emitted by
    /// that worker is then replayed on source workers so no remote worker keeps
    /// forwarding to a closed target.
    pub(super) async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        self.worker_for_user(session_key)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .users()
            .close_session_with_outcome(session_key)
            .await
            .map(|_outcome| ())
    }

    /// Removes one transport media handle from the owning worker.
    pub(super) async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        self.worker_for_user(session_key)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .media()
            .remove_media(session_key, transport_media_id)
            .await
    }

    /// Declares published media on the worker that owns the publishing session.
    ///
    /// The transport media id returned here is a local transport realization.
    /// Room state remains responsible for mapping it back to a published source.
    pub(super) async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.worker_for_user(session_key)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .media()
            .add_recv_media(
                session_key,
                signaling_to_str0m_media_kind(media_kind),
                rtp_parameters,
            )
            .await
    }

    /// Declares consumed media and installs cross-worker relay state when
    /// publisher and consumer live on different workers.
    ///
    /// Relay activation is staged on the source worker before the consumer media
    /// is created. If consumer creation fails, the source-side relay
    /// registration is removed before the error is returned.
    ///
    /// The setup order mirrors the packet path:
    ///
    /// ```text
    /// source worker W0:
    ///   source_media_id -> target W1 relay mailbox
    ///
    /// consumer worker W1:
    ///   source_media_id -> remote source control for W0
    ///   consumer media  -> local WebRTC send stream
    /// ```
    pub(super) async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_media_id: TransportMediaId,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        ensure_same_room_instance(consumer_session_key, source_session_key)?;
        let relay_route =
            self.relay_registration_workers(consumer_session_key, source_session_key)?;
        let remote_source_control = relay_route
            .as_ref()
            .map(|(source_worker, consumer_worker)| {
                source_worker
                    .media()
                    .remote_source_control(consumer_worker.as_ref())
            })
            .transpose()?;
        let consumer_worker = self
            .worker_for_user(consumer_session_key)
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        consumer_worker
            .media()
            .add_send_media(
                consumer_session_key,
                signaling_to_str0m_media_kind(media_kind),
                source_session_key,
                source_media_id,
                remote_source_control,
                consumer_rtp_parameters,
            )
            .await
    }

    /// Applies one room-owned relay route mutation to worker packet-loop cache.
    pub(super) async fn apply_relay_route_effect(
        &self,
        effect: &TransportRelayRouteEffect,
    ) -> Result<(), TransportAdapterError> {
        let source_worker = self
            .worker_for_user(&effect.source_session_key)
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        let target_worker = self
            .worker_for_media_worker_id(effect.target_media_worker_id)
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        if Arc::ptr_eq(&source_worker, &target_worker) {
            return Ok(());
        }
        match effect.action {
            TransportRelayRouteAction::Install => {
                source_worker
                    .media()
                    .activate_relay_route(
                        &effect.source_session_key,
                        effect.source_transport_media_id,
                        target_worker.as_ref(),
                    )
                    .await
            }
            TransportRelayRouteAction::Release => {
                source_worker
                    .media()
                    .deactivate_relay_route(
                        effect.source_transport_media_id,
                        target_worker.as_ref(),
                    )
                    .await
            }
            TransportRelayRouteAction::SetActive(active) => {
                source_worker
                    .media()
                    .apply_relay_target_activity(
                        &effect.source_session_key,
                        effect.source_transport_media_id,
                        target_worker.as_ref(),
                        active,
                    )
                    .await
            }
        }
    }

    /// Updates producer route activity on the producer's owning worker.
    pub(super) async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.worker_for_user(session_key)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .media()
            .set_producer_active(session_key, transport_media_id, active)
            .await
    }

    /// Updates consumer route activity on the consumer's owning worker.
    ///
    /// The source and consumer must belong to the same room instance. The worker
    /// set validates that room boundary before mutating worker state.
    pub(super) async fn set_consumer_active(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        ensure_same_room_instance(consumer_session_key, source_session_key)?;
        self.worker_for_user(consumer_session_key)
            .ok_or(TransportAdapterError::TransportUnavailable)?
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

    /// Applies a packet gate to one consumer route.
    ///
    /// Packet gates are transport execution policy produced by room-owned
    /// source selection. The transport must not reinterpret them as room layout
    /// or subscription state.
    pub(super) async fn set_consumer_packet_gate(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: SourcePacketGate,
    ) -> Result<(), TransportAdapterError> {
        ensure_same_room_instance(consumer_session_key, source_session_key)?;
        self.worker_for_user(consumer_session_key)
            .ok_or(TransportAdapterError::TransportUnavailable)?
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

    /// Requests a keyframe for a consumer route through the consumer's worker.
    ///
    /// Cross-room requests are rejected. Cross-worker requests are legal because
    /// the consumer worker knows how to forward feedback to the source worker.
    pub(super) async fn request_consumer_keyframe(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        ensure_same_room_instance(consumer_session_key, source_session_key)?;
        self.worker_for_user(consumer_session_key)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .media()
            .request_consumer_keyframe(
                consumer_session_key,
                consumer_transport_media_id,
                source_session_key,
                source_transport_media_id,
            )
            .await
    }

    /// Looks up the negotiated MID for a transport media handle.
    ///
    /// This is a best-effort observation used by diagnostics and compatibility
    /// projections. `None` can mean the media no longer exists or that the
    /// worker has not negotiated a MID for it.
    pub(super) async fn transport_media_mid(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Option<String> {
        let worker = self.worker_for_user(session_key)?;
        worker
            .media()
            .transport_media_mid(transport_media_id)
            .await
            .ok()
            .flatten()
    }

    /// Returns the latest known health for one transport session.
    ///
    /// Health is transport-observed and may lag room cleanup. Callers should use
    /// it to decide whether media connectivity appears alive, not as membership
    /// authority.
    pub(super) fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        self.worker_for_user(session_key)?
            .observability()
            .session_transport_health(session_key)
    }

    fn worker_index_for_user(&self, session_key: &TransportSessionKey) -> Option<usize> {
        self.worker_index_for_media_worker_id(session_key.media_worker_id())
    }

    fn worker_for_media_worker_id(
        &self,
        media_worker_id: usize,
    ) -> Option<Arc<RtcTransportWorker>> {
        self.worker_index_for_media_worker_id(media_worker_id)
            .and_then(|worker_index| self.worker_for_index(worker_index))
    }

    fn worker_index_for_media_worker_id(&self, media_worker_id: usize) -> Option<usize> {
        let worker_count = self.workers.len();
        if worker_count == 0 {
            return None;
        }
        Some(media_worker_id % worker_count)
    }

    fn worker_for_index(&self, worker_index: usize) -> Option<Arc<RtcTransportWorker>> {
        self.workers.get(worker_index).map(Arc::clone)
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn all_workers(&self) -> impl Iterator<Item = &Arc<RtcTransportWorker>> {
        self.workers.iter()
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

/// Returns the first media id reserved for one worker index.
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

fn signaling_to_str0m_media_kind(kind: MediaKind) -> Str0mMediaKind {
    match kind {
        MediaKind::Audio => Str0mMediaKind::Audio,
        MediaKind::Video => Str0mMediaKind::Video,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ConsumerPacketGateBatchKey {
    worker_index: usize,
    source_session_key: TransportSessionKey,
    source_transport_media_id: TransportMediaId,
}
