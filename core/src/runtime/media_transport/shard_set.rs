//! RTC worker sharding below the media transport boundary.
//!
//! `MediaTransport` hides this module from server orchestration. The shard set
//! owns the process-local RTC worker topology, maps each transport session to
//! the worker selected by its media-worker id and coordinates cross-worker
//! relay cleanup. It is still cold-path orchestration around worker services.
//! Packet-loop hot paths live inside `runtime::rtc_engine`.

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
        media_transport::config::RtcTransportShardSetConfig,
        rtc_engine::{RelayCleanup, RtcTransportShard, client_rtp_capabilities_from_answer},
    },
    transport::{
        ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, AppliedSessionAnswer,
        ConsumerPacketGateUpdate, ReceiverBandwidthSnapshot, SessionOffer, SourcePacketGate,
        SourcePolicySignal, SourcePolicyUpdateSubscription, TransportAdapterError,
        TransportBitrateSnapshot, TransportMediaId, TransportPlacementPressureSnapshot,
        TransportSessionHealth, TransportSessionKey,
    },
};

/// Distance between two shard media-id allocation ranges.
///
/// The value only needs to be large enough that one worker cannot exhaust its
/// range during a process lifetime under realistic load. Keeping the gap fixed
/// lets cross-worker route maps keep using `TransportMediaId` as their source
/// key while still avoiding collisions between spillover workers.
const MEDIA_ID_STRIDE: u64 = 1_000_000_000;

/// Process-local collection of RTC transport shards keyed by media-worker id.
///
/// The shard set is the last transport layer that knows about worker
/// distribution. Above it, callers express media intent through transport
/// ports. Below it, each [`RtcTransportShard`] owns one worker-local RTC
/// state machine and packet loop.
///
/// # Concurrency
///
/// Cloning shard handles is cheap because each shard is stored in an `Arc`.
/// Operations route to the selected shard and await the shard-owned async work.
/// The shard set does not hold a global lock across those awaits.
///
/// Cross-worker subscriptions use source-to-consumer relay. The pure room
/// router decides that a consumer route exists; this shard set installs the
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
pub struct RtcTransportShardSet {
    /// Worker used when there is only one shard or when a media-worker id maps
    /// to index zero.
    primary_shard: Arc<RtcTransportShard>,
    /// Additional worker shards addressed by media-worker id modulo shard
    /// count.
    extra_shards: Vec<Arc<RtcTransportShard>>,
    /// Shared wakeup signal used by every shard to notify room-level source
    /// policy tasks about transport-observed changes.
    source_policy_signal: Arc<SourcePolicySignal>,
}

impl RtcTransportShardSet {
    /// Builds RTC shards and installs one shared source-policy signal.
    ///
    /// Invalid worker or port-range combinations should already be rejected by
    /// `RtcTransportBuilder`. If a transitional caller bypasses that builder
    /// this constructor falls back to a single shard, preserving availability
    /// rather than panicking during startup.
    pub(super) fn new(config: &RtcTransportShardSetConfig) -> Self {
        let source_policy_signal = Arc::new(SourcePolicySignal::default());
        let Some(shard_ranges) = config
            .transport_config()
            .rtc_port_range()
            .split_for_workers(config.worker_count())
        else {
            return Self {
                primary_shard: Arc::new(RtcTransportShard::new(
                    config.transport_config(),
                    config.transport_deps(),
                    Arc::clone(&source_policy_signal),
                    media_id_base_for_shard_index(0),
                )),
                extra_shards: Vec::new(),
                source_policy_signal,
            };
        };
        let mut shard_ranges = shard_ranges.into_iter();
        let Some(primary_range) = shard_ranges.next() else {
            return Self {
                primary_shard: Arc::new(RtcTransportShard::new(
                    config.transport_config(),
                    config.transport_deps(),
                    Arc::clone(&source_policy_signal),
                    media_id_base_for_shard_index(0),
                )),
                extra_shards: Vec::new(),
                source_policy_signal,
            };
        };
        Self {
            primary_shard: Arc::new(RtcTransportShard::new(
                &config.shard_config_with_port_range(primary_range),
                config.transport_deps(),
                Arc::clone(&source_policy_signal),
                media_id_base_for_shard_index(0),
            )),
            extra_shards: shard_ranges
                .enumerate()
                .map(|(index, range)| {
                    Arc::new(RtcTransportShard::new(
                        &config.shard_config_with_port_range(range),
                        config.transport_deps(),
                        Arc::clone(&source_policy_signal),
                        media_id_base_for_shard_index(index + 1),
                    ))
                })
                .collect(),
            source_policy_signal,
        }
    }

    /// Selects the shard that owns a transport session.
    ///
    /// The mapping is deterministic and depends only on the runtime-assigned
    /// media-worker id in the session key. Room and signaling code must not
    /// infer topology from user identity.
    pub(super) fn shard_for_user(
        &self,
        session_key: &TransportSessionKey,
    ) -> Arc<RtcTransportShard> {
        self.shard_for_media_worker_id(session_key.media_worker_id())
    }

    /// Returns the source and consumer shards needed for cross-worker relay.
    ///
    /// `None` means both sessions are on the same worker and local routing is
    /// enough. A returned pair means the source shard must activate relay
    /// forwarding toward the consumer shard before the consumer route can be
    /// fully installed.
    pub(super) fn relay_registration_shards(
        &self,
        consumer_session_key: &TransportSessionKey,
        source_session_key: &TransportSessionKey,
    ) -> Option<(Arc<RtcTransportShard>, Arc<RtcTransportShard>)> {
        let consumer_shard = self.shard_for_user(consumer_session_key);
        let source_shard = self.shard_for_user(source_session_key);
        if Arc::ptr_eq(&consumer_shard, &source_shard) {
            return None;
        }
        Some((source_shard, consumer_shard))
    }

    /// Releases relay registrations created for routes removed on another
    /// shard.
    ///
    /// Cleanup records are produced by the shard that owns the removed session
    /// or media handle. The shard set uses them to clear source-side relay
    /// state without requiring the caller to understand worker topology.
    pub(super) async fn release_relay_cleanup(
        &self,
        target_shard: &Arc<RtcTransportShard>,
        relay_cleanup: &[RelayCleanup],
    ) {
        for cleanup in relay_cleanup {
            let source_shard = self.shard_for_user(cleanup.source_session_key());
            if Arc::ptr_eq(&source_shard, target_shard) {
                continue;
            }
            let _ = source_shard
                .media()
                .deactivate_relay_route(cleanup.source_transport_media_id(), target_shard.as_ref())
                .await;
        }
    }

    /// Builds a best-effort bitrate snapshot across the shards that own the
    /// requested sessions.
    ///
    /// The snapshot is observability data, not authoritative room state. It may
    /// race with packet-loop updates and session cleanup.
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

    /// Builds a best-effort receiver bandwidth snapshot across worker shards.
    ///
    /// Room policy uses this as an input to source selection. Missing entries
    /// mean the transport has no current estimate for that session.
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

    /// Builds a best-effort placement-pressure snapshot across worker shards.
    ///
    /// Egress bitrate is additive across selected sessions. Saturation signals
    /// use the hottest owning worker so one overloaded worker can activate
    /// spillover.
    pub(super) fn placement_pressure_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportPlacementPressureSnapshot {
        let mut keys_by_shard = BTreeMap::<usize, Vec<TransportSessionKey>>::new();
        for session_key in session_keys {
            keys_by_shard
                .entry(self.shard_index_for_user(session_key))
                .or_default()
                .push(session_key.clone());
        }
        let mut snapshot = TransportPlacementPressureSnapshot::default();
        for (shard_index, shard_session_keys) in keys_by_shard {
            let shard = self.shard_for_index(shard_index);
            snapshot = snapshot.merged_with(shard.placement_pressure_snapshot(&shard_session_keys));
        }
        snapshot
    }

    /// Applies packet-gate updates in shard-local batches.
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

    /// Returns the latest active-speaker observations across all shards.
    ///
    /// This is a transport-observed snapshot. It is sorted newest first and
    /// deduplicated by transport media id so room policy can consume it without
    /// learning which worker observed the packet.
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

    /// Returns diagnostic active-speaker state across all shards.
    ///
    /// The ordering is stable for operator output. Like other diagnostics this
    /// is best-effort and can race with packet processing.
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

    /// Returns the next active-speaker expiry deadline known by any shard.
    ///
    /// The runtime uses this to wake room source-policy sync without polling
    /// every room on a fixed interval.
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

    /// Returns room instances whose transport-observed active-speaker state has
    /// expired by `now`.
    ///
    /// This bridges packet-loop observations back into room-owned policy. The
    /// room remains authoritative for layout and subscription decisions.
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

    /// Creates the first offer on the shard that owns the session.
    ///
    /// Offer state remains worker-local. The shard set only applies the
    /// deterministic session-to-worker mapping.
    pub(super) async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.shard_for_user(session_key)
            .negotiation()
            .create_initial_session_offer(session_key)
            .await
    }

    /// Creates a renegotiation offer on the session's owning shard.
    ///
    /// The worker enforces whether renegotiation is legal for the current RTC
    /// state.
    pub(super) async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.shard_for_user(session_key)
            .negotiation()
            .create_session_renegotiation_offer(session_key)
            .await
    }

    /// Applies an SDP answer on the session's owning shard.
    ///
    /// The returned producer parameters are transport-derived facts used by the
    /// room to commit staged publications.
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

    /// Closes all transport state for a session and releases cross-shard relay
    /// registrations.
    ///
    /// Session cleanup starts on the owning shard. Any relay cleanup emitted by
    /// that shard is then replayed on source shards so no remote worker keeps
    /// forwarding to a closed target.
    pub(super) async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        let session_shard = self.shard_for_user(session_key);
        let close_outcome = session_shard
            .users()
            .close_session_with_outcome(session_key)
            .await?;
        self.release_relay_cleanup(&session_shard, close_outcome.relay_cleanup())
            .await;
        Ok(())
    }

    /// Removes one transport media handle and compensates relay state when the
    /// removed handle was relayed across workers.
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
            self.release_relay_cleanup(&session_shard, &relay_cleanup)
                .await;
        }
        Ok(())
    }

    /// Declares published media on the shard that owns the publishing session.
    ///
    /// The transport media id returned here is a local transport realization.
    /// Room state remains responsible for mapping it back to a published source.
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

    /// Declares consumed media and installs cross-worker relay state when
    /// publisher and consumer live on different shards.
    ///
    /// Relay activation is staged on the source shard before the consumer media
    /// is created. If consumer creation fails, the source-side relay
    /// registration is removed before the error is returned.
    ///
    /// The setup order mirrors the packet path:
    ///
    /// ```text
    /// source shard W0:
    ///   source_media_id -> target W1 relay mailbox
    ///
    /// consumer shard W1:
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
                .activate_relay_route(source_session_key, source_media_id, consumer_shard.as_ref())
                .await?;
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
                if let Err(error) = source_shard
                    .media()
                    .set_relay_route_active(
                        source_session_key,
                        source_media_id,
                        consumer_shard.as_ref(),
                        true,
                    )
                    .await
                {
                    let _ = source_shard
                        .media()
                        .deactivate_relay_route(source_media_id, consumer_shard.as_ref())
                        .await;
                    return Err(error);
                }
            } else {
                let _ = source_shard
                    .media()
                    .deactivate_relay_route(source_media_id, consumer_shard.as_ref())
                    .await;
            }
        }
        add_result
    }

    /// Updates producer route activity on the producer's owning shard.
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

    /// Updates consumer route activity on the consumer's owning shard.
    ///
    /// The source and consumer must belong to the same room instance. The shard
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

    /// Requests a keyframe for a consumer route through the consumer's shard.
    ///
    /// Cross-room requests are rejected. Cross-worker requests are legal because
    /// the consumer shard knows how to forward feedback to the source shard.
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
        self.shard_for_user(session_key)
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
        self.shard_for_user(session_key)
            .observability()
            .session_transport_health(session_key)
    }

    fn shard_index_for_user(&self, session_key: &TransportSessionKey) -> usize {
        self.shard_index_for_media_worker_id(session_key.media_worker_id())
    }

    fn shard_for_media_worker_id(&self, media_worker_id: usize) -> Arc<RtcTransportShard> {
        self.shard_for_index(self.shard_index_for_media_worker_id(media_worker_id))
    }

    fn shard_index_for_media_worker_id(&self, media_worker_id: usize) -> usize {
        let shard_count = self.extra_shards.len().saturating_add(1);
        media_worker_id % shard_count
    }

    fn shard_for_index(&self, shard_index: usize) -> Arc<RtcTransportShard> {
        if shard_index == 0 {
            return Arc::clone(&self.primary_shard);
        }
        self.extra_shards
            .get(shard_index.saturating_sub(1))
            .cloned()
            .unwrap_or_else(|| Arc::clone(&self.primary_shard))
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn all_shards(&self) -> impl Iterator<Item = &Arc<RtcTransportShard>> {
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

/// Returns the first media id reserved for one shard index.
///
/// The fallback clamps extreme indexes to the highest representable stride
/// base. Normal startup validates worker counts long before this point, so the
/// saturating behavior is only a defensive guard for transitional callers that
/// bypass the builder.
fn media_id_base_for_shard_index(shard_index: usize) -> u64 {
    u64::try_from(shard_index)
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
    shard_index: usize,
    source_session_key: TransportSessionKey,
    source_transport_media_id: TransportMediaId,
}
