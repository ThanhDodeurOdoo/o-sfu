//! RTC worker ownership below the media transport boundary.
//!
//! `MediaTransport` owns the process-local RTC worker topology, maps each
//! transport session to the worker selected by its media-worker id and
//! coordinates cross-worker relay cleanup. Packet-loop hot paths live inside
//! `engine::media_transport::rtc`.

use std::{cmp::Reverse, collections::BTreeMap, ptr};

use str0m::media::MediaKind as Str0mMediaKind;

use super::rtc::{RtcWorker, RtcWorkerCommand};
use crate::engine::{
    MediaWorkerId,
    media_transport::{
        ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, MediaTransport,
        ReceiverBandwidthSnapshot, TransportAdapterError, TransportBitrateSnapshot,
        TransportMediaId, TransportPlacementPressureSnapshot, TransportQualitySnapshot,
        TransportRelayRouteAction, TransportRelayRouteEffect, TransportSessionHealth,
        TransportSessionKey, TransportSourceActivitySnapshot, TransportSourceKey,
        TransportTeardown, TransportWorkerPressureSnapshot,
    },
};

type RelayRegistrationWorkers<'a> = Option<(&'a RtcWorker, &'a RtcWorker)>;

impl MediaTransport {
    /// Selects the worker that owns a transport session.
    ///
    /// The mapping is deterministic and depends only on the runtime-assigned
    /// media-worker id in the session key. Room and signaling code must not
    /// infer topology from user identity.
    pub(super) fn worker_for_user(&self, session_key: &TransportSessionKey) -> Option<&RtcWorker> {
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
    ) -> Result<RelayRegistrationWorkers<'_>, TransportAdapterError> {
        let consumer_worker = self.require_worker_for_user(consumer_session_key)?;
        let source_worker = self.require_worker_for_user(source_session_key)?;
        if ptr::eq(consumer_worker, source_worker) {
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
        snapshot.sort_unstable_by_key(|source| {
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

    /// Applies one cross-worker relay-route effect.
    ///
    /// Relay installation uses the target worker mailbox while release and
    /// activity updates address source-worker relay state.
    pub(super) async fn execute_relay_route_effect(
        &self,
        effect: &TransportRelayRouteEffect,
    ) -> Result<(), TransportAdapterError> {
        if effect.action == TransportRelayRouteAction::Release {
            self.teardown([TransportTeardown::ReleaseRelayRoute {
                source: effect.source.clone(),
                target_media_worker_id: effect.target_media_worker_id,
            }])
            .await;
            return Ok(());
        }
        let source_worker = self.require_worker_for_user(effect.source.session_key())?;
        let target_worker =
            self.require_worker_for_media_worker_id(effect.target_media_worker_id)?;
        if ptr::eq(source_worker, target_worker) {
            return Ok(());
        }
        let request = target_worker.relay_route_request(effect.source.clone(), effect.action);
        source_worker
            .request_worker(|response| RtcWorkerCommand::RouteControl {
                request,
                response: Some(response),
            })
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
    ) -> Result<&RtcWorker, TransportAdapterError> {
        self.worker_for_user(session_key)
            .ok_or(TransportAdapterError::TransportUnavailable)
    }

    pub(super) fn require_worker_for_media_worker_id(
        &self,
        media_worker_id: MediaWorkerId,
    ) -> Result<&RtcWorker, TransportAdapterError> {
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
                visit(worker, &worker_session_keys);
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

    pub(super) fn worker_for_index(&self, worker_index: usize) -> Option<&RtcWorker> {
        self.workers.get(worker_index)
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn all_workers(&self) -> impl Iterator<Item = &RtcWorker> {
        self.workers.iter()
    }
}

pub(super) fn signaling_to_str0m_media_kind(kind: o_sfu_router::MediaKind) -> Str0mMediaKind {
    match kind {
        o_sfu_router::MediaKind::Audio => Str0mMediaKind::Audio,
        o_sfu_router::MediaKind::Video => Str0mMediaKind::Video,
    }
}
