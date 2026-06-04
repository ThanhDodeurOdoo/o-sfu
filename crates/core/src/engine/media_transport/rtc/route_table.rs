//! worker-local RTC source route table.
//!
//! `RouteTable` owns packet-loop route, relay, gate and producer facts keyed by source transport media id.
//! negotiated browser media lookup remains in `media_registry`.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    sync::Arc,
    time::{Duration, Instant},
};

use str0m::{
    media::{KeyframeRequestKind, Rid},
    rtp::Ssrc,
};

use self::source_route::{RemovedConsumerRoute, SourceRouteState};
pub(super) use self::source_route::{RidReadinessRouteUpdate, RidReadinessSelectedGateUpdate};
#[cfg(test)]
use super::route_control::{PacketLayerMetadata, PacketRouteDecision};
use super::{
    bitrate::MediaBitrateCounter,
    commands::RemoteSourceControl,
    demux::{MediaRouteDestination, MediaRouteEntry, MediaRouteKey},
    keyframe_tracker::{
        KEYFRAME_RETRY_DRAIN_LIMIT, KeyframeRequestDeadline, KeyframeRequestDecision,
        SourceKeyframeRequest,
    },
    media_registry::{DecoderRefreshCodec, RemoteSourceRegistration},
    relay_registry::{ActiveRelayTarget, RelayPacketMailbox, RelayTargetId},
    route_control::PacketLayerGate,
};
use crate::engine::media_transport::{
    ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, TransportAdapterError, TransportMediaId,
    TransportSessionKey, TransportSourceActivity, TransportSourceActivitySnapshot,
    TransportSourceKey,
};

#[path = "source_route.rs"]
mod source_route;

#[derive(Debug, Default)]
pub(super) struct RouteTable {
    sources: BTreeMap<MediaRouteKey, SourceRouteState>,
    audio_sources: BTreeSet<TransportMediaId>,
    remote_gate_queue: Vec<TransportMediaId>,
    rid_refresh_heap: BinaryHeap<Reverse<RidRefreshDeadline>>,
    next_rid_refresh_id: u64,
    kf_deadlines: BinaryHeap<Reverse<KeyframeRequestDeadline>>,
    next_kf_request_id: u64,
}

type RidRefreshDeadline = (Instant, u64, TransportMediaId);
const STALE_DEADLINE_PEEK_CLEANUP_LIMIT: usize = 16;

impl RouteTable {
    pub(super) fn forward_view(
        &self,
        source_id: TransportMediaId,
        include_relays: bool,
    ) -> (
        Option<&MediaRouteEntry>,
        Option<&[ActiveRelayTarget]>,
        Option<PacketLayerGate>,
    ) {
        self.sources
            .get(&source_id)
            .map_or((None, None, None), |source| {
                source.forward_view(include_relays)
            })
    }

    pub(super) fn has_sources(&self) -> bool {
        !self.sources.is_empty()
    }

    pub(super) fn local_route(&self, source_id: TransportMediaId) -> Option<&MediaRouteEntry> {
        self.sources.get(&source_id)?.local_route()
    }

    pub(super) fn add_consumer_route(
        &mut self,
        source_id: TransportMediaId,
        destination: MediaRouteDestination,
    ) -> usize {
        let index = self
            .sources
            .entry(source_id)
            .or_default()
            .add_consumer_route(destination);
        self.refresh_src_pkt_gate(source_id);
        index
    }

    pub(super) fn remove_consumer_route(
        &mut self,
        source_id: TransportMediaId,
        session_key: &TransportSessionKey,
        media_id: TransportMediaId,
    ) -> Option<RemovedConsumerRoute> {
        let removed = self
            .sources
            .get_mut(&source_id)?
            .remove_consumer_route(session_key, media_id)?;
        self.refresh_src_pkt_gate(source_id);
        self.prune_unrouted_remote_src(source_id);
        self.prune_empty(source_id);
        Some(removed)
    }

    pub(super) fn set_source_active(
        &mut self,
        source_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.sources
            .get_mut(&source_id)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .set_source_active(active)
    }

    pub(super) fn set_consumer_active(
        &mut self,
        source_id: TransportMediaId,
        dst_idx: usize,
        session_key: &TransportSessionKey,
        media_id: TransportMediaId,
        active: bool,
    ) -> Result<bool, TransportAdapterError> {
        let changed = self
            .sources
            .get_mut(&source_id)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .set_consumer_active(dst_idx, session_key, media_id, active)?;
        if changed {
            self.refresh_src_pkt_gate(source_id);
        }
        Ok(changed)
    }

    pub(super) fn set_consumer_pkt_gate(
        &mut self,
        source_id: TransportMediaId,
        dst_idx: usize,
        session_key: &TransportSessionKey,
        media_id: TransportMediaId,
        packet_gate: PacketLayerGate,
        pending_gate: Option<PacketLayerGate>,
    ) -> Result<bool, TransportAdapterError> {
        let changed = self.set_consumer_pkt_gate_batch(
            source_id,
            dst_idx,
            session_key,
            media_id,
            packet_gate,
            pending_gate,
        )?;
        if changed {
            self.refresh_src_pkt_gate(source_id);
        }
        Ok(changed)
    }

    pub(super) fn set_consumer_pkt_gate_batch(
        &mut self,
        source_id: TransportMediaId,
        dst_idx: usize,
        session_key: &TransportSessionKey,
        media_id: TransportMediaId,
        packet_gate: PacketLayerGate,
        pending_gate: Option<PacketLayerGate>,
    ) -> Result<bool, TransportAdapterError> {
        self.sources
            .get_mut(&source_id)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .set_consumer_pkt_gate(dst_idx, session_key, media_id, packet_gate, pending_gate)
    }

    pub(super) fn update_rid_readiness(
        &mut self,
        source_id: TransportMediaId,
        incoming_rid: Rid,
        is_keyframe: bool,
        ready: &[Rid],
        stale: &mut Vec<Rid>,
        pending_selected: &mut Vec<Rid>,
    ) -> RidReadinessRouteUpdate {
        let update = self.sources.get_mut(&source_id).map_or_else(
            RidReadinessRouteUpdate::default,
            |source| {
                source.update_rid_readiness(
                    source_id,
                    incoming_rid,
                    is_keyframe,
                    ready,
                    stale,
                    pending_selected,
                )
            },
        );
        if update.changed_gate() {
            self.refresh_src_pkt_gate(source_id);
        }
        update
    }

    pub(super) fn take_route(&mut self, source_id: TransportMediaId) -> Option<MediaRouteEntry> {
        let source = self.sources.get_mut(&source_id)?;
        let route = source.take_local_route();
        self.refresh_src_pkt_gate(source_id);
        self.prune_unrouted_remote_src(source_id);
        self.prune_empty(source_id);
        route
    }

    pub(super) fn remove_dsts_for_session(&mut self, session_key: &TransportSessionKey) {
        let mut affected = Vec::new();
        for (source_id, source) in &mut self.sources {
            if source.remove_dsts_for_session(session_key) {
                affected.push(*source_id);
            }
        }
        for source_id in affected {
            self.refresh_src_pkt_gate(source_id);
            self.prune_unrouted_remote_src(source_id);
            self.prune_empty(source_id);
        }
    }

    pub(super) fn refresh_src_pkt_gate(&mut self, source_id: TransportMediaId) {
        let remote_packet_gate = self.sources.get_mut(&source_id).map_or(
            PacketLayerGate::Block,
            SourceRouteState::refresh_route_pkt_gate,
        );
        self.publish_remote_pkt_gate(source_id, remote_packet_gate);
    }

    pub(super) fn has_kf_demand(&self, source_id: TransportMediaId, rid: Option<Rid>) -> bool {
        self.sources
            .get(&source_id)
            .is_some_and(|source| source.has_kf_demand(rid))
    }

    pub(super) fn unregister_local_source(&mut self, source_id: TransportMediaId) {
        if let Some(source) = self.sources.get_mut(&source_id) {
            source.clear_packet_state();
        }
        self.audio_sources.remove(&source_id);
        self.prune_empty(source_id);
    }

    pub(super) fn replace_producer_ssrcs(&mut self, source_id: TransportMediaId, ssrcs: Vec<Ssrc>) {
        if ssrcs.is_empty() && !self.sources.contains_key(&source_id) {
            return;
        }
        self.sources
            .entry(source_id)
            .or_default()
            .replace_producer_ssrcs(ssrcs);
        self.prune_empty(source_id);
    }

    pub(super) fn remember_producer_ssrc(&mut self, source_id: TransportMediaId, ssrc: Ssrc) {
        self.sources
            .entry(source_id)
            .or_default()
            .remember_producer_ssrc(ssrc);
    }

    pub(super) fn clear_producer_ssrcs(
        &mut self,
        source_id: TransportMediaId,
    ) -> Option<Vec<Ssrc>> {
        let ssrcs = self.sources.get_mut(&source_id)?.take_producer_ssrcs();
        self.prune_empty(source_id);
        Some(ssrcs)
    }

    pub(super) fn register_remote_source(
        &mut self,
        source: &TransportSourceKey,
        source_control: RemoteSourceControl,
    ) -> Result<Option<RemoteSourceRegistration>, TransportAdapterError> {
        let source_id = source.transport_media_id();
        self.sources
            .entry(source_id)
            .or_default()
            .register_remote_source(source, source_control)
    }

    pub(super) fn restore_remote_source(
        &mut self,
        source_id: TransportMediaId,
        previous_registration: Option<RemoteSourceRegistration>,
    ) {
        if let Some(previous_registration) = previous_registration {
            let pending = self
                .sources
                .entry(source_id)
                .or_default()
                .restore_remote_source(previous_registration);
            if pending {
                self.queue_remote_gate(source_id);
            }
        } else {
            self.remove_remote_source(source_id);
        }
    }

    pub(super) fn remote_source(
        &self,
        source_id: TransportMediaId,
    ) -> Option<&RemoteSourceRegistration> {
        self.sources
            .get(&source_id)
            .and_then(SourceRouteState::remote_source)
    }

    pub(super) fn publish_remote_pkt_gate(
        &mut self,
        source_id: TransportMediaId,
        packet_gate: PacketLayerGate,
    ) {
        let pending = self
            .sources
            .get_mut(&source_id)
            .is_some_and(|source| source.publish_remote_pkt_gate(packet_gate));
        if pending {
            self.queue_remote_gate(source_id);
        }
    }

    pub(super) fn flush_remote_pkt_gates(&mut self) {
        let count = self.remote_gate_queue.len();
        for index in 0..count {
            let Some(source_id) = self.remote_gate_queue.get(index).copied() else {
                break;
            };
            if self
                .sources
                .get_mut(&source_id)
                .is_some_and(SourceRouteState::flush_remote_pkt_gate)
            {
                self.remote_gate_queue.push(source_id);
            }
        }
        self.remote_gate_queue.drain(..count);
    }

    pub(super) fn prune_unrouted_remote_src(&mut self, source_id: TransportMediaId) {
        if self
            .sources
            .get(&source_id)
            .is_some_and(SourceRouteState::has_local_dsts)
        {
            return;
        }
        if self.remote_source(source_id).is_some() {
            self.remove_remote_source(source_id);
        }
    }

    pub(super) fn prune_unrouted_remote_srcs(
        &mut self,
        mut keep_local: impl FnMut(&TransportMediaId) -> bool,
    ) {
        let source_ids = self.sources.keys().copied().collect::<Vec<_>>();
        for source_id in source_ids {
            let has_remote = self.remote_source(source_id).is_some();
            let keep_registered = keep_local(&source_id) || has_remote;
            if has_remote
                && !self
                    .sources
                    .get(&source_id)
                    .is_some_and(SourceRouteState::has_local_dsts)
            {
                self.remove_remote_source(source_id);
                continue;
            }
            if !keep_registered && let Some(source) = self.sources.get_mut(&source_id) {
                source.clear_packet_state();
                self.audio_sources.remove(&source_id);
            }
            self.prune_empty(source_id);
        }
    }

    pub(super) fn decoder_refresh_codec(
        &self,
        source_id: TransportMediaId,
    ) -> Option<DecoderRefreshCodec> {
        self.sources
            .get(&source_id)
            .and_then(SourceRouteState::decoder_refresh_codec)
    }

    pub(super) fn set_decoder_refresh_codec(
        &mut self,
        source_id: TransportMediaId,
        codec: Option<DecoderRefreshCodec>,
    ) {
        if let Some(codec) = codec {
            self.sources
                .entry(source_id)
                .or_default()
                .set_decoder_refresh_codec(Some(codec));
        } else if let Some(source) = self.sources.get_mut(&source_id) {
            source.set_decoder_refresh_codec(None);
            self.prune_empty(source_id);
        }
    }

    pub(super) fn observe_producer_packet(
        &mut self,
        source_id: TransportMediaId,
        rid: Option<Rid>,
        is_keyframe: bool,
        now: Instant,
    ) -> bool {
        if rid.is_none() && !is_keyframe && !self.sources.contains_key(&source_id) {
            return false;
        }
        self.sources
            .entry(source_id)
            .or_default()
            .observe_producer_packet(rid, is_keyframe, now)
    }

    pub(super) fn source_activity_snapshot(
        &self,
        source_ids: &[TransportMediaId],
        now: Instant,
        incoming_bitrate_counters: &BTreeMap<TransportMediaId, Arc<MediaBitrateCounter>>,
    ) -> TransportSourceActivitySnapshot {
        TransportSourceActivitySnapshot {
            per_media: source_ids
                .iter()
                .filter_map(|source_id| {
                    let source_last_packet_age = incoming_bitrate_counters
                        .get(source_id)
                        .and_then(|counter| counter.last_observed_age(now));
                    self.sources.get(source_id).map_or_else(
                        || {
                            source_last_packet_age.map(|last_packet_age| {
                                TransportSourceActivity::new(
                                    *source_id,
                                    last_packet_age,
                                    None,
                                    Vec::new(),
                                )
                            })
                        },
                        |source| source.source_activity(*source_id, source_last_packet_age, now),
                    )
                })
                .collect(),
        }
    }

    pub(super) fn producer_rid_is_ready(
        &self,
        source_id: TransportMediaId,
        rid: Rid,
        now: Instant,
        max_age: Duration,
    ) -> bool {
        self.sources
            .get(&source_id)
            .is_some_and(|source| source.producer_rid_is_ready(rid, now, max_age))
    }

    pub(super) fn collect_ready_producer_rids(
        &self,
        source_id: TransportMediaId,
        now: Instant,
        max_age: Duration,
        ready_rids: &mut Vec<Rid>,
    ) {
        ready_rids.clear();
        let Some(source) = self.sources.get(&source_id) else {
            return;
        };
        source.collect_ready_producer_rids(now, max_age, ready_rids);
    }

    pub(super) fn schedule_rid_refresh(
        &mut self,
        source_id: TransportMediaId,
        rid: Rid,
        request_at: Instant,
    ) {
        let refresh_id = self.next_rid_refresh_id;
        let refresh = (request_at, refresh_id, rid);
        self.next_rid_refresh_id = self.next_rid_refresh_id.saturating_add(1);
        self.sources
            .entry(source_id)
            .or_default()
            .schedule_rid_refresh(refresh);
        self.rid_refresh_heap
            .push(Reverse((request_at, refresh_id, source_id)));
    }

    pub(super) fn drain_due_rid_refreshes(
        &mut self,
        source_id: TransportMediaId,
        rid: Rid,
        now: Instant,
    ) -> usize {
        let Some(source) = self.sources.get_mut(&source_id) else {
            return 0;
        };
        let due_count = source.drain_due_rid_refreshes(rid, now);
        self.prune_empty(source_id);
        due_count
    }

    pub(super) fn drain_all_due_rid_refreshes(
        &mut self,
        now: Instant,
    ) -> Vec<(TransportMediaId, Rid)> {
        let mut due = Vec::new();
        while matches!(
            self.rid_refresh_heap.peek(),
            Some(Reverse((request_at, _, _))) if *request_at <= now
        ) {
            let Some(Reverse(refresh)) = self.rid_refresh_heap.pop() else {
                break;
            };
            let (_request_at, id, source_id) = refresh;
            if let Some(rid) = self.remove_pending_rid_refresh(source_id, id) {
                due.push((source_id, rid));
            }
        }
        due
    }

    pub(super) fn next_rid_refresh_deadline(&mut self) -> Option<Instant> {
        for _ in 0..STALE_DEADLINE_PEEK_CLEANUP_LIMIT {
            let Reverse(refresh) = self.rid_refresh_heap.peek()?;
            let (request_at, id, source_id) = *refresh;
            if self.has_pending_rid_refresh(source_id, id) {
                return Some(request_at);
            }
            self.rid_refresh_heap.pop();
        }
        self.rid_refresh_heap
            .peek()
            .map(|Reverse((request_at, _, _))| *request_at)
    }

    pub(super) fn track_kf_req(
        &mut self,
        source_id: TransportMediaId,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
        now: Instant,
    ) -> KeyframeRequestDecision {
        let id = self.next_kf_request_id;
        let (decision, deadline) = self
            .sources
            .entry(source_id)
            .or_default()
            .track_kf_req(rid, kind, now, id);
        if let Some(deadline) = deadline {
            let (deadline_at, deadline_id) = deadline;
            self.next_kf_request_id = self.next_kf_request_id.saturating_add(1);
            self.kf_deadlines
                .push(Reverse((deadline_at, deadline_id, source_id)));
        }
        decision
    }

    pub(super) fn forget_kf_req(&mut self, source_id: TransportMediaId, rid: Option<Rid>) {
        if let Some(source) = self.sources.get_mut(&source_id) {
            source.forget_kf_req(rid);
        }
        self.prune_empty(source_id);
    }

    pub(super) fn observe_decoder_refresh(
        &mut self,
        source_id: TransportMediaId,
        rid: Option<Rid>,
    ) -> usize {
        let Some(source) = self.sources.get_mut(&source_id) else {
            return 0;
        };
        let cleared = source.observe_decoder_refresh(rid);
        self.prune_empty(source_id);
        cleared
    }

    pub(super) fn drain_due_kf_reqs(
        &mut self,
        now: Instant,
        retries: &mut Vec<SourceKeyframeRequest>,
    ) {
        let mut remaining = KEYFRAME_RETRY_DRAIN_LIMIT;
        while matches!(
            self.kf_deadlines.peek(),
            Some(Reverse((deadline_at, _, _))) if *deadline_at <= now
        ) && remaining > 0
        {
            remaining -= 1;
            let Some(Reverse(deadline)) = self.kf_deadlines.pop() else {
                break;
            };
            let (deadline_at, deadline_id, source_id) = deadline;
            let Some(source) = self.sources.get_mut(&source_id) else {
                continue;
            };
            let next_id = self.next_kf_request_id;
            if let Some((request, retry_deadline)) =
                source.drain_due_kf_req(deadline_at, deadline_id, source_id, now, next_id)
            {
                self.next_kf_request_id = self.next_kf_request_id.saturating_add(1);
                self.kf_deadlines.push(Reverse(retry_deadline));
                retries.push(request);
            }
            self.prune_empty(source_id);
        }
    }

    pub(super) fn next_kf_deadline(&mut self) -> Option<Instant> {
        for _ in 0..STALE_DEADLINE_PEEK_CLEANUP_LIMIT {
            let Reverse(deadline) = self.kf_deadlines.peek()?;
            let (deadline_at, deadline_id, source_id) = *deadline;
            if self
                .sources
                .get(&source_id)
                .is_some_and(|source| source.has_kf_deadline((deadline_at, deadline_id)))
            {
                return Some(deadline_at);
            }
            self.kf_deadlines.pop();
        }
        self.kf_deadlines
            .peek()
            .map(|Reverse((deadline_at, _, _))| *deadline_at)
    }

    #[cfg(test)]
    pub(super) fn decide_packet_route(
        &self,
        source_id: TransportMediaId,
        metadata: PacketLayerMetadata,
    ) -> PacketRouteDecision {
        let Some(source) = self.sources.get(&source_id) else {
            return PacketRouteDecision::Forward;
        };
        if source
            .packet_gate()
            .unwrap_or(PacketLayerGate::Open)
            .permits(metadata)
        {
            PacketRouteDecision::Forward
        } else {
            PacketRouteDecision::Drop
        }
    }

    #[cfg(test)]
    pub(super) fn set_local_pkt_gate(
        &mut self,
        source_id: TransportMediaId,
        packet_gate: Option<PacketLayerGate>,
    ) {
        if packet_gate.is_none() && !self.sources.contains_key(&source_id) {
            return;
        }
        self.sources
            .entry(source_id)
            .or_default()
            .set_test_local_pkt_gate(packet_gate);
        self.prune_empty(source_id);
    }

    pub(super) fn observe_audio_activity(
        &mut self,
        source_id: TransportMediaId,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
    ) -> bool {
        if voice_activity.is_none()
            && audio_level_dbov.is_none()
            && !self.sources.contains_key(&source_id)
        {
            return false;
        }
        let previous_active_speakers = self.ranked_active_speakers(now);
        let previous_packet_gate = self.effective_packet_gate(source_id);
        let source = self.sources.entry(source_id).or_default();
        let (changed, has_audio) =
            source.observe_audio_activity(voice_activity, audio_level_dbov, now);
        if has_audio {
            self.audio_sources.insert(source_id);
        }
        if !changed {
            self.prune_empty(source_id);
            return false;
        }
        previous_packet_gate != self.effective_packet_gate(source_id)
            || !same_active_speaker_order(
                &previous_active_speakers,
                &self.ranked_active_speakers(now),
            )
    }

    pub(super) fn set_relay_pkt_gate(
        &mut self,
        source_id: TransportMediaId,
        target_id: RelayTargetId,
        packet_gate: PacketLayerGate,
    ) {
        self.sources
            .entry(source_id)
            .or_default()
            .set_relay_pkt_gate(target_id, packet_gate);
    }

    pub(super) fn relay_packet_gate(
        &self,
        source_id: TransportMediaId,
        target_id: RelayTargetId,
    ) -> Option<&PacketLayerGate> {
        self.sources
            .get(&source_id)
            .and_then(|source| source.relay_packet_gate(target_id))
    }

    pub(super) fn active_speaker_sources(&self, now: Instant) -> Vec<ActiveSpeakerSource> {
        let mut sources = self.unsorted_active_speakers(now);
        sources.sort_unstable_by_key(|source| {
            (
                Reverse(source.observed_at()),
                source.transport_media_id().as_u64(),
            )
        });
        sources
    }

    pub(super) fn active_speaker_diagnostics(
        &self,
        now: Instant,
    ) -> Vec<ActiveSpeakerSourceDiagnostic> {
        self.audio_source_states()
            .filter_map(|(source_id, source)| source.active_speaker_diagnostic(source_id, now))
            .collect()
    }

    pub(super) fn next_active_speaker_deadline(&self, now: Instant) -> Option<Instant> {
        self.audio_source_states()
            .filter_map(|(_source_id, source)| source.active_deadline_after(now))
            .min()
    }

    pub(super) fn expired_active_speaker_srcs(&self, now: Instant) -> Vec<TransportMediaId> {
        self.audio_source_states()
            .filter_map(|(source_id, source)| {
                source.expired_active_speaker_at(now).then_some(source_id)
            })
            .collect()
    }

    pub(super) fn effective_packet_gate(
        &self,
        source_id: TransportMediaId,
    ) -> Option<PacketLayerGate> {
        self.sources
            .get(&source_id)
            .and_then(SourceRouteState::packet_gate)
    }

    #[cfg(test)]
    pub(super) fn relay_targets_for_source(
        &self,
        source_id: TransportMediaId,
    ) -> Option<&[ActiveRelayTarget]> {
        self.sources
            .get(&source_id)
            .and_then(|source| source.forward_view(true).1)
    }

    pub(super) fn add_relay_target(
        &mut self,
        source_id: TransportMediaId,
        target_id: RelayTargetId,
        target: RelayPacketMailbox,
    ) {
        self.sources
            .entry(source_id)
            .or_default()
            .add_relay_target(target_id, target);
    }

    pub(super) fn remove_relay_target(
        &mut self,
        source_id: TransportMediaId,
        target_id: RelayTargetId,
    ) {
        if self
            .sources
            .get_mut(&source_id)
            .is_some_and(|source| source.remove_relay_target(target_id))
        {
            self.prune_empty(source_id);
        }
    }

    pub(super) fn set_relay_target_active(
        &mut self,
        source_id: TransportMediaId,
        target_id: RelayTargetId,
        active: bool,
    ) {
        if let Some(source) = self.sources.get_mut(&source_id) {
            source.set_relay_target_active(target_id, active);
        }
    }

    pub(super) fn is_relay_target_active(
        &self,
        source_id: TransportMediaId,
        target_id: RelayTargetId,
    ) -> bool {
        self.sources
            .get(&source_id)
            .is_some_and(|source| source.relay_target_is_active(target_id))
    }

    #[cfg(test)]
    pub(super) fn relay_target_count(&self, source_id: TransportMediaId) -> usize {
        self.sources
            .get(&source_id)
            .map_or(0, SourceRouteState::relay_target_count)
    }

    #[cfg(test)]
    pub(super) fn active_relay_target_count(&self, source_id: TransportMediaId) -> usize {
        self.sources
            .get(&source_id)
            .map_or(0, SourceRouteState::active_relay_target_count)
    }

    fn unsorted_active_speakers(&self, now: Instant) -> Vec<ActiveSpeakerSource> {
        self.audio_source_states()
            .filter_map(|(source_id, source)| source.active_speaker_source(source_id, now))
            .collect()
    }

    fn ranked_active_speakers(&self, now: Instant) -> Vec<ActiveSpeakerSource> {
        let mut sources = self.unsorted_active_speakers(now);
        sources.sort_unstable_by_key(|source| {
            (
                Reverse(source.observed_at()),
                Reverse(source.last_audio_level_dbov().unwrap_or(i8::MIN)),
                source.transport_media_id().as_u64(),
            )
        });
        sources
    }

    fn remove_remote_source(&mut self, source_id: TransportMediaId) {
        if let Some(source) = self.sources.get_mut(&source_id) {
            source.clear_remote_source();
        }
        self.audio_sources.remove(&source_id);
        self.remote_gate_queue.retain(|queued| *queued != source_id);
        self.prune_empty(source_id);
    }

    fn queue_remote_gate(&mut self, source_id: TransportMediaId) {
        if !self.remote_gate_queue.contains(&source_id) {
            self.remote_gate_queue.push(source_id);
        }
    }

    fn has_pending_rid_refresh(&self, source_id: TransportMediaId, id: u64) -> bool {
        self.sources
            .get(&source_id)
            .is_some_and(|source| source.has_pending_rid_refresh(id))
    }

    fn remove_pending_rid_refresh(&mut self, source_id: TransportMediaId, id: u64) -> Option<Rid> {
        let source = self.sources.get_mut(&source_id)?;
        let rid = source.remove_pending_rid_refresh(id)?;
        self.prune_empty(source_id);
        Some(rid)
    }

    fn audio_source_states(&self) -> impl Iterator<Item = (TransportMediaId, &SourceRouteState)> {
        self.audio_sources.iter().filter_map(|source_id| {
            self.sources
                .get(source_id)
                .map(|source| (*source_id, source))
        })
    }

    fn prune_empty(&mut self, source_id: TransportMediaId) {
        if self
            .sources
            .get(&source_id)
            .is_some_and(SourceRouteState::is_empty)
        {
            self.sources.remove(&source_id);
            self.audio_sources.remove(&source_id);
        }
    }
}

fn same_active_speaker_order(left: &[ActiveSpeakerSource], right: &[ActiveSpeakerSource]) -> bool {
    left.iter()
        .map(|source| source.transport_media_id())
        .eq(right.iter().map(|source| source.transport_media_id()))
}
