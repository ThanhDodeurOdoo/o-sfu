//! worker-local RTC source route table.
//!
//! `RouteTable` owns packet-loop route, relay, gate and producer facts keyed by source transport media id.
//! negotiated browser media lookup remains in `media_registry`.

use std::{
    cmp::{Ordering as CmpOrdering, Reverse},
    collections::{BTreeMap, BinaryHeap, btree_map::Entry},
    mem,
    sync::Arc,
    time::{Duration, Instant},
};

use str0m::{
    media::{KeyframeRequestKind, Mid, Rid},
    rtp::Ssrc,
};
use tracing::debug;

#[cfg(test)]
use super::route_control::{PacketLayerMetadata, PacketRouteDecision};
use super::{
    bitrate::MediaBitrateCounter,
    commands::RemoteSourceControl,
    demux::{MediaRouteDestination, MediaRouteEntry, MediaRouteKey},
    keyframe_tracker::{KeyframeRequestDecision, KeyframeRequestTracker, SourceKeyframeRequest},
    media_registry::{DecoderRefreshCodec, RemoteSourceRegistration},
    relay_registry::{
        ActiveRelayTarget, RelayPacketMailbox, RelaySourceRegistration, RelayTargetId,
    },
    route_control::{
        PacketLayerGate, SourceAudioPolicyState, aggregate_packet_gates, intersect_packet_gates,
    },
};
use crate::engine::media_transport::{
    ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, TransportAdapterError, TransportMediaId,
    TransportRidActivity, TransportSessionKey, TransportSourceActivity,
    TransportSourceActivitySnapshot, TransportSourceKey,
};

#[derive(Debug, Default)]
pub(super) struct RouteTable {
    sources: BTreeMap<MediaRouteKey, RouteSource>,
    producers: BTreeMap<MediaRouteKey, ProducerPacketState>,
    remote_gate_queue: Vec<TransportMediaId>,
    keyframe_requests: KeyframeRequestTracker,
    rid_refresh_heap: BinaryHeap<Reverse<RidKeyframeRefresh>>,
    next_rid_refresh_id: u64,
}

type MovedConsumerRoute = (TransportSessionKey, Mid, TransportMediaId, usize);
type RemovedConsumerRoute = (MediaRouteDestination, Option<MovedConsumerRoute>);

#[derive(Default)]
pub(super) struct RidReadinessRouteUpdate {
    pub(super) suspended_stale_gate: bool,
    pub(super) selected_gate: RidReadinessSelectedGateUpdate,
}

impl RidReadinessRouteUpdate {
    pub(super) fn changed_gate(&self) -> bool {
        matches!(
            self.selected_gate,
            RidReadinessSelectedGateUpdate::Activated
                | RidReadinessSelectedGateUpdate::BootstrapFallback
        ) || self.suspended_stale_gate
    }

    fn mark_pending_selected_gate(&mut self) {
        if matches!(self.selected_gate, RidReadinessSelectedGateUpdate::None) {
            self.selected_gate = RidReadinessSelectedGateUpdate::Pending;
        }
    }

    fn mark_activated_pending_gate(&mut self) {
        self.selected_gate = RidReadinessSelectedGateUpdate::Activated;
    }

    fn mark_activated_bootstrap_fallback_gate(&mut self) {
        self.selected_gate = RidReadinessSelectedGateUpdate::BootstrapFallback;
    }
}

#[derive(Default, PartialEq, Eq)]
pub(super) enum RidReadinessSelectedGateUpdate {
    #[default]
    None,
    Pending,
    Activated,
    BootstrapFallback,
}

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
        let Some(source) = self.sources.get(&source_id) else {
            return (None, None, None);
        };
        let relays = if include_relays {
            source.active_relay_targets()
        } else {
            None
        };
        (
            source.local_route.as_ref(),
            relays,
            source.packet_filter_gate(),
        )
    }

    pub(super) fn has_sources(&self) -> bool {
        !self.sources.is_empty()
    }

    pub(super) fn local_route(&self, source_id: TransportMediaId) -> Option<&MediaRouteEntry> {
        self.sources.get(&source_id)?.local_route.as_ref()
    }

    fn route_mut(&mut self, source_id: TransportMediaId) -> Option<&mut MediaRouteEntry> {
        self.sources.get_mut(&source_id)?.local_route.as_mut()
    }

    pub(super) fn add_consumer_route(
        &mut self,
        source_id: TransportMediaId,
        destination: MediaRouteDestination,
    ) -> usize {
        let index = {
            let route = self
                .sources
                .entry(source_id)
                .or_default()
                .local_route
                .get_or_insert_with(|| MediaRouteEntry::new(true));
            let index = route.destinations.len();
            route.push_destination(destination);
            index
        };
        self.refresh_source_packet_gate(source_id);
        index
    }

    pub(super) fn remove_consumer_route(
        &mut self,
        source_id: TransportMediaId,
        session_key: &TransportSessionKey,
        media_id: TransportMediaId,
    ) -> Option<RemovedConsumerRoute> {
        let (removed, moved, empty) = {
            let route = self.route_mut(source_id)?;
            let index = route.destinations.iter().position(|destination| {
                destination.dest_session == *session_key
                    && destination.dest_transport_media_id == media_id
            })?;
            let removed = route.remove_destination(index);
            let moved = route.destinations.get(index).map(|destination| {
                (
                    destination.dest_session.clone(),
                    destination.dest_mid,
                    destination.dest_transport_media_id,
                    index,
                )
            });
            (removed, moved, route.destinations.is_empty())
        };
        if empty && let Some(source) = self.sources.get_mut(&source_id) {
            source.local_route = None;
        }
        self.refresh_source_packet_gate(source_id);
        self.prune_remote_source_if_unrouted(source_id);
        self.prune_empty(source_id);
        Some((removed, moved))
    }

    pub(super) fn set_source_active(
        &mut self,
        source_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        let route = self
            .route_mut(source_id)
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        route.source_active = active;
        Ok(())
    }

    pub(super) fn set_consumer_active(
        &mut self,
        source_id: TransportMediaId,
        destination_index: usize,
        session_key: &TransportSessionKey,
        media_id: TransportMediaId,
        active: bool,
    ) -> Result<bool, TransportAdapterError> {
        let route = self
            .route_mut(source_id)
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        validate_destination(route, destination_index, session_key, media_id)?;
        let changed = route.set_destination_active(destination_index, active);
        if changed {
            self.refresh_source_packet_gate(source_id);
        }
        Ok(changed)
    }

    pub(super) fn set_consumer_packet_gate(
        &mut self,
        source_id: TransportMediaId,
        destination_index: usize,
        session_key: &TransportSessionKey,
        media_id: TransportMediaId,
        packet_gate: PacketLayerGate,
        pending_packet_gate: Option<PacketLayerGate>,
    ) -> Result<bool, TransportAdapterError> {
        let changed = self.set_consumer_packet_gate_in_batch(
            source_id,
            destination_index,
            session_key,
            media_id,
            packet_gate,
            pending_packet_gate,
        )?;
        if changed {
            self.refresh_source_packet_gate(source_id);
        }
        Ok(changed)
    }

    pub(super) fn set_consumer_packet_gate_in_batch(
        &mut self,
        source_id: TransportMediaId,
        destination_index: usize,
        session_key: &TransportSessionKey,
        media_id: TransportMediaId,
        packet_gate: PacketLayerGate,
        pending_packet_gate: Option<PacketLayerGate>,
    ) -> Result<bool, TransportAdapterError> {
        let route = self
            .route_mut(source_id)
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        let destination = validate_destination(route, destination_index, session_key, media_id)?;
        let changed = destination.packet_gate != packet_gate
            || destination.pending_packet_gate != pending_packet_gate;
        destination.packet_gate = packet_gate;
        destination.pending_packet_gate = pending_packet_gate;
        Ok(changed)
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
        let update = {
            let Some(route) = self.route_mut(source_id) else {
                return RidReadinessRouteUpdate::default();
            };
            update_selected_rid_destinations(
                route,
                source_id,
                incoming_rid,
                is_keyframe,
                ready,
                stale,
                pending_selected,
            )
        };
        if update.changed_gate() {
            self.refresh_source_packet_gate(source_id);
        }
        update
    }

    pub(super) fn take_route(&mut self, source_id: TransportMediaId) -> Option<MediaRouteEntry> {
        let source = self.sources.get_mut(&source_id)?;
        let route = source.local_route.take();
        self.refresh_source_packet_gate(source_id);
        self.prune_remote_source_if_unrouted(source_id);
        self.prune_empty(source_id);
        route
    }

    pub(super) fn remove_destinations_for_session(&mut self, session_key: &TransportSessionKey) {
        let mut affected = Vec::new();
        for (source_id, source) in &mut self.sources {
            let Some(route) = source.local_route.as_mut() else {
                continue;
            };
            let destination_count = route.destinations.len();
            route
                .destinations
                .retain(|destination| destination.dest_session != *session_key);
            if route.destinations.len() == destination_count {
                continue;
            }
            route.active_destination_count = route
                .destinations
                .iter()
                .filter(|destination| destination.active)
                .count();
            if route.destinations.is_empty() {
                source.local_route = None;
            }
            affected.push(*source_id);
        }
        for source_id in affected {
            self.refresh_source_packet_gate(source_id);
            self.prune_remote_source_if_unrouted(source_id);
            self.prune_empty(source_id);
        }
    }

    pub(super) fn refresh_source_packet_gate(&mut self, source_id: TransportMediaId) {
        let local_packet_gate = self
            .local_route(source_id)
            .and_then(local_source_packet_gate);
        let remote_packet_gate =
            remote_packet_gate_for_route(self.local_route(source_id), local_packet_gate);
        self.set_local_packet_gate(source_id, local_packet_gate);
        self.publish_remote_packet_gate(source_id, remote_packet_gate);
    }

    pub(super) fn has_keyframe_demand(
        &self,
        source_id: TransportMediaId,
        rid: Option<Rid>,
    ) -> bool {
        let Some(source) = self.sources.get(&source_id) else {
            return false;
        };
        let local_demand = source.local_route.as_ref().is_some_and(|route| {
            route.source_active
                && route.destinations.iter().any(|destination| {
                    destination.active
                        && matches!(
                            super::media_registry::destination_keyframe_target_rid(
                                destination,
                                None
                            ),
                            super::media_registry::DestinationKeyframeTarget::Current(target_rid)
                                if target_rid == rid
                        )
                })
        });
        local_demand || source.active_relay_targets().is_some()
    }

    pub(super) fn register_local_source(&mut self, source_id: TransportMediaId) {
        self.producers.entry(source_id).or_default();
    }

    pub(super) fn unregister_local_source(&mut self, source_id: TransportMediaId) {
        if let Some(source) = self.sources.get_mut(&source_id) {
            source.forget_packet_state();
        }
        self.producers.remove(&source_id);
        self.keyframe_requests.forget_source(source_id);
        self.prune_empty(source_id);
    }

    pub(super) fn replace_producer_ssrcs(&mut self, source_id: TransportMediaId, ssrcs: Vec<Ssrc>) {
        self.producers.entry(source_id).or_default().ssrcs = ssrcs;
    }

    pub(super) fn remember_producer_ssrc(&mut self, source_id: TransportMediaId, ssrc: Ssrc) {
        if let Some(producer) = self.producers.get_mut(&source_id)
            && !producer.ssrcs.contains(&ssrc)
        {
            producer.ssrcs.push(ssrc);
        }
    }

    pub(super) fn clear_producer_ssrcs(
        &mut self,
        source_id: TransportMediaId,
    ) -> Option<Vec<Ssrc>> {
        self.producers
            .get_mut(&source_id)
            .map(|producer| mem::take(&mut producer.ssrcs))
    }

    pub(super) fn register_remote_source(
        &mut self,
        source: &TransportSourceKey,
        source_control: RemoteSourceControl,
    ) -> Result<Option<RemoteSourceRegistration>, TransportAdapterError> {
        let source_id = source.transport_media_id();
        let registration = RemoteSourceRegistration::new(source.clone(), source_control);
        match self.sources.entry(source_id) {
            Entry::Occupied(mut entry) => match entry.get().remote.as_ref() {
                Some(current) if current.source() == source => {
                    Ok(entry.get_mut().remote.replace(registration))
                }
                Some(_current) => Err(TransportAdapterError::InvalidInput),
                None => {
                    entry.get_mut().remote = Some(registration);
                    Ok(None)
                }
            },
            Entry::Vacant(entry) => {
                entry.insert(RouteSource {
                    remote: Some(registration),
                    ..RouteSource::default()
                });
                Ok(None)
            }
        }
    }

    pub(super) fn restore_remote_source(
        &mut self,
        source_id: TransportMediaId,
        previous_registration: Option<RemoteSourceRegistration>,
    ) {
        if let Some(previous_registration) = previous_registration {
            let pending = previous_registration.has_pending_packet_gate();
            self.sources.entry(source_id).or_default().remote = Some(previous_registration);
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
            .and_then(|source| source.remote.as_ref())
    }

    pub(super) fn publish_remote_packet_gate(
        &mut self,
        source_id: TransportMediaId,
        packet_gate: PacketLayerGate,
    ) {
        let pending = self
            .sources
            .get_mut(&source_id)
            .and_then(|source| source.remote.as_mut())
            .is_some_and(|registration| registration.publish_packet_gate(packet_gate));
        if pending {
            self.queue_remote_gate(source_id);
        }
    }

    pub(super) fn flush_remote_packet_gates(&mut self) {
        let count = self.remote_gate_queue.len();
        for index in 0..count {
            let Some(source_id) = self.remote_gate_queue.get(index).copied() else {
                break;
            };
            if self
                .sources
                .get_mut(&source_id)
                .and_then(|source| source.remote.as_mut())
                .is_some_and(RemoteSourceRegistration::flush_pending_packet_gate)
            {
                self.remote_gate_queue.push(source_id);
            }
        }
        self.remote_gate_queue.drain(..count);
    }

    pub(super) fn prune_remote_source_if_unrouted(&mut self, source_id: TransportMediaId) {
        if self
            .local_route(source_id)
            .is_some_and(|route| !route.destinations.is_empty())
        {
            return;
        }
        if self.remote_source(source_id).is_some() {
            self.remove_remote_source(source_id);
        }
    }

    pub(super) fn prune_unrouted_remote_sources(
        &mut self,
        mut keep_local: impl FnMut(&TransportMediaId) -> bool,
    ) {
        let mut source_ids = self.sources.keys().copied().collect::<Vec<_>>();
        source_ids.extend(
            self.producers
                .keys()
                .filter(|source_id| !self.sources.contains_key(source_id))
                .copied(),
        );
        for source_id in source_ids {
            let keep_registered = keep_local(&source_id) || self.remote_source(source_id).is_some();
            if self.remote_source(source_id).is_some() && self.local_route(source_id).is_none() {
                self.remove_remote_source(source_id);
                continue;
            }
            if !keep_registered {
                if let Some(source) = self.sources.get_mut(&source_id) {
                    source.forget_packet_state();
                }
                self.keyframe_requests.forget_source(source_id);
                self.producers.remove(&source_id);
            }
            self.prune_empty(source_id);
        }
    }

    pub(super) fn decoder_refresh_codec(
        &self,
        source_id: TransportMediaId,
    ) -> Option<DecoderRefreshCodec> {
        self.producers
            .get(&source_id)
            .and_then(|producer| producer.decoder)
    }

    pub(super) fn set_decoder_refresh_codec(
        &mut self,
        source_id: TransportMediaId,
        codec: Option<DecoderRefreshCodec>,
    ) {
        if let Some(codec) = codec {
            self.producers.entry(source_id).or_default().decoder = Some(codec);
        } else if let Some(producer) = self.producers.get_mut(&source_id) {
            producer.decoder = None;
        }
    }

    pub(super) fn observe_producer_packet(
        &mut self,
        source_id: TransportMediaId,
        rid: Option<Rid>,
        is_keyframe: bool,
        now: Instant,
    ) -> bool {
        if let Some(producer) = self.producers.get_mut(&source_id) {
            return producer.observe_packet(rid, is_keyframe, now);
        }
        let mut producer = ProducerPacketState::default();
        let observed = producer.observe_packet(rid, is_keyframe, now);
        if !producer.is_empty() {
            self.producers.insert(source_id, producer);
        }
        observed
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
                    let producer = self.producers.get(source_id);
                    let source_last_packet_age = incoming_bitrate_counters
                        .get(source_id)
                        .and_then(|counter| counter.last_observed_age(now));
                    source_diagnostic(*source_id, source_last_packet_age, producer, now)
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
        self.producers
            .get(&source_id)
            .and_then(|producer| producer.rids.iter().find(|liveness| liveness.rid() == rid))
            .is_some_and(|liveness| liveness.is_ready(now, max_age))
    }

    pub(super) fn collect_ready_producer_rids(
        &self,
        source_id: TransportMediaId,
        now: Instant,
        max_age: Duration,
        ready_rids: &mut Vec<Rid>,
    ) {
        ready_rids.clear();
        let Some(producer) = self.producers.get(&source_id) else {
            return;
        };
        ready_rids.extend(
            producer
                .rids
                .iter()
                .filter(|liveness| liveness.is_ready(now, max_age))
                .map(ProducerRidLiveness::rid),
        );
    }

    pub(super) fn schedule_rid_refresh(
        &mut self,
        source_id: TransportMediaId,
        rid: Rid,
        request_at: Instant,
    ) {
        let refresh = RidKeyframeRefresh {
            request_at,
            id: self.next_rid_refresh_id,
            source_id,
            rid,
        };
        self.next_rid_refresh_id = self.next_rid_refresh_id.saturating_add(1);
        self.producers
            .entry(source_id)
            .or_default()
            .pending_rid_refreshes
            .push(refresh);
        self.rid_refresh_heap.push(Reverse(refresh));
    }

    pub(super) fn drain_due_rid_refreshes(
        &mut self,
        source_id: TransportMediaId,
        rid: Rid,
        now: Instant,
    ) -> usize {
        let Some(producer) = self.producers.get_mut(&source_id) else {
            return 0;
        };
        let mut due_count = 0;
        producer.pending_rid_refreshes.retain(|refresh| {
            let due = refresh.rid == rid && refresh.request_at <= now;
            if due {
                due_count += 1;
            }
            !due
        });
        due_count
    }

    pub(super) fn drain_due_rid_refreshes_for_all(
        &mut self,
        now: Instant,
    ) -> Vec<(TransportMediaId, Rid)> {
        let mut due = Vec::new();
        while matches!(self.rid_refresh_heap.peek(), Some(Reverse(refresh)) if refresh.request_at <= now)
        {
            let Some(Reverse(refresh)) = self.rid_refresh_heap.pop() else {
                break;
            };
            if self.remove_pending_rid_refresh(refresh) {
                due.push((refresh.source_id, refresh.rid));
            }
        }
        due
    }

    pub(super) fn next_rid_refresh_deadline(&mut self) -> Option<Instant> {
        loop {
            let Reverse(refresh) = self.rid_refresh_heap.peek()?;
            if self.has_pending_rid_refresh(*refresh) {
                return Some(refresh.request_at);
            }
            self.rid_refresh_heap.pop();
        }
    }

    pub(super) fn track_keyframe_request(
        &mut self,
        source_id: TransportMediaId,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
        now: Instant,
    ) -> KeyframeRequestDecision {
        self.keyframe_requests.track(source_id, rid, kind, now)
    }

    pub(super) fn forget_keyframe_request(
        &mut self,
        source_id: TransportMediaId,
        rid: Option<Rid>,
    ) {
        self.keyframe_requests.forget(source_id, rid);
    }

    pub(super) fn observe_decoder_refresh(
        &mut self,
        source_id: TransportMediaId,
        rid: Option<Rid>,
    ) -> usize {
        self.keyframe_requests.observe_refresh(source_id, rid)
    }

    pub(super) fn drain_due_keyframe_requests(
        &mut self,
        now: Instant,
        retries: &mut Vec<SourceKeyframeRequest>,
    ) {
        self.keyframe_requests.drain_due(now, retries);
    }

    pub(super) fn next_keyframe_deadline(&self) -> Option<Instant> {
        self.keyframe_requests.next_deadline()
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
            .effective_packet_gate()
            .unwrap_or(PacketLayerGate::Open)
            .permits(metadata)
        {
            PacketRouteDecision::Forward
        } else {
            PacketRouteDecision::Drop
        }
    }

    pub(super) fn set_local_packet_gate(
        &mut self,
        source_id: TransportMediaId,
        packet_gate: Option<PacketLayerGate>,
    ) {
        match self.sources.entry(source_id) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().set_local_packet_gate(packet_gate);
                if entry.get().is_empty() {
                    entry.remove();
                }
            }
            Entry::Vacant(entry) => {
                let Some(packet_gate) = packet_gate else {
                    return;
                };
                let mut source = RouteSource::default();
                source.set_local_packet_gate(Some(packet_gate));
                entry.insert(source);
            }
        }
        debug!(
            ?source_id,
            effective_packet_gate = ?self.effective_packet_gate(source_id),
            "updated source packet gate"
        );
    }

    pub(super) fn observe_audio_activity(
        &mut self,
        source_id: TransportMediaId,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
    ) -> bool {
        if let Entry::Vacant(entry) = self.sources.entry(source_id) {
            if voice_activity.is_none() && audio_level_dbov.is_none() {
                return false;
            }
            let mut source = RouteSource::default();
            if !source.observe_audio_activity(voice_activity, audio_level_dbov, now) {
                return false;
            }
            if !source.is_empty() {
                entry.insert(source);
                return true;
            }
            return false;
        }
        let previous_active_speakers = self.room_policy_ranked_active_speaker_sources(now);
        let previous_packet_gate = self.effective_packet_gate(source_id);
        let Some(source) = self.sources.get_mut(&source_id) else {
            return false;
        };
        if !source.observe_audio_activity(voice_activity, audio_level_dbov, now) {
            return false;
        }
        previous_packet_gate != self.effective_packet_gate(source_id)
            || !same_active_speaker_order(
                &previous_active_speakers,
                &self.room_policy_ranked_active_speaker_sources(now),
            )
    }

    pub(super) fn set_relay_packet_gate(
        &mut self,
        source_id: TransportMediaId,
        target_id: RelayTargetId,
        packet_gate: PacketLayerGate,
    ) {
        self.sources
            .entry(source_id)
            .or_default()
            .set_relay_packet_gate(target_id, packet_gate);
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
        let mut sources = self.active_speaker_sources_unsorted(now);
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
        self.sources
            .iter()
            .filter_map(|(source_id, source)| source.active_speaker_diagnostic(*source_id, now))
            .collect()
    }

    pub(super) fn next_active_speaker_deadline(&self, now: Instant) -> Option<Instant> {
        self.sources
            .values()
            .filter_map(|source| source.next_active_speaker_deadline(now))
            .min()
    }

    pub(super) fn expired_active_speaker_source_ids(&self, now: Instant) -> Vec<TransportMediaId> {
        self.sources
            .iter()
            .filter_map(|(source_id, source)| {
                source.expired_active_speaker_at(now).then_some(*source_id)
            })
            .collect()
    }

    pub(super) fn effective_packet_gate(
        &self,
        source_id: TransportMediaId,
    ) -> Option<PacketLayerGate> {
        self.sources
            .get(&source_id)
            .and_then(RouteSource::effective_packet_gate)
    }

    #[cfg(test)]
    pub(super) fn relay_targets_for_source(
        &self,
        source_id: TransportMediaId,
    ) -> Option<&[ActiveRelayTarget]> {
        self.sources
            .get(&source_id)
            .and_then(RouteSource::active_relay_targets)
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
            .relay
            .get_or_insert_with(RelaySourceRegistration::default)
            .add_target(target_id, target);
    }

    pub(super) fn remove_relay_target(
        &mut self,
        source_id: TransportMediaId,
        target_id: RelayTargetId,
    ) {
        let Some(source) = self.sources.get_mut(&source_id) else {
            return;
        };
        let Some(registration) = source.relay.as_mut() else {
            return;
        };
        let removed = registration.contains_target(target_id);
        if registration.remove_target(target_id) {
            source.relay = None;
        }
        if removed {
            source.forget_relay_packet_gate(target_id);
        }
        if removed {
            self.prune_empty(source_id);
        }
    }

    pub(super) fn set_relay_target_active(
        &mut self,
        source_id: TransportMediaId,
        target_id: RelayTargetId,
        active: bool,
    ) {
        if let Some(source) = self.sources.get_mut(&source_id)
            && let Some(registration) = source.relay.as_mut()
        {
            registration.set_target_active(target_id, active);
        }
    }

    pub(super) fn is_relay_target_active(
        &self,
        source_id: TransportMediaId,
        target_id: RelayTargetId,
    ) -> bool {
        self.sources
            .get(&source_id)
            .and_then(|source| source.relay.as_ref())
            .is_some_and(|registration| registration.is_target_active(target_id))
    }

    #[cfg(test)]
    pub(super) fn relay_target_count(&self, source_id: TransportMediaId) -> usize {
        self.sources
            .get(&source_id)
            .and_then(|source| source.relay.as_ref())
            .map_or(0, RelaySourceRegistration::target_count)
    }

    #[cfg(test)]
    pub(super) fn active_relay_target_count(&self, source_id: TransportMediaId) -> usize {
        self.sources
            .get(&source_id)
            .and_then(|source| source.relay.as_ref())
            .map_or(0, RelaySourceRegistration::active_target_count)
    }

    fn active_speaker_sources_unsorted(&self, now: Instant) -> Vec<ActiveSpeakerSource> {
        self.sources
            .iter()
            .filter_map(|(source_id, source)| source.active_speaker_source(*source_id, now))
            .collect()
    }

    fn room_policy_ranked_active_speaker_sources(&self, now: Instant) -> Vec<ActiveSpeakerSource> {
        let mut sources = self.active_speaker_sources_unsorted(now);
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
            source.remote = None;
            source.forget_packet_state();
        }
        self.remote_gate_queue.retain(|queued| *queued != source_id);
        self.producers.remove(&source_id);
        self.keyframe_requests.forget_source(source_id);
        self.prune_empty(source_id);
    }

    fn queue_remote_gate(&mut self, source_id: TransportMediaId) {
        if !self.remote_gate_queue.contains(&source_id) {
            self.remote_gate_queue.push(source_id);
        }
    }

    fn has_pending_rid_refresh(&self, refresh: RidKeyframeRefresh) -> bool {
        self.producers
            .get(&refresh.source_id)
            .is_some_and(|producer| producer.pending_rid_refreshes.contains(&refresh))
    }

    fn remove_pending_rid_refresh(&mut self, refresh: RidKeyframeRefresh) -> bool {
        let Some(producer) = self.producers.get_mut(&refresh.source_id) else {
            return false;
        };
        let Some(position) = producer
            .pending_rid_refreshes
            .iter()
            .position(|pending| *pending == refresh)
        else {
            return false;
        };
        producer.pending_rid_refreshes.swap_remove(position);
        true
    }

    fn prune_empty(&mut self, source_id: TransportMediaId) {
        if self
            .sources
            .get(&source_id)
            .is_some_and(RouteSource::is_empty)
        {
            self.sources.remove(&source_id);
        }
    }
}

#[derive(Debug, Default)]
struct RouteSource {
    local_route: Option<MediaRouteEntry>,
    remote: Option<RemoteSourceRegistration>,
    relay: Option<RelaySourceRegistration>,
    audio: Option<SourceAudioPolicyState>,
    local_gate: Option<PacketLayerGate>,
    relay_gates: BTreeMap<RelayTargetId, PacketLayerGate>,
    gate: Option<PacketLayerGate>,
}

impl RouteSource {
    fn active_relay_targets(&self) -> Option<&[ActiveRelayTarget]> {
        self.relay.as_ref().and_then(|registration| {
            registration
                .has_active_targets()
                .then(|| registration.active_targets())
        })
    }

    fn active_speaker_source(
        &self,
        source_id: TransportMediaId,
        now: Instant,
    ) -> Option<ActiveSpeakerSource> {
        self.audio
            .as_ref()
            .and_then(|audio| audio.active_speaker_source(source_id, now))
    }

    fn active_speaker_diagnostic(
        &self,
        source_id: TransportMediaId,
        now: Instant,
    ) -> Option<ActiveSpeakerSourceDiagnostic> {
        self.audio
            .as_ref()
            .map(|audio| audio.diagnostic(source_id, now))
    }

    fn observe_audio_activity(
        &mut self,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
    ) -> bool {
        let previous = self.audio.clone();
        let Some(mut audio) = self.audio.take().or_else(|| {
            (voice_activity.is_some() || audio_level_dbov.is_some())
                .then(SourceAudioPolicyState::default)
        }) else {
            return false;
        };
        audio.observe_packet(voice_activity, audio_level_dbov, now);
        self.audio = Some(audio);
        let changed = self.audio != previous;
        if changed {
            self.refresh_gate();
        }
        changed
    }

    fn set_local_packet_gate(&mut self, gate: Option<PacketLayerGate>) {
        self.local_gate = gate;
        self.refresh_gate();
    }

    fn set_relay_packet_gate(&mut self, target_id: RelayTargetId, gate: PacketLayerGate) {
        self.relay_gates.insert(target_id, gate);
        self.refresh_gate();
    }

    fn relay_packet_gate(&self, target_id: RelayTargetId) -> Option<&PacketLayerGate> {
        self.relay_gates.get(&target_id)
    }

    fn forget_relay_packet_gate(&mut self, target_id: RelayTargetId) {
        self.relay_gates.remove(&target_id);
        self.refresh_gate();
    }

    const fn effective_packet_gate(&self) -> Option<PacketLayerGate> {
        self.gate
    }

    const fn packet_filter_gate(&self) -> Option<PacketLayerGate> {
        match self.gate {
            Some(PacketLayerGate::Open) | None => None,
            gate => gate,
        }
    }

    fn next_active_speaker_deadline(&self, now: Instant) -> Option<Instant> {
        self.audio
            .as_ref()
            .and_then(|audio| audio.active_deadline_after(now))
    }

    fn expired_active_speaker_at(&self, now: Instant) -> bool {
        self.audio
            .as_ref()
            .is_some_and(|audio| audio.expired_at(now))
    }

    fn forget_packet_state(&mut self) {
        self.audio = None;
        self.local_gate = None;
        self.relay_gates.clear();
        self.gate = None;
    }

    fn refresh_gate(&mut self) {
        self.gate = intersect_packet_gates(
            aggregate_packet_gates(self.local_gate.iter().chain(self.relay_gates.values())),
            self.audio.as_ref().map(SourceAudioPolicyState::packet_gate),
        );
    }

    fn is_empty(&self) -> bool {
        self.local_route.is_none()
            && self.remote.is_none()
            && self.relay.is_none()
            && self.audio.is_none()
            && self.local_gate.is_none()
            && self.relay_gates.is_empty()
    }
}

#[derive(Debug, Default)]
struct ProducerPacketState {
    ssrcs: Vec<Ssrc>,
    decoder: Option<DecoderRefreshCodec>,
    last_keyframe: Option<Instant>,
    rids: Vec<ProducerRidLiveness>,
    pending_rid_refreshes: Vec<RidKeyframeRefresh>,
}

impl ProducerPacketState {
    fn observe_packet(&mut self, rid: Option<Rid>, is_keyframe: bool, now: Instant) -> bool {
        let Some(rid) = rid else {
            if is_keyframe {
                self.last_keyframe = Some(now);
            }
            return false;
        };
        if let Some(liveness) = self.rids.iter_mut().find(|liveness| liveness.rid() == rid) {
            liveness.observe(is_keyframe, now);
            return false;
        }
        self.rids.push(ProducerRidLiveness::new_with_keyframe(
            rid,
            is_keyframe,
            now,
        ));
        true
    }

    fn is_empty(&self) -> bool {
        self.ssrcs.is_empty()
            && self.decoder.is_none()
            && self.last_keyframe.is_none()
            && self.rids.is_empty()
            && self.pending_rid_refreshes.is_empty()
    }
}

#[derive(Debug, Clone)]
struct ProducerRidLiveness {
    rid: Rid,
    last_seen: Instant,
    last_keyframe: Option<Instant>,
}

impl ProducerRidLiveness {
    fn new_with_keyframe(rid: Rid, is_keyframe: bool, observed_at: Instant) -> Self {
        Self {
            rid,
            last_seen: observed_at,
            last_keyframe: is_keyframe.then_some(observed_at),
        }
    }

    const fn rid(&self) -> Rid {
        self.rid
    }

    fn observe(&mut self, is_keyframe: bool, observed_at: Instant) {
        self.last_seen = observed_at;
        if is_keyframe {
            self.last_keyframe = Some(observed_at);
        }
    }

    fn is_ready(&self, now: Instant, max_age: Duration) -> bool {
        now.duration_since(self.last_seen) <= max_age
    }

    fn diagnostic(&self, now: Instant) -> TransportRidActivity {
        TransportRidActivity::new(
            self.rid.to_string(),
            now.saturating_duration_since(self.last_seen),
            self.last_keyframe
                .map(|last_keyframe| now.saturating_duration_since(last_keyframe)),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RidKeyframeRefresh {
    request_at: Instant,
    id: u64,
    source_id: TransportMediaId,
    rid: Rid,
}

impl Ord for RidKeyframeRefresh {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        (self.request_at, self.id).cmp(&(other.request_at, other.id))
    }
}

impl PartialOrd for RidKeyframeRefresh {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

fn source_diagnostic(
    source_id: TransportMediaId,
    source_last_packet_age: Option<Duration>,
    producer: Option<&ProducerPacketState>,
    now: Instant,
) -> Option<TransportSourceActivity> {
    let rids = producer.map_or(&[][..], |producer| producer.rids.as_slice());
    let rid_last_seen = rids.iter().map(|rid| rid.last_seen).max();
    let last_keyframe = producer
        .and_then(|producer| producer.last_keyframe)
        .into_iter()
        .chain(rids.iter().filter_map(|rid| rid.last_keyframe))
        .max();
    let last_packet_age = source_last_packet_age
        .or_else(|| rid_last_seen.map(|last_seen| now.saturating_duration_since(last_seen)))
        .or_else(|| {
            last_keyframe.map(|last_keyframe| now.saturating_duration_since(last_keyframe))
        })?;
    Some(TransportSourceActivity::new(
        source_id,
        last_packet_age,
        last_keyframe.map(|last_keyframe| now.saturating_duration_since(last_keyframe)),
        rids.iter().map(|rid| rid.diagnostic(now)).collect(),
    ))
}

fn validate_destination<'a>(
    route: &'a mut MediaRouteEntry,
    index: usize,
    session_key: &TransportSessionKey,
    media_id: TransportMediaId,
) -> Result<&'a mut MediaRouteDestination, TransportAdapterError> {
    let destination = route
        .destinations
        .get_mut(index)
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    if destination.dest_session != *session_key || destination.dest_transport_media_id != media_id {
        return Err(TransportAdapterError::TransportUnavailable);
    }
    Ok(destination)
}

fn update_selected_rid_destinations(
    route: &mut MediaRouteEntry,
    source_id: TransportMediaId,
    incoming_rid: Rid,
    is_keyframe: bool,
    ready: &[Rid],
    stale: &mut Vec<Rid>,
    pending_selected: &mut Vec<Rid>,
) -> RidReadinessRouteUpdate {
    let mut update = RidReadinessRouteUpdate::default();
    for destination in &mut route.destinations {
        suspend_stale_destination_gate(
            destination,
            source_id,
            incoming_rid,
            ready,
            stale,
            &mut update,
        );
        let Some(selected_rid) = destination
            .pending_packet_gate
            .as_ref()
            .and_then(PacketLayerGate::selected_rid)
        else {
            continue;
        };
        push_unique_rid(pending_selected, selected_rid);
        if selected_rid != incoming_rid {
            continue;
        }
        update.mark_pending_selected_gate();
        if is_keyframe && let Some(packet_gate) = destination.pending_packet_gate.take() {
            debug!(
                ?source_id,
                consumer_session_key = ?destination.dest_session,
                consumer_transport_media_id = ?destination.dest_transport_media_id,
                ?incoming_rid,
                activated_packet_gate = ?packet_gate,
                "activated deferred strict RID packet gate after producer RID became live"
            );
            destination.packet_gate = packet_gate;
            update.mark_activated_pending_gate();
        }
    }
    if is_keyframe && update.selected_gate != RidReadinessSelectedGateUpdate::Activated {
        activate_bootstrap_fallback_destinations(route, source_id, incoming_rid, &mut update);
    }
    update
}

fn suspend_stale_destination_gate(
    destination: &mut MediaRouteDestination,
    source_id: TransportMediaId,
    incoming_rid: Rid,
    ready: &[Rid],
    stale: &mut Vec<Rid>,
    update: &mut RidReadinessRouteUpdate,
) {
    if destination.pending_packet_gate.is_some() {
        return;
    }
    let Some(selected_rid) = destination.packet_gate.selected_rid() else {
        return;
    };
    if selected_rid == incoming_rid || ready.contains(&selected_rid) {
        return;
    }
    let packet_gate = destination.packet_gate;
    debug!(
        ?source_id,
        consumer_session_key = ?destination.dest_session,
        consumer_transport_media_id = ?destination.dest_transport_media_id,
        ?incoming_rid,
        stale_rid = ?selected_rid,
        pending_packet_gate = ?packet_gate,
        "blocked stale selected RID route until selected producer RID resumes"
    );
    destination.packet_gate = PacketLayerGate::Block;
    destination.pending_packet_gate = Some(packet_gate);
    push_unique_rid(stale, selected_rid);
    update.suspended_stale_gate = true;
}

fn activate_bootstrap_fallback_destinations(
    route: &mut MediaRouteEntry,
    source_id: TransportMediaId,
    incoming_rid: Rid,
    update: &mut RidReadinessRouteUpdate,
) {
    for destination in &mut route.destinations {
        let Some(selected_rid) = destination
            .pending_packet_gate
            .as_ref()
            .and_then(PacketLayerGate::selected_rid)
        else {
            continue;
        };
        if selected_rid == incoming_rid
            || !matches!(destination.packet_gate, PacketLayerGate::Block)
        {
            continue;
        }
        debug!(
            ?source_id,
            consumer_session_key = ?destination.dest_session,
            consumer_transport_media_id = ?destination.dest_transport_media_id,
            fallback_rid = ?incoming_rid,
            pending_selected_rid = ?selected_rid,
            "activated bootstrap fallback RID packet gate while selected producer RID is pending"
        );
        destination.packet_gate = PacketLayerGate::Rid(incoming_rid);
        update.mark_activated_bootstrap_fallback_gate();
    }
}

fn push_unique_rid(rids: &mut Vec<Rid>, rid: Rid) {
    if !rids.contains(&rid) {
        rids.push(rid);
    }
}

fn local_source_packet_gate(route_entry: &MediaRouteEntry) -> Option<PacketLayerGate> {
    aggregate_packet_gates(
        route_entry
            .destinations
            .iter()
            .filter(|destination| destination.active)
            .map(|destination| &destination.packet_gate),
    )
}

fn remote_packet_gate_for_route(
    route_entry: Option<&MediaRouteEntry>,
    local_packet_gate: Option<PacketLayerGate>,
) -> PacketLayerGate {
    match (route_entry, local_packet_gate) {
        (
            Some(_),
            Some(
                PacketLayerGate::Open
                | PacketLayerGate::Rid(_)
                | PacketLayerGate::OperatingPoint(_),
            ),
        ) => PacketLayerGate::Open,
        (Some(route_entry), Some(PacketLayerGate::Block))
            if route_entry
                .destinations
                .iter()
                .any(|destination| destination.pending_packet_gate.is_some()) =>
        {
            PacketLayerGate::Open
        }
        (_route_entry, Some(packet_gate)) => packet_gate,
        (None | Some(_), None) => PacketLayerGate::Block,
    }
}

fn same_active_speaker_order(left: &[ActiveSpeakerSource], right: &[ActiveSpeakerSource]) -> bool {
    left.iter()
        .map(|source| source.transport_media_id())
        .eq(right.iter().map(|source| source.transport_media_id()))
}
