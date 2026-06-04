use std::{
    collections::BTreeMap,
    mem,
    time::{Duration, Instant},
};

use str0m::{
    media::{KeyframeRequestKind, Mid, Rid},
    rtp::Ssrc,
};
use tracing::debug;

use super::super::{
    commands::RemoteSourceControl,
    demux::{MediaRouteDestination, MediaRouteEntry},
    keyframe_tracker::{
        KeyframeRequestDeadline, KeyframeRequestDecision, SourceKeyframeDeadline,
        SourceKeyframeRequest, SourceKeyframeRequests,
    },
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
    TransportRidActivity, TransportSessionKey, TransportSourceActivity, TransportSourceKey,
};

pub(super) type MovedConsumerRoute = (TransportSessionKey, Mid, TransportMediaId, usize);
pub(super) type RemovedConsumerRoute = (MediaRouteDestination, Option<MovedConsumerRoute>);

#[derive(Default)]
pub(in crate::engine::media_transport::rtc) struct RidReadinessRouteUpdate {
    suspended_stale_gate: bool,
    pub(in crate::engine::media_transport::rtc) selected_gate: RidReadinessSelectedGateUpdate,
}

impl RidReadinessRouteUpdate {
    pub(in crate::engine::media_transport::rtc) fn changed_gate(&self) -> bool {
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

    fn mark_bootstrap_fallback(&mut self) {
        self.selected_gate = RidReadinessSelectedGateUpdate::BootstrapFallback;
    }
}

#[derive(Default, PartialEq, Eq)]
pub(in crate::engine::media_transport::rtc) enum RidReadinessSelectedGateUpdate {
    #[default]
    None,
    Pending,
    Activated,
    BootstrapFallback,
}

#[derive(Debug, Default)]
pub(super) struct SourceRouteState {
    local_route: Option<MediaRouteEntry>,
    remote: Option<RemoteSourceRegistration>,
    relay: Option<RelaySourceRegistration>,
    audio: Option<SourceAudioPolicyState>,
    local_gate: Option<PacketLayerGate>,
    relay_gates: BTreeMap<RelayTargetId, PacketLayerGate>,
    gate: Option<PacketLayerGate>,
    producer: ProducerPacketState,
    keyframes: SourceKeyframeRequests,
}

impl SourceRouteState {
    pub(super) fn forward_view(
        &self,
        include_relays: bool,
    ) -> (
        Option<&MediaRouteEntry>,
        Option<&[ActiveRelayTarget]>,
        Option<PacketLayerGate>,
    ) {
        let relays = include_relays
            .then(|| self.active_relay_targets())
            .flatten();
        let packet_gate = match self.gate {
            Some(PacketLayerGate::Open) | None => None,
            Some(gate) => Some(gate),
        };
        (self.local_route.as_ref(), relays, packet_gate)
    }

    pub(super) fn local_route(&self) -> Option<&MediaRouteEntry> {
        self.local_route.as_ref()
    }

    pub(super) fn has_local_dsts(&self) -> bool {
        self.local_route
            .as_ref()
            .is_some_and(|route| !route.destinations.is_empty())
    }

    pub(super) fn has_kf_demand(&self, rid: Option<Rid>) -> bool {
        let local_demand = self.local_route.as_ref().is_some_and(|route| {
            route.source_active
                && route.destinations.iter().any(|destination| {
                    destination.active
                        && matches!(
                            super::super::media_registry::dst_kf_target_rid(destination, None),
                            super::super::media_registry::DestinationKeyframeTarget::Current(
                                target_rid
                            ) if target_rid == rid
                        )
                })
        });
        local_demand || self.active_relay_targets().is_some()
    }

    pub(super) fn add_consumer_route(&mut self, destination: MediaRouteDestination) -> usize {
        let route = self
            .local_route
            .get_or_insert_with(|| MediaRouteEntry::new(true));
        let index = route.destinations.len();
        route.push_destination(destination);
        index
    }

    pub(super) fn take_local_route(&mut self) -> Option<MediaRouteEntry> {
        self.local_route.take()
    }

    pub(super) fn remove_consumer_route(
        &mut self,
        session_key: &TransportSessionKey,
        media_id: TransportMediaId,
    ) -> Option<RemovedConsumerRoute> {
        let route = self.local_route.as_mut()?;
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
        if route.destinations.is_empty() {
            self.local_route = None;
        }
        Some((removed, moved))
    }

    pub(super) fn remove_dsts_for_session(&mut self, session_key: &TransportSessionKey) -> bool {
        let Some(route) = self.local_route.as_mut() else {
            return false;
        };
        let destination_count = route.destinations.len();
        route
            .destinations
            .retain(|destination| destination.dest_session != *session_key);
        if route.destinations.len() == destination_count {
            return false;
        }
        route.active_destination_count = route
            .destinations
            .iter()
            .filter(|destination| destination.active)
            .count();
        if route.destinations.is_empty() {
            self.local_route = None;
        }
        true
    }

    pub(super) fn set_source_active(&mut self, active: bool) -> Result<(), TransportAdapterError> {
        let route = self
            .local_route
            .as_mut()
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        route.source_active = active;
        Ok(())
    }

    pub(super) fn set_consumer_active(
        &mut self,
        dst_idx: usize,
        session_key: &TransportSessionKey,
        media_id: TransportMediaId,
        active: bool,
    ) -> Result<bool, TransportAdapterError> {
        let route = self
            .local_route
            .as_mut()
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        validate_destination(route, dst_idx, session_key, media_id)?;
        Ok(route.set_destination_active(dst_idx, active))
    }

    pub(super) fn set_consumer_pkt_gate(
        &mut self,
        dst_idx: usize,
        session_key: &TransportSessionKey,
        media_id: TransportMediaId,
        packet_gate: PacketLayerGate,
        pending_gate: Option<PacketLayerGate>,
    ) -> Result<bool, TransportAdapterError> {
        let route = self
            .local_route
            .as_mut()
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        let dst = validate_destination(route, dst_idx, session_key, media_id)?;
        let changed = dst.packet_gate != packet_gate || dst.pending_gate != pending_gate;
        dst.packet_gate = packet_gate;
        dst.pending_gate = pending_gate;
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
        let Some(route) = self.local_route.as_mut() else {
            return RidReadinessRouteUpdate::default();
        };
        update_selected_rid_dsts(
            route,
            source_id,
            incoming_rid,
            is_keyframe,
            ready,
            stale,
            pending_selected,
        )
    }

    pub(super) fn remote_source(&self) -> Option<&RemoteSourceRegistration> {
        self.remote.as_ref()
    }

    pub(super) fn register_remote_source(
        &mut self,
        source: &TransportSourceKey,
        source_control: RemoteSourceControl,
    ) -> Result<Option<RemoteSourceRegistration>, TransportAdapterError> {
        if self
            .remote
            .as_ref()
            .is_some_and(|current| current.source() != source)
        {
            return Err(TransportAdapterError::InvalidInput);
        }
        Ok(self.remote.replace(RemoteSourceRegistration::new(
            source.clone(),
            source_control,
        )))
    }

    pub(super) fn restore_remote_source(&mut self, registration: RemoteSourceRegistration) -> bool {
        let pending = registration.has_pending_gate();
        self.remote = Some(registration);
        pending
    }

    pub(super) fn clear_remote_source(&mut self) {
        self.remote = None;
        self.clear_packet_state();
    }

    pub(super) fn publish_remote_pkt_gate(&mut self, packet_gate: PacketLayerGate) -> bool {
        self.remote
            .as_mut()
            .is_some_and(|registration| registration.publish_packet_gate(packet_gate))
    }

    pub(super) fn flush_remote_pkt_gate(&mut self) -> bool {
        self.remote
            .as_mut()
            .is_some_and(RemoteSourceRegistration::flush_pending_gate)
    }

    fn active_relay_targets(&self) -> Option<&[ActiveRelayTarget]> {
        self.relay.as_ref().and_then(|registration| {
            registration
                .has_active_targets()
                .then(|| registration.active_targets())
        })
    }

    pub(super) fn add_relay_target(
        &mut self,
        target_id: RelayTargetId,
        target: RelayPacketMailbox,
    ) {
        self.relay
            .get_or_insert_with(RelaySourceRegistration::default)
            .add_target(target_id, target);
    }

    pub(super) fn remove_relay_target(&mut self, target_id: RelayTargetId) -> bool {
        let Some(registration) = self.relay.as_mut() else {
            return false;
        };
        let removed = registration.contains_target(target_id);
        if registration.remove_target(target_id) {
            self.relay = None;
        }
        if removed {
            self.forget_relay_packet_gate(target_id);
        }
        removed
    }

    pub(super) fn set_relay_target_active(&mut self, target_id: RelayTargetId, active: bool) {
        if let Some(registration) = self.relay.as_mut() {
            registration.set_target_active(target_id, active);
        }
    }

    pub(super) fn relay_target_is_active(&self, target_id: RelayTargetId) -> bool {
        self.relay
            .as_ref()
            .is_some_and(|registration| registration.is_target_active(target_id))
    }

    #[cfg(test)]
    pub(super) fn relay_target_count(&self) -> usize {
        self.relay
            .as_ref()
            .map_or(0, RelaySourceRegistration::target_count)
    }

    #[cfg(test)]
    pub(super) fn active_relay_target_count(&self) -> usize {
        self.relay
            .as_ref()
            .map_or(0, RelaySourceRegistration::active_target_count)
    }

    pub(super) fn observe_audio_activity(
        &mut self,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
    ) -> (bool, bool) {
        if self.audio.is_none() && voice_activity.is_none() && audio_level_dbov.is_none() {
            return (false, false);
        }
        let previous = self.audio.clone();
        self.audio
            .get_or_insert_with(SourceAudioPolicyState::default)
            .observe_packet(voice_activity, audio_level_dbov, now);
        let changed = self.audio != previous;
        if changed {
            self.refresh_gate();
        }
        (changed, self.audio.is_some())
    }

    pub(super) fn active_speaker_source(
        &self,
        source_id: TransportMediaId,
        now: Instant,
    ) -> Option<ActiveSpeakerSource> {
        self.audio
            .as_ref()
            .and_then(|audio| audio.active_speaker_source(source_id, now))
    }

    pub(super) fn active_speaker_diagnostic(
        &self,
        source_id: TransportMediaId,
        now: Instant,
    ) -> Option<ActiveSpeakerSourceDiagnostic> {
        self.audio
            .as_ref()
            .map(|audio| audio.diagnostic(source_id, now))
    }

    pub(super) fn active_deadline_after(&self, now: Instant) -> Option<Instant> {
        self.audio
            .as_ref()
            .and_then(|audio| audio.active_deadline_after(now))
    }

    pub(super) fn expired_active_speaker_at(&self, now: Instant) -> bool {
        self.audio
            .as_ref()
            .is_some_and(|audio| audio.expired_at(now))
    }

    #[cfg(test)]
    pub(super) fn set_test_local_pkt_gate(&mut self, gate: Option<PacketLayerGate>) {
        self.local_gate = gate;
        self.refresh_gate();
    }

    pub(super) fn refresh_route_pkt_gate(&mut self) -> PacketLayerGate {
        let local_gate = self.local_route.as_ref().and_then(local_src_pkt_gate);
        let remote_gate = remote_pkt_gate_for_route(self.local_route.as_ref(), local_gate);
        self.local_gate = local_gate;
        self.refresh_gate();
        remote_gate
    }

    pub(super) fn set_relay_pkt_gate(&mut self, target_id: RelayTargetId, gate: PacketLayerGate) {
        self.relay_gates.insert(target_id, gate);
        self.refresh_gate();
    }

    fn forget_relay_packet_gate(&mut self, target_id: RelayTargetId) {
        self.relay_gates.remove(&target_id);
        self.refresh_gate();
    }

    pub(super) fn relay_packet_gate(&self, target_id: RelayTargetId) -> Option<&PacketLayerGate> {
        self.relay_gates.get(&target_id)
    }

    pub(super) fn packet_gate(&self) -> Option<PacketLayerGate> {
        self.gate
    }

    pub(super) fn replace_producer_ssrcs(&mut self, ssrcs: Vec<Ssrc>) {
        self.producer.ssrcs = ssrcs;
    }

    pub(super) fn remember_producer_ssrc(&mut self, ssrc: Ssrc) {
        self.producer.remember_ssrc(ssrc);
    }

    pub(super) fn take_producer_ssrcs(&mut self) -> Vec<Ssrc> {
        self.producer.take_ssrcs()
    }

    pub(super) fn decoder_refresh_codec(&self) -> Option<DecoderRefreshCodec> {
        self.producer.decoder
    }

    pub(super) fn set_decoder_refresh_codec(&mut self, codec: Option<DecoderRefreshCodec>) {
        self.producer.decoder = codec;
    }

    pub(super) fn observe_producer_packet(
        &mut self,
        rid: Option<Rid>,
        is_keyframe: bool,
        now: Instant,
    ) -> bool {
        self.producer.observe_packet(rid, is_keyframe, now)
    }

    pub(super) fn producer_rid_is_ready(&self, rid: Rid, now: Instant, max_age: Duration) -> bool {
        self.producer.rid_is_ready(rid, now, max_age)
    }

    pub(super) fn collect_ready_producer_rids(
        &self,
        now: Instant,
        max_age: Duration,
        ready_rids: &mut Vec<Rid>,
    ) {
        self.producer.collect_ready_rids(now, max_age, ready_rids);
    }

    pub(super) fn source_activity(
        &self,
        source_id: TransportMediaId,
        source_last_packet_age: Option<Duration>,
        now: Instant,
    ) -> Option<TransportSourceActivity> {
        self.producer
            .source_activity(source_id, source_last_packet_age, now)
    }

    pub(super) fn schedule_rid_refresh(&mut self, refresh: RidKeyframeRefresh) {
        self.producer.pending_rid_refreshes.push(refresh);
    }

    pub(super) fn drain_due_rid_refreshes(&mut self, rid: Rid, now: Instant) -> usize {
        self.producer.drain_due_rid_refreshes(rid, now)
    }

    pub(super) fn has_pending_rid_refresh(&self, id: u64) -> bool {
        self.producer.has_pending_rid_refresh(id)
    }

    pub(super) fn remove_pending_rid_refresh(&mut self, id: u64) -> Option<Rid> {
        self.producer.remove_pending_rid_refresh(id)
    }

    pub(super) fn track_kf_req(
        &mut self,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
        now: Instant,
        id: u64,
    ) -> (KeyframeRequestDecision, Option<SourceKeyframeDeadline>) {
        self.keyframes.track(rid, kind, now, id)
    }

    pub(super) fn forget_kf_req(&mut self, rid: Option<Rid>) {
        self.keyframes.forget(rid);
    }

    pub(super) fn observe_decoder_refresh(&mut self, rid: Option<Rid>) -> usize {
        self.keyframes.observe_refresh(rid)
    }

    pub(super) fn drain_due_kf_req(
        &mut self,
        deadline: Instant,
        id: u64,
        src_media: TransportMediaId,
        now: Instant,
        next_id: u64,
    ) -> Option<(SourceKeyframeRequest, KeyframeRequestDeadline)> {
        self.keyframes
            .drain_due(deadline, id, src_media, now, next_id)
    }

    pub(super) fn has_kf_deadline(&self, deadline: SourceKeyframeDeadline) -> bool {
        self.keyframes.has_deadline(deadline)
    }

    pub(super) fn clear_packet_state(&mut self) {
        self.audio = None;
        self.local_gate = None;
        self.relay_gates.clear();
        self.gate = None;
        self.producer = ProducerPacketState::default();
        self.keyframes = SourceKeyframeRequests::default();
    }

    pub(super) fn is_empty(&self) -> bool {
        self.local_route.is_none()
            && self.remote.is_none()
            && self.relay.is_none()
            && self.audio.is_none()
            && self.local_gate.is_none()
            && self.relay_gates.is_empty()
            && self.producer.is_empty()
            && self.keyframes.is_empty()
    }

    fn refresh_gate(&mut self) {
        self.gate = intersect_packet_gates(
            aggregate_packet_gates(self.local_gate.iter().chain(self.relay_gates.values())),
            self.audio.as_ref().map(SourceAudioPolicyState::packet_gate),
        );
    }
}

fn validate_destination<'a>(
    route: &'a mut MediaRouteEntry,
    index: usize,
    session_key: &TransportSessionKey,
    media_id: TransportMediaId,
) -> Result<&'a mut MediaRouteDestination, TransportAdapterError> {
    let dst = route
        .destinations
        .get_mut(index)
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    if dst.dest_session != *session_key || dst.dest_transport_media_id != media_id {
        return Err(TransportAdapterError::TransportUnavailable);
    }
    Ok(dst)
}

fn update_selected_rid_dsts(
    route: &mut MediaRouteEntry,
    source_id: TransportMediaId,
    incoming_rid: Rid,
    is_keyframe: bool,
    ready: &[Rid],
    stale: &mut Vec<Rid>,
    pending_selected: &mut Vec<Rid>,
) -> RidReadinessRouteUpdate {
    let mut update = RidReadinessRouteUpdate::default();
    for dst in &mut route.destinations {
        suspend_stale_dst_gate(dst, source_id, incoming_rid, ready, stale, &mut update);
        let Some(selected_rid) = dst
            .pending_gate
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
        if is_keyframe && let Some(packet_gate) = dst.pending_gate.take() {
            debug!(
                ?source_id,
                consumer_session_key = ?dst.dest_session,
                consumer_transport_media_id = ?dst.dest_transport_media_id,
                ?incoming_rid,
                activated_packet_gate = ?packet_gate,
                "activated deferred strict RID packet gate after producer RID became live"
            );
            dst.packet_gate = packet_gate;
            update.mark_activated_pending_gate();
        }
    }
    if is_keyframe && update.selected_gate != RidReadinessSelectedGateUpdate::Activated {
        activate_bootstrap_dsts(route, source_id, incoming_rid, &mut update);
    }
    update
}

fn suspend_stale_dst_gate(
    dst: &mut MediaRouteDestination,
    source_id: TransportMediaId,
    incoming_rid: Rid,
    ready: &[Rid],
    stale: &mut Vec<Rid>,
    update: &mut RidReadinessRouteUpdate,
) {
    if dst.pending_gate.is_some() {
        return;
    }
    let Some(selected_rid) = dst.packet_gate.selected_rid() else {
        return;
    };
    if selected_rid == incoming_rid || ready.contains(&selected_rid) {
        return;
    }
    let packet_gate = dst.packet_gate;
    debug!(
        ?source_id,
        consumer_session_key = ?dst.dest_session,
        consumer_transport_media_id = ?dst.dest_transport_media_id,
        ?incoming_rid,
        stale_rid = ?selected_rid,
        pending_packet_gate = ?packet_gate,
        "blocked stale selected RID route until selected producer RID resumes"
    );
    dst.packet_gate = PacketLayerGate::Block;
    dst.pending_gate = Some(packet_gate);
    push_unique_rid(stale, selected_rid);
    update.suspended_stale_gate = true;
}

fn activate_bootstrap_dsts(
    route: &mut MediaRouteEntry,
    source_id: TransportMediaId,
    incoming_rid: Rid,
    update: &mut RidReadinessRouteUpdate,
) {
    for dst in &mut route.destinations {
        let Some(selected_rid) = dst
            .pending_gate
            .as_ref()
            .and_then(PacketLayerGate::selected_rid)
        else {
            continue;
        };
        if selected_rid == incoming_rid || !matches!(dst.packet_gate, PacketLayerGate::Block) {
            continue;
        }
        debug!(
            ?source_id,
            consumer_session_key = ?dst.dest_session,
            consumer_transport_media_id = ?dst.dest_transport_media_id,
            fallback_rid = ?incoming_rid,
            pending_selected_rid = ?selected_rid,
            "activated bootstrap fallback RID packet gate while selected producer RID is pending"
        );
        dst.packet_gate = PacketLayerGate::Rid(incoming_rid);
        update.mark_bootstrap_fallback();
    }
}

fn push_unique_rid(rids: &mut Vec<Rid>, rid: Rid) {
    if !rids.contains(&rid) {
        rids.push(rid);
    }
}

fn local_src_pkt_gate(route_entry: &MediaRouteEntry) -> Option<PacketLayerGate> {
    aggregate_packet_gates(
        route_entry
            .destinations
            .iter()
            .filter(|destination| destination.active)
            .map(|destination| &destination.packet_gate),
    )
}

fn remote_pkt_gate_for_route(
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
                .any(|destination| destination.pending_gate.is_some()) =>
        {
            PacketLayerGate::Open
        }
        (_route_entry, Some(packet_gate)) => packet_gate,
        (None | Some(_), None) => PacketLayerGate::Block,
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
    fn remember_ssrc(&mut self, ssrc: Ssrc) {
        if !self.ssrcs.contains(&ssrc) {
            self.ssrcs.push(ssrc);
        }
    }

    fn take_ssrcs(&mut self) -> Vec<Ssrc> {
        mem::take(&mut self.ssrcs)
    }

    fn observe_packet(&mut self, rid: Option<Rid>, is_keyframe: bool, now: Instant) -> bool {
        let Some(rid) = rid else {
            if is_keyframe {
                self.last_keyframe = Some(now);
            }
            return false;
        };
        if let Some(liveness) = self.rids.iter_mut().find(|liveness| liveness.rid == rid) {
            liveness.observe(is_keyframe, now);
            return false;
        }
        self.rids.push(ProducerRidLiveness {
            rid,
            last_seen: now,
            last_keyframe: is_keyframe.then_some(now),
        });
        true
    }

    fn rid_is_ready(&self, rid: Rid, now: Instant, max_age: Duration) -> bool {
        self.rids
            .iter()
            .find(|liveness| liveness.rid == rid)
            .is_some_and(|liveness| now.duration_since(liveness.last_seen) <= max_age)
    }

    fn collect_ready_rids(&self, now: Instant, max_age: Duration, ready_rids: &mut Vec<Rid>) {
        ready_rids.clear();
        ready_rids.extend(
            self.rids
                .iter()
                .filter(|liveness| now.duration_since(liveness.last_seen) <= max_age)
                .map(|liveness| liveness.rid),
        );
    }

    fn source_activity(
        &self,
        source_id: TransportMediaId,
        source_last_packet_age: Option<Duration>,
        now: Instant,
    ) -> Option<TransportSourceActivity> {
        let rid_last_seen = self.rids.iter().map(|rid| rid.last_seen).max();
        let last_keyframe = self
            .last_keyframe
            .into_iter()
            .chain(self.rids.iter().filter_map(|rid| rid.last_keyframe))
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
            self.rids.iter().map(|rid| rid.diagnostic(now)).collect(),
        ))
    }

    fn is_empty(&self) -> bool {
        self.ssrcs.is_empty()
            && self.decoder.is_none()
            && self.last_keyframe.is_none()
            && self.rids.is_empty()
            && self.pending_rid_refreshes.is_empty()
    }

    fn drain_due_rid_refreshes(&mut self, rid: Rid, now: Instant) -> usize {
        let mut due_count = 0;
        self.pending_rid_refreshes.retain(|refresh| {
            let (request_at, _id, refresh_rid) = *refresh;
            let due = refresh_rid == rid && request_at <= now;
            if due {
                due_count += 1;
            }
            !due
        });
        due_count
    }

    fn has_pending_rid_refresh(&self, id: u64) -> bool {
        self.pending_rid_refreshes
            .iter()
            .any(|(_request_at, refresh_id, _rid)| *refresh_id == id)
    }

    fn remove_pending_rid_refresh(&mut self, id: u64) -> Option<Rid> {
        let position = self
            .pending_rid_refreshes
            .iter()
            .position(|(_request_at, pending_id, _rid)| *pending_id == id)?;
        let (_request_at, _id, rid) = self.pending_rid_refreshes.swap_remove(position);
        Some(rid)
    }
}

#[derive(Debug)]
struct ProducerRidLiveness {
    rid: Rid,
    last_seen: Instant,
    last_keyframe: Option<Instant>,
}

impl ProducerRidLiveness {
    fn observe(&mut self, is_keyframe: bool, observed_at: Instant) {
        self.last_seen = observed_at;
        if is_keyframe {
            self.last_keyframe = Some(observed_at);
        }
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

pub(super) type RidKeyframeRefresh = (Instant, u64, Rid);
