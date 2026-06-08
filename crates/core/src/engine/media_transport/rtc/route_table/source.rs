use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use str0m::{
    media::{Mid, Rid},
    rtp::Ssrc,
};
use tracing::debug;

use super::rid_refresh::RidKeyframeRefresh;
use crate::engine::media_transport::{
    ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, TransportAdapterError, TransportMediaId,
    TransportRidActivity, TransportSessionKey, TransportSourceActivity, TransportSourceKey,
    rtc::{
        relay_registry::{
            ActiveRelayTarget, RelayPacketMailbox, RelaySourceRegistration, RelayTargetId,
        },
        route_control::{
            PacketLayerGate, SourceAudioPolicyState, aggregate_packet_gates, intersect_packet_gates,
        },
        source_route::{
            DecoderRefreshCodec, DestinationKeyframeTarget, MediaRouteDestination, MediaRouteEntry,
            RemoteSourceRegistration,
        },
    },
};

#[derive(Default)]
pub struct RidReadinessRouteUpdate {
    pub suspended_stale_gate: bool,
    pub selected_gate: RidReadinessSelectedGateUpdate,
}

impl RidReadinessRouteUpdate {
    pub fn changed_gate(&self) -> bool {
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
pub enum RidReadinessSelectedGateUpdate {
    #[default]
    None,
    Pending,
    Activated,
    BootstrapFallback,
}

#[derive(Debug)]
pub struct MovedConsumerRoute {
    pub session_key: TransportSessionKey,
    pub mid: Mid,
    pub media_id: TransportMediaId,
    pub dst_idx: usize,
}

#[derive(Debug)]
pub struct RemovedConsumerRoute {
    pub destination: MediaRouteDestination,
    pub moved: Option<MovedConsumerRoute>,
    pub(super) stopped_forwarding: bool,
}

#[derive(Clone, Copy)]
pub(super) enum ForwardingChange {
    StillForwarding,
    StoppedForwarding,
}

impl ForwardingChange {
    pub(super) const fn stopped_forwarding(self) -> bool {
        matches!(self, Self::StoppedForwarding)
    }
}

pub(super) struct RemovedRoute {
    pub(super) route: MediaRouteEntry,
    pub(super) stopped_forwarding: bool,
}

#[derive(Debug, Default)]
pub(super) struct RouteSource {
    local_route: Option<MediaRouteEntry>,
    remote: Option<RemoteSourceRegistration>,
    remote_gate_queued: bool,
    relay: Option<RelaySourceRegistration>,
    audio: Option<SourceAudioPolicyState>,
    local_gate: Option<PacketLayerGate>,
    relay_gates: BTreeMap<RelayTargetId, PacketLayerGate>,
    gate: Option<PacketLayerGate>,
    pub(super) producer: ProducerSourceState,
}

impl RouteSource {
    pub(super) fn local_route(&self) -> Option<&MediaRouteEntry> {
        self.local_route.as_ref()
    }

    pub(super) fn remote(&self) -> Option<&RemoteSourceRegistration> {
        self.remote.as_ref()
    }

    pub(super) fn active_relay_targets(&self) -> Option<&[ActiveRelayTarget]> {
        self.relay.as_ref().and_then(|registration| {
            registration
                .has_active_targets()
                .then(|| registration.active_targets())
        })
    }

    pub(super) fn add_consumer_route(
        &mut self,
        destination: MediaRouteDestination,
    ) -> (usize, bool) {
        let became_forwarding = self.local_route.is_none() && self.relay.is_none();
        let route = self
            .local_route
            .get_or_insert_with(|| MediaRouteEntry::new(true));
        let index = route.destinations.len();
        route.push_destination(destination);
        (index, became_forwarding)
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
        let destination = route.remove_destination(index);
        let moved = route
            .destinations
            .get(index)
            .map(|destination| MovedConsumerRoute {
                session_key: destination.dest_session.clone(),
                mid: destination.dest_mid,
                media_id: destination.dest_transport_media_id,
                dst_idx: index,
            });
        let stopped_forwarding = route.destinations.is_empty() && self.relay.is_none();
        if route.destinations.is_empty() {
            self.local_route = None;
        }
        Some(RemovedConsumerRoute {
            destination,
            moved,
            stopped_forwarding,
        })
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

    pub(super) fn take_route(&mut self) -> Option<RemovedRoute> {
        let route = self.local_route.take()?;
        Some(RemovedRoute {
            stopped_forwarding: self.relay.is_none(),
            route,
        })
    }

    pub(super) fn remove_dsts_for_session(
        &mut self,
        session_key: &TransportSessionKey,
    ) -> Option<ForwardingChange> {
        let route = self.local_route.as_mut()?;
        let destination_count = route.destinations.len();
        route
            .destinations
            .retain(|destination| destination.dest_session != *session_key);
        if route.destinations.len() == destination_count {
            return None;
        }
        route.active_destination_count = route
            .destinations
            .iter()
            .filter(|destination| destination.active)
            .count();
        let stopped_forwarding = route.destinations.is_empty() && self.relay.is_none();
        if route.destinations.is_empty() {
            self.local_route = None;
        }
        Some(if stopped_forwarding {
            ForwardingChange::StoppedForwarding
        } else {
            ForwardingChange::StillForwarding
        })
    }

    pub(super) fn refresh_route_pkt_gate(&mut self) -> PacketLayerGate {
        let local_packet_gate = self.local_route.as_ref().and_then(local_src_pkt_gate);
        let remote_packet_gate =
            remote_pkt_gate_for_route(self.local_route.as_ref(), local_packet_gate);
        self.set_local_pkt_gate(local_packet_gate);
        remote_packet_gate
    }

    pub(super) fn has_kf_demand(&self, rid: Option<Rid>) -> bool {
        let local_demand = self.local_route.as_ref().is_some_and(|route| {
            route.source_active
                && route.destinations.iter().any(|destination| {
                    destination.active
                        && matches!(
                            destination.keyframe_target_rid(None),
                            DestinationKeyframeTarget::Current(target_rid)
                                if target_rid == rid
                        )
                })
        });
        local_demand || self.active_relay_targets().is_some()
    }

    pub(super) fn register_remote_source(
        &mut self,
        source: &TransportSourceKey,
        registration: RemoteSourceRegistration,
    ) -> Result<Option<RemoteSourceRegistration>, TransportAdapterError> {
        match self.remote.as_ref() {
            Some(current) if current.source() == source => Ok(self.remote.replace(registration)),
            Some(_current) => Err(TransportAdapterError::InvalidInput),
            None => {
                self.remote = Some(registration);
                Ok(None)
            }
        }
    }

    pub(super) fn restore_remote_source(&mut self, registration: RemoteSourceRegistration) {
        self.remote = Some(registration);
    }

    pub(super) fn remove_remote_source(&mut self) {
        self.remote = None;
        self.remote_gate_queued = false;
    }

    pub(super) fn publish_remote_pkt_gate(&mut self, packet_gate: PacketLayerGate) -> bool {
        let needs_retry = self
            .remote
            .as_mut()
            .is_some_and(|registration| registration.publish_packet_gate_needs_retry(packet_gate));
        if needs_retry && !self.remote_gate_queued {
            self.remote_gate_queued = true;
            return true;
        }
        false
    }

    pub(super) fn flush_remote_pkt_gate(&mut self) -> bool {
        let needs_retry = self
            .remote
            .as_mut()
            .is_some_and(RemoteSourceRegistration::flush_pending_gate);
        self.remote_gate_queued = needs_retry;
        needs_retry
    }

    pub(super) fn queue_remote_gate(&mut self) -> bool {
        if self.remote_gate_queued {
            return false;
        }
        self.remote_gate_queued = true;
        true
    }

    pub(super) fn diagnostic(
        &self,
        source_id: TransportMediaId,
        source_last_packet_age: Option<Duration>,
        now: Instant,
    ) -> Option<TransportSourceActivity> {
        let rids = self.producer.rids.as_slice();
        let rid_last_seen = rids.iter().map(|rid| rid.last_seen).max();
        let last_keyframe = self
            .producer
            .last_keyframe
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

    pub(super) fn observe_audio_activity(
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

    pub(super) fn set_local_pkt_gate(&mut self, gate: Option<PacketLayerGate>) {
        self.local_gate = gate;
        self.refresh_gate();
    }

    pub(super) fn set_relay_pkt_gate(&mut self, target_id: RelayTargetId, gate: PacketLayerGate) {
        self.relay_gates.insert(target_id, gate);
        self.refresh_gate();
    }

    pub(super) fn relay_packet_gate(&self, target_id: RelayTargetId) -> Option<&PacketLayerGate> {
        self.relay_gates.get(&target_id)
    }

    pub(super) fn next_active_speaker_deadline(&self, now: Instant) -> Option<Instant> {
        self.audio
            .as_ref()
            .and_then(|audio| audio.active_deadline_after(now))
    }

    pub(super) const fn effective_packet_gate(&self) -> Option<PacketLayerGate> {
        self.gate
    }

    pub(super) const fn packet_filter_gate(&self) -> Option<PacketLayerGate> {
        match self.gate {
            Some(PacketLayerGate::Open) | None => None,
            gate => gate,
        }
    }

    pub(super) fn add_relay_target(
        &mut self,
        target_id: RelayTargetId,
        target: RelayPacketMailbox,
    ) -> bool {
        let became_forwarding = self.local_route.is_none() && self.relay.is_none();
        self.relay
            .get_or_insert_with(RelaySourceRegistration::default)
            .add_target(target_id, target);
        became_forwarding
    }

    pub(super) fn remove_relay_target(
        &mut self,
        target_id: RelayTargetId,
    ) -> Option<ForwardingChange> {
        let registration = self.relay.as_mut()?;
        let removed = registration.contains_target(target_id);
        if registration.remove_target(target_id) {
            self.relay = None;
        }
        if removed {
            self.forget_relay_packet_gate(target_id);
            return Some(if self.relay.is_none() && self.local_route.is_none() {
                ForwardingChange::StoppedForwarding
            } else {
                ForwardingChange::StillForwarding
            });
        }
        None
    }

    pub(super) fn set_relay_target_active(&mut self, target_id: RelayTargetId, active: bool) {
        if let Some(registration) = self.relay.as_mut() {
            registration.set_target_active(target_id, active);
        }
    }

    pub(super) fn is_relay_target_active(&self, target_id: RelayTargetId) -> bool {
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

    pub(super) fn forget_packet_state(&mut self) {
        self.audio = None;
        self.local_gate = None;
        self.relay_gates.clear();
        self.gate = None;
        self.producer = ProducerSourceState::default();
    }

    pub(super) fn is_empty(&self) -> bool {
        self.local_route.is_none()
            && self.remote.is_none()
            && self.relay.is_none()
            && self.audio.is_none()
            && self.local_gate.is_none()
            && self.relay_gates.is_empty()
            && self.producer.is_empty()
    }

    fn forget_relay_packet_gate(&mut self, target_id: RelayTargetId) {
        self.relay_gates.remove(&target_id);
        self.refresh_gate();
    }

    fn refresh_gate(&mut self) {
        self.gate = intersect_packet_gates(
            aggregate_packet_gates(self.local_gate.iter().chain(self.relay_gates.values())),
            self.audio.as_ref().map(SourceAudioPolicyState::packet_gate),
        );
    }
}

#[derive(Debug, Default)]
pub(super) struct ProducerSourceState {
    pub(super) registered: bool,
    pub(super) ssrcs: Vec<Ssrc>,
    pub(super) decoder: Option<DecoderRefreshCodec>,
    last_keyframe: Option<Instant>,
    rids: Vec<ProducerRidLiveness>,
    pub(super) pending_rid_refreshes: Vec<RidKeyframeRefresh>,
}

impl ProducerSourceState {
    pub(super) fn observe_packet(
        &mut self,
        rid: Option<Rid>,
        is_keyframe: bool,
        now: Instant,
    ) -> bool {
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
        self.rids.push(ProducerRidLiveness::new_with_keyframe(
            rid,
            is_keyframe,
            now,
        ));
        true
    }

    pub(super) fn rid_is_ready(&self, rid: Rid, now: Instant, max_age: Duration) -> bool {
        self.rids
            .iter()
            .any(|liveness| liveness.rid == rid && liveness.is_ready(now, max_age))
    }

    pub(super) fn collect_ready_rids(
        &self,
        now: Instant,
        max_age: Duration,
        ready_rids: &mut Vec<Rid>,
    ) {
        ready_rids.extend(
            self.rids
                .iter()
                .filter(|liveness| liveness.is_ready(now, max_age))
                .map(|liveness| liveness.rid),
        );
    }

    pub(super) fn is_empty(&self) -> bool {
        !self.registered
            && self.ssrcs.is_empty()
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
