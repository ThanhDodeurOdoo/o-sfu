//! Packet-loop scratch storage
//!
//! The packet loop is a long-lived task, so temporary per-turn storage belongs
//! in one reusable allocation surface instead of being rebuilt while packets are
//! flowing. This module contains that surface. Callers borrow scoped scratch
//! views during one turn, then call [`PacketLoopScratch::clear`] before the next turn to reset
//! logical length while keeping capacity.
//!
//! The scratch does not own durable routing state. Values stored here are
//! staged work that must either be flushed during the current turn or dropped
//! as part of clearing the turn.

use std::net::SocketAddr;

use str0m::media::Rid;

use super::super::{
    super::{forwarded_packet::ForwardedPacket, forwarding_destination::PacketForward},
    keyframe_requests::{CoalescedKeyframeRequest, PendingKeyframeRequest},
};
#[cfg(any(test, feature = "packet-loop-verification"))]
use crate::runtime::metrics::RtpForwardDestinationKind;
use crate::runtime::{RoomInstanceId, media_transport::TransportSessionKey};

pub(in crate::runtime::rtc_engine::packet_loop) const RECEIVE_BUFFER_LEN: usize = 2000;
pub const MAX_RELAY_PACKETS_PER_ITERATION: usize = 64;

/// One queued UDP datagram ready to be written to the shard socket.
///
/// The payload buffer is reused across turns. A slot can remain allocated after
/// it leaves the logical transmit list, so readers must access transmits
/// through [`PacketLoopScratch::pending_transmits`] instead of iterating the
/// backing vector directly.
#[derive(Debug)]
pub(in crate::runtime::rtc_engine::packet_loop) struct PendingTransmit {
    destination: SocketAddr,
    contents: Vec<u8>,
}

impl PendingTransmit {
    fn empty() -> Self {
        Self {
            destination: SocketAddr::from(([0, 0, 0, 0], 0)),
            contents: Vec::new(),
        }
    }

    fn overwrite(&mut self, destination: SocketAddr, contents: &[u8]) {
        self.destination = destination;
        self.contents.clear();
        self.contents.extend_from_slice(contents);
    }

    pub(in crate::runtime::rtc_engine::packet_loop) const fn destination(&self) -> SocketAddr {
        self.destination
    }

    pub(in crate::runtime::rtc_engine::packet_loop) fn contents(&self) -> &[u8] {
        &self.contents
    }
}

/// Per-worker scratch storage reused across packet-loop turns.
///
/// # Hot-path contract
///
/// The packet loop owns one instance for the lifetime of the worker task.
/// Calling code may push staged work during a turn, but it must go through
/// phase-specific methods so reusable buffers stay owned by one object.
pub struct PacketLoopScratch {
    pending_transmits: Vec<PendingTransmit>,
    pending_transmit_count: usize,
    pending_packets: Vec<ForwardedPacket>,
    relay_packets: Vec<Option<ForwardedPacket>>,
    pending_keyframe_requests: Vec<(TransportSessionKey, PendingKeyframeRequest)>,
    coalesced_keyframe_requests: Vec<CoalescedKeyframeRequest>,
    dirty_source_policy_room_ids: Vec<RoomInstanceId>,
    forwards: Vec<PacketForward>,
    rid_readiness: RidReadinessScratch,
}

impl PacketLoopScratch {
    /// Build the reusable scratch set with small initial capacities.
    ///
    /// The capacities are only starting points. Dense rooms may grow them once,
    /// after which normal `.clear()` calls keep the larger allocation for later
    /// turns.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pending_transmits: Vec::with_capacity(64),
            pending_transmit_count: 0,
            pending_packets: Vec::with_capacity(32),
            relay_packets: Vec::with_capacity(32),
            pending_keyframe_requests: Vec::with_capacity(8),
            coalesced_keyframe_requests: Vec::with_capacity(8),
            dirty_source_policy_room_ids: Vec::with_capacity(8),
            forwards: Vec::with_capacity(64),
            rid_readiness: RidReadinessScratch::default(),
        }
    }

    /// Reset all staged work while retaining allocation capacity.
    ///
    /// This must run before a new packet-loop turn starts. It intentionally
    /// leaves `pending_transmits` slots allocated because each slot owns a byte
    /// buffer that is cheaper to overwrite than recreate.
    pub fn clear(&mut self) {
        self.pending_transmit_count = 0;
        self.pending_packets.clear();
        self.relay_packets.clear();
        self.pending_keyframe_requests.clear();
        self.coalesced_keyframe_requests.clear();
        self.dirty_source_policy_room_ids.clear();
        self.forwards.clear();
        self.rid_readiness.clear();
    }

    /// Queue a UDP transmit by overwriting an existing slot when possible.
    ///
    /// `str0m` owns the source transmit buffer, so the packet loop must copy the
    /// bytes before the async `send_to` await point. Reusing slots bounds that
    /// copy to existing capacity after warmup.
    pub fn push_pending_transmit(&mut self, destination: SocketAddr, contents: &[u8]) {
        if let Some(slot) = self.pending_transmits.get_mut(self.pending_transmit_count) {
            slot.overwrite(destination, contents);
        } else {
            let mut slot = PendingTransmit::empty();
            slot.overwrite(destination, contents);
            self.pending_transmits.push(slot);
        }
        self.pending_transmit_count = self.pending_transmit_count.saturating_add(1);
    }

    #[cfg(test)]
    pub(in crate::runtime::rtc_engine::packet_loop) fn pending_transmit(
        &self,
        transmit_idx: usize,
    ) -> Option<&PendingTransmit> {
        if transmit_idx >= self.pending_transmit_count {
            return None;
        }
        self.pending_transmits.get(transmit_idx)
    }

    pub(in crate::runtime::rtc_engine::packet_loop) fn pending_transmits(
        &self,
    ) -> impl Iterator<Item = &PendingTransmit> {
        self.pending_transmits
            .iter()
            .take(self.pending_transmit_count)
    }

    #[cfg(any(test, feature = "packet-loop-verification"))]
    pub(in crate::runtime::rtc_engine::packet_loop) fn pending_packet_count(&self) -> usize {
        self.pending_packets.len()
    }

    #[cfg(test)]
    pub(in crate::runtime::rtc_engine::packet_loop) fn forward(
        &self,
        forward_idx: usize,
    ) -> Option<&PacketForward> {
        self.forwards.get(forward_idx)
    }

    #[cfg(any(test, feature = "packet-loop-verification"))]
    #[must_use]
    pub fn forward_count(&self) -> usize {
        self.forwards.len()
    }

    #[cfg(any(test, feature = "packet-loop-verification"))]
    #[must_use]
    pub fn forward_count_by_destination_kind(
        &self,
        destination_kind: RtpForwardDestinationKind,
    ) -> usize {
        self.forwards
            .iter()
            .filter(|forward| forward.destination.metrics_kind() == destination_kind)
            .count()
    }

    pub(in crate::runtime::rtc_engine::packet_loop) fn push_pending_packet(
        &mut self,
        packet: ForwardedPacket,
    ) {
        self.pending_packets.push(packet);
    }

    pub(in crate::runtime::rtc_engine::packet_loop) fn push_pending_keyframe_request(
        &mut self,
        session_key: TransportSessionKey,
        request: PendingKeyframeRequest,
    ) {
        self.pending_keyframe_requests.push((session_key, request));
    }

    pub(in crate::runtime::rtc_engine::packet_loop) fn observe_pending_packets(
        &mut self,
        mut observe: impl FnMut(usize, &mut ForwardedPacket, &mut PacketObservationScratch<'_>),
    ) {
        let mut observation_scratch = PacketObservationScratch {
            dirty_source_policy_room_ids: &mut self.dirty_source_policy_room_ids,
            rid_readiness: &mut self.rid_readiness,
        };
        for (packet_idx, packet) in self.pending_packets.iter_mut().enumerate() {
            observe(packet_idx, packet, &mut observation_scratch);
        }
    }

    pub(in crate::runtime::rtc_engine::packet_loop) fn plan_pending_packets(
        &mut self,
        mut plan: impl FnMut(usize, &mut ForwardedPacket, &mut Vec<PacketForward>),
    ) {
        for (packet_idx, packet) in self.pending_packets.iter_mut().enumerate() {
            plan(packet_idx, packet, &mut self.forwards);
        }
    }

    pub(in crate::runtime::rtc_engine::packet_loop) fn with_forwarding_buffers(
        &mut self,
        mut flush: impl FnMut(
            &[PacketForward],
            &mut [ForwardedPacket],
            &mut Vec<Option<ForwardedPacket>>,
        ),
    ) {
        self.relay_packets.clear();
        self.relay_packets
            .resize_with(self.pending_packets.len(), || None);
        flush(
            &self.forwards,
            &mut self.pending_packets,
            &mut self.relay_packets,
        );
    }

    pub(in crate::runtime::rtc_engine::packet_loop) fn with_pending_packets(
        &self,
        observe: impl FnOnce(&[ForwardedPacket]),
    ) {
        observe(&self.pending_packets);
    }

    pub(in crate::runtime::rtc_engine::packet_loop) fn rebuild_coalesced_keyframe_requests(
        &mut self,
        mut resolve: impl FnMut(
            TransportSessionKey,
            PendingKeyframeRequest,
        ) -> Option<CoalescedKeyframeRequest>,
    ) {
        self.coalesced_keyframe_requests.clear();
        for (consumer_session_key, request) in self.pending_keyframe_requests.drain(..) {
            if let Some(request) = resolve(consumer_session_key, request) {
                self.coalesced_keyframe_requests.push(request);
            }
        }
        self.coalesced_keyframe_requests
            .sort_by_key(|request| request.source_transport_media_id);
    }

    pub(in crate::runtime::rtc_engine::packet_loop) fn drain_coalesced_keyframe_requests(
        &mut self,
    ) -> impl Iterator<Item = CoalescedKeyframeRequest> + '_ {
        self.coalesced_keyframe_requests.drain(..)
    }

    pub fn dirty_source_policy_rooms(&mut self) -> &[RoomInstanceId] {
        self.dirty_source_policy_room_ids.sort_unstable();
        self.dirty_source_policy_room_ids.dedup();
        &self.dirty_source_policy_room_ids
    }

    pub(in crate::runtime::rtc_engine::packet_loop) fn clear_dirty_source_policy_rooms(&mut self) {
        self.dirty_source_policy_room_ids.clear();
    }

    #[cfg(any(test, feature = "packet-loop-verification"))]
    pub fn mark_source_policy_dirty(&mut self, room_instance_id: RoomInstanceId) {
        self.dirty_source_policy_room_ids.push(room_instance_id);
    }

    #[cfg(test)]
    pub(in crate::runtime::rtc_engine::packet_loop) fn push_forward(
        &mut self,
        forward: PacketForward,
    ) {
        self.forwards.push(forward);
    }

    #[cfg(test)]
    pub(in crate::runtime::rtc_engine::packet_loop) fn forwards(&self) -> &[PacketForward] {
        &self.forwards
    }

    #[cfg(any(test, feature = "packet-loop-verification"))]
    #[must_use]
    pub fn is_turn_empty(&self) -> bool {
        self.pending_transmit_count == 0
            && self.pending_packets.is_empty()
            && self.relay_packets.is_empty()
            && self.pending_keyframe_requests.is_empty()
            && self.coalesced_keyframe_requests.is_empty()
            && self.dirty_source_policy_room_ids.is_empty()
            && self.forwards.is_empty()
            && self.rid_readiness.is_empty()
    }

    #[cfg(any(test, feature = "packet-loop-verification"))]
    #[must_use]
    pub fn capacities(&self) -> PacketLoopScratchCapacities {
        PacketLoopScratchCapacities {
            pending_transmits: self.pending_transmits.capacity(),
            pending_packets: self.pending_packets.capacity(),
            relay_packets: self.relay_packets.capacity(),
            pending_keyframe_requests: self.pending_keyframe_requests.capacity(),
            coalesced_keyframe_requests: self.coalesced_keyframe_requests.capacity(),
            dirty_source_policy_room_ids: self.dirty_source_policy_room_ids.capacity(),
            forwards: self.forwards.capacity(),
            rid_readiness_ready: self.rid_readiness.ready.capacity(),
            rid_readiness_stale: self.rid_readiness.stale.capacity(),
            rid_readiness_pending_selected: self.rid_readiness.pending_selected.capacity(),
        }
    }
}

pub(in crate::runtime::rtc_engine::packet_loop) struct PacketObservationScratch<'a> {
    dirty_source_policy_room_ids: &'a mut Vec<RoomInstanceId>,
    rid_readiness: &'a mut RidReadinessScratch,
}

impl PacketObservationScratch<'_> {
    pub(in crate::runtime::rtc_engine::packet_loop) fn mark_source_policy_dirty(
        &mut self,
        room_instance_id: RoomInstanceId,
    ) {
        self.dirty_source_policy_room_ids.push(room_instance_id);
    }

    pub(in crate::runtime::rtc_engine::packet_loop) fn rid_readiness(
        &mut self,
    ) -> &mut RidReadinessScratch {
        self.rid_readiness
    }
}

#[derive(Default)]
pub(in crate::runtime::rtc_engine) struct RidReadinessScratch {
    pub(in crate::runtime::rtc_engine) ready: Vec<Rid>,
    pub(in crate::runtime::rtc_engine) stale: Vec<Rid>,
    pub(in crate::runtime::rtc_engine) pending_selected: Vec<Rid>,
}

impl RidReadinessScratch {
    pub(in crate::runtime::rtc_engine) fn clear(&mut self) {
        self.ready.clear();
        self.stale.clear();
        self.pending_selected.clear();
    }

    #[cfg(any(test, feature = "packet-loop-verification"))]
    fn is_empty(&self) -> bool {
        self.ready.is_empty() && self.stale.is_empty() && self.pending_selected.is_empty()
    }
}

#[cfg(any(test, feature = "packet-loop-verification"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PacketLoopScratchCapacities {
    pub pending_transmits: usize,
    pub pending_packets: usize,
    pub relay_packets: usize,
    pub pending_keyframe_requests: usize,
    pub coalesced_keyframe_requests: usize,
    pub dirty_source_policy_room_ids: usize,
    pub forwards: usize,
    pub rid_readiness_ready: usize,
    pub rid_readiness_stale: usize,
    pub rid_readiness_pending_selected: usize,
}

impl Default for PacketLoopScratch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(test, feature = "packet-loop-verification"))]
impl PacketLoopScratchCapacities {
    #[must_use]
    pub fn retained_at_least(self, earlier: Self) -> bool {
        self.pending_transmits >= earlier.pending_transmits
            && self.pending_packets >= earlier.pending_packets
            && self.relay_packets >= earlier.relay_packets
            && self.pending_keyframe_requests >= earlier.pending_keyframe_requests
            && self.coalesced_keyframe_requests >= earlier.coalesced_keyframe_requests
            && self.dirty_source_policy_room_ids >= earlier.dirty_source_policy_room_ids
            && self.forwards >= earlier.forwards
            && self.rid_readiness_ready >= earlier.rid_readiness_ready
            && self.rid_readiness_stale >= earlier.rid_readiness_stale
            && self.rid_readiness_pending_selected >= earlier.rid_readiness_pending_selected
    }
}
