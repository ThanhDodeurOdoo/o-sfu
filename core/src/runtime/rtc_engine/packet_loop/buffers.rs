//! Packet loop buffers
//!
//! The packet loop is a long-lived task, so temporary per-turn storage belongs
//! in one reusable allocation surface instead of being rebuilt while packets are
//! flowing. This module owns that surface. Callers borrow the vectors during one
//! turn, then call [`PacketLoopBuffers::clear`] before the next turn to reset
//! logical length while keeping capacity.
//!
//! The buffers do not own durable routing state. Durable state stays in
//! `RtcBootstrapState`, `RtcSnapshotState`, worker-local relay target maps or
//! packet sinks.
//! Values stored here are staged work that must either be flushed during the
//! current turn or dropped as part of clearing the turn.
//!
//! `pending_transmits` deliberately keeps physical slots beyond
//! `pending_transmit_count`. That lets `str0m` transmit payloads overwrite
//! previous byte buffers instead of allocating a fresh `Vec<u8>` for each
//! outbound datagram.

use std::net::SocketAddr;

use super::{
    super::{forwarded_packet::ForwardedPacket, forwarding_destination::PacketForward},
    keyframe_requests::{CoalescedKeyframeRequest, PendingKeyframeRequest},
};
use crate::runtime::{
    RoomInstanceId,
    media_transport::{SourcePolicySignal, TransportSessionKey},
};

pub(super) const RECEIVE_BUFFER_LEN: usize = 2000;
pub(super) const MAX_RELAY_PACKETS_PER_ITERATION: usize = 64;

/// One queued UDP datagram ready to be written to the shard socket.
///
/// The payload buffer is reused across turns. A slot can remain allocated after
/// it leaves the logical transmit list, so readers must access transmits
/// through [`PacketLoopBuffers::pending_transmits`] instead of iterating the
/// backing vector directly.
#[derive(Debug)]
pub(super) struct PendingTransmit {
    pub(super) destination: SocketAddr,
    pub(super) contents: Vec<u8>,
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
}

/// Per-worker scratch buffers reused across packet-loop turns.
///
/// # Hot-path contract
///
/// The packet loop owns one instance for the lifetime of the worker task.
/// Calling code may push staged work during a turn, but no field is
/// authoritative after the turn is flushed. New reusable collections should be
/// added here only when they replace repeated hot-path allocation or preserve a
/// bounded batch between two packet-loop phases.
pub(super) struct PacketLoopBuffers {
    /// Reusable UDP transmit slots produced by `str0m::Output::Transmit`.
    pub(super) pending_transmits: Vec<PendingTransmit>,
    /// Logical length of [`Self::pending_transmits`] for the current turn.
    pub(super) pending_transmit_count: usize,
    /// Media packets produced by local adapter sessions or inbound relays.
    pub(super) pending_packets: Vec<ForwardedPacket>,
    /// Shared relay packet cache keyed by `pending_packets` index.
    ///
    /// Relay flush can have multiple relay destinations for one packet. This
    /// vector ensures the payload is promoted to shared ownership once for that
    /// source packet, then reused by all relay destinations in the same turn.
    pub(super) relay_packets: Vec<Option<ForwardedPacket>>,
    /// Raw keyframe feedback emitted by consumer sessions before source lookup.
    pub(super) pending_keyframe_requests: Vec<(TransportSessionKey, PendingKeyframeRequest)>,
    /// Source-keyed feedback after duplicate requests are merged.
    pub(super) coalesced_keyframe_requests: Vec<CoalescedKeyframeRequest>,
    /// Rooms whose source policy must be recomputed after packet observations.
    pub(super) dirty_source_policy_channel_ids: Vec<RoomInstanceId>,
    /// Concrete forwarding destinations planned for `pending_packets`.
    pub(super) forwards: Vec<PacketForward>,
}

impl PacketLoopBuffers {
    /// Build the reusable buffer set with small initial capacities.
    ///
    /// The capacities are only starting points. Dense rooms may grow them once,
    /// after which normal `.clear()` calls keep the larger allocation for later
    /// turns.
    pub(super) fn new() -> Self {
        Self {
            pending_transmits: Vec::with_capacity(64),
            pending_transmit_count: 0,
            pending_packets: Vec::with_capacity(32),
            relay_packets: Vec::with_capacity(32),
            pending_keyframe_requests: Vec::with_capacity(8),
            coalesced_keyframe_requests: Vec::with_capacity(8),
            dirty_source_policy_channel_ids: Vec::with_capacity(8),
            forwards: Vec::with_capacity(64),
        }
    }

    /// Reset all staged work while retaining allocation capacity.
    ///
    /// This must run before a new packet-loop turn starts. It intentionally
    /// leaves `pending_transmits` slots allocated because each slot owns a byte
    /// buffer that is cheaper to overwrite than recreate.
    pub(super) fn clear(&mut self) {
        self.pending_transmit_count = 0;
        self.pending_packets.clear();
        self.relay_packets.clear();
        self.pending_keyframe_requests.clear();
        self.coalesced_keyframe_requests.clear();
        self.dirty_source_policy_channel_ids.clear();
        self.forwards.clear();
    }

    /// Queue a UDP transmit by overwriting an existing slot when possible.
    ///
    /// `str0m` owns the source transmit buffer, so the packet loop must copy the
    /// bytes before the async `send_to` await point. Reusing slots bounds that
    /// copy to existing capacity after warmup.
    pub(super) fn push_pending_transmit(&mut self, destination: SocketAddr, contents: &[u8]) {
        if let Some(slot) = self.pending_transmits.get_mut(self.pending_transmit_count) {
            slot.overwrite(destination, contents);
        } else {
            let mut slot = PendingTransmit::empty();
            slot.overwrite(destination, contents);
            self.pending_transmits.push(slot);
        }
        self.pending_transmit_count = self.pending_transmit_count.saturating_add(1);
    }

    /// Iterate only the logical transmit slots staged for this turn.
    pub(super) fn pending_transmits(&self) -> impl Iterator<Item = &PendingTransmit> {
        self.pending_transmits
            .iter()
            .take(self.pending_transmit_count)
    }

    #[cfg(test)]
    pub(super) fn mark_source_policy_dirty(&mut self, room_instance_id: RoomInstanceId) {
        self.dirty_source_policy_channel_ids.push(room_instance_id);
    }

    /// Coalesce source-policy wakeups and publish them to the room policy layer.
    ///
    /// Audio activity and receiver bandwidth changes can mark the same room
    /// dirty multiple times during one turn. Sorting and deduplicating here
    /// keeps the external wakeup cost proportional to changed rooms, not packet
    /// count.
    pub(super) fn flush_source_policy_dirty(&mut self, source_policy_signal: &SourcePolicySignal) {
        if self.dirty_source_policy_channel_ids.is_empty() {
            return;
        }
        self.dirty_source_policy_channel_ids.sort_unstable();
        self.dirty_source_policy_channel_ids.dedup();
        source_policy_signal.mark_dirty_rooms(self.dirty_source_policy_channel_ids.iter().copied());
        self.dirty_source_policy_channel_ids.clear();
    }
}
