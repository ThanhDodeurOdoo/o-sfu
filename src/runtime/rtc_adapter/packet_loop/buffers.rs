use std::net::SocketAddr;

use super::{
    super::{forwarded_packet::ForwardedPacket, forwarding_destination::PacketForward},
    keyframe_requests::{CoalescedKeyframeRequest, PendingKeyframeRequest},
};
use crate::runtime::{
    RoomInstanceId,
    transport_adapter::{SourcePolicySignal, TransportSessionKey},
};

pub(super) const RECEIVE_BUFFER_LEN: usize = 2000;
pub(super) const MAX_RELAY_PACKETS_PER_ITERATION: usize = 64;

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

/// Reusable buffers for the packet loop, allocated once and cleared per iteration
/// to avoid steady-state heap allocations.
pub(super) struct PacketLoopBuffers {
    pub(super) pending_transmits: Vec<PendingTransmit>,
    pub(super) pending_transmit_count: usize,
    pub(super) pending_packets: Vec<ForwardedPacket>,
    pub(super) relay_packets: Vec<Option<ForwardedPacket>>,
    pub(super) pending_keyframe_requests: Vec<(TransportSessionKey, PendingKeyframeRequest)>,
    pub(super) coalesced_keyframe_requests: Vec<CoalescedKeyframeRequest>,
    pub(super) dirty_source_policy_channel_ids: Vec<RoomInstanceId>,
    pub(super) forwards: Vec<PacketForward>,
}

impl PacketLoopBuffers {
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

    pub(super) fn clear(&mut self) {
        self.pending_transmit_count = 0;
        self.pending_packets.clear();
        self.relay_packets.clear();
        self.pending_keyframe_requests.clear();
        self.coalesced_keyframe_requests.clear();
        self.dirty_source_policy_channel_ids.clear();
        self.forwards.clear();
    }

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

    pub(super) fn pending_transmits(&self) -> impl Iterator<Item = &PendingTransmit> {
        self.pending_transmits
            .iter()
            .take(self.pending_transmit_count)
    }

    #[cfg(test)]
    pub(super) fn mark_source_policy_dirty(&mut self, room_instance_id: RoomInstanceId) {
        self.dirty_source_policy_channel_ids.push(room_instance_id);
    }

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
