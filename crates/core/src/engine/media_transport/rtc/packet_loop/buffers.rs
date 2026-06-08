//! Packet loop buffers
//!
//! The packet loop is a long-lived task, so temporary per-turn storage belongs
//! in one reusable allocation surface instead of being rebuilt while packets are
//! flowing. This module contains that surface. Callers borrow the vectors during one
//! turn, then call [`PacketLoopBuffers::clear`] before the next turn to reset
//! logical length while keeping capacity.
//!
//! The buffers do not own durable routing state. Durable state stays in
//! `PacketLoopState`, `RtcSnapshotState`, worker-local relay target maps or
//! packet sinks.
//! Values stored here are staged work that must either be flushed during the
//! current turn or dropped as part of clearing the turn.

use std::{net::SocketAddr, time::Instant};

use str0m::media::Rid;

use super::{
    super::{
        forwarded_packet::{ForwardedPacket, ForwardedPacketSource},
        forwarding_destination::PacketForward,
        keyframe_tracker::SourceKeyframeRequest,
        slots::SessionHandle,
    },
    keyframe_requests::PendingKeyframeRequest,
};
use crate::engine::{
    RoomInstanceId,
    media_transport::{SourcePolicySignal, TransportMediaId, TransportSessionKey},
};

pub(super) const RECEIVE_BUFFER_LEN: usize = 2000;
pub(super) const MAX_RELAY_PACKETS_PER_ITERATION: usize = 64;

/// One queued UDP datagram ready to be written to the worker socket.
///
/// The backing storage is reused across turns, while the payload buffer is moved
/// from `str0m` output into the socket send path.
#[derive(Debug)]
pub(super) struct PendingTransmit {
    pub(super) destination: SocketAddr,
    pub(super) contents: Vec<u8>,
}

pub(super) struct PendingRidReadiness {
    pub(super) source: ForwardedPacketSource,
    pub(super) src_media: TransportMediaId,
    pub(super) rid: Rid,
    pub(super) is_keyframe: bool,
    pub(super) observed_at: Instant,
}

pub(super) struct PendingFirstVideoKeyframe {
    pub(super) source: ForwardedPacketSource,
    pub(super) src_media: TransportMediaId,
    pub(super) observed_at: Instant,
}

/// per-worker scratch buffers reused across packet-loop turns
///
/// # hot-path contract
///
/// the packet loop owns one instance for the lifetime of the worker task
/// calling code may push staged work during a turn, but no field is
/// authoritative after the turn is flushed
/// new reusable collections should be
/// added here only when they replace repeated hot-path allocation or preserve a
/// bounded batch between two packet-loop phases
pub struct PacketLoopBuffers {
    /// reusable UDP transmit slots produced by `str0m::Output::Transmit`
    pub(super) pending_transmits: Vec<PendingTransmit>,
    /// media packets produced by local adapter sessions or inbound relays
    pub pending_packets: Vec<ForwardedPacket>,
    /// raw keyframe feedback emitted by consumer sessions before source lookup
    pub pending_keyframe_requests: Vec<(TransportSessionKey, PendingKeyframeRequest)>,
    /// sessions ready for polling after dirty and timeout scheduling is merged
    pub(super) ready_sessions: Vec<SessionHandle>,
    /// source-keyed feedback after duplicate requests are merged
    pub(super) coalesced_keyframe_requests: Vec<SourceKeyframeRequest>,
    /// due keyframe retries drained from the tracker
    pub(super) keyframe_retries: Vec<SourceKeyframeRequest>,
    /// source/RID-keyed readiness work after packet-level liveness is updated
    pub(super) pending_rid_readiness: Vec<PendingRidReadiness>,
    /// first-ingress keyframe probes delayed until RID readiness work is known
    pub(super) pending_first_video_keyframes: Vec<PendingFirstVideoKeyframe>,
    /// sources whose selected-RID route state changed during this turn
    pub(super) rid_readiness_changed_sources: Vec<TransportMediaId>,
    /// rooms whose source policy must be recomputed after packet observations
    pub(super) dirty_source_policy_channel_ids: Vec<RoomInstanceId>,
    /// concrete forwarding destinations planned for `pending_packets`
    pub forwards: Vec<PacketForward>,
}

impl PacketLoopBuffers {
    /// build the reusable buffer set with small initial capacities
    ///
    /// the capacities are only starting points
    /// dense rooms may grow them once,
    /// after which normal `.clear()` calls keep the larger allocation for later
    /// turns
    pub fn new() -> Self {
        Self {
            pending_transmits: Vec::with_capacity(64),
            pending_packets: Vec::with_capacity(32),
            pending_keyframe_requests: Vec::with_capacity(8),
            ready_sessions: Vec::with_capacity(32),
            coalesced_keyframe_requests: Vec::with_capacity(8),
            keyframe_retries: Vec::with_capacity(8),
            pending_rid_readiness: Vec::with_capacity(8),
            pending_first_video_keyframes: Vec::with_capacity(8),
            rid_readiness_changed_sources: Vec::with_capacity(8),
            dirty_source_policy_channel_ids: Vec::with_capacity(8),
            forwards: Vec::with_capacity(64),
        }
    }

    /// Reset all staged work while retaining allocation capacity.
    pub fn clear(&mut self) {
        self.pending_transmits.clear();
        self.pending_packets.clear();
        self.pending_keyframe_requests.clear();
        self.ready_sessions.clear();
        self.coalesced_keyframe_requests.clear();
        self.keyframe_retries.clear();
        self.pending_rid_readiness.clear();
        self.pending_first_video_keyframes.clear();
        self.rid_readiness_changed_sources.clear();
        self.dirty_source_policy_channel_ids.clear();
        self.forwards.clear();
    }

    /// Queue a UDP transmit by moving the owned `str0m` datagram buffer.
    pub(super) fn push_pending_transmit(&mut self, destination: SocketAddr, contents: Vec<u8>) {
        self.pending_transmits.push(PendingTransmit {
            destination,
            contents,
        });
    }

    pub(super) fn pending_transmits_mut(&mut self) -> impl Iterator<Item = &mut PendingTransmit> {
        self.pending_transmits.iter_mut()
    }

    pub(super) fn push_rid_readiness(
        &mut self,
        source: &ForwardedPacketSource,
        src_media: TransportMediaId,
        rid: Rid,
        is_keyframe: bool,
        observed_at: Instant,
    ) {
        if let Some(pending) = self
            .pending_rid_readiness
            .iter_mut()
            .find(|pending| pending.src_media == src_media && pending.rid == rid)
        {
            pending.is_keyframe |= is_keyframe;
            if pending.observed_at < observed_at {
                pending.observed_at = observed_at;
            }
            return;
        }
        self.pending_rid_readiness.push(PendingRidReadiness {
            source: source.clone(),
            src_media,
            rid,
            is_keyframe,
            observed_at,
        });
    }

    pub(super) fn push_first_video_keyframe(
        &mut self,
        source: &ForwardedPacketSource,
        src_media: TransportMediaId,
        observed_at: Instant,
    ) {
        if self
            .pending_first_video_keyframes
            .iter()
            .any(|pending| pending.src_media == src_media)
        {
            return;
        }
        self.pending_first_video_keyframes
            .push(PendingFirstVideoKeyframe {
                source: source.clone(),
                src_media,
                observed_at,
            });
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
