//! packet-loop source route facts
//!
//! source routes bind one producer media id to local consumer destinations
//! plus the remote-source and decoder-refresh facts attached to that source

use o_sfu_rfc::rtp;
use str0m::media::{Mid, Pt, Rid};

use super::{
    commands::RemoteSourceControl, route_control::PacketLayerGate, slots::ConsumerStreamHandle,
};
use crate::engine::media_transport::{TransportMediaId, TransportSessionKey, TransportSourceKey};

/// forwarding destination selected for one source media route
///
/// one producer media id can fan out to many consumer transports
/// each destination keeps the consumer-negotiated RTP identity and the
/// currently effective packet gate so the packet loop can forward without
/// consulting room policy on the hot path
#[derive(Debug, Clone)]
pub(super) struct MediaRouteDestination {
    /// consumer session that owns the destination `Rtc`
    pub(super) dest_session: TransportSessionKey,
    /// consumer media id used by the destination session state
    pub(super) dest_transport_media_id: TransportMediaId,
    /// destination-owned RTP rewrite handle for this consumer route
    ///
    /// the counters live in the destination session
    /// this handle lets a route destination reach them without keying local
    /// RTP projection by sparse transport media ids on the hot path
    pub(super) dest_stream: ConsumerStreamHandle,
    /// consumer MID used when rewriting the packet for local egress
    pub(super) dest_mid: Mid,
    /// payload type negotiated for this consumer stream
    ///
    /// source payload types can differ from consumer payload types after router
    /// negotiation, so forwarding must not reuse the publisher value blindly
    pub(super) dest_payload_type: Option<Pt>,
    /// stable retransmission policy for this consumer media line
    ///
    /// this is captured when the route is registered because the consumer
    /// media kind is already known there
    /// the packet loop can then write local RTP without asking `str0m` to
    /// rediscover the media kind for every destination packet
    pub(super) nackable: bool,
    /// destination-level activity gate controlled by consumer state
    pub(super) active: bool,
    /// effective transport gate used by the packet loop right now
    pub(super) packet_gate: PacketLayerGate,
    /// selected strict gate that is waiting for a decodable live RID
    ///
    /// pending gates keep a route from opening to multiple publisher RIDs while
    /// a browser is still bringing up or refreshing the selected layer
    pub(super) pending_gate: Option<PacketLayerGate>,
}

/// packet-loop fanout state for one producer media id
///
/// `source_active` is a source-wide route gate
/// individual destinations still carry their own activity and layer gates so
/// producer pause, consumer pause and selected-layer policy remain independent
///
/// callers must mutate `destinations` through this type's helpers
/// `active_destination_count` is a cached admission invariant used by the hot
/// planner before it walks the destination vector
#[derive(Debug, Clone)]
pub(super) struct MediaRouteEntry {
    /// source-wide activity gate applied before local destination fanout
    pub(super) source_active: bool,
    /// active local destinations cached for source-route admission checks
    pub(super) active_destination_count: usize,
    /// local consumer destinations reached from this source media id
    pub(super) destinations: Vec<MediaRouteDestination>,
}

impl MediaRouteEntry {
    /// creates an empty route entry with no local destinations
    ///
    /// the active count starts at zero even when the source is active because
    /// source activity and destination activity are independent gates
    pub(super) fn new(source_active: bool) -> Self {
        Self {
            source_active,
            active_destination_count: 0,
            destinations: Vec::new(),
        }
    }

    /// returns whether local fanout has any active destination work
    ///
    /// this is the O(1) route-admission check used before source packet gates
    /// are evaluated
    pub(super) const fn has_active_destinations(&self) -> bool {
        self.active_destination_count > 0
    }

    /// appends one destination while preserving the active-count invariant
    ///
    /// route registration is the only caller that should add destinations
    /// directly because it owns source validation and consumer media ownership
    pub(super) fn push_destination(&mut self, destination: MediaRouteDestination) {
        self.active_destination_count += usize::from(destination.active);
        self.destinations.push(destination);
    }

    /// removes one destination while preserving the active-count invariant
    ///
    /// callers pass the index they found under the same mutable route borrow
    /// destination order is not semantically observable, so removal keeps the
    /// vector dense by moving the final destination into the cleared slot
    /// callers that cache destination indexes must repair the moved destination
    /// before feedback can use the cache again
    pub(super) fn remove_destination(&mut self, index: usize) -> MediaRouteDestination {
        let destination = self.destinations.swap_remove(index);
        self.active_destination_count -= usize::from(destination.active);
        destination
    }

    /// updates destination activity and reports whether the route changed
    ///
    /// a missing index is treated as unchanged because stale worker commands
    /// are rejected by the caller's ownership checks before reaching the route
    /// mutation
    pub(super) fn set_destination_active(&mut self, index: usize, active: bool) -> bool {
        let Some(destination) = self.destinations.get_mut(index) else {
            return false;
        };
        if destination.active == active {
            return false;
        }
        if active {
            self.active_destination_count += 1;
        } else {
            self.active_destination_count -= 1;
        }
        destination.active = active;
        true
    }
}

pub(super) enum DestinationKeyframeTarget {
    Current(Option<Rid>),
    Stale,
}

impl MediaRouteDestination {
    pub(super) fn keyframe_target_rid(&self, open_rid: Option<Rid>) -> DestinationKeyframeTarget {
        let target_rid = match self.packet_gate {
            PacketLayerGate::Rid(rid) => Some(rid),
            PacketLayerGate::OperatingPoint(operating_point) => operating_point.rid(),
            PacketLayerGate::Block => {
                let Some(rid) = self
                    .pending_gate
                    .as_ref()
                    .and_then(PacketLayerGate::selected_rid)
                else {
                    return DestinationKeyframeTarget::Stale;
                };
                Some(rid)
            }
            PacketLayerGate::Open => open_rid,
        };
        DestinationKeyframeTarget::Current(target_rid)
    }
}

#[derive(Debug, Clone)]
pub(super) struct RemoteSourceRegistration {
    source: TransportSourceKey,
    source_control: RemoteSourceControl,
    pending_gate: Option<PacketLayerGate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DecoderRefreshCodec {
    H264(rtp::h264::PacketizationMode),
    Vp8,
    Unsupported,
}

impl DecoderRefreshCodec {
    pub(super) fn from_parameters(parameters: &o_sfu_router::MediaStream) -> Option<Self> {
        let mut has_vp8 = false;
        let mut has_unsupported_primary = false;
        for format in parameters.formats() {
            match format.codec() {
                &rtp::CodecName::H264 => return Some(Self::from_h264_format(format)),
                &rtp::CodecName::Vp8 => has_vp8 = true,
                codec if !codec.is_rtx() => has_unsupported_primary = true,
                _ => {}
            }
        }
        if has_vp8 {
            Some(Self::Vp8)
        } else {
            has_unsupported_primary.then_some(Self::Unsupported)
        }
    }

    fn from_h264_format(format: &o_sfu_router::MediaFormat) -> Self {
        let mode = format
            .settings()
            .find_map(|setting| match setting {
                o_sfu_router::CodecSetting::H264PacketizationMode(mode) => Some(*mode),
                _ => None,
            })
            .unwrap_or(rtp::h264::PacketizationMode::SingleNalUnit);
        match mode {
            rtp::h264::PacketizationMode::SingleNalUnit
            | rtp::h264::PacketizationMode::NonInterleaved => Self::H264(mode),
            rtp::h264::PacketizationMode::Interleaved => Self::Unsupported,
        }
    }
}

impl RemoteSourceRegistration {
    pub(super) fn new(source: TransportSourceKey, source_control: RemoteSourceControl) -> Self {
        Self {
            source,
            source_control,
            pending_gate: None,
        }
    }

    pub(super) fn source(&self) -> &TransportSourceKey {
        &self.source
    }

    pub(super) fn cloned_control_path(&self) -> (TransportSourceKey, RemoteSourceControl) {
        (self.source.clone(), self.source_control.clone())
    }

    #[cfg(test)]
    pub(super) const fn pending_gate(&self) -> Option<PacketLayerGate> {
        self.pending_gate
    }

    pub(super) const fn has_pending_gate(&self) -> bool {
        self.pending_gate.is_some()
    }

    pub(super) fn publish_packet_gate_needs_retry(&mut self, packet_gate: PacketLayerGate) -> bool {
        if self.source_control.set_pkt_gate(&self.source, packet_gate) {
            self.pending_gate = None;
            false
        } else {
            self.pending_gate = Some(packet_gate);
            true
        }
    }

    pub(super) fn flush_pending_gate(&mut self) -> bool {
        let Some(packet_gate) = self.pending_gate else {
            return false;
        };
        self.source_control.record_pkt_gate_retry();
        if self.source_control.set_pkt_gate(&self.source, packet_gate) {
            self.pending_gate = None;
            self.source_control.record_pkt_gate_flushed();
            false
        } else {
            true
        }
    }
}
