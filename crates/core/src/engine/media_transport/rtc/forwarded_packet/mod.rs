//! the packet loop needs one packet shape that can cross four local concerns:
//! observation, fanout planning, local browser egress and relay egress
//! `ForwardedPacket` is that shape
//! it keeps the source session, RTP header view and `Arc` payload together
//! while letting each concern borrow only the state it owns
//!
//! relay and multi-destination local sends share the source bytes through `Arc`
//! callers that need cached source observations must read them before entering
//! the local-send path
//!
//! source-derived observations live in `PacketFacts`
//! facts are valid for the current packet-loop turn only because they are
//! computed against `PacketLoopState`
//! they let incoming stats, fanout planning and local VP8 rewriting reuse the
//! same source media id, layer metadata and codec facts without repeating header
//! or payload parsing

use std::{mem::take, sync::Arc, time::Instant};

use o_sfu_rfc::rtp::{frame_marking, h264, vp8};
use str0m::{
    media::{ExtensionValues, Mid, Rid},
    rtp::{RtpHeader, RtpPacket, Ssrc, Vp8Descriptor, Vp8Patch, Vp8PatchError},
};

use super::{
    local_forwarding::LocalForwardedRtp, local_send_rewrite::Vp8PayloadIdentity,
    route_control::PacketLayerMetadata, slots::SessionHandle, source_route::PacketCodec,
    state::PacketLoopState,
};
use crate::engine::{
    RoomInstanceId,
    media_transport::{TransportMediaId, TransportSessionKey},
};

#[cfg(any(test, feature = "internal-benchmarks"))]
#[path = "TESTS/support.rs"]
pub mod test_support;

/// one RTP packet staged for packet-loop observation and forwarding
///
/// a value may come directly from a local `str0m` session or from a relay
/// mailbox
/// both origins expose the same source identity, header metadata and
/// payload ownership contract to the rest of the packet loop
#[derive(Debug)]
pub struct ForwardedPacket {
    /// session that published the packet
    ///
    /// this also anchors the room instance used by source-policy wakeups and
    /// room-scoped packet sinks
    source: ForwardedPacketSource,
    /// producer identity resolved from MID, SSRC or relay metadata
    ///
    /// once this is known it is cached so later state churn during the same
    /// flush cannot make the packet point at a different source
    src_media: Option<TransportMediaId>,
    /// resolved RID recovered from the packet header or from the worker SSRC binding
    ///
    /// relay clones carry this value so a target worker can apply the same
    /// route-control layer metadata without owning the source worker registry
    resolved_source_rid: Option<Rid>,
    /// turn-local source observations shared by stats, route planning and egress
    facts: Option<PacketFacts>,
    /// whether source-worker side effects still belong to this packet
    ///
    /// relayed packets already passed through their origin worker, so target
    /// workers must not send them back into origin sinks or second-hop relays
    visits_origin_sinks: bool,
    /// packet timestamp used for bitrate, activity and egress metrics
    received_at: Instant,
    /// source payload bytes shared by relay and local fanout
    payload: Arc<[u8]>,
    /// origin-specific RTP header storage
    data: ForwardedPacketData,
}

/// source identity for one staged forwarded packet
///
/// local packets carry only a worker-local generation-checked handle while
/// relayed packets carry the stable public key needed to cross worker mailboxes
#[derive(Debug, Clone)]
pub(super) enum ForwardedPacketSource {
    Local(SessionHandle),
    Relayed(TransportSessionKey),
}

impl ForwardedPacketSource {
    pub(super) fn session_key<'a>(
        &'a self,
        state: &'a PacketLoopState,
    ) -> Option<&'a TransportSessionKey> {
        match self {
            Self::Local(session_handle) => state.users.key_for_handle(*session_handle),
            Self::Relayed(session_key) => Some(session_key),
        }
    }
}

/// rtp header storage for the two ingress paths handled by the packet loop
///
/// local packets keep the full `str0m` packet so browser egress can reuse the
/// exact source header view
/// relayed packets only need the header plus the `Arc` payload that already
/// lives on `ForwardedPacket`
#[derive(Debug)]
enum ForwardedPacketData {
    Str0mRtp(ForwardedRtpData),
    RelayRtp(ForwardedRelayRtpData),
}

/// local `str0m` packet metadata kept after the payload was moved out
#[derive(Debug)]
struct ForwardedRtpData {
    rtp_packet: RtpPacket,
}

/// relay packet metadata after payload ownership has moved to `ForwardedPacket`
#[derive(Debug)]
pub(super) struct ForwardedRelayRtpData {
    pub(super) header: RtpHeader,
}

/// source observations that should be computed once per packet-loop turn
///
/// facts are copied into each caller that needs them
/// they do not own packet bytes and they must not be treated as durable media
/// state after the current batch because route-control registries can change
/// between turns
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PacketFacts {
    /// source producer selected by MID, SSRC or relay metadata
    pub(super) src_media: TransportMediaId,
    /// route-control layer key derived from RID and frame marking metadata
    pub(super) layer_metadata: PacketLayerMetadata,
    /// payload length observed before local egress can move the payload
    pub(super) payload_len: usize,
    /// room that owns source-policy wakeups for this packet
    pub(super) room_instance_id: RoomInstanceId,
    /// audio activity flag projected from RTP header extensions
    pub(super) voice_activity: Option<bool>,
    /// audio level projected from RTP header extensions
    pub(super) audio_level: Option<i8>,
    /// whether this packet can refresh decoder state for the source codec
    pub(super) decoder_refresh: bool,
    /// parsed VP8 descriptor and identity needed by local egress rewrites
    pub(super) vp8_payload: Option<PacketVp8Payload>,
}

/// vp8 descriptor data that local egress can reuse for payload patches
///
/// local forwarding asks str0m to patch the descriptor during serialization
/// caching the source descriptor here keeps descriptor parsing source-scoped
/// instead of destination-scoped
#[derive(Debug, Clone, Copy)]
pub(super) struct PacketVp8Payload {
    /// descriptor parsed from the source payload
    descriptor: Vp8Descriptor,
    /// source picture identity used by destination RTP identity projection
    pub(super) identity: Vp8PayloadIdentity,
}

impl PartialEq for PacketVp8Payload {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity
    }
}

impl Eq for PacketVp8Payload {}

impl ForwardedPacket {
    /// stages one packet emitted by a local `str0m` session
    ///
    /// the payload is moved out of the str0m packet immediately so relay fanout
    /// and local fanout can share the same ownership model
    /// the remaining `str0m` packet is kept only for header metadata and the
    /// original receive time
    pub(super) fn from_rtp_packet(
        source_session_handle: SessionHandle,
        mut rtp_packet: RtpPacket,
    ) -> Self {
        Self {
            source: ForwardedPacketSource::Local(source_session_handle),
            src_media: None,
            resolved_source_rid: None,
            facts: None,
            visits_origin_sinks: true,
            received_at: rtp_packet.timestamp,
            payload: take(&mut rtp_packet.payload),
            data: ForwardedPacketData::Str0mRtp(ForwardedRtpData { rtp_packet }),
        }
    }

    #[must_use]
    pub(super) const fn source(&self) -> &ForwardedPacketSource {
        &self.source
    }

    #[must_use]
    pub(super) fn src_key<'a>(
        &'a self,
        state: &'a PacketLoopState,
    ) -> Option<&'a TransportSessionKey> {
        self.source.session_key(state)
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub fn stable_src_key(&self) -> Option<&TransportSessionKey> {
        match &self.source {
            ForwardedPacketSource::Local(_) => None,
            ForwardedPacketSource::Relayed(session_key) => Some(session_key),
        }
    }

    #[must_use]
    pub const fn received_at(&self) -> Instant {
        self.received_at
    }

    #[must_use]
    pub fn payload(&self) -> &[u8] {
        self.payload.as_ref()
    }

    #[cfg(test)]
    pub(super) fn resolve_route_control_layer_metadata(
        &mut self,
        state: &PacketLoopState,
    ) -> PacketLayerMetadata {
        if let Some(facts) = self.facts {
            return facts.layer_metadata;
        }
        self.compute_route_control_layer_metadata(state)
    }

    /// resolves and caches all source observations needed by this packet turn
    ///
    /// `None` means the packet cannot currently be attached to a source
    /// transport media id
    /// callers should treat that as a best-effort ingress miss and drop the
    /// packet from stats or fanout for this turn
    ///
    /// once facts are cached, later calls return the cached value even if source
    /// registries change before the flush completes
    /// this keeps one packet internally consistent across observation, planning
    /// and egress
    pub(super) fn resolve_facts(&mut self, state: &PacketLoopState) -> Option<PacketFacts> {
        if let Some(facts) = self.facts {
            return Some(facts);
        }
        let src_media = self.resolve_src_media(state)?;
        let layer_metadata = self.compute_route_control_layer_metadata(state);
        let packet_codec = state
            .routes
            .packet_codec(src_media, self.rtp_header().payload_type);
        let decoder_refresh = self.decoder_refresh(packet_codec);
        let extensions = self.route_control_extension_values();
        let facts = PacketFacts {
            src_media,
            layer_metadata,
            payload_len: self.payload_len(),
            room_instance_id: self.src_key(state)?.room_instance_id(),
            voice_activity: extensions.voice_activity,
            audio_level: extensions.audio_level,
            decoder_refresh,
            vp8_payload: self.resolve_vp8_payload(packet_codec),
        };
        self.facts = Some(facts);
        Some(facts)
    }

    /// returns the cached VP8 descriptor facts local egress should use for rewriting
    ///
    /// incoming observation fills `PacketFacts` before route planning and local
    /// forwarding run, so descriptor parsing remains packet-scoped instead of
    /// destination-scoped
    pub(super) fn local_vp8_payload(&self) -> Option<PacketVp8Payload> {
        self.facts.and_then(|facts| facts.vp8_payload)
    }

    fn compute_route_control_layer_metadata(
        &mut self,
        state: &PacketLoopState,
    ) -> PacketLayerMetadata {
        let extensions = self.route_control_extension_values();
        let extension_rid = extensions.rid.or(extensions.rid_repair);
        let temporal_layer_id = extensions.frame_mark.map(frame_mark_temporal_layer_id);
        let rid = extension_rid
            .or(self.resolved_source_rid)
            .or_else(|| self.route_control_rid_from_ssrc(state));
        self.resolved_source_rid = rid;
        PacketLayerMetadata::new(rid, temporal_layer_id)
    }

    pub(super) fn route_control_ssrc(&self) -> Ssrc {
        self.rtp_header().ssrc
    }

    pub(super) fn route_control_mid(&self) -> Option<Mid> {
        self.route_control_extension_values().mid
    }

    pub(super) fn route_control_rid_extension(&self) -> Option<Rid> {
        let extensions = self.route_control_extension_values();
        extensions.rid.or(extensions.rid_repair)
    }

    /// creates a relay-owned view that shares this packet payload
    ///
    /// the caller supplies the resolved source media id because relay targets
    /// must not depend on the target worker being able to rediscover source
    /// identity from local producer registries
    /// cached facts and resolved RID are preserved so the receiving worker can
    /// reuse source observations
    pub(super) fn share_for_relay(
        &self,
        state: &PacketLoopState,
        src_media: TransportMediaId,
    ) -> Option<Self> {
        Some(Self {
            source: ForwardedPacketSource::Relayed(self.source.session_key(state)?.clone()),
            src_media: Some(src_media),
            resolved_source_rid: self.resolved_source_rid,
            facts: self.facts,
            visits_origin_sinks: false,
            received_at: self.received_at,
            payload: Arc::clone(&self.payload),
            data: ForwardedPacketData::RelayRtp(ForwardedRelayRtpData {
                header: self.rtp_header().clone(),
            }),
        })
    }

    pub(super) fn payload_len(&self) -> usize {
        self.payload.len()
    }

    pub(super) const fn visits_origin_sinks(&self) -> bool {
        self.visits_origin_sinks
    }

    /// resolves the source producer that owns this packet
    ///
    /// the lookup prefers cached facts, then an explicit relay media id, then
    /// source-worker MID and SSRC registries
    /// a successful lookup is cached on the packet so a later registry update
    /// cannot split one packet across different source identities
    pub(super) fn resolve_src_media(
        &mut self,
        state: &PacketLoopState,
    ) -> Option<TransportMediaId> {
        if let Some(facts) = self.facts {
            return Some(facts.src_media);
        }
        if let Some(src_media) = self.src_media {
            return Some(src_media);
        }
        let src_key = self.source.session_key(state)?;
        let header = self.rtp_header();
        let resolved = if let Some(source_mid) = header.ext_vals.mid
            && let Some(src_media) = state.src_media_for_mid(src_key, source_mid)
        {
            Some(src_media)
        } else {
            state.src_media_for_ssrc(src_key, header.ssrc)
        };
        if let Some(src_media) = resolved {
            self.src_media = Some(src_media);
        }
        resolved
    }

    /// exposes a mutable local-send view over this packet
    ///
    /// callers should resolve any packet facts they still need before calling
    /// this method
    /// local send keeps the `Arc` payload alive while str0m queues the packet
    pub(super) fn local_send_packet(&self) -> LocalForwardedRtp<'_> {
        let payload = &self.payload;
        match &self.data {
            ForwardedPacketData::Str0mRtp(rtp_data) => {
                LocalForwardedRtp::new(&rtp_data.rtp_packet, payload)
            }
            ForwardedPacketData::RelayRtp(rtp_data) => {
                let header = &rtp_data.header;
                LocalForwardedRtp::from_relay(header, self.received_at, payload)
            }
        }
    }

    fn route_control_extension_values(&self) -> &ExtensionValues {
        &self.rtp_header().ext_vals
    }

    fn rtp_header(&self) -> &RtpHeader {
        match &self.data {
            ForwardedPacketData::Str0mRtp(rtp_data) => &rtp_data.rtp_packet.header,
            ForwardedPacketData::RelayRtp(rtp_data) => &rtp_data.header,
        }
    }

    fn route_control_rid_from_ssrc(&self, state: &PacketLoopState) -> Option<Rid> {
        let src_key = self.source.session_key(state)?;
        state.source_rid_for_ssrc(src_key, self.rtp_header().ssrc)
    }

    fn decoder_refresh(&self, codec: Option<PacketCodec>) -> bool {
        match codec {
            Some(PacketCodec::H264(packetization_mode)) => {
                h264::payload_starts_idr(self.payload.as_ref(), packetization_mode)
            }
            Some(PacketCodec::Vp8) => vp8::payload_starts_keyframe(self.payload.as_ref()),
            None => false,
        }
    }

    fn resolve_vp8_payload(&self, codec: Option<PacketCodec>) -> Option<PacketVp8Payload> {
        if codec != Some(PacketCodec::Vp8) {
            return None;
        }
        self.resolved_source_rid?;
        PacketVp8Payload::parse(self.payload.as_ref())
    }
}

impl PacketVp8Payload {
    fn parse(payload: &[u8]) -> Option<Self> {
        Vp8Descriptor::parse(payload).ok().map(Self::new)
    }

    fn new(descriptor: Vp8Descriptor) -> Self {
        Self {
            descriptor,
            identity: Vp8PayloadIdentity {
                picture_id: descriptor.picture_id(),
                tl0_pic_idx: descriptor.tl0_pic_idx(),
            },
        }
    }

    pub(super) fn patch(self, identity: Vp8PayloadIdentity) -> Option<Vp8Patch> {
        match self.build_patch(identity) {
            Ok(patch) => Some(patch),
            Err(Vp8PatchError::PictureIdTooLarge) => self
                .build_patch(Vp8PayloadIdentity {
                    picture_id: identity
                        .picture_id
                        .map(|picture_id| picture_id & vp8::SHORT_PICTURE_ID_MASK),
                    tl0_pic_idx: identity.tl0_pic_idx,
                })
                .ok(),
            Err(_) => None,
        }
    }

    fn build_patch(self, identity: Vp8PayloadIdentity) -> Result<Vp8Patch, Vp8PatchError> {
        let mut patch = self.descriptor.patch();
        if let Some(picture_id) = identity.picture_id {
            patch = patch.picture_id(picture_id);
        }
        if let Some(tl0_pic_idx) = identity.tl0_pic_idx {
            patch = patch.tl0_pic_idx(tl0_pic_idx);
        }
        patch.build()
    }
}

fn frame_mark_temporal_layer_id(frame_mark: u32) -> u8 {
    let [first_octet, ..] = frame_mark.to_be_bytes();
    frame_marking::temporal_layer_id(first_octet)
}

#[cfg(test)]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
