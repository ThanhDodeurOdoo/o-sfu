//! the packet loop needs one packet shape that can cross four local concerns:
//! observation, fanout planning, local browser egress and relay egress
//! `ForwardedPacket` is that shape
//! it keeps the source session, RTP header view and shared payload together
//! while letting each concern borrow only the state it owns
//!
//! relay and multi-destination local sends share the source bytes through
//! `SharedPayload`, while the last local destination can consume the owned
//! payload instead of cloning it
//! callers that need cached source observations must read them before entering
//! the local-send path that may move the payload
//!
//! source-derived observations live in `PacketFacts`
//! facts are valid for the current packet-loop turn only because they are
//! computed against `PacketLoopState`
//! they let incoming stats, fanout planning and local VP8 rewriting reuse the
//! same source media id, layer metadata and codec facts without repeating header
//! or payload parsing

use std::{mem::take, time::Instant};

use o_sfu_rfc::rtp::{frame_marking, h264, vp8};
use str0m::{
    media::{ExtensionValues, Mid, Rid},
    rtp::{RtpHeader, RtpPacket, Ssrc},
};

use super::{
    local_forwarding::LocalForwardedRtp, local_send_rewrite::Vp8PayloadIdentity,
    media_registry::DecoderRefreshCodec, route_control::PacketLayerMetadata,
    shared_payload::SharedPayload, slots::SessionHandle, state::PacketLoopState,
};
use crate::runtime::{
    RoomInstanceId,
    media_transport::{TransportMediaId, TransportSessionKey},
};

#[cfg(any(test, feature = "internal-benchmarks"))]
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
    source_transport_media_id: Option<TransportMediaId>,
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
    /// source payload bytes with clone-or-move ownership for fanout
    payload: SharedPayload,
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
/// relayed packets only need the header plus the shared payload that already
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

/// relay packet metadata after payload ownership has been moved to `SharedPayload`
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
    pub(super) source_transport_media_id: TransportMediaId,
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

/// vp8 descriptor data that local egress can reuse before payload mutation
///
/// local forwarding rewrites the descriptor in the destination payload
/// caching the source descriptor here keeps descriptor parsing source-scoped
/// instead of destination-scoped
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PacketVp8Payload {
    /// descriptor parsed from the source payload
    pub(super) descriptor: vp8::PayloadDescriptor,
    /// source picture identity used by destination RTP identity projection
    pub(super) identity: Vp8PayloadIdentity,
}

impl ForwardedPacket {
    /// stages one packet emitted by a local `str0m` session
    ///
    /// the payload is moved into `SharedPayload` immediately so relay fanout and
    /// local fanout can share the same ownership model
    /// the remaining `str0m` packet is kept only for header metadata and the
    /// original receive time
    pub(super) fn from_rtp_packet(
        source_session_handle: SessionHandle,
        mut rtp_packet: RtpPacket,
    ) -> Self {
        Self {
            source: ForwardedPacketSource::Local(source_session_handle),
            source_transport_media_id: None,
            resolved_source_rid: None,
            facts: None,
            visits_origin_sinks: true,
            received_at: rtp_packet.timestamp,
            payload: SharedPayload::from_vec(take(&mut rtp_packet.payload)),
            data: ForwardedPacketData::Str0mRtp(ForwardedRtpData { rtp_packet }),
        }
    }

    #[must_use]
    pub(super) const fn source(&self) -> &ForwardedPacketSource {
        &self.source
    }

    #[must_use]
    pub(super) fn source_session_key<'a>(
        &'a self,
        state: &'a PacketLoopState,
    ) -> Option<&'a TransportSessionKey> {
        self.source.session_key(state)
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::runtime) fn stable_source_session_key(&self) -> Option<&TransportSessionKey> {
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
        self.payload.as_slice()
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
        let source_transport_media_id = self.resolve_source_transport_media_id(state)?;
        let layer_metadata = self.compute_route_control_layer_metadata(state);
        let decoder_refresh = self.decoder_refresh_for_source(state, source_transport_media_id);
        let extensions = self.route_control_extension_values();
        let facts = PacketFacts {
            source_transport_media_id,
            layer_metadata,
            payload_len: self.payload_len(),
            room_instance_id: self.source_session_key(state)?.room_instance_id(),
            voice_activity: extensions.voice_activity,
            audio_level: extensions.audio_level,
            decoder_refresh,
            vp8_payload: self.resolve_vp8_payload(),
        };
        self.facts = Some(facts);
        Some(facts)
    }

    /// returns the VP8 descriptor facts local egress should use for rewriting
    ///
    /// incoming observation normally fills `PacketFacts` before local forwarding
    /// runs
    /// the lazy parse keeps the API correct for tests or exceptional paths
    /// that send a packet without first calling `resolve_facts`
    pub(super) fn local_vp8_payload(&self) -> Option<PacketVp8Payload> {
        self.facts
            .and_then(|facts| facts.vp8_payload)
            .or_else(|| self.resolve_vp8_payload())
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
        source_transport_media_id: TransportMediaId,
    ) -> Option<Self> {
        Some(Self {
            source: ForwardedPacketSource::Relayed(self.source.session_key(state)?.clone()),
            source_transport_media_id: Some(source_transport_media_id),
            resolved_source_rid: self.resolved_source_rid,
            facts: self.facts,
            visits_origin_sinks: false,
            received_at: self.received_at,
            payload: self.payload.share(),
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
    pub(super) fn resolve_source_transport_media_id(
        &mut self,
        state: &PacketLoopState,
    ) -> Option<TransportMediaId> {
        if let Some(facts) = self.facts {
            return Some(facts.source_transport_media_id);
        }
        if let Some(source_transport_media_id) = self.source_transport_media_id {
            return Some(source_transport_media_id);
        }
        let source_session_key = self.source.session_key(state)?;
        let header = self.rtp_header();
        let resolved = if let Some(source_mid) = header.ext_vals.mid
            && let Some(source_transport_media_id) =
                state.source_transport_media_id_for_mid(source_session_key, source_mid)
        {
            Some(source_transport_media_id)
        } else {
            state.source_transport_media_id_for_ssrc(source_session_key, header.ssrc)
        };
        if let Some(source_transport_media_id) = resolved {
            self.source_transport_media_id = Some(source_transport_media_id);
        }
        resolved
    }

    /// exposes a mutable local-send view over this packet
    ///
    /// callers should resolve any packet facts they still need before calling
    /// this method
    /// local send may consume the shared payload when the packet is written to
    /// its last destination
    pub(super) fn local_send_packet(&mut self) -> LocalForwardedRtp<'_> {
        let payload = &mut self.payload;
        match &mut self.data {
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
        let source_session_key = self.source.session_key(state)?;
        state.source_rid_for_ssrc(source_session_key, self.rtp_header().ssrc)
    }

    fn decoder_refresh_for_source(
        &self,
        state: &PacketLoopState,
        source_transport_media_id: TransportMediaId,
    ) -> bool {
        match state.source_decoder_refresh_codec(source_transport_media_id) {
            Some(DecoderRefreshCodec::H264(packetization_mode)) => {
                h264::payload_starts_idr(self.payload.as_slice(), packetization_mode)
            }
            Some(DecoderRefreshCodec::Vp8) | None => {
                vp8::payload_starts_keyframe(self.payload.as_slice())
            }
            Some(DecoderRefreshCodec::Unsupported) => false,
        }
    }

    fn resolve_vp8_payload(&self) -> Option<PacketVp8Payload> {
        let extensions = self.route_control_extension_values();
        extensions.rid.or(extensions.rid_repair)?;
        vp8::payload_descriptor(self.payload.as_slice()).map(PacketVp8Payload::new)
    }
}

impl PacketVp8Payload {
    pub(super) fn new(descriptor: vp8::PayloadDescriptor) -> Self {
        Self {
            descriptor,
            identity: Vp8PayloadIdentity {
                picture_id: descriptor.picture_id(),
                tl0_pic_idx: descriptor.tl0_pic_idx(),
            },
        }
    }
}

fn frame_mark_temporal_layer_id(frame_mark: u32) -> u8 {
    let [first_octet, ..] = frame_mark.to_be_bytes();
    frame_marking::temporal_layer_id(first_octet)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use o_sfu_rfc::rtp::CodecName;
    use o_sfu_router::{
        CodecSetting, MediaFormat, MediaKind as RouterMediaKind,
        MediaStream as RouterRtpParameters, StreamBinding,
    };
    use str0m::media::{Mid, Rid};

    use super::*;
    use crate::{
        Bitrate, MediaCodecFlags,
        runtime::{
            UserId,
            rtc_engine::{
                bootstrap::ensure_session_rtc_state,
                forwarded_packet::test_support::{
                    sample_forwarded_packet, sample_forwarded_packet_with_frame_mark,
                    sample_forwarded_packet_with_rid, sample_forwarded_packet_without_mid,
                    sample_local_forwarded_packet,
                },
                media_registry::RegisteredMediaHandle,
                test_support::test_transport_session_key,
            },
        },
    };

    fn install_test_session(
        state: &mut PacketLoopState,
        session_key: &TransportSessionKey,
    ) -> Option<SessionHandle> {
        assert!(
            ensure_session_rtc_state(
                &mut state.users,
                session_key,
                SocketAddr::from(([127, 0, 0, 1], 9)),
                Bitrate::from_bps(1_000_000),
                MediaCodecFlags::default(),
            )
            .is_ok()
        );
        state.users.handle_for_key(session_key)
    }

    #[test]
    fn forwarded_packet_resolves_transport_media_id_through_the_registry() {
        let session_key = test_transport_session_key(41, 0, 9, UserId::Integer(7));
        let mut state = PacketLoopState::default();
        let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: session_key.clone(),
            mid: Mid::from("aud-up"),
        });
        let mut packet = sample_forwarded_packet(session_key, "aud-up", b"payload");

        assert_eq!(
            packet.resolve_source_transport_media_id(&state),
            Some(transport_media_id)
        );
    }

    #[test]
    fn local_forwarded_packet_resolves_transport_media_id_through_session_handle() {
        let session_key = test_transport_session_key(51, 0, 19, UserId::Integer(17));
        let mut state = PacketLoopState::default();
        let session_handle = install_test_session(&mut state, &session_key);
        assert!(session_handle.is_some());
        let Some(session_handle) = session_handle else {
            return;
        };
        let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: session_key.clone(),
            mid: Mid::from("aud-up"),
        });
        let mut packet = sample_local_forwarded_packet(session_handle, "aud-up", b"payload");

        assert_eq!(packet.source_session_key(&state), Some(&session_key));
        assert_eq!(
            packet.resolve_source_transport_media_id(&state),
            Some(transport_media_id)
        );
    }

    #[test]
    fn stale_local_forwarded_packet_does_not_resolve_through_reused_slot() {
        let session_key = test_transport_session_key(52, 0, 20, UserId::Integer(18));
        let replacement_session_key = test_transport_session_key(52, 0, 21, UserId::Integer(19));
        let mut state = PacketLoopState::default();
        let stale_handle = install_test_session(&mut state, &session_key);
        assert!(stale_handle.is_some());
        let Some(stale_handle) = stale_handle else {
            return;
        };
        let _removed = state.users.remove(&session_key);
        let _replacement_handle = install_test_session(&mut state, &replacement_session_key);
        state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: replacement_session_key,
            mid: Mid::from("aud-up"),
        });
        let mut packet = sample_local_forwarded_packet(stale_handle, "aud-up", b"payload");

        assert!(packet.source_session_key(&state).is_none());
        assert_eq!(packet.resolve_facts(&state), None);
        assert!(
            packet
                .share_for_relay(&state, TransportMediaId::new(99))
                .is_none()
        );
    }

    #[test]
    fn forwarded_packet_facts_detect_h264_idr_for_decoder_refresh() {
        let session_key = test_transport_session_key(48, 0, 16, UserId::Integer(14));
        let producer_mid = Mid::from("cam-up");
        let mut state = PacketLoopState::default();
        let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: session_key.clone(),
            mid: producer_mid,
        });
        let parameters = RouterRtpParameters::new(
            vec![MediaFormat::new(
                RouterMediaKind::Video,
                CodecName::H264,
                102,
                90_000,
            )],
            vec![],
            vec![],
        );
        if let Some(session_state) = state.users.get_mut(&session_key) {
            session_state
                .sdp_negotiation
                .negotiated_producer_parameters
                .insert(producer_mid, parameters.clone());
        }
        state.refresh_source_decoder_refresh_codec(transport_media_id, &parameters);
        let mut packet = sample_forwarded_packet(session_key, "cam-up", &[0x65, 0x88]);
        let facts = packet.resolve_facts(&state);
        assert!(facts.is_some());
        let Some(facts) = facts else {
            return;
        };

        assert_eq!(facts.source_transport_media_id, transport_media_id);
        assert!(facts.decoder_refresh);
    }

    #[test]
    fn forwarded_packet_h264_refresh_detection_uses_packetization_mode() {
        let session_key = test_transport_session_key(48, 0, 16, UserId::Integer(14));
        let producer_mid = Mid::from("cam-up");
        let mut state = PacketLoopState::default();
        let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: session_key.clone(),
            mid: producer_mid,
        });
        let stap_a_idr = &[0x78, 0x00, 0x02, 0x65, 0x88];
        let mode_0_parameters = RouterRtpParameters::new(
            vec![
                MediaFormat::new(RouterMediaKind::Video, CodecName::H264, 102, 90_000)
                    .with_setting(CodecSetting::H264PacketizationMode(0)),
            ],
            vec![],
            vec![],
        );
        state.refresh_source_decoder_refresh_codec(transport_media_id, &mode_0_parameters);
        let mut mode_0_packet = sample_forwarded_packet(session_key.clone(), "cam-up", stap_a_idr);
        let mode_0_facts = mode_0_packet.resolve_facts(&state);
        assert!(mode_0_facts.is_some());
        let Some(mode_0_facts) = mode_0_facts else {
            return;
        };
        assert!(!mode_0_facts.decoder_refresh);

        let mode_1_parameters = RouterRtpParameters::new(
            vec![
                MediaFormat::new(RouterMediaKind::Video, CodecName::H264, 102, 90_000)
                    .with_setting(CodecSetting::H264PacketizationMode(1)),
            ],
            vec![],
            vec![],
        );
        state.refresh_source_decoder_refresh_codec(transport_media_id, &mode_1_parameters);
        let mut mode_1_packet = sample_forwarded_packet(session_key, "cam-up", stap_a_idr);
        let mode_1_facts = mode_1_packet.resolve_facts(&state);
        assert!(mode_1_facts.is_some());
        let Some(mode_1_facts) = mode_1_facts else {
            return;
        };
        assert!(mode_1_facts.decoder_refresh);
    }

    #[test]
    fn forwarded_packet_facts_detect_relayed_h264_idr_from_source_media_id() {
        let session_key = test_transport_session_key(49, 0, 17, UserId::Integer(15));
        let source_transport_media_id = TransportMediaId::new(23);
        let mut state = PacketLoopState::default();
        state.refresh_source_decoder_refresh_codec(
            source_transport_media_id,
            &RouterRtpParameters::new(
                vec![MediaFormat::new(
                    RouterMediaKind::Video,
                    CodecName::H264,
                    102,
                    90_000,
                )],
                vec![],
                vec![],
            ),
        );
        let source_packet = sample_forwarded_packet(session_key, "cam-up", &[0x65, 0x88]);
        let relay_packet = source_packet.share_for_relay(&state, source_transport_media_id);
        assert!(relay_packet.is_some());
        let Some(mut relay_packet) = relay_packet else {
            return;
        };
        let facts = relay_packet.resolve_facts(&state);
        assert!(facts.is_some());
        let Some(facts) = facts else {
            return;
        };

        assert_eq!(facts.source_transport_media_id, source_transport_media_id);
        assert!(facts.decoder_refresh);
    }

    #[test]
    fn forwarded_packet_exposes_recording_payload_and_received_at() {
        let session_key = test_transport_session_key(42, 0, 10, UserId::Integer(8));
        let state = PacketLoopState::default();
        let packet = sample_forwarded_packet(session_key.clone(), "aud-up", b"payload");

        assert_eq!(packet.source_session_key(&state), Some(&session_key));
        assert_eq!(packet.payload(), b"payload");
        assert_eq!(packet.payload_len(), 7);
        assert!(packet.received_at() <= Instant::now());
    }

    #[test]
    fn forwarded_packet_relay_clone_keeps_payload_and_explicit_source_media_id() {
        let session_key = test_transport_session_key(43, 0, 11, UserId::Integer(9));
        let state = PacketLoopState::default();
        let packet = sample_forwarded_packet(session_key.clone(), "aud-up", b"payload");
        let relay_packet = packet.share_for_relay(&state, TransportMediaId::new(18));
        assert!(relay_packet.is_some());
        let Some(mut relay_packet) = relay_packet else {
            return;
        };

        assert_eq!(relay_packet.source_session_key(&state), Some(&session_key));
        assert_eq!(relay_packet.payload(), b"payload");
        assert_eq!(
            relay_packet.resolve_source_transport_media_id(&PacketLoopState::default()),
            Some(TransportMediaId::new(18))
        );
    }

    #[test]
    fn forwarded_packet_projects_rid_and_frame_marking_for_route_control() {
        let session_key = test_transport_session_key(46, 0, 14, UserId::Integer(12));
        let frame_mark = u32::from(frame_marking::TEMPORAL_LAYER_ID_MAX) << 24;
        let mut packet = sample_forwarded_packet_with_frame_mark(
            session_key,
            "cam-up",
            Some("hi"),
            frame_mark,
            b"payload",
        );

        assert_eq!(
            packet.resolve_route_control_layer_metadata(&PacketLoopState::default()),
            PacketLayerMetadata::new(
                Some(Rid::from("hi")),
                Some(frame_marking::TEMPORAL_LAYER_ID_MAX)
            )
        );
    }

    #[test]
    fn forwarded_packet_facts_expose_vp8_payload_identity() {
        let session_key = test_transport_session_key(50, 0, 18, UserId::Integer(16));
        let mut state = PacketLoopState::default();
        let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: session_key.clone(),
            mid: Mid::from("cam-up"),
        });
        let mut packet = sample_forwarded_packet_with_rid(
            session_key,
            "cam-up",
            Some("hi"),
            &[0x90, 0xc0, 0x80, 0x02, 0x09, 0x00],
        );
        let facts = packet.resolve_facts(&state);
        assert!(facts.is_some());
        let Some(facts) = facts else {
            return;
        };
        let vp8_payload = facts.vp8_payload;
        assert!(vp8_payload.is_some());
        let Some(vp8_payload) = vp8_payload else {
            return;
        };

        assert_eq!(facts.source_transport_media_id, transport_media_id);
        assert_eq!(
            facts.layer_metadata,
            PacketLayerMetadata::new(Some(Rid::from("hi")), None)
        );
        assert_eq!(vp8_payload.identity.picture_id, Some(2));
        assert_eq!(vp8_payload.identity.tl0_pic_idx, Some(9));
        assert_eq!(packet.local_vp8_payload(), Some(vp8_payload));
    }

    #[test]
    fn forwarded_packet_recovers_rid_from_ssrc_binding_when_extension_is_absent() {
        let session_key = test_transport_session_key(47, 0, 15, UserId::Integer(13));
        let producer_mid = Mid::from("cam-up");
        let producer_ssrc = 76_543_u32;
        let mut state = PacketLoopState::default();
        state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: session_key.clone(),
            mid: producer_mid,
        });
        state.refresh_producer_ssrc_bindings(
            &session_key,
            producer_mid,
            &RouterRtpParameters::new(
                vec![],
                vec![],
                vec![StreamBinding::new().with_ssrc(producer_ssrc).with_rid("hi")],
            )
            .with_mid(producer_mid.to_string()),
        );
        let mut packet =
            sample_forwarded_packet_without_mid(session_key, producer_ssrc, b"payload");

        assert_eq!(
            packet.resolve_route_control_layer_metadata(&state),
            PacketLayerMetadata::new(Some(Rid::from("hi")), None)
        );
    }

    #[test]
    fn forwarded_packet_relay_clone_preserves_resolved_rid_from_source_worker() {
        let session_key = test_transport_session_key(47, 0, 15, UserId::Integer(13));
        let producer_mid = Mid::from("cam-up");
        let producer_ssrc = 76_543_u32;
        let source_transport_media_id = TransportMediaId::new(22);
        let mut source_state = PacketLoopState::default();
        source_state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: session_key.clone(),
            mid: producer_mid,
        });
        source_state.refresh_producer_ssrc_bindings(
            &session_key,
            producer_mid,
            &RouterRtpParameters::new(
                vec![],
                vec![],
                vec![StreamBinding::new().with_ssrc(producer_ssrc).with_rid("hi")],
            )
            .with_mid(producer_mid.to_string()),
        );
        let mut packet =
            sample_forwarded_packet_without_mid(session_key, producer_ssrc, b"payload");

        assert_eq!(
            packet.resolve_route_control_layer_metadata(&source_state),
            PacketLayerMetadata::new(Some(Rid::from("hi")), None)
        );
        let relay_packet = packet.share_for_relay(&source_state, source_transport_media_id);
        assert!(relay_packet.is_some());
        let Some(mut relay_packet) = relay_packet else {
            return;
        };

        assert_eq!(
            relay_packet.resolve_route_control_layer_metadata(&PacketLoopState::default()),
            PacketLayerMetadata::new(Some(Rid::from("hi")), None)
        );
    }

    #[test]
    fn forwarded_packet_falls_back_to_ssrc_when_mid_is_missing() {
        let session_key = test_transport_session_key(44, 0, 12, UserId::Integer(10));
        let producer_mid = Mid::from("cam-up");
        let producer_ssrc = 65_432_u32;
        let mut state = PacketLoopState::default();
        let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: session_key.clone(),
            mid: producer_mid,
        });
        state.refresh_producer_ssrc_bindings(
            &session_key,
            producer_mid,
            &RouterRtpParameters::new(
                vec![],
                vec![],
                vec![StreamBinding::new().with_ssrc(producer_ssrc)],
            )
            .with_mid(producer_mid.to_string()),
        );
        let mut packet =
            sample_forwarded_packet_without_mid(session_key, producer_ssrc, b"payload");

        assert_eq!(
            packet.resolve_source_transport_media_id(&state),
            Some(transport_media_id)
        );
    }

    #[test]
    fn forwarded_packet_caches_the_resolved_source_transport_media_id() {
        let session_key = test_transport_session_key(45, 0, 13, UserId::Integer(11));
        let mut state = PacketLoopState::default();
        let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: session_key.clone(),
            mid: Mid::from("aud-up"),
        });
        let mut packet = sample_forwarded_packet(session_key, "aud-up", b"payload");

        assert_eq!(
            packet.resolve_source_transport_media_id(&state),
            Some(transport_media_id)
        );
        state.remove_media_handle(transport_media_id);
        assert_eq!(
            packet.resolve_source_transport_media_id(&state),
            Some(transport_media_id)
        );
    }

    #[test]
    fn forwarded_packet_facts_cache_the_resolved_source_transport_media_id() {
        let session_key = test_transport_session_key(51, 0, 19, UserId::Integer(17));
        let mut state = PacketLoopState::default();
        let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: session_key.clone(),
            mid: Mid::from("aud-up"),
        });
        let mut packet = sample_forwarded_packet(session_key, "aud-up", b"payload");
        let facts = packet.resolve_facts(&state);
        assert!(facts.is_some());
        let Some(facts) = facts else {
            return;
        };

        assert_eq!(facts.source_transport_media_id, transport_media_id);
        state.remove_media_handle(transport_media_id);
        assert_eq!(
            packet
                .resolve_facts(&state)
                .map(|facts| facts.source_transport_media_id),
            Some(transport_media_id)
        );
    }
}
