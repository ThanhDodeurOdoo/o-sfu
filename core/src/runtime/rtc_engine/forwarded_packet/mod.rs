use std::{mem::take, time::Instant};

use o_sfu_rfc::rtp::{self, frame_marking, h264, vp8};
use str0m::{
    media::{ExtensionValues, Mid, Rid},
    rtp::{RtpHeader, RtpPacket, Ssrc},
};

use super::{
    local_forwarding::LocalForwardedRtp, route_control::PacketLayerMetadata,
    shared_payload::SharedPayload, state::RtcBootstrapState,
};
use crate::runtime::media_transport::{TransportMediaId, TransportSessionKey};

#[cfg(any(test, feature = "testing-transport"))]
pub mod test_support;

#[derive(Debug)]
pub struct ForwardedPacket {
    source_session_key: TransportSessionKey,
    source_transport_media_id: Option<TransportMediaId>,
    resolved_source_rid: Option<Rid>,
    visits_origin_sinks: bool,
    received_at: Instant,
    payload: SharedPayload,
    data: ForwardedPacketData,
}

#[derive(Debug)]
enum ForwardedPacketData {
    Str0mRtp(ForwardedRtpData),
    RelayRtp(ForwardedRelayRtpData),
}

#[derive(Debug)]
struct ForwardedRtpData {
    rtp_packet: RtpPacket,
}

#[derive(Debug)]
pub(super) struct ForwardedRelayRtpData {
    pub(super) header: RtpHeader,
}

impl ForwardedPacket {
    pub(super) fn from_rtp_packet(
        source_session_key: TransportSessionKey,
        mut rtp_packet: RtpPacket,
    ) -> Self {
        Self {
            source_session_key,
            source_transport_media_id: None,
            resolved_source_rid: None,
            visits_origin_sinks: true,
            received_at: rtp_packet.timestamp,
            payload: SharedPayload::from_vec(take(&mut rtp_packet.payload)),
            data: ForwardedPacketData::Str0mRtp(ForwardedRtpData { rtp_packet }),
        }
    }

    pub fn source_session_key(&self) -> &TransportSessionKey {
        &self.source_session_key
    }

    pub const fn received_at(&self) -> Instant {
        self.received_at
    }

    pub fn payload(&self) -> &SharedPayload {
        &self.payload
    }

    pub(super) fn resolve_route_control_layer_metadata(
        &mut self,
        state: &RtcBootstrapState,
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

    pub(super) fn route_control_audio_level(&self) -> Option<i8> {
        self.route_control_extension_values().audio_level
    }

    pub(super) fn route_control_voice_activity(&self) -> Option<bool> {
        self.route_control_extension_values().voice_activity
    }

    pub(super) fn route_control_decoder_refresh(
        &self,
        state: &RtcBootstrapState,
        source_transport_media_id: TransportMediaId,
    ) -> bool {
        if producer_uses_codec(state, source_transport_media_id, &rtp::CodecName::H264) {
            return h264::payload_starts_idr(self.payload.as_slice());
        }
        if producer_uses_codec(state, source_transport_media_id, &rtp::CodecName::Vp8)
            || producer_codec_unknown(state, source_transport_media_id)
        {
            return vp8::payload_starts_keyframe(self.payload.as_slice());
        }
        false
    }

    pub(super) fn route_control_ssrc(&self) -> Ssrc {
        match &self.data {
            ForwardedPacketData::Str0mRtp(rtp_data) => rtp_data.rtp_packet.header.ssrc,
            ForwardedPacketData::RelayRtp(rtp_data) => rtp_data.header.ssrc,
        }
    }

    pub(super) fn route_control_mid(&self) -> Option<Mid> {
        self.route_control_extension_values().mid
    }

    pub(super) fn route_control_rid_extension(&self) -> Option<Rid> {
        let extensions = self.route_control_extension_values();
        extensions.rid.or(extensions.rid_repair)
    }

    pub(super) fn share_for_relay(&self, source_transport_media_id: TransportMediaId) -> Self {
        let data = match &self.data {
            ForwardedPacketData::Str0mRtp(rtp_data) => {
                ForwardedPacketData::RelayRtp(ForwardedRelayRtpData {
                    header: rtp_data.rtp_packet.header.clone(),
                })
            }
            ForwardedPacketData::RelayRtp(rtp_data) => {
                ForwardedPacketData::RelayRtp(ForwardedRelayRtpData {
                    header: rtp_data.header.clone(),
                })
            }
        };
        Self {
            source_session_key: self.source_session_key.clone(),
            source_transport_media_id: Some(source_transport_media_id),
            resolved_source_rid: self.resolved_source_rid,
            visits_origin_sinks: false,
            received_at: self.received_at,
            payload: self.payload.share(),
            data,
        }
    }

    pub(super) fn payload_len(&self) -> usize {
        self.payload().len()
    }

    pub(super) const fn visits_origin_sinks(&self) -> bool {
        self.visits_origin_sinks
    }

    pub(super) fn resolve_source_transport_media_id(
        &mut self,
        state: &RtcBootstrapState,
    ) -> Option<TransportMediaId> {
        if let Some(source_transport_media_id) = self.source_transport_media_id {
            return Some(source_transport_media_id);
        }
        let resolved = match &self.data {
            ForwardedPacketData::Str0mRtp(rtp_data) => {
                if let Some(source_mid) = rtp_data.rtp_packet.header.ext_vals.mid
                    && let Some(source_transport_media_id) = state
                        .source_transport_media_id_for_mid(&self.source_session_key, source_mid)
                {
                    Some(source_transport_media_id)
                } else {
                    let source_ssrc = rtp_data.rtp_packet.header.ssrc;
                    state.source_transport_media_id_for_ssrc(&self.source_session_key, source_ssrc)
                }
            }
            ForwardedPacketData::RelayRtp(rtp_data) => {
                if let Some(source_mid) = rtp_data.header.ext_vals.mid
                    && let Some(source_transport_media_id) = state
                        .source_transport_media_id_for_mid(&self.source_session_key, source_mid)
                {
                    Some(source_transport_media_id)
                } else {
                    let source_ssrc = rtp_data.header.ssrc;
                    state.source_transport_media_id_for_ssrc(&self.source_session_key, source_ssrc)
                }
            }
        };
        if let Some(source_transport_media_id) = resolved {
            self.source_transport_media_id = Some(source_transport_media_id);
        }
        resolved
    }

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
        match &self.data {
            ForwardedPacketData::Str0mRtp(rtp_data) => &rtp_data.rtp_packet.header.ext_vals,
            ForwardedPacketData::RelayRtp(rtp_data) => &rtp_data.header.ext_vals,
        }
    }

    fn route_control_rid_from_ssrc(&self, state: &RtcBootstrapState) -> Option<Rid> {
        let source_ssrc = match &self.data {
            ForwardedPacketData::Str0mRtp(rtp_data) => rtp_data.rtp_packet.header.ssrc,
            ForwardedPacketData::RelayRtp(rtp_data) => rtp_data.header.ssrc,
        };
        state.source_rid_for_ssrc(&self.source_session_key, source_ssrc)
    }
}

fn producer_uses_codec(
    state: &RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
    codec_name: &rtp::CodecName,
) -> bool {
    producer_parameters(state, source_transport_media_id).is_some_and(|parameters| {
        parameters
            .formats()
            .any(|format| format.codec() == codec_name)
    })
}

fn producer_codec_unknown(
    state: &RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
) -> bool {
    producer_parameters(state, source_transport_media_id).is_none()
}

fn producer_parameters(
    state: &RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
) -> Option<&o_sfu_router::MediaStream> {
    let super::media_registry::RegisteredMediaHandle::Producer { session_key, mid } =
        state.media_handle(source_transport_media_id)?
    else {
        return None;
    };
    state
        .users
        .get(session_key)?
        .sdp_negotiation
        .negotiated_producer_parameters
        .get(mid)
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
        MediaFormat, MediaKind as RouterMediaKind, MediaStream as RouterRtpParameters,
        StreamBinding,
    };
    use str0m::media::{Mid, Rid};

    use super::*;
    use crate::{
        MediaCodecFlags,
        runtime::{
            UserId,
            rtc_engine::{
                bootstrap,
                forwarded_packet::test_support::{
                    sample_forwarded_packet, sample_forwarded_packet_with_frame_mark,
                    sample_forwarded_packet_without_mid,
                },
                media_registry::RegisteredMediaHandle,
                test_support::test_transport_session_key,
            },
        },
    };

    #[test]
    fn forwarded_packet_resolves_transport_media_id_through_the_registry() {
        let session_key = test_transport_session_key(41, 0, 9, UserId::Integer(7));
        let mut state = RtcBootstrapState::default();
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
    fn forwarded_packet_detects_h264_idr_for_decoder_refresh() {
        let session_key = test_transport_session_key(48, 0, 16, UserId::Integer(14));
        let producer_mid = Mid::from("cam-up");
        let mut state = RtcBootstrapState::default();
        let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 47_000));
        assert!(
            bootstrap::ensure_session_rtc_state(
                &mut state.users,
                &session_key,
                candidate_addr,
                10_000_000,
                MediaCodecFlags::default().with_h264(true),
            )
            .is_ok()
        );
        let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: session_key.clone(),
            mid: producer_mid,
        });
        if let Some(session_state) = state.users.get_mut(&session_key) {
            session_state
                .sdp_negotiation
                .negotiated_producer_parameters
                .insert(
                    producer_mid,
                    RouterRtpParameters::new(
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
        }
        let packet = sample_forwarded_packet(session_key, "cam-up", &[0x65, 0x88]);

        assert!(packet.route_control_decoder_refresh(&state, transport_media_id));
    }

    #[test]
    fn forwarded_packet_exposes_recording_payload_and_received_at() {
        let session_key = test_transport_session_key(42, 0, 10, UserId::Integer(8));
        let packet = sample_forwarded_packet(session_key.clone(), "aud-up", b"payload");

        assert_eq!(packet.source_session_key(), &session_key);
        assert_eq!(packet.payload().as_slice(), b"payload");
        assert_eq!(packet.payload_len(), 7);
        assert!(packet.received_at() <= Instant::now());
    }

    #[test]
    fn forwarded_packet_relay_clone_keeps_payload_and_explicit_source_media_id() {
        let session_key = test_transport_session_key(43, 0, 11, UserId::Integer(9));
        let packet = sample_forwarded_packet(session_key.clone(), "aud-up", b"payload");
        let mut relay_packet = packet.share_for_relay(TransportMediaId::new(18));

        assert_eq!(relay_packet.source_session_key(), &session_key);
        assert_eq!(relay_packet.payload().as_slice(), b"payload");
        assert_eq!(
            relay_packet.resolve_source_transport_media_id(&RtcBootstrapState::default()),
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
            packet.resolve_route_control_layer_metadata(&RtcBootstrapState::default()),
            PacketLayerMetadata::new(
                Some(Rid::from("hi")),
                Some(frame_marking::TEMPORAL_LAYER_ID_MAX)
            )
        );
    }

    #[test]
    fn forwarded_packet_recovers_rid_from_ssrc_binding_when_extension_is_absent() {
        let session_key = test_transport_session_key(47, 0, 15, UserId::Integer(13));
        let producer_mid = Mid::from("cam-up");
        let producer_ssrc = 76_543_u32;
        let mut state = RtcBootstrapState::default();
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
        let mut source_state = RtcBootstrapState::default();
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
        let mut relay_packet = packet.share_for_relay(source_transport_media_id);

        assert_eq!(
            relay_packet.resolve_route_control_layer_metadata(&RtcBootstrapState::default()),
            PacketLayerMetadata::new(Some(Rid::from("hi")), None)
        );
    }

    #[test]
    fn forwarded_packet_falls_back_to_ssrc_when_mid_is_missing() {
        let session_key = test_transport_session_key(44, 0, 12, UserId::Integer(10));
        let producer_mid = Mid::from("cam-up");
        let producer_ssrc = 65_432_u32;
        let mut state = RtcBootstrapState::default();
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
        let mut state = RtcBootstrapState::default();
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
}
