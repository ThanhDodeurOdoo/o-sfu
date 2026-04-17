use std::{mem::take, time::Instant};

use str0m::{
    media::{ExtensionValues, Rid},
    rtp::{RtpHeader, RtpPacket, SeqNo},
};

#[cfg(test)]
use str0m::{
    media::{Mid, Pt},
    rtp::Ssrc,
};

use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};

use super::local_forwarding::LocalForwardedRtp;
use super::shared_payload::SharedPayload;
use super::state::RtcBootstrapState;

#[derive(Debug)]
pub(crate) struct ForwardedPacket {
    source_session_key: TransportSessionKey,
    source_transport_media_id: Option<TransportMediaId>,
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
    pub(super) seq_no: SeqNo,
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
            received_at: rtp_packet.timestamp,
            payload: SharedPayload::from_vec(take(&mut rtp_packet.payload)),
            data: ForwardedPacketData::Str0mRtp(ForwardedRtpData { rtp_packet }),
        }
    }

    pub(crate) fn source_session_key(&self) -> &TransportSessionKey {
        &self.source_session_key
    }

    pub(crate) const fn received_at(&self) -> Instant {
        self.received_at
    }

    pub(crate) fn payload(&self) -> &SharedPayload {
        &self.payload
    }

    pub(super) fn route_control_rid(&self) -> Option<Rid> {
        match &self.data {
            ForwardedPacketData::Str0mRtp(rtp_data) => rtp_data
                .rtp_packet
                .header
                .ext_vals
                .rid
                .or(rtp_data.rtp_packet.header.ext_vals.rid_repair),
            ForwardedPacketData::RelayRtp(rtp_data) => rtp_data
                .header
                .ext_vals
                .rid
                .or(rtp_data.header.ext_vals.rid_repair),
        }
    }

    pub(super) fn route_control_audio_level(&self) -> Option<i8> {
        self.route_control_extension_values().audio_level
    }

    pub(super) fn route_control_voice_activity(&self) -> Option<bool> {
        self.route_control_extension_values().voice_activity
    }

    pub(super) fn share_for_relay(&self, source_transport_media_id: TransportMediaId) -> Self {
        let data = match &self.data {
            ForwardedPacketData::Str0mRtp(rtp_data) => {
                ForwardedPacketData::RelayRtp(ForwardedRelayRtpData {
                    seq_no: rtp_data.rtp_packet.seq_no,
                    header: rtp_data.rtp_packet.header.clone(),
                })
            }
            ForwardedPacketData::RelayRtp(rtp_data) => {
                ForwardedPacketData::RelayRtp(ForwardedRelayRtpData {
                    seq_no: rtp_data.seq_no,
                    header: rtp_data.header.clone(),
                })
            }
        };
        Self {
            source_session_key: self.source_session_key.clone(),
            source_transport_media_id: Some(source_transport_media_id),
            received_at: self.received_at,
            payload: self.payload.share(),
            data,
        }
    }

    pub(super) fn payload_len(&self) -> usize {
        self.payload().len()
    }

    pub(super) const fn visits_origin_sinks(&self) -> bool {
        self.source_transport_media_id.is_none()
    }

    pub(super) fn resolve_source_transport_media_id(
        &self,
        state: &RtcBootstrapState,
    ) -> Option<TransportMediaId> {
        if let Some(source_transport_media_id) = self.source_transport_media_id {
            return Some(source_transport_media_id);
        }
        match &self.data {
            ForwardedPacketData::Str0mRtp(rtp_data) => {
                if let Some(source_mid) = rtp_data.rtp_packet.header.ext_vals.mid
                    && let Some(source_transport_media_id) = state
                        .source_transport_media_id_for_mid(&self.source_session_key, source_mid)
                {
                    return Some(source_transport_media_id);
                }
                let source_ssrc = rtp_data.rtp_packet.header.ssrc;
                state.source_transport_media_id_for_ssrc(&self.source_session_key, source_ssrc)
            }
            ForwardedPacketData::RelayRtp(rtp_data) => {
                if let Some(source_mid) = rtp_data.header.ext_vals.mid
                    && let Some(source_transport_media_id) = state
                        .source_transport_media_id_for_mid(&self.source_session_key, source_mid)
                {
                    return Some(source_transport_media_id);
                }
                let source_ssrc = rtp_data.header.ssrc;
                state.source_transport_media_id_for_ssrc(&self.source_session_key, source_ssrc)
            }
        }
    }

    pub(super) fn local_send_packet(&mut self) -> LocalForwardedRtp<'_> {
        let payload = &mut self.payload;
        match &mut self.data {
            ForwardedPacketData::Str0mRtp(rtp_data) => {
                LocalForwardedRtp::new(&rtp_data.rtp_packet, payload)
            }
            ForwardedPacketData::RelayRtp(rtp_data) => {
                let seq_no = rtp_data.seq_no;
                let header = &rtp_data.header;
                LocalForwardedRtp::from_relay(seq_no, header, self.received_at, payload)
            }
        }
    }

    fn route_control_extension_values(&self) -> &ExtensionValues {
        match &self.data {
            ForwardedPacketData::Str0mRtp(rtp_data) => &rtp_data.rtp_packet.header.ext_vals,
            ForwardedPacketData::RelayRtp(rtp_data) => &rtp_data.header.ext_vals,
        }
    }
}

#[cfg(test)]
pub(crate) fn sample_forwarded_packet(
    source_session_key: TransportSessionKey,
    mid: &str,
    payload: &[u8],
) -> ForwardedPacket {
    sample_forwarded_packet_with_rid(source_session_key, mid, None, payload)
}

#[cfg(test)]
pub(crate) fn sample_forwarded_packet_with_rid(
    source_session_key: TransportSessionKey,
    mid: &str,
    rid: Option<&str>,
    payload: &[u8],
) -> ForwardedPacket {
    sample_forwarded_packet_with_extensions(
        source_session_key,
        mid,
        rid,
        ExtensionValues::default(),
        payload,
    )
}

#[cfg(test)]
pub(crate) fn sample_forwarded_packet_with_audio_activity(
    source_session_key: TransportSessionKey,
    mid: &str,
    voice_activity: Option<bool>,
    audio_level: Option<i8>,
    payload: &[u8],
) -> ForwardedPacket {
    sample_forwarded_packet_with_extensions(
        source_session_key,
        mid,
        None,
        ExtensionValues {
            audio_level,
            voice_activity,
            ..ExtensionValues::default()
        },
        payload,
    )
}

#[cfg(test)]
fn sample_forwarded_packet_with_extensions(
    source_session_key: TransportSessionKey,
    mid: &str,
    rid: Option<&str>,
    ext_vals: ExtensionValues,
    payload: &[u8],
) -> ForwardedPacket {
    let received_at = Instant::now();
    ForwardedPacket {
        source_session_key,
        source_transport_media_id: None,
        received_at,
        payload: SharedPayload::from_vec(payload.to_vec()),
        data: ForwardedPacketData::RelayRtp(ForwardedRelayRtpData {
            seq_no: SeqNo::from(1),
            header: RtpHeader {
                version: 2,
                has_padding: false,
                has_extension: false,
                csrc_count: 0,
                marker: false,
                payload_type: Pt::from(111),
                sequence_number: 1,
                timestamp: 1234,
                ssrc: Ssrc::from(4321),
                csrc: [0; 15],
                ext_vals: ExtensionValues {
                    rid: rid.map(Rid::from),
                    mid: Some(Mid::from(mid)),
                    ..ext_vals
                },
                header_len: 12,
            },
        }),
    }
}

#[cfg(test)]
fn sample_forwarded_packet_without_mid(
    source_session_key: TransportSessionKey,
    ssrc: u32,
    payload: &[u8],
) -> ForwardedPacket {
    let received_at = Instant::now();
    ForwardedPacket {
        source_session_key,
        source_transport_media_id: None,
        received_at,
        payload: SharedPayload::from_vec(payload.to_vec()),
        data: ForwardedPacketData::RelayRtp(ForwardedRelayRtpData {
            seq_no: SeqNo::from(1),
            header: RtpHeader {
                version: 2,
                has_padding: false,
                has_extension: false,
                csrc_count: 0,
                marker: false,
                payload_type: Pt::from(111),
                sequence_number: 1,
                timestamp: 1234,
                ssrc: Ssrc::from(ssrc),
                csrc: [0; 15],
                ext_vals: ExtensionValues::default(),
                header_len: 12,
            },
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    use o_sfu_router::{RtpParameters as RouterRtpParameters, StreamBinding};

    use crate::config::MediaCodecFlags;
    use crate::runtime::rtc_adapter::bootstrap::ensure_session_rtc_state;
    use crate::runtime::rtc_adapter::media_registry::RegisteredMediaHandle;
    use crate::signaling::shared::SessionId;

    #[test]
    fn forwarded_packet_resolves_transport_media_id_through_the_registry() {
        let session_key = TransportSessionKey::new(41, 0, 9, SessionId::Integer(7));
        let mut state = RtcBootstrapState::default();
        let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: session_key.clone(),
            mid: Mid::from("aud-up"),
        });
        let packet = sample_forwarded_packet(session_key, "aud-up", b"payload");

        assert_eq!(
            packet.resolve_source_transport_media_id(&state),
            Some(transport_media_id)
        );
    }

    #[test]
    fn forwarded_packet_exposes_recording_payload_and_received_at() {
        let session_key = TransportSessionKey::new(42, 0, 10, SessionId::Integer(8));
        let packet = sample_forwarded_packet(session_key.clone(), "aud-up", b"payload");

        assert_eq!(packet.source_session_key(), &session_key);
        assert_eq!(packet.payload().as_slice(), b"payload");
        assert_eq!(packet.payload_len(), 7);
        assert!(packet.received_at() <= Instant::now());
    }

    #[test]
    fn forwarded_packet_relay_clone_keeps_payload_and_explicit_source_media_id() {
        let session_key = TransportSessionKey::new(43, 0, 11, SessionId::Integer(9));
        let packet = sample_forwarded_packet(session_key.clone(), "aud-up", b"payload");
        let relay_packet = packet.share_for_relay(TransportMediaId::new(18));

        assert_eq!(relay_packet.source_session_key(), &session_key);
        assert_eq!(relay_packet.payload().as_slice(), b"payload");
        assert_eq!(
            relay_packet.resolve_source_transport_media_id(&RtcBootstrapState::default()),
            Some(TransportMediaId::new(18))
        );
    }

    #[test]
    fn forwarded_packet_falls_back_to_ssrc_when_mid_is_missing() {
        let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_071));
        let session_key = TransportSessionKey::new(44, 0, 12, SessionId::Integer(10));
        let producer_mid = Mid::from("cam-up");
        let producer_ssrc = 65_432_u32;
        let mut state = RtcBootstrapState::default();

        assert!(
            ensure_session_rtc_state(
                &mut state.sessions,
                &session_key,
                candidate_addr,
                MediaCodecFlags::default(),
            )
            .is_ok()
        );
        let Some(session_state) = state.sessions.get_mut(&session_key) else {
            return;
        };
        session_state
            .sdp_negotiation
            .negotiated_producer_parameters
            .insert(
                producer_mid,
                RouterRtpParameters::new(
                    vec![],
                    vec![],
                    vec![StreamBinding::new().with_ssrc(producer_ssrc)],
                )
                .with_mid(producer_mid.to_string()),
            );
        let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: session_key.clone(),
            mid: producer_mid,
        });
        let packet = sample_forwarded_packet_without_mid(session_key, producer_ssrc, b"payload");

        assert_eq!(
            packet.resolve_source_transport_media_id(&state),
            Some(transport_media_id)
        );
    }
}
