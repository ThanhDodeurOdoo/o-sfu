use std::{mem::take, time::Instant};

use str0m::{
    media::{MediaData, Rid},
    rtp::{RtpHeader, RtpPacket, SeqNo},
};

#[cfg(test)]
use str0m::{
    format::{Codec, CodecExtra, CodecSpec, FormatParams, PayloadParams},
    media::{ExtensionValues, Frequency, MediaTime, Mid, Pt},
};

use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};

use super::local_forwarding::{LocalForwardedFrame, LocalForwardedPacket, LocalForwardedRtp};
use super::shared_payload::SharedPayload;
use super::state::RtcBootstrapState;

#[derive(Debug)]
pub(crate) struct ForwardedPacket {
    source_session_key: TransportSessionKey,
    source_transport_media_id: Option<TransportMediaId>,
    received_at: Instant,
    data: ForwardedPacketData,
}

#[derive(Debug)]
enum ForwardedPacketData {
    Str0mFrame(ForwardedFrameData),
    Str0mRtp(ForwardedRtpData),
    RelayRtp(ForwardedRelayRtpData),
}

#[derive(Debug)]
struct ForwardedFrameData {
    media_data: MediaData,
    payload: SharedPayload,
}

#[derive(Debug)]
struct ForwardedRtpData {
    rtp_packet: RtpPacket,
    payload: SharedPayload,
}

#[derive(Debug)]
pub(super) struct ForwardedRelayRtpData {
    pub(super) seq_no: SeqNo,
    pub(super) header: RtpHeader,
    pub(super) timestamp: Instant,
    payload: SharedPayload,
}

impl ForwardedPacket {
    pub(super) fn from_media_data(
        source_session_key: TransportSessionKey,
        mut media_data: MediaData,
    ) -> Self {
        Self {
            source_session_key,
            source_transport_media_id: None,
            received_at: media_data.network_time,
            data: ForwardedPacketData::Str0mFrame(ForwardedFrameData {
                payload: SharedPayload::from_vec(take(&mut media_data.data)),
                media_data,
            }),
        }
    }

    pub(super) fn from_rtp_packet(
        source_session_key: TransportSessionKey,
        mut rtp_packet: RtpPacket,
    ) -> Self {
        Self {
            source_session_key,
            source_transport_media_id: None,
            received_at: rtp_packet.timestamp,
            data: ForwardedPacketData::Str0mRtp(ForwardedRtpData {
                payload: SharedPayload::from_vec(take(&mut rtp_packet.payload)),
                rtp_packet,
            }),
        }
    }

    pub(crate) fn source_session_key(&self) -> &TransportSessionKey {
        &self.source_session_key
    }

    pub(crate) const fn received_at(&self) -> Instant {
        self.received_at
    }

    pub(crate) fn payload(&self) -> &SharedPayload {
        match &self.data {
            ForwardedPacketData::Str0mFrame(frame_data) => &frame_data.payload,
            ForwardedPacketData::Str0mRtp(rtp_data) => &rtp_data.payload,
            ForwardedPacketData::RelayRtp(rtp_data) => &rtp_data.payload,
        }
    }

    pub(super) fn route_control_rid(&self) -> Option<Rid> {
        match &self.data {
            ForwardedPacketData::Str0mFrame(frame_data) => frame_data.media_data.rid,
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

    pub(super) fn share_for_relay(&self, source_transport_media_id: TransportMediaId) -> Self {
        let shared_payload = self.payload().to_shared();
        let data = match &self.data {
            ForwardedPacketData::Str0mFrame(frame_data) => {
                ForwardedPacketData::Str0mFrame(ForwardedFrameData {
                    media_data: clone_media_data_without_payload(
                        &frame_data.media_data,
                        self.received_at,
                    ),
                    payload: SharedPayload::from_shared(shared_payload),
                })
            }
            ForwardedPacketData::Str0mRtp(rtp_data) => {
                ForwardedPacketData::RelayRtp(ForwardedRelayRtpData {
                    seq_no: rtp_data.rtp_packet.seq_no,
                    header: rtp_data.rtp_packet.header.clone(),
                    timestamp: self.received_at,
                    payload: SharedPayload::from_shared(shared_payload),
                })
            }
            ForwardedPacketData::RelayRtp(rtp_data) => {
                ForwardedPacketData::RelayRtp(ForwardedRelayRtpData {
                    seq_no: rtp_data.seq_no,
                    header: rtp_data.header.clone(),
                    timestamp: rtp_data.timestamp,
                    payload: SharedPayload::from_shared(shared_payload),
                })
            }
        };
        Self {
            source_session_key: self.source_session_key.clone(),
            source_transport_media_id: Some(source_transport_media_id),
            received_at: self.received_at,
            data,
        }
    }

    pub(super) fn payload_len(&self) -> usize {
        self.payload().len()
    }

    pub(super) const fn uses_channel_side_sinks(&self) -> bool {
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
            ForwardedPacketData::Str0mFrame(frame_data) => state.source_transport_media_id_for_mid(
                &self.source_session_key,
                frame_data.media_data.mid,
            ),
            ForwardedPacketData::Str0mRtp(rtp_data) => {
                let source_mid = rtp_data.rtp_packet.header.ext_vals.mid?;
                state.source_transport_media_id_for_mid(&self.source_session_key, source_mid)
            }
            ForwardedPacketData::RelayRtp(rtp_data) => {
                let source_mid = rtp_data.header.ext_vals.mid?;
                state.source_transport_media_id_for_mid(&self.source_session_key, source_mid)
            }
        }
    }

    pub(super) fn local_send_packet(&mut self) -> LocalForwardedPacket<'_> {
        match &mut self.data {
            ForwardedPacketData::Str0mFrame(frame_data) => LocalForwardedPacket::Str0mFrame(
                LocalForwardedFrame::new(&mut frame_data.media_data, &mut frame_data.payload),
            ),
            ForwardedPacketData::Str0mRtp(rtp_data) => LocalForwardedPacket::Str0mRtp(
                LocalForwardedRtp::new(&rtp_data.rtp_packet, &mut rtp_data.payload),
            ),
            ForwardedPacketData::RelayRtp(rtp_data) => {
                let seq_no = rtp_data.seq_no;
                let timestamp = rtp_data.timestamp;
                let header = &rtp_data.header;
                let payload = &mut rtp_data.payload;
                LocalForwardedPacket::Str0mRtp(LocalForwardedRtp::from_relay(
                    seq_no, header, timestamp, payload,
                ))
            }
        }
    }
}

fn clone_media_data_without_payload(media_data: &MediaData, network_time: Instant) -> MediaData {
    MediaData {
        mid: media_data.mid,
        pt: media_data.pt,
        rid: media_data.rid,
        params: media_data.params,
        time: media_data.time,
        network_time,
        seq_range: media_data.seq_range.clone(),
        contiguous: media_data.contiguous,
        data: Vec::new(),
        ext_vals: media_data.ext_vals.clone(),
        codec_extra: media_data.codec_extra,
        last_sender_info: media_data.last_sender_info,
        audio_start_of_talk_spurt: media_data.audio_start_of_talk_spurt,
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
    ForwardedPacket::from_media_data(
        source_session_key,
        MediaData {
            mid: Mid::from(mid),
            pt: Pt::from(111),
            rid: rid.map(Rid::from),
            params: sample_payload_params(),
            time: MediaTime::from_millis(5),
            network_time: Instant::now(),
            seq_range: SeqNo::from(1)..=SeqNo::from(1),
            contiguous: true,
            data: payload.to_vec(),
            ext_vals: ExtensionValues::default(),
            codec_extra: CodecExtra::None,
            last_sender_info: None,
            audio_start_of_talk_spurt: false,
        },
    )
}

#[cfg(test)]
fn sample_payload_params() -> PayloadParams {
    PayloadParams::new(
        Pt::from(111),
        None,
        CodecSpec {
            codec: Codec::Opus,
            clock_rate: Frequency::FORTY_EIGHT_KHZ,
            channels: Some(2),
            format: FormatParams::default(),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
