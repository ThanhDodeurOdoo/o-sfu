use std::{mem::take, time::Instant};

use str0m::{media::MediaData, rtp::RtpPacket};

#[cfg(test)]
use str0m::{
    format::{Codec, CodecExtra, CodecSpec, FormatParams, PayloadParams},
    media::{ExtensionValues, Frequency, MediaTime, Mid, Pt},
    rtp::SeqNo,
};

use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};

use super::local_forwarding::{LocalForwardedFrame, LocalForwardedPacket, LocalForwardedRtp};
use super::shared_payload::SharedPayload;
use super::state::RtcBootstrapState;

#[derive(Debug)]
pub(crate) struct ForwardedPacket {
    source_session_key: TransportSessionKey,
    received_at: Instant,
    data: ForwardedPacketData,
}

#[derive(Debug)]
enum ForwardedPacketData {
    Str0mFrame(ForwardedFrameData),
    Str0mRtp(ForwardedRtpData),
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

impl ForwardedPacket {
    pub(super) fn from_media_data(
        source_session_key: TransportSessionKey,
        mut media_data: MediaData,
    ) -> Self {
        Self {
            source_session_key,
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
        }
    }

    pub(super) fn payload_len(&self) -> usize {
        self.payload().len()
    }

    pub(super) fn resolve_source_transport_media_id(
        &self,
        state: &RtcBootstrapState,
    ) -> Option<TransportMediaId> {
        match &self.data {
            ForwardedPacketData::Str0mFrame(frame_data) => state.source_transport_media_id_for_mid(
                &self.source_session_key,
                frame_data.media_data.mid,
            ),
            ForwardedPacketData::Str0mRtp(rtp_data) => {
                let source_mid = rtp_data.rtp_packet.header.ext_vals.mid?;
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
                LocalForwardedRtp::new(&mut rtp_data.rtp_packet, &mut rtp_data.payload),
            ),
        }
    }
}

#[cfg(test)]
pub(crate) fn sample_forwarded_packet(
    source_session_key: TransportSessionKey,
    mid: &str,
    payload: &[u8],
) -> ForwardedPacket {
    ForwardedPacket::from_media_data(
        source_session_key,
        MediaData {
            mid: Mid::from(mid),
            pt: Pt::from(111),
            rid: None,
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
}
