use std::{mem::take, time::Instant};

use str0m::{
    RtcError,
    media::{MediaData, Mid},
    rtp::RtpPacket,
};

#[cfg(test)]
use str0m::{
    format::{Codec, CodecExtra, CodecSpec, FormatParams, PayloadParams},
    media::{ExtensionValues, Frequency, MediaTime, Pt},
    rtp::SeqNo,
};

use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};

use super::packet_mode::{ACTIVE_PACKET_MODE, PacketMode};
use super::state::{RtcBootstrapState, RtcSessionState};

#[derive(Debug)]
pub(crate) struct ForwardedPacket {
    source_session_key: TransportSessionKey,
    received_at: Instant,
    data: ForwardedPacketData,
}

#[derive(Debug)]
enum ForwardedPacketData {
    Str0mFrame(MediaData),
    Str0mRtp(RtpPacket),
}

impl ForwardedPacket {
    pub(super) fn from_media_data(
        source_session_key: TransportSessionKey,
        media_data: MediaData,
    ) -> Self {
        Self {
            source_session_key,
            received_at: media_data.network_time,
            data: ForwardedPacketData::Str0mFrame(media_data),
        }
    }

    pub(super) fn from_rtp_packet(
        source_session_key: TransportSessionKey,
        rtp_packet: RtpPacket,
    ) -> Self {
        Self {
            source_session_key,
            received_at: rtp_packet.timestamp,
            data: ForwardedPacketData::Str0mRtp(rtp_packet),
        }
    }

    pub(crate) fn source_session_key(&self) -> &TransportSessionKey {
        &self.source_session_key
    }

    pub(crate) const fn received_at(&self) -> Instant {
        self.received_at
    }

    pub(crate) fn payload(&self) -> &[u8] {
        match &self.data {
            ForwardedPacketData::Str0mFrame(media_data) => media_data.data.as_slice(),
            ForwardedPacketData::Str0mRtp(rtp_packet) => rtp_packet.payload.as_ref(),
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
            ForwardedPacketData::Str0mFrame(media_data) => {
                state.source_transport_media_id_for_mid(&self.source_session_key, media_data.mid)
            }
            ForwardedPacketData::Str0mRtp(rtp_packet) => {
                let source_mid = rtp_packet.header.ext_vals.mid?;
                state.source_transport_media_id_for_mid(&self.source_session_key, source_mid)
            }
        }
    }

    pub(super) fn forward_to_session(
        &mut self,
        session_state: &mut RtcSessionState,
        dest_mid: Mid,
        is_last_destination: bool,
    ) -> Result<Option<usize>, RtcError> {
        match &mut self.data {
            ForwardedPacketData::Str0mFrame(media_data) => {
                if ACTIVE_PACKET_MODE != PacketMode::Frame {
                    return Ok(None);
                }
                let Some(writer) = session_state.rtc.writer(dest_mid) else {
                    return Ok(None);
                };
                let Some(pt) = writer.match_params(media_data.params) else {
                    return Ok(None);
                };
                let mut data_writer = writer;
                if let Some(rid) = media_data.rid {
                    data_writer = data_writer.rid(rid);
                }
                if media_data.audio_start_of_talk_spurt {
                    data_writer = data_writer.start_of_talkspurt(true);
                }
                let payload_len = media_data.data.len();
                data_writer.write(
                    pt,
                    media_data.network_time,
                    media_data.time,
                    take_write_payload(&mut media_data.data, is_last_destination),
                )?;
                Ok(Some(payload_len))
            }
            ForwardedPacketData::Str0mRtp(rtp_packet) => {
                if ACTIVE_PACKET_MODE != PacketMode::Rtp {
                    return Ok(None);
                }
                let nackable = session_state
                    .rtc
                    .media(dest_mid)
                    .is_some_and(|media| !media.kind().is_audio());
                let rid = rtp_packet
                    .header
                    .ext_vals
                    .rid
                    .or(rtp_packet.header.ext_vals.rid_repair);
                let payload_len = rtp_packet.payload.len();
                let write_result = {
                    let mut direct_api = session_state.rtc.direct_api();
                    if let Some(rid) = rid {
                        if let Some(stream_tx) = direct_api.stream_tx_by_mid(dest_mid, Some(rid)) {
                            stream_tx.write_rtp_with_csrc(
                                rtp_packet.header.payload_type,
                                rtp_packet.seq_no,
                                rtp_packet.header.timestamp,
                                rtp_packet.timestamp,
                                rtp_packet.header.marker,
                                rtp_packet.header.ext_vals.clone(),
                                nackable,
                                rtp_packet.payload.clone(),
                                rtp_packet.header.csrc_count,
                                rtp_packet.header.csrc,
                            )
                        } else if let Some(stream_tx) = direct_api.stream_tx_by_mid(dest_mid, None)
                        {
                            stream_tx.write_rtp_with_csrc(
                                rtp_packet.header.payload_type,
                                rtp_packet.seq_no,
                                rtp_packet.header.timestamp,
                                rtp_packet.timestamp,
                                rtp_packet.header.marker,
                                rtp_packet.header.ext_vals.clone(),
                                nackable,
                                rtp_packet.payload.clone(),
                                rtp_packet.header.csrc_count,
                                rtp_packet.header.csrc,
                            )
                        } else {
                            return Ok(None);
                        }
                    } else if let Some(stream_tx) = direct_api.stream_tx_by_mid(dest_mid, None) {
                        stream_tx.write_rtp_with_csrc(
                            rtp_packet.header.payload_type,
                            rtp_packet.seq_no,
                            rtp_packet.header.timestamp,
                            rtp_packet.timestamp,
                            rtp_packet.header.marker,
                            rtp_packet.header.ext_vals.clone(),
                            nackable,
                            rtp_packet.payload.clone(),
                            rtp_packet.header.csrc_count,
                            rtp_packet.header.csrc,
                        )
                    } else {
                        return Ok(None);
                    }
                };
                write_result.map_err(|error| {
                    RtcError::Packet(dest_mid, rtp_packet.header.payload_type, error)
                })?;
                Ok(Some(payload_len))
            }
        }
    }
}

pub(super) fn take_write_payload(data: &mut Vec<u8>, is_last_destination: bool) -> Vec<u8> {
    if is_last_destination {
        take(data)
    } else {
        data.clone()
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
        assert_eq!(packet.payload(), b"payload");
        assert_eq!(packet.payload_len(), 7);
        assert!(packet.received_at() <= Instant::now());
    }
}
