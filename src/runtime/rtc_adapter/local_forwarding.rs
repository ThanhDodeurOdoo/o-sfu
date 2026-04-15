use std::time::Instant;

use str0m::{
    RtcError,
    media::{MediaData, Mid},
    rtp::{RtpHeader, RtpPacket, SeqNo, StreamTx},
};

use super::packet_mode::{ACTIVE_PACKET_MODE, PacketMode};
use super::shared_payload::SharedPayload;
use super::state::RtcSessionState;

#[derive(Debug, Clone)]
pub(super) struct LocalPacketDestination {
    dest_mid: Mid,
}

pub(super) enum LocalForwardedPacket<'a> {
    Str0mFrame(LocalForwardedFrame<'a>),
    Str0mRtp(LocalForwardedRtp<'a>),
}

pub(super) struct LocalForwardedFrame<'a> {
    media_data: &'a mut MediaData,
    payload: &'a mut SharedPayload,
}

pub(super) struct LocalForwardedRtp<'a> {
    data: LocalForwardedRtpData<'a>,
    payload: &'a mut SharedPayload,
}

enum LocalForwardedRtpData<'a> {
    Str0m(&'a RtpPacket),
    Relay {
        seq_no: SeqNo,
        header: &'a RtpHeader,
        timestamp: Instant,
    },
}

impl<'a> LocalForwardedFrame<'a> {
    pub(super) fn new(media_data: &'a mut MediaData, payload: &'a mut SharedPayload) -> Self {
        Self {
            media_data,
            payload,
        }
    }
}

impl<'a> LocalForwardedRtp<'a> {
    pub(super) fn new(rtp_packet: &'a RtpPacket, payload: &'a mut SharedPayload) -> Self {
        Self {
            data: LocalForwardedRtpData::Str0m(rtp_packet),
            payload,
        }
    }

    pub(super) fn from_relay(
        seq_no: SeqNo,
        header: &'a RtpHeader,
        timestamp: Instant,
        payload: &'a mut SharedPayload,
    ) -> Self {
        Self {
            data: LocalForwardedRtpData::Relay {
                seq_no,
                header,
                timestamp,
            },
            payload,
        }
    }

    fn seq_no(&self) -> SeqNo {
        match self.data {
            LocalForwardedRtpData::Str0m(packet) => packet.seq_no,
            LocalForwardedRtpData::Relay { seq_no, .. } => seq_no,
        }
    }

    fn header(&self) -> &RtpHeader {
        match self.data {
            LocalForwardedRtpData::Str0m(packet) => &packet.header,
            LocalForwardedRtpData::Relay { header, .. } => header,
        }
    }

    fn timestamp(&self) -> Instant {
        match self.data {
            LocalForwardedRtpData::Str0m(packet) => packet.timestamp,
            LocalForwardedRtpData::Relay { timestamp, .. } => timestamp,
        }
    }
}

impl LocalPacketDestination {
    pub(super) fn new(dest_mid: Mid) -> Self {
        Self { dest_mid }
    }

    pub(super) fn send(
        &self,
        session_state: &mut RtcSessionState,
        packet: LocalForwardedPacket<'_>,
        is_last_destination: bool,
    ) -> Result<Option<usize>, RtcError> {
        match packet {
            LocalForwardedPacket::Str0mFrame(mut frame) => {
                self.send_frame(session_state, &mut frame, is_last_destination)
            }
            LocalForwardedPacket::Str0mRtp(mut rtp) => {
                self.send_rtp(session_state, &mut rtp, is_last_destination)
            }
        }
    }

    fn send_frame(
        &self,
        session_state: &mut RtcSessionState,
        frame: &mut LocalForwardedFrame<'_>,
        is_last_destination: bool,
    ) -> Result<Option<usize>, RtcError> {
        if ACTIVE_PACKET_MODE != PacketMode::Frame {
            return Ok(None);
        }
        let Some(writer) = session_state.rtc.writer(self.dest_mid) else {
            return Ok(None);
        };
        let Some(pt) = writer.match_params(frame.media_data.params) else {
            return Ok(None);
        };
        let mut data_writer = writer;
        if let Some(rid) = frame.media_data.rid {
            data_writer = data_writer.rid(rid);
        }
        if frame.media_data.audio_start_of_talk_spurt {
            data_writer = data_writer.start_of_talkspurt(true);
        }
        let payload_len = frame.payload.len();
        data_writer.write(
            pt,
            frame.media_data.network_time,
            frame.media_data.time,
            frame.payload.take_write_payload(is_last_destination),
        )?;
        Ok(Some(payload_len))
    }

    fn send_rtp(
        &self,
        session_state: &mut RtcSessionState,
        rtp: &mut LocalForwardedRtp<'_>,
        is_last_destination: bool,
    ) -> Result<Option<usize>, RtcError> {
        if ACTIVE_PACKET_MODE != PacketMode::Rtp {
            return Ok(None);
        }
        let nackable = session_state
            .rtc
            .media(self.dest_mid)
            .is_some_and(|media| !media.kind().is_audio());
        let rid = rtp
            .header()
            .ext_vals
            .rid
            .or(rtp.header().ext_vals.rid_repair);
        let payload_len = rtp.payload.len();
        let write_result = {
            let mut direct_api = session_state.rtc.direct_api();
            if let Some(rid) = rid {
                if let Some(stream_tx) = direct_api.stream_tx_by_mid(self.dest_mid, Some(rid)) {
                    write_rtp(stream_tx, rtp, nackable, is_last_destination, self.dest_mid)
                } else if let Some(stream_tx) = direct_api.stream_tx_by_mid(self.dest_mid, None) {
                    write_rtp(stream_tx, rtp, nackable, is_last_destination, self.dest_mid)
                } else {
                    return Ok(None);
                }
            } else if let Some(stream_tx) = direct_api.stream_tx_by_mid(self.dest_mid, None) {
                write_rtp(stream_tx, rtp, nackable, is_last_destination, self.dest_mid)
            } else {
                return Ok(None);
            }
        };
        write_result?;
        Ok(Some(payload_len))
    }
}

fn write_rtp(
    stream_tx: &mut StreamTx,
    rtp: &mut LocalForwardedRtp<'_>,
    nackable: bool,
    is_last_destination: bool,
    dest_mid: Mid,
) -> Result<(), RtcError> {
    let payload_type = rtp.header().payload_type;
    stream_tx
        .write_rtp_with_csrc(
            rtp.header().payload_type,
            rtp.seq_no(),
            rtp.header().timestamp,
            rtp.timestamp(),
            rtp.header().marker,
            rtp.header().ext_vals.clone(),
            nackable,
            rtp.payload.take_write_payload(is_last_destination),
            rtp.header().csrc_count,
            rtp.header().csrc,
        )
        .map_err(|error| RtcError::Packet(dest_mid, payload_type, error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::rtc_adapter::sample_forwarded_packet;
    use crate::runtime::transport_adapter::TransportSessionKey;
    use crate::signaling::shared::SessionId;

    #[test]
    fn local_send_contract_keeps_payload_inside_the_adapter_boundary() -> Result<(), &'static str> {
        let session_key = TransportSessionKey::new(45, 0, 12, SessionId::Integer(9));
        let mut packet = sample_forwarded_packet(session_key, "aud-up", b"payload");

        let LocalForwardedPacket::Str0mFrame(frame) = packet.local_send_packet() else {
            return Err("sample_forwarded_packet should use the frame-backed test path");
        };

        assert_eq!(frame.media_data.mid, Mid::from("aud-up"));
        assert_eq!(frame.payload.as_slice(), b"payload");
        Ok(())
    }
}
