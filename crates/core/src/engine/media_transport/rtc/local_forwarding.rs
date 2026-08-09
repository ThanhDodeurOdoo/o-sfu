//! local RTP egress into a browser consumer `Rtc` session
//!
//! forwarding has already resolved the packet and destination route
//! this boundary rewrites receiver-facing RTP identity, then hands the packet
//! to `str0m` without copying payload bytes

use std::{sync::Arc, time::Instant};

use str0m::{
    media::{ExtensionValues, Mid, Pt},
    rtp::{RtpHeader, RtpPacket, RtpWrite, SeqNo, StreamTx},
};
use tracing::debug;

use super::{
    codec,
    local_send_rewrite::{ProjectedIdentity, SourceTransition},
    slots::ConsumerStreamHandle,
    state::RtcSessionState,
};
use crate::engine::media_transport::TransportMediaId;

/// receiver route snapshot used for one local RTP write
///
/// values come from `MediaRouteDestination` after route lookup in the flush turn
#[derive(Debug)]
pub(super) struct LocalPacketDestination {
    transport_media_id: TransportMediaId,
    stream_handle: ConsumerStreamHandle,
    delivery_generation: u64,
    mid: Mid,
    payload_type: Option<Pt>,
}

/// borrowed RTP metadata plus shared payload bytes for local egress
pub(super) struct LocalForwardedRtp<'a> {
    data: LocalForwardedRtpData<'a>,
    payload: &'a Arc<[u8]>,
}

enum LocalForwardedRtpData<'a> {
    Str0m(&'a RtpPacket),
    Relay {
        header: &'a RtpHeader,
        sequence_number: SeqNo,
        timestamp: Instant,
    },
}

impl<'a> LocalForwardedRtp<'a> {
    pub(super) fn new(rtp_packet: &'a RtpPacket, payload: &'a Arc<[u8]>) -> Self {
        Self {
            data: LocalForwardedRtpData::Str0m(rtp_packet),
            payload,
        }
    }

    pub(super) fn from_relay(
        header: &'a RtpHeader,
        sequence_number: SeqNo,
        timestamp: Instant,
        payload: &'a Arc<[u8]>,
    ) -> Self {
        Self {
            data: LocalForwardedRtpData::Relay {
                header,
                sequence_number,
                timestamp,
            },
            payload,
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

    fn sequence_number(&self) -> SeqNo {
        match self.data {
            LocalForwardedRtpData::Str0m(packet) => packet.seq_no,
            LocalForwardedRtpData::Relay {
                sequence_number, ..
            } => sequence_number,
        }
    }
}

impl LocalPacketDestination {
    pub(super) fn new(
        transport_media_id: TransportMediaId,
        stream_handle: ConsumerStreamHandle,
        delivery_generation: u64,
        mid: Mid,
        payload_type: Option<Pt>,
    ) -> Self {
        Self {
            transport_media_id,
            stream_handle,
            delivery_generation,
            mid,
            payload_type,
        }
    }

    /// queues one packet in the destination `Rtc`
    ///
    /// returns `None` when no destination [`StreamTx`] exists or the stream was released
    /// successful sends clone only the `Arc<[u8]>` payload handle
    pub(super) fn send(
        &self,
        session_state: &mut RtcSessionState,
        rtp: &LocalForwardedRtp<'_>,
        codec_packet: Option<&codec::Packet>,
    ) -> Option<usize> {
        let payload_len = rtp.payload.len();
        let codec_identity =
            codec_packet.map_or_else(codec::PacketIdentity::default, codec::Packet::identity);
        let local_send_streams = &mut session_state.consumer_streams;
        let rtc = &mut session_state.rtc;
        {
            let mut direct_api = rtc.direct_api();
            let stream_tx = direct_api.stream_tx_by_mid(self.mid, None)?;
            let header = rtp.header();
            let identity = local_send_streams.project_identity(
                self.stream_handle,
                self.delivery_generation,
                header.ssrc,
                rtp.sequence_number(),
                header.timestamp,
                codec_identity,
            )?;
            if let SourceTransition::Switched { previous_ssrc } = identity.transition {
                debug!(
                    ?self.transport_media_id,
                    mid = ?self.mid,
                    previous_source_ssrc = ?previous_ssrc,
                    source_ssrc = ?header.ssrc,
                    source_rtp_timestamp = header.timestamp,
                    outbound_rtp_timestamp = identity.rtp_timestamp,
                    source_codec_identity = ?codec_identity,
                    outbound_codec_identity = ?identity.codec,
                    "projected consumer RTP identity after producer SSRC switch"
                );
            }
            self.write_rtp(stream_tx, rtp, identity, codec_packet);
        }
        Some(payload_len)
    }

    fn write_rtp(
        &self,
        stream_tx: &mut StreamTx,
        rtp: &LocalForwardedRtp<'_>,
        identity: ProjectedIdentity,
        codec_packet: Option<&codec::Packet>,
    ) {
        let header = rtp.header();
        let payload_type = outbound_payload_type(header, self.payload_type);
        let ext_vals = outbound_extension_values(header, self.mid);
        let payload = Arc::clone(rtp.payload);
        let mut write = RtpWrite::new(
            payload_type,
            identity.seq_no,
            identity.rtp_timestamp,
            rtp.timestamp(),
            payload,
        )
        .marker(header.marker)
        .ext_vals(ext_vals);
        if let Some(csrc) = header.csrc.get(..header.csrc_count) {
            write = write.csrc(csrc);
        }
        if let Some(rewrite) = codec_packet.and_then(|packet| packet.rewrite(identity.codec)) {
            write = rewrite.apply(write);
        }
        stream_tx.write_rtp(write);
    }
}

fn outbound_payload_type(header: &RtpHeader, payload_type: Option<Pt>) -> Pt {
    payload_type.unwrap_or(header.payload_type)
}

fn outbound_extension_values(header: &RtpHeader, mid: Mid) -> ExtensionValues {
    let mut ext_vals = header.ext_vals.clone();
    ext_vals.mid = Some(mid);
    ext_vals.rid = None;
    ext_vals.rid_repair = None;
    ext_vals
}

#[cfg(test)]
#[path = "TESTS/local_forwarding.rs"]
mod tests;
