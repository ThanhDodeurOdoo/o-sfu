//! local RTP egress into a browser consumer `Rtc` session
//!
//! forwarding has already resolved the packet and destination route
//! this boundary rewrites receiver-facing RTP identity, then hands the packet
//! to `str0m` without copying payload bytes

use std::{sync::Arc, time::Instant};

use str0m::{
    media::{ExtensionValues, Mid, Pt},
    rtp::{RtpHeader, RtpPacket, RtpWrite, StreamTx},
};
use tracing::debug;

use super::{
    forwarded_packet::PacketVp8Payload, local_send_rewrite::ProjectedIdentity,
    slots::ConsumerStreamHandle, state::RtcSessionState,
};
use crate::engine::media_transport::TransportMediaId;

/// receiver route snapshot used for one local RTP write
///
/// values come from `MediaRouteDestination` after route lookup in the flush turn
#[derive(Debug)]
pub(super) struct LocalPacketDestination {
    transport_media_id: TransportMediaId,
    stream_handle: ConsumerStreamHandle,
    mid: Mid,
    payload_type: Option<Pt>,
    nackable: bool,
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
        timestamp: Instant,
        payload: &'a Arc<[u8]>,
    ) -> Self {
        Self {
            data: LocalForwardedRtpData::Relay { header, timestamp },
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
}

impl LocalPacketDestination {
    pub(super) fn new(
        transport_media_id: TransportMediaId,
        stream_handle: ConsumerStreamHandle,
        mid: Mid,
        payload_type: Option<Pt>,
        nackable: bool,
    ) -> Self {
        Self {
            transport_media_id,
            stream_handle,
            mid,
            payload_type,
            nackable,
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
        vp8_payload: Option<PacketVp8Payload>,
    ) -> Option<usize> {
        let payload_len = rtp.payload.len();
        let vp8_identity = vp8_payload
            .map(|vp8_payload| vp8_payload.identity)
            .unwrap_or_default();
        let local_send_streams = &mut session_state.consumer_streams;
        let rtc = &mut session_state.rtc;
        {
            let mut direct_api = rtc.direct_api();
            let stream_tx = direct_api.stream_tx_by_mid(self.mid, None)?;
            let header = rtp.header();
            let identity = local_send_streams.project_identity(
                self.stream_handle,
                header.ssrc,
                header.timestamp,
                vp8_identity,
            )?;
            if identity.source_switched {
                debug!(
                    ?self.transport_media_id,
                    mid = ?self.mid,
                    previous_source_ssrc = ?identity.previous_source_ssrc,
                    source_ssrc = ?header.ssrc,
                    source_rtp_timestamp = header.timestamp,
                    outbound_rtp_timestamp = identity.rtp_timestamp,
                    source_vp8_picture_id = ?vp8_identity.picture_id,
                    outbound_vp8_picture_id = ?identity.vp8_payload.picture_id,
                    "projected consumer RTP identity after producer SSRC switch"
                );
            }
            self.write_rtp(stream_tx, rtp, identity, vp8_payload);
        }
        Some(payload_len)
    }

    fn write_rtp(
        &self,
        stream_tx: &mut StreamTx,
        rtp: &LocalForwardedRtp<'_>,
        identity: ProjectedIdentity,
        vp8_payload: Option<PacketVp8Payload>,
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
        .ext_vals(ext_vals)
        .nackable(self.nackable);
        if let Some(csrc) = header.csrc.get(..header.csrc_count) {
            write = write.csrc(csrc);
        }
        if let Some(patch) = vp8_payload.and_then(|packet| packet.patch(identity.vp8_payload)) {
            write = write.vp8_patch(patch);
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
