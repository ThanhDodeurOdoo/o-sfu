//! Local RTP forwarding into a browser consumer `Rtc` session.
//!
//! The forwarding planner has already selected a packet and a destination when
//! this module runs. This file owns the last translation before str0m queues a
//! packet for the receiver: rewrite publisher metadata into the consumer MID,
//! strip publisher RID extensions and apply projected RTP identity.

use std::{sync::Arc, time::Instant};

use str0m::{
    media::{ExtensionValues, Mid, Pt},
    rtp::{RtpHeader, RtpPacket, RtpWrite, StreamTx},
};
use tracing::debug;

use super::{
    forwarded_packet::PacketVp8Payload,
    local_send_rewrite::{ProjectedIdentity, next_projected_rtp_identity},
    slots::ConsumerStreamHandle,
    state::RtcSessionState,
};
use crate::engine::media_transport::TransportMediaId;

/// destination-local view of one routed RTP stream
///
/// `transport_media_id` identifies the consumer media line for logs and diagnostics
/// `stream_handle` keys the receiver-local rewrite state inside the destination
/// session
/// `mid` and
/// `payload_type` are the consumer-negotiated values that must replace the
/// publisher-facing RTP identity before packets cross into str0m
/// `nackable` is cached route state, so local egress does not consult `str0m`
/// media metadata for every packet
#[derive(Debug, Clone)]
pub(super) struct LocalPacketDestination {
    transport_media_id: TransportMediaId,
    stream_handle: ConsumerStreamHandle,
    mid: Mid,
    payload_type: Option<Pt>,
    nackable: bool,
}

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

    /// writes one routed RTP packet into the destination user's local `StreamTx`
    ///
    /// payload bytes stay source-derived, but MID and RID extensions, sequence
    /// numbers and RTP timestamps are rewritten into the destination's single
    /// consumer stream
    /// a missing destination stream is treated as a no-op because route updates
    /// and negotiated removal can race with packets already buffered for
    /// forwarding
    pub(super) fn send(
        &self,
        session_state: &mut RtcSessionState,
        packet: LocalForwardedRtp<'_>,
        vp8_payload: Option<PacketVp8Payload>,
    ) -> Option<usize> {
        let rtp = packet;
        let payload_len = rtp.payload.len();
        let vp8_identity = vp8_payload
            .map(|vp8_payload| vp8_payload.identity)
            .unwrap_or_default();
        let local_send_streams = &mut session_state.consumer_streams;
        let rtc = &mut session_state.rtc;
        {
            let mut direct_api = rtc.direct_api();
            let stream_tx = direct_api.stream_tx_by_mid(self.mid, None)?;
            let identity = next_projected_rtp_identity(
                local_send_streams,
                self.stream_handle,
                rtp.header().ssrc,
                rtp.header().timestamp,
                vp8_identity,
            )?;
            if identity.source_switched {
                debug!(
                    ?self.transport_media_id,
                    mid = ?self.mid,
                    previous_source_ssrc = ?identity.previous_source_ssrc,
                    source_ssrc = ?rtp.header().ssrc,
                    source_rtp_timestamp = rtp.header().timestamp,
                    outbound_rtp_timestamp = identity.rtp_timestamp,
                    source_vp8_picture_id = ?vp8_identity.picture_id,
                    outbound_vp8_picture_id = ?identity.vp8_payload.picture_id,
                    "projected consumer RTP identity after producer SSRC switch"
                );
            }
            let write_context = RtpWriteContext {
                identity,
                nackable: self.nackable,
                mid: self.mid,
                payload_type: self.payload_type,
                vp8_payload,
            };
            write_rtp(stream_tx, &rtp, write_context);
        }
        Some(payload_len)
    }
}

#[derive(Clone, Copy)]
struct RtpWriteContext {
    identity: ProjectedIdentity,
    nackable: bool,
    mid: Mid,
    payload_type: Option<Pt>,
    vp8_payload: Option<PacketVp8Payload>,
}

/// Push one packet into str0m with a receiver-safe RTP identity.
///
/// Str0m owns the actual send queue, but the adapter decides the per-consumer
/// sequence, timestamp and VP8 payload descriptor space immediately before the
/// packet crosses that boundary.
fn write_rtp(stream_tx: &mut StreamTx, rtp: &LocalForwardedRtp<'_>, context: RtpWriteContext) {
    let header = rtp.header();
    let payload_type = outbound_payload_type(header, context.payload_type);
    let ext_vals = outbound_extension_values(header, context.mid);
    let payload = Arc::clone(rtp.payload);
    let mut write = RtpWrite::new(
        payload_type,
        context.identity.seq_no,
        context.identity.rtp_timestamp,
        rtp.timestamp(),
        payload,
    )
    .marker(header.marker)
    .ext_vals(ext_vals)
    .nackable(context.nackable);
    if let Some(csrc) = header.csrc.get(..header.csrc_count) {
        write = write.csrc(csrc);
    }
    if let Some(patch) = context
        .vp8_payload
        .and_then(|packet| packet.patch(context.identity.vp8_payload))
    {
        write = write.vp8_patch(patch);
    }
    stream_tx.write_rtp(write);
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
