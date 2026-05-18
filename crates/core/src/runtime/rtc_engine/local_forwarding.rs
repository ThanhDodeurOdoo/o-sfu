//! Local RTP forwarding into a browser consumer `Rtc` session.
//!
//! The forwarding planner has already selected a packet and a destination when
//! this module runs. This file owns the last translation before str0m queues a
//! packet for the receiver: rewrite publisher metadata into the consumer MID,
//! strip publisher RID extensions and apply projected RTP identity.

use std::time::Instant;

use o_sfu_rfc::rtp::vp8;
use str0m::{
    RtcError,
    media::{ExtensionValues, Mid, Pt},
    rtp::{RtpHeader, RtpPacket, StreamTx},
};
use tracing::debug;

use super::{
    forwarded_packet::PacketVp8Payload,
    local_send_rewrite::{ProjectedIdentity, next_projected_rtp_identity},
    shared_payload::SharedPayload,
    state::RtcSessionState,
};
use crate::runtime::media_transport::TransportMediaId;

/// destination-local view of one routed RTP stream
///
/// `transport_media_id` keys the receiver-local rewrite state
/// `mid` and
/// `payload_type` are the consumer-negotiated values that must replace the
/// publisher-facing RTP identity before packets cross into str0m
/// `nackable` is cached route state, so local egress does not consult `str0m`
/// media metadata for every packet
#[derive(Debug, Clone)]
pub(super) struct LocalPacketDestination {
    transport_media_id: TransportMediaId,
    mid: Mid,
    payload_type: Option<Pt>,
    nackable: bool,
}

pub(super) struct LocalForwardedRtp<'a> {
    data: LocalForwardedRtpData<'a>,
    payload: &'a mut SharedPayload,
}

enum LocalForwardedRtpData<'a> {
    Str0m(&'a RtpPacket),
    Relay {
        header: &'a RtpHeader,
        timestamp: Instant,
    },
}

impl<'a> LocalForwardedRtp<'a> {
    pub(super) fn new(rtp_packet: &'a RtpPacket, payload: &'a mut SharedPayload) -> Self {
        Self {
            data: LocalForwardedRtpData::Str0m(rtp_packet),
            payload,
        }
    }

    pub(super) fn from_relay(
        header: &'a RtpHeader,
        timestamp: Instant,
        payload: &'a mut SharedPayload,
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
        mid: Mid,
        payload_type: Option<Pt>,
        nackable: bool,
    ) -> Self {
        Self {
            transport_media_id,
            mid,
            payload_type,
            nackable,
        }
    }

    pub(super) fn send(
        &self,
        session_state: &mut RtcSessionState,
        packet: LocalForwardedRtp<'_>,
        vp8_payload: Option<PacketVp8Payload>,
        is_last_destination: bool,
    ) -> Result<Option<usize>, RtcError> {
        let mut rtp = packet;
        self.send_rtp(session_state, &mut rtp, vp8_payload, is_last_destination)
    }

    /// writes one routed RTP packet into the destination user's local `StreamTx`
    ///
    /// payload bytes stay source-derived, but MID and RID extensions, sequence
    /// numbers and RTP timestamps are rewritten into the destination's single
    /// consumer stream
    /// a missing destination stream is treated as a no-op because route updates
    /// and negotiated removal can race with packets already buffered for
    /// forwarding
    fn send_rtp(
        &self,
        session_state: &mut RtcSessionState,
        rtp: &mut LocalForwardedRtp<'_>,
        vp8_payload: Option<PacketVp8Payload>,
        is_last_destination: bool,
    ) -> Result<Option<usize>, RtcError> {
        let payload_len = rtp.payload.len();
        let vp8_payload = vp8_payload.or_else(|| vp8_payload_from_rtp(rtp));
        let vp8_identity = vp8_payload
            .map(|vp8_payload| vp8_payload.identity)
            .unwrap_or_default();
        let local_send_streams = &mut session_state.consumer_streams;
        let rtc = &mut session_state.rtc;
        let write_result = {
            let mut direct_api = rtc.direct_api();
            if let Some(stream_tx) = direct_api.stream_tx_by_mid(self.mid, None) {
                let identity = next_projected_rtp_identity(
                    local_send_streams,
                    self.transport_media_id,
                    rtp.header().ssrc,
                    rtp.header().timestamp,
                    vp8_identity,
                );
                if identity.source_switched() {
                    debug!(
                        ?self.transport_media_id,
                        mid = ?self.mid,
                        previous_source_ssrc = ?identity.previous_source_ssrc(),
                        source_ssrc = ?rtp.header().ssrc,
                        source_rtp_timestamp = rtp.header().timestamp,
                        outbound_rtp_timestamp = identity.rtp_timestamp(),
                        source_vp8_picture_id = ?vp8_identity.picture_id,
                        outbound_vp8_picture_id = ?identity.vp8_payload().picture_id,
                        "projected consumer RTP identity after producer SSRC switch"
                    );
                }
                let write_context = RtpWriteContext {
                    identity,
                    nackable: self.nackable,
                    is_last_destination,
                    mid: self.mid,
                    payload_type: self.payload_type,
                    vp8_payload,
                };
                write_rtp(stream_tx, rtp, write_context)
            } else {
                return Ok(None);
            }
        };
        write_result?;
        Ok(Some(payload_len))
    }
}

#[derive(Clone, Copy)]
struct RtpWriteContext {
    identity: ProjectedIdentity,
    nackable: bool,
    is_last_destination: bool,
    mid: Mid,
    payload_type: Option<Pt>,
    vp8_payload: Option<PacketVp8Payload>,
}

/// Push one packet into str0m with a receiver-safe RTP identity.
///
/// Str0m owns the actual send queue, but the adapter decides the per-consumer
/// sequence, timestamp and VP8 payload descriptor space immediately before the
/// packet crosses that boundary.
fn write_rtp(
    stream_tx: &mut StreamTx,
    rtp: &mut LocalForwardedRtp<'_>,
    context: RtpWriteContext,
) -> Result<(), RtcError> {
    let payload_type = outbound_payload_type(rtp.header(), context.payload_type);
    let ext_vals = outbound_extension_values(rtp.header(), context.mid);
    let mut payload = rtp.payload.take_write_payload(context.is_last_destination);
    if let Some(packet_vp8_payload) = context.vp8_payload {
        let identity_vp8_payload = context.identity.vp8_payload();
        vp8::rewrite_payload_descriptor(
            &mut payload,
            packet_vp8_payload.descriptor,
            vp8::PayloadDescriptorRewrite {
                picture_id: identity_vp8_payload.picture_id,
                tl0_pic_idx: identity_vp8_payload.tl0_pic_idx,
            },
        );
    }
    stream_tx
        .write_rtp_with_csrc(
            payload_type,
            context.identity.seq_no(),
            context.identity.rtp_timestamp(),
            rtp.timestamp(),
            rtp.header().marker,
            ext_vals,
            context.nackable,
            payload,
            rtp.header().csrc_count,
            rtp.header().csrc,
        )
        .map_err(|error| RtcError::Packet(context.mid, payload_type, error))
}

fn vp8_payload_from_rtp(rtp: &LocalForwardedRtp<'_>) -> Option<PacketVp8Payload> {
    rtp.header()
        .ext_vals
        .rid
        .or(rtp.header().ext_vals.rid_repair)?;
    vp8::payload_descriptor(rtp.payload.as_slice()).map(PacketVp8Payload::new)
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
mod tests {
    use str0m::media::Rid;

    use super::*;
    use crate::runtime::{
        UserId,
        rtc_engine::test_support::{sample_forwarded_packet, test_transport_session_key},
    };

    #[test]
    fn local_send_contract_keeps_payload_inside_the_adapter_boundary() {
        let session_key = test_transport_session_key(45, 0, 12, UserId::Integer(9));
        let mut packet = sample_forwarded_packet(session_key, "aud-up", b"payload");
        let rtp = packet.local_send_packet();

        assert_eq!(rtp.header().ext_vals.mid, Some(Mid::from("aud-up")));
        assert_eq!(rtp.payload.as_slice(), b"payload");
    }

    #[test]
    fn outbound_extension_values_rewrite_source_identity_to_consumer_stream() {
        let session_key = test_transport_session_key(45, 0, 12, UserId::Integer(9));
        let mut packet = sample_forwarded_packet(session_key, "cam-up", b"payload");
        let rtp = packet.local_send_packet();
        let mut source_header = rtp.header().clone();
        source_header.ext_vals.rid = Some(Rid::from("hi"));
        source_header.ext_vals.rid_repair = Some(Rid::from("lo"));
        source_header.ext_vals.voice_activity = Some(true);

        let ext_vals = outbound_extension_values(&source_header, Mid::from("cam-down"));

        assert_eq!(ext_vals.mid, Some(Mid::from("cam-down")));
        assert_eq!(ext_vals.rid, None);
        assert_eq!(ext_vals.rid_repair, None);
        assert_eq!(ext_vals.voice_activity, Some(true));
    }

    #[test]
    fn outbound_payload_type_prefers_consumer_negotiated_payload_type() {
        let session_key = test_transport_session_key(45, 0, 12, UserId::Integer(9));
        let mut packet = sample_forwarded_packet(session_key, "cam-up", b"payload");
        let rtp = packet.local_send_packet();

        assert_eq!(
            outbound_payload_type(rtp.header(), Some(Pt::from(96))),
            Pt::from(96)
        );
        assert_eq!(outbound_payload_type(rtp.header(), None), Pt::from(111));
    }
}
