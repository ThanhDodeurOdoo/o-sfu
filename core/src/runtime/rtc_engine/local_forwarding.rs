//! Local RTP forwarding into a browser consumer host session.
//!
//! The forwarding planner has already selected a packet and a destination when
//! this module runs. This file owns the last packet translation before the host
//! session adapter queues RTP for the receiver: rewrite publisher metadata into
//! the consumer MID, strip publisher RID extensions and apply projected RTP
//! identity.

use std::time::Instant;

use o_sfu_rfc::rtp::vp8;
use str0m::{
    RtcError,
    media::{ExtensionValues, Mid, Pt},
    rtp::{RtpHeader, RtpPacket},
};

use super::{shared_payload::SharedPayload, state::RtcSessionState};
use crate::runtime::media_transport::TransportMediaId;

/// Destination-local view of one routed RTP stream.
///
/// `transport_media_id` keys the receiver-local rewrite state. `mid` and
/// `payload_type` are the consumer-negotiated values that must replace the
/// publisher-facing RTP identity before packets cross into str0m.
#[derive(Debug, Clone)]
pub(super) struct LocalPacketDestination {
    transport_media_id: TransportMediaId,
    mid: Mid,
    payload_type: Option<Pt>,
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

    pub(super) fn header(&self) -> &RtpHeader {
        match self.data {
            LocalForwardedRtpData::Str0m(packet) => &packet.header,
            LocalForwardedRtpData::Relay { header, .. } => header,
        }
    }

    pub(super) fn timestamp(&self) -> Instant {
        match self.data {
            LocalForwardedRtpData::Str0m(packet) => packet.timestamp,
            LocalForwardedRtpData::Relay { timestamp, .. } => timestamp,
        }
    }

    pub(super) fn payload_len(&self) -> usize {
        self.payload.len()
    }

    pub(super) fn payload(&self) -> &[u8] {
        self.payload.as_slice()
    }

    pub(super) fn take_write_payload(&mut self, is_last_destination: bool) -> Vec<u8> {
        self.payload.take_write_payload(is_last_destination)
    }
}

impl LocalPacketDestination {
    pub(super) fn new(
        transport_media_id: TransportMediaId,
        mid: Mid,
        payload_type: Option<Pt>,
    ) -> Self {
        Self {
            transport_media_id,
            mid,
            payload_type,
        }
    }

    pub(super) fn send(
        &self,
        session_state: &mut RtcSessionState,
        packet: LocalForwardedRtp<'_>,
        is_last_destination: bool,
    ) -> Result<Option<usize>, RtcError> {
        session_state.host_session.write_local_rtp(
            self.transport_media_id,
            self.mid,
            self.payload_type,
            packet,
            is_last_destination,
        )
    }
}

pub(super) fn vp8_payload_descriptor(
    rtp: &LocalForwardedRtp<'_>,
) -> Option<vp8::PayloadDescriptor> {
    rtp.header()
        .ext_vals
        .rid
        .or(rtp.header().ext_vals.rid_repair)?;
    vp8::payload_descriptor(rtp.payload())
}

pub(super) fn outbound_extension_values(header: &RtpHeader, mid: Mid) -> ExtensionValues {
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
}
