//! Host-owned adapter around one `str0m::Rtc` session.
//!
//! Packet-loop state can decide which session operation is needed, but the
//! actual `str0m` calls stay here. This keeps polling, UDP input handling and
//! local RTP writes on the host side of the packet-loop boundary.

use std::{collections::HashMap, net::SocketAddr, time::Instant};

use o_sfu_rfc::rtp::vp8::{self, PayloadDescriptor, PayloadDescriptorRewrite};
use str0m::{
    Event, Input, Output, Rtc, RtcError,
    change::{DirectApi, SdpApi},
    media::{KeyframeRequest, Media, Mid, Pt},
    net::{DatagramSend, Protocol, Receive},
    rtp::{RtpPacket, StreamTx},
};
use tracing::debug;

use super::{
    local_forwarding::{LocalForwardedRtp, outbound_extension_values, vp8_payload_descriptor},
    local_send_rewrite::{
        ConsumerStream, ConsumerStreamKey, ProjectedIdentity, Vp8PayloadIdentity,
        forget_transport_media_streams, next_projected_rtp_identity,
    },
};
use crate::runtime::media_transport::TransportMediaId;

pub(super) struct RtcHostSession {
    rtc: Rtc,
    consumer_streams: HashMap<ConsumerStreamKey, ConsumerStream>,
}

#[derive(Debug)]
pub(super) enum HostSessionOutput {
    Transmit {
        destination: SocketAddr,
        contents: DatagramSend,
    },
    RtpPacket(RtpPacket),
    KeyframeRequest(KeyframeRequest),
    Event(Event),
    Timeout(Instant),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HostDatagramAccept {
    Accepted,
    Rejected,
    Malformed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HostDatagramHandle {
    Handled,
    Failed,
    Malformed,
}

#[derive(Clone, Copy)]
pub(super) struct HostDatagramInput<'a> {
    pub(super) source_addr: SocketAddr,
    pub(super) candidate_addr: SocketAddr,
    pub(super) packet: &'a [u8],
    pub(super) received_at: Instant,
}

impl RtcHostSession {
    pub(super) fn new(rtc: Rtc) -> Self {
        Self {
            rtc,
            consumer_streams: HashMap::new(),
        }
    }

    pub(super) fn poll_output(&mut self) -> Result<HostSessionOutput, RtcError> {
        match self.rtc.poll_output()? {
            Output::Transmit(transmit) => Ok(HostSessionOutput::Transmit {
                destination: transmit.destination,
                contents: transmit.contents,
            }),
            Output::Event(Event::RtpPacket(packet)) => Ok(HostSessionOutput::RtpPacket(packet)),
            Output::Event(Event::KeyframeRequest(request)) => {
                Ok(HostSessionOutput::KeyframeRequest(request))
            }
            Output::Event(event) => Ok(HostSessionOutput::Event(event)),
            Output::Timeout(timeout_at) => Ok(HostSessionOutput::Timeout(timeout_at)),
        }
    }

    pub(super) fn handle_timeout(&mut self, now: Instant) -> Result<(), RtcError> {
        self.rtc.handle_input(Input::Timeout(now))
    }

    pub(super) fn accepts_datagram(&self, datagram: HostDatagramInput<'_>) -> HostDatagramAccept {
        let Some(input) = datagram.receive_input() else {
            return HostDatagramAccept::Malformed;
        };
        if self.rtc.accepts(&input) {
            HostDatagramAccept::Accepted
        } else {
            HostDatagramAccept::Rejected
        }
    }

    pub(super) fn handle_datagram(
        &mut self,
        datagram: HostDatagramInput<'_>,
    ) -> HostDatagramHandle {
        let Some(input) = datagram.receive_input() else {
            return HostDatagramHandle::Malformed;
        };
        match self.rtc.handle_input(input) {
            Ok(()) => HostDatagramHandle::Handled,
            Err(_error) => HostDatagramHandle::Failed,
        }
    }

    pub(super) fn sdp_api(&mut self) -> SdpApi<'_> {
        self.rtc.sdp_api()
    }

    pub(super) fn direct_api(&mut self) -> DirectApi<'_> {
        self.rtc.direct_api()
    }

    pub(super) fn media(&self, mid: Mid) -> Option<&Media> {
        self.rtc.media(mid)
    }

    pub(super) fn forget_transport_media_streams(&mut self, transport_media_id: TransportMediaId) {
        forget_transport_media_streams(&mut self.consumer_streams, transport_media_id);
    }

    pub(super) fn write_local_rtp(
        &mut self,
        transport_media_id: TransportMediaId,
        mid: Mid,
        payload_type: Option<Pt>,
        packet: LocalForwardedRtp<'_>,
        is_last_destination: bool,
    ) -> Result<Option<usize>, RtcError> {
        let mut rtp = packet;
        let nackable = self
            .media(mid)
            .is_some_and(|media| !media.kind().is_audio());
        let payload_len = rtp.payload_len();
        let vp8_descriptor = vp8_payload_descriptor(&rtp);
        let vp8_payload = vp8_descriptor
            .map(|descriptor| Vp8PayloadIdentity {
                picture_id: descriptor.picture_id(),
                tl0_pic_idx: descriptor.tl0_pic_idx(),
            })
            .unwrap_or_default();
        let mut direct_api = self.rtc.direct_api();
        let Some(stream_tx) = direct_api.stream_tx_by_mid(mid, None) else {
            return Ok(None);
        };
        let identity = next_projected_rtp_identity(
            &mut self.consumer_streams,
            transport_media_id,
            rtp.header().ssrc,
            rtp.header().timestamp,
            vp8_payload,
        );
        if identity.source_switched() {
            debug!(
                ?transport_media_id,
                ?mid,
                previous_source_ssrc = ?identity.previous_source_ssrc(),
                source_ssrc = ?rtp.header().ssrc,
                source_rtp_timestamp = rtp.header().timestamp,
                outbound_rtp_timestamp = identity.rtp_timestamp(),
                source_vp8_picture_id = ?vp8_payload.picture_id,
                outbound_vp8_picture_id = ?identity.vp8_payload().picture_id,
                "projected consumer RTP identity after producer SSRC switch"
            );
        }
        let write_context = RtpWriteContext {
            identity,
            nackable,
            is_last_destination,
            mid,
            payload_type,
            vp8_descriptor,
        };
        write_rtp(stream_tx, &mut rtp, write_context)?;
        Ok(Some(payload_len))
    }
}

impl HostDatagramInput<'_> {
    fn receive_input(&self) -> Option<Input<'_>> {
        let receive = Receive::new(
            Protocol::Udp,
            self.source_addr,
            self.candidate_addr,
            self.packet,
        )
        .ok()?;
        Some(Input::Receive(self.received_at, receive))
    }
}

#[derive(Clone, Copy)]
struct RtpWriteContext {
    identity: ProjectedIdentity,
    nackable: bool,
    is_last_destination: bool,
    mid: Mid,
    payload_type: Option<Pt>,
    vp8_descriptor: Option<PayloadDescriptor>,
}

fn write_rtp(
    stream_tx: &mut StreamTx,
    rtp: &mut LocalForwardedRtp<'_>,
    context: RtpWriteContext,
) -> Result<(), RtcError> {
    let payload_type = context.payload_type.unwrap_or(rtp.header().payload_type);
    let ext_vals = outbound_extension_values(rtp.header(), context.mid);
    let mut payload = rtp.take_write_payload(context.is_last_destination);
    if let Some(descriptor) = context.vp8_descriptor {
        let vp8_payload = context.identity.vp8_payload();
        vp8::rewrite_payload_descriptor(
            &mut payload,
            descriptor,
            PayloadDescriptorRewrite {
                picture_id: vp8_payload.picture_id,
                tl0_pic_idx: vp8_payload.tl0_pic_idx,
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
