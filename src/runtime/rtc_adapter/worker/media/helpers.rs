use o_sfu_router::RtpParameters as RouterRtpParameters;
use str0m::media::{Mid, Rid};
use str0m::rtp::Ssrc;

use crate::runtime::transport_adapter::TransportMediaId;

use super::super::super::{
    commands::RelayCleanup, route_control::PacketLayerGate, state::RtcBootstrapState,
};

pub(super) fn relay_cleanup_for_source(
    state: &RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
) -> Option<RelayCleanup> {
    state
        .remote_source_registration(source_transport_media_id)
        .map(|registration| {
            RelayCleanup::new(
                registration.source_session_key().clone(),
                source_transport_media_id,
            )
        })
}

pub(super) fn consumer_packet_gate(
    consumer_rtp_parameters: &RouterRtpParameters,
) -> PacketLayerGate {
    let mut selected_rid: Option<Rid> = None;
    for encoding in consumer_rtp_parameters.encodings() {
        let Some(rid) = encoding.rid().map(Rid::from) else {
            return PacketLayerGate::Open;
        };
        if let Some(current_rid) = selected_rid.as_ref() {
            if current_rid != &rid {
                return PacketLayerGate::Open;
            }
        } else {
            selected_rid = Some(rid);
        }
    }
    selected_rid.map_or(PacketLayerGate::Open, PacketLayerGate::Rid)
}

pub(super) fn transport_mid(rtp_parameters: &RouterRtpParameters) -> Option<Mid> {
    rtp_parameters.mid().map(Into::into)
}

pub(super) fn primary_encoding_identity(
    rtp_parameters: &RouterRtpParameters,
) -> Option<(Ssrc, Option<Rid>)> {
    let encoding = rtp_parameters
        .encodings()
        .find(|encoding| encoding.ssrc().is_some() || encoding.rid().is_some())?;
    let ssrc = encoding.ssrc().map(Ssrc::from)?;
    let rid = encoding.rid().map(Into::into);
    Some((ssrc, rid))
}
