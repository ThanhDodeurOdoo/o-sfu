use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::media::Rid;

use crate::{Bitrate, engine::media_transport::rtc::route_control::PacketLayerGate};

pub(super) fn initial_packet_gate(
    consumer_rtp_parameters: &RouterRtpParameters,
) -> PacketLayerGate {
    let mut first_rid = None;
    let mut lowest_bitrate_rid = None;
    let mut all_encodings_have_bitrate = true;
    for encoding in consumer_rtp_parameters.bindings() {
        let Some(rid) = encoding.rid().map(Rid::from) else {
            return PacketLayerGate::Open;
        };
        if first_rid.is_none() {
            first_rid = Some(rid);
        }
        let bitrate = encoding.max_bitrate().map(Bitrate::from_bps);
        all_encodings_have_bitrate &= bitrate.is_some();
        if let Some(bitrate) = bitrate {
            match lowest_bitrate_rid.as_mut() {
                Some((selected_rid, selected_bitrate)) if bitrate < *selected_bitrate => {
                    *selected_rid = rid;
                    *selected_bitrate = bitrate;
                }
                Some(_) => {}
                None => lowest_bitrate_rid = Some((rid, bitrate)),
            }
        }
    }
    if all_encodings_have_bitrate && let Some((rid, _bitrate)) = lowest_bitrate_rid {
        return PacketLayerGate::Rid(rid);
    }
    first_rid.map_or(PacketLayerGate::Open, PacketLayerGate::Rid)
}

#[cfg(test)]
#[path = "TESTS/consumer.rs"]
mod tests;
