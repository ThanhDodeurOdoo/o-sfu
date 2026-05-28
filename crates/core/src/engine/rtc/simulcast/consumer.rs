use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::media::Rid;

use crate::{Bitrate, engine::rtc::route_control::PacketLayerGate};

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
mod tests {
    use o_sfu_router::{MediaStream as RouterRtpParameters, StreamBinding};

    use super::*;

    #[test]
    fn initial_packet_gate_selects_lowest_bitrate_rid() {
        let parameters = RouterRtpParameters::new(
            vec![],
            vec![],
            vec![
                StreamBinding::new()
                    .with_rid("hi")
                    .with_max_bitrate(4_000_000),
                StreamBinding::new()
                    .with_rid("lo")
                    .with_max_bitrate(150_000),
            ],
        );

        assert_eq!(
            initial_packet_gate(&parameters),
            PacketLayerGate::Rid("lo".into())
        );
    }

    #[test]
    fn initial_packet_gate_uses_declared_order_for_rid_only_encodings() {
        let parameters = RouterRtpParameters::new(
            vec![],
            vec![],
            vec![
                StreamBinding::new().with_rid("lo"),
                StreamBinding::new().with_rid("hi"),
            ],
        );

        assert_eq!(
            initial_packet_gate(&parameters),
            PacketLayerGate::Rid("lo".into())
        );
    }

    #[test]
    fn initial_packet_gate_keeps_ridless_or_mixed_routes_open() {
        let ridless = RouterRtpParameters::new(vec![], vec![], vec![StreamBinding::new()]);
        let mixed = RouterRtpParameters::new(
            vec![],
            vec![],
            vec![
                StreamBinding::new().with_rid("lo"),
                StreamBinding::new().with_ssrc(72_002),
            ],
        );

        assert_eq!(initial_packet_gate(&ridless), PacketLayerGate::Open);
        assert_eq!(initial_packet_gate(&mixed), PacketLayerGate::Open);
    }
}
