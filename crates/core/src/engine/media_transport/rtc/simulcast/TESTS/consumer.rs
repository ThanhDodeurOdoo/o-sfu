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
