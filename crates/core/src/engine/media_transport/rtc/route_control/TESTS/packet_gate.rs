use super::{
    PacketLayerGate, PacketLayerMetadata, PacketOperatingPointGate, aggregate_packet_gates,
};

#[test]
fn packet_gate_only_forwards_the_selected_rid() {
    let gate = PacketLayerGate::Rid("hi".into());

    assert!(gate.permits(PacketLayerMetadata::new(Some("hi".into()), None)));
    assert!(!gate.permits(PacketLayerMetadata::new(Some("lo".into()), None)));
    assert!(!gate.permits(PacketLayerMetadata::default()));
}

#[test]
fn packet_gate_forwards_only_the_selected_operating_point() {
    let gate = PacketLayerGate::OperatingPoint(PacketOperatingPointGate::new(Some("hi".into()), 1));

    assert!(gate.permits(PacketLayerMetadata::new(Some("hi".into()), Some(1))));
    assert!(!gate.permits(PacketLayerMetadata::new(Some("hi".into()), Some(2))));
    assert!(!gate.permits(PacketLayerMetadata::new(Some("lo".into()), Some(1))));
    assert!(!gate.permits(PacketLayerMetadata::new(Some("hi".into()), None)));
}

#[test]
fn aggregate_packet_gates_prefers_a_shared_selected_rid() {
    assert_eq!(
        aggregate_packet_gates([
            &PacketLayerGate::Rid("hi".into()),
            &PacketLayerGate::Rid("hi".into()),
            &PacketLayerGate::Block,
        ]),
        Some(PacketLayerGate::Rid("hi".into()))
    );
}

#[test]
fn aggregate_packet_gates_reopens_when_routes_disagree() {
    assert_eq!(
        aggregate_packet_gates([
            &PacketLayerGate::Rid("hi".into()),
            &PacketLayerGate::Rid("lo".into()),
        ]),
        Some(PacketLayerGate::Open)
    );
    assert_eq!(
        aggregate_packet_gates([&PacketLayerGate::Rid("hi".into()), &PacketLayerGate::Open]),
        Some(PacketLayerGate::Open)
    );
}

#[test]
fn aggregate_packet_gates_widens_shared_operating_points() {
    assert_eq!(
        aggregate_packet_gates([
            &PacketLayerGate::OperatingPoint(PacketOperatingPointGate::new(Some("hi".into()), 0,)),
            &PacketLayerGate::OperatingPoint(PacketOperatingPointGate::new(Some("hi".into()), 2,)),
        ]),
        Some(PacketLayerGate::OperatingPoint(
            PacketOperatingPointGate::new(Some("hi".into()), 2)
        ))
    );
    assert_eq!(
        aggregate_packet_gates([
            &PacketLayerGate::OperatingPoint(PacketOperatingPointGate::new(Some("hi".into()), 1,)),
            &PacketLayerGate::Rid("hi".into()),
        ]),
        Some(PacketLayerGate::Rid("hi".into()))
    );
}
