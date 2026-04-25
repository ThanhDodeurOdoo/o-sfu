//! Packet-facing layer gates for routed RTP.
//!
//! These gates are transport-native. They describe whether a packet should pass
//! based on metadata already extracted by the RTC edge, not why a room policy
//! selected that layer. Room layout, bandwidth, and source identity stay above
//! this boundary.

use str0m::media::Rid;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(in crate::runtime::rtc_adapter) enum PacketLayerGate {
    #[default]
    Open,
    Block,
    Rid(Rid),
    OperatingPoint(PacketOperatingPointGate),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::rtc_adapter) struct PacketOperatingPointGate {
    rid: Option<Rid>,
    max_temporal_layer_id: u8,
}

impl PacketOperatingPointGate {
    pub(in crate::runtime::rtc_adapter) const fn new(
        rid: Option<Rid>,
        max_temporal_layer_id: u8,
    ) -> Self {
        Self {
            rid,
            max_temporal_layer_id,
        }
    }

    pub(in crate::runtime::rtc_adapter) const fn rid(self) -> Option<Rid> {
        self.rid
    }

    pub(in crate::runtime::rtc_adapter) const fn max_temporal_layer_id(self) -> u8 {
        self.max_temporal_layer_id
    }

    const fn with_rid(self, rid: Rid) -> Self {
        Self {
            rid: Some(rid),
            max_temporal_layer_id: self.max_temporal_layer_id,
        }
    }

    fn permits(self, metadata: PacketLayerMetadata) -> bool {
        if let Some(selected_rid) = self.rid
            && metadata.rid() != Some(selected_rid)
        {
            return false;
        }
        metadata
            .temporal_layer_id()
            .is_some_and(|layer_id| layer_id <= self.max_temporal_layer_id)
    }
}

impl PacketLayerGate {
    pub(in crate::runtime::rtc_adapter) fn permits(&self, metadata: PacketLayerMetadata) -> bool {
        match self {
            Self::Open => true,
            Self::Block => false,
            Self::Rid(selected_rid) => metadata.rid() == Some(*selected_rid),
            Self::OperatingPoint(operating_point) => operating_point.permits(metadata),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(in crate::runtime::rtc_adapter) struct PacketLayerMetadata {
    rid: Option<Rid>,
    temporal_layer_id: Option<u8>,
}

impl PacketLayerMetadata {
    pub(in crate::runtime::rtc_adapter) const fn new(
        rid: Option<Rid>,
        temporal_layer_id: Option<u8>,
    ) -> Self {
        Self {
            rid,
            temporal_layer_id,
        }
    }

    const fn rid(self) -> Option<Rid> {
        self.rid
    }

    const fn temporal_layer_id(self) -> Option<u8> {
        self.temporal_layer_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::rtc_adapter) enum PacketRouteDecision {
    Forward,
    Drop,
}

pub(in crate::runtime::rtc_adapter) fn aggregate_packet_gates<'a>(
    packet_gates: impl IntoIterator<Item = &'a PacketLayerGate>,
) -> Option<PacketLayerGate> {
    let mut aggregate = None;
    for packet_gate in packet_gates {
        aggregate = Some(aggregate.map_or_else(
            || packet_gate.clone(),
            |current| union_packet_gates(current, packet_gate.clone()),
        ));
        if matches!(aggregate.as_ref(), Some(PacketLayerGate::Open)) {
            return aggregate;
        }
    }
    aggregate
}

pub(super) fn intersect_packet_gates(
    first: Option<PacketLayerGate>,
    second: Option<PacketLayerGate>,
) -> Option<PacketLayerGate> {
    match (first, second) {
        (None, None) => None,
        (Some(gate), None) | (None, Some(gate)) => Some(gate),
        (Some(PacketLayerGate::Block), _) | (_, Some(PacketLayerGate::Block)) => {
            Some(PacketLayerGate::Block)
        }
        (Some(PacketLayerGate::Open), Some(gate)) | (Some(gate), Some(PacketLayerGate::Open)) => {
            Some(gate)
        }
        (Some(PacketLayerGate::Rid(first_rid)), Some(PacketLayerGate::Rid(second_rid))) => {
            if first_rid == second_rid {
                Some(PacketLayerGate::Rid(first_rid))
            } else {
                Some(PacketLayerGate::Block)
            }
        }
        (
            Some(PacketLayerGate::Rid(rid)),
            Some(PacketLayerGate::OperatingPoint(operating_point)),
        )
        | (
            Some(PacketLayerGate::OperatingPoint(operating_point)),
            Some(PacketLayerGate::Rid(rid)),
        ) => Some(intersect_operating_point_with_rid(operating_point, rid)),
        (
            Some(PacketLayerGate::OperatingPoint(first_operating_point)),
            Some(PacketLayerGate::OperatingPoint(second_operating_point)),
        ) => Some(intersect_operating_points(
            first_operating_point,
            second_operating_point,
        )),
    }
}

fn union_packet_gates(first: PacketLayerGate, second: PacketLayerGate) -> PacketLayerGate {
    match (first, second) {
        (PacketLayerGate::Open, _) | (_, PacketLayerGate::Open) => PacketLayerGate::Open,
        (PacketLayerGate::Block, gate) | (gate, PacketLayerGate::Block) => gate,
        (PacketLayerGate::Rid(first_rid), PacketLayerGate::Rid(second_rid)) => {
            if first_rid == second_rid {
                PacketLayerGate::Rid(first_rid)
            } else {
                PacketLayerGate::Open
            }
        }
        (PacketLayerGate::Rid(rid), PacketLayerGate::OperatingPoint(operating_point))
        | (PacketLayerGate::OperatingPoint(operating_point), PacketLayerGate::Rid(rid)) => {
            if operating_point.rid() == Some(rid) {
                PacketLayerGate::Rid(rid)
            } else {
                PacketLayerGate::Open
            }
        }
        (
            PacketLayerGate::OperatingPoint(first_operating_point),
            PacketLayerGate::OperatingPoint(second_operating_point),
        ) => union_operating_points(first_operating_point, second_operating_point),
    }
}

fn union_operating_points(
    first: PacketOperatingPointGate,
    second: PacketOperatingPointGate,
) -> PacketLayerGate {
    if first.rid() != second.rid() {
        return PacketLayerGate::Open;
    }
    PacketLayerGate::OperatingPoint(PacketOperatingPointGate::new(
        first.rid(),
        first
            .max_temporal_layer_id()
            .max(second.max_temporal_layer_id()),
    ))
}

fn intersect_operating_point_with_rid(
    operating_point: PacketOperatingPointGate,
    rid: Rid,
) -> PacketLayerGate {
    match operating_point.rid() {
        Some(point_rid) if point_rid == rid => PacketLayerGate::OperatingPoint(operating_point),
        Some(_) => PacketLayerGate::Block,
        None => PacketLayerGate::OperatingPoint(operating_point.with_rid(rid)),
    }
}

fn intersect_operating_points(
    first: PacketOperatingPointGate,
    second: PacketOperatingPointGate,
) -> PacketLayerGate {
    let rid = match (first.rid(), second.rid()) {
        (Some(first_rid), Some(second_rid)) if first_rid == second_rid => Some(first_rid),
        (Some(_), Some(_)) => return PacketLayerGate::Block,
        (Some(rid), None) | (None, Some(rid)) => Some(rid),
        (None, None) => None,
    };
    PacketLayerGate::OperatingPoint(PacketOperatingPointGate::new(
        rid,
        first
            .max_temporal_layer_id()
            .min(second.max_temporal_layer_id()),
    ))
}

#[cfg(test)]
mod tests {
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
        let gate =
            PacketLayerGate::OperatingPoint(PacketOperatingPointGate::new(Some("hi".into()), 1));

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
                &PacketLayerGate::OperatingPoint(PacketOperatingPointGate::new(
                    Some("hi".into()),
                    0,
                )),
                &PacketLayerGate::OperatingPoint(PacketOperatingPointGate::new(
                    Some("hi".into()),
                    2,
                )),
            ]),
            Some(PacketLayerGate::OperatingPoint(
                PacketOperatingPointGate::new(Some("hi".into()), 2)
            ))
        );
        assert_eq!(
            aggregate_packet_gates([
                &PacketLayerGate::OperatingPoint(PacketOperatingPointGate::new(
                    Some("hi".into()),
                    1,
                )),
                &PacketLayerGate::Rid("hi".into()),
            ]),
            Some(PacketLayerGate::Rid("hi".into()))
        );
    }
}
