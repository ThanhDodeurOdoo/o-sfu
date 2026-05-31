//! packet-facing layer gates for routed rtp
//!
//! this module defines the small gate language used after the packet loop has
//! extracted route-control metadata from an rtp packet
//! it does not parse codecs, choose room policy or schedule keyframes
//! it only answers whether the current packet layer can pass a gate
//!
//! source-wide route control uses this language as set algebra
//! downstream gates are unioned first so packets needed by any relay target or
//! local destination stay alive at the source
//! then the aggregate source gate is intersected with source-level policy such
//! as transport audio activity, so a stricter source policy can still block all
//! fanout for the packet
//!
//! `None` is kept by the surrounding route-control state as "no gate installed"
//! once a [`PacketLayerGate`] exists, [`PacketLayerGate::Open`] is the explicit
//! allow-all value and [`PacketLayerGate::Block`] is the explicit deny-all value

use str0m::media::Rid;

/// packet-layer predicate installed on a source, relay target or destination
///
/// gates are transport-native predicates over metadata that has already been
/// extracted from the packet header
/// they do not carry room policy meaning
/// by the time a gate reaches this file, "selected thumbnail quality" or
/// "active speaker audio policy" has already been projected into layer facts
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(in crate::engine::media_transport::rtc) enum PacketLayerGate {
    /// allow every packet layer for this route
    #[default]
    Open,
    /// drop every packet layer for this route
    Block,
    /// allow only packets whose resolved rid matches the selected rid
    ///
    /// temporal metadata is ignored here because simulcast-only selection can
    /// be expressed by rid alone
    Rid(Rid),
    /// allow packets that fit the selected rid and temporal operating point
    ///
    /// packets without temporal-layer metadata do not pass this gate because the
    /// max temporal layer cannot be enforced safely
    OperatingPoint(PacketOperatingPointGate),
}

/// selected temporal operating point for a layered video source
///
/// a missing rid means the gate applies to any rid as long as the packet exposes
/// temporal-layer metadata that can be compared with `max_temporal_layer_id`
/// this lets route control express sources that have temporal layering without
/// simulcast rid separation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::media_transport::rtc) struct PacketOperatingPointGate {
    /// optional rid restriction for the selected operating point
    rid: Option<Rid>,
    /// highest temporal layer id that may pass through the gate
    max_temporal_layer_id: u8,
}

impl PacketOperatingPointGate {
    /// creates an operating-point gate from already-projected transport facts
    ///
    /// callers are expected to validate that the temporal layer exists on the
    /// advertised source before this value reaches rtc route control
    pub const fn new(rid: Option<Rid>, max_temporal_layer_id: u8) -> Self {
        Self {
            rid,
            max_temporal_layer_id,
        }
    }

    /// returns the selected rid restriction when this operating point is rid-bound
    #[inline]
    pub const fn rid(self) -> Option<Rid> {
        self.rid
    }

    /// returns the inclusive temporal-layer ceiling for this operating point
    pub const fn max_temporal_layer_id(self) -> u8 {
        self.max_temporal_layer_id
    }

    /// returns this operating point constrained to a concrete rid
    ///
    /// intersection uses this when a rid-only gate narrows an operating point
    /// that was previously valid for any rid
    const fn with_rid(self, rid: Rid) -> Self {
        Self {
            rid: Some(rid),
            max_temporal_layer_id: self.max_temporal_layer_id,
        }
    }

    /// checks whether packet metadata is inside this operating point
    ///
    /// temporal-layer metadata is required because forwarding a packet with an
    /// unknown temporal layer would weaken the selected operating point
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
    /// returns the concrete RID selected by this gate when one exists
    ///
    /// open routes, blocked routes and RID-less operating points do not name a
    /// simulcast layer and therefore return `None`
    /// feedback routing uses this to map destination policy back to the
    /// producer layer that can satisfy a keyframe request
    #[inline]
    pub const fn selected_rid(&self) -> Option<Rid> {
        match *self {
            Self::Rid(rid) => Some(rid),
            Self::OperatingPoint(operating_point) => operating_point.rid(),
            Self::Open | Self::Block => None,
        }
    }

    /// checks whether packet metadata passes this gate without allocating
    ///
    /// the packet loop calls this on the forwarding hot path
    /// it must remain a pure metadata predicate with no source-state mutation
    pub fn permits(&self, metadata: PacketLayerMetadata) -> bool {
        match self {
            Self::Open => true,
            Self::Block => false,
            Self::Rid(selected_rid) => metadata.rid() == Some(*selected_rid),
            Self::OperatingPoint(operating_point) => operating_point.permits(metadata),
        }
    }
}

/// route-control metadata extracted from one packet before gate evaluation
///
/// the rid may come from an rtp header extension, a cached source-rid mapping or
/// relay metadata
/// temporal-layer metadata comes from frame-marking state when the packet
/// carries it
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(in crate::engine::media_transport::rtc) struct PacketLayerMetadata {
    /// resolved rid for this packet when one is known
    rid: Option<Rid>,
    /// temporal layer id carried by frame-marking metadata when present
    temporal_layer_id: Option<u8>,
}

impl PacketLayerMetadata {
    /// creates metadata for one packet after route-control extraction
    pub const fn new(rid: Option<Rid>, temporal_layer_id: Option<u8>) -> Self {
        Self {
            rid,
            temporal_layer_id,
        }
    }

    /// returns the resolved rid that gate evaluation can compare
    pub const fn rid(self) -> Option<Rid> {
        self.rid
    }

    /// returns the packet temporal layer id when the packet exposes one
    const fn temporal_layer_id(self) -> Option<u8> {
        self.temporal_layer_id
    }
}

/// source-level route-control decision for the packet loop
///
/// this decision only covers the source-wide gate
/// relay-target and local-destination gates still run during destination
/// planning so each downstream route can keep its own selected layer
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::media_transport::rtc) enum PacketRouteDecision {
    /// keep planning destinations for this packet
    Forward,
    /// stop fanout because no source-level policy permits this packet
    Drop,
}

/// computes the permissive source gate needed to preserve all downstream gates
///
/// the route-control state uses this before source-level packet filtering
/// if two downstream gates require different rids, the aggregate becomes open
/// because no narrower source-level predicate can keep both routes decodable
/// destination-level gates still apply later and perform the final narrowing
///
/// returns `None` when no gate was installed by any caller
pub(in crate::engine::media_transport::rtc) fn aggregate_packet_gates<'a>(
    packet_gates: impl IntoIterator<Item = &'a PacketLayerGate>,
) -> Option<PacketLayerGate> {
    let mut aggregate = None;
    for packet_gate in packet_gates {
        aggregate = Some(aggregate.map_or_else(
            || *packet_gate,
            |current| union_packet_gates(current, *packet_gate),
        ));
        if matches!(aggregate.as_ref(), Some(PacketLayerGate::Open)) {
            return aggregate;
        }
    }
    aggregate
}

/// composes two gates so only packets accepted by both gates can pass
///
/// `None` means "no gate installed" and acts as the identity value
/// this is used to apply source-level restrictions such as transport audio
/// policy after downstream gates have already been widened into one source gate
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

/// widens two gates so any packet allowed by either gate can survive source filtering
///
/// this produces the narrowest gate expressible by [`PacketLayerGate`] for the
/// union
/// when the grammar cannot express the union precisely, it returns open and
/// relies on destination gates to narrow fanout later
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

/// unions two operating points that refer to the same rid scope
///
/// a shared rid scope can be widened by keeping the larger temporal ceiling
/// different rid scopes cannot be represented as one operating point, so the
/// caller must open the source gate
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

/// narrows an operating point with a rid-only gate
///
/// when the operating point had no rid, the rid gate supplies that missing
/// scope
/// when both sides name different rids, the intersection is empty and blocks
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

/// intersects two operating points into their shared rid and temporal scope
///
/// matching or missing rid scopes are compatible
/// incompatible concrete rids make the intersection empty
/// compatible scopes keep the smaller temporal ceiling because both gates must
/// permit the packet
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
