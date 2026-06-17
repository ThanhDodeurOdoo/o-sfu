//! Packet forwarding planner for the RTC engine hot path.
//!
//! The packet loop receives media as `ForwardedPacket` values, but the flush
//! step needs concrete destinations that know how to write to local RTC state,
//! packet sinks such as recording or relay mailboxes. This module is the narrow
//! planning boundary between those two shapes.
//!
//! The planner owns mechanical fanout only. Room policy, receiver layout,
//! bandwidth budgeting and source selection must already have been projected
//! into packet-facing route-control gates before a packet reaches this file.
//!
//! # Hot-path contract
//!
//! Planning runs inside the worker packet loop while the packet-loop state is
//! borrowed. It must avoid async work, broad scans and steady-state allocation.
//! `PacketLoopBuffers` keeps the destination list across iterations, so this
//! module may reserve the known fanout bound but must not create detached
//! per-packet collections.
//!
//! Destination order is part of the flush contract. Packet sinks are planned
//! first so origin-side side effects see publisher packets before relay or
//! local egress. Relay destinations are planned before local RTC destinations
//! so the flush step can still identify the last destination for payload move
//! versus clone decisions.

use tracing::debug;

use super::{
    forwarded_packet::ForwardedPacket,
    forwarding_destination::PacketForward,
    relay_registry::{ActiveRelayTarget, RelayTargetId},
    route_control::{PacketLayerGate, PacketLayerMetadata},
    source_route::MediaRouteEntry,
    state::PacketLoopState,
};
use crate::engine::{
    media_transport::TransportMediaId,
    metrics::{RtcRouteControlMetrics, RtcRouteControlOutcome},
    packet_sink_registry::{PacketSinkLookup, RegisteredPacketSink},
};

/// Plans destinations for one packet using already-projected transport state.
///
/// Missing source identity is treated as a best-effort miss. The ingress path
/// can receive packets before MID or SSRC learning is complete, so the planner
/// simply skips packets it cannot attach to a transport media id yet.
///
/// Origin-side sinks and relay fanout only apply to packets that still visit
/// their source worker. Relayed packets already consumed those source-side
/// effects and must not be sent back into second-hop relay sinks.
pub(super) fn plan_forwards(
    state: &PacketLoopState,
    packet_sinks: &impl PacketSinkLookup,
    metrics: &impl RtcRouteControlMetrics,
    pkt_idx: usize,
    pkt: &mut ForwardedPacket,
    forwards: &mut Vec<PacketForward>,
) {
    let Some(facts) = pkt.resolve_facts(state) else {
        return;
    };
    let src_media = facts.src_media;
    let visits_origin = pkt.visits_origin_sinks();
    let origin_sink = if visits_origin {
        packet_sinks.sink_for_room(facts.room_instance_id)
    } else {
        None
    };
    let (route_entry, relay_targets, source_gate) = if let Some(origin_sink) = origin_sink {
        if !state.routes.has_forwarding_sources() {
            forwards.push(PacketForward::from_packet_sink(
                pkt_idx,
                src_media,
                origin_sink,
            ));
            return;
        }
        let view = state.routes.forward_view(src_media, visits_origin);
        reserve_forward_capacity(Some(&origin_sink), view.1, view.0, forwards);
        forwards.push(PacketForward::from_packet_sink(
            pkt_idx,
            src_media,
            origin_sink,
        ));
        view
    } else {
        let view = state.routes.forward_view(src_media, visits_origin);
        reserve_forward_capacity(None, view.1, view.0, forwards);
        view
    };
    if !has_routed_forward(relay_targets, route_entry) {
        return;
    }
    let metadata = facts.layer_metadata;
    if !src_gate_permits(metrics, source_gate, metadata) {
        return;
    }
    if let Some(relay_targets) = relay_targets {
        populate_relay_forwards(state, relay_targets, pkt_idx, src_media, metadata, forwards);
    }
    if let Some(route_entry) = route_entry {
        populate_local_forwards(route_entry, pkt_idx, src_media, metadata, forwards);
    }
}

/// Reserves the fanout list for the largest destination count this packet can
/// produce.
///
/// The bound counts configured destinations before packet gates
/// are applied. That may reserve a few unused slots when routes are inactive or
/// layer gates drop the packet, but it avoids allocator churn when a dense room
/// crosses a previous high-water mark.
fn reserve_forward_capacity(
    origin_sink: Option<&RegisteredPacketSink>,
    relay_targets: Option<&[ActiveRelayTarget]>,
    route_entry: Option<&MediaRouteEntry>,
    forwards: &mut Vec<PacketForward>,
) {
    let planned_forwards = usize::from(origin_sink.is_some())
        + relay_targets.map_or(0, <[ActiveRelayTarget]>::len)
        + route_entry.map_or(0, |entry| entry.destinations.len());
    if forwards.capacity().saturating_sub(forwards.len()) < planned_forwards {
        forwards.reserve(planned_forwards);
    }
}

/// Applies the source-wide packet gate that belongs to transport route control.
///
/// This is the last check before destination planning. A source-level drop
/// suppresses local and relay fanout together, which keeps active-speaker,
/// selected-layer and source-wide policy gates authoritative for every
/// downstream destination.
fn src_gate_permits(
    metrics: &impl RtcRouteControlMetrics,
    src_gate: Option<PacketLayerGate>,
    metadata: PacketLayerMetadata,
) -> bool {
    if let Some(src_gate) = src_gate
        && !src_gate.permits(metadata)
    {
        metrics.record_rtc_route_control(RtcRouteControlOutcome::LayerDropped);
        return false;
    }
    metrics.record_rtc_route_control(RtcRouteControlOutcome::LayerAllowed);
    true
}

/// Adds relay destinations whose target-specific gates permit this packet.
///
/// Relay targets represent worker or node boundaries, not room policy. The
/// registry tells this planner which targets currently need this source and
/// route control decides whether the current packet layer is allowed for each
/// target.
fn populate_relay_forwards(
    state: &PacketLoopState,
    relay_targets: &[ActiveRelayTarget],
    pkt_idx: usize,
    src_media: TransportMediaId,
    metadata: PacketLayerMetadata,
    forwards: &mut Vec<PacketForward>,
) {
    for relay_target in relay_targets {
        if !relay_target_gate_permits(state, src_media, relay_target.target_id, metadata) {
            continue;
        }
        forwards.push(PacketForward::from_relay_target(
            pkt_idx,
            src_media,
            relay_target.target.clone(),
        ));
    }
}

/// Adds local RTC destinations for active routes whose consumer gate permits
/// the current packet layer.
///
/// Local fanout remains proportional to the number of writable receiver
/// sessions. This planner can avoid avoidable allocation work, but it cannot
/// collapse receiver-specific WebRTC egress into one broadcast operation.
///
/// planned local destinations are compact route handles
/// the flush step resolves each handle against `RouteTable` before
/// touching the destination session, so planning does not clone route-stable
/// consumer identity
fn populate_local_forwards(
    route_entry: &MediaRouteEntry,
    pkt_idx: usize,
    src_media: TransportMediaId,
    metadata: PacketLayerMetadata,
    forwards: &mut Vec<PacketForward>,
) {
    if !route_entry.source_active {
        debug!(
            source_transport_media_id = ?src_media,
            "skipped forwarding because source route is inactive"
        );
        return;
    }
    if route_entry.active_destination_count == route_entry.destinations.len() {
        for (dst_idx, dst) in route_entry.destinations.iter().enumerate() {
            if dst.packet_gate.permits(metadata) {
                forwards.push(PacketForward::from_local_route_destination(
                    pkt_idx, src_media, dst_idx,
                ));
            }
        }
        return;
    }
    for (dst_idx, dst) in route_entry.destinations.iter().enumerate() {
        if dst.active && dst.packet_gate.permits(metadata) {
            forwards.push(PacketForward::from_local_route_destination(
                pkt_idx, src_media, dst_idx,
            ));
        }
    }
}

/// Checks relay-target packet policy without treating a missing gate as a drop.
///
/// Missing relay gates mean the target has no extra layer restriction beyond
/// the source-wide gate. This keeps newly activated relay targets open until
/// room or transport policy installs a narrower packet gate.
fn relay_target_gate_permits(
    state: &PacketLoopState,
    src_media: TransportMediaId,
    target_id: RelayTargetId,
    metadata: PacketLayerMetadata,
) -> bool {
    state
        .routes
        .relay_packet_gate(src_media, target_id)
        .is_none_or(|packet_gate| packet_gate.permits(metadata))
}

/// Reports whether route planning has any destination work after origin sinks.
///
/// Origin sinks are excluded because recording or similar side
/// effects must still run for source packets even when the source has no live
/// relay or local RTC consumers.
fn has_routed_forward(
    relay_targets: Option<&[ActiveRelayTarget]>,
    route_entry: Option<&MediaRouteEntry>,
) -> bool {
    route_entry.is_some_and(|entry| entry.source_active && entry.has_active_destinations())
        || relay_targets.is_some_and(|targets| !targets.is_empty())
}
