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
    demux::{MediaRouteDestination, MediaRouteEntry},
    forwarded_packet::ForwardedPacket,
    forwarding_destination::PacketForward,
    relay_registry::{ActiveRelayTarget, RelayTargetId},
    route_control::{PacketLayerMetadata, PacketRouteDecision},
    state::PacketLoopState,
};
use crate::runtime::{
    media_transport::TransportMediaId as RouteTransportMediaId,
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
pub(super) fn populate_forward_routes_for_packet(
    state: &PacketLoopState,
    packet_sinks: &impl PacketSinkLookup,
    metrics: &impl RtcRouteControlMetrics,
    packet_idx: usize,
    packet: &mut ForwardedPacket,
    forwards: &mut Vec<PacketForward>,
) {
    let Some(facts) = packet.resolve_facts(state) else {
        return;
    };
    let source_transport_media_id = facts.source_transport_media_id;
    let origin_sink = packet
        .visits_origin_sinks()
        .then(|| packet_sinks.sink_for_room(facts.room_instance_id))
        .flatten();
    let relay_targets = packet
        .visits_origin_sinks()
        .then(|| state.relay_targets_for_source(source_transport_media_id))
        .flatten();
    let route_entry = state.media_route_index.get(&source_transport_media_id);
    reserve_forward_capacity(origin_sink.as_ref(), relay_targets, route_entry, forwards);
    push_origin_sink_forward(packet_idx, source_transport_media_id, origin_sink, forwards);
    if !has_routed_forward(relay_targets, route_entry) {
        return;
    }
    let metadata = facts.layer_metadata;
    if !source_packet_gate_permits(state, metrics, source_transport_media_id, metadata) {
        return;
    }
    populate_relay_forwards(
        state,
        relay_targets,
        packet_idx,
        source_transport_media_id,
        metadata,
        forwards,
    );
    if let Some(route_entry) = route_entry {
        populate_local_forwards(
            route_entry,
            packet_idx,
            source_transport_media_id,
            metadata,
            forwards,
        );
    }
}

/// Adds the room-scoped packet sink destination when this packet still belongs
/// to the source worker.
///
/// Packet sinks are modeled as forwarding destinations rather than called
/// directly here so `flush_forward_routes` remains the single owner of
/// destination metrics and payload lifetime decisions.
fn push_origin_sink_forward(
    packet_idx: usize,
    source_transport_media_id: RouteTransportMediaId,
    sink: Option<RegisteredPacketSink>,
    forwards: &mut Vec<PacketForward>,
) {
    let Some(sink) = sink else {
        return;
    };
    forwards.push(PacketForward::from_packet_sink(
        packet_idx,
        source_transport_media_id,
        sink,
    ));
}

/// Reserves the fanout list for the largest destination count this packet can
/// produce.
///
/// The bound intentionally counts configured destinations before packet gates
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
    forwards.reserve(planned_forwards);
}

/// Applies the source-wide packet gate that belongs to transport route control.
///
/// This is the last check before destination planning. A source-level drop
/// suppresses local and relay fanout together, which keeps active-speaker,
/// selected-layer and server-owned source gates authoritative for every
/// downstream destination.
fn source_packet_gate_permits(
    state: &PacketLoopState,
    metrics: &impl RtcRouteControlMetrics,
    source_transport_media_id: RouteTransportMediaId,
    metadata: PacketLayerMetadata,
) -> bool {
    match state
        .route_control
        .decide_packet_route(source_transport_media_id, metadata)
    {
        PacketRouteDecision::Forward => {
            metrics.record_rtc_route_control(RtcRouteControlOutcome::LayerAllowed);
            true
        }
        PacketRouteDecision::Drop => {
            metrics.record_rtc_route_control(RtcRouteControlOutcome::LayerDropped);
            false
        }
    }
}

/// Adds relay destinations whose target-specific gates permit this packet.
///
/// Relay targets represent worker or node boundaries, not room policy. The
/// registry tells this planner which targets currently need this source and
/// route control decides whether the current packet layer is allowed for each
/// target.
fn populate_relay_forwards(
    state: &PacketLoopState,
    relay_targets: Option<&[ActiveRelayTarget]>,
    packet_idx: usize,
    source_transport_media_id: RouteTransportMediaId,
    metadata: PacketLayerMetadata,
    forwards: &mut Vec<PacketForward>,
) {
    let Some(relay_targets) = relay_targets else {
        return;
    };
    for relay_target in relay_targets {
        if !relay_target_gate_permits(
            state,
            source_transport_media_id,
            relay_target.target_id,
            metadata,
        ) {
            continue;
        }
        push_relay_forward(
            packet_idx,
            source_transport_media_id,
            relay_target,
            forwards,
        );
    }
}

/// Converts a registered relay target into the concrete send handle used by
/// the flush step.
///
/// Cloning here is limited to transport handles that are designed for cheap
/// enqueue ownership. Payload sharing is deferred until flush so all relay
/// destinations for a packet can reuse one shared relay packet.
fn push_relay_forward(
    packet_idx: usize,
    source_transport_media_id: RouteTransportMediaId,
    relay_target: &ActiveRelayTarget,
    forwards: &mut Vec<PacketForward>,
) {
    forwards.push(PacketForward::from_relay_target(
        packet_idx,
        source_transport_media_id,
        relay_target.target.clone(),
    ));
}

/// Adds local RTC destinations for active routes whose consumer gate permits
/// the current packet layer.
///
/// Local fanout remains proportional to the number of writable receiver
/// sessions. This planner can avoid avoidable allocation work, but it cannot
/// collapse receiver-specific WebRTC egress into one broadcast operation.
///
/// planned local destinations are compact route handles
/// the flush step resolves each handle against `media_route_index` before
/// touching the destination session, so planning does not clone route-stable
/// consumer identity
fn populate_local_forwards(
    route_entry: &MediaRouteEntry,
    packet_idx: usize,
    source_transport_media_id: RouteTransportMediaId,
    metadata: PacketLayerMetadata,
    forwards: &mut Vec<PacketForward>,
) {
    if !route_entry.source_active {
        debug!(
            ?source_transport_media_id,
            "skipped forwarding because source route is inactive"
        );
        return;
    }
    for (destination_index, destination) in route_entry.destinations.iter().enumerate() {
        if destination_packet_gate_permits(source_transport_media_id, destination, metadata) {
            forwards.push(PacketForward::from_local_route_destination(
                packet_idx,
                source_transport_media_id,
                destination_index,
            ));
        }
    }
}

/// Checks the consumer-owned route state for one local destination.
///
/// Destination activity is distinct from the source-wide gate. A producer can
/// be active while one consumer is muted, paused or waiting for a selected
/// simulcast layer to become decodable.
fn destination_packet_gate_permits(
    _source_transport_media_id: RouteTransportMediaId,
    destination: &MediaRouteDestination,
    metadata: PacketLayerMetadata,
) -> bool {
    if !destination.active {
        //debug!(
        //    ?source_transport_media_id,
        //    consumer_session_key = ?destination.dest_session,
        //    consumer_transport_media_id = ?destination.dest_transport_media_id,
        //    "skipped forwarding because destination route is inactive"
        //);
        return false;
    }
    if destination.packet_gate.permits(metadata) {
        return true;
    }
    false
}

/// Checks relay-target packet policy without treating a missing gate as a drop.
///
/// Missing relay gates mean the target has no extra layer restriction beyond
/// the source-wide gate. This keeps newly activated relay targets open until
/// room or transport policy installs a narrower packet gate.
fn relay_target_gate_permits(
    state: &PacketLoopState,
    source_transport_media_id: RouteTransportMediaId,
    target_id: RelayTargetId,
    metadata: PacketLayerMetadata,
) -> bool {
    state
        .route_control
        .relay_packet_gate(source_transport_media_id, target_id)
        .is_none_or(|packet_gate| packet_gate.permits(metadata))
}

/// Reports whether route planning has any destination work after origin sinks.
///
/// Origin sinks are intentionally excluded because recording or similar side
/// effects must still run for source packets even when the source has no live
/// relay or local RTC consumers.
fn has_routed_forward(
    relay_targets: Option<&[ActiveRelayTarget]>,
    route_entry: Option<&MediaRouteEntry>,
) -> bool {
    relay_targets.is_some_and(|targets| !targets.is_empty())
        || route_entry.is_some_and(|entry| entry.source_active && entry.has_active_destinations())
}
