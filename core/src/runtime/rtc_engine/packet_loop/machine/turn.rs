//! Deterministic packet-loop turn runner.
//!
//! A turn consumes already available host input, mutates packet-loop state,
//! writes reusable scratch buffers and emits ordered effects. The async host
//! remains responsible for draining sockets, channels, polling `str0m` and
//! executing effects against process-owned services.

use super::{
    super::{
        super::{
            forwarded_packet::ForwardedPacket,
            forwarding_planner::populate_forward_routes_for_packet,
        },
        forward_flush::record_incoming_stats,
        keyframe_requests::flush_pending_keyframe_requests,
        route_snapshot::PacketLoopRouteSnapshot,
        selected_rid::drain_due_rid_keyframe_refreshes,
        session_drain::{DrainedSessionOutput, apply_session_outputs},
        time::PacketLoopTime,
    },
    effect::PacketLoopEffects,
    scratch::PacketLoopScratch,
    state::PacketLoopState,
};

pub struct PacketLoopTurn;

pub struct PacketLoopTurnInput<'a> {
    packet_now: PacketLoopTime,
    session_outputs: &'a mut Vec<DrainedSessionOutput>,
    relay_packets: &'a mut Vec<ForwardedPacket>,
    routes: &'a PacketLoopRouteSnapshot,
}

impl<'a> PacketLoopTurnInput<'a> {
    #[must_use]
    pub fn new(
        packet_now: PacketLoopTime,
        session_outputs: &'a mut Vec<DrainedSessionOutput>,
        relay_packets: &'a mut Vec<ForwardedPacket>,
        routes: &'a PacketLoopRouteSnapshot,
    ) -> Self {
        Self {
            packet_now,
            session_outputs,
            relay_packets,
            routes,
        }
    }
}

impl PacketLoopTurn {
    pub fn step(
        state: &mut PacketLoopState,
        scratch: &mut PacketLoopScratch,
        effects: &mut PacketLoopEffects,
        input: PacketLoopTurnInput<'_>,
    ) {
        let PacketLoopTurnInput {
            packet_now,
            session_outputs,
            relay_packets,
            routes,
        } = input;
        scratch.clear();
        effects.clear();
        apply_session_outputs(session_outputs, scratch, effects);
        for packet in relay_packets.drain(..) {
            scratch.push_pending_packet(packet);
        }
        drain_due_rid_keyframe_refreshes(state, effects, packet_now);
        flush_pending_keyframe_requests(state, effects, scratch, packet_now);
        record_incoming_stats(state, effects, scratch, packet_now);
        scratch.plan_pending_packets(|packet_idx, packet, forwards| {
            populate_forward_routes_for_packet(
                state, routes, effects, packet_idx, packet, forwards,
            );
        });
    }
}
