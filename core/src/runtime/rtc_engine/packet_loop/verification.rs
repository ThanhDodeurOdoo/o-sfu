//! Cfg-gated exports for deterministic packet-loop verification.
//!
//! This module intentionally re-exports production packet-loop types instead of
//! wrapping them in a parallel facade. Miri targets, fuzz targets, Kani
//! harnesses and local benchmarks construct the same state, scratch and turn
//! runner used by the runtime host.

pub use str0m::media::KeyframeRequestKind;
use str0m::media::Mid;
use tokio::sync::mpsc;

pub use super::{
    machine::{
        effect::PacketLoopEffects,
        scratch::{
            MAX_RELAY_PACKETS_PER_ITERATION, PacketLoopScratch, PacketLoopScratchCapacities,
        },
        state::PacketLoopState,
        turn::{PacketLoopTurn, PacketLoopTurnInput},
    },
    route_snapshot::PacketLoopRouteSnapshot,
    session_drain::DrainedSessionOutput,
    time::PacketLoopTime,
};
pub use crate::runtime::rtc_engine::{
    forwarded_packet::{ForwardedPacket, test_support::sample_forwarded_packet},
    route_control::coalesce_keyframe_kind,
    routing_miss::{PacketLoopRoutingMissCache, PacketLoopRoutingMissKey, PacketLoopRoutingState},
    state::RtcBootstrapState,
};
use crate::runtime::{
    media_transport::{TransportMediaId, TransportSessionKey},
    packet_sink_registry::RoomPacketSinkRegistry,
    rtc_engine::{
        demux::{MediaRouteDestination, MediaRouteEntry},
        media_registry::RegisteredMediaHandle,
        relay_registry::{RELAY_MAILBOX_CAPACITY, RelayPacketMailbox, RelayTargetId},
        route_control::PacketLayerGate,
    },
};

#[must_use]
pub fn install_source_fixture(
    state: &mut RtcBootstrapState,
    session_key: TransportSessionKey,
    mid: &str,
) -> TransportMediaId {
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key,
        mid: Mid::from(mid),
    });
    state
        .packet_loop
        .media_route_index
        .entry(transport_media_id)
        .or_insert_with(|| MediaRouteEntry {
            source_active: true,
            destinations: Vec::new(),
        });
    transport_media_id
}

#[must_use]
pub fn install_local_destination_fixture(
    state: &mut RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
    session_key: TransportSessionKey,
    mid: &str,
) -> TransportMediaId {
    let mid = Mid::from(mid);
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Consumer {
        session_key: session_key.clone(),
        mid,
        source_transport_media_id,
    });
    state
        .packet_loop
        .media_route_index
        .entry(source_transport_media_id)
        .or_insert_with(|| MediaRouteEntry {
            source_active: true,
            destinations: Vec::new(),
        })
        .destinations
        .push(MediaRouteDestination {
            dest_session: session_key,
            dest_transport_media_id: transport_media_id,
            dest_mid: mid,
            dest_payload_type: None,
            active: true,
            packet_gate: PacketLayerGate::Open,
            pending_packet_gate: None,
        });
    transport_media_id
}

#[must_use]
pub fn install_relay_target_fixture(
    state: &mut RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
    raw_target_id: u64,
) -> PacketLoopRelayTargetFixture {
    let (tx, rx) = mpsc::channel(RELAY_MAILBOX_CAPACITY);
    let mailbox = RelayPacketMailbox::new(tx);
    let target_id = RelayTargetId::new(raw_target_id);
    state.add_relay_target(source_transport_media_id, target_id, mailbox.into());
    state.set_relay_target_active(source_transport_media_id, target_id, true);
    PacketLoopRelayTargetFixture { _rx: rx }
}

pub struct PacketLoopRelayTargetFixture {
    _rx: mpsc::Receiver<ForwardedPacket>,
}

pub fn refresh_route_snapshot_fixture(
    state: &RtcBootstrapState,
    routes: &mut PacketLoopRouteSnapshot,
) {
    routes.refresh_from(state, &RoomPacketSinkRegistry::default());
}

pub fn step_packet_loop_fixture(
    state: &mut RtcBootstrapState,
    scratch: &mut PacketLoopScratch,
    effects: &mut PacketLoopEffects,
    input: PacketLoopTurnInput<'_>,
) {
    PacketLoopTurn::step(&mut state.packet_loop, scratch, effects, input);
}
