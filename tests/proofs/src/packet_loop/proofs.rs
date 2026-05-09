//! packet-loop proof harnesses that call production verification exports
//!
//! the harnesses are intentionally narrow. if a property cannot be expressed
//! against the real packet-loop types, it does not belong in this module

use std::net::SocketAddr;

use o_sfu_core::{
    RoomInstanceId,
    server::transport::packet_loop_verification::{
        KeyframeRequestKind, PacketLoopRoutingMissCache, PacketLoopRoutingMissKey,
        PacketLoopScratch, coalesce_keyframe_kind,
    },
};

#[kani::proof]
#[kani::unwind(8)]
fn packet_loop_recent_miss_cache_is_exact_for_recorded_packet() {
    let source = SocketAddr::from(([127, 0, 0, 1], 40_000));
    let candidate = SocketAddr::from(([127, 0, 0, 1], 40_001));
    let packet: Vec<u8> = kani::bounded_any::<_, 4>();
    let query_uses_same_source = kani::any::<bool>();
    let query_uses_same_candidate = kani::any::<bool>();
    let query_packet = if kani::any::<bool>() {
        packet.clone()
    } else {
        kani::bounded_any::<_, 4>()
    };
    let query_source = if query_uses_same_source {
        source
    } else {
        SocketAddr::from(([127, 0, 0, 1], 40_002))
    };
    let query_candidate = if query_uses_same_candidate {
        candidate
    } else {
        SocketAddr::from(([127, 0, 0, 1], 40_003))
    };
    let mut cache = PacketLoopRoutingMissCache::default();
    let recorded_key = PacketLoopRoutingMissKey::new(source, candidate, &packet);
    let query_key = PacketLoopRoutingMissKey::new(query_source, query_candidate, &query_packet);

    cache.record(recorded_key, &packet);

    if cache.contains(query_key, &query_packet) {
        assert!(query_uses_same_source);
        assert!(query_uses_same_candidate);
        assert_eq!(query_packet.len(), packet.len());
        for idx in 0..packet.len() {
            assert_eq!(query_packet[idx], packet[idx]);
        }
    }
}

#[kani::proof]
#[kani::unwind(8)]
fn packet_loop_topology_invalidation_clears_recent_miss_cache() {
    let source = SocketAddr::from(([127, 0, 0, 1], 40_010));
    let candidate = SocketAddr::from(([127, 0, 0, 1], 40_011));
    let packet = symbolic_packet();
    let miss_key = PacketLoopRoutingMissKey::new(source, candidate, &packet);
    let mut cache = PacketLoopRoutingMissCache::default();

    cache.record(miss_key, &packet);
    cache.clear();

    assert!(!cache.contains(miss_key, &packet));
}

#[kani::proof]
#[kani::unwind(8)]
fn packet_loop_route_success_clears_recent_miss_cache() {
    let source = SocketAddr::from(([127, 0, 0, 1], 40_020));
    let candidate = SocketAddr::from(([127, 0, 0, 1], 40_021));
    let packet: Vec<u8> = kani::bounded_any::<_, 4>();
    let miss_key = PacketLoopRoutingMissKey::new(source, candidate, &packet);
    let mut cache = PacketLoopRoutingMissCache::default();

    cache.record(miss_key, &packet);
    cache.forget(miss_key, &packet);

    assert!(!cache.contains(miss_key, &packet));
}

#[kani::proof]
fn packet_loop_keyframe_kind_coalescing_prefers_fir() {
    let current = symbolic_keyframe_kind();
    let incoming = symbolic_keyframe_kind();
    let coalesced = coalesce_keyframe_kind(current, incoming);

    if current == KeyframeRequestKind::Fir || incoming == KeyframeRequestKind::Fir {
        assert_eq!(coalesced, KeyframeRequestKind::Fir);
    } else {
        assert_eq!(coalesced, current);
    }
}

#[kani::proof]
fn packet_loop_scratch_clear_removes_staged_work_and_keeps_capacity() {
    let mut scratch = PacketLoopScratch::new();
    let destination = SocketAddr::from(([127, 0, 0, 1], 41_000));
    let payload = symbolic_packet();

    scratch.push_pending_transmit(destination, &payload);
    scratch.mark_source_policy_dirty(RoomInstanceId::from_raw(1));
    let warmed = scratch.capacities();
    scratch.clear();

    assert!(scratch.is_turn_empty());
    assert!(scratch.capacities().retained_at_least(warmed));
}

fn symbolic_packet() -> [u8; 4] {
    kani::any::<[u8; 4]>()
}

fn symbolic_keyframe_kind() -> KeyframeRequestKind {
    if kani::any::<bool>() {
        KeyframeRequestKind::Fir
    } else {
        KeyframeRequestKind::Pli
    }
}
