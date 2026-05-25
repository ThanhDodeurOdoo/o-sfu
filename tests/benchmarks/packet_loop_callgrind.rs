//! deterministic Callgrind coverage for packet-loop hot-path slices
//!
//! this suite measures fixed units of packet-loop work with `Ir`, (the
//! instruction-count metric of by Callgrind)
//! each benchmark builds the RTC-engine state outside the measured function,
//! then repeats one stable packet-loop operation with reusable buffers
//!
//! the value of this target is base-versus-head review, not throughput proof
//! it catches accidental instruction growth in production packet-loop helpers
//! before that growth becomes visible as lower room fanout, slower ingress
//! routing or extra route-control work under load
//!
//! the measured slices are on pupose narrower than the async worker loop
//! they cover route planning, relay enqueue pressure, UDP ingress demux,
//! packet-sink fanout, selected-RID readiness and keyframe-request coalescing
//! without mixing scheduler noise or socket waits into the instruction count

#![allow(
    clippy::exit,
    reason = "iai-callgrind's generated benchmark harness exits with the measured runner status"
)]

use std::hint::black_box;

use iai_callgrind::{
    Callgrind, EventKind, LibraryBenchmarkConfig, library_benchmark, library_benchmark_group, main,
};
use o_sfu_core::server::transport::benchmark_support::{
    FanoutBenchTopology, IngressRoutingBenchFixture, KeyframeCoalescingBenchFixture,
    PacketSinkFanoutBenchFixture, RelayPressureBenchFixture, RidReadinessBenchFixture,
    routing_miss_packet_fingerprint,
};

const ROUTING_MISS_FINGERPRINT_ATTEMPTS: usize = 4096;

fn callgrind_config(ir_soft_limit: f64) -> LibraryBenchmarkConfig {
    let mut callgrind = Callgrind::default();
    callgrind.soft_limits([(EventKind::Ir, ir_soft_limit)]);
    callgrind.fail_fast(false);

    let mut config = LibraryBenchmarkConfig::default();
    config.tool(callgrind);
    config
}

fn fanout_topology(destination_count: usize) -> FanoutBenchTopology {
    FanoutBenchTopology::with_local_destinations(destination_count)
}

fn fingerprint_packet(packet_len: usize) -> Vec<u8> {
    let mut packet = Vec::with_capacity(packet_len);
    let sequence_number = 1_u16.to_be_bytes();
    let ssrc = 11_u32.to_be_bytes();
    packet.extend_from_slice(&[
        0x80,
        96,
        sequence_number[0],
        sequence_number[1],
        0,
        0,
        0,
        1,
        ssrc[0],
        ssrc[1],
        ssrc[2],
        ssrc[3],
    ]);
    for byte_index in packet.len()..packet_len {
        let mixed = byte_index
            .wrapping_mul(31)
            .wrapping_add(byte_index.rotate_left(5))
            .wrapping_add(17);
        packet.push(u8::try_from(mixed & 0xff).unwrap_or(0));
    }
    packet
}

// measures local fanout route planning for one producer and fixed local
// destination counts
//
// this protects the dense-room planner path where every extra destination is
// real work, so the useful info is if buffer reuse and route lookup stay
// proportional to the required fanout rather than adding allocator churn or
// unrelated scans
#[library_benchmark(config = callgrind_config(0.5))]
#[bench::fanout_1(args = (1usize), setup = fanout_topology)]
#[bench::fanout_8(args = (8usize), setup = fanout_topology)]
#[bench::fanout_32(args = (32usize), setup = fanout_topology)]
#[bench::fanout_64(args = (64usize), setup = fanout_topology)]
fn route_plan_1024(mut topology: FanoutBenchTopology) -> usize {
    black_box(topology.plan_route_turns())
}

// measures relay enqueue pressure at the production non-blocking mailbox
// boundary
//
// the open and overloaded cases have different expected outcomes but both are
// packet-loop work that can run for every relayed packet
// keeping them cheap preserves cross-worker forwarding under bursty rooms
#[library_benchmark(config = callgrind_config(1.0))]
#[bench::enqueue(RelayPressureBenchFixture::open_mailbox())]
#[bench::overloaded(RelayPressureBenchFixture::full_mailbox())]
fn relay_mailbox_256(fixture: RelayPressureBenchFixture) -> usize {
    black_box(fixture.run_attempts())
}

// measures UDP ingress demux for the indexed happy path and the defensive
// unknown-source miss path
//
// cached accepted routing protects the normal packet ingress path after a
// remote address has been learned
// repeated misses protect the defensive path that must stay bounded when noise
// or stale peers send datagrams that do not belong to a live RTC session
#[library_benchmark(config = callgrind_config(1.0))]
#[bench::cached_route(IngressRoutingBenchFixture::cached_accepted_route())]
#[bench::unknown_source(IngressRoutingBenchFixture::repeated_unknown_source_miss())]
#[bench::unknown_source_rtp_1200(IngressRoutingBenchFixture::repeated_large_unknown_source_miss())]
fn ingress_demux_256(mut fixture: IngressRoutingBenchFixture) -> usize {
    black_box(fixture.route_datagrams())
}

// measures the routing-miss fingerprint helper directly with an RTP-shaped
// packet large enough to represent the normal media packet case
//
// this keeps the fingerprint cost visible next to the broader ingress-demux
// benchmark that includes recent-miss cache lookup and drop accounting
#[library_benchmark(config = callgrind_config(1.0))]
#[bench::rtp_1200(args = (1200usize), setup = fingerprint_packet)]
fn routing_miss_fingerprint_4096(packet: Vec<u8>) -> u64 {
    let mut fingerprint = 0_u64;
    for _ in 0..ROUTING_MISS_FINGERPRINT_ATTEMPTS {
        fingerprint = fingerprint.wrapping_add(routing_miss_packet_fingerprint(black_box(
            packet.as_slice(),
        )));
    }
    black_box(fingerprint)
}

// measures packet-sink fanout through production route planning and flush
// delivery
//
// recording sinks share the packet-loop origin side with media forwarding
// this benchmark keeps that adjacent path visible so recording support cannot
// quietly add per-packet cost to rooms that are already forwarding media
#[library_benchmark(config = callgrind_config(1.0))]
#[bench::recording(PacketSinkFanoutBenchFixture::recording_sink())]
fn packet_sink_512(mut fixture: PacketSinkFanoutBenchFixture) -> usize {
    black_box(fixture.route_sink_turns())
}

// measures selected-RID readiness when one observed RID activates many pending
// route gates
//
// this protects video route-control updates from becoming proportional to
// repeated packets or duplicate readiness events instead of the unique source
// and destination work that must actually change
#[library_benchmark(config = callgrind_config(0.5))]
#[bench::selected(RidReadinessBenchFixture::pending_selected_rid())]
fn rid_readiness_256(mut fixture: RidReadinessBenchFixture) -> usize {
    black_box(fixture.activate_selected_rid())
}

// measures producer-side keyframe request coalescing for many consumer-local
// feedback requests
//
// coalescing keeps route-control feedback storms from turning into one remote
// source command per consumer
// this benchmark checks the sorted flush path that collapses many requests into
// the single producer-side signal the packet loop should emit
#[library_benchmark(config = callgrind_config(0.5))]
#[bench::remote_source(KeyframeCoalescingBenchFixture::remote_source_requests())]
fn keyframe_coalesce_512(mut fixture: KeyframeCoalescingBenchFixture) -> usize {
    black_box(fixture.flush_requests())
}

library_benchmark_group!(
    name = packet_loop_callgrind;
    benchmarks =
        route_plan_1024,
        relay_mailbox_256,
        ingress_demux_256,
        routing_miss_fingerprint_4096,
        packet_sink_512,
        rid_readiness_256,
        keyframe_coalesce_512
);

main!(library_benchmark_groups = packet_loop_callgrind);
