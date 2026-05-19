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
};

fn callgrind_config(ir_soft_limit: f64) -> LibraryBenchmarkConfig {
    let mut callgrind = Callgrind::default();
    callgrind.soft_limits([(EventKind::Ir, ir_soft_limit)]);
    callgrind.fail_fast(false);

    let mut config = LibraryBenchmarkConfig::default();
    config.tool(callgrind);
    config
}

fn route_planning_config() -> LibraryBenchmarkConfig {
    callgrind_config(0.5)
}

fn relay_mailbox_config() -> LibraryBenchmarkConfig {
    callgrind_config(1.0)
}

fn ingress_demux_config() -> LibraryBenchmarkConfig {
    callgrind_config(1.0)
}

fn packet_sink_config() -> LibraryBenchmarkConfig {
    callgrind_config(1.0)
}

fn route_control_config() -> LibraryBenchmarkConfig {
    callgrind_config(0.5)
}

fn fanout_topology(destination_count: usize) -> FanoutBenchTopology {
    FanoutBenchTopology::with_local_destinations(destination_count)
}

#[library_benchmark(config = route_planning_config())]
#[benches::fanout(
    args = [1usize, 8, 32, 64],
    setup = fanout_topology
)]
fn route_planning_1024_turns(mut topology: FanoutBenchTopology) -> usize {
    black_box(topology.plan_route_turns())
}

#[library_benchmark(config = relay_mailbox_config())]
#[bench::enqueue_256_attempts(RelayPressureBenchFixture::open_mailbox())]
#[bench::overloaded_256_attempts(RelayPressureBenchFixture::full_mailbox())]
fn relay_mailbox_256_attempts(fixture: RelayPressureBenchFixture) -> usize {
    black_box(fixture.run_attempts())
}

#[library_benchmark(config = ingress_demux_config())]
#[bench::cached_accepted_route_256_datagrams(IngressRoutingBenchFixture::cached_accepted_route())]
#[bench::unknown_source_miss_256_datagrams(
    IngressRoutingBenchFixture::repeated_unknown_source_miss()
)]
fn ingress_demux_256_datagrams(mut fixture: IngressRoutingBenchFixture) -> usize {
    black_box(fixture.route_datagrams())
}

#[library_benchmark(config = packet_sink_config())]
#[bench::recording_sink_512_turns(PacketSinkFanoutBenchFixture::recording_sink())]
fn packet_sink_fanout_512_turns(mut fixture: PacketSinkFanoutBenchFixture) -> usize {
    black_box(fixture.route_sink_turns())
}

#[library_benchmark(config = route_control_config())]
#[bench::selected_rid_256_destinations(RidReadinessBenchFixture::pending_selected_rid())]
fn selected_rid_readiness_256_destinations(mut fixture: RidReadinessBenchFixture) -> usize {
    black_box(fixture.activate_selected_rid())
}

#[library_benchmark(config = route_control_config())]
#[bench::remote_source_512_requests(KeyframeCoalescingBenchFixture::remote_source_requests())]
fn keyframe_coalescing_512_requests(mut fixture: KeyframeCoalescingBenchFixture) -> usize {
    black_box(fixture.flush_requests())
}

library_benchmark_group!(
    name = packet_loop_callgrind;
    benchmarks =
        route_planning_1024_turns,
        relay_mailbox_256_attempts,
        ingress_demux_256_datagrams,
        packet_sink_fanout_512_turns,
        selected_rid_readiness_256_destinations,
        keyframe_coalescing_512_requests
);

main!(library_benchmark_groups = packet_loop_callgrind);
