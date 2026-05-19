#![allow(
    clippy::exit,
    reason = "iai-callgrind's generated benchmark harness exits with the measured runner status"
)]

use std::hint::black_box;

use iai_callgrind::{
    Callgrind, EventKind, LibraryBenchmarkConfig, library_benchmark, library_benchmark_group, main,
};
use o_sfu_core::server::transport::benchmark_support::{
    FanoutBenchTopology, RelayPressureBenchFixture,
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

library_benchmark_group!(
    name = packet_loop_callgrind;
    benchmarks = route_planning_1024_turns, relay_mailbox_256_attempts
);

main!(library_benchmark_groups = packet_loop_callgrind);
