//! investigation-only Callgrind coverage for the current-thread packet loop
//!
//! this target complements the deterministic packet-loop slice gate
//! it runs a real worker task on a current-thread Tokio runtime and measures
//! mailbox-driven packet-loop turns after setup and warmup have already
//! completed
//!
//! the target is intentionally manual-only in CI
//! it can produce DHAT, cache simulation, branch simulation and flamegraph
//! artifacts without making pull requests depend on whole-worker noise
//!
//! this target owns the whole-worker investigation path
//! `packet_loop_callgrind` stays the PR comparison target because its slices are
//! small enough to compare across base and head
//! this file is for manual profiling when those slices point at a regression or
//! when scheduler and mailbox cost need a full worker context
//!
//! the measured window is explicit
//! `Callgrind` instrumentation starts after fixture setup and warmup
//! instrumentation stops before control returns to the generated harness

#![allow(
    clippy::exit,
    reason = "iai-callgrind's generated benchmark harness exits with the measured runner status"
)]

use std::{env, hint::black_box};

// github actions runs this manual target on x86_64 linux
// other targets compile no-op hooks so local checks do not require supported
// valgrind client requests
#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
use iai_callgrind::client_requests::callgrind::{start_instrumentation, stop_instrumentation};
use iai_callgrind::{
    Callgrind, EntryPoint, FlamegraphConfig, LibraryBenchmarkConfig, library_benchmark,
    library_benchmark_group, main,
};
use o_sfu_core::server::transport::benchmark_support::WorkerLoopBenchFixture;

#[cfg(not(all(target_arch = "x86_64", target_os = "linux")))]
fn start_instrumentation() {}

#[cfg(not(all(target_arch = "x86_64", target_os = "linux")))]
fn stop_instrumentation() {}

fn callgrind_worker_config() -> LibraryBenchmarkConfig {
    let mut callgrind = Callgrind::with_args(["--instr-atstart=no"]);
    callgrind.entry_point(EntryPoint::None);
    if env::var_os("O_SFU_CALLGRIND_FLAMEGRAPHS").is_some() {
        callgrind.flamegraph(FlamegraphConfig::default());
    }

    let mut config = LibraryBenchmarkConfig::default();
    config.tool(callgrind);
    config
}

#[library_benchmark(config = callgrind_worker_config())]
#[bench::active_speaker_snapshot(WorkerLoopBenchFixture::command_driven_current_thread())]
fn worker_command_roundtrips(fixture: WorkerLoopBenchFixture) -> usize {
    start_instrumentation();
    let result = fixture.run_command_roundtrips();
    stop_instrumentation();
    black_box(result)
}

library_benchmark_group!(
    name = packet_loop_worker_callgrind;
    benchmarks = worker_command_roundtrips
);

main!(library_benchmark_groups = packet_loop_worker_callgrind);
