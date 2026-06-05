//! deterministic Callgrind coverage for one realistic room-level call flow
//!
//! the setup builds the RTC transport and an empty room outside the measured
//! function. the measured flow then drives joins, readiness, publication,
//! subscription, VAD observations, source-policy refreshes and route inspection
//! through the same core room and media transport boundaries used by runtime code

#![allow(
    clippy::exit,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value,
    reason = "Gungraun's generated harness owns setup values, returns measured outputs and exits with the runner status"
)]

mod general_call;

use std::hint::black_box;

use general_call::GeneralCallFixture;
use gungraun::{
    Callgrind, EventKind, LibraryBenchmarkConfig, library_benchmark, library_benchmark_group, main,
};

const CALLGRIND_CACHE_SIM: &str = "--cache-sim=yes";

fn callgrind_config(soft_limit: f64) -> LibraryBenchmarkConfig {
    let mut callgrind = Callgrind::with_args([CALLGRIND_CACHE_SIM]);
    callgrind.soft_limits([
        (EventKind::Ir, soft_limit),
        (EventKind::EstimatedCycles, soft_limit),
    ]);
    callgrind.fail_fast(false);

    let mut config = LibraryBenchmarkConfig::default();
    config.tool(callgrind);
    config
}

#[library_benchmark(config = callgrind_config(2.0))]
#[bench::mix_10s(GeneralCallFixture::new())]
fn room_flow(fixture: GeneralCallFixture) -> usize {
    black_box(fixture.run_total_work())
}

library_benchmark_group!(
    name = general_call_callgrind;
    benchmarks = room_flow
);

main!(library_benchmark_groups = general_call_callgrind);
