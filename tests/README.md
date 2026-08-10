# Tests

## dir

  - `tests/benchmarks/`: The Gungraun (Valgrind/Callgrind) perf benchmarks
  - `tests/integration/`: integration tests
  - `tests/core-room/`: broad room lifecycle, spillover and subscription
    coverage through the public `testing-transport` harness
  - `tests/src/support/`: cross-crate integration harnesses for real server
    entry points, fake peers, websocket clients, and polling predicates
  - `tests/miri/`: UB tests
  - `tests/fuzz/`: opt-in cargo-fuzz targets
  - `tests/proofs/`: formal verification

## Default

when testing locally, run the following baseline. GitHub Actions covers the
remaining checks.

```bash
cargo +nightly fmt
cargo check --locked
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --release
npm --prefix crates/client run verify
```

## Fuzzing

the fuzz package shares the root `Cargo.lock` but is outside
`workspace.default-members`. its dependencies and binaries require the
`fuzz-targets` feature.

use cargo-fuzz for every fuzz target build because it supplies `cfg(fuzzing)`
to the core dependency graph.

type-check every fuzz target from the repository root:

```bash
cargo +nightly-2026-04-01 fuzz check --fuzz-dir tests/fuzz --features fuzz-targets
```

run one target explicitly:

```bash
cargo +nightly-2026-04-01 fuzz run --fuzz-dir tests/fuzz --features fuzz-targets protocol_decode
```

## Callgrind benchmarks

Build

```bash
cargo bench --locked -p o-sfu-tests --bench packet_loop_callgrind --no-run
cargo bench --locked -p o-sfu-tests --bench general_call_callgrind --no-run
```

Save a local baseline on a Valgrind-supported host:

```bash
cargo bench --locked -p o-sfu-tests --bench packet_loop_callgrind -- --save-baseline=local --save-summary=json
cargo bench --locked -p o-sfu-tests --bench general_call_callgrind -- --save-baseline=local --save-summary=json
```

Compare local changes against that baseline:

```bash
cargo bench --locked -p o-sfu-tests --bench packet_loop_callgrind -- --baseline=local --save-summary=json
cargo bench --locked -p o-sfu-tests --bench general_call_callgrind -- --baseline=local --save-summary=json
```

it requires `gungraun-runner` plus Valgrind. on
hosts without Valgrind support.
