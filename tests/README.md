# Tests

## dir

  - `tests/benchmarks/`: The Gungraun (Valgrind/Callgrind) perf benchmarks
  - `tests/integration/`: integration tests
  - `tests/core-room/`: broad room lifecycle, spillover and subscription
    coverage through the public `testing-transport` harness
  - `tests/src/support/`: cross-crate integration harnesses for real server
    entry points, fake peers, websocket clients, and polling predicates
  - `tests/miri/`: UB tests
  - `tests/fuzz/`: fuzzing
  - `tests/proofs/`: formal verification

## Default

when testing locally, I recommend just doing this, the rest is more combersome and will be ran on github actions

```bash
cargo fmt
cargo check --locked -p o-sfu
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --release
npm --prefix crates/client run verify
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
