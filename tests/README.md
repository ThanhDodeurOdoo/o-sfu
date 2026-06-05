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

Run from the repository root:

```bash
cargo fmt
cargo check --locked -p o-sfu
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --release
npm --prefix crates/client run verify
```

## Packet-loop Callgrind benchmarks

uses `gungraun` for deterministic instruction-count checks of
packet-loop hot-path slices

Build the target without running Valgrind:

```bash
cargo bench --locked -p o-sfu-tests --bench packet_loop_callgrind --no-run
```

Save a local baseline on a Valgrind-supported host:

```bash
cargo bench --locked -p o-sfu-tests --bench packet_loop_callgrind -- --save-baseline=local --save-summary=json
```

Compare local changes against that baseline:

```bash
cargo bench --locked -p o-sfu-tests --bench packet_loop_callgrind -- --baseline=local --save-summary=json
```

profiles and summaries are written under `target/gungraun`. the current scenarios
cover incoming packet observation, local route-planning fanout, relay-mailbox
enqueue pressure, cached ingress demux, repeated unknown-source misses,
large-packet recent-miss cache routing, ready-session scheduler churn,
routing-miss fingerprinting, packet-sink fanout, consumer packet-gate batches,
remote packet-gate retry under source-worker mailbox pressure, selected-RID
readiness, local RTP identity rewriting, active-speaker route control, keyframe
request tracking and pending keyframe retry drain
The `packet_cmd_mix` scenario also covers fanout packet sends interleaved with
worker lifecycle commands at a fixed ratio, so packet-loop publication changes
can be compared in the regular Callgrind table

local Callgrind execution requires `gungraun-runner` plus Valgrind. on
hosts without Valgrind support, use `--no-run` as the local build check and run
the baseline comparison on Linux

## CI tests

The default CI test workflow runs Rust checks as separate matrix entries so a
formatting, test, doctest, fuzz-build or clippy failure reports on its own job.
The `library nextest` entry uses locked `cargo nextest --profile ci` for
`o-sfu-core`, `o-sfu-model`, `o-sfu-protocol`, `o-sfu-rfc`, `o-sfu-router`,
`o-sfu-telemetry` plus `o-sfu-telemetry-macros`. It writes a
JUnit report to `target/nextest/ci/junit.xml`, which CI uploads as a short-lived
artifact. The root `o-sfu` crate plus the `o-sfu-tests` integration crate and
doctests stay on locked plain `cargo test`. Scheduled and manually dispatched CI
also runs `cargo test --locked --workspace --release` with default features,
matching the local default baseline without making every pull request pay the
release build cost.

Install

```bash
cargo install cargo-nextest --locked
```

Run the same split locally:

```bash
cargo nextest run --locked --profile ci -p o-sfu-core -p o-sfu-model -p o-sfu-protocol -p o-sfu-rfc -p o-sfu-router -p o-sfu-telemetry -p o-sfu-telemetry-macros
cargo test --locked -p o-sfu
cargo test --locked -p o-sfu-tests -p o-sfu-proofs
cargo test --locked --workspace --doc
```

## feature matrix

the feature-matrix CI workflow uses `cargo-hack` to catch optional-feature drift
outside the default workspace path

install

```bash
cargo install cargo-hack --version 0.6.44 --locked
```

run the CI matrix locally:

```bash
cargo hack --locked --package o-sfu --feature-powerset --depth 2 check --all-targets
cargo hack --locked --package o-sfu-core --feature-powerset --depth 2 check --all-targets
cargo check --locked --package o-sfu-core --all-targets --all-features
cargo hack --locked --package o-sfu-telemetry --feature-powerset --depth 2 check --all-targets
cargo check --locked --package o-sfu-telemetry --all-targets --all-features
cargo hack --locked --package o-sfu-protocol --each-feature check --all-targets
cargo hack --locked --package o-sfu-router --each-feature check --all-targets
cargo hack --locked --package o-sfu-tests --feature-powerset --depth 1 check --all-targets
cargo check --locked --package o-sfu-tests --all-targets --all-features
cargo check --locked --package o-sfu-proofs --all-targets --all-features
```

the matrix covers the root `otel-tracing` facade, core fuzzing,
`testing-transport`, internal benchmark and worker benchmark features, telemetry
macros and OpenTelemetry features, protocol verification models, router
test-support and proof-crate dependency features

Proof notes:

- `cargo test -p o-sfu-proofs` compiles proofs only
- `cargo kani` executes `#[kani::proof]` harnesses
- router Kani harnesses call the production `o_sfu_router::Router`
- Kani builds use router bounded proof storage
- normal builds use `std::collections::BTreeMap` plus `BTreeSet`
- `PR Formal Verification` runs
  `session_teardown_clears_reverse_indices_and_dependents` and protocol
  recovery-reset Kani shards on pull requests touching router, protocol, RFC or
  proof code
- scheduled formal verification runs one router proof per one-hour worker

## Dependency check

Install once:

```bash
cargo install cargo-deny --locked
```

Run the enforced policy checks:

```bash
cargo deny --locked check advisories bans licenses sources
mkdir -p target/cargo-deny
cargo metadata --manifest-path tests/fuzz/Cargo.toml --locked --format-version 1 > target/cargo-deny/fuzz-metadata.json
cargo deny check --metadata-path target/cargo-deny/fuzz-metadata.json advisories bans licenses sources
```

## Public API audit

Install once:

```bash
cargo install cargo-public-api --locked
```

Print the current supported and transitional public surface for review:

```bash
cargo public-api -p o-sfu-core --all-features --simplified
```

When a change intentionally adds or removes `o-sfu-core` public items, include
this output or an equivalent diff in the review context and update the
crate-level API docs when the item is part of the supported front door.

## UB tests

Install once:

```bash
rustup toolchain install nightly --component miri
cargo +nightly miri setup
```

Run the targeted Miri suite:

```bash
cargo +nightly miri test -p o-sfu-tests --test miri_router_protocol
MIRIFLAGS=-Zmiri-disable-isolation cargo +nightly miri test -p o-sfu-tests --test miri_auth_codec
cargo +nightly miri test -p o-sfu-tests --test miri_protocol_core
cargo +nightly miri test -p o-sfu-tests --test miri_rtp_negotiation
```

optional the big-endian subset (this one is one a cron):

```bash
sudo apt-get update && sudo apt-get install -y gcc-s390x-linux-gnu
rustup target add --toolchain nightly s390x-unknown-linux-gnu
MIRIFLAGS=-Zmiri-disable-isolation cargo +nightly miri test -p o-sfu-tests --target s390x-unknown-linux-gnu --test miri_auth_codec
cargo +nightly miri test -p o-sfu-tests --target s390x-unknown-linux-gnu --test miri_rtp_negotiation
```

## AddressSanitizer

AddressSanitizer complements Miri by running compiled native code. Use it for
runtime, networking, `str0m` and `aws-lc-rs` adjacent paths that Miri cannot
exercise. The suite is intentionally bounded to pure crate coverage plus exact
live-server smoke tests until broader sanitizer runtime coverage is stable.

Install once on Linux:

```bash
rustup toolchain install nightly-2026-04-01
rustup target add --toolchain nightly-2026-04-01 x86_64-unknown-linux-gnu
sudo apt-get update && sudo apt-get install -y llvm
```

Run the same environment used by CI:

```bash
export RUSTFLAGS="-Zsanitizer=address -C force-frame-pointers=yes"
export ASAN_OPTIONS="detect_leaks=0:halt_on_error=1:abort_on_error=1"
export RUST_BACKTRACE=1
export ASAN_SYMBOLIZER_PATH="$(command -v llvm-symbolizer)"
```

`detect_leaks=0` is intentional while the integration harness aborts server
tasks during cleanup. Re-enable leak detection only after those smoke tests have
graceful shutdown coverage.

Run the pure crate suite:

```bash
cargo +nightly-2026-04-01 test --locked --target x86_64-unknown-linux-gnu \
  -p o-sfu-model -p o-sfu-rfc -p o-sfu-router -p o-sfu-protocol \
  --lib --tests -- --test-threads=1 --nocapture
```

Run core runtime tests:

```bash
cargo +nightly-2026-04-01 test --locked --target x86_64-unknown-linux-gnu \
  -p o-sfu-core --lib --tests -- --test-threads=1 --nocapture
```

Run the exact live server smoke tests:

```bash
cargo +nightly-2026-04-01 test --locked --target x86_64-unknown-linux-gnu \
  -p o-sfu-tests --test server_smoke \
  websocket_welcome_and_initial_offer_work_from_integration_test \
  -- --exact --test-threads=1 --nocapture

cargo +nightly-2026-04-01 test --locked --target x86_64-unknown-linux-gnu \
  -p o-sfu-tests --test full_stack \
  video_routing::fake_rtc_peers_forward_vp8_high_rid_keyframe_without_browsers \
  -- --exact --test-threads=1 --nocapture

cargo +nightly-2026-04-01 test --locked --target x86_64-unknown-linux-gnu \
  -p o-sfu-tests --test full_stack \
  relay_spillover::fake_rtc_cross_worker_vp8_selected_rid_survives_relay \
  -- --exact --test-threads=1 --nocapture
```

## Fuzzing

Install once:

```bash
rustup toolchain install nightly
cargo install cargo-fuzz
```

Build-check the fuzz package against its committed lockfile:

```bash
cargo check --manifest-path tests/fuzz/Cargo.toml --locked
```

Run targets from `tests/fuzz/`:

```bash
cd tests/fuzz
cargo +nightly fuzz run protocol_decode
cargo +nightly fuzz run http_disconnect_auth
cargo +nightly fuzz run protocol_sequence
cargo +nightly fuzz run sdp_answer
cargo +nightly fuzz run packet_loop_ingress_demux
```

pull requests touching protocol, auth, websocket, HTTP origin, SDP projection or
fuzz code run one matrix job per target. pull request jobs use a 60 second fuzz
budget while scheduled and manually dispatched jobs use a 300 second budget per
target

## Proofs

Kani proofs are formal-verification checks. They are separate from the normal
`cargo test -p o-sfu-proofs` and compile checks used by PR CI.

Install once:

```bash
cargo install --locked kani-verifier
cargo kani setup
```

Run all scheduled proofs:

```bash
cargo kani -p o-sfu-proofs
```

Run one scheduled harness:

```bash
cargo kani -p o-sfu-proofs --harness h264_profile_level_id_parse_matches_rfc_patterns
```

Run one router proof:

```bash
cargo kani -p o-sfu-proofs \
  -Z unstable-options \
  --harness session_teardown_clears_reverse_indices_and_dependents
```
