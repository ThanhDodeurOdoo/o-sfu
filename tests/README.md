# Tests

## Layout

  - `tests/tests/`: integration tests
  - `tests/src/support/`: cross-crate integration harnesses for real server
    entry points, fake peers, websocket clients, and polling predicates
  - `tests/miri/`: UB tests
  - `tests/fuzz/`: fuzzing
  - `tests/proofs/`: formal verification

## Default

Run from the repository root:

```bash
cargo fmt
cargo check -p o-sfu
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --release
npm --prefix crates/client run verify
```

## Packet-loop Callgrind benchmarks

uses `iai-callgrind` for deterministic instruction-count checks of
packet-loop hot-path slices

Build the target without running Valgrind:

```bash
cargo bench -p o-sfu-tests --bench packet_loop_callgrind --no-run
```

Save a local baseline on a Valgrind-supported host:

```bash
cargo bench -p o-sfu-tests --bench packet_loop_callgrind -- --save-baseline=local --save-summary=json
```

Compare local changes against that baseline:

```bash
cargo bench -p o-sfu-tests --bench packet_loop_callgrind -- --baseline=local --save-summary=json
```

profiles and summaries are written under `target/iai`. the current scenarios
cover local route-planning fanout, relay-mailbox enqueue pressure, cached
ingress demux, repeated unknown-source misses, packet-sink fanout, selected-RID
readiness and keyframe-request coalescing

local Callgrind execution requires `iai-callgrind-runner` plus Valgrind. on
hosts without Valgrind support, use `--no-run` as the local build check and run
the baseline comparison on Linux

## CI tests

The default CI test workflow uses `cargo nextest` for `o-sfu-cluster`,
`o-sfu-core`, `o-sfu-protocol`, `o-sfu-rfc` plus `o-sfu-router`. The root
`o-sfu` crate plus the `o-sfu-tests` integration crate and doctests stay on
plain `cargo test`. Scheduled and manually dispatched CI also runs
`cargo test --workspace --release` with default features, matching the local
default baseline without making every pull request pay the release build cost.

Install

```bash
cargo install cargo-nextest --locked
```

Run the same split locally:

```bash
cargo nextest run -p o-sfu-cluster -p o-sfu-core -p o-sfu-protocol -p o-sfu-rfc -p o-sfu-router
cargo test -p o-sfu
cargo test -p o-sfu-tests
cargo test --workspace --doc
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

the matrix covers the root `otel-tracing` and `testing-transport` facade,
core fuzzing, internal benchmark and worker benchmark features, telemetry
macros and OpenTelemetry features, protocol verification models, router
test-support and proof-crate dependency features

Proof notes:

- `cargo test -p o-sfu-proofs` compiles proofs only
- `cargo kani` executes `#[kani::proof]` harnesses
- router Kani harnesses call the production `o_sfu_router::Router`
- Kani builds use router bounded proof storage
- normal builds use `std::collections::BTreeMap` plus `BTreeSet`
- pull requests touching router, protocol, RFC or proof code run the
  `session_teardown_clears_reverse_indices_and_dependents` and protocol
  recovery-reset Kani shards
- scheduled formal verification runs one router proof per one-hour worker

## Dependency check

Install once:

```bash
cargo install cargo-deny --locked
```

Run the enforced policy checks:

```bash
cargo deny check advisories bans licenses sources
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
```

pull requests touching protocol, auth, websocket, HTTP origin, SDP projection or
fuzz code run `protocol_decode`, `protocol_sequence`, `http_disconnect_auth`
and `sdp_answer` for 60 seconds each while scheduled and manually dispatched
fuzz runs keep the full 300 second budget per target

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
