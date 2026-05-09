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
npm --prefix client run verify
```

## CI tests

The default CI test workflow uses `cargo nextest` for `o-sfu-cluster`,
`o-sfu-core`, `o-sfu-protocol`, `o-sfu-rfc` plus `o-sfu-router`. The root
`o-sfu` crate plus the `o-sfu-tests` integration crate and doctests stay on
plain `cargo test`.

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

## Dependency check

Install once:

```bash
cargo install cargo-deny --locked
```

Run the enforced policy checks:

```bash
cargo deny check advisories bans sources
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
cargo +nightly miri test -p o-sfu-tests --test miri_packet_loop_core
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
cargo +nightly fuzz run packet_loop_demux
cargo +nightly fuzz run packet_loop_turn_trace
cargo +nightly fuzz run sdp_answer
```

## Proofs

Kani proofs are formal-verification checks. They are separate from the normal
`cargo test -p o-sfu-proofs` and compile checks used by PR CI.

Install once:

```bash
cargo install --locked kani-verifier
cargo kani setup
```

Run all proofs:

```bash
cargo kani -p o-sfu-proofs
```

Run the packet-loop proofs against production `o-sfu-core` exports:

```bash
cargo kani -p o-sfu-proofs --features packet-loop-proofs \
  --harness packet_loop_recent_miss_cache_is_exact_for_recorded_packet \
  --harness packet_loop_topology_invalidation_clears_recent_miss_cache \
  --harness packet_loop_route_success_clears_recent_miss_cache \
  --harness packet_loop_keyframe_kind_coalescing_prefers_fir \
  --harness packet_loop_scratch_clear_removes_staged_work_and_keeps_capacity
```

Run the production-router drift check for the proof model:

```bash
cargo test -p o-sfu-proofs --release router::drift_tests
```

Run one harness:

```bash
cargo kani -p o-sfu-proofs --harness session_teardown_clears_reverse_indices_and_dependents
cargo kani -p o-sfu-proofs --features packet-loop-proofs --harness packet_loop_recent_miss_cache_is_exact_for_recorded_packet
```
