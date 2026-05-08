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

The `cargo test -p o-sfu-proofs` step used by PR CI is not Kani execution. It
compiles the proof crate and runs normal Rust tests, including the router drift
chec. The `#[kani::proof]` harnesses run only through `cargo kani` in the
formal-verification workflow or in a local proof run.

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

Run the production-router drift check for the proof model:

```bash
cargo test -p o-sfu-proofs --release router::drift_tests
```

Run one harness:

```bash
cargo kani -p o-sfu-proofs --harness session_teardown_clears_reverse_indices_and_dependents
```
