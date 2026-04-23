# Tests

## Layout

  - `tests/tests/`: integration tests
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

The default CI test workflow use `cargo nextest` for the pure library
crates and keeps the root `o-sfu` crate, the `o-sfu-tests` integration crate
and doctests on plain `cargo test`.

Install

```bash
cargo install cargo-nextest --locked
```

Run the same split locally:

```bash
cargo nextest run -p o-sfu-protocol -p o-sfu-rfc -p o-sfu-router
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

Build-check the fuzz package:

```bash
cargo check --manifest-path tests/fuzz/Cargo.toml
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

Install once:

```bash
cargo install --locked kani-verifier
cargo kani setup
```

Run all proofs:

```bash
cargo kani -p o-sfu-proofs
```

Run one harness:

```bash
cargo kani -p o-sfu-proofs --harness session_teardown_clears_reverse_indices_and_dependents
```
