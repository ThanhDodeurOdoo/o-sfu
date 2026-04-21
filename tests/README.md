# Verification

## Layout

- `tests/`: `o-sfu-tests` workspace crate for integration tests and shared harness code.
- `tests/tests/`: black-box integration targets.
- `tests/miri/`: dedicated Miri-friendly pure verification targets.
- `tests/loom/`: dedicated Loom model checks for narrow coordination contracts.
- `tests/src/support/`: shared integration harness helpers.
- `tests/benchmarks/`: root-package benchmark sources
- `tests/fuzz/`: cargo-fuzz package
- `tests/proofs/`: `o-sfu-proofs` workspace crate for Kani proofs.

## Default

Run from the repository root:

```bash
cargo fmt
cargo check -p o-sfu
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --release
npm --prefix client run verify
```

## CI-oriented Rust tests

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

## Dependency policy

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
```

## Concurrency tests

Run the Loom transport coordination model:

```bash
cargo test -p o-sfu-tests --test loom_source_policy_coordination --features loom-tests
```


## Benchmarks

```bash
cargo bench --bench rtc_udp_demux --features internal-benchmarks --no-run
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
cargo kani -p o-sfu-proofs --harness join_session_preserves_invariants
```
