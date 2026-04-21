# Verification

## Layout

- `tests/`: `o-sfu-tests` workspace crate for integration tests and shared harness code.
- `tests/tests/`: black-box integration targets.
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
