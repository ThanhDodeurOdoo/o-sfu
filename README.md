# o-sfu

- `router/`: isolated router crate for the core routing domain.
- `src/runtime.rs`: application bootstrap shell around the core crates.
- `src/signaling.rs`: shared protocol entry point for the future client contract.

## Development

Run the regular workspace checks from the repository root:

```bash
cargo fmt
cargo test --workspace
cargo clippy --workspace --all-targets --all-features
```

Kani proofs are not run by `cargo test`. Install Kani separately:

```bash
cargo install --locked kani-verifier
cargo kani setup
```

Then run the proof harnesses with:

```bash
cargo kani -p o-sfu-router
```
