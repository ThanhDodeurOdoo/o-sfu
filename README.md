[![UI](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/tests.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/tests.yml)
# o-sfu

- `router/`: isolated router crate for the core routing domain.
- `src/runtime.rs`: application bootstrap shell around the core crates.
- `src/signaling.rs` + `src/signaling/`: frozen bundle-facing contract types, current wire reference types, auth claims, transport/bootstrap primitives, ...
  The first replacement prototype keeps the current wire protocol under a bundle (that will be added to odoo codebase) contract so the Odoo-facing API stays stable while the server runtime is replaced

> [!WARNING]  
> Early phase of developments, the readme may not be up to date, or be incorrect.
>

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
