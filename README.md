[![UI](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/tests.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/tests.yml)
[![Formal Verification](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/formal-verification.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/formal-verification.yml)
# o-sfu

- `router/`: isolated router crate for the core routing domain.
- `src/runtime.rs`: application bootstrap shell around the core crates.
- `src/signaling.rs` + `src/signaling/`: frozen bundle-facing contract types, current wire reference types, auth claims, transport/bootstrap primitives, ...
  The first replacement prototype keeps the current wire protocol under a bundle (that will be added to odoo codebase) contract so the Odoo-facing API stays stable while the server runtime is replaced

> [!WARNING]  
> Early phase of developments, the readme may not be up to date, or be incorrect.
> Everything is up for massive refactor, some files are just testing prototypes

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

## TODO (formatting in a pretty way later)

- explain rfc files and how they act as reference for implementations using them
- explain the metrics system (intended to be compatible with openTelemetry)
- explain kani proofs
- explain router layer
- general architecture (mostly based on odoo sfu, for compatibility reason)

## Nice to have:
- Good docstring or "readme" for each feature directory (eg: websocket_server, web_rtc, http_server,...) explaining the role of each component

## TODO cleanup later

- "rust" and "tests" github workflows have overlap, "rust" exists because it's the default one from github, will probably remove later 
The router and rfc sections may be split into separate crates later (but too annoying for now)

## random thoughts

- having a "system" for directory layout:
  - component_name/ (like http_server/)
    - mod.rs for the main logic
    - technical.rs for technical but well encapsulated logic used by the main file (helper functions, things that are not important to see to understand the logic)
    - tests.rs for tests
    - verification/... for formal verification (or a root/verification/component_name, not sure)