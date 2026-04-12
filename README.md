[![Tests](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/tests.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/tests.yml)
[![Client](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/client.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/client.yml)
[![Formal Verification](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/formal-verification.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/formal-verification.yml)
[![CodeQL](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/github-code-scanning/codeql)

# o-sfu

WORK IN PROGRESS, MAY CHANGE, MAY BE OPUT OF DATE
- `protocol/`: shraed bundle and signaling contract crate that feeds the server today and expose the first `wasm-bindgen` entry point for the future browser client layer (what odoo will interact with, like current odoo/sfu)
- `client/`: TypeScript browser-shell groundwork for the future bundle that can replace the current odoo sfu bundle
- `router/`: isolated router crate
- `src/runtime.rs`: application bootstrap shell around the core crates.
- `src/signaling.rs` + `src/signaling/`: server signaling side, auth claims, current wire reference types, and transiitonal re-exports for the extracted protocol crate.
  The first replacement prototype keeps the current wire protocol as a bundle (that will be added to odoo codebase) contract so the Odoo-facing API stays stable while the server runtime is repalced (ideally we just hot swap the client bundle, at least thats the goal...)

> [!WARNING]  
> Early phase of developments, the readme may not be up to date, or be incorrect.
> Everything is up for massive refactor, some files are just testing prototypes

## Development

Run the regular workspace checks from the repository root:

```bash
cargo fmt
cargo test --workspace
cargo clippy --workspace --all-targets --all-features
npm --prefix client run verify
```

TODO: copy the liting and formatting rules from odoo/sfu

Kani proofs are not run by `cargo test`. Install Kani separately:

```bash
cargo install --locked kani-verifier
cargo kani setup
```

Then run the proof harnesses with:

```bash
cargo kani -p o-sfu-router
```

## TODO cleanup later

- "rust" and "tests" github workflows have overlap, "rust" exists because it's the default one from github, will probably remove later 
The router and rfc sections may be split into separate crates later (but too annoying for now)

## random thoughts

## Recording:

the o-sfu architecture helps a lot with recording compared to odoo/sfu, since we now have complete control over the rtp packet dispatch, don't have to pipe streams through a transport layer and use ports and ffmpeg (at the real time recording step). we can just write packet frames to the disk directly and bypass all that old boilerplate.
another advantage is the router/recording topology, we have recording nodes that should just act as "opaque" media consuming "entities" and their locality shouldn't matter much
