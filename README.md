[![Tests](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/tests.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/tests.yml)
[![Client](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/client.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/client.yml)
[![Client Browser](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/client-browser.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/client-browser.yml)
[![Formal Verification](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/formal-verification.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/formal-verification.yml)
[![CodeQL](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/github-code-scanning/codeql)

# o-sfu

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
