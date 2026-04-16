[![Tests](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/tests.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/tests.yml)
[![Client](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/client.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/client.yml)
[![Client Browser](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/client-browser.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/client-browser.yml)
[![Fuzzing](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/fuzzing.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/fuzzing.yml)
[![Formal Verification](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/formal-verification.yml/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/formal-verification.yml)
[![CodeQL](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/ThanhDodeurOdoo/o-sfu/actions/workflows/github-code-scanning/codeql)

# o-sfu

> [!WARNING]  
> NOT PRODUCTION READY! This repo is mostly made for experimenting with ideas. The readme may not be up to date, or be incorrect.
> Everything is up for refactor, some files are just testing prototypes.

## Development

Run the regular workspace checks from the repository root:

```bash
cargo fmt
cargo check -p o-sfu
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
npm --prefix client run verify
```

## Container image

Build the server image from the repository root with:

```bash
docker build --tag o-sfu:local .
```

Run a local container with the minimum required auth key:

```bash
docker run --rm \
  -p 8080:8080 \
  -e AUTH_KEY=dev-secret \
  o-sfu:local
```

For real RTC traffic, also expose the UDP worker range and provide the advertised public IP:

```bash
docker run --rm \
  -p 8080:8080 \
  -p 40000-49999:40000-49999/udp \
  -e AUTH_KEY=dev-secret \
  -e PROXY=true \
  -e TRANSPORT_BACKEND=rtc \
  -e PUBLIC_IP=203.0.113.10 \
  o-sfu:local
```

parser/auth fuzzing is in `fuzz/` crate.
 Install `cargo-fuzz` separately:

```bash
rustup toolchain install nightly
cargo install cargo-fuzz
```

Then run the current target from the repository root:

```bash
cargo +nightly fuzz run native_decode
```

If you only need to verify that the fuzz target still builds after changing the
fuzz boundary, run:

```bash
cargo check --manifest-path fuzz/Cargo.toml
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

## TODO cleanup later

- The router and rfc sections may be split into separate crates later (but too annoying for now)

## random thoughts

## Recording:

the o-sfu architecture helps a lot with recording compared to the previous version, since we now have complete control over the rtp packet dispatch, don't have to pipe streams through a transport layer and use ports and ffmpeg (at the real time recording step). we can just write packet frames to the disk directly and bypass all that old boilerplate.
another advantage is the router/recording topology, we have recording nodes that should just act as "opaque" media consuming "entities" and their locality shouldn't matter much so recording and forwarding could be physically separated.

also the recording feature on the official repo is still in active development so the API may change, and this repo
will adapt accordingly.
