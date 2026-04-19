# Contributing

## Style guidelines

### General Rules

- **No Low-Value Comments**: Avoid trivial comments that describe obvious code or that is just a rephrase of a function or variable name. Only write comments for necessary complex logic or obscure implementation. Or the standard docstring.
- **Justify Overrides**: Any override of a linter rule or the use of `unsafe` code MUST be justified with a descriptive comment.
- **Avoid literals**: use constants or enums with a meaningful name instead. Magic numbers and strings, for example from RFCs have their dedicated rfc crate.
- **Document unhandled errors**: Errors that are thrown, or `Result` types in Rust, must have their errors documented.

### Rust

We follow standard Rust idioms and enforce strict safety.

- **Formatting**: Always run `cargo fmt` before committing.
- **Linting**: We use Clippy with strict rules. The enforced rules can be found in [Cargo.toml](../Cargo.toml), see the [Clippy documentation](https://rust-lang.github.io/rust-clippy/rust-1.92.0/index.html) for explanations.
- **Justify overrides**: Any override of a rule MUST be justified with a "reason".
- **Unsafe Code**: Use of `unsafe` is discouraged. If absolutely necessary, it must be locally scoped (as narrow as possible) and justified.
- **Tests**: Every new feature must include corresponding tests.


# Development

Run the regular workspace checks from the repository root:

```bash
cargo fmt
cargo check -p o-sfu
cargo clippy --workspace --all-targets --all-features -- -D warnings # you can skip warnings during dev if they are too strict but 0 warnings accepted in PRs (it must be instead locally overriden explicitely and justified)
cargo test --workspace --release
npm --prefix client run verify # runs all checks for the client, see the package.json for individual checks
```

## Container image

Build the server image from the repository root with:

```bash
docker build --tag o-sfu:local .
```

Run a local container by providing the auth key, the advertised RTC IP, and the UDP worker range:

```bash
docker run --rm \
  -p 8080:8080 \
  -p 40000-49999:40000-49999/udp \
  -e AUTH_KEY=dev-secret \
  -e PROXY=true \
  -e PUBLIC_IP=203.0.113.10 \
  o-sfu:local
```

The runtime always boots the RTC transport. The fake transport is
available for test and development workflows with the cfg flag
`cargo test -p o-sfu --features testing-transport`.

parser/auth fuzzing is in `fuzz/` crate.
 Install `cargo-fuzz` separately:

```bash
rustup toolchain install nightly
cargo install cargo-fuzz
```

Then run the current target from the repository root:
(i don't think there is a "fuzz all" command)
```bash
cargo +nightly fuzz run protocol_decode
cargo +nightly fuzz run http_disconnect_auth
cargo +nightly fuzz run protocol_sequence
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

To run a single proof harness:

```bash
cargo kani -p o-sfu-router --harness join_session_preserves_invariants
```
