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

## Verification

Verification commands and the `tests/` layout are at [tests/README.md](../tests/README.md).

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
