# Contributing

## Style guidelines

### General Rules

- **No Low-Value Comments**: Avoid trivial comments that describe obvious code or that is just a rephrase of a function or variable name. Only write comments for necessary complex logic or obscure implementation. Or the standard docstring.
- **Justify Overrides**: Any override of a linter rule or the use of `unsafe` code MUST be justified with a descriptive comment.
- **Avoid literals**: use constants or enums with a meaningful name instead.
- **Document unhandled errors**: Errors that are thrown, or `Result` types in Rust, must have their errors documented.

### Rust

We follow standard Rust idioms and enforce strict safety.

- **Formatting**: Always run `cargo fmt` before committing.
- **Linting**: We use Clippy with strict rules. The enforced rules can be found in [Cargo.toml](../Cargo.toml), see the [Clippy documentation](https://rust-lang.github.io/rust-clippy/rust-1.92.0/index.html) for explanations.
- **Unsafe Code**: Use of `unsafe` is discouraged. If absolutely necessary, it must be locally scoped (as narrow as possible) and justified.
- **Tests**: Every new feature must include corresponding tests.

## Validation

```bash
cargo fmt
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
npm --prefix client run verify
```

## Kani proofs

`cargo test` does not run Kani proof harnesses.
The proof harnesses in `router/src/proofs.rs` are separate and must be run with Kani:

```bash
cargo kani -p o-sfu-router
```

To run a single proof harness:

```bash
cargo kani -p o-sfu-router --harness join_session_preserves_invariants
```

If `cargo kani` is unavailable, install Kani before claiming proof coverage for a change.
