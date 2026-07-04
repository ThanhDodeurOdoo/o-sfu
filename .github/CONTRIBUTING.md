# Contributing

If you want to make a PR that does substantial changes to the codebase, before wasting time writing too much code:

**You work at Odoo**: use our internal means of communication to reach me first.

**You are an external contributor**: open an [issue](https://github.com/ThanhDodeurOdoo/o-sfu/issues) to talk about it and to defend your idea first.

> [!WARNING]
> AI policy
>
> Trivial changes are allowed (rewording docstring, basic autocompletion,...)
>
> Non-Trivial changes written by AI must be disclosed
>
> The author must always understand all the added code and can justify the changes (replying with copy-pasted AI responses does not count).

## Learning resources

- [The Rust Book](https://doc.rust-lang.org/book/)
- [Comprehensive Rust by Google](https://google.github.io/comprehensive-rust)
- [Canonical's rust best practices](https://canonical.github.io/rust-best-practices/)
- [The Rustonomicon (unsafe/advanced)](https://doc.rust-lang.org/nomicon/)
- [Rust by Example](https://doc.rust-lang.org/rust-by-example/)
- [Rust Cookbook](https://github.com/rust-lang-nursery/rust-cookbook/)
- [Rust Atomics and Locks](https://mara.nl/atomics/)
- [The Tokio doc](https://docs.rs/tokio/latest/tokio/)
- [Idiomatic Rust snippets](https://idiomatic-rust-snippets.org/)
- ["The Rust Programming Language" by Aaron Turon (video)](https://youtu.be/O5vzLKg7y-k)
- ["Living with Rust Long-Term" by Jon Gjengset (video)](https://youtu.be/r35cBkPRNMI)
- ["Rust makes cents" by No Boilerplate (video)](https://www.youtube.com/watch?v=4dvf6kM70qM)

## Style guidelines

### General Rules
(some of the rules are enforced by lint like clippy)

- **No Low-Value Comments**: Avoid trivial comments that describe obvious code or that is just a rephrase of a function or variable name. Only write comments for necessary complex logic or obscure implementation / standard docstring / header comment (for files that need a global explanation).
- **Justify Overrides**: Any override of a linter rule MUST be justified with a descriptive comment.
- **Avoid literals**: like magic numbers or string literals, use constants, static vars or enums with a meaningful name instead.
- **Document unhandled errors**: Errors that are thrown, or `Result` types in Rust, must have their errors documented.
- **Tests**: Every new feature must include corresponding tests (meaningful tests, not noisy trivial checks) / proof / fuzzing / ... (depending on the changes).
- Failing the performance CI isn't necessarily breaking, but the commit message should include a `performance` section that justifies why.

### Rust

- **Formatting**: `cargo +nightly fmt`, Always run it before committing (we use nightly for the import ordering). Our rules can be found at [rustfmt.toml](../rustfmt.toml), more information on the defaults can be found at the [rustfmt documentation](https://rust-lang.github.io/rustfmt/).
- **Linting**: `cargo clippy --workspace --all-targets --all-features -- -D warnings`, We use Clippy with strict rules. The enforced rules can be found in [Cargo.toml](../Cargo.toml), see the [Clippy documentation](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html) for explanations.
- **Justify overrides**: Any override of a rule MUST be justified with a "reason".

### TypeScript & JavaScript (Bundle)

- **No lazy typing**: The use of the `any` type is strictly forbidden. Use proper interfaces or types.
- **No double Assertions**: Avoid `as unknown as`. If you must use it, provide a justifying comment (it id jusifiable when the type is really unknown (eg: external API)).
- **Defined Assertions**: Use the `!` operator only when you are absolutely certain the value is neither `null` nor `undefined`. It may require a comment.
- **Enforce immutability**: When possible, enforce immutability (`as const` / `readonly`).

## Verification

Verification commands and the `tests/` layout are at [tests/README.md](https://github.com/ThanhDodeurOdoo/o-sfu/blob/master/tests/README.md).

## Deployment

Deployment and local container usage are covered in [DEPLOYMENT.md](../DEPLOYMENT.md).
