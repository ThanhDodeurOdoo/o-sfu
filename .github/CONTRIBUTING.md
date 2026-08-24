# Contributing

> [!WARNING]
> AI policy
>
> Trivial changes are allowed (rewording docstring, basic autocompletion,...)
>
> Non-Trivial changes written by AI must be disclosed
>
> The author must always understand all the added code and can justify the changes (replying with copy-pasted AI responses does not count).

## Learning Rust

### videos
- ["Considering Rust" by Jon Gjengset](https://www.youtube.com/watch?v=DnT-LUQgc7s)
- ["Rust for Everyone!" by Will Crichton](https://www.youtube.com/watch?v=R0dP-QR5wQo)
- ["The Rust Programming Language" by Aaron Turon](https://youtu.be/O5vzLKg7y-k)
- ["Crust of Rust async/await" by Jon Gjengset](https://www.youtube.com/watch?v=ThjvMReOXYM)
- ["Rust makes cents" by No Boilerplate](https://www.youtube.com/watch?v=4dvf6kM70qM)

### guides
- [The Rust Book](https://doc.rust-lang.org/book/)
- [Comprehensive Rust by Google](https://google.github.io/comprehensive-rust)

### references
- [Canonical's rust best practices](https://canonical.github.io/rust-best-practices/)
- [The Rustonomicon (unsafe/advanced)](https://doc.rust-lang.org/nomicon/)
- [Rust cheat sheet](https://cheats.rs/#data-structures)
- [Clippy](https://rust-lang.github.io/rust-clippy/)
- [Rust by Example](https://doc.rust-lang.org/rust-by-example/)
- [Rust Cookbook](https://github.com/rust-lang-nursery/rust-cookbook/)
- [Rust Atomics and Locks](https://mara.nl/atomics/)
- [The Tokio doc](https://docs.rs/tokio/latest/tokio/)
- [Idiomatic Rust snippets](https://idiomatic-rust-snippets.org/)
- [The Rust Performance Book by N. Nethercote](https://nnethercote.github.io/perf-book/introduction.html)

## Style guidelines

### General Rules
(some of the rules are enforced by lint like clippy)

- **No Low-Value Comments**: Avoid trivial comments that describe obvious code or that is just a rephrase of a function or variable name. Only write comments for necessary complex logic or obscure implementation / standard docstring / header comment (for files that need a global explanation).
- **Justify Overrides**: Any override of a linter rule MUST be justified with a descriptive comment.
- **Avoid literals**: like magic numbers or string literals, use constants, static vars or enums with a meaningful name instead.
- **Document non-obvious code**: If you couldn't write the simple/naive/obvious approach, add a comment to explain it. This also applies to fixing bugs, often bugs exist because the naive approach was not good enough in a non-obvious way, which should be commented.
- **Document unhandled errors**: Errors that are thrown, or `Result` types in Rust, must have their errors documented.
- **Tests**: Every new feature must include corresponding tests (meaningful tests, not noisy trivial checks) / proof / fuzzing / ... (depending on the changes).
- Failing the performance CI isn't necessarily breaking, but the commit message should include a `performance` section that justifies why.

### Rust

> [!TIP]
> Use "rust-analyzer" with your IDE,
>
> You can also add these to your rust-analyzer settings (VSCode settings example):
> ```
> "rust-analyzer.check.command": "clippy",
> "rust-analyzer.rustfmt.extraArgs": [
>   "+nightly"
> ],
> ```
> rustfmt nightly is only used to format imports nicely, but it is not enforced in the CI.

- **Formatting**: `cargo +nightly fmt`, Always run it before committing (we use nightly for the import ordering). Our rules can be found at [rustfmt.toml](/rustfmt.toml), more information on the defaults can be found at the [rustfmt documentation](https://rust-lang.github.io/rustfmt/).
- **Linting**: Run `cargo clippy --locked --all-targets --all-features -- -D warnings`. We use Clippy with strict rules. The enforced rules can be found in [Cargo.toml](/Cargo.toml). See the [Clippy documentation](https://rust-lang.github.io/rust-clippy/) for explanations.
- **Justify overrides**: Any override of a rule MUST be justified with a "reason".

### TypeScript & JavaScript (Bundle)

- **No lazy typing**: The use of the `any` type is strictly forbidden. Use proper interfaces or types.
- **No double Assertions**: Avoid `as unknown as`. If you must use it, provide a justifying comment (it id jusifiable when the type is really unknown (eg: external API)).
- **Defined Assertions**: Use the `!` operator only when you are absolutely certain the value is neither `null` nor `undefined`. It may require a comment.
- **Enforce immutability**: When possible, enforce immutability (`as const` / `readonly`).

## Verification

Verification commands and the `tests/` layout are at [tests/README.md](/tests/README.md).

## Deployment

Deployment and local container usage are covered in [DEPLOYMENT.md](/DEPLOYMENT.md).
For Odoo development, refer to [Odoo SFU Dev Deployment Guide](/.github/odoo_setup.md).
