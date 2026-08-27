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
- **Linting**: Run `cargo clippy --locked --all-targets --all-features`. We use Clippy with strict rules. The enforced rules can be found in [Cargo.toml](/Cargo.toml). See the [Clippy documentation](https://rust-lang.github.io/rust-clippy/) for explanations. Note that the CI will also deny warnings (`-D warnings`), some rules are put in warnings only for developer convenience when prototyping/drafting code, but no warning are accepted when merging.
- **Justify overrides**: Any override of a rule MUST be justified with a "reason".

### TypeScript & JavaScript (Bundle)

- **No lazy typing**: The use of the `any` type is strictly forbidden. Use proper interfaces or types.
- **No double Assertions**: Avoid `as unknown as`. If you must use it, provide a justifying comment (it id jusifiable when the type is really unknown (eg: external API)).
- **Defined Assertions**: Use the `!` operator only when you are absolutely certain the value is neither `null` nor `undefined`. It may require a comment.
- **Enforce immutability**: When possible, enforce immutability (`as const` / `readonly`).

## Verification

Verification commands and the `tests/` layout are at [tests/README.md](/tests/README.md).

## Commit guidelines

```
[TAG] module: describe your change in a short sentence (ideally < 50 chars)

Long version of the change description, including the rationale for the change,
or a summary of the feature being introduced.

Please spend a lot more time describing WHY the change is being done rather
than WHAT is being changed. This is usually easy to grasp by actually reading
the diff. WHAT should be explained only if there are technical choices
or decision involved. In that case explain WHY this decision was taken.

End the message with references, such as task or bug numbers, PR numbers, and
OPW tickets, following the suggested format:
task-123 (related to task)
Fixes #123  (close related issue on Github)
Closes #123  (close related PR on Github)
opw-123 (related to ticket)
```

possible `TAG`:
- `[FIX]` for bug fixes: mostly used in stable version but also valid if you are fixing a recent bug in development version;
- `[REF]` for refactoring: when a feature is heavily rewritten;
- `[REM]` for removing resources: removing dead code, removing views, removing modules, …;
- `[REV]` for reverting commits: if a commit causes issues or is not wanted reverting it is done using this tag;
- `[MOV]` for moving files: use git move and do not change content of moved file otherwise Git may loose track and history of the file; also used when moving code from one file to another;
- `[REL]` for release commits: new major or minor stable versions;
- `[IMP]` for improvements: most of the changes done in development version are incremental improvements not related to another tag;
- `[MERGE]` for merge commits: used in forward port of bug fixes but also as main commit for feature involving several separated commits;
- `[PERF]` for performance patches;
- `[DEP]` for changing dependencies;

possible `module`:
- any of the sub crates: `client`, `core`, `model`, `protocol`, `rfc`, `router`, `telemetry`
- `ci` for changing ci-related files like workflow `.yml` files.
- `doc` for the documentation files (typically `.md` files)
- `src` for anything in the `/src` sub dir, that's basically the root/orchestration/config layer of the SFU.
- nothing or `root` if updating a root file like `cargo.toml`.

(based on [Odoo's git guidelines](https://www.odoo.com/documentation/19.0/contributing/development/git_guidelines.html))

## Deployment

Deployment and local container usage are covered in [DEPLOYMENT.md](/DEPLOYMENT.md).
For Odoo development, refer to [Odoo SFU Dev Deployment Guide](/.github/odoo_setup.md).
