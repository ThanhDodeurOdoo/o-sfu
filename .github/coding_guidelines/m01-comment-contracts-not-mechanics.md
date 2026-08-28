# M1. Comment contracts, not mechanics

Use Rustdoc for meaning and contracts absent from names, types or signatures.
Use `//!` for subsystems, `///` for items and `//` for local reasoning beside
the code. Name identifiers and explain the invariant that rejects an obvious
alternative. Never narrate code.

Document every caller-visible failure. Rust public and boundary APIs need
`# Errors` for each `Err` condition and `# Panics` for each reachable panic.
TypeScript public APIs use `@throws` for exceptions. Promise-returning APIs
state rejection conditions. Omit empty sections and repeated return types.

> [!NOTE]
> More read: **[failure documentation in the Rust API Guidelines](https://rust-lang.github.io/api-guidelines/documentation.html#function-docs-include-error-panic-and-safety-considerations-c-failure)**, **[the rustdoc writing guide](https://doc.rust-lang.org/rustdoc/how-to-write-documentation.html)**, **[TypeDoc's `@throws` tag](https://typedoc.org/documents/Tags._throws.html)** and **[Google's code review guidance on comments](https://google.github.io/eng-practices/review/reviewer/looking-for.html#comments)**.
>
> partially enforced with: [pedantic::missing_errors_doc](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#missing_errors_doc),
> [pedantic::missing_panics_doc](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#missing_panics_doc)
> and [style::missing_safety_doc](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#missing_safety_doc).

**Example:** `next_generation` explains why `.max(1)` skips zero after
wraparound instead of narrating the arithmetic.

**Avoid**

```rust
fn next_generation(generation: u64) -> u64 {
    // Increment the generation and keep it non-zero.
    generation.wrapping_add(1).max(1)
}
```

**Prefer**

```rust
fn next_generation(generation: u64) -> u64 {
    // Keep generation 0 reserved for invalid handles after wraparound.
    generation.wrapping_add(1).max(1)
}
```

Add `# Safety` for unsafe caller obligations. Add `# Examples` when an example
prevents likely misuse. Put ordering, cancellation, protocol, compatibility,
safety and performance constraints at the boundary they govern. Comment
performance only when the reason is not obvious from the code.

Delete commented-out code. Keep compatibility fallbacks until the documented
removal condition in [R1](r01-preserve-compatibility-boundaries.md) is satisfied.

**Rationale:** Reasons prevent plausible regressions. Narration duplicates code
and can easily become stale and difficult to maintain.
