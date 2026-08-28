# C2. Handle behavior-changing variants and fields explicitly

Match every behavior-changing variant explicitly. Use `_` only when all omitted
cases, including future variants, are irrelevant. Apply `#[non_exhaustive]`
only to public types that may grow without breaking downstream crates. It does
not affect the defining crate.

> [!NOTE]
> More read: **[the non_exhaustive attribute in the Rust Reference](https://doc.rust-lang.org/reference/attributes/type_system.html#the-non_exhaustive-attribute)**.
>
> partially enforced with: [pedantic::match_wildcard_for_single_variants](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#match_wildcard_for_single_variants)
> and [style::manual_non_exhaustive](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#manual_non_exhaustive).

**Example:** An exhaustive `StreamType` match requires a decision for every
variant.

**Avoid**

```rust
// A future variant silently inherits camera behavior.
let (active, layout) = match stream_type {
    StreamType::Audio => (states.audio, None),
    _ => (states.camera, states.camera_layout),
};
```

**Prefer**

```rust
// A future variant cannot compile until its behavior is chosen.
let (active, layout) = match stream_type {
    StreamType::Audio => (states.audio, None),
    StreamType::Camera => (states.camera, states.camera_layout),
    StreamType::Screen => (states.screen, states.screen_layout),
};
```

When a new struct field must trigger review, destructure the struct without
`..`. This follows Canonical's [pattern matching discipline](https://canonical.github.io/rust-best-practices/pattern-matching-discipline.html#exhaustively-match-to-draw-attention).

**Rationale:** Exhaustive handling turns model changes into compile errors until
every affected behavior has been reviewed.
