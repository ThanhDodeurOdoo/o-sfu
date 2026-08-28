# R9. Prefer `expect` for lint exceptions

Fix real lint problems. Use `#[expect(...)]` to suppress intentional findings
and let `unfulfilled_lint_expectations` report obsolete exceptions.

Reserve `#[allow(...)]` for scopes where a lint need not appear in every build,
including configuration-dependent code or intentional crate or module policy.
Scope either attribute narrowly and name exact lints, not groups.

Each `expect` or `allow` needs `reason = "..."` explaining why the code is
correct and obeying the lint would make it worse, not repeating its name.

> [!NOTE]
> More read: **[lint attributes in the Rust Reference](https://doc.rust-lang.org/reference/attributes/diagnostics.html#lint-check-attributes)** and **[`allow_attributes` in Clippy](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#allow_attributes)**.
>
> partially enforced with: [allow_attributes_without_reason](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#allow_attributes_without_reason).

**Example:** `AvpStaticPayloadType` uses `#[repr(u8)]`, which makes its
conversion to `u8` lossless. The lint exception is local to `as_u8`:

**Avoid**

```rust
// This remains silent if the cast stops triggering the lint.
#[allow(clippy::as_conversions)]
pub const fn as_u8(self) -> u8 {
    self as u8
}
```

**Prefer**

```rust
// A removed cast leaves an unfulfilled expectation, so this exception expires.
#[expect(
    clippy::as_conversions,
    reason = "repr(u8) makes this enum-to-u8 cast lossless"
)]
pub const fn as_u8(self) -> u8 {
    self as u8
}
```

The [core-room integration tests](../../tests/core-room/tests/core_room.rs)
allow panic-based assertions as a test policy. No particular panic site must
remain:

```rust
// Any test may panic on failure, but no specific panic is expected to remain.
#![allow(
    clippy::panic,
    reason = "integration tests use panic-based assertions for clear failures"
)]
```

**Rationale:** Lint exceptions are easy to forget. `expect` exposes obsolete
exceptions and a specific `reason` lets reviewers judge the remaining tradeoff.
