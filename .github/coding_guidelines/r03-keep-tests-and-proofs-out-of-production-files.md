# R3. Keep tests and proofs out of production files

Exclude verification bodies from production files. Put tests and support in
nearest `TESTS/`, private-state Kani proofs in nearest `PROOFS/` behind
`#[cfg(kani)]` and public-API proofs in
[`tests/proofs/`](../../tests/proofs/). Production crates cannot depend on
`o-sfu-proofs`. Rustdoc examples stay beside APIs. `#[cfg(kani)]` only gates
compilation. Claim proof coverage only from an explicit Kani harness run.

Keep only gated module declarations and narrow cross-crate hooks in production
files. Add a hook only when setup or observation must pass through the
production owner. It must not bypass that owner or alter ordering, backpressure
or cleanup.

> [!NOTE]
> More read: **[conditional compilation in the Rust Reference](https://doc.rust-lang.org/reference/conditional-compilation.html)** and **[Kani proof harness usage](https://model-checking.github.io/kani/usage.html)**.

**Layout:**

```text
o-sfu/
|-- crates/core/src/engine/media_transport/rtc/codec/
|   |-- vp8.rs                 (production)
|   |-- TESTS/
|   |   `-- vp8.rs             (unit tests)
|   `-- PROOFS/
|       `-- vp8.rs             (private-state Kani proofs)
`-- tests/
    `-- proofs/                (proofs over public APIs)
```

**Example:** `vp8.rs` links its test and proof modules without containing their
bodies.

**Avoid**

```rust
// The cfg gate excludes verification from runtime builds but leaves it in this file.
#[cfg(test)]
mod tests {
    #[test]
    fn inline_test_body() {}
}

#[cfg(kani)]
mod proofs {
    #[kani::proof]
    fn inline_proof_body() {}
}
```

**Prefer**

```rust
// Path modules retain private-item access without inline verification bodies.
#[cfg(kani)]
#[path = "PROOFS/vp8.rs"]
mod proofs;

#[cfg(test)]
#[path = "TESTS/vp8.rs"]
mod tests;
```

**Rationale:** Separate files keep production modules focused and keep
verification scaffolding out of runtime code.
