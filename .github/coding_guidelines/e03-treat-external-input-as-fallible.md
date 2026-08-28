# E3. Validate external input before use

Validate external data before domain use. Parsing proves structure, not
identity. Unverified fields may choose verification keys but never authorize
access. After verification, bind the signed identity to the selected resource
before constructing a trusted type. Classify outcomes as invalid input,
unsupported behavior or internal failure.

Validate each logical input before mutation. For intentional partial
success, return per-item outcomes. External input cannot reach `panic!`,
`.unwrap()`, `.expect()` or unchecked indexing. A scoped `#[expect(...)]`
requires a locally proven invariant recorded in `reason`.

> [!NOTE]
> More read: **[argument validation in the Rust API Guidelines](https://rust-lang.github.io/api-guidelines/dependability.html#functions-validate-their-arguments-c-validate)** and **[fallible conversion with `TryFrom`](https://doc.rust-lang.org/std/convert/trait.TryFrom.html)**.
>
> partially enforced with: [expect_used](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#expect_used),
> [indexing_slicing](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#indexing_slicing),
> [panic](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#panic)
> and [unwrap_used](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#unwrap_used).

**Example:** `decode_envelope_batch` validates JSON and batch size before
constructing an `EnvelopeBatch`.

**Avoid**

```rust
// Malformed peer input panics the task.
let batch = serde_json::from_str::<Vec<WireEnvelope>>(payload).unwrap();
```

**Prefer**

```rust
// Reject malformed or oversized input before it enters domain state.
let wire_batch = serde_json::from_str::<Vec<WireEnvelope>>(payload)
    .map_err(|_error| EnvelopeBatchDecodeError::InvalidJson)?;
if wire_batch.len() > limit {
    return Err(EnvelopeBatchDecodeError::BatchTooLarge {
        actual: wire_batch.len(),
        limit,
    });
}
```

**Rationale:** External data cannot be assumed to satisfy internal invariants,
so validation keeps failures explicit and contained.
