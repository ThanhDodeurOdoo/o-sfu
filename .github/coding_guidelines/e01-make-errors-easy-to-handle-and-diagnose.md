# E1. Preserve error categories and context

Use [`thiserror`](https://docs.rs/thiserror/latest/thiserror/) for concrete
errors that callers match. Map dependency errors at boundaries, excluding their
types from caller-facing domain APIs. Use `#[from]` only when every source error
has the same meaning. Otherwise retain causes with `#[source]` and construct
the caller-visible variant.

Use [`anyhow`](https://docs.rs/anyhow/latest/anyhow/) where failures are reported
rather than matched, such as startup and configuration. Add `Context` only for
an otherwise-unknown operation or resource. Keep chains short, state facts once
and omit secrets.

> [!NOTE]
> More read: **[error types in the Rust API Guidelines](https://rust-lang.github.io/api-guidelines/interoperability.html#error-types-are-meaningful-and-well-behaved-c-good-err)**, **[`thiserror` attributes](https://docs.rs/thiserror/latest/thiserror/#details)**, **[`anyhow::Context`](https://docs.rs/anyhow/latest/anyhow/trait.Context.html)** and **[`Error::source` in the standard library](https://doc.rust-lang.org/std/error/trait.Error.html#error-source)**.
>
> partially enforced with: [map_err_ignore](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#map_err_ignore)
> and [style::result_unit_err](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#result_unit_err).

**Example:** Callers match `RoomManagerJoinError` variants.
`Env::var(...).required()` uses `anyhow::Context` to name a missing variable.

**Typed domain error**

```rust
// Callers match variants instead of parsing display strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RoomManagerJoinError {
    #[error("room not found")]
    MissingRoom,
    #[error("room is full")]
    RoomFull,
    #[error("router state error")]
    RouterState,
}
```

**Boundary context**

```rust
// The report names the exact setting that blocked startup.
let value = self
    .load()
    .with_context(|| format!("{} env variable is required", self.key))?;
```

**Rationale:** Typed errors let callers respond correctly while contextual
reports make failures diagnosable.
