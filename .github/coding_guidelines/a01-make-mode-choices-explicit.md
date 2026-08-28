# A1. Use booleans only for clear facts

Use a `bool` only for an independent fact evident from the function and
parameter names. Represent modes, policies and behavior choices with separate
operations or a semantic enum.

> [!NOTE]
> More read: **[custom argument types in the Rust API Guidelines](https://rust-lang.github.io/api-guidelines/type-safety.html#c-custom-type)**.
>
> partially enforced with: [pedantic::fn_params_excessive_bools](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#fn_params_excessive_bools)
> and [pedantic::struct_excessive_bools](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#struct_excessive_bools).

**Example:** `ProtocolCore::enqueue_envelope` takes `FlushMode`, so callers name
whether an envelope is sent immediately or may join a batch.

**Avoid**

```rust
// `true` does not say whether the response is sent now or may be batched.
core.enqueue_envelope(envelope, true)
```

**Prefer**

```rust
// The response must be sent now instead of joining a later batch.
core.enqueue_envelope(envelope, FlushMode::Immediate)
```

**Rationale:** Hidden boolean meanings make calls easy to misuse and difficult
to read at the callsite.
