# C1. Let the compiler enforce correctness

Make invalid values, states and operations unrepresentable. Use newtypes rather
than aliases when distinct concepts share a representation. Validate
construction behind private fields. Use enums, not booleans, for exclusive
states or mode-dependent fields. Reserve `bool` for isolated properties and
`bitflags` for orthogonal, freely composable options.

Encode permissions and ownership in types rather than runtime flags: cloneable
sender versus single-consumer receiver, read versus write handle and
generational keys for recycled slots.

> [!NOTE]
> More read: **[the newtype pattern in The Rust Book](https://doc.rust-lang.org/book/ch20-03-advanced-types.html#type-safety-and-abstraction-with-the-newtype-pattern)** and **[typestate programming in The Embedded Rust Book](https://docs.rust-embedded.org/book/static-guarantees/typestate-programming.html)**.
>
> partially enforced with: [pedantic::struct_excessive_bools](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#struct_excessive_bools).

**Example:** `PublishedSources::source` and
`PublishedSourceDescriptor::encoding` accept different ID types.
`SourceSelector` makes each selection mode a separate variant:

**Avoid**

```rust
// These fields allow contradictory states such as `true` with `Some(_)`.
struct SourceSelector {
    open: bool,
    encoding_id: Option<u64>,
}
```

**Prefer**

```rust
// Every variant represents one legal selection mode.
pub enum SourceSelector {
    Open,
    Encoding(SourceEncodingId),
}
```

Use typestate only when it simplifies validation at call sites. See the [Rust API
Guidelines](https://rust-lang.github.io/api-guidelines/dependability.html#functions-validate-their-arguments-c-validate).

**Rationale:** Correctly modelized types prevent invalid states and incorrect API calls from compiling.
