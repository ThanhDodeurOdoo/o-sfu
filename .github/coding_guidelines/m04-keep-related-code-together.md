# M4. Keep related code together

Keep state, behavior, errors, constants and small helpers for one responsibility
in one module. Extract a module only when it has its own clear responsibility,
never merely to separate item kinds or shorten a file.

Place main types and entry points before private helpers. Keep inherent `impl`s
beside their types unless splitting a large owner by responsibility. Expose one
module-root interface.

**Example:** `SourceModelError` stays beside `PublishedSourceDescriptor` and
the constructor that returns it.

**Avoid**

```rust
// The error has no responsibility apart from descriptor construction.
mod descriptor;
mod errors;

pub use descriptor::PublishedSourceDescriptor;
pub use errors::SourceModelError;
```

**Prefer**

```rust
mod descriptor;

// Descriptor construction and SourceModelError remain one module responsibility.
pub use descriptor::{PublishedSourceDescriptor, SourceModelError};
```

**Rationale:** Related code is easier to understand and change when it is in
one place. Arbitrary splits add navigation without reducing complexity.
