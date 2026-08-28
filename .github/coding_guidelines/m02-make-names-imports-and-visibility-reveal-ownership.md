# M2. Use consistent names, explicit imports and narrow visibility

Use one term per concept. Name values by role and operations by action. Let
types and modules provide context. Use short names only when their scope makes
the meaning obvious. Simplify the boundary when a precise name becomes
sentence-like.

Use `as_` for cheap views, `to_` for conversions that retain the source and
`into_` for conversions that consume it.

See the Rust API Guidelines on [conversion
conventions](https://rust-lang.github.io/api-guidelines/naming.html#c-conv).

Use explicit production imports. Qualify generic names where context helps,
such as `h264::Profile` or `fmt::Result`. Keep items private and grant callers
only the visibility they need.

> [!NOTE]
> More read: **[visibility and privacy in the Rust Reference](https://doc.rust-lang.org/reference/visibility-and-privacy.html)**.
>
> partially enforced with: [absolute_paths](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#absolute_paths),
> [pedantic::enum_glob_use](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#enum_glob_use),
> [pedantic::similar_names](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#similar_names),
> [pedantic::struct_field_names](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#struct_field_names),
> [pedantic::wildcard_imports](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#wildcard_imports)
> and [style::wrong_self_convention](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#wrong_self_convention).

**Example:** `PublishedSources` supplies enough context for short field names.
Its imports and visibility also show exactly what the module uses and exposes.

**Avoid**

```rust
use std::collections::*;
use super::*;
use crate::engine::source_model::*;

pub struct PublishedSources {
    // The type name is repeated instead of naming each field's role.
    pub published_source_records_by_published_source_id:
        BTreeMap<PublishedSourceId, PublishedSource>,
    pub published_source_id_by_source_key:
        BTreeMap<SourceKey, PublishedSourceId>,
}
```

**Prefer**

```rust
// Explicit imports expose this module's concrete dependencies.
use std::collections::BTreeMap;

use super::{PublishedSource, SourceKey};
use crate::engine::source_model::PublishedSourceId;

pub(super) struct PublishedSources {
    // The type supplies context while visibility keeps the index in media_graph.
    records: BTreeMap<PublishedSourceId, PublishedSource>,
    id_by_key: BTreeMap<SourceKey, PublishedSourceId>,
}
```

**Rationale:** Consistent names make code easier to read and search. Explicit
imports show where names come from. Narrow visibility stops internal details
from becoming dependencies.
