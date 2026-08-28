# M5. Define shared decisions once

When several paths share a default, mapping, limit or policy, define the
decision once and derive every use from it. Keep similar code separate when
cases may evolve independently. Do not add a helper or macro merely because
snippets look alike.

**Example:** The default outbound byte budget depends on the queue length and
the largest broadcast payload.

**Avoid**

```rust
pub const MAX_BROADCAST_PAYLOAD_BYTES: usize = 16 * 1024;
pub const DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY: usize = 128;

// This repeats both inputs. Updating either input can leave this value stale.
pub const DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY: usize = 128 * 16 * 1024;
```

**Prefer**

```rust
pub const MAX_BROADCAST_PAYLOAD_BYTES: usize = 16 * 1024;
pub const DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY: usize = 128;

// Derive the byte budget from the queue and payload limits that define it.
pub const DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY: usize =
    DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY * MAX_BROADCAST_PAYLOAD_BYTES;
```

**Rationale:** A change should not depend on finding every copy of the same
decision. Keeping unrelated similarities separate avoids coupling behavior
that can evolve independently.
