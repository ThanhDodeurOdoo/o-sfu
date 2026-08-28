# M3. Choose the simplest clear design

Prefer direct idiomatic Rust with few concepts and special cases. Remove
obsolete code and structure. Name non-obvious decisions instead of hiding them
in dense expressions.

Add custom algorithms, caches, bit tricks, `unsafe` or extra state only after
profiling finds a hot path and a production-shaped benchmark proves a useful
repeatable gain. Check correctness, security and resource bounds separately. See
[R5](r05-keep-media-hot-paths-cheap.md).

Keep a simple reference implementation in verification code when practical.
Document only trade-offs the code cannot express.

> [!NOTE]
> More read: **[complexity in Google's engineering practices](https://google.github.io/eng-practices/review/reviewer/looking-for.html#complexity)**.
>
> partially enforced with: [cognitive_complexity](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#cognitive_complexity),
> [complexity::excessive_nesting](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#excessive_nesting),
> [complexity::type_complexity](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#type_complexity)
> and [pedantic::too_many_lines](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#too_many_lines).

**Example:** `RoomState` derives its user count from the map that owns the
users. It does not maintain a second counter.

**Avoid**

```rust
pub struct RoomState {
    users: BTreeMap<UserId, ActiveUser>,
    // Every insert and removal must now update this field too.
    user_count: usize,
}

impl RoomState {
    pub fn user_count(&self) -> usize {
        self.user_count
    }
}
```

**Prefer**

```rust
pub struct RoomState {
    users: BTreeMap<UserId, ActiveUser>,
}

impl RoomState {
    pub fn user_count(&self) -> usize {
        // The users map remains the single source of truth for the count.
        self.users.len()
    }
}
```

**Rationale:** Simpler designs are easier to understand and change. Add
complexity only when it expresses required behavior or proves a measured
performance gain.
