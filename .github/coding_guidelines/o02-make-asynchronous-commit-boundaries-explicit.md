# O2. Release state guards before async effects

Never hold a blocking lock guard across `.await`. Release room-state guards
before I/O or async effects. Read or update state, take out what the effect
needs then leave the guard's scope. If I/O must succeed first, run it unlocked
then reacquire the guard to revalidate and commit.

An async ordering guard may span `.await` only to prevent effects from
overtaking one another and only when awaited code cannot lock it directly or
through another call. `Room::source_policy_turn` is one example.

> [!NOTE]
> More read: **[Tokio's shared-state guidance](https://tokio.rs/tokio/tutorial/shared-state)** and **[the async `Mutex` contract](https://docs.rs/tokio/latest/tokio/sync/struct.Mutex.html)**.
>
> partially enforced with: [significant_drop_tightening](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#significant_drop_tightening),
> [suspicious::await_holding_lock](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#await_holding_lock)
> and [suspicious::await_holding_refcell_ref](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#await_holding_refcell_ref).

The [publication lifecycle test](../../crates/core/src/engine/room/TESTS/producer_tests/publish_lifecycle.rs)
checks that a competing transition waits for the ordered turn to finish.

**Example:** `Room::update_user_info` takes the commit out of the state block
and runs its effects after the guard is gone.

**Avoid**

```rust
let mut state = self.state.write().await;
if let Some(commit) = state.apply_presence_update(...) {
    // The room-state guard remains held while the effect waits.
    RoomEffects::from_presence(commit).execute(...).await;
}
```

**Prefer**

```rust
let commit = {
    let mut state = self.state.write().await;
    state.apply_presence_update(...)
};
// The block released the room-state guard before the effect can suspend.
if let Some(commit) = commit {
    RoomEffects::from_presence(commit).execute(...).await;
}
```

**Rationale:** Releasing state guards avoids blocking waiters and lock-order
cycles. A dedicated ordering guard preserves effects that must not overtake each
other.
