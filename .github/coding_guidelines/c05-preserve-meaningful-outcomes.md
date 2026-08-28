# C5. Do not discard meaningful outcomes

Add `#[must_use]` when ignoring a function result or type value would skip a
state change, cleanup or decision. Handle or propagate `Result` and `Option`.
Do not discard errors with `.ok()`. Use enums for distinct outcomes. Use a
named type when several values form one outcome or a boolean would be
ambiguous. Reserve tuples for small obvious groups.

When the contract permits it, discard immediately with `let _ = expression`,
intentionally retain a value such as a lock guard until the end of the scope
with an `_`-prefixed binding or destroy an existing named binding immediately
with `drop(value)`. Explain non-obvious choices.

> [!NOTE]
> More read: **[the `must_use` attribute](https://doc.rust-lang.org/stable/core/attribute.must_use.html)** and **[ignored values in Rust patterns](https://doc.rust-lang.org/book/ch19-03-pattern-syntax.html#ignoring-values-in-a-pattern)**.
>
> partially enforced with: [unused_result_ok](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#unused_result_ok),
> [correctness::let_underscore_lock](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#let_underscore_lock),
> [pedantic::must_use_candidate](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#must_use_candidate)
> and [suspicious::let_underscore_future](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#let_underscore_future).

**Example:** `RoomEffects` is `#[must_use]` because discarding the batch skips its
transport, output and source-policy effects.

```rust
// Dropping `RoomEffects` would skip execution of the ordered effect plans below.
#[must_use = "room effect batches must be executed after the state transition commits"]
pub struct RoomEffects {
    policy_before_transport: bool,
    transport: RoomTransportPlan,
    output: RoomOutputPlan,
    source_policy: SourcePolicyTurn,
}
```

**Rationale:** Return values may carry unfinished work or a required decision.
Discarding them can hide incomplete work.
