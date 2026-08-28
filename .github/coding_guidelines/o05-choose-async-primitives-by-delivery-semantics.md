# O5. Match async primitives to delivery semantics

Choose by producer and receiver count, ordering, retained values and overload
behavior. `mpsc`: bounded ordered single-consumer queue. `watch`: replaceable
state. `oneshot`: one result. Use `broadcast` only with a lag policy.

`Notify` carries no data. Store the condition or work separately and check it
in a loop before waiting. `notify_one` retains at most one permit.
`notify_waiters` leaves none for future waiters. When concurrent consumers pair
shared work with `notify_one`, create, pin and enable each `Notified` before
checking the work.
See Tokio's [`Notify`
contract](https://docs.rs/tokio/latest/tokio/sync/struct.Notify.html).

Coalesce only replaceable updates sharing every semantic key, never ordered
transitions, acknowledgements or occurrence-sensitive effects.

> [!NOTE]
> More read: **[the `tokio::sync` overview](https://docs.rs/tokio/latest/tokio/sync/index.html#message-passing)**.

**Example:** `UserOutboundSender` uses bounded `mpsc` for ordered output and
`watch` for the latest terminal overflow state.

**Avoid**

```rust
// `watch` would overwrite ordered output before the receiver observes it.
messages: watch::Sender<Option<QueuedUserOutbound>>,
// Repeated terminal overflow snapshots do not need an ordered queue.
overflow: mpsc::Sender<UserOutboundOverflow>,
```

**Prefer**

```rust
// Every accepted output stays ordered until received or discarded by policy.
messages: mpsc::Sender<QueuedUserOutbound>,
// Only the latest terminal overflow state matters.
overflow: watch::Sender<Option<UserOutboundOverflow>>,
```

**Rationale:** Delivery behavior is application behavior. A mismatched primitive
can lose, duplicate or accumulate work.
