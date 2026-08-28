# R6. Limit work triggered by external input

Bound externally driven work, including work inside dependencies, by count,
bytes, time and identities. Every queue, batch, retry, loop and retained
collection needs an exhaustion policy: reject, disconnect, drop, coalesce or
backpressure.

Bound retained state by capacity and lifetime unless another limit proves its
maximum. Release or expire reservations, permits, per-origin buckets and cache
entries. Reserve capacity before accepting work. Keep the reservation while
that capacity is in use.

Limit each loop turn then recheck shutdown and control. `.await` and
`yield_now()` do not guarantee fairness. In biased `select!`, put shutdown and
control branches before high-volume input. Replace input-controlled recursion
with bounded work lists.

> [!NOTE]
> More read: **[handling overload in Google's SRE Book](https://sre.google/sre-book/handling-overload/)** and **[fairness in `tokio::select!`](https://docs.rs/tokio/latest/tokio/macro.select.html#fairness)**.

**Example:** The user outbound queue limits both message count and queued bytes.

**Avoid**

```rust
// A peer can grow this queue without limit.
let (outbound_tx, outbound_rx) = tokio::sync::mpsc::unbounded_channel();
```

**Prefer**

```rust
// `UserOutboundSender::send` rejects output before either limit is exceeded.
let limits = UserOutboundQueueLimits::new(
    outbound_queue_capacity,
    outbound_queue_byte_capacity,
);
let (outbound_tx, outbound_rx) =
    UserOutboundSender::channel_with_limits(limits, metrics);
```

**Rationale:** Without limits, one source can consume memory or CPU needed by
everyone else.
