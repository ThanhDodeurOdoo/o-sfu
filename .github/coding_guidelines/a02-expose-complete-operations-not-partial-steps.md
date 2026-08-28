# A2. Hide ordered steps behind one operation

Hide order-sensitive validation, mutation, effects and cleanup behind one
operation. Keep its step methods inside the enforcing module. Define what
commits on success, survives failure and becomes externally visible. Do not
promise incidental internal order. The operation owner must verify every
behavior-sensitive order. Never assume iteration, scheduling or completion
order. Across `.await`, cancellation must preserve that contract. See
[O4](o04-make-cancellation-behavior-explicit.md).

> [!NOTE]
> More read: **[caller assumptions in Google's Building Secure and Reliable Systems](https://google.github.io/building-secure-and-reliable-systems/raw/ch06.html#system_architecture)**.

**Example:** `UserOutboundSender::send` reserves byte capacity, enqueues one
message, releases the reservation on failure and signals overflow when a
capacity limit is exceeded. Callers never sequence those steps.

**Avoid**

```rust
// Manual sequencing can leak byte capacity and bypass overflow signaling.
let bytes = outbound.queued_bytes();
sender.reserve_bytes(bytes)?;
sender.messages.try_send(QueuedUserOutbound { outbound, bytes })?;
```

**Prefer**

```rust
// `send` owns capacity accounting, enqueue cleanup and overflow signaling.
sender.send(outbound)?;
```

**Rationale:** One complete operation prevents skipped or misordered steps.
