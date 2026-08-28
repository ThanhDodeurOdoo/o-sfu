# O3. Own spawned work through shutdown

Every spawned task needs an explicit completion policy. `CancellationToken`
provides cooperative shutdown. `TaskTracker` observes task-set completion only.
Retain and await a `JoinHandle`, `AbortOnDropHandle` or `JoinSet` when output,
panic or owner-drop cancellation matters.

`TaskTracker::close` permits `wait` completion and later spawns, so stop
admission first. `JoinHandle` drop detaches, discarding output. Detach only with
owner-enforced cooperative shutdown when result/panic observation is
unnecessary. Without owner cancellation, require bounded work that needs no
shutdown cleanup and rejects stale effects.

See Tokio's [`TaskTracker`
contract](https://docs.rs/tokio-util/latest/tokio_util/task/struct.TaskTracker.html).

> [!NOTE]
> More read: **[graceful shutdown in Tokio](https://tokio.rs/tokio/topics/shutdown)** and **[the `JoinHandle` lifecycle](https://docs.rs/tokio/latest/tokio/task/struct.JoinHandle.html)**.
>
> partially enforced with: [suspicious::let_underscore_future](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#let_underscore_future).

**Example:** Spawn background tasks inside a `TaskTracker` then close, cancel and
wait after the owner stops admitting new tasks.

**Avoid**

```rust
// Detached task ignores shutdown signals and can outlive its owner.
tokio::spawn(async move {
    worker_loop().await;
});
```

**Prefer**

```rust
// The tracker observes completion while the child token carries shutdown.
let worker_shutdown = shutdown.child_token();
tracker.spawn(async move {
    worker_loop(worker_shutdown).await;
});

// The owner has stopped admitting new tasks.
tracker.close();
shutdown.cancel();
tracker.wait().await;
```

**Rationale:** Task ownership makes completion, failure and resource lifetime
observable at shutdown.
