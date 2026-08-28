# O4. Make cancellation behavior explicit

Treat each `.await` as a possible drop point. Before racing futures, identify
what changes before each `.await` and what dropping a loser does. Recreating a
losing future in a loop requires restart-safe cancellation or progress kept
outside the future. One-shot shutdown or timeout races may lose work only by
explicit call-site policy. Otherwise finish before honoring cancellation or
move the operation into a tracked task whose result is handled. See
[R6](r06-bound-externally-driven-work.md) for polling order and fairness.

> [!NOTE]
> More read: **[cancellation safety in Tokio](https://tokio.rs/tokio/tutorial/select#cancellation-safety)** and **[future execution in the Asynchronous Programming in Rust Book](https://rust-lang.github.io/async-book/02_execution/01_chapter.html)**.

**Example:** Once `RtcWorker::request_worker` enqueues a command, its caller
waits for the result before honoring later shutdown.

`ingress_should_stop` deliberately drops one received datagram when shutdown
wins because that one-shot shutdown policy accepts the loss.

**Avoid**

```rust
tokio::select! {
    () = shutdown.cancelled() => return,
    // Cancellation can win after enqueue, leaving the result unobserved.
    result = worker.request_worker(build_command) => handle(result),
}
```

**Prefer**

```rust
if shutdown.is_cancelled() {
    return;
}

// A caller that stays alive observes the accepted command before shutdown.
let result = worker.request_worker(build_command).await;
handle(result);
```

An accepted one-shot loss also stays visible in code:

```rust
let should_stop = tokio::select! {
    biased;
    // Shutdown intentionally wins over a backpressured datagram send.
    () = shutdown.cancelled() => true,
    send_result = tx.send(datagram) => send_result.is_err(),
};
```

**Rationale:** `select!` drops unfinished losing futures. Their local work stops.
Effects already handed to another task can continue with no caller left to
observe the result.
