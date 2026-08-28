# O1. Never block an async executor

Keep blocking I/O, potentially blocking OS-thread joins and sustained CPU work
off async executors. Use async APIs when available. Use
[`spawn_blocking`](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html)
only for bounded blocking work that finishes on its own because started jobs
cannot be aborted. Long-running blocking work needs a dedicated OS thread with
a lifecycle owner and cooperative shutdown. Await completion asynchronously.
Do not poll `JoinHandle::is_finished` with `yield_now` for general completion
because Tokio may immediately repoll the task.

> [!NOTE]
> More read: **[Tokio's task documentation](https://docs.rs/tokio/latest/tokio/task/index.html#blocking-and-yielding)**, **[cooperative multitasking](https://en.wikipedia.org/wiki/Cooperative_multitasking)** and **[thread pool starvation](https://en.wikipedia.org/wiki/Starvation_\(computer_science\))**.

**Example:** After cooperative shutdown makes a dedicated worker's exit bounded,
move its blocking join off the async executor.

**Avoid**

```rust
async fn wait_for_shutdown(thread: thread::JoinHandle<()>) {
    // `thread.join()` blocks an executor worker until the OS thread exits.
    let _ = thread.join();
}

async fn poll_for_shutdown(thread: &thread::JoinHandle<()>) {
    // `yield_now` may repoll this task without advancing thread completion.
    while !thread.is_finished() {
        yield_now().await;
    }
}
```

**Prefer**

```rust
async fn join_worker(
    shutdown: &CancellationToken,
    thread: thread::JoinHandle<()>,
) -> Result<thread::Result<()>, tokio::task::JoinError> {
    shutdown.cancel();
    // Cooperative shutdown bounds the OS-thread join moved to the blocking pool.
    spawn_blocking(move || thread.join()).await
}
```

**Rationale:** Blocking an executor thread delays unrelated futures.
