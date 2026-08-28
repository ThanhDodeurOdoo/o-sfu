# A4. Add abstractions only when they simplify callers

Add abstractions only to remove caller decisions, enforce invariants, isolate
external systems or capture reused contracts. Never add one only for future
flexibility, file organization or mocking. Prefer concrete APIs until
implementations reveal a shared contract. Scope trait bounds to the smallest
function or `impl`. Hide incidental one-use type parameters with
argument-position `impl Trait`. Remove replaced structure when refactoring.

> [!NOTE]
> More read: **[avoiding unnecessary interfaces in Google's Go Style Guide](https://google.github.io/styleguide/go/best-practices.html#avoid-unnecessary-interfaces)**.
>
> partially enforced with: [complexity::extra_unused_type_parameters](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#extra_unused_type_parameters).

**Example:** `MediaTransport::publish_media` hides worker selection, command
construction and channel dispatch behind one domain call.

**Avoid**

```rust
// Callers must coordinate worker selection, command creation and dispatch.
let worker_id = transport.select_worker(&session_key)?;
let command = TransportCommand::Publish(media_kind, rtp_parameters);
let media = transport.send_command(worker_id, command).await?;
```

**Prefer**

```rust
// One domain call encapsulates worker routing and internal messaging.
let media = transport
    .publish_media(&session_key, media_kind, &rtp_parameters)
    .await?;
```

**Rationale:** An abstraction must remove more complexity from callers than it
adds to the system.
