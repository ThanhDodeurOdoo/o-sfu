# R8. Make tests prove real behavior

Test one contract through its narrowest production interface. Assert behavior a
no-op cannot produce, not fixture-derived values.

Make asynchronous tests deterministic: wait for readiness, control time and
synchronize concurrent steps. Use timeouts only for hangs or expected absence.
Never depend on execution order or shared process state.

Compatibility tests must cross production boundaries. Drive real Rust and
TypeScript producers and consumers. Check the built Odoo bundle against fixed
expectations written independently.

> [!NOTE]
> More read: **[async testing with paused time in Tokio](https://tokio.rs/tokio/topics/testing)**.

**Example:** `websocket_rejects_batches_over_protocol_envelope_limit` drives the
real WebSocket boundary then observes its close code and metrics.

**Avoid**

```rust
send_text_frame(&mut websocket, oversized_batch, "batch should send").await;
// This delay guesses when processing finished and can still race.
sleep(Duration::from_millis(50)).await;
assert_eq!(server.state.metrics.snapshot().ws_bus_parse_failures(), 1);
```

**Prefer**

```rust
send_text_frame(&mut websocket, oversized_batch, "batch should send").await;
// The peer-observed close frame is the readiness signal.
assert_eq!(
    read_close_code_promptly(&mut websocket).await,
    Some(CloseCode::Protocol),
);
assert_eq!(server.state.metrics.snapshot().ws_bus_parse_failures(), 1);
```

**Rationale:** A useful test fails on broken behavior, not scheduler timing or
private implementation changes.
