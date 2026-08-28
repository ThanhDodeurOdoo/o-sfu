# R4. Do not hide failures

Record each externally visible rejection, terminal timeout or unexpected
failure once at the handling boundary. Callers that only pass the failure upward
do not record it. For expected packet-policy drops, use bounded aggregate
metrics or current diagnostics only when useful. Expected no-ops need no record.

Use metrics with bounded labels for totals. Encode reasons as a fixed set. Add
a structured event only when one failure needs details. Labels exclude external
identifiers and free text. Reuse
[`o-sfu-telemetry`](../../crates/telemetry/) names and recorders.

Never record credentials, packet contents or raw signaling payloads. Aggregate
repeated packet-path failures instead of logging each packet. See
[R5](r05-keep-media-hot-paths-cheap.md).

> [!NOTE]
> More read: **[the OWASP Logging Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html)** and **[Prometheus instrumentation practices](https://prometheus.io/docs/practices/instrumentation/)**.
>
> partially enforced with: [dbg_macro](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#dbg_macro),
> [map_err_ignore](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#map_err_ignore),
> [print_stderr](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#print_stderr),
> [print_stdout](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#print_stdout)
> and [unused_result_ok](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#unused_result_ok).

**Example:** `handshake::reject` records why a WebSocket connection was rejected
before trying to close it.

**Avoid**

```rust
let code = WebSocketCloseCode::AuthFailed;
// The rejected connection is closed, but operators cannot tell why.
close_writer_bounded(writer, code).await;
```

**Prefer**

```rust
// Preserve the reason even if the close attempt fails.
state.metrics.record_ws_handshake_rejection(Some(code));
info!(
    event = telemetry_event::WS_HANDSHAKE_REJECTED,
    close_code = u16::from(code),
    remote_address,
    "{message}"
);
close_writer_bounded(writer, code).await;
```

**Rationale:** Recording a failure at one handling point preserves its cause
without double counting.
