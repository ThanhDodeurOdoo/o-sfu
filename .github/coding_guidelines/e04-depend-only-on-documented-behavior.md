# E4. Rely only on documented dependency behavior

Do not rely on a dependency's iteration order, timing, private state or error
text unless its public documentation guarantees it. Isolate unavoidable
assumptions and verify them against the pinned version.

**Example:** A successful `drain_single_session` polls `str0m::Rtc` until an
`Output::Timeout` whose deadline lies after the current time. If its output
budget is exhausted, the caller rolls back staged output and closes the session.
A fixed count without either terminal policy could leave `Rtc` partly drained.

**Avoid**

```rust
// Four polls may stop before str0m reaches its documented drain boundary.
for _ in 0..4 {
    handle(rtc.poll_output()?)?;
}
```

**Prefer**

```rust
// A future `Output::Timeout` is str0m's documented end-of-drain signal.
let deadline = loop {
    match rtc.poll_output()? {
        Output::Timeout(timeout_at) if timeout_at <= now => {
            // Feed an already-due deadline back before continuing the drain.
            rtc.handle_input(Input::Timeout(now))?;
        }
        Output::Timeout(timeout_at) => break timeout_at,
        output => handle(output)?,
    }
};
```

The caller also makes budget exhaustion terminal:

```rust
SessionDrainOutcome::Exhausted(session_key, limit) => {
    // Discard partial output before closing the session that exceeded its budget.
    buffers.rollback_session_drain(&checkpoint);
    context
        .rtc_metrics
        .record_rtc_output_budget_exhaustion(limit);
    worker_close_session(
        state,
        context.bitrate_registry,
        context.snapshot_state,
        &session_key,
        SessionCloseDisposition::OutputBudgetExhausted,
        context.metrics,
    );
}
```

**Rationale:** Undocumented behavior can change without an API break.
Dependency upgrades can then expose hidden assumptions.
