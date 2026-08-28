# P2. Pass dependencies and expose side effects

Pass required time, configuration, external data and services into domain
functions. Keep environment, clock, network and storage access in adapters or
orchestration code.

Construct services in orchestration code. Names and signatures must expose
mutation, resource creation, I/O, retries and caching.

**Example:** `DemuxRecoveryState::record_miss` receives the UDP ingress
timestamp instead of reading the clock.

**Avoid**

```rust
fn record_miss(&mut self, source_addr: SocketAddr) -> bool {
    // The hidden clock makes identical calls depend on execution time.
    self.source_rate_limiter
        .record_miss(source_addr, Instant::now())
}
```

**Prefer**

```rust
// Reuse the socket-completion timestamp so queue delay cannot change cooldown timing.
let entered_cooldown =
    demux.record_miss(miss_key, route.packet, route.source_addr, route.now);
```

**Rationale:** Explicit inputs and effects make behavior predictable and
testable.
