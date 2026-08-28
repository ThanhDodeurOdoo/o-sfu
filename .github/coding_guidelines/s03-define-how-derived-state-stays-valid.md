# S3. Define how derived state stays valid

Derived state covers counters, indexes, snapshots, caches and decisions
computed from authoritative state. Recompute when cheap. Otherwise define its
source, derivation, update or invalidation events, stale-use policy and rebuild
or revalidation path.

Use [M3](m03-choose-the-simplest-clear-design.md) to justify storage,
[C4](c04-put-each-invariant-in-its-lowest-owner.md) to assign ownership and
[R6](r06-bound-externally-driven-work.md) for externally driven retention.

Lossy keys, digests or summaries only narrow candidates. Verify the underlying
value before a false positive can change behavior.

> [!NOTE]
> More read: **[cache invalidation](https://en.wikipedia.org/wiki/Cache_invalidation)** and **[hash collisions](https://en.wikipedia.org/wiki/Hash_collision)**.

**Example:** `PacketLoopRoutingMissCache` retains negative routing decisions for
the current topology. Callers clear it when routing inputs change. A matching
fingerprint still requires exact packet bytes.

**Avoid**

```rust
// A fingerprint collision would suppress a different packet.
cache.iter().any(|entry| entry.key == key)
```

**Prefer**

```rust
// Exact bytes make collisions cost one comparison rather than correctness.
cache
    .iter()
    .any(|entry| entry.key == key && entry.packet.as_slice() == packet)
```

**Rationale:** Stored derived state creates a consistency obligation. Without a
defined relationship to authoritative state, a stale or lossy representation
can contradict the state it represents.
