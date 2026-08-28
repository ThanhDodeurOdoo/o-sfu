# R5. Keep packet and frame processing cheap

Keep packet, frame and destination work bounded and allocation-free. Avoid
payload copies, formatting, metric lookup, whole-room scans, blocking and
contended locks. Reuse buffers. Resolve policy and handles before the repeated
call. Compute shared facts once. Prefer bounded `Vec` scans when indexes add
more state than they save.

Measure the real path with the same workload and compiler settings. Exclude
setup and prove the workload reached it. Use allocation profiles for
allocations, load tests for latency and instruction counts for small synchronous
work.

> [!NOTE]
> More read: **[heap allocation costs](https://nnethercote.github.io/perf-book/heap-allocations.html)** and **[benchmark design](https://nnethercote.github.io/perf-book/benchmarking.html)**.
>
> partially enforced with: [assigning_clones](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#assigning_clones),
> [redundant_clone](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#redundant_clone),
> [pedantic::format_collect](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#format_collect),
> [pedantic::inefficient_to_string](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#inefficient_to_string),
> [pedantic::large_types_passed_by_value](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#large_types_passed_by_value),
> [perf::iter_overeager_cloned](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#iter_overeager_cloned),
> [perf::manual_memcpy](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#manual_memcpy),
> [perf::regex_creation_in_loops](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#regex_creation_in_loops)
> and [perf::unnecessary_to_owned](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#unnecessary_to_owned).

**Example:** `PacketLoopRoutingMissRecord::overwrite` reuses the retained packet
buffer when replacing an evicted cache entry.

**Avoid**

```rust
// This allocates a new buffer on every overwrite.
self.packet = packet.to_vec();
```

**Prefer**

```rust
// The retained capacity is reused whenever the next packet fits.
self.packet.clear();
self.packet.extend_from_slice(packet);
```

**Rationale:** Small costs multiply at media rate and reduce throughput.
