# C3. Make units, overflow and clock choice explicit

Use `Bitrate`, `Duration` and range-checked types instead of unitless integers
at internal boundaries. Use `TryFrom` for values that may not fit. Choose
checked, wrapping or saturating arithmetic by required overflow behavior.
Inline obvious local literals. Name repeated or domain-significant literals
with a constant, enum variant or domain type.

> [!NOTE]
> More read: **[integer overflow in The Rust Book](https://doc.rust-lang.org/book/ch03-02-data-types.html#integer-overflow)** and **[`Instant` in the standard library](https://doc.rust-lang.org/std/time/struct.Instant.html)**.
>
> partially enforced with: [as_conversions](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#as_conversions),
> [pedantic::cast_possible_truncation](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#cast_possible_truncation),
> [pedantic::cast_possible_wrap](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#cast_possible_wrap),
> [pedantic::cast_precision_loss](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#cast_precision_loss),
> [pedantic::cast_sign_loss](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#cast_sign_loss),
> [pedantic::manual_instant_elapsed](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#manual_instant_elapsed),
> [pedantic::unchecked_time_subtraction](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#unchecked_time_subtraction)
> and [style::manual_saturating_arithmetic](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#manual_saturating_arithmetic).

**Example:** Domain types, named policy constants and explicit arithmetic
preserve each value's meaning.

**Avoid**

```rust
// The integer informs neither that this is a bitrate nor that it is in bits per second.
let bitrate = bits_per_second;
// RTP timestamps wrap at rollover, while plain `+` may panic when overflow checks are enabled.
let next_timestamp = highest_timestamp + 1;
// The default policy is hidden in an unnamed value.
let video_limits = VideoBitrateLimits::new(Bitrate::from_mbps(4));
```

**Prefer**

```rust
// `Bitrate` keeps bitrate distinct from other integer quantities and clears unit conversion ambiguity.
let bitrate = Bitrate::from_bps(bits_per_second);
// RTP timestamps use wrapping arithmetic, including at rollover.
let next_timestamp = highest_timestamp.wrapping_add(1);
// The name and type identify both the policy and its unit.
let video_limits =
    VideoBitrateLimits::new(VideoBitrateLimits::DEFAULT_MAX_VIDEO_BITRATE);
```

Use monotonic `Instant` for deadlines and elapsed time. Keep unit-suffixed
integers at configuration or compatibility boundaries then convert them before
internal arithmetic. Use wall-clock time only when an external protocol or
output format requires it.

**Rationale:** Named values reveal their meaning. Explicit units, clocks and
overflow rules prevent incorrect arithmetic.
