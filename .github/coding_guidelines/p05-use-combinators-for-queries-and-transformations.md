# P5. Use combinators for queries and transformations

Use `Iterator`, `Option` and `Result` combinators when they express a query or
transformation directly. Prefer `find`, `any`, `all`, `filter_map`, `map`,
`and_then`, `or_else`, `unwrap_or_else`, `filter` and `collect` to manual loops,
accumulators or indexing. Keep chains short. Use a `for` loop for ordered
effects, related-state mutation, awaiting or branching that a chain would hide.
Never use `map` only for effects. `Option` and `Result` combinators propagate
`None` and `Err`.

> [!NOTE]
> More read: **[monadic composition](https://en.wikipedia.org/wiki/Monad_(functional_programming))** and **[algebraic data types](https://en.wikipedia.org/wiki/Algebraic_data_type)**.
>
> partially enforced with: [option_if_let_else](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#option_if_let_else),
> [single_option_map](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#single_option_map),
> [useless_let_if_seq](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#useless_let_if_seq),
> [complexity::bind_instead_of_map](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#bind_instead_of_map),
> [complexity::manual_filter](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#manual_filter),
> [complexity::manual_filter_map](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#manual_filter_map),
> [complexity::manual_find](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#manual_find),
> [complexity::manual_find_map](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#manual_find_map),
> [complexity::option_map_unit_fn](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#option_map_unit_fn),
> [complexity::result_map_unit_fn](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#result_map_unit_fn),
> [pedantic::needless_for_each](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#needless_for_each),
> [style::manual_map](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#manual_map)
> and [style::manual_ok_or](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#manual_ok_or).

**Example 1 (Iterator query):** `MediaCodecCapability::rtx_associated_payload_type_id`
returns the first RTX association.

**Avoid**

```rust
let mut association = None;
for setting in &self.settings {
    if let CodecSetting::RtxAssociation(payload_type) = setting {
        // Mutable state and `break` only encode a first-match query.
        association = Some(*payload_type);
        break;
    }
}
association
```

**Prefer**

```rust
// `find_map` states the first matching RTX association directly.
self.settings.iter().find_map(|setting| match setting {
    CodecSetting::RtxAssociation(payload_type) => Some(*payload_type),
    _ => None,
})
```

**Example 2 (Monadic pipeline):** Chain sequential transformations using
`and_then` rather than nested `match` or `if let` ladders.

**Avoid**

```rust
// Nested matching pyramids obscure linear data flow.
let room_id = match get_token(header) {
    Some(token) => match parse_claims(token) {
        Ok(claims) => claims.room_id,
        Err(_) => None,
    },
    None => None,
};
```

**Prefer**

```rust
// Monadic chaining models the linear transformation directly.
let room_id = get_token(header)
    .and_then(|token| parse_claims(token).ok())
    .and_then(|claims| claims.room_id);
```

`parse_codec_list` correctly keeps a loop because each iteration validates
against previously accepted codecs and may return a distinct error.

**Rationale:** Short chains show what the code computes without extra mutable
state or loop control. Monadic pipelines propagate absence and errors linearly
without nested pyramids. Loops remain clearer when ordering, mutation or
effects matter.
