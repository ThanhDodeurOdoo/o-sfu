# C4. Enforce invariants where the state lives

Put each invariant in the lowest layer that controls every update. Ingress owns
decoding, bounds and credential checks. State owners enforce authorization and
transitions. Adapters translate network or storage failures. Callers do not
repeat checks. One owner updates related records, indexes and derived views
together.

Keep identities where they are defined. Room source IDs (`PublishedSourceId`,
`SourceEncodingId`), negotiated publisher values (`Mid`, `Rid`, `Ssrc`),
transport IDs and receiver-local handles, sequence numbers and timestamps are
separate. Translate only at a boundary that owns both layers. Never replace a
room source ID with a negotiated, worker-local or receiver-local value.

> [!NOTE]
> More read: **[private struct fields in the Rust API Guidelines](https://rust-lang.github.io/api-guidelines/future-proofing.html#structs-have-private-fields-c-struct-private)**.

**Example:** `PublishedSourceDescriptor::new` rejects empty, duplicate or
cross-source encodings. `PublishedSources` updates its map and indexes together.

**Avoid**

```rust
// This repeats the empty check but misses duplicate and cross-source encodings.
if parts.encodings.is_empty() {
    return Err(SourceModelError::SourceWithoutEncodings {
        source_id: parts.source_id,
    });
}
let descriptor = PublishedSourceDescriptor::new(parts)?;
```

**Prefer**

```rust
// Successful construction proves every descriptor invariant.
let descriptor = PublishedSourceDescriptor::new(parts)?;
```

**Rationale:** One owner per invariant keeps validation, identity mappings and
representations consistent.
