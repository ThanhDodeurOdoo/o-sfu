# E2. Convert external formats at adapter boundaries

Use private wire types for transport-specific, versioned or loosely constrained
data. Convert them to O-SFU domain types at adapter boundaries. Serialize domain
types directly only when the format is their documented contract and requires
no adapter validation. Keep HTTP extractors, WebSocket frames and
external-library types inside adapters. Normalize compatibility forms before
storage or indexing. Use `serde_json::Value` only when the contract allows
arbitrary JSON.

> [!NOTE]
> More read: **[`TryFrom` in the standard library](https://doc.rust-lang.org/std/convert/trait.TryFrom.html)**.

**Example:** `decode_client_batch` converts WebSocket input into
`ClientEnvelope`. Domain code receives the enum instead of raw fields.

**Avoid**

```rust
pub async fn apply_client_envelope(
    &mut self,
    // Domain code must interpret wire tags and validate untyped payloads.
    tag: String,
    payload: Option<serde_json::Value>,
) -> Result<UserOutput, UserError>
```

**Prefer**

```rust
pub async fn apply_client_envelope(
    &mut self,
    // Unknown tags and invalid payloads were rejected at the WebSocket boundary.
    envelope: ClientEnvelope,
) -> Result<UserOutput, UserError>
```

**Rationale:** Boundary conversion keeps transport details out of typed domain
APIs.
