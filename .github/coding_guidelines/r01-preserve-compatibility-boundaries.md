# R1. Preserve compatibility between versions

Compatibility covers published APIs, wire messages, documented Odoo-facing
contracts and source-retained forms, not undocumented behavior in the replaced
SFU. Clients and servers may deploy at different times, so mixed-version
upgrades and rollbacks must remain compatible.

Prefer additive changes. Give new fields defaults when absent. Ignore permitted
unknown optional data. Negotiate tags old readers reject. Accept and normalize
retained legacy input at the edge.

Remove compatibility code only after its documented removal condition is met
and mixed-artifact tests cover remaining upgrades and rollbacks. An unavoidable
wire break needs an explicit migration coordinated across the server, client
and compiled Odoo bundle.

> [!NOTE]
> More read: **[SemVer compatibility in The Cargo Book](https://doc.rust-lang.org/cargo/reference/semver.html)** and **[protocol extension guidance in RFC 6709](https://www.rfc-editor.org/rfc/rfc6709.html)**.

**Example:** `SfuClient` keeps `updateUpload` as an alias for older Odoo code.
`ProtocolCore` accepts the legacy `sources` message from older servers.

```typescript
/** @deprecated Odoo compatibility alias. Use `publish()` for new code. */
updateUpload(type: StreamType, track: MediaStreamTrack | null | undefined): void {
    // Older Odoo callers still reach the validated publish path.
    this.publish(type, track);
}
```

```rust
// Ignore the retired snapshot without rejecting the frame. Rolled-back servers
// can still emit it.
ServerMessage::Sources(_) => Vec::new(),
```

**Rationale:** Components change at different times. Compatibility prevents a
version change from breaking calls.
