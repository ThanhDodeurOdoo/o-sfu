# o-sfu client

Current scope:

- package metadata and typescript base
- Odoo-facing public API types
- signaling envelope and payload catalog
- `ProtocolCoreWasm` is exposed from the Rust `protocol/` crate as the future browser-shell protocol "engine" (basically do all the heavy lift client side)
- `SfuClient` now exists as a smal browser shell that executes protocol-core host commands through injected WebSocket / RTCPeerConnection / timer factories
- node-based package tests now cover URL normalization, pending-request resolution, negotiation answer submission, and lowercase `track` event emission

TODO/LATER:

- generated `wasm-pack` bindings and bundle output for Odoo
- richer remote-track lifecycle handling beyond initial `ontrack` delivery
- Playwright coverage once the browser bundle exists
