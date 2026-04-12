# o-sfu client

Current scope:

- package metadata and typescript base
- Odoo-facing public API types
- signaling envelope and payload catalog
- `ProtocolCoreWasm` is exposed from the Rust `protocol/` crate as the future browser-shell protocol "engine" (basically do all the heavy lift client side)

TODO/LATER:

- `WebSocket` orchestration
- `RTCPeerConnection` orchestration
- timer/reconnection machinery
- bundle build output for Odoo
