# o-sfu client

`npm run build` regenerates the default `ProtocolCoreWasm` runtime into
`client/generated/` then compiles the TypeScript part.

`npm run build:odoo` generates an Odoo-compatible bundle at `client/dist/odoo_sfu.js`
and exports `__info__` with the build date, short git hash, repository URL and
root `o-sfu` cargo package version

`npm run test:browser` runs the headless Chromium Playwright suite against the built browser
bundle.

Requires `wasm-pack`:

```bash
cargo install wasm-pack
```

```bash
npm exec playwright install chromium
```

## notes

still heavily logging the sfu_client.ts, maybe need to cleanup at some point

## File map

### public parts:

- `src/public_api.ts`: Odoo-facing client API, events, states, and compatibility types
- `src/protocol.ts`: stable public facade for curated signaling envelope and payload types
- `src/protocol_contract.ts`: handwritten browser signaling contract with envelope aliases and client-visible literals
- `src/runtime_contract.ts`: protocol-core provider boundary, host-command types, and runtime validation.
- `src/sfu_client.ts`: public `SfuClient` facade exposed to Odoo and tests.
- `src/wasm_runtime.ts`: default async `wasm-pack` bootstrap for the normal browser bundle.

### internals:

- `src/internals/browser_runtime.ts`: executes host commands against `WebSocket`, `RTCPeerConnection`, and timers.
- `src/internals/browser_types.ts`: browser/test types used by the runtime helpers.
- `src/internals/local_uploads.ts`: local track bookeping and sender-to-mid attachment logic
- `src/internals/remote_tracks.ts`: remote track binding state and the compatibility `_consumers` map.
- `src/internals/pending_requests.ts`: request/response bookkeeping for recording-style async operations.
- `src/internals/validation.ts`: input validation and cloning helpers.
- `scripts/odoo_entry.ts`: sync Odoo bundle entrypoint to bootstrap the protocol WASM.
- `scripts/build_odoo_bundle.mjs`: builds and validates `dist/odoo_sfu.js`,
  including the exported `__info__` metadata
- `scripts/build_wasm_runtime.mjs`: regenerates the `client/generated/` WASM package from `protocol/`.
- `test/`: Node unit tests
- `playwright/`: full stack tests with playwright
- `generated/`: wasm generated files (git ignored)
- `dist/`: compiled package output and the generated shipped Odoo bundle (git ignored)
