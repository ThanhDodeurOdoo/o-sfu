# o-sfu client

`npm run build` now generates the default `ProtocolCoreWasm` runtime into `client/generated/` and
compiles the TypeScript part.

`npm run test:browser` runs the headless Chromium Playwright suite against the built browser
bundle. The suite now also boots a local `o-sfu` server for live-browser interop coverage, so it
requires both the browser bundle prerequisites and a working local Rust toolchain.

Requires `wasm-pack`:

```bash
cargo install wasm-pack
```

```bash
npm exec playwright install chromium
```
