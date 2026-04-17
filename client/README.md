# o-sfu client

`npm run build` now generates the default `ProtocolCoreWasm` runtime into `client/generated/` and
compiles the TypeScript part.

`npm run build:odoo` generates an Odoo-compatible bundle at `client/dist/odoo_sfu.js`. 

`npm run test:browser` runs the headless Chromium Playwright suite against the built browser
bundle.

Requires `wasm-pack`:

```bash
cargo install wasm-pack
```

```bash
npm exec playwright install chromium
```
