# o-sfu client

`npm run build` regenerates the default `ProtocolCoreWasm` runtime into
`client/generated/` then compiles the TypeScript part.

`npm run build:odoo` generates an Odoo-compatible bundle at `client/dist/odoo_sfu.js`

`npm run test:browser` runs the Playwright suite against the built browser
bundle.

Requires `wasm-pack`:

```bash
cargo install wasm-pack
```

```bash
npm exec playwright install chromium
```
