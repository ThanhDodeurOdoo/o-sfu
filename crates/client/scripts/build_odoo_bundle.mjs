import assert from "node:assert/strict";
import { mkdir, readFile } from "node:fs/promises";
import { fileURLToPath, pathToFileURL } from "node:url";

import { build } from "esbuild";

const clientDirectory = fileURLToPath(new URL("..", import.meta.url));
const entryPoint = fileURLToPath(new URL("./odoo_entry.ts", import.meta.url));
const outputPath = fileURLToPath(new URL("../dist/odoo_sfu.js", import.meta.url));

await mkdir(fileURLToPath(new URL("../dist", import.meta.url)), { recursive: true });

await build({
    bundle: true,
    entryPoints: [entryPoint],
    format: "esm",
    loader: {
        ".wasm": "binary"
    },
    outfile: outputPath,
    platform: "browser",
    sourcemap: false,
    target: ["es2020"],
    banner: {
        js: "/* @odoo-module */"
    },
    absWorkingDir: clientDirectory
});

const output = await readFile(outputPath, "utf8");
assert.match(output, /^\/\* @odoo-module \*\//, "the Odoo bundle must start as an Odoo module");
assert.equal(
    output.includes("import.meta"),
    false,
    "the Odoo bundle must not depend on import.meta.url"
);
assert.equal(
    output.includes("fetch("),
    false,
    "the Odoo bundle must not fetch a wasm sidecar at runtime"
);
assert.equal(
    output.includes("new URL("),
    false,
    "the Odoo bundle must not resolve a wasm sidecar URL at runtime"
);

const bundleModule = await import(`${pathToFileURL(outputPath).href}?t=${Date.now()}`);
assert.equal(typeof bundleModule.SfuClient, "function");
assert.equal(bundleModule.SFU_CLIENT_STATE.CONNECTED, "connected");
assert.equal(bundleModule.createProtocolCore().state, "disconnected");

console.log(`Built Odoo SFU bundle at ${outputPath}`);
