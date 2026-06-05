import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdir, readFile } from "node:fs/promises";
import { fileURLToPath, pathToFileURL } from "node:url";

import { build } from "esbuild";

const repositoryUrl = "https://github.com/ThanhDodeurOdoo/o-sfu";
const repositoryDirectory = fileURLToPath(new URL("../../..", import.meta.url));
const repositoryManifestPath = fileURLToPath(new URL("../../../Cargo.toml", import.meta.url));
const clientDirectory = fileURLToPath(new URL("..", import.meta.url));
const entryPoint = fileURLToPath(new URL("./odoo_entry.ts", import.meta.url));
const outputPath = fileURLToPath(new URL("../dist/odoo_sfu.js", import.meta.url));

function commandOutput(command, args) {
    return execFileSync(command, args, {
        cwd: repositoryDirectory,
        encoding: "utf8"
    }).trim();
}

function packageVersion() {
    const metadata = JSON.parse(
        commandOutput("cargo", ["metadata", "--no-deps", "--format-version", "1"])
    );
    const rootPackage = metadata.packages.find(
        (pkg) => pkg.manifest_path === repositoryManifestPath
    );
    assert(rootPackage, "cargo metadata must include the root o-sfu package");
    assert.equal(typeof rootPackage.version, "string");
    return rootPackage.version;
}

const bundleInfo = {
    date: new Date().toISOString(),
    hash: commandOutput("git", ["rev-parse", "--short", "HEAD"]),
    url: repositoryUrl,
    version: packageVersion()
};

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
    footer: {
        js: `export const __info__ = ${JSON.stringify(bundleInfo, null, 4)};`
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
assert.deepEqual(bundleModule.__info__, bundleInfo);

console.log(`Built Odoo SFU bundle at ${outputPath}`);
