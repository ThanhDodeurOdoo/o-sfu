import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const clientDirectory = fileURLToPath(new URL("..", import.meta.url));
const generatedDirectory = fileURLToPath(new URL("../generated", import.meta.url));
const protocolDirectory = fileURLToPath(new URL("../../protocol", import.meta.url));

const result = spawnSync(
    "wasm-pack",
    [
        "build",
        protocolDirectory,
        "--target",
        "web",
        "--release",
        "--out-dir",
        generatedDirectory,
        "--out-name",
        "o_sfu_protocol",
        "--",
        "--locked"
    ],
    {
        cwd: clientDirectory,
        stdio: "inherit"
    }
);

if (result.error) {
    if (result.error.code === "ENOENT") {
        throw new Error(
            "wasm-pack is required to build the o-sfu client runtime; install it with `cargo install wasm-pack`."
        );
    }
    throw result.error;
}

if (result.status !== 0) {
    process.exit(result.status ?? 1);
}
