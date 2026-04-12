import { statSync } from "node:fs";
import { fileURLToPath } from "node:url";

const GENERATED_WASM_PATH = fileURLToPath(
    new URL("../generated/o_sfu_protocol_bg.wasm", import.meta.url)
);
const WASM_SIZE_BUDGET_BYTES = 350 * 1024;

const { size } = statSync(GENERATED_WASM_PATH);

if (size > WASM_SIZE_BUDGET_BYTES) {
    throw new Error(
        `o-sfu protocol WASM exceeds the ${WASM_SIZE_BUDGET_BYTES}-byte budget (${size} bytes).`
    );
}

console.log(
    `o-sfu protocol WASM size ${size} bytes is within the ${WASM_SIZE_BUDGET_BYTES}-byte budget.`
);
