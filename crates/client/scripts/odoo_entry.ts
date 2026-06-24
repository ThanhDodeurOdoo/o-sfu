/**
 * Odoo bundle entrypoint that initializes the generated WASM module once and
 * connects the bundle to the default WASM protocol-core provider
 */
import {
    configureDefaultWasmProtocolCoreProvider,
    type ProtocolCoreBindings
} from "../src/runtime_contract.ts";
import { ProtocolCoreWasm, initSync } from "../generated/o_sfu_protocol.js";
import wasmModule from "../generated/o_sfu_protocol_bg.wasm";

let protocolModuleInitialized = false;

function ensureProtocolModuleInitialized() {
    if (protocolModuleInitialized) {
        return;
    }
    initSync({ module: wasmModule });
    protocolModuleInitialized = true;
}

configureDefaultWasmProtocolCoreProvider(() => {
    ensureProtocolModuleInitialized();
    // as unknown as cast:
    // wasm-bindgen widens `state` to `string` in the generated TS,
    // so the provider needs a local cast back to the validated client
    // contract until the generated declarations resolve
    return new ProtocolCoreWasm() as unknown as ProtocolCoreBindings;
});

export * from "../src/public_api.ts";
export * from "../src/sfu_client.ts";
