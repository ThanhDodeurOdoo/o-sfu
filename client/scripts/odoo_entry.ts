import { configureDefaultProtocolCoreFactory } from "../src/default_protocol_core_factory.ts";
import { ProtocolCoreWasm, initSync } from "../generated/o_sfu_protocol.js";
import wasmModule from "../generated/o_sfu_protocol_bg.wasm";

configureDefaultProtocolCoreFactory(() => {
    initSync({ module: wasmModule });
    return new ProtocolCoreWasm();
});

export * from "../src/protocol.ts";
export * from "../src/public_api.ts";
export * from "../src/runtime_contract.ts";
export * from "../src/sfu_client.ts";
