import {
    configureDefaultProtocolCoreProvider,
    type ProtocolCoreBindings,
    type ProtocolCoreProvider
} from "./runtime_contract.js";

type GeneratedProtocolModule = {
    default: (
        input?:
            | BufferSource
            | URL
            | {
                  module_or_path?: BufferSource | URL;
              }
    ) => Promise<unknown>;
    ProtocolCoreWasm: new () => ProtocolCoreBindings;
};

const GENERATED_MODULE_URL = new URL("../generated/o_sfu_protocol.js", import.meta.url);
const GENERATED_WASM_URL = new URL("../generated/o_sfu_protocol_bg.wasm", import.meta.url);

const generatedProtocolModule = await initializeGeneratedProtocolModule();

export const defaultProtocolCoreProvider: ProtocolCoreProvider = () =>
    new generatedProtocolModule.ProtocolCoreWasm();

configureDefaultProtocolCoreProvider(defaultProtocolCoreProvider);

async function initializeGeneratedProtocolModule(): Promise<GeneratedProtocolModule> {
    const module = (await import(GENERATED_MODULE_URL.href)) as GeneratedProtocolModule;
    await module.default({ module_or_path: await resolveGeneratedWasmInput() });
    return module;
}

async function resolveGeneratedWasmInput(): Promise<BufferSource | URL> {
    if (isNodeRuntime()) {
        const { readFile } = (await import(nodeFsPromisesSpecifier())) as {
            readFile: (path: URL) => Promise<BufferSource>;
        };
        return readFile(GENERATED_WASM_URL);
    }
    return GENERATED_WASM_URL;
}

function isNodeRuntime(): boolean {
    const globalWithProcess = globalThis as typeof globalThis & {
        process?: {
            versions?: {
                node?: string;
            };
        };
    };
    return typeof globalWithProcess.process?.versions?.node === "string";
}

function nodeFsPromisesSpecifier(): string {
    return "node:fs/promises";
}
