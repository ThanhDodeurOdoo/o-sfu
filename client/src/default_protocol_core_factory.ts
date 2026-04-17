import type { ProtocolCoreBindings, ProtocolCoreFactory } from "./runtime_contract.js";

let defaultProtocolCoreFactory: ProtocolCoreFactory | undefined;

export function configureDefaultProtocolCoreFactory(factory: ProtocolCoreFactory): void {
    defaultProtocolCoreFactory = factory;
}

export function requireDefaultProtocolCoreFactory(): ProtocolCoreFactory {
    if (!defaultProtocolCoreFactory) {
        throw new Error(
            "default protocol core factory is not configured; import the package entrypoint or configure one explicitly"
        );
    }
    return defaultProtocolCoreFactory;
}

export type { ProtocolCoreBindings };
