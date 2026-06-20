import { CommandKind, type HostCommand } from "../runtime_contract.js";
import type { PendingRequestCallbacks } from "./browser_types.js";

type BeginPendingRequestCommand = Extract<
    HostCommand,
    { kind: typeof CommandKind.BEGIN_PENDING_REQUEST }
>;

export class PendingRequests {
    private _pendingRequestResolvers = new Map<string, PendingRequestCallbacks>();

    begin(
        getCommands: () => HostCommand[],
        enqueue: (commands: HostCommand[]) => void,
        onRuntimeError: (error: unknown) => void
    ): Promise<boolean> {
        let commands: HostCommand[];
        let request: BeginPendingRequestCommand | null;
        try {
            commands = getCommands();
            request = this.findBeginRequestCommand(commands);
        } catch (error) {
            onRuntimeError(error);
            return Promise.reject(error);
        }
        if (!request) {
            enqueue(commands);
            return Promise.resolve(false);
        }
        if (this._pendingRequestResolvers.has(request.requestId)) {
            const error = new Error(`pending request ${request.requestId} is already registered`);
            onRuntimeError(error);
            return Promise.reject(error);
        }
        return new Promise<boolean>((resolve, reject) => {
            this._pendingRequestResolvers.set(request.requestId, { resolve, reject });
            enqueue(commands);
        });
    }

    resolve(requestId: string, ok: boolean): void {
        const callbacks = this._pendingRequestResolvers.get(requestId);
        if (!callbacks) {
            return;
        }
        this._pendingRequestResolvers.delete(requestId);
        callbacks.resolve(ok);
    }

    rejectAll(error: Error): void {
        for (const callbacks of this._pendingRequestResolvers.values()) {
            callbacks.reject(error);
        }
        this._pendingRequestResolvers.clear();
    }

    has(requestId: string): boolean {
        return this._pendingRequestResolvers.has(requestId);
    }

    private findBeginRequestCommand(commands: HostCommand[]): BeginPendingRequestCommand | null {
        let request: BeginPendingRequestCommand | null = null;
        for (const command of commands) {
            if (command.kind !== CommandKind.BEGIN_PENDING_REQUEST) {
                continue;
            }
            if (request) {
                throw new Error("pending request command batches must begin at most one request");
            }
            request = command;
        }
        return request;
    }
}
