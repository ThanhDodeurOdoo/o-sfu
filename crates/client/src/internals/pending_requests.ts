import { COMMAND_KIND } from "../protocol_contract.js";
import type { HostCommand, PendingRequest } from "../runtime_contract.js";
import type { PendingRequestCallbacks } from "./browser_types.js";

export class PendingRequests {
    private _pendingRequestResolvers = new Map<string, PendingRequestCallbacks>();

    constructor(
        private readonly _enqueueRequest: (getCommands: () => HostCommand[]) => Promise<void>,
        private readonly _scheduleTimer: (timeoutTimerId: number, timeoutMs: number) => void
    ) {}

    drainRequestCommands(getCommands: () => HostCommand[]): Promise<boolean> {
        let completion: Promise<boolean> | undefined;
        return this._enqueueRequest(() => {
            const commands = getCommands();
            const first = commands[0];
            if (first?.kind !== COMMAND_KIND.BEGIN_PENDING_REQUEST) {
                return commands;
            }
            completion = this.register(first.request);
            void completion.catch(() => undefined);
            this._scheduleTimer(first.request.timeoutTimerId, first.request.timeoutMs);
            return commands.slice(1);
        }).then(
            () => completion ?? false,
            (error) => completion ?? Promise.reject(error)
        );
    }

    private register(request: PendingRequest): Promise<boolean> {
        if (this._pendingRequestResolvers.has(request.requestId)) {
            throw new Error(`pending request ${request.requestId} is already registered`);
        }
        return new Promise<boolean>((resolve, reject) => {
            this._pendingRequestResolvers.set(request.requestId, { resolve, reject });
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
}
