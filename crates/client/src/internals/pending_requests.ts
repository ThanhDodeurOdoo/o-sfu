import { COMMAND_KIND } from "../protocol_contract.js";
import type { HostCommand, PendingRequest } from "../runtime_contract.js";
import type { PendingRequestCallbacks } from "./browser_types.js";

export class PendingRequests {
    private _pendingRequestResolvers = new Map<string, PendingRequestCallbacks>();

    constructor(
        private readonly _enqueueCommands: (commands: HostCommand[]) => void,
        private readonly _enqueueRequest: (
            commands: HostCommand[],
            begin: () => void
        ) => Promise<void>,
        private readonly _scheduleTimer: (timeoutTimerId: number, timeoutMs: number) => void
    ) {}

    drainRequestCommands(commands: HostCommand[]): Promise<boolean> {
        const first = commands[0];
        if (first?.kind !== COMMAND_KIND.BEGIN_PENDING_REQUEST) {
            this._enqueueCommands(commands);
            return Promise.resolve(false);
        }

        const req = first.request;
        let completion: Promise<boolean> | undefined;
        return this._enqueueRequest(commands.slice(1), () => {
            completion = this.register(req);
            void completion.catch(() => undefined);
            this._scheduleTimer(req.timeoutTimerId, req.timeoutMs);
        }).then(
            () =>
                completion ??
                Promise.reject(new Error("pending request skipped before registration")),
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
