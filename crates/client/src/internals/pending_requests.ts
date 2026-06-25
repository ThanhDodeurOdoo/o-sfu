import type { PendingRequest } from "../runtime_contract.js";
import type { PendingRequestCallbacks } from "./browser_types.js";

export class PendingRequests {
    private _pendingRequestResolvers = new Map<string, PendingRequestCallbacks>();

    begin(request: PendingRequest): Promise<boolean> {
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

    has(requestId: string): boolean {
        return this._pendingRequestResolvers.has(requestId);
    }
}
