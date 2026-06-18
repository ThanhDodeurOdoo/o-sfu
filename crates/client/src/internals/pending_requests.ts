import {
    CommandKind,
    PENDING_REQUEST_KIND,
    type HostCommand,
    type PendingRequestKind
} from "../runtime_contract.js";
import type { PendingRequestCallbacks } from "./browser_types.js";

const ALL_PENDING_REQUEST_KINDS = Object.values(PENDING_REQUEST_KIND) as PendingRequestKind[];
type PendingRequestRegistrationCommand = Extract<
    HostCommand,
    { kind: typeof CommandKind.REGISTER_PENDING_REQUEST }
>;

export class PendingRequests {
    private _pendingRequestResolvers = new Map<string, PendingRequestCallbacks>();
    private _requestWaiters: Record<PendingRequestKind, PendingRequestCallbacks[]> = {
        [PENDING_REQUEST_KIND.START_RECORDING]: [],
        [PENDING_REQUEST_KIND.STOP_RECORDING]: []
    };

    begin(
        getCommands: () => HostCommand[],
        enqueue: (commands: HostCommand[]) => void,
        onRuntimeError: (error: unknown) => void
    ): Promise<boolean> {
        let commands: HostCommand[];
        let registration: PendingRequestRegistrationCommand | null;
        try {
            commands = getCommands();
            registration = this.findRegistrationCommand(commands);
        } catch (error) {
            onRuntimeError(error);
            return Promise.reject(error);
        }
        if (!registration) {
            enqueue(commands);
            return Promise.resolve(false);
        }
        return new Promise<boolean>((resolve, reject) => {
            this._requestWaiters[registration.requestKind].push({ resolve, reject });
            enqueue(commands);
        });
    }

    register(requestId: string, requestKind: PendingRequestKind): void {
        const callbacks = this._requestWaiters[requestKind].shift();
        if (!callbacks) {
            throw new Error(`missing pending request waiter for ${requestKind}`);
        }
        this._pendingRequestResolvers.set(requestId, callbacks);
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
        for (const requestKind of ALL_PENDING_REQUEST_KINDS) {
            const waiters = this._requestWaiters[requestKind];
            for (const waiter of waiters) {
                waiter.reject(error);
            }
            waiters.length = 0;
        }
    }

    private findRegistrationCommand(
        commands: HostCommand[]
    ): PendingRequestRegistrationCommand | null {
        let registration: PendingRequestRegistrationCommand | null = null;
        for (const command of commands) {
            if (command.kind !== CommandKind.REGISTER_PENDING_REQUEST) {
                continue;
            }
            if (registration) {
                throw new Error(
                    "pending request command batches must register at most one request"
                );
            }
            registration = command;
        }
        return registration;
    }
}
