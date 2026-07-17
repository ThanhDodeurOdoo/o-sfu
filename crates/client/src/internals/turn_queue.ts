/*
 * Runs queued operations one at a time and lets an interrupt invalidate the
 * active operation.
 *
 * `BrowserRuntime` awaits WebRTC effects that yield to callbacks. Without this
 * boundary a later Rust transition can overtake earlier browser effects or an
 * obsolete Promise completion can submit feedback after recovery or teardown.
 */
export type TurnGuard = () => boolean;

type TurnOperation = (isCurrent: TurnGuard) => void | Promise<void>;

type TurnSettlement = {
    reject: (reason: unknown) => void;
    resolve: () => void;
};

type TaggedTurn<Tag> = {
    operation: TurnOperation;
    settlement?: TurnSettlement;
    tag: Tag;
};

type ControlTurn = {
    operation: TurnOperation;
};

type Turn<Tag> = TaggedTurn<Tag> | ControlTurn;

export class TurnQueue<Tag> {
    private _active: Turn<Tag> | null = null;
    private _incoming: TaggedTurn<Tag>[] = [];
    private _pumpScheduled = false;
    private _ready: TaggedTurn<Tag>[] = [];

    constructor(private readonly _onError: (error: unknown) => void) {}

    get hasControlTurn(): boolean {
        return this._active !== null && !("tag" in this._active);
    }

    enqueue(operation: TurnOperation, tag: Tag): void {
        this._incoming.push({ operation, tag });
        this.schedulePump();
    }

    enqueueAndWait(operation: TurnOperation, tag: Tag): Promise<void> {
        return new Promise((resolve, reject) => {
            this._incoming.push({ operation, settlement: { reject, resolve }, tag });
            this.schedulePump();
        });
    }

    interrupt(operation: TurnOperation, retain: (tag: Tag) => boolean, error?: Error): void {
        const active = this._active;
        const retained: TaggedTurn<Tag>[] = [];
        const control = { operation };
        this._active = control;
        const outcome = error === undefined ? "resolve" : "reject";
        this.settle(active, outcome, error);
        for (const turn of this.takePending()) {
            if (retain(turn.tag)) {
                retained.push(turn);
            } else {
                this.settle(turn, outcome, error);
            }
        }
        this._incoming = retained;
        queueMicrotask(() => {
            if (this._active === control) {
                this.run(control);
            }
        });
    }

    cancelPending(error?: Error): void {
        const outcome = error === undefined ? "resolve" : "reject";
        for (const turn of this.takePending()) {
            this.settle(turn, outcome, error);
        }
    }

    private schedulePump(): void {
        if (this._active || this._pumpScheduled) {
            return;
        }
        this._pumpScheduled = true;
        queueMicrotask(() => {
            this._pumpScheduled = false;
            if (this._active) {
                return;
            }
            const turn = this.takeNext();
            if (!turn) {
                return;
            }
            this._active = turn;
            this.run(turn);
        });
    }

    private run(turn: Turn<Tag>): void {
        let result: void | Promise<void>;
        try {
            result = turn.operation(() => this._active === turn);
        } catch (error) {
            this.complete(turn, "reject", error);
            return;
        }
        if (result === undefined) {
            this.complete(turn, "resolve");
            return;
        }
        void result.then(
            () => this.complete(turn, "resolve"),
            (error: unknown) => this.complete(turn, "reject", error)
        );
    }

    private complete(turn: Turn<Tag>, outcome: "reject" | "resolve", error?: unknown): void {
        if (this._active !== turn) {
            return;
        }
        this._active = null;
        this.settle(turn, outcome, error);
        if (outcome === "reject") {
            this._onError(error);
        }
        this.schedulePump();
    }

    private settle(turn: Turn<Tag> | null, outcome: "reject" | "resolve", error?: unknown): void {
        if (!turn || !("settlement" in turn) || !turn.settlement) {
            return;
        }
        if (outcome === "resolve") {
            turn.settlement.resolve();
        } else {
            turn.settlement.reject(error);
        }
    }

    private takeNext(): TaggedTurn<Tag> | undefined {
        if (this._ready.length === 0) {
            this._ready = this._incoming.reverse();
            this._incoming = [];
        }
        return this._ready.pop();
    }

    private takePending(): TaggedTurn<Tag>[] {
        const pending = this._ready.reverse();
        pending.push(...this._incoming);
        this._incoming = [];
        this._ready = [];
        return pending;
    }
}
