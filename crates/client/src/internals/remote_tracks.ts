import type { TrackBinding } from "../protocol_contract.js";
import {
    CLIENT_UPDATE,
    type ClientUpdateDetail,
    type DownloadStates,
    type SessionId,
    type StreamType
} from "../public_api.js";
import {
    createEmptyConsumers,
    type ConsumersCompat,
    type MediaTrack,
    type PeerConnectionTrackEvent
} from "./browser_types.js";
import { mergeDownloadStates } from "./validation.js";

type TrackUpdateEmitter = (update: ClientUpdateDetail) => void;

type SlotBinding = Pick<TrackBinding, "active" | "sessionId" | "type">;
type RemoteMediaSlot = { binding?: SlotBinding; track?: MediaTrack; unbindTrack?: () => void };

export class RemoteTracks {
    public readonly consumers = new Map<SessionId, ConsumersCompat>();

    private _slots = new Map<string, RemoteMediaSlot>();
    private _subscriptionStates = new Map<SessionId, DownloadStates>();

    resetAll(): void {
        this.clearPeerConnectionState();
        this._subscriptionStates.clear();
    }

    clearPeerConnectionState(): void {
        this.consumers.clear();
        for (const slot of this._slots.values()) {
            this.clearSlotTrack(slot);
        }
        this._slots.clear();
    }

    replaceTrackBindings(bindings: TrackBinding[], emitUpdate: TrackUpdateEmitter): void {
        const nextBindings = new Map<string, TrackBinding>();
        for (const binding of bindings) {
            nextBindings.set(binding.mid, binding);
        }

        for (const mid of this._slots.keys()) {
            if (this._slots.get(mid)?.binding && !nextBindings.has(mid)) {
                this.removeSlot(mid);
            }
        }

        for (const [mid, binding] of nextBindings) {
            this.applyBinding(mid, binding, emitUpdate);
        }
    }

    removeSessionTracks(sessionId: SessionId): void {
        this.consumers.delete(sessionId);
        for (const [mid, slot] of this._slots) {
            if (slot.binding?.sessionId === sessionId) {
                this.removeSlot(mid);
            }
        }
    }

    updateSubscriptionStates(
        sessionId: SessionId,
        states: DownloadStates,
        emitUpdate: TrackUpdateEmitter
    ): void {
        const previousStates = this._subscriptionStates.get(sessionId);
        const nextStates = mergeDownloadStates(previousStates, states);
        if (Object.keys(nextStates).length === 0) {
            this._subscriptionStates.delete(sessionId);
        } else {
            this._subscriptionStates.set(sessionId, nextStates);
        }
        const previousDownloadStates = previousStates ?? {};
        for (const slot of this._slots.values()) {
            if (slot.binding?.sessionId !== sessionId) {
                continue;
            }
            const previous = this.applySubscriptionState(slot.binding, previousDownloadStates);
            this.publishSlot(slot, previous, emitUpdate);
        }
    }

    handleTrackEvent(event: PeerConnectionTrackEvent, emitUpdate: TrackUpdateEmitter): void {
        const mid = event.transceiver.mid;
        if (!mid) {
            return;
        }
        const slot = this.getOrCreateSlot(mid);
        const previous = slot.binding
            ? this.applyCurrentSubscriptionState(slot.binding)
            : undefined;
        this.clearSlotTrack(slot);
        slot.track = event.track;
        this.bindTrackLifecycle(slot, mid, event.track, emitUpdate);
        this.publishSlot(slot, previous, emitUpdate);
    }

    private applyBinding(mid: string, binding: TrackBinding, emitUpdate: TrackUpdateEmitter): void {
        const slot = this.getOrCreateSlot(mid);
        const previous = slot.binding
            ? this.applyCurrentSubscriptionState(slot.binding)
            : undefined;
        const { active, sessionId, type } = binding;
        const rebinding =
            previous !== undefined && (previous.sessionId !== sessionId || previous.type !== type);
        if (rebinding) {
            this.clearConsumer(previous.sessionId, previous.type);
            this.clearSlotTrack(slot);
        }
        slot.binding = { active, sessionId, type };
        if (!rebinding) {
            this.publishSlot(slot, previous, emitUpdate);
        }
    }

    private publishSlot(
        slot: RemoteMediaSlot,
        previous: SlotBinding | undefined,
        emitUpdate: TrackUpdateEmitter,
        force = false
    ): void {
        const { binding, track } = slot;
        if (!binding || !track) {
            return;
        }
        const appliedBinding = this.applyCurrentSubscriptionState(binding);
        if (
            !force &&
            previous &&
            previous.active === appliedBinding.active &&
            previous.sessionId === appliedBinding.sessionId &&
            previous.type === appliedBinding.type &&
            this.consumers.get(appliedBinding.sessionId)?.[appliedBinding.type]?.track === track
        ) {
            return;
        }
        if (previous) {
            this.clearConsumer(previous.sessionId, previous.type);
        }
        const consumers = this.consumers.get(appliedBinding.sessionId) ?? createEmptyConsumers();
        consumers[appliedBinding.type] = {
            track
        };
        this.consumers.set(appliedBinding.sessionId, consumers);
        emitUpdate({
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: appliedBinding.active,
                sessionId: appliedBinding.sessionId,
                track,
                type: appliedBinding.type
            }
        });
    }

    private bindTrackLifecycle(
        currentSlot: RemoteMediaSlot,
        mid: string,
        track: MediaTrack,
        emitUpdate: TrackUpdateEmitter
    ): void {
        if (!("addEventListener" in track) || typeof track.addEventListener !== "function") {
            return;
        }
        const emitTrackUpdate = () => {
            const slot = this._slots.get(mid);
            if (!slot?.binding || slot.track !== track) {
                return;
            }
            const previous = this.applyCurrentSubscriptionState(slot.binding);
            this.publishSlot(slot, previous, emitUpdate, true);
        };
        track.addEventListener("mute", emitTrackUpdate);
        track.addEventListener("unmute", emitTrackUpdate);
        currentSlot.unbindTrack = () => {
            track.removeEventListener("mute", emitTrackUpdate);
            track.removeEventListener("unmute", emitTrackUpdate);
        };
    }

    private applyCurrentSubscriptionState(binding: SlotBinding): SlotBinding {
        return this.applySubscriptionState(
            binding,
            this._subscriptionStates.get(binding.sessionId)
        );
    }

    private applySubscriptionState(
        binding: SlotBinding,
        states: DownloadStates | undefined
    ): SlotBinding {
        return {
            active: binding.active && (states?.[binding.type] ?? true),
            sessionId: binding.sessionId,
            type: binding.type
        };
    }

    private removeSlot(mid: string): void {
        const slot = this._slots.get(mid);
        if (!slot) {
            return;
        }
        if (slot.binding) {
            this.clearConsumer(slot.binding.sessionId, slot.binding.type);
        }
        this.clearSlotTrack(slot);
        this._slots.delete(mid);
    }

    private getOrCreateSlot(mid: string): RemoteMediaSlot {
        let slot = this._slots.get(mid);
        if (slot) {
            return slot;
        }
        slot = {};
        this._slots.set(mid, slot);
        return slot;
    }

    private clearSlotTrack(slot: RemoteMediaSlot): void {
        slot.unbindTrack?.();
        slot.unbindTrack = undefined;
        slot.track = undefined;
    }

    private clearConsumer(sessionId: SessionId, streamType: StreamType): void {
        const consumers = this.consumers.get(sessionId);
        if (!consumers) {
            return;
        }
        consumers[streamType] = null;
        if (!consumers.audio && !consumers.camera && !consumers.screen) {
            this.consumers.delete(sessionId);
        }
    }
}
