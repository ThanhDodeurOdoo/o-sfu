import type { TrackBinding } from "../protocol_contract.js";
import {
    CLIENT_UPDATE,
    type ClientUpdateDetail,
    type ConsumersCompat,
    type DownloadStates,
    type SessionId,
    type StreamType
} from "../public_api.js";
import type { MediaTrack, PeerConnectionTrackEvent } from "./browser_types.js";
import { mergeDownloadStates } from "./validation.js";

type TrackUpdateEmitter = (update: ClientUpdateDetail) => void;

type SlotBinding = Pick<TrackBinding, "active" | "sessionId" | "type">;
type RemoteMediaSlot = {
    binding?: SlotBinding;
    track?: MediaTrack;
    removeTrackListeners?: () => void;
};

export class RemoteMedia {
    public readonly consumers = new Map<SessionId, ConsumersCompat>();

    private _slots = new Map<string, RemoteMediaSlot>();
    private _subscriptionStates = new Map<SessionId, DownloadStates>();

    clearSessionState(): void {
        this.clearPeerMedia();
        this._subscriptionStates.clear();
    }

    clearPeerMedia(): void {
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

        for (const [mid, slot] of this._slots) {
            if (slot.binding && !nextBindings.has(mid)) {
                this.removeSlot(mid);
            }
        }

        for (const [mid, binding] of nextBindings) {
            this.applyBinding(mid, binding, emitUpdate);
        }
    }

    removeSession(sessionId: SessionId): void {
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
        for (const slot of this._slots.values()) {
            if (slot.binding?.sessionId !== sessionId) {
                continue;
            }
            const previousEffectiveBinding = this.applySubscriptionState(
                slot.binding,
                previousStates
            );
            this.projectTrackSlot(slot, previousEffectiveBinding, emitUpdate);
        }
    }

    handleTrackEvent(event: PeerConnectionTrackEvent, emitUpdate: TrackUpdateEmitter): void {
        const mid = event.transceiver.mid;
        if (!mid) {
            return;
        }
        const slot = this.getOrCreateSlot(mid);
        const previousEffectiveBinding = slot.binding
            ? this.effectiveBinding(slot.binding)
            : undefined;
        this.clearSlotTrack(slot);
        slot.track = event.track;
        this.attachTrackListeners(slot, mid, event.track, emitUpdate);
        this.projectTrackSlot(slot, previousEffectiveBinding, emitUpdate);
    }

    private applyBinding(mid: string, binding: TrackBinding, emitUpdate: TrackUpdateEmitter): void {
        const slot = this.getOrCreateSlot(mid);
        const previousEffectiveBinding = slot.binding
            ? this.effectiveBinding(slot.binding)
            : undefined;
        const { active, sessionId, type } = binding;
        const rebinding =
            previousEffectiveBinding !== undefined &&
            (previousEffectiveBinding.sessionId !== sessionId ||
                previousEffectiveBinding.type !== type);
        if (rebinding) {
            this.clearConsumer(previousEffectiveBinding.sessionId, previousEffectiveBinding.type);
            this.clearSlotTrack(slot);
        }
        slot.binding = { active, sessionId, type };
        if (!rebinding) {
            this.projectTrackSlot(slot, previousEffectiveBinding, emitUpdate);
        }
    }

    private projectTrackSlot(
        slot: RemoteMediaSlot,
        previousEffectiveBinding: SlotBinding | undefined,
        emitUpdate: TrackUpdateEmitter,
        forceEmit = false
    ): void {
        const { binding, track } = slot;
        if (!binding || !track) {
            return;
        }
        const effectiveBinding = this.effectiveBinding(binding);
        if (
            !forceEmit &&
            previousEffectiveBinding &&
            previousEffectiveBinding.active === effectiveBinding.active &&
            previousEffectiveBinding.sessionId === effectiveBinding.sessionId &&
            previousEffectiveBinding.type === effectiveBinding.type &&
            this.consumers.get(effectiveBinding.sessionId)?.[effectiveBinding.type]?.track === track
        ) {
            return;
        }
        if (previousEffectiveBinding) {
            this.clearConsumer(previousEffectiveBinding.sessionId, previousEffectiveBinding.type);
        }
        const consumers: ConsumersCompat = this.consumers.get(effectiveBinding.sessionId) ?? {
            audio: null,
            camera: null,
            screen: null
        };
        consumers[effectiveBinding.type] = { track };
        this.consumers.set(effectiveBinding.sessionId, consumers);
        emitUpdate({
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: effectiveBinding.active,
                sessionId: effectiveBinding.sessionId,
                track,
                type: effectiveBinding.type
            }
        });
    }

    private attachTrackListeners(
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
            const previousEffectiveBinding = this.effectiveBinding(slot.binding);
            this.projectTrackSlot(slot, previousEffectiveBinding, emitUpdate, true);
        };
        track.addEventListener("mute", emitTrackUpdate);
        track.addEventListener("unmute", emitTrackUpdate);
        currentSlot.removeTrackListeners = () => {
            track.removeEventListener("mute", emitTrackUpdate);
            track.removeEventListener("unmute", emitTrackUpdate);
        };
    }

    private effectiveBinding(binding: SlotBinding): SlotBinding {
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
        slot.removeTrackListeners?.();
        slot.removeTrackListeners = undefined;
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
