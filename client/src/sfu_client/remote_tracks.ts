import type { TrackBinding } from "../protocol.js";
import {
    CLIENT_UPDATE,
    type ClientUpdateDetail,
    type SessionId,
    type StreamType
} from "../public_api.js";
import type { ProtocolCoreBindings } from "../runtime_contract.js";
import {
    createEmptyConsumers,
    type AppliedTrackBinding,
    type ConsumersCompat,
    type RtcTrackEventLike,
    type TrackLike
} from "./browser_types.js";

type TrackUpdateEmitter = (update: ClientUpdateDetail) => void;

export class RemoteTracks {
    public readonly consumers = new Map<SessionId, ConsumersCompat>();

    private _remoteTrackBindings = new Map<string, AppliedTrackBinding>();
    private _remoteTracksByMid = new Map<string, TrackLike>();
    private _staleRemoteTrackMids = new Set<string>();

    clearPeerConnectionState(): void {
        this.consumers.clear();
        this._remoteTrackBindings.clear();
        this._remoteTracksByMid.clear();
        this._staleRemoteTrackMids.clear();
    }

    applyCompatUpdate(update: ClientUpdateDetail): void {
        if (update.name !== CLIENT_UPDATE.DISCONNECT) {
            return;
        }
        const { sessionId } = update.payload;
        this.consumers.delete(sessionId);
        for (const [mid, binding] of this._remoteTrackBindings) {
            if (binding.sessionId !== sessionId) {
                continue;
            }
            this._remoteTrackBindings.delete(mid);
            this._remoteTracksByMid.delete(mid);
            this._staleRemoteTrackMids.delete(mid);
        }
    }

    handleTrackEvent(
        event: RtcTrackEventLike,
        protocolCore: ProtocolCoreBindings,
        emitUpdate: TrackUpdateEmitter
    ): void {
        const mid = event.transceiver.mid;
        if (!mid) {
            return;
        }
        this._remoteTracksByMid.set(mid, event.track);
        this._staleRemoteTrackMids.delete(mid);
        this.syncTrack(mid, protocolCore, emitUpdate);
    }

    syncAll(protocolCore: ProtocolCoreBindings, emitUpdate: TrackUpdateEmitter): void {
        for (const mid of this._remoteTracksByMid.keys()) {
            this.syncTrack(mid, protocolCore, emitUpdate);
        }
    }

    private syncTrack(
        mid: string,
        protocolCore: ProtocolCoreBindings,
        emitUpdate: TrackUpdateEmitter
    ): void {
        const binding = protocolCore.trackBinding(mid);
        const previousBinding = this._remoteTrackBindings.get(mid);
        if (!binding) {
            if (previousBinding) {
                this.clearConsumer(previousBinding.sessionId, previousBinding.type);
                this._remoteTrackBindings.delete(mid);
            }
            this._remoteTracksByMid.delete(mid);
            this._staleRemoteTrackMids.delete(mid);
            return;
        }
        const bindingIdentityChanged = this.bindingIdentityChanged(previousBinding, binding);
        if (bindingIdentityChanged && previousBinding) {
            this.clearConsumer(previousBinding.sessionId, previousBinding.type);
            this._staleRemoteTrackMids.add(mid);
        }
        this._remoteTrackBindings.set(mid, {
            active: binding.active,
            sessionId: binding.sessionId,
            type: binding.type
        });
        const track = this._remoteTracksByMid.get(mid);
        if (!track || this._staleRemoteTrackMids.has(mid)) {
            return;
        }
        if (
            previousBinding &&
            !bindingIdentityChanged &&
            previousBinding.active === binding.active &&
            previousBinding.sessionId === binding.sessionId &&
            previousBinding.type === binding.type &&
            this.consumers.get(binding.sessionId)?.[binding.type]?.track === track
        ) {
            return;
        }
        if (previousBinding && !bindingIdentityChanged) {
            this.clearConsumer(previousBinding.sessionId, previousBinding.type);
        }
        const consumers = this.consumers.get(binding.sessionId) ?? createEmptyConsumers();
        consumers[binding.type] = {
            track
        };
        this.consumers.set(binding.sessionId, consumers);
        emitUpdate({
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: binding.active,
                sessionId: binding.sessionId,
                track,
                type: binding.type
            }
        });
    }

    private bindingIdentityChanged(
        previousBinding: AppliedTrackBinding | undefined,
        binding: TrackBinding
    ): boolean {
        return Boolean(
            previousBinding &&
            (previousBinding.sessionId !== binding.sessionId ||
                previousBinding.type !== binding.type)
        );
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
