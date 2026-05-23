/**
 * remote media track management
 *
 * this module tracks remote media streams received from the sfu and binds
 * them to their protocol-level sessions and types. it handles track events
 * from the peer connection and propagates updates to the client
 */

import type { TrackBinding } from "../protocol.js";
import {
    CLIENT_UPDATE,
    type ClientUpdateDetail,
    type DownloadStates,
    type SessionId,
    type StreamType
} from "../public_api.js";
import {
    createEmptyConsumers,
    type AppliedTrackBinding,
    type ConsumersCompat,
    type MediaTrack,
    type PeerConnectionTrackEvent
} from "./browser_types.js";

type TrackUpdateEmitter = (update: ClientUpdateDetail) => void;

/**
 * manager for remote media tracks and their session bindings
 */
export class RemoteTracks {
    public readonly consumers = new Map<SessionId, ConsumersCompat>();

    private _remoteTrackBindings = new Map<string, AppliedTrackBinding>();
    private _remoteTracksByMid = new Map<string, MediaTrack>();
    private _subscriptionStates = new Map<SessionId, DownloadStates>();
    private _staleRemoteTrackMids = new Set<string>();

    /**
     * clears all remote track state and bindings
     */
    resetAll(): void {
        this.clearPeerConnectionState();
        this._remoteTrackBindings.clear();
        this._subscriptionStates.clear();
    }

    /**
     * clears peer connection specific state while keeping session bindings
     */
    clearPeerConnectionState(): void {
        this.consumers.clear();
        this._remoteTracksByMid.clear();
        this._staleRemoteTrackMids.clear();
    }

    /**
     * updates the set of active track bindings from the protocol core
     *
     * @param bindings new list of track-to-session bindings
     * @param emitUpdate callback for propagating track updates
     */
    replaceTrackBindings(bindings: TrackBinding[], emitUpdate: TrackUpdateEmitter): void {
        const nextBindings = new Map(bindings.map((binding) => [binding.mid, binding]));

        for (const mid of [...this._remoteTrackBindings.keys()]) {
            if (!nextBindings.has(mid)) {
                this.removeBinding(mid);
            }
        }

        for (const [mid, binding] of nextBindings) {
            this.applyBinding(mid, binding, emitUpdate);
        }
    }

    /**
     * removes all tracks associated with a specific session
     *
     * @param sessionId id of the session whose tracks should be removed
     */
    removeSessionTracks(sessionId: SessionId): void {
        this.consumers.delete(sessionId);
        for (const [mid, binding] of [...this._remoteTrackBindings]) {
            if (binding.sessionId === sessionId) {
                this.removeBinding(mid);
            }
        }
    }

    /**
     * updates the desired subscription state for a session
     *
     * @param sessionId id of the session to update
     * @param states new desired download states (audio, video, etc)
     * @param emitUpdate callback for propagating resulting track updates
     */
    updateSubscriptionStates(
        sessionId: SessionId,
        states: DownloadStates,
        emitUpdate: TrackUpdateEmitter
    ): void {
        const previousStates = this._subscriptionStates.get(sessionId);
        const nextStates = this.mergeSubscriptionStates(previousStates, states);
        if (this.downloadStatesAreEmpty(nextStates)) {
            this._subscriptionStates.delete(sessionId);
        } else {
            this._subscriptionStates.set(sessionId, nextStates);
        }
        for (const [mid, binding] of this._remoteTrackBindings) {
            if (binding.sessionId !== sessionId) {
                continue;
            }
            const previousBinding = this.applySubscriptionState(binding, previousStates);
            const nextBinding = this.applySubscriptionState(binding, nextStates);
            this.publishTrack(mid, nextBinding, previousBinding, emitUpdate);
        }
    }

    /**
     * handles a new remote track event from the peer connection
     *
     * @param event rtc track event
     * @param emitUpdate callback for propagating the new track
     */
    handleTrackEvent(event: PeerConnectionTrackEvent, emitUpdate: TrackUpdateEmitter): void {
        const mid = event.transceiver.mid;
        if (!mid) {
            return;
        }
        this._remoteTracksByMid.set(mid, event.track);
        this._staleRemoteTrackMids.delete(mid);
        this.bindTrackLifecycle(mid, event.track, emitUpdate);
        const binding = this._remoteTrackBindings.get(mid);
        if (!binding) {
            return;
        }
        const appliedBinding = this.applyCurrentSubscriptionState(binding);
        this.publishTrack(mid, appliedBinding, appliedBinding, emitUpdate);
    }

    private applyBinding(mid: string, binding: TrackBinding, emitUpdate: TrackUpdateEmitter): void {
        const previousBinding = this._remoteTrackBindings.get(mid);
        const bindingIdentityChanged = this.bindingIdentityChanged(previousBinding, binding);
        if (bindingIdentityChanged && previousBinding) {
            this.clearConsumer(previousBinding.sessionId, previousBinding.type);
            this._staleRemoteTrackMids.add(mid);
        }
        const appliedBinding = {
            active: binding.active,
            sessionId: binding.sessionId,
            type: binding.type
        };
        this._remoteTrackBindings.set(mid, appliedBinding);
        if (this._staleRemoteTrackMids.has(mid)) {
            return;
        }
        this.publishTrack(
            mid,
            this.applyCurrentSubscriptionState(appliedBinding),
            previousBinding ? this.applyCurrentSubscriptionState(previousBinding) : undefined,
            emitUpdate
        );
    }

    private publishTrack(
        mid: string,
        binding: AppliedTrackBinding,
        previousBinding: AppliedTrackBinding | undefined,
        emitUpdate: TrackUpdateEmitter,
        force = false
    ): void {
        const track = this._remoteTracksByMid.get(mid);
        if (!track) {
            return;
        }
        if (
            !force &&
            previousBinding &&
            previousBinding.active === binding.active &&
            previousBinding.sessionId === binding.sessionId &&
            previousBinding.type === binding.type &&
            this.consumers.get(binding.sessionId)?.[binding.type]?.track === track
        ) {
            return;
        }
        if (previousBinding) {
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

    private bindTrackLifecycle(
        mid: string,
        track: MediaTrack,
        emitUpdate: TrackUpdateEmitter
    ): void {
        if (!("addEventListener" in track) || typeof track.addEventListener !== "function") {
            return;
        }
        const emitTrackUpdate = () => {
            const binding = this._remoteTrackBindings.get(mid);
            if (!binding || this._remoteTracksByMid.get(mid) !== track) {
                return;
            }
            const appliedBinding = this.applyCurrentSubscriptionState(binding);
            this.publishTrack(mid, appliedBinding, appliedBinding, emitUpdate, true);
        };
        track.addEventListener("mute", emitTrackUpdate);
        track.addEventListener("unmute", emitTrackUpdate);
    }

    private applyCurrentSubscriptionState(binding: AppliedTrackBinding): AppliedTrackBinding {
        return this.applySubscriptionState(
            binding,
            this._subscriptionStates.get(binding.sessionId)
        );
    }

    private applySubscriptionState(
        binding: AppliedTrackBinding,
        states: DownloadStates | undefined
    ): AppliedTrackBinding {
        return {
            active: binding.active && this.downloadStateForStreamType(states, binding.type),
            sessionId: binding.sessionId,
            type: binding.type
        };
    }

    private mergeSubscriptionStates(
        previous: DownloadStates | undefined,
        next: DownloadStates
    ): DownloadStates {
        const merged = { ...(previous ?? {}) };
        for (const streamType of ["audio", "camera", "screen"] as const) {
            if (next[streamType] === undefined) {
                continue;
            }
            merged[streamType] = next[streamType];
        }
        if (next.cameraLayout !== undefined) {
            merged.cameraLayout = next.cameraLayout;
        }
        if (next.screenLayout !== undefined) {
            merged.screenLayout = next.screenLayout;
        }
        return merged;
    }

    private downloadStateForStreamType(
        states: DownloadStates | undefined,
        streamType: StreamType
    ): boolean {
        return states?.[streamType] ?? true;
    }

    private downloadStatesAreEmpty(states: DownloadStates): boolean {
        return (
            states.audio === undefined &&
            states.camera === undefined &&
            states.screen === undefined &&
            states.cameraLayout === undefined &&
            states.screenLayout === undefined
        );
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

    private removeBinding(mid: string): void {
        const previousBinding = this._remoteTrackBindings.get(mid);
        if (previousBinding) {
            this.clearConsumer(previousBinding.sessionId, previousBinding.type);
            this._remoteTrackBindings.delete(mid);
        }
        this._remoteTracksByMid.delete(mid);
        this._staleRemoteTrackMids.delete(mid);
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
