import {
    CLIENT_LOG_LEVEL,
    SFU_CLIENT_STATE,
    STREAM_TYPES,
    type ClientLogDetail,
    type ClientUpdateDetail,
    type ConnectionState,
    type SfuStats,
    type StreamType
} from "../public_api.js";
import type { NegotiationKind } from "../protocol_contract.js";
import type { ClientPeerConnection } from "./browser_types.js";
import { LocalUploads, type UploadSlot } from "./local_uploads.js";
import type { RemoteMedia } from "./remote_media.js";
import { localDescriptionHasOnlyInactiveMedia } from "./sdp_media_direction.js";

type RuntimeLog = (level: ClientLogDetail["level"], message: string) => void;

type NegotiationAnswer = {
    answerSdp: string;
    shouldSignalTransportReady: boolean;
};

type PublicationEffect = { active?: boolean; peerTask?: () => Promise<void> };

export class PeerSession {
    private _iceServers?: RTCIceServer[];
    private _activePeer: ClientPeerConnection | null = null;
    private readonly _uploads = new LocalUploads();

    constructor(
        private readonly _create: (config: RTCConfiguration) => ClientPeerConnection,
        private readonly _media: RemoteMedia,
        private readonly _onUpdate: (update: ClientUpdateDetail) => void,
        private readonly _onTransportReady: () => void,
        private readonly _closeSocketForTransportFailure: () => void,
        private readonly _log: RuntimeLog
    ) {}

    setIceServers(iceServers?: RTCIceServer[]): void {
        this._iceServers = iceServers;
    }

    resumePublications(): void {
        this._uploads.resumePublications();
    }

    clearPublications(): void {
        this._uploads.clearPublications();
    }

    setPublication(
        type: StreamType,
        track: MediaStreamTrack | null,
        state: ConnectionState
    ): PublicationEffect {
        const canAttach =
            state === SFU_CLIENT_STATE.AUTHENTICATED || state === SFU_CLIENT_STATE.CONNECTED;
        const transition = this._uploads.setPublication(type, track, canAttach);
        if (!transition.hadTrack && !transition.hasTrack) {
            return {};
        }
        const replacing = transition.hadTrack && transition.hasTrack;
        const resuming =
            !transition.hadTrack && transition.hasTrack && transition.boundMid !== undefined;
        let action = "publishing";
        if (replacing) {
            action = "replacing";
        } else if (resuming) {
            action = "resuming";
        } else if (transition.hadTrack) {
            action = "pausing";
        }
        this._log(
            replacing ? CLIENT_LOG_LEVEL.DEBUG : CLIENT_LOG_LEVEL.INFO,
            `${action} ${type} track${transition.boundMid ? ` on mid ${transition.boundMid}` : ""}`
        );
        let peerTask: PublicationEffect["peerTask"];
        if (transition.boundMid !== undefined) {
            const mid = transition.boundMid;
            if (transition.hasTrack) {
                peerTask = () => this._uploads.attachTrack(this._activePeer, mid, type);
            } else {
                peerTask = () => this._uploads.detachTrack(this._activePeer, type);
            }
        }
        if (transition.hadTrack === transition.hasTrack) {
            return { peerTask };
        }
        return {
            active: transition.hasTrack,
            peerTask
        };
    }

    create(): void {
        if (this._activePeer) {
            this.close();
        }
        const peer = this._create({
            iceServers: this._iceServers
        });
        this._log(CLIENT_LOG_LEVEL.DEBUG, "created RTCPeerConnection");
        peer.onconnectionstatechange = () => this.handleConnectionState(peer);
        peer.ontrack = (event) => {
            if (this._activePeer !== peer) {
                return;
            }
            this._log(
                CLIENT_LOG_LEVEL.DEBUG,
                `received remote track event for mid ${event.transceiver.mid ?? "unknown"} (kind=${event.track.kind})`
            );
            this._media.handleTrackEvent(event, this._onUpdate);
        };
        this._activePeer = peer;
    }

    close(): void {
        const peer = this._activePeer;
        this._activePeer = null;
        this._uploads.clearPeerConnectionState();
        this._media.clearPeerConnectionState();
        if (peer) {
            peer.close();
            this._log(CLIENT_LOG_LEVEL.INFO, "closed RTCPeerConnection");
        }
    }

    async getStats(): Promise<SfuStats> {
        const peer = this._activePeer;
        if (!peer) {
            return {};
        }
        const stats: SfuStats = {};
        if (typeof peer.getStats === "function") {
            const peerStats = await peer.getStats();
            stats.uploadStats = peerStats;
            stats.downloadStats = peerStats;
        }
        for (const streamType of STREAM_TYPES) {
            const mid = this._uploads.boundMidFor(streamType);
            if (!mid) {
                continue;
            }
            const transceiver = peer.getTransceivers().find((candidate) => candidate.mid === mid);
            if (transceiver && typeof transceiver.sender.getStats === "function") {
                stats[streamType] = await transceiver.sender.getStats();
            }
        }
        return stats;
    }

    async negotiate(
        requestId: string,
        negotiationKind: NegotiationKind,
        offerSdp: string,
        uploadSlots: UploadSlot[]
    ): Promise<NegotiationAnswer | null> {
        const peer = this._activePeer;
        if (!peer) {
            throw new Error("received negotiation command without an active peer connection");
        }
        this._log(
            CLIENT_LOG_LEVEL.DEBUG,
            `applying ${negotiationKind} negotiation request ${requestId}`
        );
        await peer.setRemoteDescription({
            sdp: offerSdp,
            type: "offer"
        });
        if (!this.isActive(peer)) {
            return null;
        }
        await this._uploads.attachPendingTracks(peer, uploadSlots, offerSdp);
        if (!this.isActive(peer)) {
            return null;
        }
        const answer = await peer.createAnswer();
        if (!this.isActive(peer)) {
            return null;
        }
        await peer.setLocalDescription(answer);
        if (!this.isActive(peer)) {
            return null;
        }
        const answerSdp = await this.awaitStableLocalDescription(peer);
        if (!this.isActive(peer)) {
            return null;
        }
        this._log(
            CLIENT_LOG_LEVEL.DEBUG,
            `answered ${negotiationKind} negotiation request ${requestId}`
        );
        if (negotiationKind !== "offer") {
            return {
                answerSdp,
                shouldSignalTransportReady: false
            };
        }
        const connected = peer.connectionState === "connected";
        const needsTransportReadyFallback =
            !connected &&
            (typeof peer.connectionState !== "string" ||
                localDescriptionHasOnlyInactiveMedia(answerSdp));
        if (needsTransportReadyFallback) {
            this._log(
                CLIENT_LOG_LEVEL.WARN,
                "falling back to immediate transport-ready because the initial answer stayed inactive"
            );
        }
        return {
            answerSdp,
            shouldSignalTransportReady: connected || needsTransportReadyFallback
        };
    }

    private handleConnectionState(peer: ClientPeerConnection): void {
        if (!this.isActive(peer)) {
            return;
        }
        const state = peer.connectionState;
        this._log(
            state === "failed" ? CLIENT_LOG_LEVEL.WARN : CLIENT_LOG_LEVEL.DEBUG,
            `peer connection state changed to ${state}`
        );
        if (state === "connected") {
            this._onTransportReady();
        } else if (state === "failed") {
            this._closeSocketForTransportFailure();
        }
    }

    private async awaitStableLocalDescription(peer: ClientPeerConnection): Promise<string> {
        const initialSdp = peer.localDescription?.sdp;
        if (!initialSdp) {
            throw new Error("peer connection local description is missing after createAnswer");
        }
        if (peer.iceGatheringState === "complete") {
            return initialSdp;
        }

        return new Promise((resolve) => {
            const previousIceCandidate = peer.onicecandidate;
            const previousIceGatheringStateChange = peer.onicegatheringstatechange;
            const finishIfReady = (candidate: { candidate: string } | null | undefined) => {
                const localSdp = peer.localDescription?.sdp;
                if (!localSdp) {
                    return;
                }
                if (candidate !== null && peer.iceGatheringState !== "complete") {
                    return;
                }
                peer.onicecandidate = previousIceCandidate;
                peer.onicegatheringstatechange = previousIceGatheringStateChange;
                resolve(localSdp);
            };

            peer.onicecandidate = (event) => {
                previousIceCandidate?.(event);
                finishIfReady(event.candidate);
            };
            peer.onicegatheringstatechange = () => {
                previousIceGatheringStateChange?.();
                if (peer.iceGatheringState === "complete") {
                    finishIfReady(undefined);
                }
            };
            if (peer.iceGatheringState === "complete") {
                finishIfReady(undefined);
            }
        });
    }

    private isActive(peer: ClientPeerConnection): boolean {
        return this._activePeer === peer;
    }
}
