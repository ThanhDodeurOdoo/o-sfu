import {
    CLIENT_LOG_LEVEL,
    STREAM_TYPES,
    type ClientLogDetail,
    type ClientUpdateDetail,
    type SfuStats,
    type StreamType
} from "../public_api.js";
import type { NegotiationUploadSlot } from "../protocol.js";
import type { NegotiationKind } from "../runtime_contract.js";
import type { ClientPeerConnection } from "./browser_types.js";
import type { LocalUploads } from "./local_uploads.js";
import type { RemoteTracks } from "./remote_tracks.js";
import { localDescriptionHasOnlyInactiveMedia } from "./sdp_media_direction.js";

type RuntimeLog = (level: ClientLogDetail["level"], message: string) => void;

type NegotiationAnswer = {
    answerSdp: string;
    shouldSignalTransportReady: boolean;
};

export class PeerSession {
    private _iceServers?: RTCIceServer[];
    private _activePeer: ClientPeerConnection | null = null;

    constructor(
        private readonly _create: (config: RTCConfiguration) => ClientPeerConnection,
        private readonly _uploads: LocalUploads,
        private readonly _tracks: RemoteTracks,
        private readonly _onUpdate: (update: ClientUpdateDetail) => void,
        private readonly _onTransportReady: () => void,
        private readonly _closeSocketForTransportFailure: () => void,
        private readonly _log: RuntimeLog
    ) {}

    setIceServers(iceServers?: RTCIceServer[]): void {
        this._iceServers = iceServers;
    }

    create(): void {
        this.close();
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
            this._tracks.handleTrackEvent(event, this._onUpdate);
        };
        this._activePeer = peer;
    }

    close(): void {
        const peer = this._activePeer;
        if (peer) {
            peer.close();
            this._log(CLIENT_LOG_LEVEL.INFO, "closed RTCPeerConnection");
            this._uploads.clearPeerConnectionState();
        }
        this._tracks.clearPeerConnectionState();
        this._activePeer = null;
    }

    async attachTrack(mid: string, streamType: StreamType): Promise<void> {
        await this._uploads.attachTrack(this._activePeer, mid, streamType);
    }

    async detachTrack(streamType: StreamType): Promise<void> {
        await this._uploads.detachTrack(this._activePeer, streamType);
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
        uploadSlots: NegotiationUploadSlot[]
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
        await this._uploads.attachPendingTracks(peer, uploadSlots);
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
