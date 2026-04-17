import type { StreamType } from "../public_api.js";
import { STREAM_KIND, type ClientPeerConnection, type MediaTrack } from "./browser_types.js";

type UploadTransition = {
    hadTrack: boolean;
    hasTrack: boolean;
    knownMid?: string;
};

export class LocalUploads {
    private _localTracks = new Map<StreamType, MediaTrack | null>();
    private _senderMidByType = new Map<StreamType, string>();

    setTrack(type: StreamType, track: MediaStreamTrack | null): UploadTransition {
        const previousTrack = this._localTracks.get(type) ?? null;
        this._localTracks.set(type, track);
        return {
            hadTrack: previousTrack !== null,
            hasTrack: track !== null,
            knownMid: this._senderMidByType.get(type)
        };
    }

    clearPeerConnectionState(): void {
        this._senderMidByType.clear();
    }

    boundMidFor(streamType: StreamType): string | undefined {
        return this._senderMidByType.get(streamType);
    }

    async attachTrack(
        peerConnection: ClientPeerConnection | null,
        mid: string,
        streamType: StreamType
    ): Promise<void> {
        if (!peerConnection) {
            throw new Error("cannot attach track without an active peer connection");
        }
        const track = this._localTracks.get(streamType) ?? null;
        const transceiver = peerConnection
            .getTransceivers()
            .find((candidate) => candidate.mid === mid);
        if (!transceiver) {
            throw new Error(`missing transceiver for mid ${mid}`);
        }
        await transceiver.sender.replaceTrack(track);
        updateTransceiverDirection(transceiver, track);
        this._senderMidByType.set(streamType, mid);
    }

    async detachTrack(
        peerConnection: ClientPeerConnection | null,
        streamType: StreamType
    ): Promise<void> {
        if (!peerConnection) {
            return;
        }
        const knownMid = this._senderMidByType.get(streamType);
        const transceiver = peerConnection
            .getTransceivers()
            .find((candidate) =>
                knownMid
                    ? candidate.mid === knownMid
                    : candidate.sender.track?.kind === STREAM_KIND[streamType]
            );
        if (transceiver) {
            await transceiver.sender.replaceTrack(null);
            updateTransceiverDirection(transceiver, null);
        }
        this._senderMidByType.delete(streamType);
    }

    async attachPendingRenegotiationTracks(
        peerConnection: ClientPeerConnection | null
    ): Promise<void> {
        if (!peerConnection) {
            return;
        }
        const pendingTracks = orderedStreamTypes().filter(
            (streamType) =>
                (this._localTracks.get(streamType) ?? null) !== null &&
                !this._senderMidByType.has(streamType)
        );
        if (pendingTracks.length === 0) {
            return;
        }
        const knownMids = new Set(this._senderMidByType.values());
        const candidateTransceivers = peerConnection.getTransceivers().filter((transceiver) => {
            const mid = transceiver.mid;
            return (
                typeof mid === "string" &&
                mid.length > 0 &&
                !knownMids.has(mid) &&
                transceiver.direction === "recvonly" &&
                transceiver.currentDirection === null &&
                transceiver.sender.track == null
            );
        });
        for (const streamType of pendingTracks) {
            const transceiverIndex = candidateTransceivers.findIndex(
                (transceiver) => transceiver.receiver?.track?.kind === STREAM_KIND[streamType]
            );
            if (transceiverIndex < 0) {
                continue;
            }
            const [transceiver] = candidateTransceivers.splice(transceiverIndex, 1);
            if (!transceiver || !transceiver.mid) {
                continue;
            }
            await this.attachTrack(peerConnection, transceiver.mid, streamType);
        }
    }
}

function orderedStreamTypes(): StreamType[] {
    return ["audio", "camera", "screen"];
}

function updateTransceiverDirection(
    transceiver: ReturnType<ClientPeerConnection["getTransceivers"]>[number],
    track: MediaTrack | null
): void {
    const direction = transceiver.direction;
    if (track) {
        if (direction === "recvonly" || direction === "inactive") {
            transceiver.direction = "sendonly";
        }
        return;
    }
    if (direction === "sendonly") {
        transceiver.direction = "inactive";
    } else if (direction === "sendrecv") {
        transceiver.direction = "recvonly";
    }
}
