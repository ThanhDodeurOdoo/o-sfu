import type { StreamType } from "../public_api.js";
import { STREAM_KIND, type ClientPeerConnection, type MediaTrack } from "./browser_types.js";

type UploadTransition = {
    hadTrack: boolean;
    hasTrack: boolean;
    knownMid?: string;
};

export type PendingRenegotiationAttachment = {
    mid: string;
    streamType: StreamType;
};

export type PendingRenegotiationAttachmentResult = {
    attached: PendingRenegotiationAttachment[];
    skipped: StreamType[];
};

export type UploadSlot = {
    kind: "audio" | "video";
    mid: string;
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
        const transceivers = peerConnection.getTransceivers();
        const transceiver = knownMid
            ? transceivers.find((candidate) => candidate.mid === knownMid)
            : uniqueSenderKindTransceiver(transceivers, STREAM_KIND[streamType]);
        if (transceiver) {
            await transceiver.sender.replaceTrack(null);
            updateTransceiverDirection(transceiver, null);
        }
        this._senderMidByType.delete(streamType);
    }

    async attachPendingTracks(
        peerConnection: ClientPeerConnection | null,
        uploadSlots: UploadSlot[]
    ): Promise<PendingRenegotiationAttachmentResult> {
        if (!peerConnection) {
            return { attached: [], skipped: [] };
        }
        const pendingTracks = orderedStreamTypes().filter(
            (streamType) =>
                (this._localTracks.get(streamType) ?? null) !== null &&
                !this._senderMidByType.has(streamType)
        );
        if (pendingTracks.length === 0) {
            return { attached: [], skipped: [] };
        }
        const knownMids = new Set(this._senderMidByType.values());
        const candidateTransceivers = peerConnection.getTransceivers().filter((transceiver) => {
            const mid = transceiver.mid;
            return (
                typeof mid === "string" &&
                mid.length > 0 &&
                uploadSlots.some((slot) => slot.mid === mid) &&
                !knownMids.has(mid) &&
                transceiver.direction === "recvonly" &&
                transceiver.currentDirection === null &&
                transceiver.sender.track == null
            );
        });
        const transceiverMidSet = new Set(
            candidateTransceivers.map((transceiver) => transceiver.mid)
        );
        const remainingSlots = uploadSlots.filter((slot) => transceiverMidSet.has(slot.mid));
        const attached: PendingRenegotiationAttachment[] = [];
        const skipped: StreamType[] = [];
        for (const streamType of pendingTracks) {
            const slotIndex = remainingSlots.findIndex(
                (slot) => slot.kind === STREAM_KIND[streamType]
            );
            if (slotIndex < 0) {
                skipped.push(streamType);
                continue;
            }
            const [slot] = remainingSlots.splice(slotIndex, 1);
            if (!slot) {
                skipped.push(streamType);
                continue;
            }
            await this.attachTrack(peerConnection, slot.mid, streamType);
            attached.push({ mid: slot.mid, streamType });
        }
        return { attached, skipped };
    }
}

function orderedStreamTypes(): StreamType[] {
    return ["audio", "camera", "screen"];
}

function uniqueSenderKindTransceiver(
    transceivers: ReturnType<ClientPeerConnection["getTransceivers"]>,
    kind: "audio" | "video"
): (typeof transceivers)[number] | undefined {
    const matchingTransceivers = transceivers.filter(
        (candidate) => candidate.sender.track?.kind === kind
    );
    return matchingTransceivers.length === 1 ? matchingTransceivers[0] : undefined;
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
