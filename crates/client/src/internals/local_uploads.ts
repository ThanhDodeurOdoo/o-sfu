import { STREAM_TYPES, type StreamType } from "../public_api.js";
import { STREAM_KIND, type ClientPeerConnection, type MediaTrack } from "./browser_types.js";
import { applyUploadPublicationPolicy, type SimulcastEncodingOffer } from "./publication_policy.js";

type UploadTransition = {
    hadTrack: boolean;
    hasTrack: boolean;
    boundMid?: string;
};

export type UploadSlot = {
    kind: "audio" | "video";
    mid: string;
    simulcastEncodings?: readonly SimulcastEncodingOffer[];
};

export class LocalUploads {
    private _localTracks = new Map<StreamType, MediaTrack | null>();
    private _senderMidByType = new Map<StreamType, string>();
    private _uploadIntentByType = new Set<StreamType>();

    setTrack(type: StreamType, track: MediaStreamTrack | null): UploadTransition {
        const previousTrack = this._localTracks.get(type) ?? null;
        this._localTracks.set(type, track);
        if (track === null) {
            this._uploadIntentByType.delete(type);
        }
        return {
            hadTrack: previousTrack !== null,
            hasTrack: track !== null,
            boundMid: this._senderMidByType.get(type)
        };
    }

    setUploadIntent(type: StreamType, active: boolean): void {
        if (active) {
            this._uploadIntentByType.add(type);
        } else {
            this._uploadIntentByType.delete(type);
        }
    }

    clearPeerConnectionState(): void {
        this._senderMidByType.clear();
        this._uploadIntentByType.clear();
    }

    boundMidFor(streamType: StreamType): string | undefined {
        return this._senderMidByType.get(streamType);
    }

    async attachTrack(
        peerConnection: ClientPeerConnection | null,
        mid: string,
        streamType: StreamType,
        uploadSlot?: UploadSlot
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
        if (track) {
            await applyUploadPublicationPolicy(
                streamType,
                transceiver,
                uploadSlot?.simulcastEncodings ?? []
            );
        }
        this._senderMidByType.set(streamType, mid);
    }

    async detachTrack(
        peerConnection: ClientPeerConnection | null,
        streamType: StreamType
    ): Promise<void> {
        if (!peerConnection) {
            return;
        }
        const boundMid = this._senderMidByType.get(streamType);
        if (!boundMid) {
            return;
        }
        const transceiver = peerConnection
            .getTransceivers()
            .find((candidate) => candidate.mid === boundMid);
        if (transceiver) {
            await transceiver.sender.replaceTrack(null);
            updateTransceiverDirection(transceiver, null);
        }
        this._senderMidByType.delete(streamType);
    }

    async attachPendingTracks(
        peerConnection: ClientPeerConnection | null,
        uploadSlots: UploadSlot[]
    ): Promise<void> {
        if (!peerConnection) {
            return;
        }
        const pendingStreamTypes = STREAM_TYPES.filter(
            (streamType) =>
                (this._localTracks.get(streamType) ?? null) !== null &&
                this._uploadIntentByType.has(streamType) &&
                !this._senderMidByType.has(streamType)
        );
        if (pendingStreamTypes.length === 0) {
            return;
        }
        const boundMids = new Set(this._senderMidByType.values());
        const candidateTransceivers = peerConnection.getTransceivers().filter((transceiver) => {
            const mid = transceiver.mid;
            return (
                typeof mid === "string" &&
                mid.length > 0 &&
                // transceiver must be one of the slots offered by the sfu
                uploadSlots.some((slot) => slot.mid === mid) &&
                // and it must not be already assigned to another local stream
                !boundMids.has(mid) &&
                // when the sfu offers an upload slot (a=recvonly), the browser
                // sets the local transceiver to recvonly until we flip it to sendonly.
                // we also check that it's unnegotiated (currentDirection === null)
                // to be sure we only touch fresh or reset slots
                transceiver.direction === "recvonly" &&
                transceiver.currentDirection === null &&
                transceiver.sender.track == null
            );
        });
        const transceiverMidSet = new Set(
            candidateTransceivers.map((transceiver) => transceiver.mid)
        );
        const remainingSlots = uploadSlots.filter((slot) => transceiverMidSet.has(slot.mid));
        for (const streamType of pendingStreamTypes) {
            const slotIndex = remainingSlots.findIndex(
                (slot) => slot.kind === STREAM_KIND[streamType]
            );
            if (slotIndex < 0) {
                continue;
            }
            const [slot] = remainingSlots.splice(slotIndex, 1);
            await this.attachTrack(peerConnection, slot.mid, streamType, slot);
            this._uploadIntentByType.delete(streamType);
        }
    }
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
