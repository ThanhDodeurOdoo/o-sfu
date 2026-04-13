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
        }
        this._senderMidByType.delete(streamType);
    }
}
