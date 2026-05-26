/**
 * local media upload management
 *
 * this module tracks which local tracks are intended for upload and how they
 * are bound to the underlying peer connection transceivers. it handles the
 * state transition between desired tracks and negotiated upload slots
 */

import type { StreamType } from "../public_api.js";
import { STREAM_KIND, type ClientPeerConnection, type MediaTrack } from "./browser_types.js";
import {
    applyUploadPublicationPolicy,
    type SimulcastEncodingOffer,
    type UploadPublicationPolicy
} from "./publication_policy.js";

type UploadTransition = {
    hadTrack: boolean;
    hasTrack: boolean;
    knownMid?: string;
};

/**
 * describes a track that was attached during a renegotiation cycle
 */
export type PendingRenegotiationAttachment = {
    mid: string;
    publicationPolicy: UploadPublicationPolicy;
    streamType: StreamType;
};

/**
 * result of a bulk attachment operation
 */
export type PendingRenegotiationAttachmentResult = {
    attached: PendingRenegotiationAttachment[];
    skipped: StreamType[];
};

/**
 * metadata for a media section that can accept a client upload
 */
export type UploadSlot = {
    codecs?: readonly string[];
    kind: "audio" | "video";
    mid: string;
    simulcastEncodings?: readonly SimulcastEncodingOffer[];
};

/**
 * manager for local media tracks and their transceiver bindings
 *
 * it keeps track of which local tracks (audio, camera, screen) are active
 * and maps them to the appropriate transceivers once the server provides
 * compatible upload slots in an offer
 */
export class LocalUploads {
    private _localTracks = new Map<StreamType, MediaTrack | null>();
    private _senderMidByType = new Map<StreamType, string>();
    private _uploadIntentByType = new Set<StreamType>();

    /**
     * updates the local track for a specific stream type
     *
     * this only updates the desired state. the track is not actually attached
     * to the peer connection until the next negotiation cycle or an explicit
     * attachment call. returns the transition state to help the caller decide
     * if renegotiation is needed
     *
     * @param type stream type to update
     * @param track new media track or null to clear
     * @returns transition state describing the change
     */
    setTrack(type: StreamType, track: MediaStreamTrack | null): UploadTransition {
        const previousTrack = this._localTracks.get(type) ?? null;
        this._localTracks.set(type, track);
        if (track === null) {
            this._uploadIntentByType.delete(type);
        }
        return {
            hadTrack: previousTrack !== null,
            hasTrack: track !== null,
            knownMid: this._senderMidByType.get(type)
        };
    }

    setUploadIntent(type: StreamType, active: boolean): void {
        if (active) {
            this._uploadIntentByType.add(type);
        } else {
            this._uploadIntentByType.delete(type);
        }
    }

    /**
     * forgets all current transceiver bindings
     *
     * called when the peer connection is closed or replaced. it does not
     * clear the desired local tracks, only their connection-specific mapping
     */
    clearPeerConnectionState(): void {
        this._senderMidByType.clear();
        this._uploadIntentByType.clear();
    }

    /**
     * returns the mid currently bound to a stream type, if any
     *
     * @param streamType stream type to look up
     * @returns the mid or undefined if not bound
     */
    boundMidFor(streamType: StreamType): string | undefined {
        return this._senderMidByType.get(streamType);
    }

    /**
     * binds a local track to a specific transceiver mid
     *
     * this is the point where a local media stream actually enters the peer
     * connection. it applies the publication policy (like simulcast) based
     * on the slot metadata provided by the server
     *
     * @param peerConnection active peer connection
     * @param mid transceiver mid to attach to
     * @param streamType type of the stream being attached
     * @param uploadSlot optional slot metadata from the server
     * @returns applied publication policy
     */
    async attachTrack(
        peerConnection: ClientPeerConnection | null,
        mid: string,
        streamType: StreamType,
        uploadSlot?: UploadSlot
    ): Promise<UploadPublicationPolicy> {
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
        const publicationPolicy = track
            ? await applyUploadPublicationPolicy(streamType, transceiver, {
                  codecs: uploadSlot?.codecs ?? [],
                  simulcastEncodings: uploadSlot?.simulcastEncodings ?? []
              })
            : {
                  kind: "single" as const,
                  reason: "no local track is attached"
              };
        this._senderMidByType.set(streamType, mid);
        return publicationPolicy;
    }

    /**
     * removes a track from the peer connection while keeping the binding
     *
     * sets the transceiver direction to inactive so the slot can be reused
     * or remains silent during the next negotiation
     *
     * @param peerConnection active peer connection
     * @param streamType stream type to detach
     */
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

    /**
     * matches unassigned local tracks to newly offered upload slots
     *
     * this is called during negotiation when the server sends an offer
     * containing new media sections. it finds eligible transceivers that
     * aren't already in use and attaches any pending local tracks to them
     *
     * @param peerConnection active peer connection
     * @param uploadSlots available upload slots from the sfu
     * @returns result of the bulk attachment operation
     */
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
                this._uploadIntentByType.has(streamType) &&
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
                // transceiver must be one of the slots offered by the sfu
                uploadSlots.some((slot) => slot.mid === mid) &&
                // and it must not be already assigned to another local stream
                !knownMids.has(mid) &&
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
            const publicationPolicy = await this.attachTrack(
                peerConnection,
                slot.mid,
                streamType,
                slot
            );
            this._uploadIntentByType.delete(streamType);
            attached.push({ mid: slot.mid, publicationPolicy, streamType });
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
