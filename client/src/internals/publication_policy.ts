import type { StreamType } from "../public_api.js";
import type { PeerConnectionTransceiver } from "./browser_types.js";

export type SimulcastEncodingOffer = {
    maxBitrate?: number;
    rid: string;
};

export type UploadPublicationPolicy = {
    kind: "single" | "simulcast";
    reason?: string;
};

export type UploadPublicationPlan = {
    codecs: readonly string[];
    simulcastEncodings: readonly SimulcastEncodingOffer[];
};

const MIN_SIMULCAST_ENCODINGS = 2;
const SUPPORTED_SIMULCAST_CODEC = "VP8";

export async function applyUploadPublicationPolicy(
    streamType: StreamType,
    transceiver: PeerConnectionTransceiver,
    plan: UploadPublicationPlan | undefined
): Promise<UploadPublicationPolicy> {
    if (streamType === "audio") {
        return singleEncoding("audio uploads do not use simulcast");
    }
    if (!plan || plan.simulcastEncodings.length < MIN_SIMULCAST_ENCODINGS) {
        return singleEncoding("offer did not advertise multiple simulcast encodings");
    }
    if (!plan.codecs.some((codec) => codec.toUpperCase() === SUPPORTED_SIMULCAST_CODEC)) {
        return singleEncoding("offer did not include the VP8 simulcast path");
    }
    if (!transceiver.sender.getParameters || !transceiver.sender.setParameters) {
        return singleEncoding("sender parameter API is unavailable");
    }

    const parameters = transceiver.sender.getParameters();
    const previousEncodings = Array.isArray(parameters.encodings) ? parameters.encodings : [];
    const encodings = plan.simulcastEncodings.map((encoding, index) => ({
        ...(previousEncodings[index] ?? {}),
        active: true,
        maxBitrate: encoding.maxBitrate,
        rid: encoding.rid,
        scaleResolutionDownBy: scaleResolutionDownBy(index, plan.simulcastEncodings.length)
    }));

    try {
        await transceiver.sender.setParameters({
            ...parameters,
            encodings
        });
    } catch (error) {
        return singleEncoding(
            error instanceof Error
                ? `sender rejected simulcast parameters: ${error.message}`
                : "sender rejected simulcast parameters"
        );
    }

    return { kind: "simulcast" };
}

function singleEncoding(reason: string): UploadPublicationPolicy {
    return {
        kind: "single",
        reason
    };
}

function scaleResolutionDownBy(index: number, encodingCount: number): number {
    return 2 ** Math.max(0, encodingCount - index - 1);
}
