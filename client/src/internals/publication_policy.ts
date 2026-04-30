import type { StreamType } from "../public_api.js";
import type { PeerConnectionTransceiver } from "./browser_types.js";

export type SimulcastEncodingOffer = {
    maxFramerate?: number;
    maxBitrate?: number;
    policyRole?: "featured" | "thumbnail" | "degradedThumbnail";
    rid: string;
    resolutionScale?: number;
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
const PRODUCTION_SIMULCAST_CODEC = "VP8";

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
    if (!plan.codecs.some((codec) => codec.toUpperCase() === PRODUCTION_SIMULCAST_CODEC)) {
        return singleEncoding("offer did not include the production VP8 simulcast path");
    }
    if (!transceiver.sender.getParameters || !transceiver.sender.setParameters) {
        return singleEncoding("sender parameter API is unavailable");
    }
    if (!plan.simulcastEncodings.every(isValidSimulcastEncodingOffer)) {
        return singleEncoding("offer advertised an invalid simulcast encoding profile");
    }

    const parameters = transceiver.sender.getParameters();
    const previousEncodings = Array.isArray(parameters.encodings) ? parameters.encodings : [];
    const encodings = plan.simulcastEncodings.map((encoding, index) =>
        senderEncodingParameters(previousEncodings[index] ?? {}, encoding)
    );

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

function senderEncodingParameters(
    previousEncoding: RTCRtpEncodingParameters,
    encoding: SimulcastEncodingOffer
): RTCRtpEncodingParameters {
    return {
        ...previousEncoding,
        active: true,
        ...(encoding.maxBitrate === undefined ? {} : { maxBitrate: encoding.maxBitrate }),
        ...(encoding.maxFramerate === undefined ? {} : { maxFramerate: encoding.maxFramerate }),
        rid: encoding.rid,
        ...(encoding.resolutionScale === undefined
            ? {}
            : { scaleResolutionDownBy: encoding.resolutionScale })
    };
}

function isValidSimulcastEncodingOffer(encoding: SimulcastEncodingOffer): boolean {
    return (
        typeof encoding.rid === "string" &&
        encoding.rid.length > 0 &&
        isOptionalPositiveInteger(encoding.maxBitrate) &&
        isOptionalPositiveInteger(encoding.maxFramerate) &&
        isOptionalPositiveNumber(encoding.resolutionScale) &&
        isValidPolicyRole(encoding.policyRole)
    );
}

function isOptionalPositiveInteger(value: number | undefined): boolean {
    return value === undefined || (Number.isInteger(value) && value > 0);
}

function isOptionalPositiveNumber(value: number | undefined): boolean {
    return value === undefined || (Number.isFinite(value) && value > 0);
}

function isValidPolicyRole(value: SimulcastEncodingOffer["policyRole"]): boolean {
    return (
        value === undefined ||
        value === "featured" ||
        value === "thumbnail" ||
        value === "degradedThumbnail"
    );
}
