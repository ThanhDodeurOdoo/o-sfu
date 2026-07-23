type SdpMediaDirection = "sendrecv" | "sendonly" | "recvonly" | "inactive";

type SdpMediaSection = {
    direction?: SdpMediaDirection;
    mid?: string;
    rejected: boolean;
};

const DEFAULT_MEDIA_DIRECTION: SdpMediaDirection = "sendrecv";
const SDP_MEDIA_PREFIX = "m=";
const SDP_ATTRIBUTE_PREFIX = "a=";
const SDP_MID_PREFIX = "a=mid:";
const MEDIA_DIRECTIONS = new Set<SdpMediaDirection>([
    "sendrecv",
    "sendonly",
    "recvonly",
    "inactive"
]);

/**
 * Returns true only when every media section resolves to `a=inactive`.
 *
 * SDP direction attributes inherit from the session level unless a media-level
 * value overrides them; when neither level declares a direction, RFC 8866
 * section 6.7 says `sendrecv` is the default. That default is active for this
 * compatibility fallback, so missing direction attributes must not mark the
 * transport as ready.
 */
export function localDescriptionHasOnlyInactiveMedia(sdp: string): boolean {
    const { mediaSections, sessionDirection } = parseMediaSections(sdp);
    return (
        mediaSections.length > 0 &&
        mediaSections.every(
            (media) =>
                (media.direction ?? sessionDirection ?? DEFAULT_MEDIA_DIRECTION) === "inactive"
        )
    );
}

export function remoteDescriptionAcceptsUploadMid(sdp: string, mid: string): boolean {
    const { mediaSections, sessionDirection } = parseMediaSections(sdp);
    const media = mediaSections.find((candidate) => candidate.mid === mid);
    if (!media || media.rejected) {
        return false;
    }
    const direction = media.direction ?? sessionDirection ?? DEFAULT_MEDIA_DIRECTION;
    return direction === "recvonly" || direction === "sendrecv";
}

function parseMediaSections(sdp: string): {
    mediaSections: SdpMediaSection[];
    sessionDirection?: SdpMediaDirection;
} {
    let sessionDirection: SdpMediaDirection | undefined;
    let currentMedia: SdpMediaSection | undefined;
    const mediaSections: SdpMediaSection[] = [];

    for (const rawLine of sdp.split(/\r\n|\n|\r/)) {
        const line = rawLine.trimEnd();
        if (line.startsWith(SDP_MEDIA_PREFIX)) {
            currentMedia = {
                rejected: mediaLinePort(line) === "0"
            };
            mediaSections.push(currentMedia);
            continue;
        }

        const direction = parseDirectionAttribute(line);
        if (direction) {
            if (currentMedia) {
                currentMedia.direction = direction;
            } else {
                sessionDirection = direction;
            }
            continue;
        }
        if (currentMedia && line.startsWith(SDP_MID_PREFIX)) {
            currentMedia.mid = line.slice(SDP_MID_PREFIX.length);
        }
    }

    return { mediaSections, sessionDirection };
}

function parseDirectionAttribute(line: string): SdpMediaDirection | undefined {
    if (!line.startsWith(SDP_ATTRIBUTE_PREFIX)) {
        return undefined;
    }
    const attribute = line.slice(SDP_ATTRIBUTE_PREFIX.length);
    return isSdpMediaDirection(attribute) ? attribute : undefined;
}

function isSdpMediaDirection(value: string): value is SdpMediaDirection {
    return MEDIA_DIRECTIONS.has(value as SdpMediaDirection);
}

function mediaLinePort(line: string): string | undefined {
    return line.split(/\s+/, 3)[1]?.split("/", 1)[0];
}
