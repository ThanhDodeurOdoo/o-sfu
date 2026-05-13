type SdpMediaDirection = "sendrecv" | "sendonly" | "recvonly" | "inactive";

const DEFAULT_MEDIA_DIRECTION: SdpMediaDirection = "sendrecv";
const SDP_MEDIA_PREFIX = "m=";
const SDP_ATTRIBUTE_PREFIX = "a=";
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
    let sessionDirection: SdpMediaDirection | undefined;
    let currentMediaIndex: number | undefined;
    const mediaDirections: (SdpMediaDirection | undefined)[] = [];

    for (const rawLine of sdp.split(/\r\n|\n|\r/)) {
        const line = rawLine.trimEnd();
        if (line.startsWith(SDP_MEDIA_PREFIX)) {
            mediaDirections.push(undefined);
            currentMediaIndex = mediaDirections.length - 1;
            continue;
        }

        const direction = parseDirectionAttribute(line);
        if (!direction) {
            continue;
        }
        if (currentMediaIndex === undefined) {
            sessionDirection = direction;
        } else {
            mediaDirections[currentMediaIndex] = direction;
        }
    }

    return (
        mediaDirections.length > 0 &&
        mediaDirections.every(
            (mediaDirection) =>
                (mediaDirection ?? sessionDirection ?? DEFAULT_MEDIA_DIRECTION) === "inactive"
        )
    );
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
