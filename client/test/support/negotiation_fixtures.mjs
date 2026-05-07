const BASE_SDP_LINES = ["v=0", "o=- 1 1 IN IP4 0.0.0.0", "s=-", "t=0 0"];

export const sdp = (...sections) => [...BASE_SDP_LINES, ...sections.flat()].join("\r\n");

export const audioMedia = (mid, direction = "recvonly") => [
    "m=audio 9 UDP/TLS/RTP/SAVPF 111",
    `a=mid:${mid}`,
    `a=${direction}`
];

export const videoMedia = (
    mid,
    { direction = "recvonly", payloadType = 96, rtpmap = null } = {}
) => [
    `m=video 9 UDP/TLS/RTP/SAVPF ${payloadType}`,
    `a=mid:${mid}`,
    `a=${direction}`,
    ...(rtpmap ? [`a=rtpmap:${payloadType} ${rtpmap}`] : [])
];

export const audioUploadSlot = (mid) => ({
    codecs: ["opus"],
    kind: "audio",
    mid,
    simulcastEncodings: []
});

export const videoUploadSlot = (
    mid,
    {
        codecs = ["VP8"],
        simulcastEncodings = [
            {
                maxBitrate: 150000,
                policyRole: "thumbnail",
                rid: "lo",
                resolutionScale: 2
            },
            {
                maxBitrate: 900000,
                policyRole: "featured",
                rid: "hi",
                resolutionScale: 1
            }
        ]
    } = {}
) => ({
    codecs,
    kind: "video",
    mid,
    simulcastEncodings
});

export const negotiationCommand = ({ negotiationKind, requestId, sdp, uploadSlots }) => ({
    kind: "applyNegotiation",
    negotiationKind,
    requestId,
    sdp,
    uploadSlots
});
