import assert from "node:assert/strict";
import test from "node:test";

import {
    localDescriptionHasOnlyInactiveMedia,
    remoteDescriptionAcceptsUploadMid
} from "../dist/internals/sdp_media_direction.js";

const BASE_SESSION_LINES = ["v=0", "o=- 1 1 IN IP4 0.0.0.0", "s=-", "t=0 0"];

function sdp(lines, lineEnding = "\r\n") {
    return [...BASE_SESSION_LINES, ...lines].join(lineEnding);
}

const cases = [
    {
        name: "CRLF inactive media sections are inactive-only",
        expected: true,
        sdp: sdp([
            "m=audio 9 UDP/TLS/RTP/SAVPF 111",
            "a=inactive",
            "m=video 9 UDP/TLS/RTP/SAVPF 96",
            "a=inactive"
        ])
    },
    {
        name: "LF inactive media sections are inactive-only",
        expected: true,
        sdp: sdp(
            [
                "m=audio 9 UDP/TLS/RTP/SAVPF 111",
                "a=inactive",
                "m=video 9 UDP/TLS/RTP/SAVPF 96",
                "a=inactive"
            ],
            "\n"
        )
    },
    {
        name: "session-level inactive applies to media sections with no override",
        expected: true,
        sdp: sdp([
            "a=inactive",
            "m=audio 9 UDP/TLS/RTP/SAVPF 111",
            "a=mid:0",
            "m=video 9 UDP/TLS/RTP/SAVPF 96",
            "a=mid:1"
        ])
    },
    {
        name: "missing direction defaults to active sendrecv",
        expected: false,
        sdp: sdp(["m=audio 9 UDP/TLS/RTP/SAVPF 111", "a=mid:0"])
    },
    {
        name: "media-level direction overrides session-level inactive",
        expected: false,
        sdp: sdp([
            "a=inactive",
            "m=audio 9 UDP/TLS/RTP/SAVPF 111",
            "a=sendrecv",
            "m=video 9 UDP/TLS/RTP/SAVPF 96"
        ])
    },
    {
        name: "mixed active and inactive media sections are not inactive-only",
        expected: false,
        sdp: sdp([
            "m=audio 9 UDP/TLS/RTP/SAVPF 111",
            "a=inactive",
            "m=video 9 UDP/TLS/RTP/SAVPF 96",
            "a=recvonly"
        ])
    },
    {
        name: "SDP without media sections is not inactive-only",
        expected: false,
        sdp: sdp(["a=inactive"])
    }
];

for (const entry of cases) {
    test(entry.name, () => {
        assert.equal(localDescriptionHasOnlyInactiveMedia(entry.sdp), entry.expected);
    });
}

for (const entry of [
    {
        direction: "recvonly",
        expected: true,
        name: "remote recvonly media accepts an upload"
    },
    {
        direction: "sendrecv",
        expected: true,
        name: "remote sendrecv media accepts an upload"
    },
    {
        direction: "inactive",
        expected: false,
        name: "remote inactive media rejects an upload"
    },
    {
        direction: "sendonly",
        expected: false,
        name: "remote sendonly media rejects an upload"
    }
]) {
    test(entry.name, () => {
        assert.equal(
            remoteDescriptionAcceptsUploadMid(
                sdp(["m=video 9 UDP/TLS/RTP/SAVPF 96", "a=mid:camera", `a=${entry.direction}`]),
                "camera"
            ),
            entry.expected
        );
    });
}

test("rejected or missing remote media rejects an upload", () => {
    const rejected = sdp(["m=video 0 UDP/TLS/RTP/SAVPF 96", "a=mid:camera", "a=recvonly"]);

    assert.equal(remoteDescriptionAcceptsUploadMid(rejected, "camera"), false);
    assert.equal(remoteDescriptionAcceptsUploadMid(rejected, "screen"), false);
});
