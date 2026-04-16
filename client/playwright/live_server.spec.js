import { expect, test } from "@playwright/test";

import {
    connectPeer,
    createChannel,
    createConnectToken,
    createPeerPage,
    peerSnapshot,
    publishSyntheticCamera,
    setCameraDownload,
    unpublishCamera
} from "./live_server_helpers.mjs";

const PUBLISHER_SESSION_ID = 41;
const SUBSCRIBER_SESSION_ID = 42;

test("browser compatibility upload and download flows survive live-server replacement", async ({
    context
}) => {
    const channelUuid = await createChannel();
    const publisher = await createPeerPage(context);
    const subscriber = await createPeerPage(context);

    await connectPeer(publisher, {
        channelUuid,
        jwt: createConnectToken(channelUuid, PUBLISHER_SESSION_ID)
    });
    await connectPeer(subscriber, {
        channelUuid,
        jwt: createConnectToken(channelUuid, SUBSCRIBER_SESSION_ID)
    });

    await expect.poll(async () => (await peerSnapshot(publisher)).state).toBe("connected");
    await expect.poll(async () => (await peerSnapshot(subscriber)).state).toBe("connected");

    await publishSyntheticCamera(publisher, "initial-camera");

    await expect
        .poll(async () => {
            const snapshot = await peerSnapshot(subscriber);
            return snapshot.updates.filter(
                (update) =>
                    update.name === "track" &&
                    update.payload.sessionId === PUBLISHER_SESSION_ID &&
                    update.payload.type === "camera"
            );
        })
        .toContainEqual({
            name: "track",
            payload: {
                active: true,
                sessionId: PUBLISHER_SESSION_ID,
                track: {
                    enabled: true,
                    id: expect.any(String),
                    kind: "video",
                    muted: false,
                    readyState: "live"
                },
                type: "camera"
            }
        });

    await setCameraDownload(subscriber, PUBLISHER_SESSION_ID, false);

    await expect
        .poll(async () => {
            const snapshot = await peerSnapshot(subscriber);
            return snapshot.updates.filter(
                (update) =>
                    update.name === "track" &&
                    update.payload.sessionId === PUBLISHER_SESSION_ID &&
                    update.payload.type === "camera"
            );
        })
        .toContainEqual({
            name: "track",
            payload: {
                active: false,
                sessionId: PUBLISHER_SESSION_ID,
                track: {
                    enabled: true,
                    id: expect.any(String),
                    kind: "video",
                    muted: false,
                    readyState: "live"
                },
                type: "camera"
            }
        });

    const replacement = await createPeerPage(context);
    await connectPeer(replacement, {
        channelUuid,
        jwt: createConnectToken(channelUuid, PUBLISHER_SESSION_ID)
    });

    await expect.poll(async () => (await peerSnapshot(replacement)).state).toBe("connected");
    await expect
        .poll(async () => {
            const snapshot = await peerSnapshot(publisher);
            return snapshot.stateChanges.at(-1);
        })
        .toEqual({
            cause: "kicked",
            state: "closed"
        });
    await expect
        .poll(async () => {
            const snapshot = await peerSnapshot(subscriber);
            return snapshot.updates.filter(
                (update) =>
                    update.name === "disconnect" &&
                    update.payload.sessionId === PUBLISHER_SESSION_ID
            ).length;
        })
        .toBeGreaterThan(0);

    await publishSyntheticCamera(replacement, "replacement-camera");

    await expect
        .poll(async () => {
            const snapshot = await peerSnapshot(subscriber);
            return snapshot.updates.filter(
                (update) =>
                    update.name === "track" &&
                    update.payload.sessionId === PUBLISHER_SESSION_ID &&
                    update.payload.type === "camera"
            );
        })
        .toContainEqual({
            name: "track",
            payload: {
                active: false,
                sessionId: PUBLISHER_SESSION_ID,
                track: {
                    enabled: true,
                    id: expect.any(String),
                    kind: "video",
                    muted: false,
                    readyState: "live"
                },
                type: "camera"
            }
        });

    await setCameraDownload(subscriber, PUBLISHER_SESSION_ID, true);

    await expect
        .poll(async () => {
            const snapshot = await peerSnapshot(subscriber);
            return snapshot.updates.filter(
                (update) =>
                    update.name === "track" &&
                    update.payload.sessionId === PUBLISHER_SESSION_ID &&
                    update.payload.type === "camera"
            );
        })
        .toContainEqual({
            name: "track",
            payload: {
                active: true,
                sessionId: PUBLISHER_SESSION_ID,
                track: {
                    enabled: true,
                    id: expect.any(String),
                    kind: "video",
                    muted: false,
                    readyState: "live"
                },
                type: "camera"
            }
        });

    await unpublishCamera(replacement);

    await expect
        .poll(async () => (await peerSnapshot(subscriber)).consumers["41"]?.camera ?? null)
        .toBeNull();
});
