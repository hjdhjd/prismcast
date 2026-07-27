/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * lifecycle.test.ts: Integration coverage confirming the stream registry stays consistent across its full lifecycle. The unit tier (registry.test.ts,
 * lifecycle.test.ts) covers each function in isolation against synthetic entries; this suite verifies the integration: register a stream, retrieve it via
 * the channel index, terminate, and confirm every storage layer (registry map, channelToStreamId index) is cleaned up. Lifecycle bugs in production
 * manifest as "ghost" streams that show up in the dashboard after termination - streams that still resolve through getStream() or getChannelStreamId()
 * after terminateStream() has returned.
 */
import { createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import { deleteChannelStreamId, getChannelStreamId, setChannelStreamId, terminateStream } from "../../../src/streaming/lifecycle.ts";
import { describe, test } from "node:test";
import { getStream, registerStream, unregisterStream } from "../../../src/streaming/registry.ts";
import assert from "node:assert/strict";
import { makeRegistryEntry } from "../../../src/streaming/registry.helpers.ts";

describe("stream registry lifecycle", () => {

  test("register + index lookup + retrieve round-trip preserves the entry", async () => {

    /* The full happy path: register a stream entry, write the channel-to-id index, retrieve via id from the registry and via channel name from the index.
     * Both paths must return the same entry.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    const entry = makeRegistryEntry({ channelName: "abc" });

    registerStream(entry);
    setChannelStreamId("abc", entry.id);

    assert.equal(getStream(entry.id)?.streamIdStr, entry.streamIdStr, "registry returns the entry by id");
    assert.equal(getChannelStreamId("abc"), entry.id, "channel index returns the id");

    // Manual cleanup so this test does not strand state in the global registry between tests.
    deleteChannelStreamId("abc");
    unregisterStream(entry.id);
  });

  test("terminateStream cleans up the registry and the channel index", async () => {

    /* terminateStream is the production cleanup path. After it runs, getStream and getChannelStreamId must both return undefined for the terminated stream's
     * id and channel respectively. The 75a63cc bug class was a regression where one of these layers leaked across termination - the dashboard kept showing
     * the stream because the index still pointed at the (now-undefined) registry entry.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    const entry = makeRegistryEntry({ channelName: "abc" });

    registerStream(entry);
    setChannelStreamId("abc", entry.id);

    terminateStream(entry.id, "abc", "test cleanup");

    assert.equal(getStream(entry.id), undefined, "registry should not return a terminated entry");
    assert.equal(getChannelStreamId("abc"), undefined, "channel index should not point at a terminated stream");
  });

  test("terminateStream is a no-op on repeat calls against an already-terminated stream", async () => {

    /* The termination path can be triggered from multiple sources (client disconnect, monitor recovery, graceful shutdown). It must be safe to call more
     * than once, so a second termination attempt does not throw or corrupt other registry state.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    const entry = makeRegistryEntry({ channelName: "abc" });

    registerStream(entry);
    setChannelStreamId("abc", entry.id);
    terminateStream(entry.id, "abc", "first");

    assert.doesNotThrow(() => { terminateStream(entry.id, "abc", "second"); }, "second termination must not throw");
  });

  test("two streams against different channels can coexist; terminating one does not affect the other", async () => {

    /* Cross-stream isolation. Two channels each have their own stream entry; terminating one must leave the other untouched.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    const a = makeRegistryEntry({ channelName: "abc" });
    const b = makeRegistryEntry({ channelName: "nbc" });

    registerStream(a);
    registerStream(b);
    setChannelStreamId("abc", a.id);
    setChannelStreamId("nbc", b.id);

    terminateStream(a.id, "abc", "test");

    assert.equal(getStream(a.id), undefined, "abc stream is gone");
    assert.equal(getChannelStreamId("abc"), undefined, "abc channel index is gone");
    assert.ok(getStream(b.id), "nbc stream survives");
    assert.equal(getChannelStreamId("nbc"), b.id, "nbc channel index survives");

    // Cleanup the surviving entry.
    terminateStream(b.id, "nbc", "test cleanup");
  });
});
