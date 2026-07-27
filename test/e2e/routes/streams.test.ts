/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * streams.test.ts: HTTP-level integration coverage for the stream-management routes (src/routes/streams.ts). GET /streams projects the live registry into a JSON
 * row per stream; DELETE /streams/:id routes through the authoritative terminateStream() and returns the success envelope. The registry is the single source of
 * truth these routes read, so we seed it directly with makeRegistryEntry + registerStream (the same synthetic-stream pattern test/e2e/streaming/lifecycle.test.ts
 * uses) rather than launching a real capture.
 *
 * The projection's default-fill path is the detail this suite exists to pin: a freshly registered stream has never emitted a status, so getStreamStatus() returns
 * undefined and every status-derived field must fall back through its ?? default (health "healthy", clientCount 0, clients [], escalationLevel 0, logoUrl "",
 * recoveryAttempts 0, showName ""). The 400 (NaN id) and 404 (missing id) DELETE branches are pinned by the cross-tree envelope sweep in
 * routes/error-envelope.test.ts; this suite adds the populated GET projection and the DELETE success path those do not exercise.
 */
import { bootApp, createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { registerStream, unregisterStream } from "../../../src/streaming/registry.ts";
import assert from "node:assert/strict";
import { makeRegistryEntry } from "../../../src/streaming/registry.helpers.ts";

interface StreamRow {

  channel: string | null;
  clientCount: number;
  clients: unknown[];
  duration: number;
  escalationLevel: number;
  health: string;
  id: number;
  logoUrl: string;
  recoveryAttempts: number;
  showName: string;
  startTime: string;
  url: string;
}

interface StreamsResponse {

  count: number;
  limit: number;
  streams: StreamRow[];
}

describe("GET /streams - populated projection", () => {

  test("projects a registered stream with every status-derived field defaulted when no status has been emitted", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);
    const entry = makeRegistryEntry({ channelName: "abc", url: "https://origin.test/abc.m3u8" });

    registerStream(entry);

    try {

      const response = await fetch(urlFor("/streams"));

      assert.equal(response.status, 200, "the streams listing responds 200");

      const body = await response.json() as StreamsResponse;

      assert.equal(body.count, 1, "the registered stream is counted");
      assert.equal(body.streams.length, 1, "one row is projected");

      const [row] = body.streams;

      assert.ok(row, "the projected row is present");
      assert.equal(row.id, entry.id, "the row carries the registry id");
      assert.equal(row.channel, "abc", "the row carries the channel name");
      assert.equal(row.url, "https://origin.test/abc.m3u8", "the row carries the source url");

      // The default-fill path: with no status emitted, getStreamStatus() is undefined and each field must resolve to its ?? default. A regression that dropped
      // any default (for example, reading an undefined status field straight into the response) would surface here as an undefined or missing field.
      assert.equal(row.clientCount, 0, "clientCount defaults to 0");
      assert.deepEqual(row.clients, [], "clients defaults to an empty array");
      assert.equal(row.escalationLevel, 0, "escalationLevel defaults to 0");
      assert.equal(row.health, "healthy", "health defaults to healthy");
      assert.equal(row.logoUrl, "", "logoUrl defaults to an empty string");
      assert.equal(row.recoveryAttempts, 0, "recoveryAttempts defaults to 0");
      assert.equal(row.showName, "", "showName defaults to an empty string");
      assert.equal(typeof row.duration, "number", "duration is projected as a number of seconds");
      assert.ok(row.duration >= 0, "duration is non-negative");
    } finally {

      unregisterStream(entry.id);
    }
  });
});

describe("DELETE /streams/:id - success path", () => {

  test("terminates a registered stream, returns the success envelope, and removes it from the listing", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);
    const entry = makeRegistryEntry({ channelName: "nbc" });

    registerStream(entry);

    const response = await fetch(urlFor("/streams/" + String(entry.id)), { method: "DELETE" });

    assert.equal(response.status, 200, "deleting a live stream responds 200");

    // sendSuccess spreads the payload's data fields at the top level of the envelope (alongside success/message), so streamId is a top-level field, not nested.
    const body = await response.json() as { message: string; streamId: number; success: boolean };

    assert.equal(body.success, true, "the response carries the success envelope marker");
    assert.equal(body.streamId, entry.id, "the terminated stream id is echoed back");
    assert.equal(body.message, "Stream terminated.", "the success message is the documented copy");

    // terminateStream() is the authoritative cleanup path; a follow-up listing must no longer show the stream, proving the route unregistered it rather than just
    // replying success.
    const listing = await (await fetch(urlFor("/streams"))).json() as StreamsResponse;

    assert.equal(listing.streams.some((row) => row.id === entry.id), false, "the terminated stream no longer appears in the listing");
  });
});
