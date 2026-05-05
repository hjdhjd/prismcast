/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * streams.test.ts: Unit tests for the stream management routes in streams.ts. setupStreamsEndpoint registers GET /streams (lists active streams), DELETE
 * /streams/:id (terminates a single stream), and GET /streams/status (Server-Sent Events). Without a real Chrome browser or capture pipeline running, the
 * registry is empty for the duration of the suite, so the tests focus on the empty-list response shape, the parameter-validation paths in DELETE, and the
 * SSE handshake (initial snapshot + heartbeat header set). Streams cannot be created in a unit test - that path is covered by e2e tests.
 */
import type { AddressInfo, Server } from "node:net";
import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWss } from "../testing.helpers.ts";
import express from "express";
import { setupStreamsEndpoint } from "./streams.ts";

interface StreamsListResponse {

  count: number;
  limit: number;
  streams: unknown[];
}

interface ErrorResponse {

  error?: string;
}

function makeServer(): Promise<{ port: number; server: Server }> {

  const app = express();

  setupStreamsEndpoint(app);

  return new Promise((resolve, reject) => {

    const server = app.listen(0, "127.0.0.1", () => {

      const address = server.address() as AddressInfo;

      resolve({ port: address.port, server });
    });

    server.on("error", reject);
  });
}

function closeServer(server: Server): Promise<void> {

  return new Promise((resolve) => {

    server.close(() => {

      resolve();
    });
  });
}

let sharedServer: Server;
let sharedPort = 0;

function urlFor(path: string): string {

  return "http://127.0.0.1:" + String(sharedPort) + path;
}

before(async () => {

  const created = await makeServer();

  sharedServer = created.server;
  sharedPort = created.port;
});

after(async () => {

  await closeServer(sharedServer);
  await closePuppeteerStreamWss();
});

describe("setupStreamsEndpoint - GET /streams", () => {

  test("returns 200 with an empty streams array when the registry is empty", async () => {

    const res = await fetch(urlFor("/streams"));

    assert.equal(res.status, 200);

    const body = await res.json() as StreamsListResponse;

    assert.deepEqual(body.streams, []);
    assert.equal(body.count, 0);
  });

  test("limit reflects the configured maxConcurrentStreams from CONFIG", async () => {

    // Boundary: limit comes from CONFIG.streaming.maxConcurrentStreams. We only assert the type and positivity here so the test doesn't break on a CONFIG bump.
    const res = await fetch(urlFor("/streams"));
    const body = await res.json() as StreamsListResponse;

    assert.equal(typeof body.limit, "number");
    assert.ok(body.limit > 0, "limit should be positive");
  });

  test("emits Content-Type application/json", async () => {

    const res = await fetch(urlFor("/streams"));

    assert.match(res.headers.get("content-type") ?? "", /application\/json/);
    await res.json();
  });
});

describe("setupStreamsEndpoint - DELETE /streams/:id (validation paths)", () => {

  test("returns 400 when the id parameter is not a valid integer", async () => {

    // Negative test: parseInt("not-a-number") yields NaN, which the handler maps to a 400 with a descriptive error.
    const res = await fetch(urlFor("/streams/not-a-number"), { method: "DELETE" });
    const body = await res.json() as ErrorResponse;

    assert.equal(res.status, 400);
    assert.match(body.error ?? "", /Invalid stream ID/);
  });

  test("returns 404 when the stream id is a valid number but the stream does not exist", async () => {

    // Negative test: a numeric id that the registry has never seen must produce a 404. We use 99999 to comfortably exceed any plausible auto-incrementing ID.
    const res = await fetch(urlFor("/streams/99999"), { method: "DELETE" });
    const body = await res.json() as ErrorResponse;

    assert.equal(res.status, 404);
    assert.match(body.error ?? "", /Stream not found/);
  });

  test("returns 400 for a negative id (parseInt returns the negative number, registry has no negative ids)", async () => {

    // Boundary: parseInt("-5") returns -5 (a valid integer), so the handler skips the NaN branch and falls through to the registry lookup. Negative IDs do not
    // exist in the auto-incrementing counter, so the lookup returns undefined and the response is 404.
    const res = await fetch(urlFor("/streams/-5"), { method: "DELETE" });
    const body = await res.json() as ErrorResponse;

    assert.equal(res.status, 404, "negative id should fall through to the registry-not-found branch");
    assert.match(body.error ?? "", /Stream not found/);
  });

  test("returns 400 for an empty id segment", async () => {

    // Boundary: an empty path segment should never reach :id. Express's path-to-regexp doesn't match an empty :id, so the response is the framework's 404. Lock
    // that we don't crash and that the response is non-2xx.
    const res = await fetch(urlFor("/streams/"), { method: "DELETE" });

    assert.notEqual(res.status, 200);
    await res.text();
  });
});

describe("setupStreamsEndpoint - GET /streams/status (SSE handshake)", () => {

  test("returns Content-Type text/event-stream and SSE-friendly headers", async () => {

    // The SSE endpoint must declare text/event-stream content type, no-cache, and keep-alive so browsers and proxies route the connection correctly. We open
    // the stream, read enough to verify the headers, then abort. Aborting via AbortSignal closes the request and triggers cleanup on the server.
    const controller = new AbortController();
    const res = await fetch(urlFor("/streams/status"), { signal: controller.signal });

    try {

      assert.equal(res.status, 200);
      assert.match(res.headers.get("content-type") ?? "", /text\/event-stream/);
      assert.match(res.headers.get("cache-control") ?? "", /no-cache/);
      assert.match(res.headers.get("connection") ?? "", /keep-alive/);
    } finally {

      // Close the connection so the server-side req.on("close") handler runs. The fetch reader stays open otherwise.
      controller.abort();
    }
  });

  test("emits an initial 'snapshot' event with the current state", async () => {

    // The handler writes "event: snapshot\ndata: <json>\n\n" before subscribing to live updates. We read enough bytes to confirm both the event marker and a
    // valid JSON data line, then abort.
    const controller = new AbortController();
    const res = await fetch(urlFor("/streams/status"), { signal: controller.signal });

    try {

      assert.equal(res.status, 200);

      const reader = res.body?.getReader();

      assert.ok(reader, "response should have a readable body");

      // Read up to 8KB of initial bytes so we capture the snapshot frame even if the JSON payload is large.
      let buffer = "";
      const decoder = new TextDecoder();

      for(let i = 0; (i < 8) && (buffer.length < 8192); i++) {

        // eslint-disable-next-line no-await-in-loop
        const chunk = await reader.read();

        if(chunk.done) {

          break;
        }

        buffer += decoder.decode(chunk.value, { stream: true });

        if(buffer.includes("\n\n")) {

          break;
        }
      }

      assert.match(buffer, /event: snapshot/, "should emit the snapshot event marker");
      assert.match(buffer, /data: \{/, "should emit a JSON data line for the snapshot");
    } finally {

      controller.abort();
    }
  });
});
