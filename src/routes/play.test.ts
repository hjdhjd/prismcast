/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * play.test.ts: Unit tests for the ad-hoc URL streaming route in play.ts. The module exports setupPlayEndpoint which registers GET /play and delegates to
 * handlePlayStream in streaming/hls.ts. The full streaming path requires a real Chrome browser - that coverage lives in the e2e suite. What this suite covers is
 * the validation the handler performs at its ingress, before any browser setup or capacity reservation: a missing, empty, or whitespace-only url parameter, and
 * a url whose scheme falls outside the allowlist.
 *
 * The scheme case carries an assertion the others do not. Its status and body are identical whether the URL is refused at the ingress or deeper inside
 * setupStream, so the observable that distinguishes them is whether an outbound request was ever attempted on the URL's behalf - which is why that test spies on
 * the global fetch rather than reading the response alone.
 */
import type { AddressInfo, Server } from "node:net";
import { after, before, describe, mock, test } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWss } from "../testing.helpers.ts";
import express from "express";
import { setupPlayEndpoint } from "./play.ts";

// An ad-hoc URL whose scheme is outside validateStreamUrl's allowlist. Named once because the pin below both requests it and asserts nothing was sent to it.
const BAD_SCHEME_URL = "ftp://redirect-probe.invalid/stream";

function makeServer(): Promise<{ port: number; server: Server }> {

  const app = express();

  setupPlayEndpoint(app);

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

describe("setupPlayEndpoint - GET /play (validation paths)", () => {

  test("returns 400 when no url query parameter is provided", async () => {

    // Negative test: handlePlayStream short-circuits with 400 before any browser interaction when url is missing or empty after trim.
    const res = await fetch(urlFor("/play"));

    assert.equal(res.status, 400);

    const body = await res.text();

    assert.match(body, /url query parameter is required/);
  });

  test("returns 400 when the url parameter is whitespace-only (trim collapses to empty)", async () => {

    // Boundary: the handler trims the URL before checking. A whitespace-only value collapses to "" and triggers the same 400 path.
    const res = await fetch(urlFor("/play?url=%20%20%20"));

    assert.equal(res.status, 400);

    const body = await res.text();

    assert.match(body, /url query parameter is required/);
  });

  test("returns 400 when the url parameter is an empty string", async () => {

    const res = await fetch(urlFor("/play?url="));

    assert.equal(res.status, 400);
    await res.text();
  });

  test("an unsupported scheme is refused at the ingress without any outbound request on its behalf", async () => {

    /* The status and body are the same whether the URL is judged at the ingress or deep inside setupStream, so neither one tells the two apart. What does is
     * whether the request ever leaves the machine. Profile resolution is keyed on hostname, and no profile maps the hostname below, so this URL resolves to the
     * "default" profile - which is the condition that sends setupStream to the redirect probe, and the probe issues a HEAD through chromeFetch before the
     * deeper validation runs. Judging the URL at the ingress means the probe is never reached, so we spy on the global fetch and assert that nothing was
     * addressed to the rejected URL.
     *
     * The spy passes through to the real implementation because this test's own request to the local listener goes through the same global; the assertion
     * filters to calls that name the rejected URL rather than counting calls outright.
     */
    const attempted: string[] = [];
    const realFetch = globalThis.fetch;

    mock.method(globalThis, "fetch", async (input: unknown, init?: unknown): Promise<Response> => {

      attempted.push(String(input));

      return realFetch(input as RequestInfo, init as RequestInit);
    });

    try {

      const res = await fetch(urlFor("/play?url=" + encodeURIComponent(BAD_SCHEME_URL)));

      assert.equal(res.status, 400, "an unsupported scheme is refused with 400");

      const body = await res.text();

      // The body must stay exactly what the deeper path produces - the bare validation reason - so a client sees no difference in where the judgement was made.
      assert.equal(body, "Unsupported protocol: ftp:", "the rejection body is the bare validation reason");
      assert.equal(attempted.includes(BAD_SCHEME_URL), false, "no outbound request may be addressed to a URL the ingress refuses");
    } finally {

      mock.restoreAll();
    }
  });
});
