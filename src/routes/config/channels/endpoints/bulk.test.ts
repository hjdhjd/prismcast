/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * bulk.test.ts: Tests for the bulk channel-operation endpoints (auto-number, hdhr-bulk, bulk-tags). The endpoints validate input shape, derive the visible
 * channel set, and route to the shared mutation helpers. We exercise the validation paths and the no-op paths with mock req/res; the success-with-changes paths
 * delegate to helpers tested in their own files.
 */
import type { Express, RequestHandler } from "express";
import { afterEach, beforeEach, describe, test } from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import { PLAYLIST_HINT } from "../http/playlistHint.ts";
import assert from "node:assert/strict";
import { initializeDataDir } from "../../../../config/paths.ts";
import { initializeUserChannels } from "../../../../config/userChannels.ts";
import { makeReqRes } from "../../../express.helpers.ts";
import os from "node:os";
import path from "node:path";
import { registerBulkRoutes } from "./bulk.ts";

interface CapturedRoute {

  handler: RequestHandler;
  method: "delete" | "patch" | "post" | "put";
  path: string;
}

function makeMockApp(): { app: Express; routes: CapturedRoute[] } {

  const routes: CapturedRoute[] = [];

  function capture(method: CapturedRoute["method"]) {

    return (routePath: string, handler: RequestHandler): unknown => {

      routes.push({ handler, method, path: routePath });

      return undefined;
    };
  }

  const app = { delete: capture("delete"), patch: capture("patch"), post: capture("post"), put: capture("put") } as unknown as Express;

  return { app, routes };
}

function findRoute(routes: CapturedRoute[], method: CapturedRoute["method"], routePath: string): RequestHandler {

  const route = routes.find((r) => (r.method === method) && (r.path === routePath));

  assert.ok(route, "no route registered for " + method + " " + routePath);

  return route.handler;
}

describe("registerBulkRoutes", () => {

  test("registers the three bulk-operation endpoints", () => {

    const { app, routes } = makeMockApp();

    registerBulkRoutes(app);

    assert.ok(routes.find((r) => (r.method === "post") && (r.path === "/config/channels/auto-number")));
    assert.ok(routes.find((r) => (r.method === "post") && (r.path === "/config/channels/hdhr-bulk")));
    assert.ok(routes.find((r) => (r.method === "post") && (r.path === "/config/channels/bulk-tags")));
  });
});

describe("POST /config/channels/auto-number", () => {

  let dir: string;
  let autoNumber: RequestHandler;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-autonum-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    const { app, routes } = makeMockApp();

    registerBulkRoutes(app);

    autoNumber = findRoute(routes, "post", "/config/channels/auto-number");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("rejects a starting number outside [1, 99999] (negative)", async () => {

    const { json, req, res, status } = makeReqRes({ body: { start: -5 } });

    await autoNumber(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /between 1 and 99999/);
  });

  test("rejects a starting number outside [1, 99999] (too large)", async () => {

    const { json, req, res, status } = makeReqRes({ body: { start: 100_000 } });

    await autoNumber(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /between 1 and 99999/);
  });

  test("rejects an unknown sortField", async () => {

    const { json, req, res, status } = makeReqRes({ body: { sortField: "garbage" } });

    await autoNumber(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /Invalid sort field/);
  });

  test("accepts start=0 (clear-mode boundary), bypassing the [1, 99999] range check", async () => {

    // Boundary: start=0 is the documented "clear all channel numbers" sentinel. The range check explicitly excludes this case via the !clearMode guard.
    const { json, req, res } = makeReqRes({ body: { start: 0 } });

    await autoNumber(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
  });

  test("accepts a valid request and returns a success envelope", async () => {

    const { json, req, res } = makeReqRes({ body: { start: 1 } });

    await autoNumber(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
  });

  test("appends the playlist reload hint when channels were numbered", async () => {

    // Channel numbers are playlist-visible, so a run that actually numbered channels tells the user to reload. The default listing is non-empty, so this is the
    // hinted direction; the hint is gated on the affected count, which an empty visible listing would drive to zero.
    const { json, req, res } = makeReqRes({ body: { start: 1 } });

    await autoNumber(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.ok(((body["patch"] as { rows?: unknown[] } | undefined)?.rows?.length ?? 0) > 0, "the run numbered at least one channel");
    assert.ok((body["message"] as string).endsWith(PLAYLIST_HINT), "the success message carries the reload hint");
  });
});

describe("POST /config/channels/hdhr-bulk", () => {

  let dir: string;
  let hdhrBulk: RequestHandler;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-hdhr-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    const { app, routes } = makeMockApp();

    registerBulkRoutes(app);

    hdhrBulk = findRoute(routes, "post", "/config/channels/hdhr-bulk");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("returns 'No changes needed' when every visible channel already matches the requested state", async () => {

    // Default state: all predefined channels are HDHR-enabled. Requesting enable=true is a no-op since current === enable for every entry.
    const { json, req, res } = makeReqRes({ body: { enable: true } });

    await hdhrBulk(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.equal(body["message"], "No changes needed.");
  });
});

describe("POST /config/channels/bulk-tags", () => {

  let dir: string;
  let bulkTags: RequestHandler;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-tags-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    const { app, routes } = makeMockApp();

    registerBulkRoutes(app);

    bulkTags = findRoute(routes, "post", "/config/channels/bulk-tags");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("rejects an action other than 'add' or 'remove'", async () => {

    const { json, req, res, status } = makeReqRes({ body: { action: "swap", tag: "Sports" } });

    await bulkTags(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /'add' or 'remove'/);
  });

  test("rejects an empty tag", async () => {

    const { json, req, res, status } = makeReqRes({ body: { action: "add", tag: "" } });

    await bulkTags(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /Tag is required/);
  });

  test("rejects a tag that is not in the active vocabulary", async () => {

    const { json, req, res, status } = makeReqRes({ body: { action: "add", tag: "DefinitelyNotAKnownTag" } });

    await bulkTags(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /Unknown tag/);
  });

  test("appends the playlist reload hint when a tag was applied to channels", async () => {

    // Tags render in the playlist as group-title and tvc-guide-tags, so a bulk retag is a playlist-visible change and the success message says so.
    const { json, req, res } = makeReqRes({ body: { action: "add", tag: "Sports" } });

    await bulkTags(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.ok((body["message"] as string).endsWith(PLAYLIST_HINT), "the success message carries the reload hint");
  });
});
