/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * prefs.test.ts: Tests for the channel display-preferences and setup-completed endpoints. The endpoints validate input shape and route to mutateChannelDisplayPrefs
 * / markSetupCompleted helpers - the validation logic lives at the HTTP boundary, the persistence lives in config/userChannels.ts. We test the validation
 * branches with mock req/res and confirm the success path returns the expected envelope shape.
 */
import type { Express, RequestHandler } from "express";
import { afterEach, beforeEach, describe, test } from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import assert from "node:assert/strict";
import { initializeDataDir } from "../../../../config/paths.ts";
import { initializeUserChannels } from "../../../../config/userChannels.ts";
import { makeReqRes } from "../../../express.helpers.ts";
import os from "node:os";
import path from "node:path";
import { registerPrefsRoutes } from "./prefs.ts";

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

describe("registerPrefsRoutes", () => {

  test("registers the display-prefs and setup-completed endpoints", () => {

    const { app, routes } = makeMockApp();

    registerPrefsRoutes(app);

    assert.ok(routes.find((r) => (r.method === "post") && (r.path === "/config/channels/display-prefs")));
    assert.ok(routes.find((r) => (r.method === "post") && (r.path === "/config/channels/setup-completed")));
  });
});

describe("POST /config/channels/display-prefs", () => {

  let dir: string;
  let prefs: RequestHandler;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-prefs-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    const { app, routes } = makeMockApp();

    registerPrefsRoutes(app);

    prefs = findRoute(routes, "post", "/config/channels/display-prefs");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("rejects when visibleColumns is not an array", async () => {

    const { json, req, res, status } = makeReqRes({ body: { visibleColumns: "not-an-array" } });

    await prefs(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /must be an array/);
  });

  test("rejects when visibleColumns contains an unknown column name", async () => {

    const { json, req, res, status } = makeReqRes({ body: { visibleColumns: [ "channelNumber", "totally-bogus" ] } });

    await prefs(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /Unknown column: totally-bogus/);
  });

  test("rejects when sortField is not a recognized field", async () => {

    const { json, req, res, status } = makeReqRes({ body: { sortField: "not-a-real-field" } });

    await prefs(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /Unknown sort field/);
  });

  test("rejects when sortDirection is neither 'asc' nor 'desc'", async () => {

    const { json, req, res, status } = makeReqRes({ body: { sortDirection: "sideways" } });

    await prefs(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /asc.*desc/);
  });

  test("succeeds when given a valid sortField + sortDirection combination", async () => {

    const { json, req, res } = makeReqRes({ body: { sortDirection: "desc", sortField: "name" } });

    await prefs(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
  });

  test("accepts an empty body and returns success (every field is optional)", async () => {

    const { json, req, res } = makeReqRes({ body: {} });

    await prefs(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
  });

  test("accepts a valid visibleColumns array", async () => {

    const { json, req, res } = makeReqRes({ body: { visibleColumns: [ "channelNumber", "tags" ] } });

    await prefs(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
  });
});

describe("POST /config/channels/setup-completed", () => {

  let dir: string;
  let setup: RequestHandler;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-setup-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    const { app, routes } = makeMockApp();

    registerPrefsRoutes(app);

    setup = findRoute(routes, "post", "/config/channels/setup-completed");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("returns a counts-only patch on success", async () => {

    const { json, req, res } = makeReqRes({});

    await setup(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);

    const patch = body["patch"] as Record<string, unknown>;

    assert.ok(patch, "patch should be present");
    assert.ok(patch["counts"], "counts must be present");
    assert.deepEqual(patch["rows"], [], "rows must be empty (counts-only)");
    assert.ok(patch["scopeCounts"], "scopeCounts must be present");
  });
});
