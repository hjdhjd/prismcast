/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * predefined.test.ts: Tests for the predefined channel toggle endpoints. These endpoints validate input shape, route to enable/disable helpers, and return a
 * patch-style success envelope. We exercise the validation paths with mock req/res objects since the success path delegates entirely to helpers in
 * userChannels.ts that are tested in their own file.
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
import { registerPredefinedRoutes } from "./predefined.ts";

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

describe("registerPredefinedRoutes", () => {

  test("registers the toggle and bulk-toggle endpoints", () => {

    const { app, routes } = makeMockApp();

    registerPredefinedRoutes(app);

    assert.ok(routes.find((r) => (r.method === "post") && (r.path === "/config/channels/toggle-predefined")));
    assert.ok(routes.find((r) => (r.method === "post") && (r.path === "/config/channels/bulk-toggle-predefined")));
  });
});

describe("POST /config/channels/toggle-predefined", () => {

  let dir: string;
  let toggle: RequestHandler;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-predefined-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    const { app, routes } = makeMockApp();

    registerPredefinedRoutes(app);

    toggle = findRoute(routes, "post", "/config/channels/toggle-predefined");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("rejects when key is missing", async () => {

    const { json, req, res, status } = makeReqRes({ body: { enabled: true } });

    await toggle(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /Channel key is required/);
  });

  test("rejects when enabled is not a boolean", async () => {

    const { json, req, res, status } = makeReqRes({ body: { enabled: "yes", key: "abc" } });

    await toggle(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /true\/false/);
  });

  test("rejects when key is not a predefined channel", async () => {

    const { json, req, res, status } = makeReqRes({ body: { enabled: true, key: "not-a-real-key" } });

    await toggle(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /not a predefined channel/);
  });

  test("succeeds when toggling a real predefined channel (enable path)", async () => {

    const { json, req, res } = makeReqRes({ body: { enabled: true, key: "abc" } });

    await toggle(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
  });

  test("succeeds when toggling a real predefined channel (disable path)", async () => {

    const { json, req, res } = makeReqRes({ body: { enabled: false, key: "abc" } });

    await toggle(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
  });
});

describe("POST /config/channels/bulk-toggle-predefined", () => {

  let dir: string;
  let bulkToggle: RequestHandler;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-bulk-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    const { app, routes } = makeMockApp();

    registerPredefinedRoutes(app);

    bulkToggle = findRoute(routes, "post", "/config/channels/bulk-toggle-predefined");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("rejects when enabled is missing", async () => {

    const { json, req, res, status } = makeReqRes({ body: { scope: "all" } });

    await bulkToggle(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /true\/false/);
  });

  test("rejects when scope is invalid", async () => {

    const { json, req, res, status } = makeReqRes({ body: { enabled: true, scope: "garbage" } });

    await bulkToggle(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /'all', 'pacific', or 'east'/);
  });

  test("rejects when scope is missing entirely", async () => {

    const { json, req, res, status } = makeReqRes({ body: { enabled: true } });

    await bulkToggle(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /Scope must be/);
  });

  test("succeeds for scope='all' enable", async () => {

    const { json, req, res } = makeReqRes({ body: { enabled: true, scope: "all" } });

    await bulkToggle(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
  });

  test("succeeds for scope='pacific' enable", async () => {

    const { json, req, res } = makeReqRes({ body: { enabled: true, scope: "pacific" } });

    await bulkToggle(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
  });

  test("succeeds for scope='east' disable (covers the disable branch)", async () => {

    const { json, req, res } = makeReqRes({ body: { enabled: false, scope: "east" } });

    await bulkToggle(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
  });
});
