/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * service.test.ts: Tests for the service-selection and service-filter endpoints. The endpoints validate input shape and route to setServiceSelection /
 * mutateEnabledServices / mutateServiceSelections helpers. We test the validation paths with mock req/res; the success-with-changes paths delegate to helpers
 * tested in their own files.
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
import { registerServiceRoutes } from "./service.ts";

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

describe("registerServiceRoutes", () => {

  test("registers all four service-related endpoints", () => {

    const { app, routes } = makeMockApp();

    registerServiceRoutes(app);

    assert.ok(routes.find((r) => (r.method === "put") && (r.path === "/config/channels/:key/service")));
    assert.ok(routes.find((r) => (r.method === "post") && (r.path === "/config/service-filter")));
    assert.ok(routes.find((r) => (r.method === "post") && (r.path === "/config/service-bulk-assign")));
    assert.ok(routes.find((r) => (r.method === "post") && (r.path === "/config/service-bulk-restore")));
  });
});

describe("PUT /config/channels/:key/service", () => {

  let dir: string;
  let updateService: RequestHandler;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-svc-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    const { app, routes } = makeMockApp();

    registerServiceRoutes(app);

    updateService = findRoute(routes, "put", "/config/channels/:key/service");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("rejects when key is missing", async () => {

    const { json, req, res, status } = makeReqRes({ body: { service: "abc-hulu" }, params: {} });

    await updateService(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /Channel key is required/);
  });

  test("rejects when service is missing", async () => {

    const { json, req, res, status } = makeReqRes({ body: {}, params: { key: "abc" } });

    await updateService(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /Service key is required/);
  });

  test("rejects when channel does not have multiple services", async () => {

    // Use a key that's not in any service group. The handler queries getServiceGroup and rejects with the documented message.
    const { json, req, res, status } = makeReqRes({ body: { service: "whatever" }, params: { key: "not-a-real-channel-key" } });

    await updateService(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /does not have multiple services/);
  });

  test("rejects when service is not a valid variant for the channel", async () => {

    const { json, req, res, status } = makeReqRes({ body: { service: "abc-not-a-real-service" }, params: { key: "abc" } });

    await updateService(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /Invalid service/);
  });

  test("succeeds when given a valid channel key + service variant pair", async () => {

    const { json, req, res } = makeReqRes({ body: { service: "abc-hulu" }, params: { key: "abc" } });

    await updateService(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
  });
});

describe("POST /config/service-filter", () => {

  let dir: string;
  let filter: RequestHandler;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-filter-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    const { app, routes } = makeMockApp();

    registerServiceRoutes(app);

    filter = findRoute(routes, "post", "/config/service-filter");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("rejects when enabledServices is not an array", async () => {

    const { json, req, res, status } = makeReqRes({ body: { enabledServices: "not-an-array" } });

    await filter(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /must be an array/);
  });

  test("rejects when an unknown service tag is provided", async () => {

    const { json, req, res, status } = makeReqRes({ body: { enabledServices: ["definitely-not-a-known-service"] } });

    await filter(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /Unknown service tag/);
  });

  test("accepts an empty array (no filter, all services visible)", async () => {

    const { json, req, res } = makeReqRes({ body: { enabledServices: [] } });

    await filter(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);

    const patch = body["patch"] as Record<string, unknown>;

    assert.ok(patch, "service-bulk-restore success response should include a patch object");
    assert.deepEqual(patch["rows"], [], "counts-only patch (rows empty)");
  });

  test("accepts an array of known service tags", async () => {

    const { json, req, res } = makeReqRes({ body: { enabledServices: ["hulu"] } });

    await filter(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
  });
});

describe("POST /config/service-bulk-assign", () => {

  let dir: string;
  let bulkAssign: RequestHandler;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-bulkasgn-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    const { app, routes } = makeMockApp();

    registerServiceRoutes(app);

    bulkAssign = findRoute(routes, "post", "/config/service-bulk-assign");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("rejects when service tag is missing", async () => {

    const { json, req, res, status } = makeReqRes({ body: {} });

    await bulkAssign(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /Service tag is required/);
  });

  test("succeeds with a known service tag (returns counts in data)", async () => {

    const { json, req, res } = makeReqRes({ body: { service: "hulu" } });

    await bulkAssign(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.equal(typeof body["affected"], "number", "affected count present");
    assert.equal(typeof body["total"], "number", "total count present");
  });
});

describe("POST /config/service-bulk-restore", () => {

  let dir: string;
  let bulkRestore: RequestHandler;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-bulkrest-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    const { app, routes } = makeMockApp();

    registerServiceRoutes(app);

    bulkRestore = findRoute(routes, "post", "/config/service-bulk-restore");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("rejects when selections is missing", async () => {

    const { json, req, res, status } = makeReqRes({ body: {} });

    await bulkRestore(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /Selections map is required/);
  });

  test("rejects when selections is not an object", async () => {

    // The handler tests `typeof selections === "object"`. Passing a primitive flunks the check.
    const { req, res, status } = makeReqRes({ body: { selections: "not-an-object" } });

    await bulkRestore(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);
  });

  test("succeeds with an empty selections object (no-op)", async () => {

    const { json, req, res } = makeReqRes({ body: { selections: {} } });

    await bulkRestore(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.equal(body["restored"], 0);
  });
});
