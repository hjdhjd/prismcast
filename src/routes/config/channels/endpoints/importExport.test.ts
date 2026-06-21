/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * importExport.test.ts: Tests for the channel import/export endpoints. Coverage focuses on the validation surface (M3U body presence, conflict-mode literals,
 * imported-channels validation) and the export response headers; the channel-replacement and conflict-detection paths delegate to validateImportedChannels and
 * mutateChannels which are tested in their own files.
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
import { registerImportExportRoutes } from "./importExport.ts";

interface CapturedRoute {

  handler: RequestHandler;
  method: "delete" | "get" | "patch" | "post" | "put";
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

  const app = {

    delete: capture("delete"),
    get: capture("get"),
    patch: capture("patch"),
    post: capture("post"),
    put: capture("put")
  } as unknown as Express;

  return { app, routes };
}

function findRoute(routes: CapturedRoute[], method: CapturedRoute["method"], routePath: string): RequestHandler {

  const route = routes.find((r) => (r.method === method) && (r.path === routePath));

  assert.ok(route, "no route registered for " + method + " " + routePath);

  return route.handler;
}

describe("registerImportExportRoutes", () => {

  test("registers export, import, and import-m3u endpoints", () => {

    const { app, routes } = makeMockApp();

    registerImportExportRoutes(app);

    assert.ok(routes.find((r) => (r.method === "get") && (r.path === "/config/channels/export")));
    assert.ok(routes.find((r) => (r.method === "post") && (r.path === "/config/channels/import")));
    assert.ok(routes.find((r) => (r.method === "post") && (r.path === "/config/channels/import-m3u")));
  });
});

describe("GET /config/channels/export", () => {

  let dir: string;
  let exporter: RequestHandler;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-export-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    const { app, routes } = makeMockApp();

    registerImportExportRoutes(app);

    exporter = findRoute(routes, "get", "/config/channels/export");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("sets the Content-Type header to application/json", () => {

    const { req, res, setHeader } = makeReqRes();

    void exporter(req, res, () => undefined);

    const headerCalls = setHeader.mock.calls.map((c) => c.arguments);
    const contentType = headerCalls.find((args) => args[0] === "Content-Type");

    assert.ok(contentType, "Content-Type header set");
    assert.equal(contentType[1], "application/json");
  });

  test("sets the Content-Disposition header to attachment", () => {

    const { req, res, setHeader } = makeReqRes();

    void exporter(req, res, () => undefined);

    const headerCalls = setHeader.mock.calls.map((c) => c.arguments);
    const disposition = headerCalls.find((args) => args[0] === "Content-Disposition");

    assert.ok(disposition, "Content-Disposition header set");
    assert.match(disposition[1] as string, /attachment/);
    assert.match(disposition[1] as string, /\.json/);
  });

  test("emits a JSON-serialized body via res.send", () => {

    const { req, res, send } = makeReqRes();

    void exporter(req, res, () => undefined);

    assert.equal(send.mock.callCount(), 1, "send must be called once");

    const body = send.mock.calls[0]?.arguments[0] as string;

    // The body is JSON; on an empty user-channels store it should still parse into an empty object.
    const parsed = JSON.parse(body) as Record<string, unknown>;

    assert.equal(typeof parsed, "object");
  });
});

describe("POST /config/channels/import", () => {

  let dir: string;
  let importer: RequestHandler;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-import-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    const { app, routes } = makeMockApp();

    registerImportExportRoutes(app);

    importer = findRoute(routes, "post", "/config/channels/import");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("rejects when validation fails (e.g., missing required fields)", async () => {

    // The validator requires name and url as strings on every entry. An entry missing both fails validation.
    const { json, req, res, status } = makeReqRes({ body: { foo: { /* missing required fields */ } } });

    await importer(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /Validation errors/);
  });

  test("succeeds for a valid import payload (single user channel)", async () => {

    // Round-trip through the handler. The mutateChannels callback receives the full ChannelsFileData and reassigns data.channels rather than mutating top-level
    // keys; this exercises the import path end-to-end against the real persistence layer.
    const payload = { mychannel: { name: "My Channel", url: "https://example.com/live.m3u8" } };
    const { json, req, res } = makeReqRes({ body: payload });

    await importer(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.match(body["message"] as string, /Imported 1 channel/);
  });
});

describe("POST /config/channels/import-m3u", () => {

  let dir: string;
  let importM3u: RequestHandler;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-impm3u-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    const { app, routes } = makeMockApp();

    registerImportExportRoutes(app);

    importM3u = findRoute(routes, "post", "/config/channels/import-m3u");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("rejects when content is empty", async () => {

    const { json, req, res, status } = makeReqRes({ body: { content: "" } });

    await importM3u(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /No M3U content/);
  });

  test("rejects when content is missing entirely", async () => {

    const { json, req, res, status } = makeReqRes({ body: {} });

    await importM3u(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /No M3U content/);
  });

  test("rejects an invalid conflictMode", async () => {

    const { json, req, res, status } = makeReqRes({ body: { conflictMode: "merge", content: "#EXTM3U\n#EXTINF:-1,Channel\nhttps://example.com/live" } });

    await importM3u(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /skip.*replace/);
  });

  test("rejects when the M3U content yields no parsed channels", async () => {

    const { json, req, res, status } = makeReqRes({ body: { content: "garbage that is not m3u" } });

    await importM3u(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /No channels found/);
  });

  test("succeeds for a valid M3U payload (single channel)", async () => {

    const m3u = "#EXTM3U\n#EXTINF:-1,My Channel\nhttps://example.com/live";
    const { json, req, res } = makeReqRes({ body: { content: m3u } });

    await importM3u(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.equal(body["imported"], 1, "one channel imported");
  });
});
