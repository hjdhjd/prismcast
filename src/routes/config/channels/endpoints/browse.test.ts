/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * browse.test.ts: Tests for the browse-channels modal endpoint. The endpoint dispatches four action types ('add', 'enable', 'switch', 'remove') from a single
 * batch. Coverage focuses on the validation surface, the no-op response, and the per-entry error reporting (missing fields, invalid URLs).
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
import { registerBrowseRoutes } from "./browse.ts";

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

describe("registerBrowseRoutes", () => {

  test("registers POST /config/channels/modify", () => {

    const { app, routes } = makeMockApp();

    registerBrowseRoutes(app);

    assert.ok(routes.find((r) => (r.method === "post") && (r.path === "/config/channels/modify")));
  });
});

describe("POST /config/channels/modify", () => {

  let dir: string;
  let modify: RequestHandler;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-browse-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    const { app, routes } = makeMockApp();

    registerBrowseRoutes(app);

    modify = findRoute(routes, "post", "/config/channels/modify");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("rejects when channels is missing", async () => {

    const { json, req, res, status } = makeReqRes({ body: {} });

    await modify(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /No channels provided/);
  });

  test("rejects when channels is not an array", async () => {

    const { req, res, status } = makeReqRes({ body: { channels: "not-an-array" } });

    await modify(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);
  });

  test("rejects when channels is an empty array", async () => {

    const { json, req, res, status } = makeReqRes({ body: { channels: [] } });

    await modify(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /No channels provided/);
  });

  test("returns 'No changes made.' when given an entry that produces no effect (add with no name/url)", async () => {

    // Per-entry validation failures are intentionally non-fatal: a bulk browse batch applies its valid entries and silently drops the invalid ones, so an entry
    // with no add/switch/remove effect leaves nothing counted and the envelope deliberately reports only the success counts with 'No changes made.' here.
    const { json, req, res } = makeReqRes({ body: { channels: [{ action: "add" }] } });

    await modify(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.equal(body["message"], "No changes made.");
  });

  test("counts and reports added channels in the success message", async () => {

    const channels = [{ action: "add", name: "My New Channel", url: "https://example.com/live" }];
    const { json, req, res } = makeReqRes({ body: { channels } });

    await modify(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.match(body["message"] as string, /Added 1 channel/);
  });

  test("counts and reports a switch action", async () => {

    const channels = [{ action: "switch", canonicalKey: "abc", channelSelector: "ABC", serviceSlug: "hulu", url: "https://www.hulu.com/live" }];
    const { json, req, res } = makeReqRes({ body: { channels } });

    await modify(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.match(body["message"] as string, /Switched 1 channel/);
  });

  test("rejects a switch entry that is missing canonicalKey but still returns 200 with no changes", async () => {

    // Per-entry validation failures are intentionally non-fatal, so when nothing succeeds the envelope deliberately returns 200 with the no-changes message.
    const channels = [{ action: "switch", channelSelector: "ABC", name: "ABC", serviceSlug: "hulu" }];
    const { json, req, res } = makeReqRes({ body: { channels } });

    await modify(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.equal(body["message"], "No changes made.");
  });

  test("rejects an add entry that is missing the URL", async () => {

    const channels = [{ action: "add", name: "My Channel" }];
    const { json, req, res } = makeReqRes({ body: { channels } });

    await modify(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true, "envelope still success even when per-entry validation fails");
    assert.equal(body["message"], "No changes made.");
  });

  test("rejects an add entry with an invalid URL (validateChannelUrl path)", async () => {

    const channels = [{ action: "add", name: "My Channel", url: "definitely not a url" }];
    const { json, req, res } = makeReqRes({ body: { channels } });

    await modify(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.equal(body["message"], "No changes made.");
  });
});
