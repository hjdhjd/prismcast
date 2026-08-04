/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * tags.test.ts: Tests for the tag vocabulary management endpoints. Coverage focuses on the validation surface (name patterns, length cap, vocabulary
 * conflicts) and the response shape; the cascading channel updates run inside transformChannelTags which is tested in its own file.
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
import { registerTagRoutes } from "./tags.ts";

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

describe("registerTagRoutes", () => {

  test("registers GET, POST, DELETE, restore, and rename endpoints", () => {

    const { app, routes } = makeMockApp();

    registerTagRoutes(app);

    assert.ok(routes.find((r) => (r.method === "get") && (r.path === "/config/tags")));
    assert.ok(routes.find((r) => (r.method === "post") && (r.path === "/config/tags")));
    assert.ok(routes.find((r) => (r.method === "delete") && (r.path === "/config/tags/:tag")));
    assert.ok(routes.find((r) => (r.method === "post") && (r.path === "/config/tags/restore")));
    assert.ok(routes.find((r) => (r.method === "post") && (r.path === "/config/tags/rename")));
  });
});

describe("GET /config/tags", () => {

  let dir: string;
  let getTags: RequestHandler;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-tags-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    const { app, routes } = makeMockApp();

    registerTagRoutes(app);

    getTags = findRoute(routes, "get", "/config/tags");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("returns the active vocabulary, predefined list, and registry", () => {

    const { json, req, res } = makeReqRes();

    void getTags(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.ok(Array.isArray(body["active"]), "active is an array");
    assert.ok(Array.isArray(body["predefined"]), "predefined is an array");
    assert.ok(body["registry"], "registry present");
  });
});

describe("POST /config/tags", () => {

  let dir: string;
  let createTag: RequestHandler;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-tagcreate-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    const { app, routes } = makeMockApp();

    registerTagRoutes(app);

    createTag = findRoute(routes, "post", "/config/tags");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("rejects an empty tag name", async () => {

    const { json, req, res, status } = makeReqRes({ body: { tag: "  " } });

    await createTag(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /required/);
  });

  test("rejects a tag name longer than 30 characters", async () => {

    const { json, req, res, status } = makeReqRes({ body: { tag: "a".repeat(31) } });

    await createTag(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /30 characters or less/);
  });

  test("rejects a tag name with disallowed leading character", async () => {

    const { json, req, res, status } = makeReqRes({ body: { tag: "-Sports" } });

    await createTag(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /letter or number/);
  });

  test("rejects a tag name with disallowed trailing character", async () => {

    const { req, res, status } = makeReqRes({ body: { tag: "Sports-" } });

    await createTag(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);
  });

  test("rejects a duplicate tag (already in active vocabulary)", async () => {

    // "Sports" is in PREDEFINED_TAGS, so it appears in the active vocabulary by default.
    const { json, req, res, status } = makeReqRes({ body: { tag: "Sports" } });

    await createTag(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 409);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /already exists/);
  });

  test("succeeds for a valid new tag name and includes the tag UI bundle in the response", async () => {

    const { json, req, res } = makeReqRes({ body: { tag: "MyCustomTag" } });

    await createTag(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.ok(Array.isArray(body["active"]), "active vocabulary in bundle");
    assert.ok(body["registry"], "registry in bundle");
  });
});

describe("DELETE /config/tags/:tag", () => {

  let dir: string;
  let deleteTag: RequestHandler;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-tagdel-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    const { app, routes } = makeMockApp();

    registerTagRoutes(app);

    deleteTag = findRoute(routes, "delete", "/config/tags/:tag");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("rejects an empty tag name in the URL", async () => {

    const { json, req, res, status } = makeReqRes({ params: { tag: "  " } });

    await deleteTag(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /required/);
  });

  test("returns 404 for a tag that is not in any vocabulary", async () => {

    const { json, req, res, status } = makeReqRes({ params: { tag: "NotARealTagName" } });

    await deleteTag(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 404);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /not found/);
  });

  test("succeeds when deleting a predefined tag (adds to deletedTags)", async () => {

    const { json, req, res } = makeReqRes({ params: { tag: "Sports" } });

    await deleteTag(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
  });
});

describe("POST /config/tags/restore", () => {

  let dir: string;
  let restore: RequestHandler;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-tagrest-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    const { app, routes } = makeMockApp();

    registerTagRoutes(app);

    restore = findRoute(routes, "post", "/config/tags/restore");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("rejects an empty tag name", async () => {

    const { json, req, res, status } = makeReqRes({ body: { tag: "" } });

    await restore(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /required/);
  });

  test("returns 404 for a tag that is not in deletedTags", async () => {

    const { json, req, res, status } = makeReqRes({ body: { tag: "NotADeletedTag" } });

    await restore(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 404);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /not a deleted predefined tag/);
  });
});

describe("POST /config/tags/rename", () => {

  let dir: string;
  let rename: RequestHandler;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-tagrname-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    const { app, routes } = makeMockApp();

    registerTagRoutes(app);

    rename = findRoute(routes, "post", "/config/tags/rename");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("rejects when oldTag is missing", async () => {

    const { json, req, res, status } = makeReqRes({ body: { newTag: "Sports2" } });

    await rename(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /required/);
  });

  test("rejects when oldTag and newTag are identical", async () => {

    const { json, req, res, status } = makeReqRes({ body: { newTag: "Sports", oldTag: "Sports" } });

    await rename(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /must differ/);
  });

  test("rejects when newTag fails the name pattern", async () => {

    const { req, res, status } = makeReqRes({ body: { newTag: "-Bad", oldTag: "Sports" } });

    await rename(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 400);
  });

  test("returns 404 when oldTag is not in the vocabulary", async () => {

    const { json, req, res, status } = makeReqRes({ body: { newTag: "Movies2", oldTag: "TotallyNotARealTag" } });

    await rename(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 404);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /not found/);
  });

  test("rejects when newTag collides with an existing different tag (not a case-only rename)", async () => {

    // Both are real predefined tags. Renaming one to the other would collide.
    const { json, req, res, status } = makeReqRes({ body: { newTag: "Movies", oldTag: "Sports" } });

    await rename(req, res, () => undefined);

    assert.equal(status.mock.calls[0]?.arguments[0], 409);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.match(body["error"] as string, /already exists/);
  });
});

/* Vocabulary mutations cascade into channel tag assignments, and tags render in the playlist, so a mutation that touched any channel is a playlist-visible
 * change. Each endpoint reports what it did and appends the reload hint only when the cascade actually reached a channel - a vocabulary-only change shows
 * nothing in the playlist.
 */
describe("tag vocabulary mutation responses", () => {

  let dir: string;
  let createTag: RequestHandler;
  let deleteTag: RequestHandler;
  let rename: RequestHandler;
  let restore: RequestHandler;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-tagmsg-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    const { app, routes } = makeMockApp();

    registerTagRoutes(app);

    createTag = findRoute(routes, "post", "/config/tags");
    deleteTag = findRoute(routes, "delete", "/config/tags/:tag");
    rename = findRoute(routes, "post", "/config/tags/rename");
    restore = findRoute(routes, "post", "/config/tags/restore");
  });

  afterEach(async () => {

    await rm(dir, { force: true, recursive: true });
  });

  test("delete reports the tag and appends the hint when the cascade stripped it from channels", async () => {

    const { json, req, res } = makeReqRes({ params: { tag: "Sports" } });

    await deleteTag(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.equal(body["message"], "Tag 'Sports' deleted." + PLAYLIST_HINT);
  });

  test("delete reports the tag without the hint when no channel carried it", async () => {

    // A freshly created user tag has no channel assignments, so deleting it changes the vocabulary and nothing else - the playlist is unaffected.
    const created = makeReqRes({ body: { tag: "UnusedTestTag" } });

    await createTag(created.req, created.res, () => undefined);

    const { json, req, res } = makeReqRes({ params: { tag: "UnusedTestTag" } });

    await deleteTag(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.equal(body["message"], "Tag 'UnusedTestTag' deleted.", "a vocabulary-only change carries no reload hint");
  });

  test("restore reports the tag and appends the hint when the cascade put it back on channels", async () => {

    const deleted = makeReqRes({ params: { tag: "Sports" } });

    await deleteTag(deleted.req, deleted.res, () => undefined);

    const { json, req, res } = makeReqRes({ body: { tag: "Sports" } });

    await restore(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.equal(body["message"], "Tag 'Sports' restored." + PLAYLIST_HINT);
  });

  test("rename reports both names and appends the hint when the cascade renamed channel assignments", async () => {

    const { json, req, res } = makeReqRes({ body: { newTag: "Sports Extra", oldTag: "Sports" } });

    await rename(req, res, () => undefined);

    const body = json.mock.calls[0]?.arguments[0] as Record<string, unknown>;

    assert.equal(body["success"], true);
    assert.equal(body["message"], "Tag 'Sports' renamed to 'Sports Extra'." + PLAYLIST_HINT);
  });
});
