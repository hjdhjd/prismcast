/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * setup.test.ts: Tests for the setupChannelRoutes aggregator. The aggregator's contract is structural: it must invoke every per-feature register function so the
 * full channel-config endpoint surface is wired onto the Express app. We mock the app to capture each registration and verify the documented endpoints land
 * in the resulting route table.
 */
import type { Express, RequestHandler } from "express";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { setupChannelRoutes } from "./setup.ts";

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

describe("setupChannelRoutes", () => {

  test("registers the full set of channel-config endpoints across every feature group", () => {

    const { app, routes } = makeMockApp();

    setupChannelRoutes(app);

    /* The complete documented endpoint surface across the eight per-feature register functions. We assert each verb+path combination lands so a regression that
     * silently drops a feature group (e.g., forgetting to call registerTagRoutes) surfaces here as a missing route rather than only as a runtime 404 in
     * production.
     */
    const expected: { method: CapturedRoute["method"]; path: string }[] = [
      // browse.ts
      { method: "post", path: "/config/channels/modify" },
      // bulk.ts
      { method: "post", path: "/config/channels/auto-number" },
      { method: "post", path: "/config/channels/hdhr-bulk" },
      { method: "post", path: "/config/channels/bulk-tags" },
      // crud.ts
      { method: "post", path: "/config/channels" },
      { method: "put", path: "/config/channels/:key" },
      { method: "delete", path: "/config/channels/:key" },
      { method: "post", path: "/config/channels/:key/revert" },
      { method: "patch", path: "/config/channels/:key" },
      // importExport.ts
      { method: "get", path: "/config/channels/export" },
      { method: "post", path: "/config/channels/import" },
      { method: "post", path: "/config/channels/import-m3u" },
      // predefined.ts
      { method: "post", path: "/config/channels/toggle-predefined" },
      { method: "post", path: "/config/channels/bulk-toggle-predefined" },
      // prefs.ts
      { method: "post", path: "/config/channels/display-prefs" },
      { method: "post", path: "/config/channels/setup-completed" },
      // service.ts
      { method: "put", path: "/config/channels/:key/service" },
      { method: "post", path: "/config/service-filter" },
      { method: "post", path: "/config/service-bulk-assign" },
      { method: "post", path: "/config/service-bulk-restore" },
      // tags.ts
      { method: "get", path: "/config/tags" },
      { method: "post", path: "/config/tags" },
      { method: "delete", path: "/config/tags/:tag" },
      { method: "post", path: "/config/tags/restore" },
      { method: "post", path: "/config/tags/rename" }
    ];

    for(const { method, path } of expected) {

      const found = routes.find((r) => (r.method === method) && (r.path === path));

      assert.ok(found, "missing route: " + method.toUpperCase() + " " + path);
    }
  });

  test("registers exactly the expected number of routes (catches accidental duplicate or extra registrations)", () => {

    const { app, routes } = makeMockApp();

    setupChannelRoutes(app);

    // 25 documented endpoints across the eight feature groups; lock the count so any drift surfaces as a clear test diff.
    assert.equal(routes.length, 25, "expected 25 channel-config routes");
  });
});
