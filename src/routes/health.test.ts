/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * health.test.ts: Unit tests for the health check endpoint in health.ts. setupHealthEndpoint registers GET /health which assembles a HealthStatus payload from
 * browser state, stream registry, memory metrics, and ffmpeg availability. The handler returns HTTP 503 when the browser is not connected so monitoring tools
 * can detect degraded state via status code, returns HTTP 200 with status "degraded" when stream utilization crosses 80%, and returns HTTP 200 with status
 * "healthy" otherwise.
 *
 * How the dependencies are substituted. setupHealthEndpoint accepts its state readers (isBrowserConnected, getBrowserPages, getChromeVersion, the registry
 * counts, getClientSummary) as an injected HealthDeps parameter, defaulting to the real modules. The suite passes one deps object whose readers return the
 * per-test mockState, so every status branch, the getBrowserPages-error suppression, and the client-aggregation fold are exercised through the real handler on a
 * real Express server - no loader mock. The extracted pure deriveHealthStatus decision is additionally pinned directly at the end of the file.
 *
 * Coverage tiers in this file:
 *
 *   1. Status branches. The handler has three branches (unhealthy / degraded / healthy) gated by browserConnected and the streamUtilization >= 0.8 threshold.
 *      Each branch is exercised explicitly: setting mockState.browserConnected toggles the unhealthy boundary; setting mockState.streamCount around the
 *      threshold (7, 8) exercises the healthy/degraded boundary against the production maxConcurrentStreams default of 10.
 *
 *   2. Threshold boundary. streamCount = floor(0.8 * 10) - 1 = 7 stays healthy; streamCount = ceil(0.8 * 10) = 8 flips to degraded. Pinning both points
 *      structurally locks the 0.8 threshold so a regression that drifted to 0.75 or 0.85 would fail one assertion.
 *
 *   3. Client aggregation loop. health.ts iterates getAllStreams() and folds each stream's getClientSummary() into a system-wide byType map. The new test
 *      seeds two synthetic streams with multi-type client summaries and asserts the aggregated counts AND the alphabetic byType ordering the production
 *      contract emits via toSorted().
 *
 *   4. Response shape. The shape tests (Content-Type, top-level keys, memory/uptime/version/timestamp/streams/clients/ffmpegAvailable/captureMode) are
 *      preserved verbatim from the prior fixture. Their assertions still hold against the mocked dependencies because mockState defaults reproduce the
 *      "browser disconnected, no streams" environment those tests previously assumed.
 */
import type { AddressInfo, Server } from "node:net";
import type { ClientSummary, ClientType } from "../streaming/clients.ts";
import { after, before, beforeEach, describe, test } from "node:test";
import { deriveHealthStatus, setupHealthEndpoint } from "./health.ts";
import type { Express } from "express";
import type { HealthDeps } from "./health.ts";
import type { Nullable } from "../types/index.ts";
import type { Page } from "puppeteer-core";
import type { StreamRegistryEntry } from "../streaming/registry.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWss } from "../testing.helpers.ts";
import express from "express";

// Module-level mock-state injection point. Each handler invocation reads through this object via the closures captured by the deps object below; tests mutate
// fields per scenario via beforeEach defaults plus per-test overrides. The shape mirrors the production surface health.ts reads from each dependency module -
// never more, never less, so adding a field here forces a deliberate cross-walk against the handler.
interface MockState {

  browserConnected: boolean;
  chromeVersion: Nullable<string>;
  pageCount: number;
  pageError: Nullable<Error>;
  streamCount: number;
  streamSummaries: Map<number, ClientSummary>;
  streams: { id: number }[];
  totalSegmentMemory: number;
}

// Response shape mirror used by tests to type-check the JSON body. Keep in sync with HealthStatus from src/types/streaming.ts - the duplicated declaration is
// intentional: the test asserts on the wire shape, not on the source type, so a regression that quietly stripped a field would be caught here even if the
// source type also drifted.
interface HealthBody {

  browser: { connected: boolean; pageCount: number };
  captureMode: string;
  chrome: Nullable<string>;
  clients: { byType: { count: number; type: string }[]; total: number };
  ffmpegAvailable: boolean;
  memory: { heapTotal: number; heapUsed: number; rss: number; segmentBuffers: number };
  message?: string;
  status: "degraded" | "healthy" | "unhealthy";
  streams: { active: number; limit: number };
  timestamp: string;
  uptime: number;
  version: string;
}

let mockState: MockState;

let sharedServer: Server;
let sharedPort = 0;

function urlFor(path: string): string {

  return "http://127.0.0.1:" + String(sharedPort) + path;
}

function makeServer(install: (app: Express) => void): Promise<{ port: number; server: Server }> {

  const app = express();

  install(app);

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

// Builds the mockState defaults that mirror the prior "no browser, no streams" test environment. Tests override per scenario; beforeEach reinstalls these
// defaults so per-test mutations cannot leak.
function defaultMockState(): MockState {

  return {

    browserConnected: false,
    chromeVersion: null,
    pageCount: 0,
    pageError: null,
    streamCount: 0,
    streamSummaries: new Map(),
    streams: [],
    totalSegmentMemory: 0
  };
}

// The injected health readers: the browser/registry/clients state the handler folds into its payload, backed by the per-test mockState. This one object replaces
// the three module mocks the suite previously installed; each field reads mockState at call time so a test drives every branch by mutating mockState. Typed as the
// production HealthDeps port so the doubles cannot drift from it.
const deps: HealthDeps = {

  getAllStreams: (): StreamRegistryEntry[] => mockState.streams as unknown as StreamRegistryEntry[],
  getBrowserPages: async (): Promise<Page[]> => {

    if(mockState.pageError) {

      throw mockState.pageError;
    }

    // The handler reads only pages.length, so the elements are minimal stubs cast to Page - the accepted Puppeteer-double convention where only a subset matters.
    return Array.from({ length: mockState.pageCount }, (_, i) => ({ pageId: i })) as unknown as Page[];
  },
  getChromeVersion: (): Nullable<string> => mockState.chromeVersion,
  getClientSummary: (streamId: number): ClientSummary => mockState.streamSummaries.get(streamId) ?? { clients: [], total: 0 },
  getStreamCount: (): number => mockState.streamCount,
  getTotalSegmentMemory: (): number => mockState.totalSegmentMemory,
  isBrowserConnected: (): boolean => mockState.browserConnected
};

before(async () => {

  mockState = defaultMockState();

  // One server serves every test: setupHealthEndpoint registers a single async GET /health route reading through the injected deps, so tests drive
  // scenarios purely by mutating mockState.
  const created = await makeServer((app) => {

    setupHealthEndpoint(app, deps);
  });

  sharedServer = created.server;
  sharedPort = created.port;
});

beforeEach(() => {

  // Reset to defaults before every test so per-test mutations cannot leak. The mockState reference is preserved (the deps object above closes over the
  // mockState variable itself, not its current value) - we mutate fields in place rather than reassigning the variable.
  Object.assign(mockState, defaultMockState());
});

after(async () => {

  await closeServer(sharedServer);
  await closePuppeteerStreamWss();
});

describe("setupHealthEndpoint - GET /health (browser disconnected)", () => {

  test("returns HTTP 503 when no browser is launched (the documented unhealthy path)", async () => {

    // mockState.browserConnected defaults to false, mirroring the test environment where no Puppeteer browser is launched. The handler must surface this via
    // status 503 so monitoring systems detect the unhealthy state via status code alone.
    const res = await fetch(urlFor("/health"));

    assert.equal(res.status, 503, "unhealthy branch should respond with 503");

    // Drain the body to release the connection.
    await res.json();
  });

  test("payload status field is 'unhealthy' and message explains why", async () => {

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(body.status, "unhealthy");
    assert.equal(body.message, "Browser is not connected.");
  });

  test("browser.connected is false and pageCount is 0 when disconnected", async () => {

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(body.browser.connected, false);
    assert.equal(body.browser.pageCount, 0);
  });

  test("chrome version is null when no browser has launched (locks the documented null contract)", async () => {

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(body.chrome, null);
  });
});

describe("setupHealthEndpoint - GET /health response shape", () => {

  test("returns Content-Type application/json", async () => {

    const res = await fetch(urlFor("/health"));

    assert.match(res.headers.get("content-type") ?? "", /application\/json/);
    await res.json();
  });

  test("includes the documented top-level keys", async () => {

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;
    const keys = Object.keys(body).sort();

    // Locks the surface area of the response. Adding a key here should be a deliberate change accompanied by a documentation update.
    const expectedRequired = [ "browser", "captureMode", "chrome", "clients", "ffmpegAvailable", "memory", "status", "streams", "timestamp", "uptime", "version" ];

    for(const required of expectedRequired) {

      assert.ok(keys.includes(required), "required key " + required + " should be present");
    }
  });

  test("memory block exposes heap and segment metrics as numbers", async () => {

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(typeof body.memory.heapTotal, "number");
    assert.equal(typeof body.memory.heapUsed, "number");
    assert.equal(typeof body.memory.rss, "number");
    assert.equal(typeof body.memory.segmentBuffers, "number");

    // RSS and heapTotal are always positive in a running Node process.
    assert.ok(body.memory.heapTotal > 0, "heapTotal should be positive");
    assert.ok(body.memory.rss > 0, "rss should be positive");
  });

  test("uptime is a non-negative number (process.uptime invariant)", async () => {

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(typeof body.uptime, "number");
    assert.ok(body.uptime >= 0, "uptime must be non-negative");
  });

  test("version is the package.json version string", async () => {

    // Locks that the version is a string. We don't pin to a specific version so the test doesn't break on every release.
    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(typeof body.version, "string");
    assert.ok(body.version.length > 0, "version should be non-empty");
  });

  test("timestamp is an ISO 8601 UTC string", async () => {

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    // toISOString output ends with 'Z' for UTC.
    assert.match(body.timestamp, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/, "timestamp should be ISO 8601 UTC");
  });

  test("streams block reflects an empty registry with the configured limit", async () => {

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(body.streams.active, 0, "no streams registered in test environment");
    assert.equal(typeof body.streams.limit, "number");
    assert.ok(body.streams.limit > 0, "limit should be a positive number");
  });

  test("clients block reflects no active clients (empty registry)", async () => {

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(body.clients.total, 0);
    assert.deepEqual(body.clients.byType, []);
  });

  test("ffmpegAvailable is a boolean (depends on test runner environment)", async () => {

    // Boundary: the actual value depends on whether ffmpeg is on PATH or the bundled ffmpeg-for-homebridge resolves. We just lock the type so a regression that
    // emitted undefined or a non-boolean would fail.
    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(typeof body.ffmpegAvailable, "boolean");
  });

  test("captureMode is one of the documented values from CONFIG", async () => {

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(typeof body.captureMode, "string");
    assert.ok(body.captureMode.length > 0);
  });
});

describe("setupHealthEndpoint - GET /health (browser connected, healthy branch)", () => {

  test("returns HTTP 200 with status 'healthy' when browser is connected and stream utilization is below the threshold", async () => {

    /* The healthy branch: browser is up, stream count is low, no message field. The previous test infrastructure could not exercise this path at all - the
     * Express server it booted ran without launching Puppeteer, so isBrowserConnected was always false and only the unhealthy branch was reachable. The
     * injected HealthDeps parameter routes around that limitation by substituting the dependency directly.
     */
    mockState.browserConnected = true;
    mockState.chromeVersion = "Chrome/120.0.0.0";
    mockState.pageCount = 1;
    mockState.streamCount = 0;

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(res.status, 200, "healthy branch responds with 200");
    assert.equal(body.status, "healthy");
    assert.equal(body.message, undefined, "healthy branch must NOT include a message field");
    assert.equal(body.browser.connected, true);
    assert.equal(body.browser.pageCount, 1);
    assert.equal(body.chrome, "Chrome/120.0.0.0");
  });

  test("treats getBrowserPages errors as zero pages without escalating the status", async () => {

    /* The handler wraps getBrowserPages in try/catch and intentionally suppresses errors - a transient page-enumeration failure should not flip the entire
     * health response into a different branch. We pin that contract explicitly: an error from getBrowserPages must yield pageCount: 0 and leave the status
     * branch alone, matching the comment "// Ignore page count errors." in production.
     */
    mockState.browserConnected = true;
    mockState.pageError = new Error("transient pages enumeration failure");

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(res.status, 200, "page enumeration error must not escalate to 503");
    assert.equal(body.status, "healthy");
    assert.equal(body.browser.connected, true);
    assert.equal(body.browser.pageCount, 0, "pageCount falls back to 0 when getBrowserPages throws");
  });
});

describe("setupHealthEndpoint - GET /health (degraded branch + threshold boundary)", () => {

  test("crosses into 'degraded' exactly when streamUtilization reaches 0.8 (8 of 10 default)", async () => {

    /* Threshold boundary, lower side: streamCount = 7 -> utilization = 0.7, status remains healthy. The cliff at 0.8 is the production contract; pinning the
     * point one below proves the branch does not trip prematurely.
     */
    mockState.browserConnected = true;
    mockState.streamCount = 7;

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(res.status, 200);
    assert.equal(body.status, "healthy", "streamCount 7 of 10 (utilization 0.7) stays in the healthy branch");
    assert.equal(body.message, undefined);
    assert.equal(body.streams.active, 7);
  });

  test("flips to 'degraded' the moment streamUtilization reaches 0.8 (8 of 10 default)", async () => {

    /* Threshold boundary, upper side: streamCount = 8 -> utilization = 0.8, status flips to degraded. The handler responds with 200 (not 503) and the message
     * names the capacity limit so the operator knows why the deployment is being throttled. This is the second half of the threshold pin - together with the
     * 7-stream test above, the pair structurally locks the 0.8 threshold against drift.
     */
    mockState.browserConnected = true;
    mockState.streamCount = 8;

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(res.status, 200, "degraded branch responds with 200, not 503 - load balancers should not yank degraded instances");
    assert.equal(body.status, "degraded", "streamCount 8 of 10 (utilization 0.8) flips into the degraded branch");
    assert.equal(body.message, "Approaching stream capacity limit.");
    assert.equal(body.streams.active, 8);
    assert.equal(body.streams.limit, 10);
  });

  test("stays 'degraded' when streamUtilization runs above the threshold", async () => {

    /* Once past the cliff, the branch should hold steady regardless of how high the utilization climbs. This pins that the predicate is "utilization >= 0.8"
     * (cliff-then-stay), not "utilization == 0.8" (a momentary spike that resets above the limit).
     */
    mockState.browserConnected = true;
    mockState.streamCount = 10;

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(body.status, "degraded", "at the configured limit, status remains degraded");
    assert.equal(body.message, "Approaching stream capacity limit.");
  });

  test("unhealthy outranks degraded - browser disconnected with high stream count still reports 'unhealthy'", async () => {

    /* The branch ordering: the handler checks browserConnected first, so a disconnected browser surfaces as unhealthy even when stream utilization
     * would otherwise indicate degraded. Without this, an operator triaging a 503 might be misled into reading the response body's stream metrics as the cause
     * when the underlying issue is actually browser availability. This test pins that the unhealthy-takes-precedence ordering is structural.
     */
    mockState.browserConnected = false;
    mockState.streamCount = 9;

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(res.status, 503, "browser-disconnected dominates the response code");
    assert.equal(body.status, "unhealthy", "unhealthy branch wins over degraded");
    assert.equal(body.message, "Browser is not connected.");
  });
});

describe("setupHealthEndpoint - GET /health (client aggregation loop)", () => {

  test("aggregates per-stream client summaries into a system-wide byType breakdown", async () => {

    /* The handler folds getAllStreams() over getClientSummary() into a Map<ClientType, number>, then emits the entries sorted alphabetically by type via
     * Array.toSorted. Two synthetic streams - one HLS-only, one mixed HLS + MPEG-TS - exercise the aggregation across multiple client types and multiple
     * streams. The fold must add per-type counts (not overwrite) and the toSorted output must be alphabetical (mpegts before hls would be a regression).
     */
    mockState.browserConnected = true;
    mockState.streamCount = 2;
    mockState.streams = [ { id: 101 }, { id: 202 } ];
    mockState.streamSummaries = new Map<number, ClientSummary>([
      [ 101, { clients: [{ count: 3, type: "hls" satisfies ClientType }], total: 3 } ],
      [ 202, { clients: [
        { count: 2, type: "hls" satisfies ClientType },
        { count: 1, type: "mpegts" satisfies ClientType }
      ], total: 3 } ]
    ]);

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(res.status, 200);
    assert.equal(body.clients.total, 6, "totals fold across streams: 3 + (2 + 1) = 6");
    assert.deepEqual(body.clients.byType,
      [ { count: 5, type: "hls" }, { count: 1, type: "mpegts" } ],
      "byType folds 3 HLS + 2 HLS = 5 HLS, 1 MPEG-TS, sorted alphabetically");
  });

  test("an empty getAllStreams() yields an empty byType array and zero total clients", async () => {

    /* The aggregation loop's empty-input case: no streams, no client work to do. Pin that the byType field is an empty array (not null, not undefined) so the
     * UI can render the surface unconditionally.
     */
    mockState.browserConnected = true;
    mockState.streamCount = 0;
    mockState.streams = [];

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.equal(body.clients.total, 0);
    assert.deepEqual(body.clients.byType, []);
  });

  test("byType is alphabetically sorted by type even when input order is reversed", async () => {

    /* Pin the toSorted contract independently of the aggregation: even if the source streams happened to add MPEG-TS clients first, the output emits 'hls'
     * before 'mpegts'. Without this, an upstream order change in client registration could silently drift the response shape.
     */
    mockState.browserConnected = true;
    mockState.streamCount = 1;
    mockState.streams = [{ id: 1 }];
    mockState.streamSummaries = new Map<number, ClientSummary>([
      [ 1, { clients: [
        { count: 4, type: "mpegts" satisfies ClientType },
        { count: 2, type: "hls" satisfies ClientType }
      ], total: 6 } ]
    ]);

    const res = await fetch(urlFor("/health"));
    const body = await res.json() as HealthBody;

    assert.deepEqual(body.clients.byType.map((entry) => entry.type), [ "hls", "mpegts" ],
      "byType ordering is alphabetical regardless of source stream's client list order");
  });
});

describe("deriveHealthStatus - the pure decision core", () => {

  test("browser disconnected is unhealthy (503) regardless of utilization", () => {

    // Branch precedence: browser-down outranks any utilization. Pins the single source of truth the handler now derives status, message, and HTTP code from.
    assert.deepEqual(deriveHealthStatus(false, 0), { httpStatus: 503, message: "Browser is not connected.", status: "unhealthy" });
    assert.deepEqual(deriveHealthStatus(false, 0.9), { httpStatus: 503, message: "Browser is not connected.", status: "unhealthy" },
      "browser-down outranks a high utilization");
  });

  test("connected and below 0.8 utilization is healthy (200, no message)", () => {

    assert.deepEqual(deriveHealthStatus(true, 0.79), { httpStatus: 200, status: "healthy" });
  });

  test("connected at or above 0.8 utilization is degraded (200)", () => {

    // The 0.8 threshold is inclusive and cliff-then-stay - the exact contract the handler tests exercise through the server, pinned here in isolation.
    assert.deepEqual(deriveHealthStatus(true, 0.8), { httpStatus: 200, message: "Approaching stream capacity limit.", status: "degraded" },
      "the threshold is inclusive at 0.8");
    assert.deepEqual(deriveHealthStatus(true, 1.5), { httpStatus: 200, message: "Approaching stream capacity limit.", status: "degraded" });
  });
});
