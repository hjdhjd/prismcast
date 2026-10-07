/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * showInfo.test.ts: Unit tests for the show name and channel logo subsystem. showInfo.ts integrates with the Channels DVR API to discover the active DVR host,
 * fetch active recording jobs and program guide entries, and populate channel logos in two tiers. The module exposes a small public API (getDvrHost, setDvrHost,
 * getShowName, clearShowName, triggerShowNameUpdate, fetchFromDvr, getDeviceMappings, matchesM3uDevice, updateChannelLogo) plus the start/stop polling
 * lifecycle. Tests focus on the pure helpers (getShowName/clearShowName, getDvrHost/setDvrHost, fetchFromDvr success/timeout paths, matchesM3uDevice's overlap
 * boundaries) and avoid the polling start/stop which spawns intervals. The DVR host is the running configuration's, so the host rows seed and reset it by
 * re-initializing CONFIG from an empty in-memory store; the rows covering a save that changes the host or the port run against the real store in
 * test/e2e/streaming/show-info.test.ts.
 */
import { CONFIG, initializeConfiguration } from "../config/index.ts";
import { TestClock, settle } from "homebridge-plugin-utils/testing";
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { clearShowName, fetchFromDvr, getDvrHost, getShowName, matchesM3uDevice, setDvrHost } from "./showInfo.ts";
import { closePuppeteerStreamWssOnIdle, pendingBodyFetch } from "../testing.helpers.ts";
import type { ConfigStore } from "../config/index.ts";
import { LOG } from "../utils/index.ts";
import assert from "node:assert/strict";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

// The DVR request's own window, mirrored from showInfo.ts so a row advances exactly the bound the module arms.
const API_TIMEOUT_MS = 5000;

// The macrotask boundaries a drain crosses so a request chain the module fired without awaiting has run to completion before the next row starts.
const SETTLE_TURNS = 10;

// The Channels DVR port the defaults carry, which the running configuration holds in every row that does not change it.
const DEFAULT_DVR_PORT = 8089;

// An empty configuration file held in memory. Initializing from it resets CONFIG, the DVR host among it, to the defaults, and it writes nothing anywhere.
const emptyStore: ConfigStore = {

  mutateConfig: async (): Promise<void> => {

    // Intentional no-op: no row here saves through the configuration layer.
  },
  readConfig: async () => ({ config: {}, parseError: false, readError: false })
};

/* Counts the debug lines carrying the module's fetch-failure template for one host - the line a lapse must not produce and a real failure must. The host narrows
 * the count to the calling row's own request, because the spy sits on a logger every row in the file shares.
 */
function failureLines(spy: ReturnType<typeof mock.method>, host: string): number {

  return spy.mock.calls.filter((call) => String(call.arguments[1]).startsWith("Failed to fetch") && (call.arguments[3] === host)).length;
}

describe("getShowName / clearShowName", () => {

  test("returns the empty string for unknown stream IDs", () => {

    // The cache is a private Map keyed by stream ID. A never-set ID must surface as the empty string, not undefined - this is the contract the SSE status emitter
    // depends on for falling back to the empty string in StreamStatus.
    assert.equal(getShowName(999999), "");
  });

  test("clear is a no-op for an unknown ID", () => {

    // Negative test: cleanup paths in lifecycle.terminateStream call clearShowName for every stream regardless of whether a name was ever cached.
    assert.doesNotThrow(() => {

      clearShowName(999999);
    });
  });
});

describe("getDvrHost / setDvrHost", () => {

  let originalFetch: typeof globalThis.fetch;

  /* Setting the host fires the logo population without awaiting it, and its device-mapping request would otherwise reach the sentinel host after the row has
   * finished and log its failure during a later row. A stub answering every request with an empty listing lets that population settle, silently, inside the
   * row that started it; the drain before the restore is what keeps a late request from reaching the real fetch.
   */
  // Each row starts from the defaults, so the running configuration holds no DVR host until the row sets one.
  beforeEach(async () => {

    originalFetch = globalThis.fetch;
    globalThis.fetch = (async (): Promise<Response> => new Response("[]", { status: 200 }));
    await initializeConfiguration(undefined, emptyStore);
  });

  afterEach(async () => {

    await settle(SETTLE_TURNS);
    globalThis.fetch = originalFetch;
  });

  test("getDvrHost answers the host the running configuration holds, and null while it holds none", () => {

    CONFIG.channelsDvr.host = "test-host-0.example.invalid";

    assert.equal(getDvrHost(), "test-host-0.example.invalid", "the running configuration's host is the DVR host");

    CONFIG.channelsDvr.host = "";

    assert.equal(getDvrHost(), null, "an empty host means no DVR host is known");
  });

  test("setDvrHost writes the host into the running configuration so getDvrHost surfaces it", () => {

    // Use a sentinel host that won't collide with real hostnames. setDvrHost also persists the host and populates the logos, neither of which this row reads:
    // the persist has no data directory to write to here and its failure is logged, and the stub above answers the population.
    setDvrHost("test-host-1.example.invalid");

    assert.equal(CONFIG.channelsDvr.host, "test-host-1.example.invalid", "the running configuration holds the host at once");
    assert.equal(getDvrHost(), "test-host-1.example.invalid");
  });

  test("setDvrHost is safe to call twice with the same value - it does not change behavior", () => {

    setDvrHost("test-host-2.example.invalid");
    setDvrHost("test-host-2.example.invalid");

    assert.equal(getDvrHost(), "test-host-2.example.invalid");
  });

  test("setDvrHost rejects colon-bearing inputs so post-migration drift cannot reintroduce host:port at runtime", () => {

    /* The runtime safety net for the v3 migration's architectural cleanup. The schema migration splits any legacy host:port value at read time, but only on
     * disk - a future caller mistakenly passing "1.2.3.4:8089" through setDvrHost would silently reintroduce the embedded-port form into module state and (via
     * persist) onto disk. The colon-rejection in setDvrHost prevents that drift: colon-bearing inputs are dropped (with a debug log; not asserted here because
     * the observable contract is the absent state change). This test asserts both halves of the contract - a valid host updates state, a colon-bearing host does
     * not - so a refactor that loosens the rejection (e.g., to "strip the port portion") would fail loudly instead of silently undoing the migration's intent.
     */
    setDvrHost("1.2.3.4");

    assert.equal(getDvrHost(), "1.2.3.4", "a host-only input updates the running host");

    setDvrHost("1.2.3.4:8089");

    assert.equal(getDvrHost(), "1.2.3.4", "a colon-bearing input leaves the running host unchanged - the prior accepted value is preserved");
    assert.equal(CONFIG.channelsDvr.host, "1.2.3.4", "and the running configuration with it");
  });
});

describe("fetchFromDvr", () => {

  let originalFetch: typeof globalThis.fetch;

  beforeEach(() => {

    originalFetch = globalThis.fetch;
  });

  afterEach(() => {

    globalThis.fetch = originalFetch;
    mock.reset();
  });

  test("returns the parsed JSON array on a 200 response", async () => {

    // Happy path: fetch resolves with status 200 and a JSON array body. The function returns the parsed array verbatim.
    globalThis.fetch = (async (): Promise<Response> => new Response(JSON.stringify([ { Name: "Show A" }, { Name: "Show B" } ]), { status: 200 }));

    const result = await fetchFromDvr<{ Name: string }>("dvr.example.invalid", DEFAULT_DVR_PORT, "/dvr/jobs");

    assert.equal(result.length, 2);
    assert.equal(result[0]?.Name, "Show A");
    assert.equal(result[1]?.Name, "Show B");
  });

  test("returns an empty array on a non-OK status (4xx, 5xx)", async () => {

    // Negative test: the function silently swallows non-OK status codes and surfaces an empty array. This is the failure-graceful contract - show name lookup
    // never breaks streaming.
    globalThis.fetch = (async (): Promise<Response> => new Response("Not found", { status: 404 }));

    const result = await fetchFromDvr<unknown>("dvr.example.invalid", DEFAULT_DVR_PORT, "/dvr/jobs");

    assert.deepEqual(result, []);
  });

  test("returns an empty array on a 500 status", async () => {

    globalThis.fetch = (async (): Promise<Response> => new Response("server error", { status: 500 }));

    const result = await fetchFromDvr<unknown>("dvr.example.invalid", DEFAULT_DVR_PORT, "/anything");

    assert.deepEqual(result, []);
  });

  test("returns an empty array when fetch throws (network error)", async () => {

    // Negative test: any thrown error from fetch (network down, DNS failure, etc.) is caught and surfaced as an empty array.
    const debug = mock.method(LOG, "debug", () => { /* Captured via the mock. */ });

    globalThis.fetch = (async (): Promise<Response> => {

      throw new Error("Network unreachable");
    });

    const result = await fetchFromDvr<unknown>("dvr.example.invalid", DEFAULT_DVR_PORT, "/anything");

    assert.deepEqual(result, []);

    // The other half of the silence contract: a failure that is not this fetch's own lapse still reports itself, so the silence below is selective rather than total.
    assert.equal(failureLines(debug, "dvr.example.invalid"), 1, "a real failure logs exactly one line");
  });

  test("stays silent and returns an empty array when its own bound lapses", async () => {

    /* An unreachable DVR is the ordinary case here, so a lapse of this fetch's own bound has to leave no line behind while every other failure still reports
     * itself. The bound carries the module's own error as its abort reason, which is what lets the catch tell the two apart by reference. The stub errors its
     * body only on the request signal's abort, so the bound also has to span the json() read for this row to end at all.
     */
    const clock = new TestClock();
    const debug = mock.method(LOG, "debug", () => { /* Captured via the mock. */ });

    mock.method(globalThis, "fetch", async (_url: string | URL, init?: RequestInit): Promise<Response> => pendingBodyFetch(init));

    const resultPromise = fetchFromDvr<unknown>("dvr.example.invalid", DEFAULT_DVR_PORT, "/anything", clock);

    assert.equal(clock.pending, 1, "the request's bound is armed on the clock it was handed");
    assert.deepEqual(clock.requested, [API_TIMEOUT_MS], "and it waits the DVR request's own window");

    // Let the headers land and the body read begin, so the advance below lapses a bound that is spanning an open body rather than an unstarted request.
    await settle();

    clock.advance(API_TIMEOUT_MS);

    assert.deepEqual(await resultPromise, [], "the lapsed bound abandons the open body and the call surfaces an empty array");
    assert.equal(clock.pending, 0, "and the bound was cancelled rather than left armed");
    assert.equal(failureLines(debug, "dvr.example.invalid"), 0, "the fetch's own lapse leaves no failure line behind");
  });

  test("constructs the URL from the host and the port it is handed, whatever port the running configuration holds", async () => {

    // The caller hands the port in, so a request made while a save is reconciled reaches the port the candidate names. The running configuration holds the
    // default port throughout, which is the negative control: a URL built from CONFIG would carry 8089.
    let observedUrl = "";

    globalThis.fetch = (async (input: Request | URL | string): Promise<Response> => {

      // The implementation passes a plain URL string, but fetch's type union admits Request and URL too. We narrow explicitly so eslint's no-base-to-string rule
      // does not fire on a record.toString() path that fetchFromDvr never exercises.
      observedUrl = (typeof input === "string") ? input : (input instanceof URL ? input.toString() : input.url);

      return new Response("[]", { status: 200 });
    });

    assert.equal(CONFIG.channelsDvr.port, DEFAULT_DVR_PORT, "precondition: the running configuration holds the default port");

    await fetchFromDvr<unknown>("192.168.1.99", 19191, "/devices");

    assert.equal(observedUrl, "http://192.168.1.99:19191/devices");
  });

  test("sends the Accept: application/json header", async () => {

    let observedHeaders: Headers | undefined;

    globalThis.fetch = (async (_input: Request | URL | string, init?: RequestInit): Promise<Response> => {

      observedHeaders = new Headers(init?.headers);

      return new Response("[]", { status: 200 });
    });

    await fetchFromDvr<unknown>("dvr.example.invalid", DEFAULT_DVR_PORT, "/dvr/jobs");

    assert.equal(observedHeaders?.get("accept"), "application/json");
  });
});

describe("matchesM3uDevice", () => {

  test("matches at exactly 80% overlap", () => {

    // Device and prismcast sets are both size 5, sharing 4 entries (a, b, c, d). maxSize is 5, so overlapRatio is exactly 0.8. The accept test is
    // `!(overlapRatio < 0.8)`, which must accept the boundary value itself - a regression to `overlapRatio >= 0.8` would also pass this case, but a regression
    // that rounds or truncates the ratio before comparing would not.
    const deviceChannelIds = new Set([ "a", "b", "c", "d", "f" ]);
    const prismcastChannelKeys = new Set([ "a", "b", "c", "d", "e" ]);

    const overlap = matchesM3uDevice(deviceChannelIds, prismcastChannelKeys);

    assert.equal(overlap.overlapCount, 4);
    assert.equal(overlap.maxSize, 5);
    assert.equal(overlap.overlapRatio, 0.8);
    assert.equal(overlap.matches, true);
  });

  test("does not match just below 80% overlap", () => {

    // Negative path: the passing gate is exercised elsewhere (getDeviceMappings' own integration coverage); this test locks the reject branch.
    // 3 of 5 keys overlap (a, b, c), giving overlapRatio 0.6 - well under the 0.8 threshold.
    const deviceChannelIds = new Set([ "a", "b", "c" ]);
    const prismcastChannelKeys = new Set([ "a", "b", "c", "d", "e" ]);

    const overlap = matchesM3uDevice(deviceChannelIds, prismcastChannelKeys);

    assert.equal(overlap.overlapCount, 3);
    assert.equal(overlap.maxSize, 5);
    assert.equal(overlap.overlapRatio, 0.6);
    assert.equal(overlap.matches, false);
  });

  test("matches on full overlap with ratio 1", () => {

    // Identical sets: every key overlaps, and neither set has an extra entry to dilute the ratio.
    const deviceChannelIds = new Set([ "a", "b", "c" ]);
    const prismcastChannelKeys = new Set([ "a", "b", "c" ]);

    const overlap = matchesM3uDevice(deviceChannelIds, prismcastChannelKeys);

    assert.equal(overlap.overlapCount, 3);
    assert.equal(overlap.maxSize, 3);
    assert.equal(overlap.overlapRatio, 1);
    assert.equal(overlap.matches, true);
  });

  test("a superset device dilutes the ratio via the larger maxSize denominator", () => {

    // Every prismcast key (a, b, c, d) is found in the device, so a denominator drawn from the smaller (prismcast) set would wrongly report 100% overlap.
    // The device carries two extra channels (e, f) that PrismCast does not have, so maxSize must be drawn from the larger device set (6), yielding
    // overlapRatio 4/6 - below the 0.8 threshold. This asserts Math.max(deviceChannelIds.size, prismcastChannelKeys.size) as the denominator rather than either
    // set's size alone.
    const deviceChannelIds = new Set([ "a", "b", "c", "d", "e", "f" ]);
    const prismcastChannelKeys = new Set([ "a", "b", "c", "d" ]);

    const overlap = matchesM3uDevice(deviceChannelIds, prismcastChannelKeys);

    assert.equal(overlap.overlapCount, 4);
    assert.equal(overlap.maxSize, 6);
    assert.ok(overlap.overlapRatio < 0.8);
    assert.equal(overlap.matches, false);
  });

  test("both-empty sets yield a NaN ratio that is treated as a match", () => {

    // Edge case: 0 overlapping keys divided by a maxSize of 0 is NaN in JavaScript. `!(NaN < 0.8)` evaluates to true, so the function accepts this case -
    // this is why the implementation is written as a negated less-than rather than `overlapRatio >= 0.8` (`NaN >= 0.8` is false, which would reject). In
    // practice getDeviceMappings never reaches this function with an empty deviceChannelIds, because it continues past any device whose Channels list is
    // empty before calling matchesM3uDevice - this test documents the predicate's own boundary behavior in isolation.
    const deviceChannelIds = new Set<string>();
    const prismcastChannelKeys = new Set<string>();

    const overlap = matchesM3uDevice(deviceChannelIds, prismcastChannelKeys);

    assert.equal(overlap.overlapCount, 0);
    assert.equal(overlap.maxSize, 0);
    assert.ok(Number.isNaN(overlap.overlapRatio));
    assert.equal(overlap.matches, true);
  });
});
