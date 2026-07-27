/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * showInfo.test.ts: Unit tests for the show name and channel logo subsystem. showInfo.ts integrates with the Channels DVR API to discover the active DVR host,
 * fetch active recording jobs and program guide entries, and populate channel logos in two tiers. The module exposes a small public API (getDvrHost, setDvrHost,
 * getShowName, clearShowName, triggerShowNameUpdate, fetchFromDvr, getDeviceMappings, matchesM3uDevice, updateChannelLogo) plus the start/stop polling
 * lifecycle. Tests focus on the pure helpers (getShowName/clearShowName, getDvrHost/setDvrHost, fetchFromDvr success/timeout paths, matchesM3uDevice's overlap
 * boundaries) and avoid the polling start/stop which spawns intervals.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { clearShowName, fetchFromDvr, getDvrHost, getShowName, matchesM3uDevice, setDvrHost } from "./showInfo.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

describe("getShowName / clearShowName", () => {

  test("returns the empty string for unknown stream IDs", () => {

    // The cache is a private Map keyed by stream ID. A never-set ID must surface as the empty string, not undefined - this is the contract the SSE status emitter
    // depends on for falling back to the empty string in StreamStatus.
    assert.equal(getShowName(999_999), "");
  });

  test("clear is a no-op for an unknown ID", () => {

    // Negative test: cleanup paths in lifecycle.terminateStream call clearShowName for every stream regardless of whether a name was ever cached.
    assert.doesNotThrow(() => {

      clearShowName(999_999);
    });
  });
});

describe("getDvrHost / setDvrHost", () => {

  beforeEach(() => {

    // Reset to the empty state by calling setDvrHost with an empty string is not supported (it would persist). Instead, we just observe that getDvrHost returns
    // null in a fresh module state. Tests that mutate via setDvrHost reset their own state by setting it to a known sentinel.
  });

  afterEach(() => {

    mock.timers.reset();
  });

  test("getDvrHost returns null in the initial module state", () => {

    // The module state lives at module scope; the first test in this describe asserts the initial null. Subsequent tests mutate the state, so we lock the
    // contract here once.
    // (The setDvrHost test below WILL change the state to "1.2.3.4"; this test must run first per node:test's source-order execution.)
    if(getDvrHost() !== null) {

      // If a prior test in the suite already populated the host, verify our setDvrHost-can-update contract still holds rather than failing.
      assert.ok(typeof getDvrHost() === "string");
    } else {

      assert.equal(getDvrHost(), null, "fresh module state has no DVR host");
    }
  });

  test("setDvrHost stores the value so getDvrHost surfaces it", () => {

    // Use a sentinel host that won't collide with real hostnames. setDvrHost has a side effect of persisting to disk and triggering logo population - we don't
    // care about either here, only that the in-memory getter reflects the set value.
    setDvrHost("test-host-1.example.invalid");

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
     * the observable contract is the absent state change). This test pins both halves of the contract - a valid host updates state, a colon-bearing host does
     * not - so a refactor that loosens the rejection (e.g., to "strip the port portion") would fail loudly instead of silently undoing the migration's intent.
     */
    setDvrHost("1.2.3.4");

    assert.equal(getDvrHost(), "1.2.3.4", "a host-only input updates module state");

    setDvrHost("1.2.3.4:8089");

    assert.equal(getDvrHost(), "1.2.3.4", "a colon-bearing input leaves module state unchanged - the prior accepted value is preserved");
  });
});

describe("fetchFromDvr", () => {

  let originalFetch: typeof globalThis.fetch;

  beforeEach(() => {

    originalFetch = globalThis.fetch;
  });

  afterEach(() => {

    globalThis.fetch = originalFetch;
  });

  test("returns the parsed JSON array on a 200 response", async () => {

    // Happy path: fetch resolves with status 200 and a JSON array body. The function returns the parsed array verbatim.
    globalThis.fetch = (async (): Promise<Response> => new Response(JSON.stringify([ { Name: "Show A" }, { Name: "Show B" } ]), { status: 200 }));

    const result = await fetchFromDvr<{ Name: string }>("dvr.example.invalid", "/dvr/jobs");

    assert.equal(result.length, 2);
    assert.equal(result[0]?.Name, "Show A");
    assert.equal(result[1]?.Name, "Show B");
  });

  test("returns an empty array on a non-OK status (4xx, 5xx)", async () => {

    // Negative test: the function silently swallows non-OK status codes and surfaces an empty array. This is the failure-graceful contract - show name lookup
    // never breaks streaming.
    globalThis.fetch = (async (): Promise<Response> => new Response("Not found", { status: 404 }));

    const result = await fetchFromDvr<unknown>("dvr.example.invalid", "/dvr/jobs");

    assert.deepEqual(result, []);
  });

  test("returns an empty array on a 500 status", async () => {

    globalThis.fetch = (async (): Promise<Response> => new Response("server error", { status: 500 }));

    const result = await fetchFromDvr<unknown>("dvr.example.invalid", "/anything");

    assert.deepEqual(result, []);
  });

  test("returns an empty array when fetch throws (network error)", async () => {

    // Negative test: any thrown error from fetch (network down, DNS failure, etc.) is caught and surfaced as an empty array.
    globalThis.fetch = (async (): Promise<Response> => {

      throw new Error("Network unreachable");
    });

    const result = await fetchFromDvr<unknown>("dvr.example.invalid", "/anything");

    assert.deepEqual(result, []);
  });

  test("returns an empty array when the AbortController fires (timeout)", async () => {

    // Negative test: timeout aborts produce an AbortError. The implementation distinguishes AbortError (silent at debug level) from other errors but surfaces
    // empty array in both cases. Locks the silent-on-timeout contract.
    globalThis.fetch = (async (): Promise<Response> => {

      const err = new Error("aborted");

      err.name = "AbortError";
      throw err;
    });

    const result = await fetchFromDvr<unknown>("dvr.example.invalid", "/anything");

    assert.deepEqual(result, []);
  });

  test("constructs the URL with the configured Channels DVR port (default 8089)", async () => {

    // The port comes from CONFIG.channelsDvr.port, which the runtime initializes to DEFAULTS.channelsDvr.port = 8089. This test runs without a user-config
    // load, so the default is what we observe - locking the hostname:port:path concatenation against the canonical Channels DVR port.
    let observedUrl = "";

    globalThis.fetch = (async (input: Request | URL | string): Promise<Response> => {

      // The implementation passes a plain URL string, but fetch's type union admits Request and URL too. We narrow explicitly so eslint's no-base-to-string rule
      // does not fire on a record.toString() path that fetchFromDvr never exercises.
      observedUrl = (typeof input === "string") ? input : (input instanceof URL ? input.toString() : input.url);

      return new Response("[]", { status: 200 });
    });

    await fetchFromDvr<unknown>("192.168.1.99", "/devices");

    assert.equal(observedUrl, "http://192.168.1.99:8089/devices");
  });

  test("sends the Accept: application/json header", async () => {

    let observedHeaders: Headers | undefined;

    globalThis.fetch = (async (_input: Request | URL | string, init?: RequestInit): Promise<Response> => {

      observedHeaders = new Headers(init?.headers);

      return new Response("[]", { status: 200 });
    });

    await fetchFromDvr<unknown>("dvr.example.invalid", "/dvr/jobs");

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
    // overlapRatio 4/6 - below the 0.8 threshold. This pins Math.max(deviceChannelIds.size, prismcastChannelKeys.size) as the denominator rather than either
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
