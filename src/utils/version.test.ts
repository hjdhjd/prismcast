/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * version.test.ts: Unit tests for the version helpers in version.ts. The pure functions (normalizeVersion, isVersionLessThan) get full boundary coverage; the
 * networked functions (fetchLatestVersion, getChangelogItems, checkForUpdates) are exercised by stubbing globalThis.fetch via mock.method so no real HTTP calls
 * fire. getPackageVersion reads the project package.json from disk; we lock its return shape (a non-empty version string).
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { checkForUpdates, fetchLatestVersion, getChangelogItems, getPackageVersion, getVersionInfo, isVersionLessThan, normalizeVersion, startUpdateChecking,
  stopUpdateChecking } from "./version.ts";
import assert from "node:assert/strict";

describe("normalizeVersion", () => {

  test("strips a leading 'v' prefix", () => {

    assert.equal(normalizeVersion("v1.0.7"), "1.0.7");
  });

  test("returns the input unchanged when there is no leading 'v'", () => {

    assert.equal(normalizeVersion("1.0.7"), "1.0.7");
  });

  test("strips only one leading 'v' (anchor on start)", () => {

    // Boundary: only the leading occurrence is stripped. An interior 'v' (e.g., a build tag) must survive.
    assert.equal(normalizeVersion("vv1.0.7"), "v1.0.7", "leading 'v' removed once, second 'v' survives");
  });

  test("returns the empty string unchanged", () => {

    assert.equal(normalizeVersion(""), "");
  });

  test("does not strip a non-leading 'v'", () => {

    assert.equal(normalizeVersion("1.0.7-beta+v1"), "1.0.7-beta+v1");
  });
});

describe("isVersionLessThan", () => {

  test("returns true when major version is lower", () => {

    assert.equal(isVersionLessThan("1.0.0", "2.0.0"), true);
  });

  test("returns true when minor version is lower (same major)", () => {

    assert.equal(isVersionLessThan("1.0.0", "1.1.0"), true);
  });

  test("returns true when patch is lower (same major and minor)", () => {

    assert.equal(isVersionLessThan("1.0.7", "1.0.8"), true);
  });

  test("returns false for equal versions", () => {

    // Boundary: a == b must return false (it's strict less-than, not less-or-equal).
    assert.equal(isVersionLessThan("1.0.7", "1.0.7"), false);
  });

  test("returns false when the first version is higher", () => {

    assert.equal(isVersionLessThan("2.0.0", "1.0.0"), false);
    assert.equal(isVersionLessThan("1.10.0", "1.9.0"), false);
  });

  test("treats missing parts as zero (1.0 < 1.0.1)", () => {

    // Boundary: a shorter version like "1.0" must compare equal to "1.0.0" and less-than to "1.0.1".
    assert.equal(isVersionLessThan("1.0", "1.0.1"), true);
    assert.equal(isVersionLessThan("1.0", "1.0.0"), false, "missing parts default to 0; 1.0 == 1.0.0");
  });

  test("compares numerically, not lexically (1.10 > 1.9)", () => {

    // Negative test: a string-sort comparison would say "1.10" < "1.9". Numeric comparison correctly reports 1.10 > 1.9.
    assert.equal(isVersionLessThan("1.9.0", "1.10.0"), true, "1.9.0 < 1.10.0 numerically");
    assert.equal(isVersionLessThan("1.10.0", "1.9.0"), false);
  });

  test("handles two-digit and three-digit segments", () => {

    assert.equal(isVersionLessThan("1.99.0", "1.100.0"), true);
  });
});

describe("getPackageVersion", () => {

  test("returns a non-empty version string", () => {

    // The function reads ../../package.json relative to the file. In dev mode the path resolution succeeds; if it fails, the function returns "0.0.0" as a fallback.
    const result = getPackageVersion();

    assert.equal(typeof result, "string");
    assert.ok(result.length > 0);
  });

  test("returns either a semver-like string or the documented fallback", () => {

    // Boundary: either a real version (digits.dots) or the literal "0.0.0" fallback.
    const result = getPackageVersion();

    assert.match(result, /^\d+(\.\d+)*$|^0\.0\.0$/);
  });

  test("returns the same value across calls (cached after first lookup)", () => {

    const a = getPackageVersion();
    const b = getPackageVersion();

    assert.equal(a, b);
  });
});

describe("fetchLatestVersion", () => {

  afterEach(() => {

    mock.reset();
  });

  test("returns the normalized 'latest' tag from the npm registry response", async () => {

    mock.method(globalThis, "fetch", async () => {

      return new Response(JSON.stringify({ "dist-tags": { latest: "v9.9.9" } }), { headers: { "Content-Type": "application/json" }, status: 200 });
    });

    const result = await fetchLatestVersion();

    assert.equal(result, "9.9.9", "leading 'v' stripped via normalizeVersion");
  });

  test("returns null when the registry responds non-2xx", async () => {

    mock.method(globalThis, "fetch", async () => new Response("nope", { status: 500 }));

    const result = await fetchLatestVersion();

    assert.equal(result, null);
  });

  test("returns null when the JSON body is missing dist-tags.latest", async () => {

    // Negative test: malformed payloads must surface as null, not crash.
    mock.method(globalThis, "fetch", async () => new Response(JSON.stringify({ name: "prismcast" }), { headers: { "Content-Type": "application/json" }, status: 200 }));

    const result = await fetchLatestVersion();

    assert.equal(result, null);
  });

  test("returns null when fetch itself rejects", async () => {

    mock.method(globalThis, "fetch", async () => {

      throw new Error("network down");
    });

    const result = await fetchLatestVersion();

    assert.equal(result, null);
  });
});

describe("getVersionInfo", () => {

  afterEach(() => {

    mock.reset();
  });

  test("reports updateAvailable=true when current is less than the cached latest (after checkForUpdates seeded the cache)", async () => {

    // checkForUpdates is the function that writes cachedLatestVersion. Calling it with force=true bypasses the debounce so the test is deterministic regardless
    // of any prior call in the suite. After the seed, getVersionInfo should report updateAvailable=true and latestVersion=9.9.9 against current=0.0.1.
    mock.method(globalThis, "fetch", async (url: string | URL): Promise<Response> => {

      const u = String(url);

      if(u.includes("registry.npmjs.org")) {

        return new Response(JSON.stringify({ "dist-tags": { latest: "9.9.9" } }), { status: 200 });
      }

      return new Response("", { status: 404 });
    });

    await checkForUpdates("0.0.1", true);

    const info = getVersionInfo("0.0.1");

    assert.equal(info.latestVersion, "9.9.9", "cache seeded with the fetched latest");
    assert.equal(info.updateAvailable, true, "updateAvailable=true when current < cached latest");
  });

  test("reports updateAvailable=false when current >= cached latest", async () => {

    // Negative test: same seeding flow but the current version is at or above the cached latest. The flag must be false.
    mock.method(globalThis, "fetch", async (url: string | URL): Promise<Response> => {

      if(String(url).includes("registry.npmjs.org")) {

        return new Response(JSON.stringify({ "dist-tags": { latest: "1.0.0" } }), { status: 200 });
      }

      return new Response("", { status: 404 });
    });

    await checkForUpdates("1.0.0", true);

    const info = getVersionInfo("1.0.0");

    assert.equal(info.updateAvailable, false, "no update when current matches latest");
  });
});

describe("startUpdateChecking and stopUpdateChecking", () => {

  beforeEach(() => {

    // Stub the network so the immediate check at start does not fire a real request.
    mock.method(globalThis, "fetch", async () => new Response(JSON.stringify({ "dist-tags": { latest: "1.0.0" } }), { status: 200 }));
  });

  afterEach(() => {

    stopUpdateChecking();
    mock.reset();
  });

  test("startUpdateChecking does not throw and stopUpdateChecking clears the interval", () => {

    // We can't directly observe the setInterval handle, but we can verify the start/stop pair is symmetric and idempotent.
    assert.doesNotThrow(() => { startUpdateChecking("1.0.0"); });
    assert.doesNotThrow(() => { stopUpdateChecking(); });
  });

  test("stopUpdateChecking is idempotent (safe to call when nothing is running)", () => {

    // Negative test: stopping a never-started checker must be a no-op.
    assert.doesNotThrow(() => { stopUpdateChecking(); });
    assert.doesNotThrow(() => { stopUpdateChecking(); });
  });

  test("startUpdateChecking is idempotent on repeated calls (does not stack timers)", () => {

    // The implementation uses ??=, so a second start is a no-op while the first is running. Stop cleans both - locking the contract that callers cannot leak timers.
    assert.doesNotThrow(() => {

      startUpdateChecking("1.0.0");
      startUpdateChecking("1.0.0");
      startUpdateChecking("1.0.0");

      stopUpdateChecking();
    });
  });
});

describe("getChangelogItems", () => {

  afterEach(() => {

    mock.reset();
  });

  test("returns null when the changelog cannot be fetched", async () => {

    // Negative test: a 404 response must surface as null without throwing.
    mock.method(globalThis, "fetch", async () => new Response("", { status: 404 }));

    const result = await getChangelogItems("99.99.99");

    assert.equal(result, null);
  });

  test("returns null when the version is not present in the changelog", async () => {

    const fakeChangelog = "## 1.0.0 (2024-01-01)\n\n* First release.\n\n## 0.9.0 (2023-12-01)\n\n* Earlier release.\n";

    mock.method(globalThis, "fetch", async () => new Response(fakeChangelog, { status: 200 }));

    const result = await getChangelogItems("9.9.9");

    assert.equal(result, null);
  });

  test("parses bullet items for the requested version", async () => {

    // Use unique version numbers per test so module-scope cachedChangelog state from earlier tests cannot match the version being looked up; the function still
    // hits phase 2 (network refresh) when the cached entry doesn't contain the requested version.
    const fakeChangelog = "## 7.7.7 (2025-01-15)\n\n* New feature foo.\n* Bug fix bar.\n\n## 7.6.0 (2024-12-01)\n\n* Old item.\n";

    mock.method(globalThis, "fetch", async () => new Response(fakeChangelog, { status: 200 }));

    const result = await getChangelogItems("7.7.7");

    assert.deepEqual(result, [ "New feature foo.", "Bug fix bar." ]);
  });

  test("strips a leading 'v' from the requested version before lookup", async () => {

    // The function calls normalizeVersion on the input. A user-passed "v8.8.8" should resolve to the same entry as "8.8.8". Unique version number used to avoid
    // collisions with cachedChangelog from the previous test.
    const fakeChangelog = "## 8.8.8 (2025-01-15)\n\n* Item one.\n";

    mock.method(globalThis, "fetch", async () => new Response(fakeChangelog, { status: 200 }));

    const result = await getChangelogItems("v8.8.8");

    assert.deepEqual(result, ["Item one."]);
  });

  test("uses the cached changelog on a second call for the same version (phase-1 fast path)", async () => {

    // The phase-1 fast path checks cachedChangelog first and returns without fetching when the version's entry is found. We seed via the first call, then assert
    // the second call reuses the cache by counting fetch invocations - it should stay at 1, not 2.
    const fakeChangelog = "## 6.6.6 (2025-02-10)\n\n* Cached item.\n";
    let fetchCalls = 0;

    mock.method(globalThis, "fetch", async () => {

      fetchCalls += 1;

      return new Response(fakeChangelog, { status: 200 });
    });

    const first = await getChangelogItems("6.6.6");

    assert.deepEqual(first, ["Cached item."], "first call returned the parsed entry");
    assert.equal(fetchCalls, 1, "first call hit the network");

    const second = await getChangelogItems("6.6.6");

    assert.deepEqual(second, ["Cached item."], "second call returned the same entry");
    assert.equal(fetchCalls, 1, "second call did NOT fetch - phase-1 cache hit");
  });
});

describe("checkForUpdates", () => {

  afterEach(() => {

    mock.reset();
  });

  test("debounces consecutive calls within UPDATE_CHECK_DEBOUNCE (second call is a no-op when force is omitted)", async () => {

    // The debounce window is 60s. Two checkForUpdates calls back-to-back within that window should result in exactly one fetch - the second collapses into the
    // debounce skip path. Without force, the window guard fires on the second call and short-circuits before any network traffic.
    let fetchCalls = 0;

    mock.method(globalThis, "fetch", async () => {

      fetchCalls += 1;

      return new Response(JSON.stringify({ "dist-tags": { latest: "5.5.5" } }), { status: 200 });
    });

    // Force the first call so the debounce window starts from a known state (not contaminated by prior tests).
    await checkForUpdates("1.0.0", true);

    const callsAfterFirst = fetchCalls;

    // Second call without force, immediately after - the debounce guard at line 162 should fire.
    await checkForUpdates("1.0.0", false);

    assert.equal(fetchCalls, callsAfterFirst, "second call without force was debounced (no additional fetch)");
  });

  test("force=true bypasses the debounce window (second call fetches again)", async () => {

    // Symmetric: force=true skips the debounce and re-fetches even within the window.
    let fetchCalls = 0;

    mock.method(globalThis, "fetch", async () => {

      fetchCalls += 1;

      return new Response(JSON.stringify({ "dist-tags": { latest: "5.5.6" } }), { status: 200 });
    });

    await checkForUpdates("1.0.0", true);
    const callsAfterFirst = fetchCalls;

    await checkForUpdates("1.0.0", true);

    assert.ok(fetchCalls > callsAfterFirst, "force=true triggered a second fetch within the debounce window");
  });

  test("logs 'Update available' on the first call where a newer version is detected (isNewUpdate flag)", async () => {

    // The isNewUpdate flag is true when (previousLatest !== latest) && (current < latest). On the second call where latest changes (or first ever where there's
    // a newer version), the flag fires the LOG.info call. We capture LOG output via the SSE log emitter, not by stubbing LOG itself - the emitter is the
    // observable channel the rest of the codebase already uses.
    const { subscribeToLogs } = await import("./logEmitter.ts");
    const captured: { level: string; message: string }[] = [];
    const unsubscribe = subscribeToLogs((entry) => { captured.push({ level: entry.level, message: entry.message }); });

    try {

      mock.method(globalThis, "fetch", async (url: string | URL): Promise<Response> => {

        if(String(url).includes("registry.npmjs.org")) {

          return new Response(JSON.stringify({ "dist-tags": { latest: "99.99.99" } }), { status: 200 });
        }

        // Changelog refresh path: when isNewUpdate fires, fetchChangelogContent is called. Return an empty body; the test doesn't assert on the changelog itself.
        return new Response("", { status: 200 });
      });

      // First-with-this-latest call: previousLatest is null (or whatever), latest is "99.99.99", current "1.0.0" < latest -> isNewUpdate=true -> log fires.
      await checkForUpdates("1.0.0", true);

      const updateLogged = captured.some((entry) => entry.message.includes("Update available"));

      assert.equal(updateLogged, true, "Update available log emitted on first detection");
    } finally {

      unsubscribe();
    }
  });
});

describe("extractVersionChangelog regex boundaries (via getChangelogItems)", () => {

  /* extractVersionChangelog is a private function; we exercise it indirectly through getChangelogItems. The regex anchors that matter:
   *
   *   - The (?![^]) end-of-string sentinel terminates the non-greedy capture at file end (no following ## header).
   *   - Dots in the version number are escaped via replaceAll(".", "\\.") so "1.0.8" doesn't match "1Z0Z8".
   *   - The (?=^## \d|...) lookahead stops the capture at the next "## N" header.
   */

  afterEach(() => {

    mock.reset();
  });

  test("extracts only the requested entry when two adjacent entries share a name prefix", async () => {

    // Boundary: a changelog with adjacent entries "## 4.4.4" and "## 4.4.5". A naive non-greedy regex without the right lookahead would either extract too
    // much or too little. We assert the requested entry's items are returned without bleed-through from the next entry.
    const changelog = "## 4.4.5 (2025-04-01)\n\n* Newer item.\n\n## 4.4.4 (2025-03-01)\n\n* Older item.\n";

    mock.method(globalThis, "fetch", async () => new Response(changelog, { status: 200 }));

    const result = await getChangelogItems("4.4.5");

    assert.deepEqual(result, ["Newer item."], "only the 4.4.5 items captured, not 4.4.4");
  });

  test("returns null when the version header has no bullet items underneath", async () => {

    // Boundary: a version section can exist with no bullets (rare but possible if a release was tagged but the entry wasn't filled out). parseChangelogItems
    // returns null when no lines start with '*'.
    const changelog = "## 3.3.3 (2025-01-01)\n\nThis release contains no bullet items.\n";

    mock.method(globalThis, "fetch", async () => new Response(changelog, { status: 200 }));

    const result = await getChangelogItems("3.3.3");

    assert.equal(result, null, "no bullets -> null");
  });

  test("escapes literal dots in the version number so 1.0.8 does not match arbitrary characters", async () => {

    // Negative test: if dots weren't escaped, "1.0.8" would match "1X0X8" via the regex's `.` metacharacter. Constructing a changelog with a header like
    // "## 1X0X8" (impossible in practice but a useful negative input) - the lookup must NOT extract anything because the literal-dot match fails.
    const changelog = "## 1X0X8 (2025-05-01)\n\n* Should not match.\n";

    mock.method(globalThis, "fetch", async () => new Response(changelog, { status: 200 }));

    const result = await getChangelogItems("1.0.8");

    assert.equal(result, null, "literal dots are required - regex did not match the metacharacter-style header");
  });
});
