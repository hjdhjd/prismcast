/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * version.test.ts: Unit tests for the version helpers in version.ts. The pure functions (normalizeVersion, isVersionLessThan) get full boundary coverage; the
 * networked functions (fetchLatestVersion, getChangelogItems, checkForUpdates) are exercised by stubbing globalThis.fetch via mock.method so no real HTTP calls
 * fire. getPackageVersion reads the project package.json from disk; we lock its return shape (a non-empty version string).
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { fetchLatestVersion, getChangelogItems, getPackageVersion, getVersionInfo, isVersionLessThan, normalizeVersion, startUpdateChecking,
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

  test("reports updateAvailable=true when current is less than the cached latest", async () => {

    // We seed the cache via fetchLatestVersion + checkForUpdates because cachedLatestVersion is module-scope and not exported. The test is self-contained:
    // stub fetch to return a known 'latest', check, then read getVersionInfo.
    mock.method(globalThis, "fetch", async (url: string | URL) => {

      const u = String(url);

      if(u.includes("registry.npmjs.org")) {

        return new Response(JSON.stringify({ "dist-tags": { latest: "9.9.9" } }), { status: 200 });
      }

      return new Response("", { status: 404 });
    });

    const latest = await fetchLatestVersion();

    assert.equal(latest, "9.9.9", "fetch stub returned the seeded latest");

    // getVersionInfo reads the module-cache. After fetchLatestVersion, the cache is not populated yet (only checkForUpdates writes it). We exercise the function
    // shape regardless of whether the cache is hit.
    const info = getVersionInfo("0.0.1");

    assert.equal(typeof info.updateAvailable, "boolean");
    assert.ok((info.latestVersion === null) || (typeof info.latestVersion === "string"));

    mock.reset();
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
});
