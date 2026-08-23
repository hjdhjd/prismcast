/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * upgrade.test.ts: Unit tests for the upgrade routes in upgrade.ts. setupUpgradeEndpoint registers GET /upgrade/info and POST /upgrade.
 *
 * Two servers are booted. The first runs on the real dependencies and covers only the GET /upgrade/info JSON envelope, including both the success and failure
 * branches of fetchLatestVersion, mocked at the fetch boundary.
 *
 * The whole POST decision tree lives on the second server, which runs on an injected UpgradeDeps: the non-upgradeable guard, the dispatch of the detected
 * InstallInfo, both UpgradeStep kinds, and the failed-command path, each with its own shutdown expectation. Injecting the boundary is what makes those branches
 * testable at all - running them for real would invoke a package manager and exit the process. Actually installing anything remains e2e territory.
 */
import type { AddressInfo, Server } from "node:net";
import { after, afterEach, before, beforeEach, describe, mock, test } from "node:test";
import { closePuppeteerStreamWss, firstOf } from "../testing.helpers.ts";
import type { InstallInfo } from "../upgrade/detection.ts";
import { LOG } from "../utils/index.ts";
import type { UpgradeDeps } from "./upgrade.ts";
import type { UpgradeStep } from "../upgrade/lifecycle.ts";
import assert from "node:assert/strict";
import express from "express";
import { setupUpgradeEndpoint } from "./upgrade.ts";

interface UpgradeInfoResponse {

  currentVersion: string;
  latestVersion: string | null;
  method: string;
  updateAvailable: boolean;
  upgradeCommand?: string;
  upgradeable: boolean;
}

function makeServer(deps?: UpgradeDeps): Promise<{ port: number; server: Server }> {

  const app = express();

  setupUpgradeEndpoint(app, deps);

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

let sharedServer: Server;
let sharedPort = 0;

function urlFor(path: string): string {

  return "http://127.0.0.1:" + String(sharedPort) + path;
}

before(async () => {

  const created = await makeServer();

  sharedServer = created.server;
  sharedPort = created.port;
});

after(async () => {

  await closeServer(sharedServer);
  await closePuppeteerStreamWss();
});

/* The /upgrade/info handler calls fetchLatestVersion which hits the npm registry. We mock globalThis.fetch in beforeEach/afterEach so each test controls the
 * registry response without making real network calls. The mock.restoreAll() pattern in afterEach guarantees other test files (and the test runner itself)
 * see the original fetch.
 */
const originalFetch = globalThis.fetch;

beforeEach(() => {

  globalThis.fetch = mock.fn(async (input: string | URL | Request, init?: RequestInit): Promise<Response> => {

    const url = (typeof input === "string") ? input : (input instanceof URL) ? input.toString() : input.url;

    // Return a fake "latest version" response for the npm registry lookup, leaving everything else to the real fetch (e.g., the test server itself). The init
    // argument carries the method, body, and headers; without forwarding it, POST requests would silently degrade to GET and produce 404 from the test server.
    if(url.startsWith("https://registry.npmjs.org/")) {

      return new Response(JSON.stringify({ "dist-tags": { latest: "99.99.99" } }), {

        headers: { "content-type": "application/json" },
        status: 200
      });
    }

    return originalFetch(input, init);
  });
});

afterEach(() => {

  globalThis.fetch = originalFetch;
  mock.restoreAll();
});

describe("setupUpgradeEndpoint - GET /upgrade/info", () => {

  test("returns 200 with the documented response keys", async () => {

    const res = await fetch(urlFor("/upgrade/info"));

    assert.equal(res.status, 200);
    assert.match(res.headers.get("content-type") ?? "", /application\/json/);

    const body = await res.json() as UpgradeInfoResponse;

    // Locks the public surface of the response. Any addition or removal here should be intentional.
    assert.equal(typeof body.currentVersion, "string");
    assert.ok([ "string", "object" ].includes(typeof body.latestVersion), "latestVersion is string or null");
    assert.equal(typeof body.method, "string");
    assert.equal(typeof body.updateAvailable, "boolean");
    assert.equal(typeof body.upgradeable, "boolean");
  });

  test("currentVersion is normalized (no leading 'v') and non-empty", async () => {

    const res = await fetch(urlFor("/upgrade/info"));
    const body = await res.json() as UpgradeInfoResponse;

    // normalizeVersion strips only a leading lowercase 'v' (the regex is /^v/, case-sensitive). The version comes from package.json so we only verify it
    // doesn't start with 'v' and is non-empty.
    assert.ok(body.currentVersion.length > 0);
    assert.doesNotMatch(body.currentVersion, /^[vV]/);
  });

  test("updateAvailable is true when the mocked latest version (99.99.99) is greater than the current version", async () => {

    // Boundary: this exercises the version comparison branch. The mocked latest is far higher than any real PrismCast version, so updateAvailable must be true.
    const res = await fetch(urlFor("/upgrade/info"));
    const body = await res.json() as UpgradeInfoResponse;

    assert.equal(body.updateAvailable, true, "99.99.99 > current version, so updateAvailable=true");
    assert.equal(body.latestVersion, "99.99.99");
  });

  test("updateAvailable is false when the registry returns a lower version", async () => {

    // Boundary: locking the inverse of the comparison. Override the mock for this test to return a low version.
    globalThis.fetch = mock.fn(async (input: string | URL | Request, init?: RequestInit): Promise<Response> => {

      const url = (typeof input === "string") ? input : (input instanceof URL) ? input.toString() : input.url;

      if(url.startsWith("https://registry.npmjs.org/")) {

        return new Response(JSON.stringify({ "dist-tags": { latest: "0.0.1" } }), { status: 200 });
      }

      return originalFetch(input, init);
    });

    const res = await fetch(urlFor("/upgrade/info"));
    const body = await res.json() as UpgradeInfoResponse;

    assert.equal(body.updateAvailable, false, "0.0.1 < current version, so updateAvailable=false");
  });

  test("updateAvailable is false when the registry fetch fails (latestVersion=null)", async () => {

    // Boundary: when fetchLatestVersion returns null, the handler computes (null !== null) && ... which is false. Lock that no update is reported.
    globalThis.fetch = mock.fn(async (input: string | URL | Request, init?: RequestInit): Promise<Response> => {

      const url = (typeof input === "string") ? input : (input instanceof URL) ? input.toString() : input.url;

      if(url.startsWith("https://registry.npmjs.org/")) {

        return new Response("server error", { status: 500 });
      }

      return originalFetch(input, init);
    });

    const res = await fetch(urlFor("/upgrade/info"));
    const body = await res.json() as UpgradeInfoResponse;

    assert.equal(body.latestVersion, null);
    assert.equal(body.updateAvailable, false);
  });

  test("method is one of the documented installation methods", async () => {

    // The detection module enumerates docker, homebrew, npm-global, npm-local, and unknown. Locking the constraint catches a regression that emitted a
    // bogus method label.
    const res = await fetch(urlFor("/upgrade/info"));
    const body = await res.json() as UpgradeInfoResponse;

    assert.match(body.method, /^(docker|homebrew|npm-global|npm-local|source|unknown)$/);
  });
});

/* The deps-injected POST coverage. setupUpgradeEndpoint takes its detection, upgrade dispatch, service probe, and shutdown as an injected UpgradeDeps parameter,
 * so the whole POST decision tree runs against a real Express server without a package manager ever being invoked and without the process exiting. The scenario
 * lives in one mutable state object the deps read through, mirroring health.test.ts's mockState: beforeEach restores the defaults and each test overrides only
 * the field its branch turns on.
 */
interface MockUpgradeState {

  info: InstallInfo;
  isService: boolean;
  performCalls: InstallInfo[];
  shutdowns: number;
  step: UpgradeStep;
}

let mockUpgrade: MockUpgradeState;

const injectedDeps: UpgradeDeps = {

  detect: (): InstallInfo => mockUpgrade.info,
  isRunningAsService: (): boolean => mockUpgrade.isService,
  performUpgrade: async (info: InstallInfo): Promise<UpgradeStep> => {

    mockUpgrade.performCalls.push(info);

    return mockUpgrade.step;
  },
  scheduleShutdown: (): void => {

    mockUpgrade.shutdowns++;
  }
};

function makeInstallInfo(overrides: Partial<InstallInfo> = {}): InstallInfo {

  return {

    displayName: "npm (global)",
    method: "npm-global",
    upgradeCommand: "npm install -g prismcast@latest",
    upgradeable: true,
    ...overrides
  } as InstallInfo;
}

interface UpgradePostResponse {

  error?: string;
  logPath?: string;
  message?: string;
  success: boolean;
  willRestart?: boolean;
}

describe("setupUpgradeEndpoint - POST /upgrade against injected deps", () => {

  let injectedServer: Server;
  let injectedPort = 0;

  before(async () => {

    const created = await makeServer(injectedDeps);

    injectedServer = created.server;
    injectedPort = created.port;
  });

  after(async () => {

    await closeServer(injectedServer);
  });

  beforeEach(() => {

    mockUpgrade = { info: makeInstallInfo(), isService: false, performCalls: [], shutdowns: 0, step: { kind: "ran", success: true } };
  });

  function postUpgrade(): Promise<Response> {

    return fetch("http://127.0.0.1:" + String(injectedPort) + "/upgrade", { method: "POST" });
  }

  test("short-circuits a non-upgradeable install without reaching the lifecycle", async () => {

    // The guard the route keeps ahead of the dispatch. Docker and unrecognized layouts cannot be upgraded in place, and the lifecycle must never be asked to
    // try - a strategy that ran here would execute a command the detection layer already ruled out.
    mockUpgrade.info = makeInstallInfo({ manualUpgradeMessage: ["Upgrade manually:"], method: "docker", upgradeable: false });

    const res = await postUpgrade();

    assert.equal(res.status, 400);

    const body = await res.json() as UpgradePostResponse;

    assert.equal(body.success, false);
    assert.match(body.error ?? "", /does not support in-place upgrades/);
    assert.equal(mockUpgrade.performCalls.length, 0, "the lifecycle is never dispatched for a non-upgradeable install");
    assert.equal(mockUpgrade.shutdowns, 0);
  });

  test("hands the detected install info to the lifecycle verbatim", async () => {

    // The route decides WHAT (an upgrade was requested) and the lifecycle decides HOW. Passing the InstallInfo through untouched is what lets packageDir reach
    // the runner as its working directory without the route knowing that npm-local is the method that needs one.
    mockUpgrade.info = makeInstallInfo({ method: "npm-local", packageDir: "/Users/me/proj", upgradeCommand: "npm install prismcast@latest" });

    await postUpgrade();

    assert.equal(mockUpgrade.performCalls.length, 1);

    const dispatched = firstOf(mockUpgrade.performCalls, "performUpgrade call");

    assert.equal(dispatched.upgradeCommand, "npm install prismcast@latest");
    assert.equal(dispatched.packageDir, "/Users/me/proj");
  });

  test("a handed-off outcome answers with the helper log path and shuts down", async () => {

    // The Windows path. A detached helper is already waiting on this process to exit before it can rename the install directory, so the response goes out and
    // the shutdown follows unconditionally. willRestart stays false because the client polls for a restart when it is true and this upgrade outlasts that poll.
    mockUpgrade.step = { kind: "handed-off", logPath: "C:\\Users\\jp\\.prismcast\\upgrade.log" };

    const res = await postUpgrade();

    assert.equal(res.status, 200);

    const body = await res.json() as UpgradePostResponse;

    assert.equal(body.success, true);
    assert.equal(body.logPath, "C:\\Users\\jp\\.prismcast\\upgrade.log");
    assert.equal(body.willRestart, false);
    assert.match(body.message ?? "", /running in the background/);
    assert.match(body.message ?? "", /otherwise, restart PrismCast manually/);
    assert.equal(mockUpgrade.shutdowns, 1);
  });

  test("a handed-off outcome shuts down even when PrismCast is registered as a service", async () => {

    // The exit is unconditional for this outcome, in both directions: the helper cannot begin until we are gone, whether or not a service task will bring us
    // back. A regression that reused the in-process branch's isRunningAsService gate would strand the helper waiting on a process that never exits.
    mockUpgrade.isService = true;
    mockUpgrade.step = { kind: "handed-off", logPath: "C:\\log.txt" };

    await postUpgrade();

    assert.equal(mockUpgrade.shutdowns, 1);
  });

  test("an in-process success under a service manager reports willRestart and shuts down", async () => {

    // launchd, systemd, and Task Scheduler all bring the process back after it exits, so exiting is how the new version takes effect. The client reads
    // willRestart to decide whether to wait for the server to return.
    mockUpgrade.isService = true;

    const res = await postUpgrade();

    assert.equal(res.status, 200);

    const body = await res.json() as UpgradePostResponse;

    assert.equal(body.success, true);
    assert.equal(body.willRestart, true);
    assert.equal(body.message, "Upgrade complete.");
    assert.equal(mockUpgrade.shutdowns, 1);
  });

  test("an in-process success outside a service manager stays up", async () => {

    // Nothing would restart a manual install, so exiting would leave the user with no PrismCast at all. We stay on the old version until they restart, which is
    // exactly what willRestart=false tells the client to say.
    const res = await postUpgrade();
    const body = await res.json() as UpgradePostResponse;

    assert.equal(body.success, true);
    assert.equal(body.willRestart, false);
    assert.equal(mockUpgrade.shutdowns, 0, "a manual install is never shut down out from under the user");
  });

  test("an in-process failure answers on the error envelope and stays up", async (t) => {

    // A package manager that exits non-zero is a failed upgrade, not a completed one. The response carries the same error envelope a thrown failure produces so
    // the web UI's single error path covers both, and nothing shuts down - the old version is still the running one.
    t.mock.method(LOG, "error", () => { /* Captured via the mock. */ });

    mockUpgrade.step = { kind: "ran", success: false };

    const res = await postUpgrade();

    assert.equal(res.status, 500);

    const body = await res.json() as UpgradePostResponse;

    assert.equal(body.success, false);
    assert.match(body.error ?? "", /the upgrade command reported a failure/);
    assert.equal(mockUpgrade.shutdowns, 0, "a failed upgrade never takes the running version down");
  });
});
