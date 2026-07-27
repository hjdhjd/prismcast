/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.test.ts: Unit tests for the HDHomeRun emulation server lifecycle and its live-apply config-change handler. Coverage spans the observable behaviors
 * of startHdhrServer / stopHdhrServer - the disabled short-circuit, automatic DeviceID generation when missing or invalid, graceful EADDRINUSE handling on
 * port collision, and shutdown that is safe to call more than once - plus applyHdhrConfigChanges, including the end-to-end HTTP rebind on a port change
 * and rejection of a change to an already-occupied port.
 * Each test uses an OS-assigned port (port 0) so it never collides with the production HDHR port; data-directory side effects are routed into a per-test
 * temp dir so persistence calls inside startHdhrServer cannot leak to the user's real ~/.prismcast directory.
 */
import type { ChangeOutcome, ConfigChange } from "../config/reactivity.ts";
import { afterEach, beforeEach, describe, test } from "node:test";
import { applyHdhrConfigChanges, startHdhrServer, stopHdhrServer } from "./index.ts";
import { CONFIG } from "../config/index.ts";
import type { Server } from "node:http";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { generateDeviceId } from "./deviceId.ts";
import { initializeDataDir } from "../config/paths.ts";
import { withTempDir } from "../testing.helpers.ts";

// snapshotConfig captures the specific CONFIG.hdhr fields these tests mutate (deviceId, discoveryEnabled, enabled, port) so each test can restore the prior
// values verbatim. It does not cover every field startHdhrServer reads, such as CONFIG.server.host, only the ones these tests exercise.
function snapshotConfig(): { deviceId: string; discoveryEnabled: boolean; enabled: boolean; port: number } {

  return { deviceId: CONFIG.hdhr.deviceId, discoveryEnabled: CONFIG.hdhr.discoveryEnabled, enabled: CONFIG.hdhr.enabled, port: CONFIG.hdhr.port };
}

function restoreConfig(prior: { deviceId: string; discoveryEnabled: boolean; enabled: boolean; port: number }): void {

  CONFIG.hdhr.deviceId = prior.deviceId;
  CONFIG.hdhr.discoveryEnabled = prior.discoveryEnabled;
  CONFIG.hdhr.enabled = prior.enabled;
  CONFIG.hdhr.port = prior.port;
}

// listenOnEphemeral reserves a real port by listening on it without serving anything; used to force EADDRINUSE in collision tests. The host defaults to
// 127.0.0.1, but tests that need a genuine conflict with the HDHR server (which binds CONFIG.server.host, default "0.0.0.0") must pass the matching host -
// a 0.0.0.0 listener and a 127.0.0.1 listener on the same port coexist under SO_REUSEADDR and would not actually collide. We close the port in the test's
// afterEach (or its own finally) to keep the OS sockets clean.
async function listenOnEphemeral(host = "127.0.0.1"): Promise<{ port: number; server: Server }> {

  const { promise, resolve, reject } = Promise.withResolvers<{ port: number; server: Server }>();
  const server = createServer();

  server.listen(0, host, () => {

    const address = server.address();

    if((typeof address !== "object") || (address === null)) {

      reject(new Error("Failed to obtain ephemeral port"));

      return;
    }

    resolve({ port: address.port, server });
  });

  server.on("error", reject);

  return promise;
}

async function closeServer(server: Server): Promise<void> {

  // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
  const { promise, resolve } = Promise.withResolvers<void>();

  server.close(() => { resolve(); });

  return promise;
}

describe("startHdhrServer - disabled", () => {

  let prior: { deviceId: string; discoveryEnabled: boolean; enabled: boolean; port: number };

  beforeEach(() => {

    prior = snapshotConfig();
  });

  afterEach(async () => {

    await stopHdhrServer();
    restoreConfig(prior);
  });

  test("returns early without starting a server when CONFIG.hdhr.enabled is false", async () => {

    CONFIG.hdhr.enabled = false;
    CONFIG.hdhr.port = 0;
    CONFIG.hdhr.deviceId = generateDeviceId();

    // The function returns void in either path; we observe the no-server effect by attempting a probe and confirming the connection is refused. Since we set
    // port to 0 (which is an invalid client target anyway), the simpler observation is that startHdhrServer resolves without throwing and stopHdhrServer is a
    // no-op that doesn't crash.
    await startHdhrServer();

    // stopHdhrServer must be a safe no-op when no server was started.
    await assert.doesNotReject(stopHdhrServer);
  });

  test("does not regenerate the DeviceID when disabled (skips the validation branch)", async () => {

    const validId = generateDeviceId();

    CONFIG.hdhr.enabled = false;
    CONFIG.hdhr.deviceId = validId;
    CONFIG.hdhr.port = 0;

    await startHdhrServer();

    assert.equal(CONFIG.hdhr.deviceId, validId, "DeviceID untouched when disabled");
  });
});

describe("startHdhrServer - successful start", () => {

  let prior: { deviceId: string; discoveryEnabled: boolean; enabled: boolean; port: number };

  beforeEach(() => {

    prior = snapshotConfig();

    // Tests in this suite enable HDHR HTTP but never want LAN discovery: binding UDP 65001 in a test risks colliding with a real HDHomeRun on the developer's
    // network or with another test process. The HTTP-only path is what these tests exercise; UDP-specific coverage lives in udp.test.ts.
    CONFIG.hdhr.discoveryEnabled = false;
  });

  afterEach(async () => {

    await stopHdhrServer();
    restoreConfig(prior);
  });

  test("starts the HTTP server on an OS-assigned port without throwing", async () => {

    // Port 0 lets the OS pick a free port. We can't observe the chosen port from the public API (the controller's HTTP surface owns it), but we can confirm that
    // start completes without throwing and stopHdhrServer cleanly tears it down.
    CONFIG.hdhr.enabled = true;
    CONFIG.hdhr.deviceId = generateDeviceId();
    CONFIG.hdhr.port = 0;

    await assert.doesNotReject(() => startHdhrServer(), "start should resolve when port is available");
  });

  test("preserves a valid existing DeviceID without regenerating", async () => {

    // When deviceId passes validateDeviceId, the function leaves it alone. Locking this prevents a regression where every restart would mint a fresh ID.
    const validId = generateDeviceId();

    CONFIG.hdhr.enabled = true;
    CONFIG.hdhr.deviceId = validId;
    CONFIG.hdhr.port = 0;

    await startHdhrServer();

    assert.equal(CONFIG.hdhr.deviceId, validId, "valid DeviceID was preserved across start");
  });

  test("regenerates the DeviceID when the existing one is empty", async () => {

    await withTempDir(async (dir) => {

      // The persistence call inside startHdhrServer needs a valid data dir; without one it falls into the catch path and warns. Either branch leaves CONFIG
      // populated with a valid generated ID, which is what we assert here.
      initializeDataDir(dir);

      CONFIG.hdhr.enabled = true;
      CONFIG.hdhr.deviceId = "";
      CONFIG.hdhr.port = 0;

      await startHdhrServer();

      assert.notEqual(CONFIG.hdhr.deviceId, "", "DeviceID was generated");
      assert.match(CONFIG.hdhr.deviceId, /^[0-9a-f]{8}$/, "DeviceID is valid hex");
    });
  });

  test("regenerates the DeviceID when the existing one fails the checksum", async () => {

    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      CONFIG.hdhr.enabled = true;
      // 10000000 has the right shape but a nonzero checksum (caught by validateDeviceId).
      CONFIG.hdhr.deviceId = "10000000";
      CONFIG.hdhr.port = 0;

      await startHdhrServer();

      assert.notEqual(CONFIG.hdhr.deviceId, "10000000", "invalid-checksum DeviceID was replaced");
      assert.match(CONFIG.hdhr.deviceId, /^[0-9a-f]{8}$/, "new DeviceID is valid hex");
    });
  });
});

describe("startHdhrServer - port collision", () => {

  let prior: { deviceId: string; discoveryEnabled: boolean; enabled: boolean; port: number };
  let blocker: Server | null = null;

  beforeEach(() => {

    prior = snapshotConfig();

    // Port-collision tests exercise the HTTP bind failure path. UDP must stay off so the failure is unambiguously about the HTTP server.
    CONFIG.hdhr.discoveryEnabled = false;
  });

  afterEach(async () => {

    await stopHdhrServer();

    if(blocker) {

      await closeServer(blocker);
      blocker = null;
    }

    restoreConfig(prior);
  });

  test("handles EADDRINUSE gracefully without throwing or starting a server", async () => {

    // We claim a real port first so app.listen on the same port produces EADDRINUSE. The handler is supposed to swallow the error and log a warning rather than
    // propagate it.
    const reserved = await listenOnEphemeral();

    blocker = reserved.server;
    CONFIG.hdhr.enabled = true;
    CONFIG.hdhr.deviceId = generateDeviceId();
    CONFIG.hdhr.port = reserved.port;

    await assert.doesNotReject(() => startHdhrServer(), "EADDRINUSE must be caught, not propagated");

    // After the failed start, stopHdhrServer must still be safe to call (the server reference should be null).
    await assert.doesNotReject(stopHdhrServer);
  });
});

describe("stopHdhrServer", () => {

  let prior: { deviceId: string; discoveryEnabled: boolean; enabled: boolean; port: number };

  beforeEach(() => {

    prior = snapshotConfig();
    CONFIG.hdhr.discoveryEnabled = false;
  });

  afterEach(async () => {

    await stopHdhrServer();
    restoreConfig(prior);
  });

  test("is a no-op when called before any server was started", async () => {

    // The controller's surfaces are down on a fresh test run, so the call must resolve without rejecting.
    await assert.doesNotReject(stopHdhrServer);
  });

  test("is idempotent when called twice in a row", async () => {

    CONFIG.hdhr.enabled = true;
    CONFIG.hdhr.deviceId = generateDeviceId();
    CONFIG.hdhr.port = 0;

    await startHdhrServer();

    await stopHdhrServer();

    // The surfaces are down after the first call; the second must observe no bound socket and short-circuit.
    await assert.doesNotReject(stopHdhrServer, "second stop is a safe no-op");
  });

  test("allows a fresh start after a stop", async () => {

    CONFIG.hdhr.enabled = true;
    CONFIG.hdhr.deviceId = generateDeviceId();
    CONFIG.hdhr.port = 0;

    await startHdhrServer();
    await stopHdhrServer();

    // After a stop, the surfaces are down but reusable; a second start should succeed identically.
    await assert.doesNotReject(() => startHdhrServer(), "restart after stop must succeed");
  });
});

describe("applyHdhrConfigChanges - live-apply handler", () => {

  let prior: { deviceId: string; discoveryEnabled: boolean; enabled: boolean; port: number };

  // makeChange constructs a synthetic ConfigChange. The path drives the dispatch; the previous/current values are opaque to the handler (it reads live CONFIG).
  function makeChange(path: string): ConfigChange {

    return { current: null, path, previous: null };
  }

  beforeEach(() => {

    prior = snapshotConfig();
    CONFIG.hdhr.discoveryEnabled = false;
  });

  afterEach(async () => {

    await stopHdhrServer();
    restoreConfig(prior);
  });

  test("hdhr.enabled false-to-true with valid DeviceID brings the HTTP server up live", async () => {

    // Start in disabled state, then flip CONFIG and call the handler. The post-condition is that startHdhrServer-equivalent side effects ran.
    CONFIG.hdhr.enabled = false;
    CONFIG.hdhr.deviceId = generateDeviceId();
    CONFIG.hdhr.port = 0;

    // The handler reads the new CONFIG value, so we flip it before invoking.
    CONFIG.hdhr.enabled = true;

    const outcomes = await applyHdhrConfigChanges([makeChange("hdhr.enabled")]);

    assert.deepEqual(outcomes, [{ kind: "applied", path: "hdhr.enabled" }] satisfies ChangeOutcome[]);
  });

  test("hdhr.enabled true-to-false stops the HTTP server live", async () => {

    CONFIG.hdhr.enabled = true;
    CONFIG.hdhr.deviceId = generateDeviceId();
    CONFIG.hdhr.port = 0;

    await startHdhrServer();

    // Flip off and run the handler; stopHdhrServer should be invoked.
    CONFIG.hdhr.enabled = false;

    const outcomes = await applyHdhrConfigChanges([makeChange("hdhr.enabled")]);

    assert.deepEqual(outcomes, [{ kind: "applied", path: "hdhr.enabled" }] satisfies ChangeOutcome[]);
  });

  test("hdhr.discoveryEnabled toggle while HDHR is disabled is a no-op applied", async () => {

    CONFIG.hdhr.enabled = false;
    CONFIG.hdhr.discoveryEnabled = true;

    const outcomes = await applyHdhrConfigChanges([makeChange("hdhr.discoveryEnabled")]);

    assert.deepEqual(outcomes, [{ kind: "applied", path: "hdhr.discoveryEnabled" }] satisfies ChangeOutcome[]);
  });

  test("hdhr.deviceId and hdhr.friendlyName are no-op applied (consumers read CONFIG live)", async () => {

    const outcomes = await applyHdhrConfigChanges([ makeChange("hdhr.deviceId"), makeChange("hdhr.friendlyName") ]);

    assert.deepEqual(outcomes, [
      { kind: "applied", path: "hdhr.deviceId" },
      { kind: "applied", path: "hdhr.friendlyName" }
    ] satisfies ChangeOutcome[]);
  });

  test("an unknown hdhr.* path defers with a documented reason so unhandled new fields prompt a restart", async () => {

    const outcomes = await applyHdhrConfigChanges([makeChange("hdhr.someUnknownFutureField")]);

    assert.equal(outcomes.length, 1);
    assert.equal(outcomes[0]?.kind, "deferred");
  });

  test("multiple changes in one batch each receive an outcome in input order", async () => {

    CONFIG.hdhr.enabled = false;
    const outcomes = await applyHdhrConfigChanges([
      makeChange("hdhr.friendlyName"),
      makeChange("hdhr.deviceId"),
      makeChange("hdhr.discoveryEnabled")
    ]);

    assert.deepEqual(outcomes.map((o: ChangeOutcome) => o.path), [ "hdhr.friendlyName", "hdhr.deviceId", "hdhr.discoveryEnabled" ]);
  });

  test("hdhr.port live-apply rebinds the HTTP server on the new port end-to-end", async () => {

    // This test verifies that after applyHdhrConfigChanges fires for hdhr.port, /discover.json answers on the new port through the rebuilt server; it does
    // not check that the old port stops responding. The test exercises the live-rebind close-then-bind sequencing that motivated awaiting the server close
    // before the rebind inside HttpSurface.ensureBound.
    CONFIG.hdhr.enabled = true;
    CONFIG.hdhr.deviceId = generateDeviceId();
    CONFIG.hdhr.port = 0;

    await startHdhrServer();

    // We start on OS-assigned port 0; the running server picks a port we cannot predict, so we reserve a second ephemeral port via a sacrificial listener,
    // close it to free the port, then ask the live-apply handler to switch onto it. The reserved-port-then-released pattern keeps the test deterministic
    // without making us guess a free port.
    const reserved = await listenOnEphemeral();
    const newPort = reserved.port;

    await closeServer(reserved.server);

    CONFIG.hdhr.port = newPort;

    const outcomes = await applyHdhrConfigChanges([makeChange("hdhr.port")]);

    assert.deepEqual(outcomes, [{ kind: "applied", path: "hdhr.port" }] satisfies ChangeOutcome[]);

    // Fetch /discover.json on the new port. A successful 200 with a DeviceID-bearing payload confirms the rebind landed end-to-end.
    const res = await fetch("http://127.0.0.1:" + String(newPort) + "/discover.json");

    assert.equal(res.status, 200, "live-rebound HTTP server answers on the new port");

    const body = await res.json() as Record<string, unknown>;

    assert.equal(typeof body["DeviceID"], "string", "discover.json includes DeviceID after rebind");
    assert.equal(body["DeviceID"], CONFIG.hdhr.deviceId.toUpperCase());
  });

  test("a port change to an occupied port is rejected and keeps the previous port alive (concession)", async () => {

    // Reserve a free port for the initial bind (reserve then release so it is a known-free number we can fetch later), and a second port we keep occupied so
    // the rebind fails. Both blockers bind the SAME host the HDHR server uses (CONFIG.server.host, default "0.0.0.0") so the conflict is a real exact-address
    // collision - a 0.0.0.0 bind and a 127.0.0.1 listener on the same port coexist under SO_REUSEADDR and would not collide. The handler must report rejected
    // AND leave HTTP serving on the original port: a typo'd port must not take down a working tuner.
    const hdhrHost = CONFIG.server.host;
    const initial = await listenOnEphemeral(hdhrHost);
    const priorPort = initial.port;

    await closeServer(initial.server);

    CONFIG.hdhr.enabled = true;
    CONFIG.hdhr.deviceId = generateDeviceId();
    CONFIG.hdhr.port = priorPort;

    await startHdhrServer();

    // Occupy a different port on the same host and keep the blocker open so the rebind to it fails with EADDRINUSE.
    const blocker = await listenOnEphemeral(hdhrHost);

    try {

      CONFIG.hdhr.port = blocker.port;

      const outcomes = await applyHdhrConfigChanges([makeChange("hdhr.port")]);

      assert.equal(outcomes.length, 1);
      assert.equal(outcomes[0]?.kind, "rejected", "an occupied-port change is rejected, not falsely reported applied");

      // Concession: the previous port is still serving. /discover.json on the prior port must answer 200 (fetch via loopback, which a 0.0.0.0 bind accepts).
      const res = await fetch("http://127.0.0.1:" + String(priorPort) + "/discover.json");

      assert.equal(res.status, 200, "the tuner stays alive on its previous port after a rejected port change");
    } finally {

      await closeServer(blocker.server);
    }
  });
});
