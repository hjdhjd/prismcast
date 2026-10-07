/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.test.ts: Unit tests for the HDHomeRun emulation server lifecycle and its live-apply config-change handler. Coverage spans the observable behaviors
 * of startHdhrServer / stopHdhrServer - the disabled short-circuit, automatic DeviceID generation when missing or invalid, graceful EADDRINUSE handling on
 * port collision, and shutdown that is safe to call more than once - plus applyHdhrConfigChanges, which realizes the candidate it is handed and returns only
 * the rejections the surfaces earned, including a save to an already-occupied port driven end to end through saveConfiguration.
 * Each test uses an OS-assigned or freshly reserved port so it never collides with the production HDHR port; data-directory side effects are routed into a
 * per-test temp dir so persistence calls inside startHdhrServer cannot leak to the user's real ~/.prismcast directory.
 */
import { CONFIG, initializeConfiguration, saveConfiguration } from "../config/index.ts";
import { afterEach, beforeEach, describe, test } from "node:test";
import { applyHdhrConfigChanges, startHdhrServer, stopHdhrServer } from "./index.ts";
import type { Config } from "../types/index.ts";
import type { ConfigChange } from "../config/reactivity.ts";
import type { ConfigStore } from "../config/index.ts";
import type { Server } from "node:http";
import type { UserConfig } from "../config/userConfig.ts";
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

  test("leaves a valid DeviceID untouched while the surface is disabled", async () => {

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

  test("is a no-op when called twice in a row", async () => {

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

/* The handler rows hold CONFIG.hdhr apart from the candidate they hand the handler, so each one proves the handler realizes the state it is handed rather than
 * the running configuration, and that it commits nothing itself - the reconcile commits what a handler did not refuse. Each row re-initializes CONFIG from an
 * in-memory store holding a valid DeviceID with LAN discovery off, because binding UDP 65001 in a test risks colliding with a real HDHomeRun on the network.
 */
describe("applyHdhrConfigChanges - live-apply handler", () => {

  const validDeviceId = generateDeviceId();

  /**
   * Builds an in-memory config store whose mutations apply to the file it holds, so a row can drive a real save through it.
   * @param initial - The file the store starts with.
   * @returns The store.
   */
  function memoryStore(initial: UserConfig): ConfigStore {

    let file = structuredClone(initial);

    return {

      mutateConfig: async (fn): Promise<void> => {

        const working = structuredClone(file);

        fn(working);
        file = working;
      },
      readConfig: async () => ({ config: structuredClone(file), parseError: false, readError: false })
    };
  }

  /**
   * Builds the candidate running configuration a save would hand the handler: CONFIG with the given HDHomeRun fields applied.
   * @param hdhr - The HDHomeRun fields the candidate changes.
   * @returns The candidate.
   */
  function candidate(hdhr: Partial<Config["hdhr"]>): Config {

    const next = structuredClone(CONFIG);

    Object.assign(next.hdhr, hdhr);

    return next;
  }

  // makeChange constructs a synthetic ConfigChange. The path drives the dispatch; the handler reads the values it realizes from the candidate it is handed.
  function makeChange(path: string): ConfigChange {

    return { current: null, path, previous: null };
  }

  // Reserves a port that is currently free by listening on it and closing the listener, so a row can bind a known number.
  async function reserveFreePort(host = "127.0.0.1"): Promise<number> {

    const reserved = await listenOnEphemeral(host);

    await closeServer(reserved.server);

    return reserved.port;
  }

  beforeEach(async () => {

    await initializeConfiguration(undefined, memoryStore({ hdhr: { deviceId: validDeviceId, discoveryEnabled: false, enabled: false } }));
  });

  afterEach(async () => {

    await stopHdhrServer();
  });

  test("enabling brings the HTTP surface up on the candidate's port, returns no rejection, and commits nothing itself", async () => {

    const port = await reserveFreePort();

    const rejections = await applyHdhrConfigChanges([makeChange("hdhr.enabled")], candidate({ enabled: true, port }));

    assert.deepEqual(rejections, []);
    assert.equal((await fetch("http://127.0.0.1:" + String(port) + "/discover.json")).status, 200, "the surface answers on the candidate's port");
    assert.equal(CONFIG.hdhr.enabled, false, "the handler realized the candidate without committing it");
  });

  test("disabling brings the HTTP surface down even while CONFIG still says enabled", async () => {

    const port = await reserveFreePort();

    CONFIG.hdhr.enabled = true;
    CONFIG.hdhr.port = port;
    await startHdhrServer();

    assert.equal((await fetch("http://127.0.0.1:" + String(port) + "/discover.json")).status, 200, "precondition: the surface is up");

    const rejections = await applyHdhrConfigChanges([makeChange("hdhr.enabled")], candidate({ enabled: false }));

    assert.deepEqual(rejections, []);
    await assert.rejects(fetch("http://127.0.0.1:" + String(port) + "/discover.json"), "the surface no longer answers");
    assert.equal(CONFIG.hdhr.enabled, true, "the handler committed nothing");
  });

  test("a port change rebinds the HTTP surface on the candidate's port", async () => {

    const initialPort = await reserveFreePort();

    CONFIG.hdhr.enabled = true;
    CONFIG.hdhr.port = initialPort;
    await startHdhrServer();

    const newPort = await reserveFreePort();
    const rejections = await applyHdhrConfigChanges([makeChange("hdhr.port")], candidate({ enabled: true, port: newPort }));

    assert.deepEqual(rejections, []);

    const res = await fetch("http://127.0.0.1:" + String(newPort) + "/discover.json");
    const body = await res.json() as Record<string, unknown>;

    assert.equal(res.status, 200, "the rebound surface answers on the candidate's port");
    assert.equal(body["DeviceID"], CONFIG.hdhr.deviceId.toUpperCase());
    assert.equal(CONFIG.hdhr.port, initialPort, "the handler committed nothing");
  });

  test("the fields read where they are used, and discovery while the surface is disabled, return no rejection", async () => {

    const rejections = await applyHdhrConfigChanges([ makeChange("hdhr.discoveryEnabled"), makeChange("hdhr.friendlyName"), makeChange("hdhr.someFutureField") ],
      candidate({ discoveryEnabled: true, friendlyName: "Den" }));

    assert.deepEqual(rejections, []);
  });

  test("an enabled surface replaces a DeviceID that failed its checksum with a generated one and refuses the saved id", async () => {

    await withTempDir(async (dir) => {

      // The generated id is persisted through the module's own store, which needs a data directory.
      initializeDataDir(dir);

      const port = await reserveFreePort();
      const rejections = await applyHdhrConfigChanges([makeChange("hdhr.deviceId")], candidate({ deviceId: "10000000", enabled: true, port }));

      assert.deepEqual(rejections, [{ path: "hdhr.deviceId", reason: "The saved HDHomeRun DeviceID failed its checksum, so a newly generated DeviceID replaced it." }]);
      assert.notEqual(CONFIG.hdhr.deviceId, "10000000", "the invalid id never reached CONFIG");
      assert.notEqual(CONFIG.hdhr.deviceId, validDeviceId, "a fresh id was generated in its place");
      assert.match(CONFIG.hdhr.deviceId, /^[0-9a-f]{8}$/);
    });
  });

  test("a disabled surface refuses a DeviceID that failed its checksum and keeps the running id", async () => {

    const rejections = await applyHdhrConfigChanges([makeChange("hdhr.deviceId")], candidate({ deviceId: "10000000", enabled: false }));

    assert.deepEqual(rejections, [{ path: "hdhr.deviceId", reason: "The saved HDHomeRun DeviceID failed its checksum, so the running DeviceID was kept." }]);
    assert.equal(CONFIG.hdhr.deviceId, validDeviceId);
  });

  test("a save to an occupied port is rejected, CONFIG keeps the bound port, and the documents advertise it", async () => {

    // Every listener this row opens binds the host the HDHR server uses (CONFIG.server.host, default "0.0.0.0") so the conflict is a real exact-address
    // collision - a 0.0.0.0 bind and a 127.0.0.1 listener on the same port coexist under SO_REUSEADDR and would not collide.
    const hdhrHost = CONFIG.server.host;
    const boundPort = await reserveFreePort(hdhrHost);
    const store = memoryStore({ hdhr: { deviceId: validDeviceId, discoveryEnabled: false, enabled: true, port: boundPort } });

    await initializeConfiguration(undefined, store);
    await startHdhrServer();

    const blocker = await listenOnEphemeral(hdhrHost);

    try {

      const result = await saveConfiguration((current) => {

        current.hdhr = { ...current.hdhr, port: blocker.port };
      }, store);

      assert.deepEqual(result.rejected, [{ change: { current: blocker.port, path: "hdhr.port", previous: boundPort },
        reason: "HDHomeRun could not bind port " + String(blocker.port) + ", so the change was not applied." }]);
      assert.equal(CONFIG.hdhr.port, boundPort, "the refused port never reached the running configuration");

      const res = await fetch("http://127.0.0.1:" + String(boundPort) + "/discover.json");
      const body = await res.json() as Record<string, unknown>;

      assert.equal(res.status, 200, "the tuner answers on the bound port after the refusal");
      assert.equal(body["BaseURL"], "http://127.0.0.1:" + String(boundPort), "the advertised BaseURL names the bound port");
    } finally {

      await closeServer(blocker.server);
    }
  });
});
