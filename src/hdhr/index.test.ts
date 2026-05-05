/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.test.ts: Unit tests for the HDHomeRun emulation server lifecycle. Coverage focuses on the four observable behaviors of startHdhrServer / stopHdhrServer:
 * the disabled short-circuit, automatic DeviceID generation when missing or invalid, graceful EADDRINUSE handling on port collision, and idempotent shutdown.
 * Each test uses an OS-assigned port (port 0) so it never collides with the production HDHR port; data-directory side effects are routed into a per-test temp
 * dir so persistence calls inside startHdhrServer cannot leak to the user's real ~/.prismcast directory.
 */
import { afterEach, beforeEach, describe, test } from "node:test";
import { startHdhrServer, stopHdhrServer } from "./index.ts";
import { CONFIG } from "../config/index.ts";
import type { Server } from "node:http";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { generateDeviceId } from "./deviceId.ts";
import { initializeDataDir } from "../config/paths.ts";
import { withTempDir } from "../testing.helpers.ts";

// snapshotConfig captures the parts of CONFIG that startHdhrServer reads or writes so each test can restore the prior values verbatim. Saving the whole CONFIG
// object would over-protect; the targeted snapshot makes intent explicit.
function snapshotConfig(): { deviceId: string; enabled: boolean; port: number } {

  return { deviceId: CONFIG.hdhr.deviceId, enabled: CONFIG.hdhr.enabled, port: CONFIG.hdhr.port };
}

function restoreConfig(prior: { deviceId: string; enabled: boolean; port: number }): void {

  CONFIG.hdhr.deviceId = prior.deviceId;
  CONFIG.hdhr.enabled = prior.enabled;
  CONFIG.hdhr.port = prior.port;
}

// listenOnEphemeral reserves a real port by listening on it without serving anything; used to force EADDRINUSE in the collision test. We close the port in the
// test's afterEach to keep the OS sockets clean.
function listenOnEphemeral(): Promise<{ port: number; server: Server }> {

  return new Promise((resolve, reject) => {

    const server = createServer();

    server.listen(0, "127.0.0.1", () => {

      const address = server.address();

      if((typeof address !== "object") || (address === null)) {

        reject(new Error("Failed to obtain ephemeral port"));

        return;
      }

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

describe("startHdhrServer - disabled", () => {

  let prior: { deviceId: string; enabled: boolean; port: number };

  beforeEach(() => {

    prior = snapshotConfig();
  });

  afterEach(() => {

    stopHdhrServer();
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
    assert.doesNotThrow(stopHdhrServer);
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

  let prior: { deviceId: string; enabled: boolean; port: number };

  beforeEach(() => {

    prior = snapshotConfig();
  });

  afterEach(() => {

    stopHdhrServer();
    restoreConfig(prior);
  });

  test("starts the HTTP server on an OS-assigned port without throwing", async () => {

    // Port 0 lets the OS pick a free port. We can't observe the chosen port from the public API (it's stored in module-level state), but we can confirm that
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

  let prior: { deviceId: string; enabled: boolean; port: number };
  let blocker: Server | null = null;

  beforeEach(() => {

    prior = snapshotConfig();
  });

  afterEach(async () => {

    stopHdhrServer();

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
    assert.doesNotThrow(stopHdhrServer);
  });
});

describe("stopHdhrServer", () => {

  let prior: { deviceId: string; enabled: boolean; port: number };

  beforeEach(() => {

    prior = snapshotConfig();
  });

  afterEach(() => {

    stopHdhrServer();
    restoreConfig(prior);
  });

  test("is a no-op when called before any server was started", () => {

    // The function checks the module-level reference for null; a fresh test run has no server, so the call must not throw.
    assert.doesNotThrow(stopHdhrServer);
  });

  test("is idempotent when called twice in a row", async () => {

    CONFIG.hdhr.enabled = true;
    CONFIG.hdhr.deviceId = generateDeviceId();
    CONFIG.hdhr.port = 0;

    await startHdhrServer();

    stopHdhrServer();

    // The reference is cleared on the first call; the second must observe null and short-circuit.
    assert.doesNotThrow(stopHdhrServer, "second stop is a safe no-op");
  });

  test("allows a fresh start after a stop", async () => {

    CONFIG.hdhr.enabled = true;
    CONFIG.hdhr.deviceId = generateDeviceId();
    CONFIG.hdhr.port = 0;

    await startHdhrServer();
    stopHdhrServer();

    // After a stop, the module is back to the no-server state; a second start should succeed identically.
    await assert.doesNotReject(() => startHdhrServer(), "restart after stop must succeed");
  });
});
