/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.test.ts: Unit tests for the HDHomeRun emulation server lifecycle and its live-apply config-change handler. Coverage spans the observable behaviors
 * of startHdhrServer / stopHdhrServer - the disabled short-circuit, an OS-assigned port advertised as bound, the advertised DeviceID the boot generated when
 * the stored one was missing or invalid, a route that throws answered through the request error handler, a boot onto an occupied port and a bind error of
 * another kind each logged with no server announced, and shutdown that is safe to call more than once - plus applyHdhrConfigChanges, which realizes the
 * candidate it is handed and returns only the rejections the surfaces earned, including an enable onto an occupied port, a save to an already-occupied port
 * driven end to end through saveConfiguration and a port change that binds the requested port before it closes the bound one. LAN discovery is driven on
 * controllers of the suite's own, each built by one helper that binds the responder on the loopback address, so no row binds the protocol's discovery port on
 * every address, where it would answer real discovery on the network; every describe that drives the module's own controller keeps discovery off. Each test
 * uses an OS-assigned or freshly reserved port so it never collides with the production HDHR port, and every configuration a row boots or saves goes through an
 * in-memory store double, so nothing reaches the user's real ~/.prismcast directory.
 */
import { CONFIG, initializeConfiguration, saveConfiguration } from "../config/index.ts";
import type { Config, HdhrConfig } from "../types/index.ts";
import { LOG, generateDeviceId, validateDeviceId } from "../utils/index.ts";
import { afterEach, beforeEach, describe, test } from "node:test";
import { applyHdhrConfigChanges, createHdhrConfigHandler, createHdhrController, startHdhrServer, stopHdhrServer } from "./index.ts";
import { makeDiscoverRequest, readBaseUrl, sendAndReceive } from "./protocol.helpers.ts";
import type { ConfigChange } from "../config/reactivity.ts";
import { HDHR_DISCOVERY_PORT } from "./udp.ts";
import { HDHR_WILDCARD } from "./protocol.ts";
import type { HdhrController } from "./index.ts";
import type { LogEntry } from "../utils/logEmitter.ts";
import type { MemoryConfigStore } from "../config/index.helpers.ts";
import type { Server } from "node:http";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { createSocket } from "node:dgram";
import { makeMemoryConfigStore } from "../config/index.helpers.ts";
import { subscribeToLogs } from "../utils/logEmitter.ts";

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

/**
 * Runs an action while capturing every log entry it emits, so a row can read which servers announced a confirmed bind during it.
 * @param action - The action to run.
 * @returns The action's result and the captured entries.
 */
async function withLogCapture<T>(action: () => Promise<T>): Promise<{ entries: LogEntry[]; result: T }> {

  const entries: LogEntry[] = [];
  const unsubscribe = subscribeToLogs((entry: LogEntry) => {

    entries.push(entry);
  });

  try {

    return { entries, result: await action() };
  } finally {

    unsubscribe();
  }
}

// The listening lines among captured entries: the line a server logs once its bind is confirmed, naming the host, the bound port and the DeviceID.
function listeningLines(entries: readonly LogEntry[]): string[] {

  return entries.filter((entry) => entry.message.startsWith("HDHomeRun emulation is now listening on ")).map((entry) => entry.message);
}

// The listening line a server bound on the HDHR host at the given port logs for the given DeviceID.
function listeningLine(port: number, deviceId: string): string {

  return "HDHomeRun emulation is now listening on " + CONFIG.server.host + ":" + String(port) + " (DeviceID: " + deviceId.toUpperCase() + ").";
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

// The UDP bind every controller this suite builds takes: the loopback address on an OS-assigned port, so no row binds the discovery protocol's port on every
// address, where its responder would answer real LAN discovery.
const LOOPBACK_DISCOVERY = { udp: { bindAddress: "127.0.0.1", port: 0 } } as const;

/**
 * Builds a controller for a discovery row, with the desired state the row reconciles: the emulation and LAN discovery turned on over CONFIG's HDHomeRun state, on
 * the HTTP port the row names. The controller's responder binds the loopback address, on an OS-assigned port unless the row names a loopback port it holds so
 * the bind fails. This is the one place the suite builds a controller and the one place it asks for discovery.
 * @param occupiedUdpPort - A loopback UDP port the row holds, for the row whose responder bind must fail.
 * @returns The controller and the desired state's builder.
 */
function createLoopbackController(occupiedUdpPort?: number): { controller: HdhrController; desired: (httpPort: number) => HdhrConfig } {

  const options = (occupiedUdpPort === undefined) ? LOOPBACK_DISCOVERY : { udp: { ...LOOPBACK_DISCOVERY.udp, port: occupiedUdpPort } };

  return {

    controller: createHdhrController(options),
    desired: (httpPort: number): HdhrConfig => ({ ...CONFIG.hdhr, discoveryEnabled: true, enabled: true, port: httpPort })
  };
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

    // With emulation disabled, startHdhrServer returns without starting a server. It returns void in either path, so the row asserts that it resolves without
    // throwing and that stopHdhrServer afterward is a safe no-op.
    await startHdhrServer();

    // stopHdhrServer must be a safe no-op when no server was started.
    await assert.doesNotReject(stopHdhrServer);
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

  test("starts the HTTP server on an OS-assigned port and advertises the port the OS assigned", async () => {

    // Port 0 lets the OS pick a free port. The controller's HTTP surface owns the chosen port, so the row reads it from the listening line, which names the port
    // from the server's own address, and the documents must advertise that same port while CONFIG still names 0.
    await initializeConfiguration(undefined, makeMemoryConfigStore({ hdhr: { deviceId: generateDeviceId(), discoveryEnabled: false, enabled: true, port: 0 } }));

    assert.equal(CONFIG.hdhr.port, 0, "precondition: CONFIG names port 0");

    const { entries } = await withLogCapture(() => startHdhrServer());
    const [line] = listeningLines(entries);
    const port = Number(/:(\d+) \(DeviceID/.exec(line ?? "")?.[1]);

    assert.ok(port > 0, "the listening line names the port the OS assigned: " + String(line));

    const body = await (await fetch("http://127.0.0.1:" + String(port) + "/discover.json")).json() as Record<string, unknown>;

    assert.equal(body["BaseURL"], "http://127.0.0.1:" + String(port), "the documents advertise the assigned port, not the port CONFIG names");
  });

  test("a route that throws is logged and answered with a sentence by the request error handler", async (t) => {

    CONFIG.hdhr.enabled = true;
    CONFIG.hdhr.deviceId = generateDeviceId();
    CONFIG.hdhr.port = 0;

    const { entries } = await withLogCapture(() => startHdhrServer());
    const port = Number(/:(\d+) \(DeviceID/.exec(listeningLines(entries)[0] ?? "")?.[1]);
    const error = t.mock.method(LOG, "error", () => { /* Captured via the mock. */ });

    // discover.json upper-cases the running DeviceID per request, so a DeviceID that is not a string makes that route throw once the server is up. Express's own
    // fallback would answer with its default page, so the sentence in the body is the shared handler's.
    CONFIG.hdhr.deviceId = null as unknown as string;

    const res = await fetch("http://127.0.0.1:" + String(port) + "/discover.json");

    assert.equal(res.status, 500, "the throwing route answered 500");
    assert.equal(await res.text(), "Internal server error.", "the body is the request error handler's sentence");
    assert.deepEqual(error.mock.calls.map((call) => call.arguments[0]), ["A request failed with an unhandled error."], "the failure was logged once");
  });

  /**
   * Boots the configuration from a file holding the given DeviceID with the emulation enabled, starts the surface on an OS-assigned port, and answers the
   * listening line it logged with the port that line names. The boot runs on the file's default port, because port 0 is no port the configuration accepts, and
   * the row points the running configuration at port 0 only once the boot has written its correction.
   * @param deviceId - The DeviceID the stored file holds.
   * @returns The store the boot wrote through, and the listening line with its port.
   */
  async function bootAndStart(deviceId: string): Promise<{ line: string | undefined; port: number; store: MemoryConfigStore }> {

    const store = makeMemoryConfigStore({ hdhr: { deviceId, discoveryEnabled: false, enabled: true } });

    await initializeConfiguration(undefined, store);

    CONFIG.hdhr.port = 0;

    const { entries } = await withLogCapture(() => startHdhrServer());
    const [line] = listeningLines(entries);

    return { line, port: Number(/:(\d+) \(DeviceID/.exec(line ?? "")?.[1]), store };
  }

  test("regenerates the DeviceID when the existing one is empty", async () => {

    // The configuration layer generates the DeviceID as the boot loads a file that holds none, so the surface advertises the id the boot stored.
    const { line, port, store } = await bootAndStart("");

    assert.ok(validateDeviceId(CONFIG.hdhr.deviceId), "the running DeviceID passes its checksum");
    assert.notEqual(CONFIG.hdhr.deviceId, "", "a DeviceID was generated");
    assert.equal(line, listeningLine(port, CONFIG.hdhr.deviceId), "the listening line names the running DeviceID");
    assert.equal(store.file.hdhr?.deviceId, CONFIG.hdhr.deviceId, "the held file carries the running DeviceID");
  });

  test("regenerates the DeviceID when the existing one fails the checksum", async () => {

    // 10000000 has the right shape but a nonzero checksum, so the boot replaces it before the surface starts.
    const { line, port, store } = await bootAndStart("10000000");

    assert.ok(validateDeviceId(CONFIG.hdhr.deviceId), "the running DeviceID passes its checksum");
    assert.notEqual(CONFIG.hdhr.deviceId, "10000000", "the stored id was replaced");
    assert.equal(line, listeningLine(port, CONFIG.hdhr.deviceId), "the listening line names the running DeviceID");
    assert.equal(store.file.hdhr?.deviceId, CONFIG.hdhr.deviceId, "the held file carries the running DeviceID");
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

  test("a boot onto an occupied port logs the in-use warning, announces no listener, and leaves the surface unbound", async () => {

    // The blocker binds the host the HDHR server binds, CONFIG.server.host, so the collision is a real exact-address one: a listener on another address of the
    // same port could coexist with the server. The blocker answers no request, so the row reads the outcome from the log rather than from a fetch.
    const reserved = await listenOnEphemeral(CONFIG.server.host);

    blocker = reserved.server;
    CONFIG.hdhr.enabled = true;
    CONFIG.hdhr.deviceId = generateDeviceId();
    CONFIG.hdhr.port = reserved.port;

    const { entries } = await withLogCapture(() => startHdhrServer());

    assert.ok(entries.some((entry) => (entry.level === "warn") &&
      (entry.message === "HDHomeRun port " + String(reserved.port) + " is already in use. Check for conflicting services on this port.")),
    "the boot logged the in-use warning for the occupied port");
    assert.deepEqual(listeningLines(entries), [], "no server announced a listener");
    assert.ok(!entries.some((entry) => entry.message.includes("keeping the previous port")), "an unbound surface keeps no previous port");

    // The surface stayed down, so a stop has nothing to close.
    await assert.doesNotReject(stopHdhrServer);
  });

  test("a bind error other than an address in use logs the start failure and announces no listener", async () => {

    // 192.0.2.1 is TEST-NET-1, an address no interface carries, so the bind fails with EADDRNOTAVAIL rather than EADDRINUSE and reaches the other arm.
    const originalHost = CONFIG.server.host;

    CONFIG.hdhr.enabled = true;
    CONFIG.hdhr.deviceId = generateDeviceId();
    CONFIG.hdhr.port = 0;
    CONFIG.server.host = "192.0.2.1";

    try {

      const { entries } = await withLogCapture(() => startHdhrServer());

      assert.ok(entries.some((entry) => (entry.level === "warn") && entry.message.startsWith("Failed to start the HDHomeRun HTTP server: ")),
        "the boot logged the start failure");
      assert.deepEqual(listeningLines(entries), [], "no server announced a listener");
    } finally {

      CONFIG.server.host = originalHost;
    }
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
   * Builds the candidate running configuration a save would hand the handler: CONFIG with the given HDHomeRun fields applied.
   * @param hdhr - The HDHomeRun fields the candidate changes.
   * @returns The candidate.
   */
  function candidate(hdhr: Partial<Config["hdhr"]>): Config {

    const next = structuredClone(CONFIG);

    Object.assign(next.hdhr, hdhr);

    return next;
  }

  beforeEach(async () => {

    await initializeConfiguration(undefined, makeMemoryConfigStore({ hdhr: { deviceId: validDeviceId, discoveryEnabled: false, enabled: false } }));
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

  test("enabling onto an occupied port refuses hdhr.enabled", async () => {

    // The blocker binds the host the HDHR server binds, for the reason the occupied-port rows below state, so the enable meets a real collision.
    const blocker = await listenOnEphemeral(CONFIG.server.host);

    try {

      const rejections = await applyHdhrConfigChanges([makeChange("hdhr.enabled")], candidate({ enabled: true, port: blocker.port }));

      assert.deepEqual(rejections, [{ path: "hdhr.enabled", reason: "HDHomeRun could not bind port " + String(blocker.port) + ", so the change was not applied." }]);
    } finally {

      await closeServer(blocker.server);
    }
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

  test("a port change answers on the candidate's port and advertises it, and the previous port refuses connections once the new one answers", async () => {

    const initialPort = await reserveFreePort();

    CONFIG.hdhr.enabled = true;
    CONFIG.hdhr.port = initialPort;
    await startHdhrServer();

    // The candidate carries a DeviceID of its own, held apart from the running one, so the listening line shows the surface names the DeviceID it is handed.
    const candidateId = generateDeviceId();
    const newPort = await reserveFreePort();

    assert.notEqual(candidateId, CONFIG.hdhr.deviceId, "precondition: the candidate's DeviceID differs from the running one");

    const { entries, result } = await withLogCapture(() => applyHdhrConfigChanges([makeChange("hdhr.port")],
      candidate({ deviceId: candidateId, enabled: true, port: newPort })));

    assert.deepEqual(result, []);
    assert.deepEqual(listeningLines(entries), [listeningLine(newPort, candidateId)], "one server announced its bind: the new one, under the candidate's DeviceID");

    const res = await fetch("http://127.0.0.1:" + String(newPort) + "/discover.json");
    const body = await res.json() as Record<string, unknown>;

    assert.equal(res.status, 200, "the rebound surface answers on the candidate's port");
    assert.equal(body["DeviceID"], CONFIG.hdhr.deviceId.toUpperCase());
    assert.equal(CONFIG.hdhr.port, initialPort, "the handler committed nothing");
    assert.equal(body["BaseURL"], "http://127.0.0.1:" + String(newPort), "the documents advertise the bound port, not the port CONFIG names");
    await assert.rejects(fetch("http://127.0.0.1:" + String(initialPort) + "/discover.json"), "the previous port refuses connections once the new one answers");
  });

  test("a refused port change keeps the bound server answering and never closes it", async () => {

    // Every listener this row opens binds the host the HDHR server uses, for the reason the occupied-port row below states.
    const hdhrHost = CONFIG.server.host;
    const boundPort = await reserveFreePort(hdhrHost);

    CONFIG.hdhr.enabled = true;
    CONFIG.hdhr.port = boundPort;
    await startHdhrServer();

    const blocker = await listenOnEphemeral(hdhrHost);

    try {

      const { entries, result } = await withLogCapture(() => applyHdhrConfigChanges([makeChange("hdhr.port")],
        candidate({ enabled: true, port: blocker.port })));

      assert.deepEqual(result, [{ path: "hdhr.port", reason: "HDHomeRun could not bind port " + String(blocker.port) + ", so the change was not applied." }]);

      // A surface that closed the bound server before the attempt could keep the tuner up only by binding that port again, which announces the bind; a surface
      // that binds before it closes never lets go of the bound server, so no server announces anything during the refusal.
      assert.deepEqual(listeningLines(entries), [], "no listening line for the bound port during the refusal");
      assert.ok(entries.some((entry) => (entry.level === "warn") && entry.message.includes("keeping the previous port " + String(boundPort) + " active")),
        "the warning names the requested port unavailable and the bound port still active");
      assert.equal((await fetch("http://127.0.0.1:" + String(boundPort) + "/discover.json")).status, 200, "the bound port answers after the refusal");

      // The surface must still report the bound port as bound, the value the Discover reply advertises, so a candidate asking for that port again is already
      // realized and binds nothing. A surface that dropped its reference to the bound server would try to bind the port that server still holds, and fail.
      const again = await withLogCapture(() => applyHdhrConfigChanges([makeChange("hdhr.port")], candidate({ enabled: true, port: boundPort })));

      assert.deepEqual(again.result, [], "the surface still reports the previous port as bound after the refusal");
      assert.deepEqual(listeningLines(again.entries), [], "asking for the bound port again binds nothing");
    } finally {

      await closeServer(blocker.server);
    }
  });

  test("the fields read where they are used, and discovery while the surface is disabled, return no rejection", async () => {

    const rejections = await applyHdhrConfigChanges([ makeChange("hdhr.discoveryEnabled"), makeChange("hdhr.friendlyName"), makeChange("hdhr.someFutureField") ],
      candidate({ discoveryEnabled: true, friendlyName: "Den" }));

    assert.deepEqual(rejections, []);
  });

  test("a DeviceID change refuses nothing, and the enabled surface binds with the DeviceID the candidate carries", async () => {

    // The configuration layer corrects a DeviceID before any candidate reaches the handler, so the handler binds with the id it is handed and refuses none.
    const port = await reserveFreePort();
    const { entries, result } = await withLogCapture(() => applyHdhrConfigChanges([makeChange("hdhr.deviceId")],
      candidate({ deviceId: "10000000", enabled: true, port })));

    assert.deepEqual(result, [], "the handler refuses no DeviceID change");
    assert.deepEqual(listeningLines(entries), [listeningLine(port, "10000000")], "the listening line names the DeviceID the candidate carries");
  });

  test("a save to an occupied port is rejected, CONFIG keeps the bound port, and the documents advertise it", async () => {

    // Every listener this row opens binds the host the HDHR server uses (CONFIG.server.host, default "0.0.0.0") so the conflict is a real exact-address
    // collision - a 0.0.0.0 bind and a 127.0.0.1 listener on the same port coexist under SO_REUSEADDR and would not collide.
    const hdhrHost = CONFIG.server.host;
    const boundPort = await reserveFreePort(hdhrHost);
    const store = makeMemoryConfigStore({ hdhr: { deviceId: validDeviceId, discoveryEnabled: false, enabled: true, port: boundPort } });

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

/* LAN discovery under the reconcile, on controllers of the suite's own. Each row builds its controller through createLoopbackController, so the responder binds the
 * loopback address on an OS-assigned port, and tears it down in a finally, so no socket outlives the row. The module's own controller is never asked for
 * discovery here, and each configuration is initialized with discovery off, as every other describe keeps it.
 */
describe("createHdhrController - LAN discovery under the reconcile", () => {

  beforeEach(async () => {

    await initializeConfiguration(undefined, makeMemoryConfigStore({ hdhr: { deviceId: generateDeviceId(), discoveryEnabled: false, enabled: false } }));
  });

  test("brings discovery up only once the HTTP surface is bound, on a port other than the protocol's, and its Discover reply advertises the HTTP port",
    async () => {

      const { controller, desired } = createLoopbackController();
      const httpPort = await reserveFreePort();

      try {

        assert.equal(controller.udpBoundPort, null, "precondition: discovery is down before the reconcile");
        assert.deepEqual(await controller.reconcile(desired(httpPort)), { httpFailed: false, udpFailure: null }, "the HTTP and UDP surfaces reached the desired state");

        const udpPort = controller.udpBoundPort;

        assert.ok((typeof udpPort === "number") && (udpPort !== HDHR_DISCOVERY_PORT), "the responder is bound on a port other than the protocol's: " +
          String(udpPort));

        const baseUrl = readBaseUrl(await sendAndReceive(udpPort, makeDiscoverRequest(HDHR_WILDCARD, HDHR_WILDCARD)));

        assert.ok(baseUrl.endsWith(":" + String(httpPort)), "the Discover reply advertises the bound HTTP port: " + baseUrl);
      } finally {

        await controller[Symbol.asyncDispose]();
      }
    });

  test("refuses hdhr.discoveryEnabled when the UDP port its responder asks for is occupied, naming that port and the bind error", async () => {

    const blocker = createSocket({ reuseAddr: false, type: "udp4" });
    const bound = Promise.withResolvers<number>();

    blocker.bind(0, "127.0.0.1", () => {

      bound.resolve(blocker.address().port);
    });

    const occupiedPort = await bound.promise;
    const { controller, desired } = createLoopbackController(occupiedPort);

    try {

      const httpPort = await reserveFreePort();
      const next = structuredClone(CONFIG);

      next.hdhr = desired(httpPort);

      const rejections = await createHdhrConfigHandler(controller)([makeChange("hdhr.discoveryEnabled")], next);

      assert.deepEqual(rejections.map((rejection) => rejection.path), ["hdhr.discoveryEnabled"], "the one change refused is the discovery setting");
      assert.match(rejections[0]?.reason ?? "", new RegExp("^HDHomeRun LAN discovery could not bind UDP port " + String(occupiedPort) +
        " \\(.*EADDRINUSE.*\\), so the change was not applied\\.$"), "the refusal names the occupied port and the bind error");
      assert.equal(controller.udpBoundPort, null, "discovery stayed down");
      assert.equal((await fetch("http://127.0.0.1:" + String(httpPort) + "/discover.json")).status, 200, "the HTTP surface came up regardless");
    } finally {

      await controller[Symbol.asyncDispose]();

      const closed = Promise.withResolvers<true>();

      blocker.close(() => {

        closed.resolve(true);
      });

      await closed.promise;
    }
  });

  test("an HTTP surface whose bind is refused leaves discovery down", async () => {

    // The blocker binds the host the HDHR server binds, so the HTTP bind meets a real collision, and discovery, gated on a bound HTTP surface, never comes up.
    const blocker = await listenOnEphemeral(CONFIG.server.host);
    const { controller, desired } = createLoopbackController();

    try {

      const next = structuredClone(CONFIG);

      next.hdhr = desired(blocker.port);

      const rejections = await createHdhrConfigHandler(controller)([ makeChange("hdhr.enabled"), makeChange("hdhr.discoveryEnabled") ], next);

      assert.deepEqual(rejections, [{ path: "hdhr.enabled", reason: "HDHomeRun could not bind port " + String(blocker.port) + ", so the change was not applied." }],
        "only the HTTP-driving change was refused");
      assert.equal(controller.udpBoundPort, null, "discovery never came up behind an HTTP surface that is not bound");
    } finally {

      await controller[Symbol.asyncDispose]();
      await closeServer(blocker.server);
    }
  });
});
