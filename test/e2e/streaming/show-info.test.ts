/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * show-info.test.ts: Integration coverage for src/streaming/showInfo.ts. The module has no dependency-injection point for its Channels DVR HTTP calls - it
 * builds each URL from the DVR host and port that CONFIG and config.json hold, or from the ones a save names, and calls the global fetch - so this suite stubs
 * globalThis.fetch (routed purely by URL path, exactly the mechanism the co-located unit test showInfo.test.ts already uses) rather than standing up a bootStubServer.
 * The persistence subsystem (config.json, channels.json) and the stream registry are real, booted per test via createIntegrationContext, initializeConfiguration, and
 * initializePersistence, so the config round-trip and the registered-stream lookup are exercised end-to-end.
 *
 * Every internal function under test (updateShowNames, updateShowNamesForHost, getGuideShowNames, populateChannelLogos, and the config-change handler) is
 * module-private, so this suite drives them exclusively through setDvrHost, startShowInfoPolling, triggerShowNameUpdate, and saveConfiguration.
 * The behavior groups asserted:
 *
 *   A. Show-name resolution - recording-job precedence over the program guide, guide fallback when no recording matches, and stale-name clearing when
 *      neither source matches anymore.
 *   B. The DVR host as the running configuration holds it - the boot reads channelsDvr.host from config.json into CONFIG, where the poller's start finds it;
 *      setDvrHost writes only the host field, never touching channelsDvr.port, reaching config.json and CONFIG together through one process write, with the
 *      rule rejecting a colon in the host value asserted alongside; a save that changes the port clears the device mappings and repopulates the logos at the
 *      saved port; and a host change repopulates them from the saved host and clears nothing, so a host the cache already holds keeps its mappings.
 *   C. Two-tier channel logo population - the /devices endpoint's tier-1 logos land in the shared logo cache and are broadcast via the channelUpdate SSE
 *      event, normalized through normalizeLogoUrl. A tier-2 TMS station-name fallback is attempted for a channel with a station ID but no tier-1 logo.
 *
 * Device-mapping fixtures mirror PrismCast's actual channel lineup (built from getAllChannels() after initializePersistence, not a hand-picked subset) so
 * the >=80% channel-ID overlap gate in getDeviceMappings() passes deterministically regardless of how the predefined catalog evolves.
 *
 * Reset discipline: stopShowInfoPolling() unconditionally clears the poll interval, the logo refresh interval, the pending debounce timer, the show-name
 * cache, the device-mappings cache, and the shared channel logo cache - so calling it in afterEach fully resets module state even for tests that never started
 * polling. The DVR host is the running configuration's, so each row resets it by calling initializeConfiguration() against its own data directory before
 * initializePersistence. Every population - a setDvrHost write, a poller start with a host known, a save that changes the host or the port - fires
 * populateChannelLogos as an un-awaited side effect; buildFullDevice bounds that sweep to at most a couple of network round trips against the test's own stub, and
 * afterEach gives it a brief grace window before restoring globalThis.fetch to the real implementation, so no straggling call can resolve against a live network address
 * once the stub is gone. Any registered stream and status subscription are also torn down so no state leaks into the next test.
 */
import { CONFIG, initializeConfiguration, saveConfiguration } from "../../../src/config/index.ts";
import { TestClock, settle } from "homebridge-plugin-utils/testing";
import { afterEach, beforeEach, describe, test } from "node:test";
import { closePuppeteerStreamWssOnIdle, firstOf, nthOf } from "../../../src/testing.helpers.ts";
import { createIntegrationContext, initializePersistence, readPersistedJson, writePersistedJson } from "../../helpers/integration.helpers.ts";
import { getAllChannels, getChannelListing, getChannelStationId } from "../../../src/config/userChannels.ts";
import { getDvrHost, getShowName, setDvrHost, startShowInfoPolling, stopShowInfoPolling, triggerShowNameUpdate } from "../../../src/streaming/showInfo.ts";
import { registerStream, unregisterStream } from "../../../src/streaming/registry.ts";
import assert from "node:assert/strict";
import { delay } from "../../../src/utils/delay.ts";
import { makeRegistryEntry } from "../../../src/streaming/registry.helpers.ts";
import { subscribeToStatus } from "../../../src/streaming/statusEmitter.ts";

// test/helpers/integration.helpers.ts imports setupRoutes from src/routes/index.ts, which transitively pulls in puppeteer-stream (via browser/); that package
// IIFE-spawns a long-lived WebSocketServer at module load that is never unref'd. Scheduling the cleanup on a 0ms unref'd timer here lets this suite exit
// cleanly on its own even when run outside the npm test:integration script (which otherwise masks the same handle via --test-force-exit).
closePuppeteerStreamWssOnIdle();

// Types.

/**
 * Fixture shape for a single channel entry inside a /devices response, mirroring the wire fields showInfo.ts reads (GuideNumber, ID, optional Logo).
 */
interface FixtureChannel {

  GuideNumber: string;
  ID: string;
  Logo?: string;
}

/**
 * Fixture shape for a single /devices entry.
 */
interface FixtureDevice {

  Channels: FixtureChannel[];
  DeviceID: string;
  Provider: string;
}

/**
 * Fixture shape for a single /dvr/jobs entry.
 */
interface FixtureJob {

  Channel: string;
  DeviceID: string;
  Name: string;
}

/**
 * Fixture shape for a single airing inside a /devices/{id}/guide/now entry.
 */
interface FixtureAiring {

  Title: string;
}

/**
 * Fixture shape for a single /devices/{id}/guide/now entry.
 */
interface FixtureGuideEntry {

  Airings?: FixtureAiring[];
  Channel: { ChannelID: string };
}

/**
 * Fixture shape for a single /tms/stations/{name} result.
 */
interface FixtureTmsResult {

  preferredImage?: { uri?: string };
  stationId?: string;
}

/**
 * Mutable fixture state backing the stubbed fetch. Each test populates the subset of routes its scenario needs; unconfigured routes fall through to an
 * empty array, matching the real DVR's "no data" response shape.
 */
interface DvrStubState {

  devices: FixtureDevice[];
  guideByDevice: Map<string, FixtureGuideEntry[]>;
  jobs: FixtureJob[];

  // The host and port of every /devices request, in order, so a row can read which DVR address a device-mapping fetch reached.
  deviceAddresses: string[];

  // How many times the stub has served each URL path, so a row that drives the poll cadence can count the fetches a cadence produced rather than infer them
  // from a downstream effect.
  pathCalls: Map<string, number>;

  tmsByName: Map<string, FixtureTmsResult[]>;
}

/**
 * The device-fixture-plus-guide-number-lookup pair returned by buildFullDevice.
 */
interface FullDeviceFixture {

  device: FixtureDevice;
  guideNumberOf: Map<string, string>;
}

/**
 * Options for buildFullDevice's per-channel tier-1 logo assignment.
 */
interface BuildFullDeviceOptions {

  // Channel keys that must NOT receive a tier-1 Logo field, so a test can force populateChannelLogos' tier-2 TMS fallback for exactly those channels.
  excludeLogoFor?: Set<string>;

  // Per-channel logo URL overrides. A channel not present here (and not in excludeLogoFor) receives the baseline logo URL.
  logoOverrides?: Map<string, string>;
}

// Helpers.

/**
 * Creates an empty DvrStubState. Tests populate the fields they need before registering the fetch stub.
 * @returns A fresh, empty stub state.
 */
function createDvrStubState(): DvrStubState {

  return { deviceAddresses: [], devices: [], guideByDevice: new Map(), jobs: [], pathCalls: new Map(), tmsByName: new Map() };
}

/**
 * Wraps a JSON-serializable body in a 200 Response, matching the shape showInfo.ts's fetchFromDvr expects (response.ok, response.json()).
 * @param body - The value to serialize as the response body.
 * @returns A Response carrying the JSON-encoded body.
 */
function jsonResponse(body: unknown): Response {

  return new Response(JSON.stringify(body), { status: 200 });
}

/**
 * Installs a globalThis.fetch stub that routes requests purely by URL path against the given state - showInfo.ts always builds "http://" + host + ":" + port +
 * path, and the stub answers whatever host and port a request names. It records the address of each /devices request, which is how a row reads the DVR a
 * device-mapping fetch reached.
 * @param state - The mutable fixture state the stub reads from on every call.
 */
function installDvrFetchStub(state: DvrStubState): void {

  globalThis.fetch = (async (input: Request | URL | string): Promise<Response> => {

    // The implementation always passes a plain URL string, but fetch's type union admits Request and URL too. We narrow explicitly, mirroring the
    // co-located unit test's pattern, so eslint's no-base-to-string rule never fires on a code path fetchFromDvr never exercises.
    const urlStr = (typeof input === "string") ? input : (input instanceof URL ? input.toString() : input.url);
    const url = new URL(urlStr);

    state.pathCalls.set(url.pathname, (state.pathCalls.get(url.pathname) ?? 0) + 1);

    if(url.pathname === "/devices") {

      state.deviceAddresses.push(url.host);

      return jsonResponse(state.devices);
    }

    if(url.pathname === "/dvr/jobs") {

      return jsonResponse(state.jobs);
    }

    const guideMatch = /^\/devices\/([^/]+)\/guide\/now$/.exec(url.pathname);

    if(guideMatch) {

      const deviceId = decodeURIComponent(nthOf(guideMatch, 1, "guide route device id capture group"));

      return jsonResponse(state.guideByDevice.get(deviceId) ?? []);
    }

    const tmsMatch = /^\/tms\/stations\/(.+)$/.exec(url.pathname);

    if(tmsMatch) {

      const channelName = decodeURIComponent(nthOf(tmsMatch, 1, "tms route channel name capture group"));

      return jsonResponse(state.tmsByName.get(channelName) ?? []);
    }

    return jsonResponse([]);
  });
}

/**
 * Builds a /devices fixture whose Channels array mirrors PrismCast's full current lineup (every key from getAllChannels()), so the >=80% overlap gate in
 * getDeviceMappings() passes deterministically. Each channel is assigned a stable, unique GuideNumber (its index in insertion order); the returned map lets
 * callers look up a channel's GuideNumber to build matching /dvr/jobs fixtures.
 *
 * Every channel receives a tier-1 Logo field by default (a shared baseline URL, or its entry in logoOverrides), except channels listed in excludeLogoFor.
 * This matters beyond the logo tests themselves: populateChannelLogos' tier-2 pass only makes a network round trip for channels that reach it WITHOUT a
 * cached logo, so leaving tier 1 sparse (the naive "only set a Logo for the one channel this test cares about" approach) would force a sequential TMS fetch
 * for every other station-id-bearing channel in PrismCast's lineup on every population in this suite. Since populateChannelLogos is fired fire-and-forget (void), that
 * sweep can straggle past the test's own completion and, once afterEach restores globalThis.fetch to the real implementation, resolve against a live network call
 * and hold the process open for minutes. Defaulting every channel to a baseline logo bounds every test's tier-2 work to at most the handful of channels a test
 * deliberately excludes.
 * @param deviceId - The DeviceID to assign to the fabricated M3U device.
 * @param options - Per-channel logo shaping. See BuildFullDeviceOptions.
 * @returns The fabricated device fixture plus its channel-key-to-GuideNumber map.
 */
function buildFullDevice(deviceId: string, options: BuildFullDeviceOptions = {}): FullDeviceFixture {

  const excludeLogoFor = options.excludeLogoFor ?? new Set<string>();
  const logoOverrides = options.logoOverrides ?? new Map<string, string>();
  const channelKeys = Object.keys(getAllChannels());
  const guideNumberOf = new Map<string, string>();

  const channels: FixtureChannel[] = channelKeys.map((key, index) => {

    const guideNumber = String(index + 1);

    guideNumberOf.set(key, guideNumber);

    const channel: FixtureChannel = { GuideNumber: guideNumber, ID: key };

    if(!excludeLogoFor.has(key)) {

      channel.Logo = logoOverrides.get(key) ?? "https://tms-images.example.test/baseline-logo.png?size=360x270";
    }

    return channel;
  });

  return { device: { Channels: channels, DeviceID: deviceId, Provider: "m3u" }, guideNumberOf };
}

/**
 * Polls a predicate at short intervals until it returns true or the deadline passes. Used to observe the eventual effect of showInfo.ts's fire-and-forget
 * internal update chains (updateShowNames, populateChannelLogos) without coupling to their exact async shape. The predicate may be sync or async.
 * @param predicate - Condition to poll. May return a boolean directly or a Promise of one.
 * @param timeoutMs - Maximum time to poll before giving up.
 * @param label - Descriptive label folded into the timeout error so a failing wait points at what it was waiting for.
 */
async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs: number, label: string): Promise<void> {

  const deadline = Date.now() + timeoutMs;

  while(Date.now() < deadline) {

    // eslint-disable-next-line no-await-in-loop -- the predicate is awaited sequentially by design; the loop IS the polling drain (see the delay await below).
    if(await predicate()) {

      return;
    }

    // eslint-disable-next-line no-await-in-loop -- the loop semantically IS the polling drain, mirroring the established pattern in native-hls-proxy.test.ts.
    await delay(25);
  }

  throw new Error("waitFor timeout (" + String(timeoutMs) + "ms): " + label);
}

describe("showInfo: Channels DVR API integration (show names, DVR host persistence, two-tier logo population)", () => {

  let originalFetch: typeof globalThis.fetch;
  let activeStreamIds: number[];
  let unsubscribeStatus: (() => void) | null;

  beforeEach(() => {

    originalFetch = globalThis.fetch;
    activeStreamIds = [];
    unsubscribeStatus = null;
  });

  afterEach(async () => {

    // stopShowInfoPolling() runs unconditionally regardless of whether a given test ever started polling - its cache clears are the reset point for this
    // module's state, and the DVR host is reset per row by initializeConfiguration(). It runs first so no queued interval callback can fire against a stream we
    // are about to unregister below.
    stopShowInfoPolling();

    for(const id of activeStreamIds) {

      unregisterStream(id);
    }

    // Every population in this suite fires populateChannelLogos as an un-awaited side effect. buildFullDevice bounds that sweep to at most a couple of network round
    // trips against THIS test's own fetch stub, but we still give it a brief window to fully settle before swapping globalThis.fetch back to the real implementation
    // below - a straggler that raced past this point would otherwise resolve against a live network call for a fabricated test host, which is exactly the hang class this
    // bound (and this grace window) exists to prevent.
    await delay(300);

    globalThis.fetch = originalFetch;
    unsubscribeStatus?.();
  });

  describe("the poller's timers run on the injected clock", () => {

    /* The rows in this block drive the poller's cadence and debounce directly, which the rows below cannot: they observe fire-and-forget chains on real time by
     * design, where these assert WHEN a timer comes due. Each row hands startShowInfoPolling a virtual clock, so every cadence the poller owns runs on one
     * timeline the row advances explicitly, and the stub's per-path call counter is what makes a fetch that a cadence produced countable.
     */

    // The instant each row's virtual clock is seeded at, matching the baseline the sibling streaming suites anchor on.
    const BASE_TIME_MS = 1700000000000;

    // The poller's debounce window for triggered updates, from showInfo.ts's own TRIGGER_DEBOUNCE_MS. Restated here because the constant is module-private.
    const TRIGGER_DEBOUNCE_MS = 2000;

    test("polls at the start and once per interval on the injected clock, and stops polling once the poller is stopped", async () => {

      await using ctx = await createIntegrationContext();

      // The boot reads the host from the file, so the host is known when the poller starts and no population runs before the start: a write of the host would
      // start one, and the mappings it cached would leave the start's own population nothing to request.
      await writePersistedJson(ctx, "config.json", { channelsDvr: { host: "showinfo-test-clock1.example.invalid" } });
      await initializeConfiguration();
      await initializePersistence(ctx);

      const clock = new TestClock(BASE_TIME_MS);
      const state = createDvrStubState();

      installDvrFetchStub(state);

      const deviceId = "M3U-Prism-Test-Clock-1";
      const { device, guideNumberOf } = buildFullDevice(deviceId);

      state.devices = [device];

      const targetKey = firstOf(Object.keys(getAllChannels()), "visible channel key");
      const targetGuideNumber = guideNumberOf.get(targetKey);

      assert.ok(targetGuideNumber, "buildFullDevice must assign a GuideNumber to every channel key it processes");

      state.jobs = [{ Channel: targetGuideNumber, DeviceID: deviceId, Name: "Clocked Recording" }];

      const entry = makeRegistryEntry({ info: { lastPlaylistRequest: 0, storeKey: targetKey } });

      registerStream(entry);
      activeStreamIds.push(entry.id);

      startShowInfoPolling(clock);

      // The host is known when the poller starts, so the start also begins the logo population, whose /devices request is bounded on the poller's clock.
      assert.equal(clock.pending, 3, "the start armed the show-name poll, the logo refresh, and the population's request bound on the injected clock");

      // The start runs an immediate update, which is the first jobs fetch.
      await waitFor(() => (state.pathCalls.get("/dvr/jobs") ?? 0) >= 1, 5000, "the immediate update at start reaches the jobs endpoint");

      const afterStart = state.pathCalls.get("/dvr/jobs") ?? 0;

      // Crossing one poll interval on the injected clock runs exactly one further update.
      clock.advance(30000);
      await settle();

      await waitFor(() => (state.pathCalls.get("/dvr/jobs") ?? 0) > afterStart, 5000, "the poll interval fires a further update on the injected clock");

      const afterOneInterval = state.pathCalls.get("/dvr/jobs") ?? 0;

      stopShowInfoPolling();

      assert.equal(clock.pending, 0, "the stop drained every timer the poller held");

      clock.advance(30000);
      await settle();
      await delay(100);

      assert.equal(state.pathCalls.get("/dvr/jobs") ?? 0, afterOneInterval, "a stopped poller runs no further update when its clock advances");
    });

    test("debounces triggered updates so the latest trigger wins, and arms nothing once the poller is stopped", async () => {

      await using ctx = await createIntegrationContext();

      await initializeConfiguration();
      await initializePersistence(ctx);

      const clock = new TestClock(BASE_TIME_MS);
      const state = createDvrStubState();

      installDvrFetchStub(state);

      const deviceId = "M3U-Prism-Test-Clock-2";
      const { device, guideNumberOf } = buildFullDevice(deviceId);

      state.devices = [device];

      const targetKey = firstOf(Object.keys(getAllChannels()), "visible channel key");
      const targetGuideNumber = guideNumberOf.get(targetKey);

      assert.ok(targetGuideNumber, "buildFullDevice must assign a GuideNumber to every channel key it processes");

      state.jobs = [{ Channel: targetGuideNumber, DeviceID: deviceId, Name: "Debounced Recording" }];

      const entry = makeRegistryEntry({ info: { lastPlaylistRequest: 0, storeKey: targetKey } });

      registerStream(entry);
      activeStreamIds.push(entry.id);

      await setDvrHost("showinfo-test-clock2.example.invalid");
      startShowInfoPolling(clock);

      await waitFor(() => (state.pathCalls.get("/dvr/jobs") ?? 0) >= 1, 5000, "the immediate update at start reaches the jobs endpoint");

      const afterStart = state.pathCalls.get("/dvr/jobs") ?? 0;

      // Two triggers back to back register under one key, so the second replaces the first rather than stacking a second update behind it.
      triggerShowNameUpdate();

      assert.equal(clock.pending, 3, "the first trigger armed the debounce beside the two intervals");

      triggerShowNameUpdate();

      assert.equal(clock.pending, 3, "the second trigger replaced the first rather than arming a second debounce");

      clock.advance(TRIGGER_DEBOUNCE_MS);
      await settle();

      await waitFor(() => (state.pathCalls.get("/dvr/jobs") ?? 0) > afterStart, 5000, "the debounce fires one update once its window elapses");

      const afterDebounce = state.pathCalls.get("/dvr/jobs") ?? 0;

      assert.equal(afterDebounce, afterStart + 1, "the two triggers collapsed into exactly one further update");
      assert.equal(clock.pending, 2, "the fired debounce left only the two intervals armed");

      // A trigger after the poller has stopped has no registry to arm on, so it is a no-op rather than a timer that fires against a stopped poller.
      stopShowInfoPolling();
      triggerShowNameUpdate();

      assert.equal(clock.pending, 0, "a trigger raised after the stop arms nothing");

      clock.advance(TRIGGER_DEBOUNCE_MS);
      await settle();
      await delay(100);

      assert.equal(state.pathCalls.get("/dvr/jobs") ?? 0, afterDebounce, "the post-stop trigger never ran an update");
    });
  });

  describe("show-name resolution: recording-vs-guide precedence and stale-name clearing", () => {

    test("prefers the recording job's Name over the program guide's Title for the same channel", async () => {

      await using ctx = await createIntegrationContext();

      await initializeConfiguration();
      await initializePersistence(ctx);

      const state = createDvrStubState();

      installDvrFetchStub(state);

      const deviceId = "M3U-Prism-Test-A1";
      const { device, guideNumberOf } = buildFullDevice(deviceId);

      state.devices = [device];

      const channelKeys = Object.keys(getAllChannels());
      const targetKey = firstOf(channelKeys, "visible channel key");
      const targetGuideNumber = guideNumberOf.get(targetKey);

      assert.ok(targetGuideNumber, "buildFullDevice must assign a GuideNumber to every channel key it processes");

      // The recording and guide fixtures deliberately carry DIFFERENT titles for the same channel, so a precedence regression (guide winning instead of the
      // recording) would flip this assertion rather than passing regardless of which source won.
      state.jobs = [{ Channel: targetGuideNumber, DeviceID: deviceId, Name: "Live Recording Title" }];
      state.guideByDevice.set(deviceId, [{ Airings: [{ Title: "Guide Title (must be ignored)" }], Channel: { ChannelID: targetKey } }]);

      // clientAddress stays at the factory default of null so updateShowNames' discovery phase has nothing to iterate over; the DVR host is set directly
      // below via setDvrHost, which is the injection point the lookup phase actually keys off.
      const entry = makeRegistryEntry({ info: { lastPlaylistRequest: 0, storeKey: targetKey } });

      registerStream(entry);
      activeStreamIds.push(entry.id);

      await setDvrHost("showinfo-test-a1.example.invalid");
      startShowInfoPolling();

      await waitFor(() => getShowName(entry.id) !== "", 5000, "show name resolves for the registered stream");

      assert.equal(getShowName(entry.id), "Live Recording Title", "the active recording job's Name must win over the guide's Title for the same channel");
    });

    test("falls back to the program guide's first airing Title when no recording job matches the channel", async () => {

      await using ctx = await createIntegrationContext();

      await initializeConfiguration();
      await initializePersistence(ctx);

      const state = createDvrStubState();

      installDvrFetchStub(state);

      const deviceId = "M3U-Prism-Test-A2";
      const { device, guideNumberOf } = buildFullDevice(deviceId);

      state.devices = [device];

      const channelKeys = Object.keys(getAllChannels());
      const targetKey = nthOf(channelKeys, 0, "visible channel key");
      const otherKey = nthOf(channelKeys, 1, "visible channel key");
      const otherGuideNumber = guideNumberOf.get(otherKey);

      assert.ok(otherGuideNumber, "buildFullDevice must assign a GuideNumber to every channel key it processes");

      // The job matches an UNRELATED channel (not the target), proving the guide fallback fires because nothing genuinely matched, not merely because the
      // jobs list happened to be empty. Two airings are supplied for the target channel so the "first airing wins" contract is asserted, not just "guide wins".
      state.jobs = [{ Channel: otherGuideNumber, DeviceID: deviceId, Name: "Unrelated Recording" }];
      state.guideByDevice.set(deviceId, [{

        Airings: [ { Title: "Currently Airing Show" }, { Title: "Second Airing (must not be used)" } ],
        Channel: { ChannelID: targetKey }
      }]);

      const entry = makeRegistryEntry({ info: { lastPlaylistRequest: 0, storeKey: targetKey } });

      registerStream(entry);
      activeStreamIds.push(entry.id);

      await setDvrHost("showinfo-test-a2.example.invalid");
      startShowInfoPolling();

      await waitFor(() => getShowName(entry.id) !== "", 5000, "show name resolves for the registered stream");

      assert.equal(getShowName(entry.id), "Currently Airing Show",
        "the guide's FIRST airing Title is used when no recording job matches the channel");
    });

    test("clears a previously-resolved show name once neither a recording nor a guide entry matches the channel anymore", async () => {

      await using ctx = await createIntegrationContext();

      await initializeConfiguration();
      await initializePersistence(ctx);

      const state = createDvrStubState();

      installDvrFetchStub(state);

      const deviceId = "M3U-Prism-Test-A3";
      const { device, guideNumberOf } = buildFullDevice(deviceId);

      state.devices = [device];

      const channelKeys = Object.keys(getAllChannels());
      const targetKey = firstOf(channelKeys, "visible channel key");
      const targetGuideNumber = guideNumberOf.get(targetKey);

      assert.ok(targetGuideNumber, "buildFullDevice must assign a GuideNumber to every channel key it processes");

      // Drive an initial, known-non-empty show name in via the recording path.
      state.jobs = [{ Channel: targetGuideNumber, DeviceID: deviceId, Name: "Initial Recording Title" }];
      state.guideByDevice.set(deviceId, []);

      const entry = makeRegistryEntry({ info: { lastPlaylistRequest: 0, storeKey: targetKey } });

      registerStream(entry);
      activeStreamIds.push(entry.id);

      await setDvrHost("showinfo-test-a3.example.invalid");
      startShowInfoPolling();

      await waitFor(() => getShowName(entry.id) !== "", 5000, "initial show name resolves via the recording job");
      assert.equal(getShowName(entry.id), "Initial Recording Title", "sanity check: the initial show name reflects the seeded recording");

      // Flip both sources to empty for this channel and drive a fresh lookup pass via the debounced trigger (startShowInfoPolling is a no-op once a poll
      // interval already exists, so triggerShowNameUpdate is the function that forces a second updateShowNames pass within a single test).
      state.jobs = [];
      state.guideByDevice.set(deviceId, []);

      triggerShowNameUpdate();

      await waitFor(() => getShowName(entry.id) === "", 5000, "show name is cleared once neither source has a match");

      assert.equal(getShowName(entry.id), "",
        "a stream with no matching recording or guide entry must have its show name cleared, not stale-retained from the prior pass");
    });
  });

  describe("persisted DVR host round-trip", () => {

    test("the boot reads the persisted channelsDvr.host into the running configuration, and the poller's start populates the logos from it", async () => {

      await using ctx = await createIntegrationContext();

      await writePersistedJson(ctx, "config.json", { channelsDvr: { host: "1.2.3.4", port: 8089 } });
      await initializeConfiguration();
      await initializePersistence(ctx);

      // The host is known the moment the boot has read the file, before the poller starts.
      assert.equal(getDvrHost(), "1.2.3.4", "getDvrHost reflects the persisted channelsDvr.host after the boot");

      const state = createDvrStubState();

      installDvrFetchStub(state);

      // A fully-covered device fixture keeps the population's tier-2 TMS sweep bounded to zero network round trips (see buildFullDevice).
      state.devices = [buildFullDevice("M3U-Prism-Test-B1").device];

      startShowInfoPolling();

      await waitFor(() => state.deviceAddresses.length > 0, 5000, "the poller's start populates the logos from the running host");

      assert.deepEqual(state.deviceAddresses, ["1.2.3.4:8089"], "the device mappings are fetched from the running host and port");
    });

    test("setDvrHost persists only channelsDvr.host, leaving channelsDvr.port untouched, and rejects colon-bearing input", async () => {

      await using ctx = await createIntegrationContext();

      await initializeConfiguration();
      await initializePersistence(ctx);

      const state = createDvrStubState();

      installDvrFetchStub(state);

      // A fully-covered device fixture keeps the population setDvrHost's write starts bounded to zero tier-2 network round trips (see buildFullDevice); this
      // test asserts on the persisted config only, not on logos.
      state.devices = [buildFullDevice("M3U-Prism-Test-B2").device];

      // Seed a NON-DEFAULT port (the default is 8089) so a "clobbers port" regression - the host-only mutate widening to touch .port too - would be caught
      // by the readback below rather than accidentally matching the default value either way.
      await writePersistedJson(ctx, "config.json", { channelsDvr: { host: "0.0.0.0", port: 19191 } });

      await setDvrHost("5.6.7.8");

      assert.equal(CONFIG.channelsDvr.host, "5.6.7.8", "the running configuration holds the host once the call resolves");

      const persistedAfterHostChange = await readPersistedJson(ctx, "config.json") as { channelsDvr?: { host?: string; port?: number } };
      const dvrAfterHostChange = persistedAfterHostChange.channelsDvr;

      assert.ok(dvrAfterHostChange, "the persisted config carries a channelsDvr section after the host change");
      assert.equal(dvrAfterHostChange.host, "5.6.7.8", "setDvrHost writes the new host to config.json before it resolves");
      assert.equal(dvrAfterHostChange.port, 19191, "setDvrHost must not touch channelsDvr.port - the write is host-only");

      // A colon-bearing host must be rejected outright: no in-memory change, and, since setDvrHost returns before it writes, no write either. The call resolves
      // only once any write it made has landed, so the file read next shows whether the rejected input reached it.
      await setDvrHost("9.9.9.9:8089");

      assert.equal(getDvrHost(), "5.6.7.8", "a colon-bearing host is rejected; the previously accepted host is preserved in memory");

      const persistedAfterRejectedInput = await readPersistedJson(ctx, "config.json") as { channelsDvr?: { host?: string; port?: number } };
      const dvrAfterRejectedInput = persistedAfterRejectedInput.channelsDvr;

      assert.ok(dvrAfterRejectedInput, "the persisted config still carries a channelsDvr section after the rejected input");
      assert.equal(dvrAfterRejectedInput.host, "5.6.7.8", "a rejected colon-bearing host must not be persisted to disk either");
      assert.equal(dvrAfterRejectedInput.port, 19191, "channelsDvr.port remains untouched after the rejected input");
    });

    test("a save that changes the port clears the device mappings and repopulates the logos at the saved port, and one that changes the host repopulates " +
      "them from the saved host", async () => {

      /* The handler reads the address from the candidate the save hands it, while CONFIG still holds the previous one. The device-mapping cache is keyed by
       * host and was filled at the running port, so a port change clears it: the port change keeps the host, whose cached mappings are otherwise still fresh,
       * and only the cleared cache sends the population back to /devices, at the new port. A host change clears nothing, and the population reaches the saved
       * host because the cache holds no mappings for it yet. Each population starts its /devices request before the handler returns, which is when the running
       * configuration still holds the previous address, so a handler reading CONFIG would request the previous one.
       */
      await using ctx = await createIntegrationContext();

      await writePersistedJson(ctx, "config.json", { channelsDvr: { host: "showinfo-test-b3.example.invalid" } });
      await initializeConfiguration();
      await initializePersistence(ctx);

      const state = createDvrStubState();

      installDvrFetchStub(state);
      state.devices = [buildFullDevice("M3U-Prism-Test-B3").device];

      startShowInfoPolling();

      await waitFor(() => state.deviceAddresses.length === 1, 5000, "precondition: the start fetched the device mappings from the boot's address");

      const portSave = await saveConfiguration((config) => {

        config.channelsDvr ??= {};
        config.channelsDvr.port = 19191;
      });

      assert.deepEqual(portSave.applied.map((change) => change.path), ["channelsDvr.port"], "the port is realized live");
      assert.deepEqual(state.deviceAddresses, [ "showinfo-test-b3.example.invalid:8089", "showinfo-test-b3.example.invalid:19191" ],
        "the cleared cache sends the population back to /devices at the saved port");
      assert.equal(CONFIG.channelsDvr.port, 19191, "the running configuration takes the port once the reconcile commits");

      const hostSave = await saveConfiguration((config) => {

        config.channelsDvr ??= {};
        config.channelsDvr.host = "showinfo-test-b3b.example.invalid";
      });

      assert.deepEqual(hostSave.applied.map((change) => change.path), ["channelsDvr.host"], "the host is realized live");
      assert.deepEqual(hostSave.deferred, [], "and held for no restart");
      assert.equal(state.deviceAddresses.at(-1), "showinfo-test-b3b.example.invalid:19191", "the population reaches the saved host at the running port");
      assert.equal(getDvrHost(), "showinfo-test-b3b.example.invalid", "getDvrHost follows the save");
    });

    test("a host change keeps the device mappings cached for the host it leaves, so saving a second host and then the first again requests the first once",
      async () => {

        /* The poller's start fetches the first host's mappings into the cache. A save of a second host repopulates from it, a host the cache does not hold, and
         * a save of the first host again repopulates from the mappings its start cached, which a host change leaves in place, so /devices is requested at the
         * first host once. A handler that cleared the cache on every change would send that last population back to /devices at the first host.
         */
        await using ctx = await createIntegrationContext();

        await writePersistedJson(ctx, "config.json", { channelsDvr: { host: "showinfo-test-b4.example.invalid" } });
        await initializeConfiguration();
        await initializePersistence(ctx);

        const state = createDvrStubState();
        const firstHostRequests = (): number => state.deviceAddresses.filter((address) => address === "showinfo-test-b4.example.invalid:8089").length;

        installDvrFetchStub(state);
        state.devices = [buildFullDevice("M3U-Prism-Test-B4").device];

        startShowInfoPolling();

        await waitFor(() => state.deviceAddresses.length === 1, 5000, "precondition: the start fetched the device mappings from the boot's host");

        await saveConfiguration((config) => {

          config.channelsDvr ??= {};
          config.channelsDvr.host = "showinfo-test-b4b.example.invalid";
        });

        await waitFor(() => state.deviceAddresses.includes("showinfo-test-b4b.example.invalid:8089"), 5000,
          "precondition: the save of the second host repopulated from it");

        await saveConfiguration((config) => {

          config.channelsDvr ??= {};
          config.channelsDvr.host = "showinfo-test-b4.example.invalid";
        });

        // The population the last save started reads the cache before any request, so the window below is room for a request a cleared cache would make.
        await delay(200);

        assert.equal(getDvrHost(), "showinfo-test-b4.example.invalid", "the running host is the first host again");
        assert.equal(firstHostRequests(), 1, "the first host's mappings, cached at the start, survived the host change, so /devices was requested there once");
      });
  });

  describe("two-tier channel logo population and channelUpdate SSE emission", () => {

    test("emits a channelUpdate SSE payload carrying a normalized tier-1 (device) logo for a channel with a station id", async () => {

      await using ctx = await createIntegrationContext();

      await initializeConfiguration();
      await initializePersistence(ctx);

      const state = createDvrStubState();

      installDvrFetchStub(state);

      const channelKeys = Object.keys(getAllChannels());
      const targetKey = channelKeys.find((key) => getChannelStationId(key) !== undefined);

      assert.ok(targetKey, "at least one visible channel must carry a station id for this fixture to be meaningful");

      const rawLogoUrl = "https://tms-images.example.test/logo.png?width=360&height=270";
      const deviceId = "M3U-Prism-Test-C1";
      const { device } = buildFullDevice(deviceId, { logoOverrides: new Map([[ targetKey, rawLogoUrl ]]) });

      state.devices = [device];

      const capturedChannelUpdates: { logos?: Record<string, string> }[] = [];

      unsubscribeStatus = subscribeToStatus((event, data) => {

        if(event === "channelUpdate") {

          capturedChannelUpdates.push(data as { logos?: Record<string, string> });
        }
      });

      await setDvrHost("showinfo-test-c1.example.invalid");

      // populateChannelLogos emits only once, after BOTH the tier-1 device pass and the tier-2 TMS sweep complete. Every channel here already carries a
      // tier-1 logo (baseline or override, per buildFullDevice), so tier 2 needs zero network round trips - the generous timeout is headroom, not a budget
      // for a real sweep.
      await waitFor(() => capturedChannelUpdates.some((patch) => patch.logos?.[targetKey] !== undefined), 15000,
        "a channelUpdate SSE event carries the tier-1 logo for the target channel");

      const matching = capturedChannelUpdates.find((patch) => patch.logos?.[targetKey] !== undefined);

      assert.ok(matching, "at least one captured channelUpdate payload must carry the target channel's logo");
      assert.equal(matching.logos?.[targetKey], "https://tms-images.example.test/logo.png?h=48",
        "the logo URL's query parameters are replaced with the height-only sizing parameter (normalizeLogoUrl)");
    });

    test("falls back to a TMS station-name search for a channel with a station id but no tier-1 logo", async () => {

      await using ctx = await createIntegrationContext();

      await initializeConfiguration();
      await initializePersistence(ctx);

      const state = createDvrStubState();

      installDvrFetchStub(state);

      const listing = getChannelListing();
      const targetEntry = listing.find((entry) => getChannelStationId(entry.key) !== undefined);

      assert.ok(targetEntry, "at least one listing entry must carry a station id");

      const targetStationId = getChannelStationId(targetEntry.key);

      assert.ok(targetStationId, "the resolved station id must be defined for the chosen entry");

      const channelName = targetEntry.channel.name ?? targetEntry.key;
      const deviceId = "M3U-Prism-Test-C2";

      // Excluding the target channel from tier-1 coverage is what forces populateChannelLogos to fall through to the tier-2 TMS search for it; every other
      // channel still gets the baseline tier-1 logo so the sweep needs only this one network round trip.
      const { device } = buildFullDevice(deviceId, { excludeLogoFor: new Set([targetEntry.key]) });

      state.devices = [device];
      state.tmsByName.set(channelName,
        [{ preferredImage: { uri: "https://tms-images.example.test/tier2-logo.png?w=360" }, stationId: targetStationId }]);

      const capturedChannelUpdates: { logos?: Record<string, string> }[] = [];

      unsubscribeStatus = subscribeToStatus((event, data) => {

        if(event === "channelUpdate") {

          capturedChannelUpdates.push(data as { logos?: Record<string, string> });
        }
      });

      await setDvrHost("showinfo-test-c2.example.invalid");

      await waitFor(() => capturedChannelUpdates.some((patch) => patch.logos?.[targetEntry.key] !== undefined), 15000,
        "a channelUpdate SSE event carries the tier-2 (TMS) logo for the target channel");

      const matching = capturedChannelUpdates.find((patch) => patch.logos?.[targetEntry.key] !== undefined);

      assert.ok(matching, "at least one captured channelUpdate payload must carry the tier-2 logo");
      assert.equal(matching.logos?.[targetEntry.key], "https://tms-images.example.test/tier2-logo.png?h=48",
        "the TMS search result's logo URL is normalized to the height-only sizing parameter");
    });
  });
});
