/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * pretune.test.ts: Integration coverage for the pretune scheduling state machine in src/streaming/pretune.ts. The architectural unit under test is pretune's
 * decision logic - read the DVR job schedule, dedupe against active streams, schedule per-job timers within the horizon, cancel cleanly on stop. The HTTP
 * fetch layer to the DVR is not the boundary under test - it is data acquisition for the decision layer. We mock the data-acquisition seam (fetchFromDvr,
 * getDeviceMappings, getDvrHost) so the test feeds synthetic schedule data straight into the decision logic, and we mock initializeStream so the test can
 * observe pretune's go / no-go decisions without firing the real browser-launching capture path.
 *
 * Why mock.module + dynamic import. pretune.ts captures its imports at module load time; ESM bindings are read-only after the fact, so neither mock.method on
 * a namespace object nor monkey-patch-after-import would propagate into pretune's call sites. mock.module replaces the module's exports for all subsequent
 * imports, so dynamic-importing pretune.ts AFTER the mocks are registered is the canonical way to make the spies visible at the call site. Static-importing
 * pretune.ts in this file would resolve before mock.module runs and bind the real exports - defeating the seam. The trade-off is sequencing discipline: the
 * suite-level before() must complete its mocks-then-import dance before any test body runs.
 *
 * Architectural findings surfaced during construction (recorded alongside the integration-tests roadmap):
 *
 *   1. The Channels DVR port is user-configurable via CONFIG.channelsDvr.port - src/streaming/showInfo.ts builds the DVR URL from that value, so there is no
 *      hard-coded port. Suite 12 routes around the HTTP layer entirely by mocking fetchFromDvr at the module boundary, because the HTTP layer is not the
 *      architectural unit under test - binding a stub to a fixed port would conflate two different concerns.
 *
 *   2. mock.module is the canonical interception seam for module-level dependencies in this repo. node:test's --experimental-test-module-mocks is enabled
 *      via the integration test runner; the dynamic-import-after-mock pattern below is the precedent for future suites that need this kind of seam.
 *
 *   3. getDeviceMappings caches by host with a 5-minute TTL. We sidestep host-keyed pollution by mocking getDeviceMappings entirely - the cache itself is
 *      bypassed, so test isolation is structural rather than depending on TTL math or unique-host-per-test conventions.
 *
 * Note on bootStubServer: this suite mocks the DVR data layer at the module boundary rather than standing up a stub HTTP server. The HTTP layer is incidental
 * to pretune's decision logic, so binding a stub server would conflate data acquisition with the decision path under test. The bootStubServer helper serves
 * suites whose relationship with their upstream IS the unit under test - the native HLS proxy suite - and is intentionally not used here.
 */
import type * as PretuneModule from "../../../src/streaming/pretune.ts";
import { afterEach, before, describe, mock, test } from "node:test";
import { createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import { deleteChannelStreamId, setChannelStreamId } from "../../../src/streaming/lifecycle.ts";
import { disablePredefinedChannels, enablePredefinedChannels, mutateChannels } from "../../../src/config/userChannels.ts";
import { registerStream, unregisterStream } from "../../../src/streaming/registry.ts";
import assert from "node:assert/strict";
import { makeRegistryEntry } from "../../../src/streaming/registry.helpers.ts";
import { mutateEnabledServices } from "../../../src/config/services.ts";

// Module-scope handles populated in the suite-level before() block. Each is set exactly once so all tests share the same mocks; tests reset call history per
// test rather than rebuild the mocks.
let pretune: typeof PretuneModule;
let initializeStreamSpy: ReturnType<typeof mock.fn<(options: unknown) => Promise<number | null>>>;
let fetchFromDvrSpy: ReturnType<typeof mock.fn<(host: string, path: string) => Promise<unknown[]>>>;
let getDvrHostStub: () => string | null;

// Synthetic schedule data set per test by reassigning fetchFromDvrSpy's behavior. The function's path argument routes between /api/v1/jobs (returns scheduled
// jobs) and any other path (defaults to empty - we do not use other endpoints in pretune's path).
let scheduledJobs: ScheduledJobFixture[] = [];

// The DVR API uses snake_case in the wire protocol; the production ScheduledJob type mirrors that exactly. The fixture reproduces the protocol shape so its
// shape stays in lockstep with what pretune.ts reads - we are matching an external contract, not authoring our own naming.
interface ScheduledJobFixture {

  channels: string[];
  id: string;
  item: { cancelled?: boolean; completed?: boolean };
  name: string;
  skipped?: boolean;
  start_time: number;
}

// Synthetic device mappings - guide number to channel ID. pretune calls getDeviceMappings(host) to resolve job.channels[0] (a guide number) to a PrismCast
// channel key. The mock returns one M3U device with whatever guide-to-channel entries the test sets.
let deviceGuideMap = new Map<string, string>();

/* drainMicrotasks yields to the microtask queue several times so that async chains queued from a mock.timers.tick callback resolve completely before the
 * test asserts. pollForUpcomingJobs awaits twice (fetchFromDvr, then getDeviceMappings) before scheduling per-job timers, and pretuneChannel itself awaits
 * inside its retry loop. A single Promise.resolve() drains one continuation; we drain several for headroom against future await additions in the production
 * path. The loop body intentionally does no work other than yielding.
 */
async function drainMicrotasks(): Promise<void> {

  for(let i = 0; i < 8; i++) {

    // eslint-disable-next-line no-await-in-loop -- the loop semantically IS the sequential drain.
    await Promise.resolve();
  }
}

describe("pretune scheduling state machine", () => {

  before(async () => {

    // Capture the real exports of hls.ts and showInfo.ts so we can passthrough the names pretune does not call directly. Without this, mock.module would
    // declare the mocked module's namedExports as the FULL set of exports for that module, and any incidental access to a non-listed name would resolve to
    // undefined. The safe pattern: real exports + targeted overrides.
    const realHls = await import("../../../src/streaming/hls.ts");
    const realShowInfo = await import("../../../src/streaming/showInfo.ts");

    initializeStreamSpy = mock.fn<(options: unknown) => Promise<number | null>>(async () => 999);
    fetchFromDvrSpy = mock.fn<(host: string, path: string) => Promise<unknown[]>>(async (_host, path) => {

      if(path === "/api/v1/jobs") {

        return scheduledJobs;
      }

      return [];
    });

    // The DVR-host stub returns a stable synthetic value. The real persistence path is not exercised here - we drive pretune directly via getDvrHost rather
    // than via a real setDvrHost+persistConfig round-trip.
    getDvrHostStub = (): string | null => "stub-dvr-host";

    const hlsUrl = new URL("../../../src/streaming/hls.ts", import.meta.url).href;
    const showInfoUrl = new URL("../../../src/streaming/showInfo.ts", import.meta.url).href;

    mock.module(hlsUrl, {

      // mock.module accepts namedExports to declare the mocked module's export set. @types/node marks namedExports as deprecated in favor of the newer
      // exports field, but the runtime still honors namedExports, so it continues to work for the passthrough-plus-override pattern used here.
      namedExports: { ...realHls, initializeStream: initializeStreamSpy }
    });

    mock.module(showInfoUrl, {

      namedExports: {

        ...realShowInfo,
        fetchFromDvr: fetchFromDvrSpy,
        getDeviceMappings: async (): Promise<Map<string, Map<string, string>>> => new Map([[ "test-device", deviceGuideMap ]]),
        getDvrHost: (): string | null => getDvrHostStub()
      }
    });

    // Now that the mocks are in place, dynamic-import pretune.ts so its captured references resolve to the mocks rather than the real exports. This must
    // happen here, not at the top of the file - a static import would resolve before the mocks are registered.
    pretune = await import("../../../src/streaming/pretune.ts");
  });

  afterEach(() => {

    // Reset only what the test mutated: spy call history, timer state, fixture data, and pretune's internal timer maps (cleared by stopPretunePolling). The
    // module mocks themselves persist across tests because they were established in before() once.
    pretune.stopPretunePolling();
    mock.timers.reset();
    initializeStreamSpy.mock.resetCalls();
    fetchFromDvrSpy.mock.resetCalls();
    scheduledJobs = [];
    deviceGuideMap = new Map();
  });

  test("an empty schedule from the DVR is handled cleanly with no pretune events scheduled", async () => {

    /* The boundary case: the DVR returns no jobs. Pretune must poll, see nothing eligible, and remain quiet - no errors, no spurious scheduling, no
     * initializeStream calls when the per-job timers later (would have) fired. We tick well past the polling interval to confirm the quiet state persists.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    mock.timers.enable({ apis: [ "Date", "setInterval", "setTimeout" ], now: 1_700_000_000_000 });

    scheduledJobs = [];

    pretune.startPretunePolling();

    // Drive the initial 5-second startup poll, then a full 60-second polling interval. Both polls should hit the empty schedule and exit cleanly.
    mock.timers.tick(5_000);
    await drainMicrotasks();
    mock.timers.tick(60_000);
    await drainMicrotasks();

    assert.equal(initializeStreamSpy.mock.callCount(), 0, "no pretune calls should fire on an empty schedule");
    assert.ok(fetchFromDvrSpy.mock.callCount() >= 1, "the DVR jobs endpoint must be polled at least once");
  });

  test("a job for a channel that is already streaming does NOT trigger pretune (cf2e9c7 invariant)", async () => {

    /* The cf2e9c7 regression class: pretune must coexist with active streams without spawning duplicates. A scheduled program for channel X arriving in the
     * polling window when X is already streaming must end at the existingStreamId guard inside pretuneChannel - no second initializeStream call for X. The
     * symptom this catches in production is duplicate streams competing for the same channel slot, capacity-limit-related rejection cascades, or in the worst
     * case, two browser tabs racing on the same provider session.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    mock.timers.enable({ apis: [ "Date", "setInterval", "setTimeout" ], now: 1_700_000_000_000 });

    // Add the channel so validateChannel inside pretuneChannel succeeds. The user-channel form populates only the fields validateChannel needs.
    await mutateChannels((data) => {

      data.channels["abc"] = { name: "ABC", url: "https://example.test/abc" };
    });

    // Pre-register an active stream for "abc" - this is what triggers the cf2e9c7 skip.
    const activeEntry = makeRegistryEntry({ channelName: "abc" });

    registerStream(activeEntry);
    setChannelStreamId("abc", activeEntry.id);

    ctx.registerCleanup(() => {

      deleteChannelStreamId("abc");
      unregisterStream(activeEntry.id);
    });

    // Map guide number "100" to channel "abc" so pretune's resolveGuideNumber path finds it.
    deviceGuideMap = new Map([[ "100", "abc" ]]);

    // Schedule a job 90 seconds in the future (well within the 5-minute horizon, well past the 30s pretune lead).
    scheduledJobs = [{

      channels: ["100"],
      id: "job-cf2e9c7",
      item: {},
      name: "Late Show",
      // eslint-disable-next-line camelcase -- DVR wire protocol field name.
      start_time: Math.floor((Date.now() + 90_000) / 1000)
    }];

    pretune.startPretunePolling();

    // Tick past the 5s startup poll - pretune schedules a setTimeout for (90s - 30s) = 60s from now.
    mock.timers.tick(5_000);
    await drainMicrotasks();

    // Tick past the per-job timer's effective delay - the timer fires and pretuneChannel runs. The channel-already-streaming guard must short-circuit before
    // initializeStream is reached.
    mock.timers.tick(60_000);
    await drainMicrotasks();

    assert.equal(initializeStreamSpy.mock.callCount(), 0, "pretune must NOT call initializeStream when the channel is already streaming");
  });

  test("stopPretunePolling cancels scheduled per-job timers so pretune does not fire after stop", async () => {

    /* The shutdown contract: stopPretunePolling must drain in-flight pretune timers, not merely stop the polling loop. A regression where stop() left
     * dangling per-job timers would surface in production as pretune events firing minutes after intended shutdown - capturing the channel into a stream
     * that the operator believed was no longer being managed by this layer.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    mock.timers.enable({ apis: [ "Date", "setInterval", "setTimeout" ], now: 1_700_000_000_000 });

    await mutateChannels((data) => {

      data.channels["abc"] = { name: "ABC", url: "https://example.test/abc" };
    });

    deviceGuideMap = new Map([[ "100", "abc" ]]);

    // Schedule a job 4 minutes ahead - well inside the horizon, well before the polling interval would refresh it.
    scheduledJobs = [{

      channels: ["100"],
      id: "job-stop-test",
      item: {},
      name: "Tonight Show",
      // eslint-disable-next-line camelcase -- DVR wire protocol field name.
      start_time: Math.floor((Date.now() + 240_000) / 1000)
    }];

    pretune.startPretunePolling();

    // Initial 5s startup poll - schedules the per-job timer for (240s - 30s) = 210s from now.
    mock.timers.tick(5_000);
    await drainMicrotasks();

    // Stop polling. This must clear both the polling interval AND the per-job timer.
    pretune.stopPretunePolling();

    // Advance past the original effective delay - if the per-job timer was not cancelled, this would fire pretuneChannel and call initializeStream.
    mock.timers.tick(220_000);
    await drainMicrotasks();

    assert.equal(initializeStreamSpy.mock.callCount(), 0, "stop() must cancel pending per-job timers; no pretune should fire after stop");
  });

  test("a job within the horizon for an idle channel triggers exactly one pretune call when the per-job timer fires", async () => {

    /* The positive case: a clean schedule with an eligible job where no active stream exists. Pretune must read the schedule, schedule a setTimeout for
     * (start - 30s), and when that timer fires, call initializeStream once with the right options. This pins the happy-path flow that all the negative tests
     * implicitly depend on - if the positive path is broken, the negative-test assertions become uninformative.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    mock.timers.enable({ apis: [ "Date", "setInterval", "setTimeout" ], now: 1_700_000_000_000 });

    await mutateChannels((data) => {

      data.channels["nbc"] = { name: "NBC", url: "https://example.test/nbc" };
    });

    deviceGuideMap = new Map([[ "200", "nbc" ]]);

    scheduledJobs = [{

      channels: ["200"],
      id: "job-positive",
      item: {},
      name: "Saturday Night Live",
      // eslint-disable-next-line camelcase -- DVR wire protocol field name.
      start_time: Math.floor((Date.now() + 90_000) / 1000)
    }];

    pretune.startPretunePolling();

    mock.timers.tick(5_000);
    await drainMicrotasks();

    assert.equal(initializeStreamSpy.mock.callCount(), 0, "no pretune call yet - the per-job timer has not fired");

    // Fire the per-job timer. Effective delay was (90s - 30s) = 60s; we ticked 5s above, so 55s more reaches the firing point.
    mock.timers.tick(55_000);
    await drainMicrotasks();
    // The pretune timer's setTimeout callback enters an async function (pretuneChannel). We need a tick of microtask flush for the call to register.
    await drainMicrotasks();
    await drainMicrotasks();

    assert.equal(initializeStreamSpy.mock.callCount(), 1, "exactly one pretune call when the per-job timer fires");

    const [options] = initializeStreamSpy.mock.calls[0]?.arguments ?? [];

    assert.ok(options && (typeof options === "object"), "pretune must pass an options object to initializeStream");
    assert.equal((options as { channelName: string }).channelName, "nbc", "options.channelName identifies the pretuned channel");
    assert.equal((options as { preTuned: boolean }).preTuned, true, "options.preTuned must be true so the stream is exempt from idle timeout until a client connects");
  });

  /* Phase 2.5 Suite 38: pretune contract for non-streamable channels.
   *
   * The four tests below close coverage gaps adjacent to Suite 12's "already-streaming" invariant. Each pins what pretune does when the DVR job's resolved channel
   * is not currently streamable from PrismCast - either because the channel does not exist (test 1), is structurally hidden by the user's service filter
   * (test 2), or is on the user's predefined-disabled list (test 3). Test 4 is a positive control proving the new tests' assertions are informative: a
   * normally-available predefined channel still pretunes through the same code paths the negative tests share.
   *
   * The pretune entry point is pretuneChannel(channelId, jobName, startTimeMs), which calls validateChannel(channelId) to gate the actual capture. The decision
   * surface that matters per scenario:
   *
   *   - Channel not in catalog: validateChannel returns invalid (Channel not found, 404). Pretune short-circuits before initializeStream.
   *   - Channel disabled via disabledPredefined: validateChannel returns invalid (Channel is disabled, 404). Pretune short-circuits.
   *   - Channel filtered by enabledServices: validateChannel returns invalid (Channel not available, 404) via the isChannelAvailableByService gate. Pretune
   *     short-circuits before initializeStream - same end-to-end rule as the M3U playlist, the HDHomeRun lineup, and getAllChannels.
   *
   * Test setup discipline: each test explicitly resets module-level CONFIG state at the top (mutateEnabledServices, enablePredefinedChannels) because the
   * createIntegrationContext temp-dir reset does not roll back in-process module singletons. Without these resets, state leakage from a prior test can produce
   * deceptive passes on a later test. See the "test isolation" note in the integration helper for the underlying constraint.
   */

  test("scheduled job for a channel key NOT in the catalog does NOT trigger pretune", async () => {

    /* Suite 38 test 1: missing channel. The guide number maps to "missing-channel-x9z2", a key that exists nowhere - not in PREDEFINED_CHANNELS, not in user
     * channels. validateChannel walks the channelsRef lookup, the getAllChannels fallback, and finds nothing - returns 404 invalid. Pretune short-circuits with
     * a debug log and never calls initializeStream. The negative invariant pins that pretune cannot fire on phantom channels (a guide-mapping drift, a renamed
     * channel still referenced by the DVR's queued job, a typo in the operator's mapping table).
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    // Reset CONFIG state. The temp-dir-per-test pattern does not roll back module-level singletons; we explicitly clear the filter and re-enable any historically
    // disabled predefined keys to insulate this test from prior-test mutations of the same module-state cache.
    await mutateEnabledServices([]);
    await enablePredefinedChannels([ "abcnews", "cnn", "nbc" ]);

    mock.timers.enable({ apis: [ "Date", "setInterval", "setTimeout" ], now: 1_700_000_000_000 });

    deviceGuideMap = new Map([[ "999", "missing-channel-x9z2" ]]);

    scheduledJobs = [{

      channels: ["999"],
      id: "job-missing-channel",
      item: {},
      name: "Phantom Show",
      // eslint-disable-next-line camelcase -- DVR wire protocol field name.
      start_time: Math.floor((Date.now() + 90_000) / 1000)
    }];

    pretune.startPretunePolling();

    // 5s startup poll, then advance through the per-job timer's effective delay (90s - 30s = 60s).
    mock.timers.tick(5_000);
    await drainMicrotasks();
    mock.timers.tick(60_000);
    await drainMicrotasks();
    await drainMicrotasks();

    assert.equal(initializeStreamSpy.mock.callCount(), 0, "pretune must NOT call initializeStream when the resolved channel does not exist in the catalog");
  });

  test("scheduled job for a channel filtered out by enabledServices does NOT trigger pretune", async () => {

    /* Suite 38 test 2: filtered channel. enabledServices is the user's "I do not subscribe to / am not interested in this service" signal. A channel whose every
     * variant tag is excluded is not actionable for the user - pretuning it would capture a browser tab the user cannot see, increment the recovery budget on a
     * service the user explicitly excluded, and diverge from the M3U playlist (built on getVisibleChannels), the HDHomeRun lineup (iterates getAllChannels), and
     * getAllChannels itself - all three of which already exclude filtered-out channels.
     *
     * The end-to-end rule is enforced at the streaming boundary by validateChannel via isChannelAvailableByService - the same canonical predicate used by
     * getVisibleChannels, the channel table renderer, and the bulk-action allowlist builders. The negative invariant: any future caller of validateChannel
     * (HLS direct tuning, MPEG-TS for HDHomeRun, pretune) gets the same rejection for filtered-out channels, so the bug class cannot resurface from a new
     * streaming entry point as long as it routes through validateChannel.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await enablePredefinedChannels([ "abcnews", "cnn", "nbc" ]);

    // Set a filter that excludes every variant of abcnews. abcnews has no `direct` tag (no site URL), so isChannelAvailableByService(abcnews) is false: this is
    // structurally a filtered-out channel from the user's perspective, but the pretune path does not consult that predicate. Use a tag that does not match any
    // service in the channel catalog so we are unambiguously outside any abcnews variant's tag set.
    await mutateEnabledServices(["nonexistent-service-tag"]);

    ctx.registerCleanup(async () => {

      // Reset module-level filter so subsequent tests see an empty filter.
      await mutateEnabledServices([]);
    });

    mock.timers.enable({ apis: [ "Date", "setInterval", "setTimeout" ], now: 1_700_000_000_000 });

    deviceGuideMap = new Map([[ "100", "abcnews" ]]);

    scheduledJobs = [{

      channels: ["100"],
      id: "job-filtered",
      item: {},
      name: "ABC News Tonight",
      // eslint-disable-next-line camelcase -- DVR wire protocol field name.
      start_time: Math.floor((Date.now() + 90_000) / 1000)
    }];

    pretune.startPretunePolling();

    mock.timers.tick(5_000);
    await drainMicrotasks();
    mock.timers.tick(60_000);
    await drainMicrotasks();
    await drainMicrotasks();

    assert.equal(initializeStreamSpy.mock.callCount(), 0,
      "pretune must NOT call initializeStream when every variant of the resolved channel is excluded by enabledServices");
  });

  test("scheduled job for a disabledPredefined channel does NOT trigger pretune", async () => {

    /* Suite 38 test 3: a predefined channel on the user's disabledPredefined list. validateChannel checks isPredefinedChannelDisabled first, short-
     * circuits with 404 "Channel is disabled," and pretuneChannel returns before initializeStream. The negative invariant: a user explicitly hiding a channel
     * from the playlist also suppresses pretune for that channel - the two paths share the same on-off semantics.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await mutateEnabledServices([]);
    await disablePredefinedChannels(["abcnews"]);

    ctx.registerCleanup(async () => {

      // Re-enable abcnews so subsequent tests in this run see the default state.
      await enablePredefinedChannels(["abcnews"]);
    });

    mock.timers.enable({ apis: [ "Date", "setInterval", "setTimeout" ], now: 1_700_000_000_000 });

    deviceGuideMap = new Map([[ "100", "abcnews" ]]);

    scheduledJobs = [{

      channels: ["100"],
      id: "job-disabled",
      item: {},
      name: "ABC News Disabled",
      // eslint-disable-next-line camelcase -- DVR wire protocol field name.
      start_time: Math.floor((Date.now() + 90_000) / 1000)
    }];

    pretune.startPretunePolling();

    mock.timers.tick(5_000);
    await drainMicrotasks();
    mock.timers.tick(60_000);
    await drainMicrotasks();
    await drainMicrotasks();

    assert.equal(initializeStreamSpy.mock.callCount(), 0, "pretune must NOT call initializeStream when the channel is on disabledPredefined");
  });

  test("scheduled job for a normally-available predefined channel triggers exactly one pretune call", async () => {

    /* Suite 38 test 4: positive control adjacent to the negative tests above. A clean state (no filter, nothing disabled), a predefined channel (cnn) that
     * structurally exists in PREDEFINED_CHANNELS, and a job within the horizon. Pretune resolves the channel, validates it, and fires initializeStream. This
     * pins that the negative-test assertions are informative - if the positive path were broken, all negative-test 0-counts would pass for the wrong reason.
     *
     * Distinct from the existing positive control (which uses a custom-seeded "nbc" channel with a synthetic URL). This one uses the predefined cnn entry
     * unmodified, so the validation passes through the predefined-only branches that the existing test does not exercise.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await mutateEnabledServices([]);
    await enablePredefinedChannels(["cnn"]);

    mock.timers.enable({ apis: [ "Date", "setInterval", "setTimeout" ], now: 1_700_000_000_000 });

    deviceGuideMap = new Map([[ "200", "cnn" ]]);

    scheduledJobs = [{

      channels: ["200"],
      id: "job-positive-cnn",
      item: {},
      name: "Anderson Cooper 360",
      // eslint-disable-next-line camelcase -- DVR wire protocol field name.
      start_time: Math.floor((Date.now() + 90_000) / 1000)
    }];

    pretune.startPretunePolling();

    mock.timers.tick(5_000);
    await drainMicrotasks();
    mock.timers.tick(60_000);
    await drainMicrotasks();
    await drainMicrotasks();

    assert.equal(initializeStreamSpy.mock.callCount(), 1, "predefined cnn with no filter and not disabled must pretune exactly once");

    const [options] = initializeStreamSpy.mock.calls[0]?.arguments ?? [];

    assert.equal((options as { channelName: string }).channelName, "cnn", "options.channelName must be the predefined cnn key");
    assert.equal((options as { preTuned: boolean }).preTuned, true, "options.preTuned must be true so the stream is exempt from idle timeout until a client connects");
  });
});
