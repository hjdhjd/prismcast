/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * pretune.test.ts: Integration coverage for the pretune scheduling state machine in src/streaming/pretune.ts. The architectural unit under test is pretune's
 * decision logic - read the DVR job schedule, dedupe against active streams, schedule per-job timers within the horizon, cancel cleanly on stop. The HTTP
 * fetch layer to the DVR is not the boundary under test - it is data acquisition for the decision layer. pretune composes those calls behind its injectable
 * PretuneDeps port (clock, fetchFromDvr, getDeviceMappings, getDvrHost, initializeStream), so we pass a deps object that feeds synthetic schedule data straight into
 * the decision logic and records go / no-go decisions through an initializeStream spy - no real HTTP round-trip, no real browser-launching capture path, and
 * no loader mock.
 *
 * Architectural constraints that shape this suite:
 *
 *   1. The Channels DVR port is user-configurable via CONFIG.channelsDvr.port - pretune reads it at each poll and hands it to src/streaming/showInfo.ts,
 *      which builds the DVR URL from it, so there is no hard-coded port. This suite routes around the HTTP layer entirely by injecting fetchFromDvr at
 *      pretune's port, because the HTTP layer is not the architectural unit under test - binding a stub to a fixed port would conflate two different concerns.
 *
 *   2. getDeviceMappings caches by host with a 5-minute TTL. We sidestep host-keyed pollution by injecting getDeviceMappings entirely - the cache itself is
 *      bypassed, so test isolation is structural rather than depending on TTL math or unique-host-per-test conventions.
 *
 *   3. The scheduler runs entirely on the clock its deps carry: every read, the poll cadence, the per-job timers, the retry sleeps, and the safety timers. Each
 *      row therefore builds a TestClock at BASE_TIME_MS, hands it to a row-scoped deps object, and drives the whole schedule with advance() plus settle() - one
 *      virtual timeline, no global timer mocking, and no real-time wait anywhere in the suite.
 *
 * Note on bootStubServer: this suite injects the DVR data layer at pretune's port rather than standing up a stub HTTP server. The HTTP layer is incidental to
 * pretune's decision logic, so binding a stub server would conflate data acquisition with the decision path under test. The bootStubServer helper serves suites
 * whose relationship with their upstream IS the unit under test - the native HLS proxy suite - and is intentionally not used here.
 */
import * as pretune from "../../../src/streaming/pretune.ts";
import { CONFIG, initializeConfiguration } from "../../../src/config/index.ts";
import { TestClock, settle } from "homebridge-plugin-utils/testing";
import { afterEach, describe, mock, test } from "node:test";
import { createIntegrationContext, initializePersistence, writePersistedJson } from "../../helpers/integration.helpers.ts";
import { deleteChannelStreamId, setChannelStreamId } from "../../../src/streaming/lifecycle.ts";
import { disablePredefinedChannels, enablePredefinedChannels, mutateChannels } from "../../../src/config/userChannels.ts";
import { getStream, registerStream, unregisterStream } from "../../../src/streaming/registry.ts";
import type { Clock } from "homebridge-plugin-utils";
import assert from "node:assert/strict";
import { firstOf } from "../../../src/testing.helpers.ts";
import { getDvrHost } from "../../../src/streaming/showInfo.ts";
import { makeRegistryEntry } from "../../../src/streaming/registry.helpers.ts";
import { mutateEnabledServices } from "../../../src/config/services.ts";

// The reference instant every row's virtual clock is seeded at, so a row's expected timestamps read as offsets rather than absolute epochs.
const BASE_TIME_MS = 1700000000000;

// Synthetic schedule data set per test. The injected fetchFromDvr returns these for the /api/v1/jobs path (and nothing for any other path - pretune reads no other
// endpoint). Reassigned per test; the injected deps' closures read it at call time. The row type is the production ScheduledJob (the wire shape) directly, so the
// fixture stays in lockstep with what pretune reads rather than duplicating the type.
let scheduledJobs: pretune.ScheduledJob[] = [];

// Synthetic device mappings - guide number to channel key. pretune calls getDeviceMappings(host, port, now) to resolve job.channels[0] (a guide number) to a
// PrismCast channel key. The injected getDeviceMappings returns one synthetic device whose guide map is this. Reassigned per test.
let deviceGuideMap = new Map<string, string>();

// The initializeStream spy stands in for the browser-launching capture path so tests observe pretune's go / no-go decisions by call count and arguments. Typed as
// the production PretuneDeps field so the double cannot drift from initializeStream's real signature.
const initializeStreamSpy = mock.fn<pretune.PretuneDeps["initializeStream"]>(async () => 999);

// The fetchFromDvr spy returns the per-test scheduledJobs for the jobs endpoint and nothing for any other path.
const fetchFromDvrSpy = mock.fn<pretune.PretuneDeps["fetchFromDvr"]>(async (_host, _port, path) => ((path === "/api/v1/jobs") ? scheduledJobs : []));

/* The injected pretune dependencies: the DVR data-acquisition trio and the initializeStream go-action, substituted at pretune's PretuneDeps port so the decision
 * logic runs against synthetic schedule data with no HTTP round-trip or real capture. getDeviceMappings returns one synthetic device whose guide map is the per-
 * test deviceGuideMap; getDvrHost is a stable stub. The clock is the row's own, so each row owns its timeline outright and no state carries between rows. Typed
 * as PretuneDeps so the doubles cannot drift from the production port.
 * @param clock - The row's virtual clock, which every timer and every read the scheduler performs runs on.
 * @returns The dependency object to hand startPretunePolling.
 */
function makeDeps(clock: Clock): pretune.PretuneDeps {

  return {

    clock,
    fetchFromDvr: fetchFromDvrSpy,
    getDeviceMappings: async (): Promise<Map<string, Map<string, string>>> => new Map([[ "test-device", deviceGuideMap ]]),
    getDvrHost: (): string => "stub-dvr-host",
    initializeStream: initializeStreamSpy
  };
}

describe("pretune scheduling state machine", () => {

  afterEach(() => {

    // Reset only what the test mutated: spy call history, fixture data, and pretune's own registries (disposed by stopPretunePolling). Each row's clock is its
    // own local, so there is no timer state here to reset.
    pretune.stopPretunePolling();
    initializeStreamSpy.mock.resetCalls();
    fetchFromDvrSpy.mock.resetCalls();
    scheduledJobs = [];
    deviceGuideMap = new Map();
  });

  test("a stop within the first five seconds of the start cancels the deferred first poll", async () => {

    /* The first poll is a tracked one-shot on the scheduler's poll registry, so a stop before it comes due drains it along with the interval. The pending count is
     * what separates a drained registry from a first poll that was armed on some timeline this row cannot see - either would leave the DVR spy untouched, so the
     * count is what makes the assertion mean the timer is gone rather than merely quiet.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const clock = new TestClock(BASE_TIME_MS);
    const deps = makeDeps(clock);

    scheduledJobs = [];

    pretune.startPretunePolling(deps);

    assert.equal(clock.pending, 2, "the start armed the deferred first poll and the repeating poll interval");

    pretune.stopPretunePolling();

    assert.equal(clock.pending, 0, "the stop drained both, the first poll included");

    clock.advance(60000);
    await settle();

    assert.equal(fetchFromDvrSpy.mock.callCount(), 0, "neither the cancelled first poll nor the drained interval ever reached the DVR");
  });

  test("an empty schedule from the DVR is handled cleanly with no pretune events scheduled", async () => {

    /* The boundary case: the DVR returns no jobs. Pretune must poll, see nothing eligible, and remain quiet - no errors, no spurious scheduling, no
     * initializeStream calls when the per-job timers later (would have) fired. We tick well past the polling interval to confirm the quiet state persists.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const clock = new TestClock(BASE_TIME_MS);
    const deps = makeDeps(clock);

    scheduledJobs = [];

    pretune.startPretunePolling(deps);

    // Drive the initial 5-second startup poll, then a full 60-second polling interval. Both polls should hit the empty schedule and exit cleanly.
    clock.advance(5000);
    await settle();
    clock.advance(60000);
    await settle();

    assert.equal(initializeStreamSpy.mock.callCount(), 0, "no pretune calls should fire on an empty schedule");
    assert.ok(fetchFromDvrSpy.mock.callCount() >= 1, "the DVR jobs endpoint must be polled at least once");
  });

  test("a job for a channel that is already streaming does NOT trigger pretune (duplicate-stream guard)", async () => {

    /* The already-streaming guard: pretune must coexist with active streams without spawning duplicates. A scheduled program for channel X arriving in the
     * polling window when X is already streaming must end at the existingStreamId guard inside pretuneChannel - no second initializeStream call for X. The
     * symptom this catches in production is duplicate streams competing for the same channel slot, capacity-limit-related rejection cascades, or in the worst
     * case, two browser tabs racing on the same provider session.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const clock = new TestClock(BASE_TIME_MS);
    const deps = makeDeps(clock);

    // Add the channel so validateChannel inside pretuneChannel succeeds. The user-channel form populates only the fields validateChannel needs.
    await mutateChannels((data) => {

      data.channels["abc"] = { name: "ABC", url: "https://example.test/abc" };
    });

    // Pre-register an active stream for "abc" - this is what triggers the already-streaming skip.
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
      id: "job-already-streaming",
      item: {},
      name: "Late Show",
      // eslint-disable-next-line camelcase -- DVR wire protocol field name.
      start_time: Math.floor((clock.now() + 90000) / 1000)
    }];

    pretune.startPretunePolling(deps);

    // Tick past the 5s startup poll - pretune schedules a setTimeout for (90s - 30s) = 60s from now.
    clock.advance(5000);
    await settle();

    // Tick past the per-job timer's effective delay - the timer fires and pretuneChannel runs. The channel-already-streaming guard must short-circuit before
    // initializeStream is reached.
    clock.advance(60000);
    await settle();

    assert.equal(initializeStreamSpy.mock.callCount(), 0, "pretune must NOT call initializeStream when the channel is already streaming");
  });

  test("stopPretunePolling cancels scheduled per-job timers so pretune does not fire after stop", async () => {

    /* The shutdown contract: stopPretunePolling must drain in-flight pretune timers, not merely stop the polling loop. A regression where stop() left
     * dangling per-job timers would surface in production as pretune events firing minutes after intended shutdown - capturing the channel into a stream
     * that the operator believed was no longer being managed by this layer.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const clock = new TestClock(BASE_TIME_MS);
    const deps = makeDeps(clock);

    await mutateChannels((data) => {

      data.channels["abc"] = { name: "ABC", url: "https://example.test/abc" };
    });

    deviceGuideMap = new Map([[ "100", "abc" ]]);

    // Schedule a job 4 minutes ahead - well inside the horizon - so its per-job timer is still pending when the row stops polling after the startup poll.
    scheduledJobs = [{

      channels: ["100"],
      id: "job-stop-test",
      item: {},
      name: "Tonight Show",
      // eslint-disable-next-line camelcase -- DVR wire protocol field name.
      start_time: Math.floor((clock.now() + 240000) / 1000)
    }];

    pretune.startPretunePolling(deps);

    // Initial 5s startup poll - schedules the per-job timer for (240s - 30s) = 210s from now.
    clock.advance(5000);
    await settle();

    // Stop polling. This must clear both the polling interval AND the per-job timer.
    pretune.stopPretunePolling();

    // Advance past the original effective delay - if the per-job timer was not cancelled, this would fire pretuneChannel and call initializeStream.
    clock.advance(220000);
    await settle();

    assert.equal(initializeStreamSpy.mock.callCount(), 0, "stop() must cancel pending per-job timers; no pretune should fire after stop");
  });

  test("a job within the horizon for an idle channel triggers exactly one pretune call when the per-job timer fires", async () => {

    /* The positive case: a clean schedule with an eligible job where no active stream exists. Pretune must read the schedule, schedule a setTimeout for
     * (start - 30s), and when that timer fires, call initializeStream once with the right options. This asserts the happy-path flow that all the negative tests
     * implicitly depend on - if the positive path is broken, the negative-test assertions become uninformative.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const clock = new TestClock(BASE_TIME_MS);
    const deps = makeDeps(clock);

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
      start_time: Math.floor((clock.now() + 90000) / 1000)
    }];

    pretune.startPretunePolling(deps);

    clock.advance(5000);
    await settle();

    assert.equal(initializeStreamSpy.mock.callCount(), 0, "no pretune call yet - the per-job timer has not fired");

    // Fire the per-job timer. Effective delay was (90s - 30s) = 60s; we ticked 5s above, so 55s more reaches the firing point.
    clock.advance(55000);
    await settle();
    // The pretune timer's setTimeout callback enters an async function (pretuneChannel). We need a tick of microtask flush for the call to register.
    await settle();
    await settle();

    assert.equal(initializeStreamSpy.mock.callCount(), 1, "exactly one pretune call when the per-job timer fires");

    const [options] = initializeStreamSpy.mock.calls[0]?.arguments ?? [];

    assert.ok(options && (typeof options === "object"), "pretune must pass an options object to initializeStream");
    assert.equal((options as { channelName: string }).channelName, "nbc", "options.channelName identifies the pretuned channel");
    assert.equal((options as { preTuned: boolean }).preTuned, true, "options.preTuned must be true so the stream is exempt from idle timeout until a client connects");
  });

  /* Pretune contract for non-streamable channels.
   *
   * The tests below close coverage gaps adjacent to the "already-streaming" rule above. Each asserts what pretune does when the DVR job's resolved channel
   * is not currently streamable from PrismCast - either because the channel does not exist (the missing-channel row), is structurally hidden by the user's
   * service filter (the filtered-channel row), or is on the user's predefined-disabled list (the disabled-predefined row). The predefined positive control
   * proves these rows' assertions are informative: a normally-available predefined channel still pretunes through the same code paths the negative tests share.
   *
   * The pretune entry point is pretuneChannel(channelId, jobName, startTimeMs, deps), which calls validateChannel(channelId) to gate the actual capture. The decision
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

    /* Missing channel. The guide number maps to "missing-channel-x9z2", a key that exists nowhere - not in PREDEFINED_CHANNELS, not in user
     * channels. validateChannel walks the channelsRef lookup, the getAllChannels fallback, and finds nothing - returns 404 invalid. Pretune short-circuits with
     * a debug log and never calls initializeStream. The negative case asserts that pretune cannot fire on phantom channels (a guide-mapping drift, a renamed
     * channel still referenced by the DVR's queued job, a typo in the operator's mapping table).
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Reset CONFIG state. The temp-dir-per-test pattern does not roll back module-level singletons; we explicitly clear the filter and re-enable any historically
    // disabled predefined keys to insulate this test from prior-test mutations of the same module-state cache.
    await mutateEnabledServices([]);
    await enablePredefinedChannels([ "abcnews", "cnn", "nbc" ]);

    const clock = new TestClock(BASE_TIME_MS);
    const deps = makeDeps(clock);

    deviceGuideMap = new Map([[ "999", "missing-channel-x9z2" ]]);

    scheduledJobs = [{

      channels: ["999"],
      id: "job-missing-channel",
      item: {},
      name: "Phantom Show",
      // eslint-disable-next-line camelcase -- DVR wire protocol field name.
      start_time: Math.floor((clock.now() + 90000) / 1000)
    }];

    pretune.startPretunePolling(deps);

    // 5s startup poll, then advance through the per-job timer's effective delay (90s - 30s = 60s).
    clock.advance(5000);
    await settle();
    clock.advance(60000);
    await settle();
    await settle();

    assert.equal(initializeStreamSpy.mock.callCount(), 0, "pretune must NOT call initializeStream when the resolved channel does not exist in the catalog");
  });

  test("scheduled job for a channel filtered out by enabledServices does NOT trigger pretune", async () => {

    /* Filtered channel. enabledServices is the user's "I do not subscribe to / am not interested in this service" signal. A channel whose every
     * variant tag is excluded is not actionable for the user - pretuning it would capture a browser tab the user cannot see, increment the recovery budget on a
     * service the user explicitly excluded, and diverge from the M3U playlist (built on getVisibleChannels), the HDHomeRun lineup (iterates getAllChannels), and
     * getAllChannels itself - all three of which already exclude filtered-out channels.
     *
     * The end-to-end rule is enforced at the streaming boundary by validateChannel via isChannelAvailableByService - the same canonical predicate used by
     * getVisibleChannels, the channel table renderer, and the bulk-action allowlist builders. The negative case: any future caller of validateChannel
     * (HLS direct tuning, MPEG-TS for HDHomeRun, pretune) gets the same rejection for filtered-out channels, so the bug class cannot resurface from a new
     * streaming entry point as long as it routes through validateChannel.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await enablePredefinedChannels([ "abcnews", "cnn", "nbc" ]);

    // Set a filter that excludes every variant of abcnews. abcnews has no `direct` tag (no site URL), so isChannelAvailableByService(abcnews) is false: this is
    // structurally a filtered-out channel from the user's perspective, and validateChannel, which pretuneChannel calls, rejects it with "Channel not available."
    // Use a known tag, which the running filter keeps, that no abcnews variant carries, so we are unambiguously outside every abcnews variant's tag set.
    await mutateEnabledServices(["paramountplus"]);

    ctx.registerCleanup(async () => {

      // Reset module-level filter so subsequent tests see an empty filter.
      await mutateEnabledServices([]);
    });

    const clock = new TestClock(BASE_TIME_MS);
    const deps = makeDeps(clock);

    deviceGuideMap = new Map([[ "100", "abcnews" ]]);

    scheduledJobs = [{

      channels: ["100"],
      id: "job-filtered",
      item: {},
      name: "ABC News Tonight",
      // eslint-disable-next-line camelcase -- DVR wire protocol field name.
      start_time: Math.floor((clock.now() + 90000) / 1000)
    }];

    pretune.startPretunePolling(deps);

    clock.advance(5000);
    await settle();
    clock.advance(60000);
    await settle();
    await settle();

    assert.equal(initializeStreamSpy.mock.callCount(), 0,
      "pretune must NOT call initializeStream when every variant of the resolved channel is excluded by enabledServices");
  });

  test("scheduled job for a disabledPredefined channel does NOT trigger pretune", async () => {

    /* A predefined channel on the user's disabledPredefined list. validateChannel checks isPredefinedChannelDisabled first, short-circuits with 404 "Channel
     * is disabled," and pretuneChannel returns before initializeStream. The negative case: a user explicitly hiding a channel from the playlist also
     * suppresses pretune for that channel - the two paths share the same on-off semantics.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateEnabledServices([]);
    await disablePredefinedChannels(["abcnews"]);

    ctx.registerCleanup(async () => {

      // Re-enable abcnews so subsequent tests in this run see the default state.
      await enablePredefinedChannels(["abcnews"]);
    });

    const clock = new TestClock(BASE_TIME_MS);
    const deps = makeDeps(clock);

    deviceGuideMap = new Map([[ "100", "abcnews" ]]);

    scheduledJobs = [{

      channels: ["100"],
      id: "job-disabled",
      item: {},
      name: "ABC News Disabled",
      // eslint-disable-next-line camelcase -- DVR wire protocol field name.
      start_time: Math.floor((clock.now() + 90000) / 1000)
    }];

    pretune.startPretunePolling(deps);

    clock.advance(5000);
    await settle();
    clock.advance(60000);
    await settle();
    await settle();

    assert.equal(initializeStreamSpy.mock.callCount(), 0, "pretune must NOT call initializeStream when the channel is on disabledPredefined");
  });

  test("scheduled job for a normally-available predefined channel triggers exactly one pretune call", async () => {

    /* Positive control adjacent to the negative tests above. A clean state (no filter, nothing disabled), a predefined channel (cnn) that
     * structurally exists in PREDEFINED_CHANNELS, and a job within the horizon. Pretune resolves the channel, validates it, and fires initializeStream. This
     * asserts that the negative-test assertions are informative - if the positive path were broken, all negative-test 0-counts would pass for the wrong reason.
     *
     * Distinct from the existing positive control (which uses a custom-seeded "nbc" channel with a synthetic URL). This one uses the predefined cnn entry
     * unmodified, so the validation passes through the predefined-only branches that the existing test does not exercise.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateEnabledServices([]);
    await enablePredefinedChannels(["cnn"]);

    const clock = new TestClock(BASE_TIME_MS);
    const deps = makeDeps(clock);

    deviceGuideMap = new Map([[ "200", "cnn" ]]);

    scheduledJobs = [{

      channels: ["200"],
      id: "job-positive-cnn",
      item: {},
      name: "Anderson Cooper 360",
      // eslint-disable-next-line camelcase -- DVR wire protocol field name.
      start_time: Math.floor((clock.now() + 90000) / 1000)
    }];

    pretune.startPretunePolling(deps);

    clock.advance(5000);
    await settle();
    clock.advance(60000);
    await settle();
    await settle();

    assert.equal(initializeStreamSpy.mock.callCount(), 1, "predefined cnn with no filter and not disabled must pretune exactly once");

    const [options] = initializeStreamSpy.mock.calls[0]?.arguments ?? [];

    assert.equal((options as { channelName: string }).channelName, "cnn", "options.channelName must be the predefined cnn key");
    assert.equal((options as { preTuned: boolean }).preTuned, true, "options.preTuned must be true so the stream is exempt from idle timeout until a client connects");
  });

  /* Scheduler-branch coverage. The tests below assert the internal branches of pretune's state machine that the go / no-go tests above do not exercise:
   * the safety-timeout reaper (claimed vs unclaimed), the retry loop and its past-start abandonment guard, the pre-schedule skips (cancelled/skipped,
   * outside horizon, already started, empty or unresolvable guide, empty device mappings), and the timer-map hygiene (dedup on re-poll, stale-timer cleanup on
   * job disappearance). Each drives the scheduler on its own virtual clock and asserts the observable effect a regression would break - the initializeStream spy
   * call count, the registry state after a timer fires, or the termination of a reaped stream.
   */

  test("an unclaimed pretuned stream is torn down by the safety timeout at start + 90s", async () => {

    /* The reaper's positive case: a pretuned stream that no real client ever claims must be terminated when its safety timer fires (start_time + 90s), so a
     * speculatively-tuned browser tab does not linger forever after the DVR declined to record. The safety callback reads getStream(streamId)?.preTuned; because
     * the injected initializeStream shortcuts to returning a stream id rather than registering one, the test seeds a matching preTuned registry entry so the
     * callback finds the live stream production would have registered.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateEnabledServices([]);
    await mutateChannels((data) => {

      data.channels["psafe"] = { name: "PSafe", url: "https://example.test/psafe" };
    });

    const clock = new TestClock(BASE_TIME_MS);
    const deps = makeDeps(clock);

    // Seed the registry entry the injected initializeStream will "return" so the safety timer's getStream(streamId) finds a live, preTuned stream to reap.
    const entry = makeRegistryEntry({ channelName: "psafe", preTuned: true });

    registerStream(entry);
    ctx.registerCleanup(() => { unregisterStream(entry.id); });

    // The next initializeStream call resolves to this seeded stream's id; subsequent calls (none expected here) fall back to the default stub.
    initializeStreamSpy.mock.mockImplementationOnce(async () => entry.id);

    deviceGuideMap = new Map([[ "300", "psafe" ]]);

    scheduledJobs = [{

      channels: ["300"],
      id: "job-safety-unclaimed",
      item: {},
      name: "Late Night Unclaimed",
      // eslint-disable-next-line camelcase -- DVR wire protocol field name.
      start_time: Math.floor((clock.now() + 50000) / 1000)
    }];

    pretune.startPretunePolling(deps);

    // Startup poll runs at +5s and schedules the per-job timer for +15s more (50s start - 30s lead - 5s elapsed). Firing it well before the 60s polling interval
    // keeps the interval re-poll out of this scenario.
    clock.advance(5000);
    await settle();

    // Fire the per-job timer: pretuneChannel calls initializeStream and arms the safety timer 120s out (start + 90s from the current clock).
    clock.advance(15000);
    await settle();
    await settle();

    assert.equal(initializeStreamSpy.mock.callCount(), 1, "the per-job timer must fire pretune exactly once");
    assert.ok(getStream(entry.id)?.preTuned, "the pretuned stream is registered and still unclaimed before the safety timeout");

    // Advance to start + 90s. The safety timer fires, sees preTuned still true, and terminates the unclaimed stream.
    clock.advance(120000);
    await settle();
    await settle();

    assert.equal(getStream(entry.id), undefined, "an unclaimed pretuned stream must be terminated by the safety timeout");
  });

  test("a claimed pretuned stream (preTuned cleared) is left running by the safety timeout", async () => {

    /* The reaper's negative case: once a real client claims a pretuned stream, the normal lifecycle clears its preTuned flag. When the safety timer later fires
     * it must observe preTuned false and leave the stream alone - reaping a stream a client is actively watching would drop live video. This is the guard inside
     * the safety callback (stream?.preTuned) and is the counterpart to the unclaimed-teardown test above.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateEnabledServices([]);
    await mutateChannels((data) => {

      data.channels["pclaim"] = { name: "PClaim", url: "https://example.test/pclaim" };
    });

    const clock = new TestClock(BASE_TIME_MS);
    const deps = makeDeps(clock);

    const entry = makeRegistryEntry({ channelName: "pclaim", preTuned: true });

    registerStream(entry);
    ctx.registerCleanup(() => { unregisterStream(entry.id); });

    initializeStreamSpy.mock.mockImplementationOnce(async () => entry.id);

    deviceGuideMap = new Map([[ "301", "pclaim" ]]);

    scheduledJobs = [{

      channels: ["301"],
      id: "job-safety-claimed",
      item: {},
      name: "Late Night Claimed",
      // eslint-disable-next-line camelcase -- DVR wire protocol field name.
      start_time: Math.floor((clock.now() + 50000) / 1000)
    }];

    pretune.startPretunePolling(deps);

    // Fire the per-job timer at +20s, before the 60s polling interval, so no interval re-poll enters this scenario.
    clock.advance(5000);
    await settle();
    clock.advance(15000);
    await settle();
    await settle();

    assert.equal(initializeStreamSpy.mock.callCount(), 1, "the per-job timer must fire pretune exactly once");

    // Simulate a real client claiming the stream: the normal lifecycle clears preTuned when a client connects.
    entry.preTuned = false;

    // Advance to start + 90s. The safety timer fires but sees preTuned cleared, so it must NOT terminate the now-claimed stream.
    clock.advance(120000);
    await settle();
    await settle();

    assert.ok(getStream(entry.id), "a claimed stream (preTuned cleared) must be left running by the safety timeout");
    assert.equal(getStream(entry.id)?.preTuned, false, "the claimed stream stays registered with preTuned cleared");
  });

  test("a throwing initializeStream is retried up to MAX_RETRIES attempts within the pretune window", async () => {

    /* The retry loop: pretuneChannel makes up to MAX_RETRIES (5) attempts at a failing capture, sleeping RETRY_DELAY_MS (5s) between them, because a transient
     * launch failure inside the pretune window should still land the stream before the recording starts. The spy call count of exactly 5 is the observable a
     * regression (retrying too few, too many, or forever) would break.
     *
     * Timing: the inter-attempt sleep runs through the injected Clock, so the row walks the retry budget by advancing RETRY_DELAY_MS once per attempt. The clock
     * starts at the per-job fire instant (start_time - 30s) and rises only 5s per sleep, so after four sleeps it is still below start_time and the past-start
     * abandonment guard never trips - all five attempts run and the loop then caps.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateEnabledServices([]);
    await mutateChannels((data) => {

      data.channels["pretry"] = { name: "PRetry", url: "https://example.test/pretry" };
    });

    const clock = new TestClock(BASE_TIME_MS);
    const deps = makeDeps(clock);

    // Make every capture attempt fail so pretuneChannel walks its full retry budget. Restore the default success stub at scope exit so no throwing implementation
    // leaks into a later test.
    initializeStreamSpy.mock.mockImplementation(async () => { throw new Error("pretune capture failed"); });
    ctx.registerCleanup(() => { initializeStreamSpy.mock.mockImplementation(async () => 999); });

    deviceGuideMap = new Map([[ "310", "pretry" ]]);

    scheduledJobs = [{

      channels: ["310"],
      id: "job-retry",
      item: {},
      name: "Retry Show",
      // eslint-disable-next-line camelcase -- DVR wire protocol field name.
      start_time: Math.floor((clock.now() + 50000) / 1000)
    }];

    pretune.startPretunePolling(deps);

    // Fire the per-job timer at +20s (before the 60s interval). Its callback enters the retry loop; attempt 1 throws and parks on the first RETRY_DELAY_MS sleep on
    // the virtual clock.
    clock.advance(5000);
    await settle();
    clock.advance(15000);
    await settle();
    await settle();

    assert.equal(initializeStreamSpy.mock.callCount(), 1, "the per-job timer ran the first attempt, which threw and parked on the retry sleep");

    // Release one RETRY_DELAY_MS sleep per remaining attempt. Advancing the clock is what resolves each parked sleep, so the count rises by exactly one per step -
    // a regression that retried without waiting, or that never woke, breaks a step rather than the final total alone.
    for(let attempt = 2; attempt <= 5; attempt++) {

      clock.advance(5000);

      // eslint-disable-next-line no-await-in-loop -- each attempt has to reach the queue before the next sleep is released.
      await settle();

      assert.equal(initializeStreamSpy.mock.callCount(), attempt, "each released retry sleep runs exactly one further attempt");
    }

    // A further advance runs nothing more: the loop caps at MAX_RETRIES rather than retrying forever.
    clock.advance(5000);
    await settle();

    assert.equal(initializeStreamSpy.mock.callCount(), 5, "a throwing initializeStream must be retried up to MAX_RETRIES (5) attempts");
  });

  test("pretune retries are abandoned as soon as clock.now() passes start_time", async () => {

    /* The retry loop's abandonment guard: once the clock passes the scheduled start_time there is no point retrying - the recording has begun and a late pretune
     * would only spin up a stream nobody is waiting on. pretuneChannel reads the clock after each attempt, compares it against startTimeMs, and returns early. The
     * injected initializeStream jumps the virtual clock past start_time on its first (and only) attempt, so the post-attempt guard trips and the loop returns
     * without sleeping or retrying.
     *
     * Non-vacuous by construction: were the guard removed, the loop would park on the retry sleep, and the advance below would release it into a second attempt.
     * With the guard intact the count stays 1, so a broken guard surfaces as a count above 1.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateEnabledServices([]);
    await mutateChannels((data) => {

      data.channels["pabandon"] = { name: "PAbandon", url: "https://example.test/pabandon" };
    });

    const clock = new TestClock(BASE_TIME_MS);
    const deps = makeDeps(clock);

    // The scheduled start in epoch milliseconds. The spy jumps the mocked clock just past it so the guard trips right after the first attempt.
    const startMs = Math.floor((clock.now() + 50000) / 1000) * 1000;

    initializeStreamSpy.mock.mockImplementation(async () => {

      // Jump the virtual clock past start_time so pretuneChannel's post-attempt guard abandons the loop. The advance is synchronous and crosses no other
      // deadline: the first poll fired at 5s, the per-job timer at 20s, and the poll interval sits at 60s.
      clock.advance((startMs + 1000) - clock.now());

      throw new Error("pretune capture failed");
    });
    ctx.registerCleanup(() => { initializeStreamSpy.mock.mockImplementation(async () => 999); });

    deviceGuideMap = new Map([[ "311", "pabandon" ]]);

    scheduledJobs = [{

      channels: ["311"],
      id: "job-abandon",
      item: {},
      name: "Abandon Show",
      // eslint-disable-next-line camelcase -- DVR wire protocol field name.
      start_time: startMs / 1000
    }];

    pretune.startPretunePolling(deps);

    // Fire the per-job timer at +20s (before the 60s interval). Attempt 1 runs, jumps the clock past start_time, and throws; the guard must then abandon.
    clock.advance(5000);
    await settle();
    clock.advance(15000);
    await settle();
    await settle();

    // Release a full retry sleep. A broken guard would have parked on that sleep and would wake here into a second attempt; an intact guard already returned, so
    // nothing is waiting and the count holds.
    clock.advance(5000);
    await settle();

    assert.equal(initializeStreamSpy.mock.callCount(), 1, "retries must be abandoned once the clock passes start_time; no second attempt after the guard trips");
  });

  test("cancelled and skipped jobs are never scheduled while an eligible job still fires", async () => {

    /* The pre-schedule skip for lifecycle flags: pollForUpcomingJobs drops any job with item.cancelled or a top-level skipped before it ever creates a timer, so
     * a recording the user cancelled or skipped never triggers a speculative tune. The eligible control job proves the polling and scheduling machinery ran - a
     * bare 0-count could otherwise pass for the wrong reason - and its single pretune call, identified by channel, confirms only the eligible job scheduled.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateEnabledServices([]);
    await mutateChannels((data) => {

      data.channels["pcancel"] = { name: "PCancel", url: "https://example.test/pcancel" };
      data.channels["pctrl"] = { name: "PCtrl", url: "https://example.test/pctrl" };
      data.channels["pskip"] = { name: "PSkip", url: "https://example.test/pskip" };
    });

    const clock = new TestClock(BASE_TIME_MS);
    const deps = makeDeps(clock);

    deviceGuideMap = new Map([ [ "320", "pctrl" ], [ "321", "pcancel" ], [ "322", "pskip" ] ]);

    const startTime = Math.floor((clock.now() + 50000) / 1000);

    scheduledJobs = [
      {

        channels: ["321"],
        id: "job-cancelled",
        item: { cancelled: true },
        name: "Cancelled Show",
        // eslint-disable-next-line camelcase -- DVR wire protocol field name.
        start_time: startTime
      },
      {

        channels: ["322"],
        id: "job-skipped",
        item: {},
        name: "Skipped Show",
        skipped: true,
        // eslint-disable-next-line camelcase -- DVR wire protocol field name.
        start_time: startTime
      },
      {

        channels: ["320"],
        id: "job-control",
        item: {},
        name: "Control Show",
        // eslint-disable-next-line camelcase -- DVR wire protocol field name.
        start_time: startTime
      }
    ];

    pretune.startPretunePolling(deps);

    // Fire the control job's per-job timer at +20s, before the 60s interval, so only the startup poll's scheduling decisions are under test.
    clock.advance(5000);
    await settle();
    clock.advance(15000);
    await settle();
    await settle();

    assert.equal(initializeStreamSpy.mock.callCount(), 1, "only the eligible control job may schedule and fire; cancelled and skipped jobs are never scheduled");

    const call = firstOf(initializeStreamSpy.mock.calls, "initializeStream call");
    const [options] = call.arguments;

    assert.equal((options as { channelName: string }).channelName, "pctrl", "the single pretune must be for the eligible control channel, not the skipped ones");
  });

  test("a job that already has an active timer is not rescheduled on a subsequent poll", async () => {

    /* Per-job registry dedup: when a poll re-observes a job it already armed, the registry's has(job.id) guard skips it so a duplicate timer is never armed.
     * A regression would leave two timers for one job, both firing near the pretune time and calling initializeStream twice. With the start 4 minutes out, the
     * per-job timer survives across two poll cycles; advancing to its firing point must yield exactly one pretune call.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateEnabledServices([]);
    await mutateChannels((data) => {

      data.channels["pdedup"] = { name: "PDedup", url: "https://example.test/pdedup" };
    });

    const clock = new TestClock(BASE_TIME_MS);
    const deps = makeDeps(clock);

    deviceGuideMap = new Map([[ "330", "pdedup" ]]);

    scheduledJobs = [{

      channels: ["330"],
      id: "job-dedup",
      item: {},
      name: "Dedup Show",
      // eslint-disable-next-line camelcase -- DVR wire protocol field name.
      start_time: Math.floor((clock.now() + 240000) / 1000)
    }];

    pretune.startPretunePolling(deps);

    // Startup poll schedules the per-job timer (fires ~205s out). Then advance to the 60s interval poll, which re-observes the same job and must dedup it.
    clock.advance(5000);
    await settle();
    clock.advance(55000);
    await settle();

    // Fire the per-job timer (205s after the startup poll). A single armed timer means exactly one pretune call.
    clock.advance(150000);
    await settle();
    await settle();

    assert.equal(initializeStreamSpy.mock.callCount(), 1, "a job with an active timer must not be rescheduled on a subsequent poll");
  });

  test("a job that disappears from the schedule has its pending timer cleared", async () => {

    /* Stale-timer cleanup: when a previously-scheduled job vanishes from a later poll (cancelled, rescheduled, or moved outside the horizon) while other jobs
     * remain, pollForUpcomingJobs clears its orphaned timer via the seenJobIds sweep so it cannot fire against a recording that is no longer planned. Two jobs
     * are scheduled; the first is removed before the interval poll, and only the survivor may fire - proven by the single pretune call being for the survivor.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateEnabledServices([]);
    await mutateChannels((data) => {

      data.channels["pstalea"] = { name: "PStaleA", url: "https://example.test/pstalea" };
      data.channels["pstaleb"] = { name: "PStaleB", url: "https://example.test/pstaleb" };
    });

    const clock = new TestClock(BASE_TIME_MS);
    const deps = makeDeps(clock);

    deviceGuideMap = new Map([ [ "340", "pstalea" ], [ "341", "pstaleb" ] ]);

    const startTime = Math.floor((clock.now() + 240000) / 1000);

    scheduledJobs = [
      {

        channels: ["340"],
        id: "job-stale-a",
        item: {},
        name: "Stale A Show",
        // eslint-disable-next-line camelcase -- DVR wire protocol field name.
        start_time: startTime
      },
      {

        channels: ["341"],
        id: "job-stale-b",
        item: {},
        name: "Stale B Show",
        // eslint-disable-next-line camelcase -- DVR wire protocol field name.
        start_time: startTime
      }
    ];

    pretune.startPretunePolling(deps);

    // Startup poll schedules both per-job timers.
    clock.advance(5000);
    await settle();

    // Remove the first job from the schedule. The next poll must clear its now-orphaned timer while re-observing and keeping the survivor.
    scheduledJobs = [firstOf(scheduledJobs.slice(1), "surviving job")];

    // Advance to the 60s interval poll, which runs the stale-timer sweep.
    clock.advance(55000);
    await settle();

    // Fire the remaining per-job timer. The cleared timer must not fire, so only the survivor pretunes.
    clock.advance(150000);
    await settle();
    await settle();

    assert.equal(initializeStreamSpy.mock.callCount(), 1, "the disappeared job's timer must be cleared; only the surviving job may fire");

    const call = firstOf(initializeStreamSpy.mock.calls, "initializeStream call");
    const [options] = call.arguments;

    assert.equal((options as { channelName: string }).channelName, "pstaleb", "the single pretune must be for the surviving job, not the disappeared one");
  });

  test("a job outside the scheduling horizon is skipped while a nearer job fires", async () => {

    /* The horizon skip: pollForUpcomingJobs ignores jobs whose start is beyond SCHEDULING_HORIZON_MS (5 minutes) so it does not tune far too early. The far job
     * starts well past the horizon and must never schedule within the ticked window; the nearer job proves the machinery ran, so the far job's silence is a real
     * skip rather than an idle poll.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateEnabledServices([]);
    await mutateChannels((data) => {

      data.channels["pfar"] = { name: "PFar", url: "https://example.test/pfar" };
      data.channels["pnear"] = { name: "PNear", url: "https://example.test/pnear" };
    });

    const clock = new TestClock(BASE_TIME_MS);
    const deps = makeDeps(clock);

    deviceGuideMap = new Map([ [ "350", "pfar" ], [ "351", "pnear" ] ]);

    scheduledJobs = [
      {

        channels: ["350"],
        id: "job-far",
        item: {},
        name: "Far Show",
        // eslint-disable-next-line camelcase -- DVR wire protocol field name.
        start_time: Math.floor((clock.now() + 400000) / 1000)
      },
      {

        channels: ["351"],
        id: "job-near",
        item: {},
        name: "Near Show",
        // eslint-disable-next-line camelcase -- DVR wire protocol field name.
        start_time: Math.floor((clock.now() + 50000) / 1000)
      }
    ];

    pretune.startPretunePolling(deps);

    // Startup poll schedules only the near job (fires 15s later); the far job is beyond the horizon and is skipped. Stop before the 60s interval poll so the far
    // job is never re-evaluated as time advances toward it.
    clock.advance(5000);
    await settle();
    clock.advance(15000);
    await settle();
    await settle();

    assert.equal(initializeStreamSpy.mock.callCount(), 1, "a job outside the scheduling horizon must be skipped while the nearer job fires");

    const call = firstOf(initializeStreamSpy.mock.calls, "initializeStream call");
    const [options] = call.arguments;

    assert.equal((options as { channelName: string }).channelName, "pnear", "the single pretune must be for the in-horizon job, not the far one");
  });

  test("a job whose start has already passed is skipped while a future job fires", async () => {

    /* The already-started skip: pollForUpcomingJobs ignores jobs whose start is at or before now, so a recording already in progress is never speculatively
     * tuned. The past job's start is before the clock; the future job proves the poll ran and asserts that only it schedules.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateEnabledServices([]);
    await mutateChannels((data) => {

      data.channels["ppast"] = { name: "PPast", url: "https://example.test/ppast" };
      data.channels["pnear2"] = { name: "PNear2", url: "https://example.test/pnear2" };
    });

    const clock = new TestClock(BASE_TIME_MS);
    const deps = makeDeps(clock);

    deviceGuideMap = new Map([ [ "360", "ppast" ], [ "361", "pnear2" ] ]);

    scheduledJobs = [
      {

        channels: ["360"],
        id: "job-past",
        item: {},
        name: "Past Show",
        // eslint-disable-next-line camelcase -- DVR wire protocol field name.
        start_time: Math.floor((clock.now() - 10000) / 1000)
      },
      {

        channels: ["361"],
        id: "job-future",
        item: {},
        name: "Future Show",
        // eslint-disable-next-line camelcase -- DVR wire protocol field name.
        start_time: Math.floor((clock.now() + 50000) / 1000)
      }
    ];

    pretune.startPretunePolling(deps);

    clock.advance(5000);
    await settle();
    clock.advance(15000);
    await settle();
    await settle();

    assert.equal(initializeStreamSpy.mock.callCount(), 1, "a job whose start has already passed must be skipped while the future job fires");

    const call = firstOf(initializeStreamSpy.mock.calls, "initializeStream call");
    const [options] = call.arguments;

    assert.equal((options as { channelName: string }).channelName, "pnear2", "the single pretune must be for the future job, not the already-started one");
  });

  test("a job whose channels[0] is empty is skipped while a resolvable job fires", async () => {

    /* The empty-guide skip: a job whose preferred channel entry is empty yields no guide number to resolve, so pretune skips it before resolving the guide
     * number against the device mappings. The control job with a resolvable guide proves the poll ran and asserts that only it schedules.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateEnabledServices([]);
    await mutateChannels((data) => {

      data.channels["pchanctrl"] = { name: "PChanCtrl", url: "https://example.test/pchanctrl" };
    });

    const clock = new TestClock(BASE_TIME_MS);
    const deps = makeDeps(clock);

    deviceGuideMap = new Map([[ "370", "pchanctrl" ]]);

    const startTime = Math.floor((clock.now() + 50000) / 1000);

    scheduledJobs = [
      {

        channels: [""],
        id: "job-empty-channel",
        item: {},
        name: "Empty Channel Show",
        // eslint-disable-next-line camelcase -- DVR wire protocol field name.
        start_time: startTime
      },
      {

        channels: ["370"],
        id: "job-channel-control",
        item: {},
        name: "Channel Control Show",
        // eslint-disable-next-line camelcase -- DVR wire protocol field name.
        start_time: startTime
      }
    ];

    pretune.startPretunePolling(deps);

    // Fire the resolvable control job's per-job timer at +20s, before the 60s interval.
    clock.advance(5000);
    await settle();
    clock.advance(15000);
    await settle();
    await settle();

    assert.equal(initializeStreamSpy.mock.callCount(), 1, "a job with an empty channels[0] must be skipped while the resolvable job fires");

    const call = firstOf(initializeStreamSpy.mock.calls, "initializeStream call");
    const [options] = call.arguments;

    assert.equal((options as { channelName: string }).channelName, "pchanctrl", "the single pretune must be for the resolvable-guide job");
  });

  test("a job whose guide number resolves to no PrismCast channel is skipped while a mapped job fires", async () => {

    /* The unresolved-guide skip: a guide number the device mappings do not know maps to no PrismCast channel, so resolveGuideNumber returns undefined and pretune
     * skips the job. This guards against guide-mapping drift where the DVR references a channel PrismCast no longer serves. The mapped control job proves the poll
     * ran and asserts that only it schedules.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateEnabledServices([]);
    await mutateChannels((data) => {

      data.channels["pguidectrl"] = { name: "PGuideCtrl", url: "https://example.test/pguidectrl" };
    });

    const clock = new TestClock(BASE_TIME_MS);
    const deps = makeDeps(clock);

    // Only the control guide is mapped; "99999" resolves to nothing.
    deviceGuideMap = new Map([[ "380", "pguidectrl" ]]);

    const startTime = Math.floor((clock.now() + 50000) / 1000);

    scheduledJobs = [
      {

        channels: ["99999"],
        id: "job-unresolved-guide",
        item: {},
        name: "Unresolved Guide Show",
        // eslint-disable-next-line camelcase -- DVR wire protocol field name.
        start_time: startTime
      },
      {

        channels: ["380"],
        id: "job-guide-control",
        item: {},
        name: "Guide Control Show",
        // eslint-disable-next-line camelcase -- DVR wire protocol field name.
        start_time: startTime
      }
    ];

    pretune.startPretunePolling(deps);

    // Fire the mapped control job's per-job timer at +20s, before the 60s interval.
    clock.advance(5000);
    await settle();
    clock.advance(15000);
    await settle();
    await settle();

    assert.equal(initializeStreamSpy.mock.callCount(), 1, "a job whose guide resolves to no PrismCast channel must be skipped while the mapped job fires");

    const call = firstOf(initializeStreamSpy.mock.calls, "initializeStream call");
    const [options] = call.arguments;

    assert.equal((options as { channelName: string }).channelName, "pguidectrl", "the single pretune must be for the mapped-guide job");
  });

  test("an empty device-mappings result short-circuits the whole poll", async () => {

    /* The empty-mappings short-circuit: if getDeviceMappings returns nothing, pretune cannot resolve any guide number, so pollForUpcomingJobs returns before
     * scheduling anything - even for an otherwise-eligible job. The fetchFromDvr spy still records the poll, proving the short-circuit happened after data
     * acquisition (on empty mappings) rather than because the poll never ran.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateEnabledServices([]);
    await mutateChannels((data) => {

      data.channels["pmap"] = { name: "PMap", url: "https://example.test/pmap" };
    });

    const clock = new TestClock(BASE_TIME_MS);
    const deps = makeDeps(clock);

    // A deps variant whose device mappings are empty, so mappings.size === 0 short-circuits the poll. Everything else mirrors the shared deps.
    const emptyMappingDeps: pretune.PretuneDeps = { ...deps, getDeviceMappings: async (): Promise<Map<string, Map<string, string>>> => new Map() };

    deviceGuideMap = new Map([[ "390", "pmap" ]]);

    scheduledJobs = [{

      channels: ["390"],
      id: "job-empty-mappings",
      item: {},
      name: "Empty Mappings Show",
      // eslint-disable-next-line camelcase -- DVR wire protocol field name.
      start_time: Math.floor((clock.now() + 50000) / 1000)
    }];

    pretune.startPretunePolling(emptyMappingDeps);

    // Run the startup poll (and a little beyond); the empty mappings must short-circuit it before any per-job timer is armed.
    clock.advance(5000);
    await settle();
    clock.advance(15000);
    await settle();
    await settle();

    assert.ok(fetchFromDvrSpy.mock.callCount() >= 1, "the DVR jobs endpoint must be polled, so the short-circuit is on empty mappings, not a skipped poll");
    assert.equal(initializeStreamSpy.mock.callCount(), 0, "an empty device-mappings result must short-circuit the poll before any pretune is scheduled");
  });
});

describe("the poll reaches the DVR address the running configuration holds", () => {

  afterEach(() => {

    pretune.stopPretunePolling();
    fetchFromDvrSpy.mock.resetCalls();
  });

  test("the first poll reaches the host the boot read from the file, and a later poll reaches the host and port the running configuration moved to", async () => {

    /* The scheduler takes the production host accessor here rather than the stub the rows above share, so each poll reads the DVR host from the running
     * configuration and the port with it; the DVR data layer stays the spy, so nothing leaves the process. The boot reads the host from the file before the
     * scheduler starts, so the first poll has nothing left to wait on, and a value a save commits reaches the next poll.
     */
    await using ctx = await createIntegrationContext();

    await writePersistedJson(ctx, "config.json", { channelsDvr: { host: "pretune-dvr-a.example.invalid" } });
    await initializeConfiguration();
    await initializePersistence(ctx);

    const clock = new TestClock(BASE_TIME_MS);

    pretune.startPretunePolling({ ...makeDeps(clock), getDvrHost });

    clock.advance(5000);
    await settle();

    assert.deepEqual(fetchFromDvrSpy.mock.calls.map((call) => [ call.arguments[0], call.arguments[1] ]), [[ "pretune-dvr-a.example.invalid", 8089 ]],
      "the first poll reaches the host the boot read, at the running port");

    // The commit a save's reconcile makes once the DVR handler has realized a new host and port, leaf by leaf.
    CONFIG.channelsDvr.host = "pretune-dvr-b.example.invalid";
    CONFIG.channelsDvr.port = 19191;

    clock.advance(60000);
    await settle();

    const latest = fetchFromDvrSpy.mock.calls.at(-1);

    assert.deepEqual([ latest?.arguments[0], latest?.arguments[1] ], [ "pretune-dvr-b.example.invalid", 19191 ], "the next poll reaches the running host and port");
  });
});
