/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * monitor.test.ts: Unit tests for the playback health monitor's tick discipline. monitorPlaybackHealth offers no injection point for its recovery actions - they
 * drive a real Chrome through the browser layer - so the coverage here is the part that is drivable without one: how the monitor schedules and bounds its health
 * reads, and what it does with a read that fails or a recovery that finishes after the stream is already gone. The pins run the real module against a Page double
 * and node:test's mock clock, so what they exercise is the shipped code path, not a re-implementation of it.
 *
 * Two mechanics make that possible and are baked into every pin below. The clock advances in steps no larger than one monitor interval, because a single large
 * step fires the interval once and silently skips the nested timer firings a real run would see. And microtasks are flushed between a settlement and the next
 * step, because a rejection surfacing through the evaluate wrapper, the tick's catch, and the dispatcher's finally needs one turn per link before the next tick
 * can observe the result.
 *
 * The recovery-action interiors (tab replacement, source reload, fullscreen reinforcement, segment-health escalation) still need a real browser and a live
 * capture pipeline, and stay with the e2e tier. The pure decision helpers they rest on - checkCircuitBreaker, getIssueCategory, formatIssueType,
 * recordRecoveryAttempt/Success, getRecoveryMethod - live in recovery.ts and are covered by recovery.test.ts.
 */
import type { MonitorHandle, TabReplacementResult } from "./recovery.ts";
import { closePuppeteerStreamWssOnIdle, flushMicrotasks, makeFakePage } from "../testing.helpers.ts";
import { describe, test } from "node:test";
import { emitStreamAdded, emitStreamRemoved, subscribeToStatus } from "./statusEmitter.ts";
import { CONFIG } from "../config/index.ts";
import type { CaptureImpairment } from "../browser/index.ts";
import { LOG } from "../utils/index.ts";
import type { MonitorDeps } from "./monitor.ts";
import type { Nullable } from "../types/index.ts";
import type { StreamStatus } from "./statusEmitter.ts";
import type { TestContext } from "node:test";
import assert from "node:assert/strict";
import { makeProfile } from "../config/profiles.helpers.ts";
import { monitorPlaybackHealth } from "./monitor.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

// The cadence the monitor ticks at. Read from configuration rather than hard-coded, so a pin that depends on the cadence steps in true intervals.
const MONITOR_INTERVAL = CONFIG.playback.monitorInterval;

// The evaluate wrapper's default bound, which is what detects a hung tab on the first read of a streak.
const DEFAULT_EVALUATE_TIMEOUT = 15000;

// The bound the monitor issues confirmation probes under once a timeout streak is open. Held here as a literal on purpose: the monitor's own constant is closure
// scoped, so pinning the contracted value from outside is what makes a change to it visible.
const UNRESPONSIVE_PROBE_TIMEOUT = 2000;

// A video state with no video element present, which is what sends the tick down the video-not-found ladder.
const NO_VIDEO = null;

// What the presence check returns when the DOM holds no video at all.
const NO_PRESENCE = { anyVideoExists: false, readyVideoFound: false, videoCount: 0 };

// What the validation read returns when the re-search finds no usable video.
const NOT_VALIDATED = { found: false };

/* A readable video state that costs the tick exactly one evaluate. The intrinsic dimensions are zero so resolution monitoring returns before it measures, and
 * readyState sits below the ready threshold so the fullscreen reinforcement block - the tick's other evaluate - is skipped. That keeps evaluate counts in the
 * pins a direct reading of how many health reads the monitor issued.
 * @param currentTime - The playback position to report, so successive reads can show progression.
 * @returns The state object a getVideoState read resolves with.
 */
function readableState(currentTime: number): Record<string, unknown> {

  return {

    currentTime,
    ended: false,
    error: false,
    muted: false,
    networkState: 2,
    paused: false,
    readyState: 2,
    videoHeight: 0,
    videoWidth: 0,
    volume: 1
  };
}

/* A readable state carrying real intrinsic dimensions, so the resolution check runs its full body instead of returning at the zero-dimension guard. Every reading
 * is the same size, so the first one sets the stream's peak and each one after is full quality against it and triggers no recovery.
 * @param currentTime - The playback position to report, so successive reads can show progression.
 * @returns The state object a getVideoState read resolves with.
 */
function resolutionReadableState(currentTime: number): Record<string, unknown> {

  return { ...readableState(currentTime), videoHeight: 720, videoWidth: 1280 };
}

/**
 * Advances the mock clock, in steps no larger than one monitor interval, flushing microtasks after each step.
 * @param t - The test context owning the mock timers.
 * @param totalMs - How far to advance.
 */
async function advance(t: TestContext, totalMs: number): Promise<void> {

  let remaining = totalMs;

  while(remaining > 0) {

    const step = Math.min(remaining, MONITOR_INTERVAL);

    t.mock.timers.tick(step);
    remaining -= step;

    // Sequential by definition: each step must let the work it caused settle before the next step fires.
    // eslint-disable-next-line no-await-in-loop
    await flushMicrotasks();
  }
}

// The impairment a marked browser reports, and the deps object that reports it. The recovery ladder consults this read before offering tab replacement, so
// substituting it is what lets a pin drive the ladder's availability decision with no browser anywhere in the picture.
const IMPAIRED: CaptureImpairment = { reason: "Could not start video source", since: 0 };

const IMPAIRED_DEPS: MonitorDeps = {

  getCaptureImpairment: (): Nullable<CaptureImpairment> => IMPAIRED,
  syncWindowVisibility: async (): Promise<void> => undefined
};

const HEALTHY_DEPS: MonitorDeps = {

  getCaptureImpairment: (): Nullable<CaptureImpairment> => null,
  syncWindowVisibility: async (): Promise<void> => undefined
};

/**
 * Starts a monitor against a Page double, with the minimal profile that keeps the tune path shallow: no channel selection, no iframe search, no click-to-play.
 * @param page - The Page double to monitor.
 * @param streamId - The stream id string for log context and abort lookup.
 * @param numericStreamId - The numeric stream id the status and registry lookups use.
 * @param options - The collaborators a pin substitutes: a tab-replacement handler, a circuit-break stub, and the browser-boundary deps. Each defaults to what the
 *                  monitor sees in the pins written before they existed - no handler, a no-op break, and the real defaults.
 * @returns The monitor handle.
 */
function startMonitor(page: ReturnType<typeof makeFakePage>["page"], streamId: string, numericStreamId: number, options: {
  deps?: MonitorDeps;
  onCircuitBreak?: () => void;
  onTabReplacement?: () => Promise<Nullable<TabReplacementResult>>;
} = {}): MonitorHandle {

  return monitorPlaybackHealth(page, page, makeProfile(), "https://monitor.test/watch", streamId, {

    channelName: "Monitor Test",
    numericStreamId,
    serviceName: "monitor-test",
    startTime: new Date()
  }, options.onCircuitBreak ?? ((): void => { /* The circuit-break callback is not what these pins exercise. */ }), options.onTabReplacement, options.deps);
}

/**
 * Captures LOG.warn and LOG.error messages for the life of a test, returning the format strings in emission order. Capturing the format string rather than the
 * rendered line is enough to tell the monitor's messages apart and keeps the assertions independent of interpolated values.
 * @param t - The test context whose mock registry restores the methods at test end.
 * @returns The array the captured messages accumulate into.
 */
function captureLogs(t: TestContext): string[] {

  const messages: string[] = [];

  t.mock.method(LOG, "warn", (message: string) => { messages.push(message); });
  t.mock.method(LOG, "error", (message: string) => { messages.push(message); });

  return messages;
}

/**
 * Counts captured messages that begin with the supplied prefix.
 * @param messages - The captured messages.
 * @param prefix - The message prefix to count.
 * @returns How many messages start with the prefix.
 */
function countMessages(messages: string[], prefix: string): number {

  return messages.filter((message) => message.startsWith(prefix)).length;
}

describe("monitorPlaybackHealth", () => {

  test("is exported as a function with the documented signature", () => {

    // Smoke test: lock the public-API contract that other modules import. If a future refactor inadvertently turns monitorPlaybackHealth into a default export
    // or removes it, callers (notably setup.ts) break - this test surfaces the change.
    assert.equal(typeof monitorPlaybackHealth, "function", "monitorPlaybackHealth is a function");
  });

  test("runs one health read at a time: interval firings during an outstanding read are skipped", async (t) => {

    /* The incident this pin exists for: a hung tab left one read outstanding for the full evaluate bound while the interval kept firing, and every firing that
     * landed in that window started another tick body against the same counters. Here the read is never answered, so any firing that dispatched a body would
     * show up as a second evaluate.
     */
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const fake = makeFakePage();
    const handle = startMonitor(fake.page, "serialize-1", 9001);

    await advance(t, MONITOR_INTERVAL * 5);

    assert.equal(fake.evaluations.length, 1, "five interval firings, one health read");

    // Answer the outstanding read. The next firing is then free to run, which is the other half of the contract: serialization must not starve the monitor.
    fake.evaluations[0]?.resolve(readableState(1));

    await flushMicrotasks();
    await advance(t, MONITOR_INTERVAL);

    assert.equal(fake.evaluations.length, 2, "the next firing runs once the previous body settled");

    handle.dispose();
  });

  test("reads the capture surface once for the monitor's lifetime rather than once per tick", async (t) => {

    /* The quality preset is restart-gated, so a stream's capture surface cannot change while that stream runs. Re-deriving it on every two-second tick would
     * spend work to reach the same answer forever. The values alone cannot tell a once-per-lifetime read from a once-per-tick one, so the pin counts reads: a
     * counting accessor stands in front of the configured preset, which is the single property the viewport getter consults.
     */
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const configured = CONFIG.streaming.qualityPreset;
    const descriptor = Object.getOwnPropertyDescriptor(CONFIG.streaming, "qualityPreset");

    let presetReads = 0;

    Object.defineProperty(CONFIG.streaming, "qualityPreset", {

      configurable: true,
      get: (): string => {

        presetReads++;

        return configured;
      }
    });

    try {

      const fake = makeFakePage();
      const handle = startMonitor(fake.page, "surface-read-1", 9010);

      // Four ticks, each answered with a healthy state carrying real intrinsic dimensions, so the resolution comparison runs its full body every time.
      for(let tick = 0; tick < 4; tick++) {

        // Sequential by definition: each tick's read must settle before the next firing.
        // eslint-disable-next-line no-await-in-loop
        await advance(t, MONITOR_INTERVAL);
        fake.evaluations[tick]?.resolve(resolutionReadableState(tick + 1));

        // eslint-disable-next-line no-await-in-loop
        await flushMicrotasks();
      }

      handle.dispose();

      assert.equal(fake.evaluations.length, 4, "four ticks issued four health reads");
      assert.equal(presetReads, 1, "the capture surface was read once across all four ticks");
    } finally {

      if(descriptor) {

        Object.defineProperty(CONFIG.streaming, "qualityPreset", descriptor);
      }
    }
  });

  test("emits status on the firings it skips, so subscribers stay current during a long read", async (t) => {

    // The skip path is the common case under a hung tab, so it has to keep feeding the status stream that the web UI reads. The stream is registered with the
    // emitter first because the emitter drops updates for streams it has never seen.
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const numericStreamId = 9002;

    // The emitter reads only the id to register the stream, so an id-carrying literal is the whole fixture it needs.
    emitStreamAdded({ id: numericStreamId } as unknown as StreamStatus);

    let healthEvents = 0;

    const unsubscribe = subscribeToStatus((event) => {

      if(event === "streamHealthChanged") {

        healthEvents++;
      }
    });

    const fake = makeFakePage();
    const handle = startMonitor(fake.page, "skip-status-1", numericStreamId);

    await advance(t, MONITOR_INTERVAL * 4);

    assert.equal(fake.evaluations.length, 1, "the read is still outstanding");
    assert.ok(healthEvents >= 3, "the skipped firings emitted status, giving " + String(healthEvents) + " updates");

    handle.dispose();
    unsubscribe();
    emitStreamRemoved(numericStreamId);
  });

  test("reports the source's own size beside the surface capture encodes at", async (t) => {

    /* The pair is the point: the surface comes from the configured preset and the source size from the tick's own reading, so an operator can see a 720p source
     * being captured at 1080p. The fixture makes the two values differ - a 1920x1080 reading under the default 720p preset - so a swap or a cross-derivation
     * cannot pass by rendering the same number twice.
     */
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const numericStreamId = 9011;

    // The emitter drops updates for streams it has never seen, so the stream is registered before the monitor starts.
    emitStreamAdded({ id: numericStreamId } as unknown as StreamStatus);

    const payloads: StreamStatus[] = [];

    const unsubscribe = subscribeToStatus((event, data) => {

      if(event === "streamHealthChanged") {

        payloads.push(data as StreamStatus);
      }
    });

    const fake = makeFakePage();
    const handle = startMonitor(fake.page, "resolution-report-1", numericStreamId);

    await advance(t, MONITOR_INTERVAL);
    fake.evaluations[0]?.resolve({ ...readableState(1), videoHeight: 1080, videoWidth: 1920 });
    await flushMicrotasks();

    const reported = payloads.at(-1);

    assert.ok(reported, "the tick emitted a status update");
    assert.equal(reported.sourceResolution, "1920x1080", "the source size is the reading the tick took");
    assert.equal(reported.captureResolution, "1280x720", "the capture size is the configured surface");

    handle.dispose();
    unsubscribe();
    emitStreamRemoved(numericStreamId);
  });

  test("never judges a source degraded for staying at the best it has ever delivered", async (t) => {

    /* The false-positive class this replaces: a 480x270 source read against a 1280x720 capture surface is 37 percent of it in either dimension, so the surface
     * comparison called forty steady readings a degradation and reloaded the page. Measured against the stream's own peak - which this source sets on its first
     * reading and then matches - the same forty readings are full quality.
     */
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const messages = captureLogs(t);
    const fake = makeFakePage();
    const handle = startMonitor(fake.page, "resolution-steady-1", 9012);

    for(let tick = 0; tick < 40; tick++) {

      // Sequential by definition: each tick's read must settle before the next firing.
      // eslint-disable-next-line no-await-in-loop
      await advance(t, MONITOR_INTERVAL);
      fake.evaluations[tick]?.resolve({ ...readableState(tick + 1), videoHeight: 270, videoWidth: 480 });

      // eslint-disable-next-line no-await-in-loop
      await flushMicrotasks();
    }

    handle.dispose();

    assert.equal(countMessages(messages, "Video resolution has been degraded for"), 0, "a source at its own best is never judged degraded");
    assert.equal(fake.navigations.length, 0, "and no recovery navigation was issued");
  });

  test("rides out a one-rung adaptive downshift without recovering against it", async (t) => {

    /* The exposure this threshold closes: sixteen readings at 1600x900 establish the peak, then the service steps one rung down to 1024x576 - 41 percent of
     * that peak by area, the ordinary pacing of an adaptive stream rather than a collapse. Every recovery the detector drives is a capture restart, so a dip of
     * this size has to ride: no degradation warning and no recovery navigation across the same forty ticks that fire for a genuine collapse.
     */
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const messages = captureLogs(t);
    const fake = makeFakePage();
    const handle = startMonitor(fake.page, "resolution-onerung-1", 9014);

    for(let tick = 0; tick < 40; tick++) {

      // Sequential by definition: each tick's read must settle before the next firing.
      // eslint-disable-next-line no-await-in-loop
      await advance(t, MONITOR_INTERVAL);
      fake.evaluations[tick]?.resolve((tick < 16) ? { ...readableState(tick + 1), videoHeight: 900, videoWidth: 1600 } :
        { ...readableState(tick + 1), videoHeight: 576, videoWidth: 1024 });

      // eslint-disable-next-line no-await-in-loop
      await flushMicrotasks();
    }

    handle.dispose();

    assert.equal(countMessages(messages, "Video resolution has been degraded for"), 0, "a one-rung downshift is not a degradation");
    assert.equal(fake.navigations.length, 0, "and no recovery navigation was issued");
  });

  test("still recovers a drop to below half the picture the stream had been delivering", async (t) => {

    /* The other direction, and the behavior the detector exists for: sixteen readings at 1280x720 establish the peak, then the source sticks on 480x270 - 14
     * percent of that peak by area. The ladder's first step still fires, one reading count later than the grace window and the count threshold together allow,
     * and it issues exactly one page navigation.
     */
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const messages = captureLogs(t);
    const fake = makeFakePage();
    const handle = startMonitor(fake.page, "resolution-drop-1", 9013);

    let warnTick = -1;

    for(let tick = 0; tick < 40; tick++) {

      // Sequential by definition: each tick's read must settle before the next firing.
      // eslint-disable-next-line no-await-in-loop
      await advance(t, MONITOR_INTERVAL);
      fake.evaluations[tick]?.resolve((tick < 16) ? resolutionReadableState(tick + 1) : { ...readableState(tick + 1), videoHeight: 270, videoWidth: 480 });

      // eslint-disable-next-line no-await-in-loop
      await flushMicrotasks();

      if(countMessages(messages, "Video resolution has been degraded for") > 0) {

        warnTick = tick;

        break;
      }
    }

    assert.equal(countMessages(messages, "Video resolution has been degraded for"), 1, "the ladder warned exactly once");
    assert.equal(warnTick, 30, "the warn landed on the reading that crossed the count threshold");

    await flushMicrotasks();

    assert.equal(fake.navigations.length, 1, "the warn was followed by exactly one page navigation");

    // The navigation is left pending by the double, so it is failed here and allowed to settle before the monitor is disposed.
    fake.navigations[0]?.reject(new Error("net::ERR_ABORTED"));

    await flushMicrotasks();

    handle.dispose();
  });

  test("bounds a read issued during a timeout streak by the short confirmation probe", async (t) => {

    /* Both bounds are pinned here. The first read carries the full-length bound, which is what detects the hang. The read that follows carries the probe bound:
     * the streak is still open at that point, and a tab that answers evaluates at all answers well inside it. The two assertions bracket the contracted value -
     * a shorter bound would strike before the first assertion, and the full-length default would not have struck by the second.
     */
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const messages = captureLogs(t);
    const fake = makeFakePage();
    const handle = startMonitor(fake.page, "probe-1", 9003);

    await advance(t, MONITOR_INTERVAL);

    assert.equal(fake.evaluations.length, 1, "the first firing issued a read");

    await advance(t, DEFAULT_EVALUATE_TIMEOUT);

    assert.equal(countMessages(messages, "Monitor check timed out"), 1, "the full-length bound produced the first strike");

    // The next firing issues the confirmation probe. Its dispatch time is what the two bracketing advances below are measured from.
    await advance(t, MONITOR_INTERVAL);

    assert.equal(fake.evaluations.length, 2, "the streak's next read was issued");

    const probeIssuedAt = fake.evaluations[1]?.at ?? 0;

    assert.equal(probeIssuedAt, Date.now(), "the probe was issued at the current clock value");

    await advance(t, UNRESPONSIVE_PROBE_TIMEOUT - 100);

    assert.equal(countMessages(messages, "Monitor check timed out"), 1, "the probe has not lapsed a hundred milliseconds short of its bound");

    await advance(t, 200);

    assert.equal(countMessages(messages, "Monitor check timed out"), 2, "the probe lapsed at its bound rather than at the full-length default");

    handle.dispose();
  });

  test("routes a page-death read failure into a context re-search within the same tick", async (t) => {

    // A destroyed execution context means the video may simply live in a context other than the one the monitor holds, so the tick re-searches instead of
    // treating the failure as a dead player. The follow-up read inside the same tick is the re-search's validation of the context it found.
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const fake = makeFakePage({ onEvaluate: (call, index) => {

      if(index === 0) {

        call.reject(new Error("Execution context was destroyed, most likely because of a navigation"));
      }
    } });

    const handle = startMonitor(fake.page, "routing-1", 9004);

    await advance(t, MONITOR_INTERVAL);

    assert.equal(fake.evaluations.length, 2, "the tick re-searched after the context died");

    handle.dispose();
  });

  test("does not route an unrelated failure into a context re-search, even when it carries a page-death word", async (t) => {

    /* The narrowing this pins: "destroyed" on its own describes plenty of failures that have nothing to do with a dead page, and treating them as page death
     * sends real errors into recovery machinery built for something else. Such a failure belongs to the tick's general error handling instead, which is what
     * the single read and the failure log together show.
     */
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const messages = captureLogs(t);
    const fake = makeFakePage({ onEvaluate: (call, index) => {

      if(index === 0) {

        call.reject(new Error("The object was destroyed elsewhere"));
      }
    } });

    const handle = startMonitor(fake.page, "routing-2", 9005);

    await advance(t, MONITOR_INTERVAL);

    assert.equal(fake.evaluations.length, 1, "no re-search followed the unrelated failure");
    assert.equal(countMessages(messages, "Monitor check failed"), 1, "the failure went to the tick's general error handling");

    handle.dispose();
  });

  test("does not route a plain protocol failure into a context re-search", async (t) => {

    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const messages = captureLogs(t);
    const fake = makeFakePage({ onEvaluate: (call, index) => {

      if(index === 0) {

        call.reject(new Error("Protocol error: something unrelated"));
      }
    } });

    const handle = startMonitor(fake.page, "routing-3", 9006);

    await advance(t, MONITOR_INTERVAL);

    assert.equal(fake.evaluations.length, 1, "no re-search followed the protocol failure");
    assert.equal(countMessages(messages, "Monitor check failed"), 1, "the failure went to the tick's general error handling");

    handle.dispose();
  });

  test("applies nothing when a recovery finishes after the stream is already gone", async (t) => {

    /* The incident's core. The video-not-found ladder escalates to page navigation, the navigation is still in flight when the stream terminates, and the
     * navigation then fails. What must not happen is the resumption running anyway: it would mark a discontinuity, open a grace window, and tally a navigation
     * failure, all of them bookkeeping for a stream that has already ended. The distinguishing observable is the resumption's own log line - the recovery's
     * failure line above it still emits, which is what shows the recovery genuinely resumed rather than never running at all.
     */
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const messages = captureLogs(t);
    const fake = makeFakePage();
    const handle = startMonitor(fake.page, "stale-1", 9007);

    // First pass down the ladder: no video, and no video element anywhere in the DOM.
    await advance(t, MONITOR_INTERVAL);
    fake.evaluations[0]?.resolve(NO_VIDEO);
    await flushMicrotasks();

    assert.equal(fake.evaluations.length, 2, "the tick followed up with a presence check");

    fake.evaluations[1]?.resolve(NO_PRESENCE);
    await flushMicrotasks();

    // Second pass: the ladder re-searches the frames before it escalates.
    await advance(t, MONITOR_INTERVAL);
    fake.evaluations[2]?.resolve(NO_VIDEO);
    await flushMicrotasks();
    fake.evaluations[3]?.resolve(NO_PRESENCE);
    await flushMicrotasks();

    assert.equal(fake.evaluations.length, 5, "the second pass re-searched for the video");

    fake.evaluations[4]?.resolve(NOT_VALIDATED);
    await flushMicrotasks();

    // Third pass: the ladder escalates to page navigation, which the double holds at its first navigation step.
    await advance(t, MONITOR_INTERVAL);
    fake.evaluations[5]?.resolve(NO_VIDEO);
    await flushMicrotasks();
    fake.evaluations[6]?.resolve(NO_PRESENCE);
    await flushMicrotasks();

    assert.equal(fake.navigations.length, 1, "the ladder escalated to page navigation");

    // The stream terminates while the navigation is in flight, and the navigation then fails.
    handle.dispose();
    fake.navigations[0]?.reject(new Error("net::ERR_ABORTED"));
    await flushMicrotasks();

    assert.equal(countMessages(messages, "Failed to reinitialize video after page navigation"), 1, "the recovery resumed and reported its own failure");
    assert.equal(countMessages(messages, "Page navigation did not restore playback"), 0, "the resumption applied nothing after the stream was gone");
    assert.equal(handle.getMetrics().pageNavigationSuccesses, 0, "no recovery success was recorded");
  });
});

describe("monitorPlaybackHealth: tab replacement on a browser that can no longer start captures", () => {

  /* Drives a tab that has stopped answering evaluates past three timeout strikes. The first strike lapses at the full-length bound; the two that follow lapse at
   * the short confirmation probe, which is the cadence the streak pin above bracket-proves.
   * @param t - The test context owning the mock timers.
   * @param fake - The Page double whose evaluates are left pending.
   */
  async function driveToThirdTimeout(t: TestContext, fake: ReturnType<typeof makeFakePage>): Promise<void> {

    await advance(t, MONITOR_INTERVAL);
    await advance(t, DEFAULT_EVALUATE_TIMEOUT);
    await advance(t, MONITOR_INTERVAL);
    await advance(t, UNRESPONSIVE_PROBE_TIMEOUT);
    await advance(t, MONITOR_INTERVAL);
    await advance(t, UNRESPONSIVE_PROBE_TIMEOUT);

    assert.ok(fake.evaluations.length >= 3, "three reads were issued, one per strike");
  }

  test("terminates a hung tab through the breaker rather than replacing it", async (t) => {

    /* The branch the mark makes reachable. A hung tab in a marked browser cannot be replaced - the replacement would start a capture the browser refuses - and
     * leaving it in the registry would hold open the very relaunch that would cure the browser. So the stream terminates: the recovering line is never logged, the
     * handler is never called, the breaker fires once, and the monitor stops issuing reads.
     */
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const messages = captureLogs(t);
    const fake = makeFakePage();

    let replacements = 0;
    let breaks = 0;

    const handle = startMonitor(fake.page, "impaired-unresponsive-1", 9101, {

      deps: IMPAIRED_DEPS,
      onCircuitBreak: (): void => { breaks++; },
      onTabReplacement: async (): Promise<Nullable<TabReplacementResult>> => {

        replacements++;

        return null;
      }
    });

    await driveToThirdTimeout(t, fake);

    const readsAtTermination = fake.evaluations.length;

    assert.equal(replacements, 0, "the replacement handler was never called on a marked browser");
    assert.equal(countMessages(messages, "Tab unresponsive - recovering via"), 0, "and no recovery was announced");
    assert.equal(countMessages(messages, "Tab unresponsive and tab replacement is unavailable"), 1, "the termination was announced exactly once");
    assert.equal(breaks, 1, "and the breaker fired exactly once");

    await advance(t, MONITOR_INTERVAL * 5);

    assert.equal(fake.evaluations.length, readsAtTermination, "no further read was issued after the stop");

    handle.dispose();
  });

  test("replaces the hung tab as usual when the browser can still start captures", async (t) => {

    // The mutation half. The identical drive against an unmarked browser takes the replacement path, which is what makes the pin above a statement about the mark
    // rather than about the drive.
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const messages = captureLogs(t);
    const fake = makeFakePage();

    let replacements = 0;

    const handle = startMonitor(fake.page, "healthy-unresponsive-1", 9102, {

      deps: HEALTHY_DEPS,
      onTabReplacement: async (): Promise<Nullable<TabReplacementResult>> => {

        replacements++;

        return null;
      }
    });

    await driveToThirdTimeout(t, fake);

    assert.ok(replacements >= 1, "the replacement handler was called");
    assert.equal(countMessages(messages, "Tab unresponsive - recovering via"), 1, "and the recovery was announced once");
    assert.equal(countMessages(messages, "Tab unresponsive and tab replacement is unavailable"), 0, "with no termination line");

    handle.dispose();
  });
});

describe("monitorPlaybackHealth: resolution escalation on a browser that can no longer start captures", () => {

  /* Builds a Page double that answers every read itself: the first sixteen readings at full size to establish the peak, every reading after them at 480x270, which
   * is fourteen percent of that peak by area. Navigations reject, so the ladder's first step completes as a failed reload and the second step becomes reachable.
   * @param establishing - How many readings report the peak size before the drop begins.
   * @returns The Page double.
   */
  function makeDegradingPage(establishing = 16): ReturnType<typeof makeFakePage> {

    let reads = 0;

    return makeFakePage({

      onEvaluate: (call): void => {

        const index = reads++;

        call.resolve((index < establishing) ? resolutionReadableState(index + 1) : { ...readableState(index + 1), videoHeight: 270, videoWidth: 480 });
      },
      onGoto: (call): void => call.reject(new Error("net::ERR_ABORTED")),
      onWaitForSelector: (call): void => call.resolve(null)
    });
  }

  /* Runs the monitor until the ladder announces its second step or the tick budget runs out. The budget covers the first step's count threshold, the grace window
   * it arms afterwards, and the second count threshold that follows.
   * @param t - The test context owning the mock timers.
   * @param messages - The captured log messages.
   * @returns How many ticks ran.
   */
  async function runUntilSecondStep(t: TestContext, messages: string[]): Promise<number> {

    for(let tick = 0; tick < 120; tick++) {

      // Sequential by definition: each tick's work must settle before the next firing.
      // eslint-disable-next-line no-await-in-loop
      await advance(t, MONITOR_INTERVAL);

      // eslint-disable-next-line no-await-in-loop
      await flushMicrotasks();

      if(countMessages(messages, "Video resolution is still degraded after") > 0) {

        return tick;
      }
    }

    return -1;
  }

  test("skips the ladder's tab-replacement step on a marked browser", async (t) => {

    /* The ladder's second step is a tab replacement, so on a marked browser it is unavailable and the ladder skips to acceptance. The first step still runs - the
     * page reload is unaffected by the mark - which is what makes the absence of the second step a decision rather than a stalled drive.
     */
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const messages = captureLogs(t);
    const fake = makeDegradingPage();

    let replacements = 0;

    const handle = startMonitor(fake.page, "impaired-resolution-1", 9103, {

      deps: IMPAIRED_DEPS,
      onTabReplacement: async (): Promise<Nullable<TabReplacementResult>> => {

        replacements++;

        return null;
      }
    });

    const reached = await runUntilSecondStep(t, messages);

    assert.equal(reached, -1, "the ladder never announced its second step");
    assert.equal(countMessages(messages, "Video resolution has been degraded for"), 1, "though its first step ran, so the drive did reach the ladder");
    assert.equal(countMessages(messages, "Video resolution remains degraded"), 1, "and it skipped to acceptance, which is what proves the decision point was reached");
    assert.equal(replacements, 0, "and the replacement handler was never called");

    handle.dispose();
  });

  test("takes the ladder's tab-replacement step when the browser can still start captures", async (t) => {

    // The mutation half: the identical drive against an unmarked browser reaches the second step and calls the handler.
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const messages = captureLogs(t);
    const fake = makeDegradingPage();

    let replacements = 0;

    const handle = startMonitor(fake.page, "healthy-resolution-1", 9104, {

      deps: HEALTHY_DEPS,
      onTabReplacement: async (): Promise<Nullable<TabReplacementResult>> => {

        replacements++;

        return null;
      }
    });

    const reached = await runUntilSecondStep(t, messages);

    assert.notEqual(reached, -1, "the ladder announced its second step");
    assert.equal(countMessages(messages, "Video resolution is still degraded after"), 1, "exactly once");
    assert.ok(replacements >= 1, "and the replacement handler was called");

    handle.dispose();
  });
});
