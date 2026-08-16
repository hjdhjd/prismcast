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
import { closePuppeteerStreamWssOnIdle, flushMicrotasks, makeFakePage } from "../testing.helpers.ts";
import { describe, test } from "node:test";
import { emitStreamAdded, emitStreamRemoved, subscribeToStatus } from "./statusEmitter.ts";
import { CONFIG } from "../config/index.ts";
import { LOG } from "../utils/index.ts";
import type { MonitorHandle } from "./recovery.ts";
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

/**
 * Starts a monitor against a Page double, with the minimal profile that keeps the tune path shallow: no channel selection, no iframe search, no click-to-play.
 * @param page - The Page double to monitor.
 * @param streamId - The stream id string for log context and abort lookup.
 * @param numericStreamId - The numeric stream id the status and registry lookups use.
 * @returns The monitor handle.
 */
function startMonitor(page: ReturnType<typeof makeFakePage>["page"], streamId: string, numericStreamId: number): MonitorHandle {

  return monitorPlaybackHealth(page, page, makeProfile(), "https://monitor.test/watch", streamId, {

    channelName: "Monitor Test",
    numericStreamId,
    serviceName: "monitor-test",
    startTime: new Date()
  }, () => { /* The circuit-break callback is not what these pins exercise. */ });
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
