/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * monitor.twoPhase.test.ts: Tests for the monitor half of the two-phase replacement - the grace window that throttles a stream whose replacement did not cure it,
 * and the native capture fallback's per-outcome handling of the proxy it is leaving behind.
 *
 * Both subjects share a shape: the thing that can go wrong is invisible in a single pass and only shows up in counts across several. A replacement that keeps
 * re-firing looks identical to one that fired once unless the count is read, and a fallback that leaks its relay looks identical to one that released it unless
 * the stop calls are counted. So every row here counts, and every throttle row carries its own positive control - a later pass, outside the window, that shows the
 * trigger was still satisfied all along and the throttle is what held it.
 *
 * The file also holds the monitor's reading of the stream's settings: its bitrates and segment duration into the undersized-segment floor, which the derivation's
 * own rows cannot reach because they measure the formula, and only a running monitor shows the stream's values are what it is handed; and its tick interval,
 * which only a running monitor's cadence shows.
 */
import type { MonitorDeps, MonitorStreamInfo } from "./monitor.ts";
import type { MonitorHandle, TabReplacementResult } from "./recovery.ts";
import { TINY_SEGMENT_FLOOR_CAP_BYTES_PER_SECOND, deriveTinySegmentThresholdBytes } from "./recovery.ts";
import { afterEach, beforeEach, describe, test } from "node:test";
import { closePuppeteerStreamWssOnIdle, flushMicrotasks, makeFakePage } from "../testing.helpers.ts";
import { makeNativeIdentity, makeRegistryEntry } from "./registry.helpers.ts";
import { makePendingCaptureIdentity, registerStream, unregisterStream } from "./registry.ts";
import { CONFIG } from "../config/index.ts";
import type { CaptureCodec } from "./codec.ts";
import type { CaptureImpairment } from "../browser/browserSupervisor.ts";
import type { CaptureSession } from "./captureSession.ts";
import type { FMP4SegmenterResult } from "./fmp4Segmenter.ts";
import { LOG } from "../utils/index.ts";
import type { NativeProxy } from "../native/proxy.ts";
import type { Nullable } from "../types/index.ts";
import type { StreamRegistryEntry } from "./registry.ts";
import type { StreamSettings } from "../config/streamSettings.ts";
import { TestClock } from "homebridge-plugin-utils/testing";
import type { TestContext } from "node:test";
import assert from "node:assert/strict";
import { makeProfile } from "../config/profiles.helpers.ts";
import { makeStreamSettings } from "../config/streamSettings.helpers.ts";
import { monitorPlaybackHealth } from "./monitor.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

// The tick step: the interval of the default settings the streamInfo helper hands a row's monitor unless the row states its own.
const MONITOR_INTERVAL = makeStreamSettings().monitorInterval;

// The window a level-3 recovery arms, from the monitor's own grace table. Every throttle row spends less than this and then more.
const RECOVERY_GRACE_MS = 10000;

// The evaluate wrapper's default bound, which is what turns an unanswered read into a timeout strike.
const DEFAULT_EVALUATE_TIMEOUT = 15000;

// The short bound a read carries once a timeout streak is open, which is the cadence every strike after the first lapses at.
const UNRESPONSIVE_PROBE_TIMEOUT = 2000;

// A segment size well above the undersized threshold the monitor watches for, so a drive built to reach the staleness trigger never trips the tiny-segment
// trigger on its way there.
const HEALTHY_SEGMENT_BYTES = 1000000;

/**
 * Advances the clock in interval-sized steps, letting each tick body settle before the next firing.
 * @param clock - The clock the row's monitor runs on.
 * @param totalMs - How much time to advance.
 */
async function advance(clock: TestClock, totalMs: number): Promise<void> {

  let remaining = totalMs;

  while(remaining > 0) {

    const step = Math.min(MONITOR_INTERVAL, remaining);

    clock.advance(step);
    remaining -= step;

    // Sequential by definition: each step must let the work it caused settle before the next step fires.
    // eslint-disable-next-line no-await-in-loop
    await settle();
  }
}

/**
 * Lets an in-flight tick body run to completion. A tick that awaits a read, a recovery, and a status emission needs several microtask turns before the monitor is
 * idle again, and a firing that lands while the previous body is still running is skipped - so a drive that did not settle would silently exercise half the ticks
 * it appears to.
 */
async function settle(): Promise<void> {

  for(let turn = 0; turn < 6; turn++) {

    // Sequential by definition: each turn drains what the previous one released.
    // eslint-disable-next-line no-await-in-loop
    await flushMicrotasks();
  }
}

/**
 * Captures the monitor's warning and error lines for the life of a row.
 * @param t - The test context whose mock registry restores the methods at row end.
 * @returns The array the captured format strings accumulate into.
 */
function captureLogs(t: TestContext): string[] {

  const messages: string[] = [];

  t.mock.method(LOG, "warn", (message: string) => { messages.push(message); });
  t.mock.method(LOG, "error", (message: string) => { messages.push(message); });

  return messages;
}

/**
 * Counts captured messages beginning with the supplied prefix.
 * @param messages - The captured messages.
 * @param prefix - The prefix to count.
 * @returns The count.
 */
function countMessages(messages: string[], prefix: string): number {

  return messages.filter((message) => message.startsWith(prefix)).length;
}

/* The deps every row starts from: an unmarked browser, so tab replacement is on the table, and codec answers that DIVERGE from the pending identity's null and
 * false. The divergence is what lets a row tell a fallback that re-derived its codec facts from one that merely left them as it found them. The clock is the
 * row's own, because the monitor arms its tick and reads every instant through it.
 * @param clock - The clock the row drives.
 * @returns The dependency set.
 */
function makeDivergentDeps(clock: TestClock): MonitorDeps {

  return {

    clock,
    getCaptureImpairment: (): null => null,
    getEffectiveCaptureCodec: (): CaptureCodec => "hevc",
    isCaptureHardwareAccelerated: (): boolean => true,
    syncWindowVisibility: async (): Promise<void> => { syncs++; }
  };
}

// The mark a browser carries once it can no longer start a capture, and the deps object that reports it. Every replacement decision consults that read, so a
// frozen mark is what puts a row on a browser where no replacement can start. The codec answers match DIVERGENT_DEPS, so the mark is the only thing that differs
// from the deps every other row starts from.
const MARK: CaptureImpairment = { reason: "Could not start video source", since: 0 };

/**
 * Builds the deps a marked browser reports through, over the row's own clock.
 * @param clock - The clock the row drives.
 * @returns The dependency set.
 */
function makeMarkedDeps(clock: TestClock): MonitorDeps {

  return {

    clock,
    getCaptureImpairment: (): Nullable<CaptureImpairment> => MARK,
    getEffectiveCaptureCodec: (): CaptureCodec => "hevc",
    isCaptureHardwareAccelerated: (): boolean => true,
    syncWindowVisibility: async (): Promise<void> => { syncs++; }
  };
}

// How many window syncs the fallback asked for.
let syncs: number;

// The video playhead a healthy drive reports, advancing monotonically for the life of a row so the stall detector stays quiet.
let playheadSeconds = 0;

/**
 * Builds a native proxy double that records the calls the rows read: the stop that the leak matrix is about, and the health reads that show a restored proxy is
 * being consulted again.
 * @param options - Whether the proxy reports itself errored, and what its segment clock says.
 * @returns The double plus its recorded counts.
 */
function makeProxyDouble(options: { errored: boolean; lastSegmentTime?: number }): { healthReads: () => number; proxy: NativeProxy; stops: () => number } {

  let healthReads = 0;
  let stops = 0;

  const proxy = {

    getConsecutiveErrors: (): number => 0,
    getLastSegmentTime: (): number => options.lastSegmentTime ?? 0,
    getSegmentIndex: (): number => 0,
    getStats: (): { fetchErrors: number; segmentsFetched: number; tokenRefreshes: number } => ({ fetchErrors: 0, segmentsFetched: 0, tokenRefreshes: 0 }),
    getTargetDuration: (): number => 2,
    hasErrored: (): boolean => {

      healthReads++;

      return options.errored;
    },
    isStopped: (): boolean => false,
    stop: (): void => { stops++; }
  } as unknown as NativeProxy;

  return { healthReads: (): number => healthReads, proxy, stops: (): number => stops };
}

/**
 * Builds a capture session double for a replacement handler stub to install, standing in for what the real hls.ts handler writes at its swap.
 * @returns The session double.
 */
function makeSwappedSession(): CaptureSession {

  return {

    attachSegmenter: (): void => { /* Nothing to wire on a double. */ },
    dispose: (): void => { /* Nothing to tear down. */ },
    disposed: false,
    segmenter: null,
    [Symbol.dispose]: (): void => { /* Nothing to tear down. */ }
  };
}

/**
 * Builds a segmenter double whose segment index advances on every read and whose segments are always undersized and video-free, which is the exact condition the
 * tiny-segment trigger watches for.
 * @returns The segmenter double.
 */
function makeStarvingSegmenter(): FMP4SegmenterResult {

  let index = 0;

  return {

    getLastSegmentHasVideo: (): boolean => false,
    getLastSegmentSize: (): number => 16,
    getSegmentIndex: (): number => ++index,
    pipe: (): void => { /* Nothing consumes this double. */ },
    stop: (): void => { /* Nothing to stop. */ }
  } as unknown as FMP4SegmenterResult;
}

/**
 * Answers a run of health reads with a progressing, healthy video state. Every outstanding read is answered on each pass rather than one indexed read, so the
 * drive stays correct whichever tick a given read belongs to; a read that is already settled ignores a second answer.
 * @param clock - The clock the row's monitor runs on.
 * @param fake - The Page double whose reads are answered.
 * @param ticks - How many ticks to drive.
 */
async function driveHealthyTicks(clock: TestClock, fake: ReturnType<typeof makeFakePage>, ticks: number): Promise<void> {

  for(let tick = 0; tick < ticks; tick++) {

    // Sequential by definition: each tick's read must settle before the next firing.
    // eslint-disable-next-line no-await-in-loop
    await advance(clock, MONITOR_INTERVAL);

    /* The playhead advances monotonically across the whole drive, not within one call. A drive that restarted it would report a stalled video, which sends the
     * general recovery ladder down a path this row is not about and takes the segment trigger out of reach entirely.
     */
    playheadSeconds++;

    for(const pending of fake.evaluations) {

      pending.resolve({ currentTime: playheadSeconds, ended: false, error: false, muted: false, networkState: 2, paused: false, readyState: 3, videoHeight: 0,
        videoWidth: 0, volume: 1 });
    }

    // eslint-disable-next-line no-await-in-loop
    await settle();
  }
}

/**
 * Builds the registry entry a row registers under the id its monitor was started with.
 * @param numericStreamId - The id the monitor and registry agree on.
 * @returns The entry.
 */
function makeEntry(numericStreamId: number): StreamRegistryEntry {

  return makeRegistryEntry({ id: numericStreamId });
}

// The registry entry and monitor handle each row builds.
let entry: StreamRegistryEntry;
let handle: Nullable<MonitorHandle>;

const streamInfo = (numericStreamId: number, clock: TestClock, settings: StreamSettings = makeStreamSettings()): MonitorStreamInfo => ({

  channelName: "Two Phase Test",
  numericStreamId,
  serviceName: "two-phase-test",
  settings,
  startTime: clock.now()
});

beforeEach(() => {

  handle = null;
  playheadSeconds = 0;
  syncs = 0;
});

afterEach(() => {

  handle?.dispose();
  unregisterStream(entry.id);
});

describe("monitorPlaybackHealth: a failed replacement throttles the next attempt", () => {

  test("a stream whose replacement did not cure it re-attempts once per grace window, not once per tick", async (t) => {

    /* The throttle, read as a count. The drive keeps the trigger satisfied continuously - every tick produces another undersized, video-free segment - so a
     * monitor with no throttle escalates every two seconds and burns the circuit breaker down in under half a minute on a stream that is still serving its
     * recording. The row spends most of a grace window and demands the count stay at exactly one; the positive control that follows spends past the window and
     * demands it become two, which is what proves the trigger was still live the whole time and the window is what held it.
     */
    const clock = new TestClock();

    const segmenter = makeStarvingSegmenter();
    const session = { attachSegmenter: (): void => undefined, dispose: (): void => undefined, disposed: false, segmenter,
      [Symbol.dispose]: (): void => undefined } as unknown as CaptureSession;

    entry = { ...makeEntry(9301), identity: { ...makePendingCaptureIdentity(), captureSession: session } };
    registerStream(entry);

    const messages = captureLogs(t);
    const fake = makeFakePage({ clock });

    let replacements = 0;

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "tiny-grace-1", streamInfo(9301, clock),
      (): void => { /* The breaker is not what this row reads. */ }, async (): Promise<Nullable<TabReplacementResult>> => {

        replacements++;

        return null;
      }, makeDivergentDeps(clock));

    // Drive one tick at a time until the undersized-segment count crosses its threshold, so the window opens at an instant this row knows.
    const episodes = (): number => countMessages(messages, "Detected");

    for(let tick = 0; (tick < 60) && (episodes() === 0); tick++) {

      // Sequential by definition: the trigger's own state advances one tick at a time.
      // eslint-disable-next-line no-await-in-loop
      await driveHealthyTicks(clock, fake, 1);
    }

    assert.equal(episodes(), 1, "the trigger fired, once");
    assert.ok(replacements > 0, "and reached the replacement handler");

    const openedAt = clock.now();

    // Most of the window, with the trigger continuously satisfied: every tick still produces another undersized, video-free segment.
    while((clock.now() - openedAt) < (RECOVERY_GRACE_MS - (MONITOR_INTERVAL * 2))) {

      // eslint-disable-next-line no-await-in-loop
      await driveHealthyTicks(clock, fake, 1);
    }

    assert.equal(episodes(), 1, "still exactly one escalation inside the window");

    const attemptsInsideWindow = replacements;

    /* Past the window, where the still-satisfied trigger is free again. This is the control: without it, a row asserting "exactly one" would pass just as well
     * against a monitor that had stopped detecting anything at all, which is the failure mode a throttle is one edit away from becoming.
     */
    for(let tick = 0; (tick < 60) && (episodes() === 1); tick++) {

      // eslint-disable-next-line no-await-in-loop
      await driveHealthyTicks(clock, fake, 1);
    }

    assert.equal(episodes(), 2, "the window closing releases exactly one more escalation");
    assert.ok(replacements > attemptsInsideWindow, "which reached the handler again");
  });

  test("a hung tab is re-attempted once per grace window as well", async (t) => {

    /* The same throttle on the trigger with the sharpest consequence. Evaluate timeouts against a page mid-establishment are ordinary, so a monitor that
     * escalated on them inside the settling window would spend attempts - and eventually terminate the stream - on evidence the window exists to discount.
     */
    const clock = new TestClock();

    entry = makeEntry(9302);
    registerStream(entry);

    const messages = captureLogs(t);
    const fake = makeFakePage({ clock });

    let replacements = 0;

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "hung-grace-1", streamInfo(9302, clock),
      (): void => { /* The breaker is not what this row reads. */ }, async (): Promise<Nullable<TabReplacementResult>> => {

        replacements++;

        return null;
      }, makeDivergentDeps(clock));

    const episodes = (): number => countMessages(messages, "Tab unresponsive - recovering via");

    // Three unanswered reads take the tab past its timeout streak and fire the first replacement.
    await driveTimeoutStrike(clock, true);
    await driveTimeoutStrike(clock, false);
    await driveTimeoutStrike(clock, false);

    assert.equal(episodes(), 1, "the hung tab was replaced, once");
    assert.ok(replacements > 0, "and the replacement handler was reached");

    const openedAt = clock.now();
    const attemptsAtFirstEscalation = replacements;

    // A further strike inside the window. The timeout tally keeps climbing, so the trigger condition is satisfied on it too.
    await driveTimeoutStrike(clock, false);

    assert.ok((clock.now() - openedAt) < RECOVERY_GRACE_MS, "the strike landed inside the window, which is what this row is about");
    assert.equal(episodes(), 1, "no second escalation inside the window");
    assert.equal(replacements, attemptsAtFirstEscalation, "and the handler was not reached again");

    // The control: past the window, the same strike escalates.
    await advance(clock, RECOVERY_GRACE_MS);
    await driveTimeoutStrike(clock, false);

    assert.equal(episodes(), 2, "the window closing releases exactly one more escalation");
  });

  test("a failure that marks the browser arms no window, so the hung tab terminates on its next strike", async (t) => {

    /* The condition the window is armed for, absent. A window throttles the next replacement attempt, and on a browser that can start no replacement there is no
     * next attempt to throttle - every trigger reaches its no-replacement arm instead of the handler. Arming one there only holds the arm that terminates, and
     * holding that holds every other recording's re-tune behind this stream, because the relaunch that cures the browser waits on the last stream's end.
     *
     * The row directly above is the negative control: the same drive on an unmarked browser still waits the window out before it escalates again. The one thing
     * that differs here is the mark the replacement itself lands, which is why the deps object is mutable rather than one of the file's frozen ones.
     */
    const clock = new TestClock();

    entry = makeEntry(9303);
    registerStream(entry);

    const messages = captureLogs(t);
    const fake = makeFakePage({ clock });

    let impairment: Nullable<CaptureImpairment> = null;
    let replacements = 0;
    let breaks = 0;

    // The deps every row starts from, with the impairment read made live so the mark the replacement lands is visible to every later read.
    const markableDeps: MonitorDeps = {

      clock,
      getCaptureImpairment: (): Nullable<CaptureImpairment> => impairment,
      getEffectiveCaptureCodec: (): CaptureCodec => "hevc",
      isCaptureHardwareAccelerated: (): boolean => true,
      syncWindowVisibility: async (): Promise<void> => { syncs++; }
    };

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "hung-marked-1", streamInfo(9303, clock),
      (): void => { breaks++; }, async (): Promise<Nullable<TabReplacementResult>> => {

        replacements++;

        /* Chrome refusing the capture start mid-attempt is what marks the browser, so the mark lands while this attempt is still settling - before the exhaustion
         * that follows it reads whether another replacement could start.
         */
        impairment = { reason: "Could not start video source", since: clock.now() };

        return null;
      }, markableDeps);

    // Three unanswered reads take the tab past its timeout streak and fire the first replacement.
    await driveTimeoutStrike(clock, true);
    await driveTimeoutStrike(clock, false);
    await driveTimeoutStrike(clock, false);

    assert.equal(countMessages(messages, "Tab unresponsive - recovering via"), 1, "the hung tab was replaced, once");
    assert.ok(replacements > 0, "and the replacement handler was reached");
    assert.equal(countMessages(messages, "Tab replacement was unsuccessful"), 1, "the attempt was exhausted, on the browser it had just marked");

    const failedAt = clock.now();
    const attemptsAtFailure = replacements;

    // The next strike, at an instant a window would still have been covering had one been armed.
    await driveTimeoutStrike(clock, false);

    assert.ok((clock.now() - failedAt) < RECOVERY_GRACE_MS, "the strike landed inside what would have been the window, which is what this row is about");
    assert.equal(countMessages(messages, "Tab unresponsive and tab replacement is unavailable"), 1, "and the unrecoverable stream terminated on it");
    assert.equal(breaks, 1, "through the breaker, exactly once");
    assert.equal(replacements, attemptsAtFailure, "with no further attempt made, because none could start");
  });

  /**
   * Drives one evaluate timeout strike against a page that never answers. The first strike of a streak lapses at the evaluate wrapper's full bound; every strike
   * after it lapses at the short confirmation probe the streak arms, which is the cadence a genuinely hung tab produces.
   * @param clock - The clock the row's monitor runs on.
   * @param first - Whether this is the strike that opens the streak.
   */
  async function driveTimeoutStrike(clock: TestClock, first: boolean): Promise<void> {

    await advance(clock, MONITOR_INTERVAL);
    await advance(clock, first ? DEFAULT_EVALUATE_TIMEOUT : UNRESPONSIVE_PROBE_TIMEOUT);
  }
});

describe("monitorPlaybackHealth: a dead pipeline on a browser that can start no replacement", () => {

  // The line the terminate arm emits, which is what every row here counts.
  const TERMINATION_LINE = "Capture pipeline stalled and tab replacement is unavailable";

  /**
   * Builds a segmenter double that reports one healthy, video-bearing segment and then never advances again, which is the exact condition the staleness trigger
   * watches for: an index that has moved at least once, so the stream is past startup, and has not moved since.
   * @returns The segmenter double.
   */
  function makeStalledSegmenter(): FMP4SegmenterResult {

    return {

      getLastSegmentHasVideo: (): boolean => true,
      getLastSegmentSize: (): number => HEALTHY_SEGMENT_BYTES,
      getSegmentIndex: (): number => 1,
      pipe: (): void => { /* Nothing consumes this double. */ },
      stop: (): void => { /* Nothing to stop. */ }
    } as unknown as FMP4SegmenterResult;
  }

  /**
   * Registers the row's entry carrying a capture session built around the supplied segmenter, which is what the monitor reads every segment fact through.
   * @param numericStreamId - The id the monitor and registry agree on.
   * @param segmenter - The segmenter double the session exposes.
   */
  function registerCapturing(numericStreamId: number, segmenter: FMP4SegmenterResult): void {

    const session = { attachSegmenter: (): void => undefined, dispose: (): void => undefined, disposed: false, segmenter,
      [Symbol.dispose]: (): void => undefined } as unknown as CaptureSession;

    entry = { ...makeEntry(numericStreamId), identity: { ...makePendingCaptureIdentity(), captureSession: session } };
    registerStream(entry);
  }

  test("the tiny-segment trigger terminates the stream on the tick it fires", async (t) => {

    /* The arm read as counts on one tick. A capture that died on a browser which can start no replacement has nothing left that would revive it: every rung of
     * the in-page ladder spends an attempt and part of the breaker's window on a stream it cannot save, and holds the relaunch that would cure the browser behind
     * a stream still sitting in the registry. So the row demands the termination land on the trigger's first firing - one error line, the breaker reached once,
     * no replacement attempted, and no ladder announcement at all. The throttle describe's tiny-segment row is the control: the same double and the same drive on
     * an unmarked browser reach the replacement handler instead.
     */
    const clock = new TestClock();

    registerCapturing(9320, makeStarvingSegmenter());

    const messages = captureLogs(t);
    const fake = makeFakePage({ clock });

    let replacements = 0;
    let breaks = 0;

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "tiny-marked-1", streamInfo(9320, clock),
      (): void => { breaks++; }, async (): Promise<Nullable<TabReplacementResult>> => {

        replacements++;

        return null;
      }, makeMarkedDeps(clock));

    // Drive one tick at a time until the stream terminates, so nothing but the trigger's own firing decides which tick the assertions read.
    const terminations = (): number => countMessages(messages, TERMINATION_LINE);

    for(let tick = 0; (tick < 80) && (terminations() === 0); tick++) {

      // Sequential by definition: the trigger's own state advances one tick at a time.
      // eslint-disable-next-line no-await-in-loop
      await driveHealthyTicks(clock, fake, 1);
    }

    assert.equal(terminations(), 1, "the unrecoverable stream was terminated, once");
    assert.equal(countMessages(messages, "Detected"), 1, "on the trigger's first firing rather than a later one");
    assert.equal(breaks, 1, "through the breaker, exactly once");
    assert.equal(replacements, 0, "with no replacement attempted, because none could start");
    assert.equal(countMessages(messages, "Playback"), 0, "and with no rung of the in-page ladder announced");

    const evaluationsAtTermination = fake.evaluations.length;

    // The positive control on the termination itself: a monitor that logged and broke without stopping would keep reading the page on every later tick.
    await driveHealthyTicks(clock, fake, 5);

    assert.equal(fake.evaluations.length, evaluationsAtTermination, "and the monitor stopped, so no later tick read the page");
  });

  test("the staleness trigger terminates the stream on the tick it fires", async (t) => {

    /* The same judgment reached through the other trigger. A frozen segment index leaves the video element looking perfectly healthy - the playhead advances and
     * nothing errors - so this is the case where only the segment facts say the capture is gone, and the decision site has to be the same one either way.
     */
    const clock = new TestClock();

    registerCapturing(9321, makeStalledSegmenter());

    const messages = captureLogs(t);
    const fake = makeFakePage({ clock });

    let replacements = 0;
    let breaks = 0;

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "stale-marked-1", streamInfo(9321, clock),
      (): void => { breaks++; }, async (): Promise<Nullable<TabReplacementResult>> => {

        replacements++;

        return null;
      }, makeMarkedDeps(clock));

    const terminations = (): number => countMessages(messages, TERMINATION_LINE);

    for(let tick = 0; (tick < 80) && (terminations() === 0); tick++) {

      // Sequential by definition: the staleness clock advances one tick at a time.
      // eslint-disable-next-line no-await-in-loop
      await driveHealthyTicks(clock, fake, 1);
    }

    assert.equal(terminations(), 1, "the unrecoverable stream was terminated, once");
    assert.equal(countMessages(messages, "No new segments produced"), 1, "on the trigger's first firing rather than a later one");
    assert.equal(breaks, 1, "through the breaker, exactly once");
    assert.equal(replacements, 0, "with no replacement attempted, because none could start");
    assert.equal(countMessages(messages, "Playback"), 0, "and with no rung of the in-page ladder announced");

    const evaluationsAtTermination = fake.evaluations.length;

    await driveHealthyTicks(clock, fake, 5);

    assert.equal(fake.evaluations.length, evaluationsAtTermination, "and the monitor stopped, so no later tick read the page");
  });

  test("the staleness trigger reaches the replacement handler on a browser that can still start one", async (t) => {

    /* The control for the row above, and the one that keeps it meaningful. The double and the drive are identical and the mark is the only difference, so a row
     * that terminated here would be saying the double never reached the trigger at all rather than that the mark is what decides the outcome.
     */
    const clock = new TestClock();

    registerCapturing(9322, makeStalledSegmenter());

    const messages = captureLogs(t);
    const fake = makeFakePage({ clock });

    let replacements = 0;
    let breaks = 0;

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "stale-able-1", streamInfo(9322, clock),
      (): void => { breaks++; }, async (): Promise<Nullable<TabReplacementResult>> => {

        replacements++;

        return null;
      }, makeDivergentDeps(clock));

    for(let tick = 0; (tick < 80) && (replacements === 0); tick++) {

      // Sequential by definition: the staleness clock advances one tick at a time.
      // eslint-disable-next-line no-await-in-loop
      await driveHealthyTicks(clock, fake, 1);
    }

    assert.equal(countMessages(messages, "No new segments produced"), 1, "the trigger fired, once");
    assert.ok(replacements > 0, "and reached the replacement handler");
    assert.equal(countMessages(messages, TERMINATION_LINE), 0, "with no termination, because a replacement could still start");
    assert.equal(breaks, 0, "so the breaker was never reached");
  });

  test("a tick that resumed after the monitor stopped neither terminates nor replaces, even with the pipeline judged dead", async (t) => {

    /* The recovery action's entry stop check, read through the resumption it exists for. A tick issues its health read, the monitor is disposed while that read
     * is outstanding, and the read then resolves - so the rest of the tick body runs against a stream that has already ended. Without the check, the resumption
     * reaches the stalled-pipeline arm and, on a browser that can start no replacement, terminates a stream that is already gone and spends a breaker trip on it.
     *
     * The row calibrates rather than encoding the trigger's count. A control monitor built from the same double and driven the same way records which tick the
     * trigger fires on; the subject then stops one tick short and parks that tick at its health read. Encoding the number here would make the row a hostage of
     * the undersized-segment threshold rather than a statement about the stop check.
     */
    const clock = new TestClock();

    const messages = captureLogs(t);

    // The control, run only to learn which tick the trigger fires on. Same double, same deps, same one-tick-at-a-time drive as the rows above.
    registerCapturing(9323, makeStarvingSegmenter());

    const controlEntryId = entry.id;
    const controlPage = makeFakePage({ clock });
    const controlHandle = monitorPlaybackHealth(controlPage.page, controlPage.page, makeProfile(), "https://two-phase.test/watch", "resumed-control-1",
      streamInfo(9323, clock), (): void => { /* The control counts ticks and nothing else. */ },
      async (): Promise<Nullable<TabReplacementResult>> => null, makeMarkedDeps(clock));

    const terminations = (): number => countMessages(messages, TERMINATION_LINE);

    let ticksToTrigger = 0;

    for(; (ticksToTrigger < 80) && (terminations() === 0); ticksToTrigger++) {

      // Sequential by definition: the trigger's own state advances one tick at a time.
      // eslint-disable-next-line no-await-in-loop
      await driveHealthyTicks(clock, controlPage, 1);
    }

    controlHandle.dispose();
    unregisterStream(controlEntryId);

    assert.equal(terminations(), 1, "the control reached the termination, so the count it recorded is the trigger's own");

    // The subject: the same drive stopped one tick short, so the trigger fires on a tick whose health read is still outstanding when the monitor is disposed.
    registerCapturing(9324, makeStarvingSegmenter());

    const fake = makeFakePage({ clock });

    let replacements = 0;
    let breaks = 0;

    const subjectHandle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "resumed-subject-1", streamInfo(9324, clock),
      (): void => { breaks++; }, async (): Promise<Nullable<TabReplacementResult>> => {

        replacements++;

        return null;
      }, makeMarkedDeps(clock));

    handle = subjectHandle;

    const sliceStart = messages.length;

    await driveHealthyTicks(clock, fake, ticksToTrigger - 1);

    // The trigger tick fires and parks at its health read, the monitor stops while that read is outstanding, and only then does the read resolve.
    await advance(clock, MONITOR_INTERVAL);

    subjectHandle.dispose();
    playheadSeconds++;

    /* Answer every outstanding read in turn. The resumed tick issues further reads once the health read settles - the fullscreen verification among them - so a
     * single pass would leave the body parked short of the decision site and the row would pass without ever reaching what it is about.
     */
    for(let pass = 0; pass < 4; pass++) {

      for(const pending of fake.evaluations) {

        pending.resolve({ currentTime: playheadSeconds, ended: false, error: false, muted: false, networkState: 2, paused: false, readyState: 3, videoHeight: 0,
          videoWidth: 0, volume: 1 });
      }

      // Sequential by definition: each pass answers what the previous one released.
      // eslint-disable-next-line no-await-in-loop
      await settle();
    }

    const subjectMessages = messages.slice(sliceStart);

    assert.equal(countMessages(subjectMessages, "Detected"), 1, "the resumption ran the segment check, so the tick did reach the decision site");
    assert.equal(countMessages(subjectMessages, TERMINATION_LINE), 0, "and terminated nothing, because the stream it would terminate has already ended");
    assert.equal(breaks, 0, "so the breaker was never reached");
    assert.equal(replacements, 0, "and no replacement was attempted");
  });
});

describe("monitorPlaybackHealth: the undersized-segment floor follows the stream's settings", () => {

  /**
   * Starts a monitor over a capture whose every segment carries video at the supplied size, drives a few healthy ticks, and reports what the monitor made of
   * those segments. A row states the stream's bitrates and duration through the settings it passes, which the monitor reads its floor's inputs from at its start.
   * The segment must sit under the cap's floor at the stream's duration, so that only a floor following the stream's rates can clear it.
   * @param options - The size every segment reports, the stream's settings, the id the monitor and registry agree on, and the row's test context, which restores
   *                  its mocks at row end.
   * @returns How many segment sizes the monitor read, and how many segments it counted undersized.
   */
  async function judgeSteadySegments(options: { segmentBytes: number; settings: StreamSettings; streamId: number; t: TestContext }):
  Promise<{ sizeReads: number; undersized: number }> {

    assert.ok(options.segmentBytes < (TINY_SEGMENT_FLOOR_CAP_BYTES_PER_SECOND * options.settings.segmentDuration),
      "the segment sits under the cap's floor at the stream's duration, so only the stream's rates can clear it");

    const clock = new TestClock();

    let index = 0;
    let sizeReads = 0;

    const segmenter = {

      getLastSegmentHasVideo: (): boolean => true,
      getLastSegmentSize: (): number => {

        sizeReads++;

        return options.segmentBytes;
      },
      getSegmentIndex: (): number => ++index,
      pipe: (): void => { /* Nothing consumes this double. */ },
      stop: (): void => { /* Nothing to stop. */ }
    } as unknown as FMP4SegmenterResult;

    const session = { attachSegmenter: (): void => undefined, dispose: (): void => undefined, disposed: false, segmenter,
      [Symbol.dispose]: (): void => undefined } as unknown as CaptureSession;

    entry = { ...makeEntry(options.streamId), identity: { ...makePendingCaptureIdentity(), captureSession: session } };
    registerStream(entry);

    const breadcrumbs: string[] = [];

    options.t.mock.method(LOG, "debug", (_category: string, message: string) => { breadcrumbs.push(message); });

    const fake = makeFakePage({ clock });

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "floor-rate-" + String(options.streamId),
      streamInfo(options.streamId, clock, options.settings), (): void => { /* The breaker is not what these rows read. */ },
      async (): Promise<Nullable<TabReplacementResult>> => null, makeDivergentDeps(clock));

    // A few healthy ticks, each producing another segment of the same size.
    await driveHealthyTicks(clock, fake, 5);

    return { sizeReads, undersized: countMessages(breadcrumbs, "Below-threshold segment") };
  }

  test("a 300000-byte segment at a 1000000 bit-per-second video rate is not counted undersized", async (t) => {

    /* The floor read through a running monitor rather than through the derivation, because the stream's bitrates reach the floor only when the monitor reads
     * them at its start and passes them on. The segment sits under the cap's floor at the stream's duration and well above half the stream's shaped rate, so
     * only a floor that follows the stream's rates clears it...a floor held at its cap would count every such segment undersized and replace a healthy
     * low-bitrate capture once per evidence window. The size reads are the control: they show the monitor measured the segments it was fed, so the absence of an
     * undersized judgment is a verdict rather than a check that never ran.
     */
    const judged = await judgeSteadySegments({ segmentBytes: 300000, settings: makeStreamSettings({ videoBitsPerSecond: 1000000 }), streamId: 9330, t });

    assert.ok(judged.sizeReads > 0, "the monitor measured the segments it was fed");
    assert.equal(judged.undersized, 0, "and counted none of them undersized");
  });

  test("a 140000-byte segment at a 1000000 video and 32000 audio bit-per-second rate is not counted undersized", async (t) => {

    /* The audio half of the same wiring. The video rate is shaped as in the row above, so the audio rate is what separates this floor from one read at the
     * default audio rate: at the stream's duration the segment sits above half the stream's shaped rate and below half of the same video rate beside the default
     * audio rate, so a monitor that passed the default audio rate in place of the stream's would count every such segment undersized.
     */
    const settings = makeStreamSettings({ audioBitsPerSecond: 32000, videoBitsPerSecond: 1000000 });
    const judged = await judgeSteadySegments({ segmentBytes: 140000, settings, streamId: 9331, t });

    assert.ok(judged.sizeReads > 0, "the monitor measured the segments it was fed");
    assert.equal(judged.undersized, 0, "and counted none of them undersized");
  });

  test("a 700000-byte segment is counted undersized at a 4-second stream duration that clears it at the default duration", async (t) => {

    /* The duration half of the same wiring. At the default bitrates the floor sits at its cap, so the segment clears it at the default duration and falls under
     * it at the stream's 4 seconds; a monitor that scaled its floor by the default duration, by the running configuration's, or by its own tick interval in
     * seconds would count none of these segments undersized. The running configuration is assigned a duration apart from the stream's as the negative control.
     */
    const settings = makeStreamSettings({ segmentDuration: 4 });
    const defaults = makeStreamSettings();
    const configuredDuration = CONFIG.hls.segmentDuration;

    assert.ok(700000 >= deriveTinySegmentThresholdBytes({ audioBitsPerSecond: defaults.audioBitsPerSecond, segmentDurationSeconds: defaults.segmentDuration,
      videoBitsPerSecond: defaults.videoBitsPerSecond }), "precondition: the segment clears the floor at the default duration");

    CONFIG.hls.segmentDuration = 1;

    try {

      const judged = await judgeSteadySegments({ segmentBytes: 700000, settings, streamId: 9332, t });

      assert.ok(judged.sizeReads > 0, "the monitor measured the segments it was fed");
      assert.ok(judged.undersized > 0, "and counted them undersized against the floor at the stream's duration");
    } finally {

      CONFIG.hls.segmentDuration = configuredDuration;
    }
  });
});

describe("monitorPlaybackHealth: the tick follows the stream's interval", () => {

  test("a monitor whose settings carry a 500 ms interval ticks exactly four times across 2000 ms while the running configuration holds another", async (t) => {

    /* A static-capture tick reads the page's closed state once and returns, so counting those reads counts ticks. The running configuration is assigned an
     * interval four times the stream's as the negative control, so a monitor that armed its tick at the configuration's interval would tick once across the
     * drive rather than four times.
     */
    const clock = new TestClock();
    const settings = makeStreamSettings({ monitorInterval: 500 });
    const fake = makeFakePage({ clock });
    const configuredInterval = CONFIG.playback.monitorInterval;

    let ticks = 0;

    t.mock.method(fake.page, "isClosed", (): boolean => {

      ticks++;

      return false;
    });

    entry = makeEntry(9333);
    registerStream(entry);

    CONFIG.playback.monitorInterval = settings.monitorInterval * 4;

    try {

      handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile({ staticCapture: true }), "https://two-phase.test/watch", "tick-interval-1",
        streamInfo(9333, clock, settings), (): void => { /* The breaker is not what this row reads. */ }, undefined, makeDivergentDeps(clock));

      clock.advance(2000);
      await settle();
    } finally {

      CONFIG.playback.monitorInterval = configuredInterval;
    }

    assert.equal(ticks, 4, "the monitor ticked once per 500 ms of the stream's interval");
  });
});

describe("executeNativeL3Fallback: the relay is released on exactly the exits that hand the stream on", () => {

  test("a successful fallback releases the relay and re-derives the codec facts from the capture decision", async () => {

    /* The success arm. The relay is done - the capture pipeline owns the stream now - so it is stopped, and the identity the handler wrote is refreshed with the
     * codec facts the capture decision actually produced. Without that refresh the entry keeps the label read off the service's manifest, describing a feed this
     * stream stopped consuming, for the rest of its life. The injected codec answers diverge from the pending identity's null and false precisely so a fallback
     * that skipped the refresh cannot pass by coincidence.
     */
    const clock = new TestClock();

    const relay = makeProxyDouble({ errored: true });
    const swapped = makeSwappedSession();

    entry = { ...makeEntry(9310), identity: makeNativeIdentity({ captureCodec: "H264", nativeProxy: relay.proxy }) };
    registerStream(entry);

    const fake = makeFakePage({ clock });

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "l3-success-1", streamInfo(9310, clock),
      (): void => { /* The breaker is not reached on this arm. */ }, async (): Promise<Nullable<TabReplacementResult>> => {

        // Stands in for the real handler's swap: it installs the capture pipeline on the entry and hands back the new page.
        entry.identity = { ...makePendingCaptureIdentity(), captureSession: swapped };

        return { context: fake.page, page: fake.page };
      }, makeDivergentDeps(clock));

    await advance(clock, MONITOR_INTERVAL * 2);

    assert.equal(relay.stops(), 1, "the relay is released exactly once");
    assert.equal(entry.identity.mode, "capture", "the stream is capturing");
    assert.equal((entry.identity as { captureSession: unknown }).captureSession, swapped, "on the pipeline the handler installed, untouched by the refresh");
    assert.equal(entry.identity.captureCodec, "HEVC", "with the codec the capture decision produced, not the manifest label it used to carry");
    assert.equal((entry.identity as { hardwareAccelerated: boolean }).hardwareAccelerated, true, "and the acceleration the capture decision produced");
  });

  test("a failed fallback keeps the relay running and hands back the very identity it was holding", async (t) => {

    /* The arm the two-phase design creates. Because nothing was disposed before the swap, a fallback that cannot establish its capture hands the stream back
     * exactly as it found it - the same identity object, carrying the same live relay - rather than leaving it native with nothing to relay through. The
     * reference-equality assertion is what says "the same one" rather than "an equivalent one", and the health-read count on the next tick is what says the
     * restored relay is genuinely back in service rather than sitting in a state the monitor no-ops over.
     */
    const clock = new TestClock();

    const relay = makeProxyDouble({ errored: true });

    entry = { ...makeEntry(9311), identity: makeNativeIdentity({ nativeProxy: relay.proxy }) };
    registerStream(entry);

    const held = entry.identity;
    const messages = captureLogs(t);
    const fake = makeFakePage({ clock });

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "l3-failed-1", streamInfo(9311, clock),
      (): void => { /* The breaker does not trip on a single failure. */ }, async (): Promise<Nullable<TabReplacementResult>> => null, makeDivergentDeps(clock));

    await advance(clock, MONITOR_INTERVAL * 2);

    assert.equal(relay.stops(), 0, "the relay was never stopped - it is what the stream is still running on");
    assert.equal(entry.identity, held, "and the entry holds the very identity object it started with");
    assert.ok(syncs >= 1, "the window presentation was settled now that no capture is being attempted");
    assert.equal(countMessages(messages, "Capture fallback failed for"), 1, "the failure was announced once");

    const readsAfterFallback = relay.healthReads();

    await advance(clock, MONITOR_INTERVAL);

    assert.ok(relay.healthReads() > readsAfterFallback, "the restored relay is consulted again on the next native tick");
  });

  test("a stall inside the grace window holds the fallback at the ladder and enters nothing", async (t) => {

    /* The staleness ladder's own window read, which is the same discipline the fast path below applies to a dead relay. A stall persists tick after tick, so a
     * ladder that escalated on every one of them would run the whole cycle - the pre-flip, the attempt, the revert, the window sync, the warning - twice a second
     * inside a window every other trigger is respecting. The sync count is the instrument, because it advances once per cycle that actually entered the fallback,
     * and the debug breadcrumb is what says the tick was seen and held rather than never reaching the decision at all.
     */
    const clock = new TestClock();

    const relay = makeProxyDouble({ errored: false, lastSegmentTime: 1 });

    entry = { ...makeEntry(9312), identity: makeNativeIdentity({ nativeProxy: relay.proxy }) };
    registerStream(entry);

    const held = entry.identity;
    const messages = captureLogs(t);
    const breadcrumbs: string[] = [];

    t.mock.method(LOG, "debug", (_category: string, message: string) => { breadcrumbs.push(message); });

    const fake = makeFakePage({ clock });

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "l3-deferred-1", streamInfo(9312, clock),
      (): void => { /* The breaker does not trip on a single failure. */ }, async (): Promise<Nullable<TabReplacementResult>> => null, makeDivergentDeps(clock));

    /* The staleness ladder needs a lapsed reload attempt before it escalates, and the entry carries no probe identity, so the reload declines and leaves the
     * attempt counted - which is exactly the "the reload did not work" signal the classifier escalates on.
     */
    await advance(clock, MONITOR_INTERVAL * 8);

    assert.equal(countMessages(messages, "Capture fallback failed for"), 1, "the first escalation ran and failed, arming the window");
    assert.equal(syncs, 1, "one cycle ran and reverted");

    const heldBreadcrumb = "Capture fallback for %s waits for the recovery grace window to close.";
    const narrationAfterFailure = messages.length;
    const breadcrumbsAfterFailure = countMessages(breadcrumbs, heldBreadcrumb);

    // Two more stalled ticks, each satisfying the same escalation condition, inside the window the failure armed.
    await advance(clock, MONITOR_INTERVAL * 2);

    assert.equal(syncs, 1, "and not one of those ticks re-entered the fallback");
    assert.equal(entry.identity, held, "the entry was never even pre-flipped, so it still holds the identity it started with");
    assert.equal(relay.stops(), 0, "and the relay keeps running, because nothing took the stream from it");
    assert.equal(messages.length, narrationAfterFailure, "with nothing narrated at all - a held fallback is the throttle working, not evidence about the stream");
    assert.equal(countMessages(breadcrumbs, heldBreadcrumb) - breadcrumbsAfterFailure, 2, "one breadcrumb for each of the two in-window ticks this drive fired");

    /* Past the window, where the still-stalled stream is free to escalate again. This is the control: without it, a row asserting "nothing happened" would pass
     * just as well against a monitor that had stopped detecting the stall at all.
     */
    await advance(clock, RECOVERY_GRACE_MS);

    assert.equal(countMessages(messages, "Capture fallback failed for"), 2, "the window closing releases exactly one more escalation");
    assert.equal(syncs, 2, "which entered the fallback and reverted out of it, exactly once");
  });

  test("a dead relay does not re-enter the fallback on every tick inside the window", async () => {

    /* The fast path above the fallback reads the window itself rather than leaving it to the replacement primitive, because a relay that stopped itself stays
     * stopped: the condition holds on every tick from then on. Without the read here the whole cycle - the mode pre-flip, the attempt, the revert, the window
     * sync - would run twice a second inside a window every other trigger is respecting. The sync count is the instrument, because it advances once per cycle
     * that actually entered the fallback.
     */
    const clock = new TestClock();

    const relay = makeProxyDouble({ errored: true });

    entry = { ...makeEntry(9315), identity: makeNativeIdentity({ nativeProxy: relay.proxy }) };
    registerStream(entry);

    const held = entry.identity;
    const fake = makeFakePage({ clock });

    let attempts = 0;

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "l3-fastpath-grace-1", streamInfo(9315, clock),
      (): void => { /* The breaker does not trip on a single failure. */ }, async (): Promise<Nullable<TabReplacementResult>> => {

        attempts++;

        return null;
      }, makeDivergentDeps(clock));

    await advance(clock, MONITOR_INTERVAL * 2);

    assert.equal(syncs, 1, "one cycle ran and reverted");

    const attemptsAfterFirstCycle = attempts;
    const openedAt = clock.now();

    // Several more ticks inside the window, with the relay reporting itself dead on every one of them.
    await advance(clock, MONITOR_INTERVAL * 3);

    assert.ok((clock.now() - openedAt) < RECOVERY_GRACE_MS, "those ticks landed inside the window, which is what this row is about");
    assert.equal(syncs, 1, "and not one of them re-entered the fallback");
    assert.equal(attempts, attemptsAfterFirstCycle, "nor reached the replacement handler again");
    assert.equal(entry.identity, held, "the entry was never even pre-flipped, so it still holds the identity it started with");
  });

  test("an exhausted replacement leaves a stream whose page is still open running, and the monitor still ticking", async (t) => {

    /* The boundary the stranded-stream safety net sits on. It exists for the one state that genuinely cannot recover - the page a replacement would have taken
     * over is gone - and a pre-swap failure is not that state: the page is open and still serving, so the net must stay quiet, the breaker simply counts, and the
     * monitor goes on running against the stream it kept. A net that fired here would terminate a live recording, which is the loss this whole shape prevents.
     */
    const clock = new TestClock();

    const relay = makeProxyDouble({ errored: true });

    entry = { ...makeEntry(9316), identity: makeNativeIdentity({ nativeProxy: relay.proxy }) };
    registerStream(entry);

    const messages = captureLogs(t);
    const fake = makeFakePage({ clock });

    let breaks = 0;

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "exhaustion-open-page-1", streamInfo(9316, clock),
      (): void => { breaks++; }, async (): Promise<Nullable<TabReplacementResult>> => null, makeDivergentDeps(clock));

    await advance(clock, MONITOR_INTERVAL * 2);

    assert.equal(countMessages(messages, "Tab replacement was unsuccessful"), 1, "the attempt exhausted");
    assert.equal(breaks, 0, "and the stream was not terminated - its page is still open and still serving");
    assert.equal(countMessages(messages, "Tab replacement failed and the original page"), 0, "so the stranded-stream net never fired");

    const readsAfterExhaustion = relay.healthReads();

    await advance(clock, MONITOR_INTERVAL * 2);

    assert.ok(relay.healthReads() > readsAfterExhaustion, "and the monitor is still running against the stream it kept");
  });

  test("a fallback whose stream terminates mid-attempt releases the relay exactly once", async () => {

    /* The orphan exits, where the stream is gone or going. Termination disposed whatever the registry held, which during the attempt is the pending capture
     * identity - and that holds nothing - so the relay this frame is still carrying would poll and refresh forever if this arm did not release it. The count is
     * the whole assertion: one stop, from the single site that owns the matrix, not one per arm.
     */
    const clock = new TestClock();

    const relay = makeProxyDouble({ errored: true });

    entry = { ...makeEntry(9313), identity: makeNativeIdentity({ nativeProxy: relay.proxy }) };
    registerStream(entry);

    const fake = makeFakePage({ clock });

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "l3-stopped-1", streamInfo(9313, clock),
      (): void => { /* The breaker is not what ends this stream. */ }, async (): Promise<Nullable<TabReplacementResult>> => {

        // The stream terminates while the replacement is in flight, which stops the monitor.
        handle?.dispose();

        return null;
      }, makeDivergentDeps(clock));

    await advance(clock, MONITOR_INTERVAL * 2);

    assert.equal(relay.stops(), 1, "the relay is released exactly once on the orphan exit");
  });

  test("a fallback that ends in termination releases the relay exactly once", async () => {

    /* The other orphan exit: the attempt exhausts while the page it would have replaced is already gone, which is the one state that still strands a stream, so
     * the monitor terminates it explicitly. The relay is released on the way out for the same reason as the stop arm.
     */
    const clock = new TestClock();

    const relay = makeProxyDouble({ errored: true });

    entry = { ...makeEntry(9314), identity: makeNativeIdentity({ nativeProxy: relay.proxy }) };
    registerStream(entry);

    const fake = makeFakePage({ clock });

    let breaks = 0;

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "l3-terminated-1", streamInfo(9314, clock),
      (): void => { breaks++; }, async (): Promise<Nullable<TabReplacementResult>> => {

        // The page the stream was running on is gone by the time the attempt exhausts.
        fake.setClosed(true);

        return null;
      }, makeDivergentDeps(clock));

    await advance(clock, MONITOR_INTERVAL * 2);

    assert.equal(breaks, 1, "the stranded stream was terminated through the breaker");
    assert.equal(relay.stops(), 1, "and the relay was released exactly once");
  });
});

