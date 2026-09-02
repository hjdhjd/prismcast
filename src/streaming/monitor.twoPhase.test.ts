/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * monitor.twoPhase.test.ts: Tests for the monitor half of the two-phase replacement - the grace window that throttles a stream whose replacement did not cure it,
 * and the native capture fallback's per-outcome handling of the proxy it is leaving behind.
 *
 * Both subjects share a shape: the thing that can go wrong is invisible in a single pass and only shows up in counts across several. A replacement that keeps
 * re-firing looks identical to one that fired once unless the count is read, and a fallback that leaks its relay looks identical to one that released it unless
 * the stop calls are counted. So every row here counts, and every throttle row carries its own positive control - a later pass, outside the window, that shows the
 * trigger was still satisfied all along and the throttle is what held it.
 */
import type { MonitorDeps, MonitorStreamInfo } from "./monitor.ts";
import { afterEach, beforeEach, describe, test } from "node:test";
import { closePuppeteerStreamWssOnIdle, flushMicrotasks, makeFakePage } from "../testing.helpers.ts";
import { makeNativeIdentity, makeRegistryEntry } from "./registry.helpers.ts";
import { makePendingCaptureIdentity, registerStream, unregisterStream } from "./registry.ts";
import { CONFIG } from "../config/index.ts";
import type { CaptureCodec } from "./codec.ts";
import type { CaptureSession } from "./captureSession.ts";
import type { FMP4SegmenterResult } from "./fmp4Segmenter.ts";
import { LOG } from "../utils/index.ts";
import type { MonitorHandle } from "./recovery.ts";
import type { NativeProxy } from "../native/proxy.ts";
import type { Nullable } from "../types/index.ts";
import type { StreamRegistryEntry } from "./registry.ts";
import type { TabReplacementResult } from "./recovery.ts";
import type { TestContext } from "node:test";
import assert from "node:assert/strict";
import { makeProfile } from "../config/profiles.helpers.ts";
import { monitorPlaybackHealth } from "./monitor.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

const MONITOR_INTERVAL = CONFIG.playback.monitorInterval;

// The window a level-3 recovery arms, from the monitor's own grace table. Every throttle row spends less than this and then more.
const RECOVERY_GRACE_MS = 10000;

// The evaluate wrapper's default bound, which is what turns an unanswered read into a timeout strike.
const DEFAULT_EVALUATE_TIMEOUT = 15000;

// The short bound a read carries once a timeout streak is open, which is the cadence every strike after the first lapses at.
const UNRESPONSIVE_PROBE_TIMEOUT = 2000;

/**
 * Advances mock timers in interval-sized steps, letting each tick body settle before the next firing.
 * @param t - The test context owning the mock timers.
 * @param totalMs - How much time to advance.
 */
async function advance(t: TestContext, totalMs: number): Promise<void> {

  let remaining = totalMs;

  while(remaining > 0) {

    const step = Math.min(MONITOR_INTERVAL, remaining);

    t.mock.timers.tick(step);
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

// The deps every row starts from: an unmarked browser, so tab replacement is on the table, and codec answers that DIVERGE from the pending identity's null and
// false. The divergence is what lets a row tell a fallback that re-derived its codec facts from one that merely left them as it found them.
const DIVERGENT_DEPS: MonitorDeps = {

  getCaptureImpairment: (): null => null,
  getEffectiveCaptureCodec: (): CaptureCodec => "hevc",
  isCaptureHardwareAccelerated: (): boolean => true,
  syncWindowVisibility: async (): Promise<void> => { syncs++; }
};

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

const streamInfo = (numericStreamId: number): MonitorStreamInfo => ({

  channelName: "Two Phase Test",
  numericStreamId,
  serviceName: "two-phase-test",
  startTime: new Date()
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

  test("a stream whose replacement did not cure it re-attempts once per grace window, not once per tick", async (t) => {

    /* The throttle, read as a count. The drive keeps the trigger satisfied continuously - every tick produces another undersized, video-free segment - so a
     * monitor with no throttle escalates every two seconds and burns the circuit breaker down in under half a minute on a stream that is still serving its
     * recording. The row spends most of a grace window and demands the count stay at exactly one; the positive control that follows spends past the window and
     * demands it become two, which is what proves the trigger was still live the whole time and the window is what held it.
     */
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const segmenter = makeStarvingSegmenter();
    const session = { attachSegmenter: (): void => undefined, dispose: (): void => undefined, disposed: false, segmenter,
      [Symbol.dispose]: (): void => undefined } as unknown as CaptureSession;

    entry = { ...makeEntry(9301), identity: { ...makePendingCaptureIdentity(), captureSession: session } };
    registerStream(entry);

    const messages = captureLogs(t);
    const fake = makeFakePage();

    let replacements = 0;

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "tiny-grace-1", streamInfo(9301),
      (): void => { /* The breaker is not what this row reads. */ }, async (): Promise<Nullable<TabReplacementResult>> => {

        replacements++;

        return null;
      }, DIVERGENT_DEPS);

    // Drive one tick at a time until the undersized-segment count crosses its threshold, so the window opens at an instant this row knows.
    const episodes = (): number => countMessages(messages, "Detected");

    for(let tick = 0; (tick < 60) && (episodes() === 0); tick++) {

      // Sequential by definition: the trigger's own state advances one tick at a time.
      // eslint-disable-next-line no-await-in-loop
      await driveHealthyTicks(t, fake, 1);
    }

    assert.equal(episodes(), 1, "the trigger fired, once");
    assert.ok(replacements > 0, "and reached the replacement handler");

    const openedAt = Date.now();

    // Most of the window, with the trigger continuously satisfied: every tick still produces another undersized, video-free segment.
    while((Date.now() - openedAt) < (RECOVERY_GRACE_MS - (MONITOR_INTERVAL * 2))) {

      // eslint-disable-next-line no-await-in-loop
      await driveHealthyTicks(t, fake, 1);
    }

    assert.equal(episodes(), 1, "still exactly one escalation inside the window");

    const attemptsInsideWindow = replacements;

    /* Past the window, where the still-satisfied trigger is free again. This is the control: without it, a row asserting "exactly one" would pass just as well
     * against a monitor that had stopped detecting anything at all, which is the failure mode a throttle is one edit away from becoming.
     */
    for(let tick = 0; (tick < 60) && (episodes() === 1); tick++) {

      // eslint-disable-next-line no-await-in-loop
      await driveHealthyTicks(t, fake, 1);
    }

    assert.equal(episodes(), 2, "the window closing releases exactly one more escalation");
    assert.ok(replacements > attemptsInsideWindow, "which reached the handler again");
  });

  test("a hung tab is re-attempted once per grace window as well", async (t) => {

    /* The same throttle on the trigger with the sharpest consequence. Evaluate timeouts against a page mid-establishment are ordinary, so a monitor that
     * escalated on them inside the settling window would spend attempts - and eventually terminate the stream - on evidence the window exists to discount.
     */
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    entry = makeEntry(9302);
    registerStream(entry);

    const messages = captureLogs(t);
    const fake = makeFakePage();

    let replacements = 0;

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "hung-grace-1", streamInfo(9302),
      (): void => { /* The breaker is not what this row reads. */ }, async (): Promise<Nullable<TabReplacementResult>> => {

        replacements++;

        return null;
      }, DIVERGENT_DEPS);

    const episodes = (): number => countMessages(messages, "Tab unresponsive - recovering via");

    // Three unanswered reads take the tab past its timeout streak and fire the first replacement.
    await driveTimeoutStrike(t, true);
    await driveTimeoutStrike(t, false);
    await driveTimeoutStrike(t, false);

    assert.equal(episodes(), 1, "the hung tab was replaced, once");
    assert.ok(replacements > 0, "and the replacement handler was reached");

    const openedAt = Date.now();
    const attemptsAtFirstEscalation = replacements;

    // A further strike inside the window. The timeout tally keeps climbing, so the trigger condition is satisfied on it too.
    await driveTimeoutStrike(t, false);

    assert.ok((Date.now() - openedAt) < RECOVERY_GRACE_MS, "the strike landed inside the window, which is what this row is about");
    assert.equal(episodes(), 1, "no second escalation inside the window");
    assert.equal(replacements, attemptsAtFirstEscalation, "and the handler was not reached again");

    // The control: past the window, the same strike escalates.
    await advance(t, RECOVERY_GRACE_MS);
    await driveTimeoutStrike(t, false);

    assert.equal(episodes(), 2, "the window closing releases exactly one more escalation");
  });

  /**
   * Answers a run of health reads with a progressing, healthy video state. Every outstanding read is answered on each pass rather than one indexed read, so the
   * drive stays correct whichever tick a given read belongs to; a read that is already settled ignores a second answer.
   * @param t - The test context owning the mock timers.
   * @param fake - The Page double whose reads are answered.
   * @param ticks - How many ticks to drive.
   */
  async function driveHealthyTicks(t: TestContext, fake: ReturnType<typeof makeFakePage>, ticks: number): Promise<void> {

    for(let tick = 0; tick < ticks; tick++) {

      // Sequential by definition: each tick's read must settle before the next firing.
      // eslint-disable-next-line no-await-in-loop
      await advance(t, MONITOR_INTERVAL);

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
   * Drives one evaluate timeout strike against a page that never answers. The first strike of a streak lapses at the evaluate wrapper's full bound; every strike
   * after it lapses at the short confirmation probe the streak arms, which is the cadence a genuinely hung tab produces.
   * @param t - The test context owning the mock timers.
   * @param first - Whether this is the strike that opens the streak.
   */
  async function driveTimeoutStrike(t: TestContext, first: boolean): Promise<void> {

    await advance(t, MONITOR_INTERVAL);
    await advance(t, first ? DEFAULT_EVALUATE_TIMEOUT : UNRESPONSIVE_PROBE_TIMEOUT);
  }
});

describe("executeNativeL3Fallback: the relay is released on exactly the exits that hand the stream on", () => {

  test("a successful fallback releases the relay and re-derives the codec facts from the capture decision", async (t) => {

    /* The success arm. The relay is done - the capture pipeline owns the stream now - so it is stopped, and the identity the handler wrote is refreshed with the
     * codec facts the capture decision actually produced. Without that refresh the entry keeps the label read off the service's manifest, describing a feed this
     * stream stopped consuming, for the rest of its life. The injected codec answers diverge from the pending identity's null and false precisely so a fallback
     * that skipped the refresh cannot pass by coincidence.
     */
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const relay = makeProxyDouble({ errored: true });
    const swapped = makeSwappedSession();

    entry = { ...makeEntry(9310), identity: makeNativeIdentity({ captureCodec: "H264", nativeProxy: relay.proxy }) };
    registerStream(entry);

    const fake = makeFakePage();

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "l3-success-1", streamInfo(9310),
      (): void => { /* The breaker is not reached on this arm. */ }, async (): Promise<Nullable<TabReplacementResult>> => {

        // Stands in for the real handler's swap: it installs the capture pipeline on the entry and hands back the new page.
        entry.identity = { ...makePendingCaptureIdentity(), captureSession: swapped };

        return { context: fake.page, page: fake.page };
      }, DIVERGENT_DEPS);

    await advance(t, MONITOR_INTERVAL * 2);

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
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const relay = makeProxyDouble({ errored: true });

    entry = { ...makeEntry(9311), identity: makeNativeIdentity({ nativeProxy: relay.proxy }) };
    registerStream(entry);

    const held = entry.identity;
    const messages = captureLogs(t);
    const fake = makeFakePage();

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "l3-failed-1", streamInfo(9311),
      (): void => { /* The breaker does not trip on a single failure. */ }, async (): Promise<Nullable<TabReplacementResult>> => null, DIVERGENT_DEPS);

    await advance(t, MONITOR_INTERVAL * 2);

    assert.equal(relay.stops(), 0, "the relay was never stopped - it is what the stream is still running on");
    assert.equal(entry.identity, held, "and the entry holds the very identity object it started with");
    assert.ok(syncs >= 1, "the window presentation was settled now that no capture is being attempted");
    assert.equal(countMessages(messages, "Capture fallback failed for"), 1, "the failure was announced once");

    const readsAfterFallback = relay.healthReads();

    await advance(t, MONITOR_INTERVAL);

    assert.ok(relay.healthReads() > readsAfterFallback, "the restored relay is consulted again on the next native tick");
  });

  test("a stall inside the grace window holds the fallback at the ladder and enters nothing", async (t) => {

    /* The staleness ladder's own window read, which is the same discipline the fast path below applies to a dead relay. A stall persists tick after tick, so a
     * ladder that escalated on every one of them would run the whole cycle - the pre-flip, the attempt, the revert, the window sync, the warning - twice a second
     * inside a window every other trigger is respecting. The sync count is the instrument, because it advances once per cycle that actually entered the fallback,
     * and the debug breadcrumb is what says the tick was seen and held rather than never reaching the decision at all.
     */
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const relay = makeProxyDouble({ errored: false, lastSegmentTime: 1 });

    entry = { ...makeEntry(9312), identity: makeNativeIdentity({ nativeProxy: relay.proxy }) };
    registerStream(entry);

    const held = entry.identity;
    const messages = captureLogs(t);
    const breadcrumbs: string[] = [];

    t.mock.method(LOG, "debug", (category: string, message: string) => { breadcrumbs.push(message); });

    const fake = makeFakePage();

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "l3-deferred-1", streamInfo(9312),
      (): void => { /* The breaker does not trip on a single failure. */ }, async (): Promise<Nullable<TabReplacementResult>> => null, DIVERGENT_DEPS);

    /* The staleness ladder needs a lapsed reload attempt before it escalates, and the entry carries no probe identity, so the reload declines and leaves the
     * attempt counted - which is exactly the "the reload did not work" signal the classifier escalates on.
     */
    await advance(t, MONITOR_INTERVAL * 8);

    assert.equal(countMessages(messages, "Capture fallback failed for"), 1, "the first escalation ran and failed, arming the window");
    assert.equal(syncs, 1, "one cycle ran and reverted");

    const heldBreadcrumb = "Capture fallback for %s waits for the recovery grace window to close.";
    const narrationAfterFailure = messages.length;
    const breadcrumbsAfterFailure = countMessages(breadcrumbs, heldBreadcrumb);

    // Two more stalled ticks, each satisfying the same escalation condition, inside the window the failure armed.
    await advance(t, MONITOR_INTERVAL * 2);

    assert.equal(syncs, 1, "and not one of those ticks re-entered the fallback");
    assert.equal(entry.identity, held, "the entry was never even pre-flipped, so it still holds the identity it started with");
    assert.equal(relay.stops(), 0, "and the relay keeps running, because nothing took the stream from it");
    assert.equal(messages.length, narrationAfterFailure, "with nothing narrated at all - a held fallback is the throttle working, not evidence about the stream");
    assert.equal(countMessages(breadcrumbs, heldBreadcrumb) - breadcrumbsAfterFailure, 2, "one breadcrumb for each of the two in-window ticks this drive fired");

    /* Past the window, where the still-stalled stream is free to escalate again. This is the control: without it, a row asserting "nothing happened" would pass
     * just as well against a monitor that had stopped detecting the stall at all.
     */
    await advance(t, RECOVERY_GRACE_MS);

    assert.equal(countMessages(messages, "Capture fallback failed for"), 2, "the window closing releases exactly one more escalation");
    assert.equal(syncs, 2, "which entered the fallback and reverted out of it, exactly once");
  });

  test("a dead relay does not re-enter the fallback on every tick inside the window", async (t) => {

    /* The fast path above the fallback reads the window itself rather than leaving it to the replacement primitive, because a relay that stopped itself stays
     * stopped: the condition holds on every tick from then on. Without the read here the whole cycle - the mode pre-flip, the attempt, the revert, the window
     * sync - would run twice a second inside a window every other trigger is respecting. The sync count is the instrument, because it advances once per cycle
     * that actually entered the fallback.
     */
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const relay = makeProxyDouble({ errored: true });

    entry = { ...makeEntry(9315), identity: makeNativeIdentity({ nativeProxy: relay.proxy }) };
    registerStream(entry);

    const held = entry.identity;
    const fake = makeFakePage();

    let attempts = 0;

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "l3-fastpath-grace-1", streamInfo(9315),
      (): void => { /* The breaker does not trip on a single failure. */ }, async (): Promise<Nullable<TabReplacementResult>> => {

        attempts++;

        return null;
      }, DIVERGENT_DEPS);

    await advance(t, MONITOR_INTERVAL * 2);

    assert.equal(syncs, 1, "one cycle ran and reverted");

    const attemptsAfterFirstCycle = attempts;
    const openedAt = Date.now();

    // Several more ticks inside the window, with the relay reporting itself dead on every one of them.
    await advance(t, MONITOR_INTERVAL * 3);

    assert.ok((Date.now() - openedAt) < RECOVERY_GRACE_MS, "those ticks landed inside the window, which is what this row is about");
    assert.equal(syncs, 1, "and not one of them re-entered the fallback");
    assert.equal(attempts, attemptsAfterFirstCycle, "nor reached the replacement handler again");
    assert.equal(entry.identity, held, "the entry was never even pre-flipped, so it still holds the identity it started with");
  });

  test("an exhausted replacement leaves a stream whose page is still open running, and the monitor still ticking", async (t) => {

    /* The boundary the stranded-stream safety net sits on. It exists for the one state that genuinely cannot recover - the page a replacement would have taken
     * over is gone - and a pre-swap failure is not that state: the page is open and still serving, so the net must stay quiet, the breaker simply counts, and the
     * monitor goes on running against the stream it kept. A net that fired here would terminate a live recording, which is the loss this whole shape prevents.
     */
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const relay = makeProxyDouble({ errored: true });

    entry = { ...makeEntry(9316), identity: makeNativeIdentity({ nativeProxy: relay.proxy }) };
    registerStream(entry);

    const messages = captureLogs(t);
    const fake = makeFakePage();

    let breaks = 0;

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "exhaustion-open-page-1", streamInfo(9316),
      (): void => { breaks++; }, async (): Promise<Nullable<TabReplacementResult>> => null, DIVERGENT_DEPS);

    await advance(t, MONITOR_INTERVAL * 2);

    assert.equal(countMessages(messages, "Tab replacement was unsuccessful"), 1, "the attempt exhausted");
    assert.equal(breaks, 0, "and the stream was not terminated - its page is still open and still serving");
    assert.equal(countMessages(messages, "Tab replacement failed and the original page"), 0, "so the stranded-stream net never fired");

    const readsAfterExhaustion = relay.healthReads();

    await advance(t, MONITOR_INTERVAL * 2);

    assert.ok(relay.healthReads() > readsAfterExhaustion, "and the monitor is still running against the stream it kept");
  });

  test("a fallback whose stream terminates mid-attempt releases the relay exactly once", async (t) => {

    /* The orphan exits, where the stream is gone or going. Termination disposed whatever the registry held, which during the attempt is the pending capture
     * identity - and that holds nothing - so the relay this frame is still carrying would poll and refresh forever if this arm did not release it. The count is
     * the whole assertion: one stop, from the single site that owns the matrix, not one per arm.
     */
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const relay = makeProxyDouble({ errored: true });

    entry = { ...makeEntry(9313), identity: makeNativeIdentity({ nativeProxy: relay.proxy }) };
    registerStream(entry);

    const fake = makeFakePage();

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "l3-stopped-1", streamInfo(9313),
      (): void => { /* The breaker is not what ends this stream. */ }, async (): Promise<Nullable<TabReplacementResult>> => {

        // The stream terminates while the replacement is in flight, which stops the monitor.
        handle?.dispose();

        return null;
      }, DIVERGENT_DEPS);

    await advance(t, MONITOR_INTERVAL * 2);

    assert.equal(relay.stops(), 1, "the relay is released exactly once on the orphan exit");
  });

  test("a fallback that ends in termination releases the relay exactly once", async (t) => {

    /* The other orphan exit: the attempt exhausts while the page it would have replaced is already gone, which is the one state that still strands a stream, so
     * the monitor terminates it explicitly. The relay is released on the way out for the same reason as the stop arm.
     */
    t.mock.timers.enable({ apis: [ "setInterval", "setTimeout", "Date" ] });

    const relay = makeProxyDouble({ errored: true });

    entry = { ...makeEntry(9314), identity: makeNativeIdentity({ nativeProxy: relay.proxy }) };
    registerStream(entry);

    const fake = makeFakePage();

    let breaks = 0;

    handle = monitorPlaybackHealth(fake.page, fake.page, makeProfile(), "https://two-phase.test/watch", "l3-terminated-1", streamInfo(9314),
      (): void => { breaks++; }, async (): Promise<Nullable<TabReplacementResult>> => {

        // The page the stream was running on is gone by the time the attempt exhausts.
        fake.setClosed(true);

        return null;
      }, DIVERGENT_DEPS);

    await advance(t, MONITOR_INTERVAL * 2);

    assert.equal(breaks, 1, "the stranded stream was terminated through the breaker");
    assert.equal(relay.stops(), 1, "and the relay was released exactly once");
  });
});

