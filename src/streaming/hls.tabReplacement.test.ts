/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hls.tabReplacement.test.ts: Tests for the two-phase tab replacement handler in hls.ts.
 *
 * The whole point of the design is what happens to the EXISTING pipeline while a replacement is being built, so these rows drive the real handler and assert
 * against the real registry: the outgoing capture session records its disposals, the outgoing page records its close, and every row that expects the recording to
 * survive says so by asserting those counts are zero. A row that only checked the handler's return value would pass just as happily against a handler that tore
 * the stream down first.
 *
 * The handler reaches the establishment through its injected TabReplacementDeps, so a substituted createPageWithCapture stands in for a Chrome tune - which is
 * also what lets a row fire the pipeline's own FFmpeg error callback at a chosen moment, before or after the swap, and read which way it was routed.
 */
import type { CreatePageWithCaptureOptions, CreatePageWithCaptureResult } from "./setup.ts";
import { afterEach, beforeEach, describe, test } from "node:test";
import { getStream, makePendingCaptureIdentity, registerStream, unregisterStream } from "./registry.ts";
import type { CaptureSession } from "./captureSession.ts";
import type { FMP4SegmenterResult } from "./fmp4Segmenter.ts";
import type { Nullable } from "../types/index.ts";
import type { Page } from "puppeteer-core";
import type { SegmenterContinuity } from "./fmp4Segmenter.ts";
import type { StreamRegistryEntry } from "./registry.ts";
import type { TabReplacementDeps } from "./hls.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { createTabReplacementHandler } from "./hls.ts";
import { makeProfile } from "../config/profiles.helpers.ts";
import { makeRegistryEntry } from "./registry.helpers.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

/**
 * A capture session that records its own teardown. Every row's real question is how many times a given pipeline was disposed, so the count is the fixture's
 * whole purpose; attachSegmenter records what was wired to it, which is how a row reads the seed the new segmenter was built with.
 */
interface RecordingSession extends CaptureSession {

  // How many times dispose() has been called on this session.
  readonly disposals: () => number;

  // The segmenter attachSegmenter received, or null before one was wired.
  readonly attached: () => Nullable<FMP4SegmenterResult>;
}

/**
 * Builds a capture session double that counts its disposals and remembers what was attached to it.
 * @param onAttach - Invoked as attachSegmenter runs, so a row can observe the exact instant of the wire-up.
 * @returns The recording session.
 */
function makeRecordingSession(onAttach?: () => void): RecordingSession {

  let attached: Nullable<FMP4SegmenterResult> = null;
  let disposals = 0;

  const dispose = (): void => { disposals++; };

  return {

    attachSegmenter: (segmenter: FMP4SegmenterResult): void => {

      attached = segmenter;
      onAttach?.();
    },
    attached: (): Nullable<FMP4SegmenterResult> => attached,
    disposals: (): number => disposals,
    dispose,
    disposed: false,
    segmenter: null,
    [Symbol.dispose]: dispose
  };
}

/**
 * Builds a segmenter double whose continuity snapshot is read live from the supplied counter, so a row can advance the sequence between the moment the
 * replacement starts and the moment it swaps - which is the only way to tell a snapshot taken at the swap from one taken at the top.
 * @param readIndex - Supplies the segment index the snapshot reports at the moment it is called.
 * @returns The segmenter double plus the count of snapshot reads it served.
 */
function makeOutgoingSegmenter(readIndex: () => number): { segmenter: FMP4SegmenterResult; snapshots: () => number } {

  let snapshots = 0;

  const segmenter = {

    getContinuitySnapshot: (): SegmenterContinuity => {

      snapshots++;

      return {

        initialTrackTimestamps: new Map<number, bigint>([[ 1, 90000n ]]),
        previousInitSegment: null,
        priorSessionStats: { malformedMoofCount: 0, syncSpreadCount: 0, syncSpreadMaxMs: 0, syncSpreadMinMs: 0, syncSpreadSumMs: 0, tabReplacementCount: 4 },
        startingInitVersion: 7,
        startingSegmentIndex: readIndex()
      };
    },
    pipe: (): void => { /* Nothing consumes this double's output. */ },
    stop: (): void => { /* The recording session's dispose is what the rows count. */ }
  } as unknown as FMP4SegmenterResult;

  return { segmenter, snapshots: (): number => snapshots };
}

/**
 * A page double that records whether it was closed. The outgoing page staying open is the visible half of "the recording survived", so every row that expects
 * survival reads this.
 * @returns The page double plus its close count.
 */
function makePageDouble(): { closes: () => number; page: Page } {

  let closes = 0;
  let closed = false;

  const page = {

    close: async (): Promise<void> => {

      closes++;
      closed = true;
    },
    isClosed: (): boolean => closed
  } as unknown as Page;

  return { closes: (): number => closes, page };
}

// The stream every row builds on: a registered entry in capture mode whose pipeline is a recording session with a live outgoing segmenter attached.
let entry: StreamRegistryEntry;
let oldSession: RecordingSession;
let oldPage: { closes: () => number; page: Page };
let outgoing: { segmenter: FMP4SegmenterResult; snapshots: () => number };
let outgoingIndex: number;

// The pages the injected unregisterManagedPage was asked to release, in call order.
let unregistered: Page[];

beforeEach(() => {

  outgoingIndex = 100;
  outgoing = makeOutgoingSegmenter(() => outgoingIndex);
  oldSession = makeRecordingSession();
  oldPage = makePageDouble();
  unregistered = [];

  // The recording session reports the outgoing segmenter through the same member production reads it through.
  Object.defineProperty(oldSession, "segmenter", { get: (): FMP4SegmenterResult => outgoing.segmenter });

  entry = makeRegistryEntry({

    identity: { ...makePendingCaptureIdentity(), captureCodec: "HEVC", captureSession: oldSession, hardwareAccelerated: true },
    page: oldPage.page
  });

  registerStream(entry);
});

afterEach(() => {

  unregisterStream(entry.id);
});

/**
 * Builds the handler under test with a substituted establishment.
 * @param establish - Stands in for createPageWithCapture; receives the same options the real one would.
 * @param onCircuitBreak - The breaker callback, so a row can count the times a fault was routed to it.
 * @returns The handler.
 */
function makeHandler(establish: TabReplacementDeps["createPageWithCapture"],
  onCircuitBreak: () => void = (): void => { /* Most rows expect the breaker never to be reached. */ }): () => Promise<Nullable<unknown>> {

  const deps: TabReplacementDeps = {

    createPageWithCapture: establish,
    unregisterManagedPage: (page: Page): void => { unregistered.push(page); }
  };

  return createTabReplacementHandler(entry.id, "tab-test", "tab-test-channel", "https://replacement.test/watch", makeProfile(), undefined, onCircuitBreak, deps);
}

/**
 * A successful establishment plus the recording handles behind it, so a row can read what happened to the fresh resources as well as to the outgoing ones.
 */
interface Establishment {

  // The fresh page, and how many times it was closed.
  readonly newPage: { closes: () => number; page: Page };

  // The fresh capture pipeline, and what it recorded.
  readonly newSession: RecordingSession;

  // The result the substituted establishment hands back.
  readonly result: CreatePageWithCaptureResult;
}

/**
 * Builds a successful establishment result around a fresh recording session and page.
 * @param onAttach - Forwarded to the new session, so a row can observe the wire-up instant.
 * @returns The establishment result plus the recording handles behind it.
 */
function makeEstablishment(onAttach?: () => void): Establishment {

  const newSession = makeRecordingSession(onAttach);
  const newPage = makePageDouble();

  return { newPage, newSession, result: { captureSession: newSession, context: newPage.page, directTune: false, manifestInterception: null,
    page: newPage.page } };
}

describe("createTabReplacementHandler: the replacement builds before it tears down", () => {

  test("an establishment that fails leaves the existing capture running and the stream alive", async () => {

    /* The acceptance behaviour, and the row that would have saved the two recordings lost to a browser that refused new capture starts. A handler that disposed
     * first would satisfy the return value and the log line just as well, so the assertions that carry the weight are the two counts: the outgoing pipeline was
     * never disposed and the outgoing page was never closed.
     */
    const handler = makeHandler(async (): Promise<CreatePageWithCaptureResult> => { throw new Error("Chrome refused to start the capture."); });

    const result = await handler();

    assert.equal(result, null, "the handler reports the miss");
    assert.equal(oldSession.disposals(), 0, "the outgoing capture pipeline was never disposed");
    assert.equal(oldPage.closes(), 0, "the outgoing page was never closed");
    assert.equal(getStream(entry.id)?.identity.mode, "capture", "the stream is still registered and still capturing");
    assert.equal((getStream(entry.id)?.identity as { captureSession: unknown }).captureSession, oldSession, "and still on its original pipeline");
  });

  test("a successful replacement swaps the pipeline and carries the encoder facts across", async () => {

    const establishment = makeEstablishment();
    const handler = makeHandler(async (): Promise<CreatePageWithCaptureResult> => establishment.result);

    const result = await handler();

    assert.notEqual(result, null, "the handler reports success");
    assert.equal(oldSession.disposals(), 1, "the outgoing pipeline is disposed exactly once, at the swap");
    assert.equal(oldPage.closes(), 1, "and the outgoing page closed with it");

    const identity = getStream(entry.id)?.identity;

    assert.ok(identity, "the stream is still registered");
    assert.equal(identity.mode, "capture");
    assert.equal((identity as { captureSession: unknown }).captureSession, establishment.newSession, "the entry points at the new pipeline");
    assert.equal(identity.captureCodec, "HEVC", "a replacement changes the page, not the codec decision");
    assert.equal((identity as { hardwareAccelerated: boolean }).hardwareAccelerated, true, "nor the acceleration fact");
  });

  test("the continuity seeding the new segmenter is read at the swap, not at the start", async () => {

    /* The outgoing segmenter keeps producing for the whole time the replacement page spends tuning, so its sequence advances across that window. The fixture
     * advances the index while the establishment is in flight; a handler that read continuity up front would seed 100 and rewind the playlist by fourteen
     * segments.
     */
    const establishment = makeEstablishment();

    const handler = makeHandler(async (): Promise<CreatePageWithCaptureResult> => {

      outgoingIndex = 114;

      return establishment.result;
    });

    await handler();

    assert.equal(outgoing.snapshots(), 1, "the continuity was read exactly once");

    const seeded = establishment.newSession.attached();

    assert.ok(seeded, "a segmenter was wired to the new pipeline");
    assert.equal(seeded.getSegmentIndex(), 114, "the new segmenter continues from where the outgoing one actually reached");
    assert.equal(seeded.getInitVersion(), 7, "and from the outgoing init version");
    assert.equal(seeded.getSessionStats().tabReplacementCount, 5, "the prior session's statistics carried across and counted this replacement");
  });

  test("a second attempt after a failed one re-reads continuity from the still-live outgoing segmenter", async () => {

    /* The retry semantics the monitor depends on. Because the first attempt disposed nothing, the second reads a segmenter that has gone on producing - so the
     * two attempts must seed different values. A handler that hoisted the continuity read into a closure would seed both attempts identically.
     */
    const establishment = makeEstablishment();
    let attempts = 0;

    const handler = makeHandler(async (): Promise<CreatePageWithCaptureResult> => {

      attempts++;
      outgoingIndex += 20;

      if(attempts === 1) {

        throw new Error("The first attempt could not establish.");
      }

      return establishment.result;
    });

    const first = await handler();

    assert.equal(first, null, "the first attempt reports the miss");
    assert.equal(oldSession.disposals(), 0, "and disposed nothing, which is what makes a second attempt worth making");
    assert.equal(outgoing.snapshots(), 0, "a failed attempt never even reads continuity");

    await handler();

    assert.equal(oldSession.disposals(), 1, "the second attempt is the one that swaps");
    assert.equal(establishment.newSession.attached()?.getSegmentIndex(), 140, "and seeds from the sequence the outgoing segmenter had actually reached by then");
  });

  test("the swap completes in one synchronous frame - nothing interleaves between the disposal and the entry write", async () => {

    /* The structural guarantee, read behaviourally. A microtask queued at the instant the new segmenter is attached is the earliest anything else could possibly
     * run; if the swap is synchronous it cannot run until the whole swap is done, so it observes the finished state. An await introduced anywhere inside the swap
     * lets it observe a stream whose old pipeline is gone and whose new one is not installed yet - which is precisely the window where two segmenters could write
     * one playlist.
     */
    let observedAtMicrotask: unknown = "the microtask never ran";
    let observedMode: unknown = null;

    const establishment = makeEstablishment(() => {

      queueMicrotask(() => {

        const identity = getStream(entry.id)?.identity;

        observedMode = identity?.mode;
        observedAtMicrotask = (identity as { captureSession: unknown } | undefined)?.captureSession;
      });
    });

    const handler = makeHandler(async (): Promise<CreatePageWithCaptureResult> => establishment.result);

    await handler();

    assert.equal(observedMode, "capture", "the earliest interleaving point already sees a capture identity");
    assert.equal(observedAtMicrotask, establishment.newSession, "and already sees the new pipeline installed - the swap never yielded midway");
  });
});

describe("createTabReplacementHandler: the replacement's own FFmpeg faults are routed by phase", () => {

  test("a fault before the swap discards the fresh resources and leaves the existing pipeline untouched", async () => {

    /* The wiring hazard the two-phase order creates: the replacement pipeline's error callback is live from the moment capture is acquired, and it points at the
     * same circuit breaker that terminates the stream. Before the swap that stream is the OLD one, which is healthy - so the fault must be absorbed. The fresh
     * page and capture are then discarded, which is asserted because leaking them would leave a Chrome tab and an FFmpeg child behind on every pre-swap fault.
     */
    const establishment = makeEstablishment();
    let breaks = 0;

    const handler = makeHandler(async (options: CreatePageWithCaptureOptions): Promise<CreatePageWithCaptureResult> => {

      options.onFFmpegError?.(new Error("The replacement's FFmpeg died during the tune."));

      return establishment.result;
    }, () => { breaks++; });

    const result = await handler();

    assert.equal(result, null, "the attempt is abandoned");
    assert.equal(breaks, 0, "the breaker is never reached - the stream the fault would have killed is the healthy one");
    assert.equal(oldSession.disposals(), 0, "the outgoing pipeline is untouched");
    assert.equal(oldPage.closes(), 0, "as is the outgoing page");
    assert.equal(establishment.newSession.disposals(), 1, "the fresh pipeline is disposed");
    assert.equal(establishment.newPage.closes(), 1, "and the fresh page is closed rather than leaked");
    assert.ok(unregistered.includes(establishment.newPage.page), "the fresh page is released from managed tracking on the way out");
  });

  test("a fault after the swap reaches the circuit breaker exactly once", async () => {

    // The other polarity, and the reason the phase is a tri-state rather than a suppression flag: once the swap has committed, the new pipeline IS the stream, so
    // a fault on it is a real stream failure and must escalate exactly as it always has.
    let breaks = 0;
    let raise: Nullable<(error: Error) => void> = null;

    const establishment = makeEstablishment();

    const handler = makeHandler(async (options: CreatePageWithCaptureOptions): Promise<CreatePageWithCaptureResult> => {

      raise = (error: Error): void => options.onFFmpegError?.(error);

      return establishment.result;
    }, () => { breaks++; });

    const result = await handler();

    assert.notEqual(result, null, "the replacement committed");
    assert.equal(breaks, 0, "and nothing had faulted yet");

    (raise as unknown as (error: Error) => void)(new Error("The live pipeline's FFmpeg died."));

    assert.equal(breaks, 1, "a post-swap fault breaks the circuit exactly once");
  });
});
