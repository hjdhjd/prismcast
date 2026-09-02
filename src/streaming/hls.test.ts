/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * hls.test.ts: Unit tests for the synchronous helpers in the HLS request handler module. hls.ts orchestrates the entire HLS streaming pipeline (channel
 * validation, pending-stream registration, native vs capture path selection, segmenter creation, monitor wiring) and the orchestration entrypoints
 * (handleHLSPlaylist, handleHLSSegment, ensureChannelStream, initializeStream, startHLSStream, completeStreamSetup) require a real Chrome browser, FFmpeg
 * subprocess, and Express runtime to exercise honestly. The unit-testable surface here is the module's pure, browser-free helpers, which translate inputs to
 * values without touching the browser or the registry beyond config lookups.
 *
 * The login-mode 503 branch lives in a sibling file (hls.loginMode.test.ts), which drives the real isLoginModeActive() flag through the setBrowserAccessors()
 * dependency injection point - the same one browser/index.ts wires at startup - with a stub browser and page, rather than substituting the accessor.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { buildResumeContinuity, handleHLSSegment, hasStreamCapacity, sendValidationError, validateChannel } from "./hls.ts";
import { registerStream, unregisterStream } from "./registry.ts";
import { setChannelStreamId, terminateStream } from "./lifecycle.ts";
import { storeInitSegment, storeNamedInitSegment, storeSegment } from "./hlsSegments.ts";
import { CONFIG } from "../config/index.ts";
import { LOG } from "../utils/index.ts";
import type { Response } from "express";
import type { ResumeData } from "./hlsResume.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { makeRegistryEntry } from "./registry.helpers.ts";
import { makeReqRes } from "../routes/express.helpers.ts";
import { setServiceSelections } from "../config/services.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

describe("validateChannel", () => {

  // Snapshot CONFIG.channels.disabledPredefined and the serviceSelections cache so tests that mutate either restore them in afterEach. Module-level state is
  // shared across the test process; without restoration, sibling tests in this file (or sibling test files) would observe leaked state.
  let savedDisabled: string[] = [];

  beforeEach(() => {

    savedDisabled = [...CONFIG.channels.disabledPredefined];
  });

  afterEach(() => {

    CONFIG.channels.disabledPredefined = savedDisabled;
    setServiceSelections({});
  });

  test("returns valid: false with status 404 for an unknown channel", () => {

    // Negative test: a totally unknown channel must produce a 404 with a textual body.
    const result = validateChannel("totally-not-a-real-channel-x9z2");

    assert.equal(result.valid, false);

    // The result type is a discriminated union: when valid is false the body is Record<string, string> | string and the status code is the HTTP code. We cast
    // explicitly here so the type system surfaces the body as the string variant rather than relying on String() coercion which would stringify a record as
    // "[object Object]".
    const failure = result as { body: Record<string, string> | string; statusCode: number };

    assert.equal(failure.statusCode, 404);
    assert.equal(typeof failure.body, "string");

    if(typeof failure.body === "string") {

      assert.match(failure.body, /not found/i);
    }
  });

  test("returns valid: false with status 404 'Channel is disabled.' when the predefined channel is in disabledPredefined", () => {

    /* The disabled-channel arm of the discriminated union. We mutate CONFIG.channels.disabledPredefined to include a known predefined channel ("abc") and call
     * validateChannel against it. isPredefinedChannelDisabled is the first check in validateChannel so this branch fires before any service-resolution work,
     * keeping the test independent of serviceGroups state.
     */
    CONFIG.channels.disabledPredefined = ["abc"];

    const result = validateChannel("abc");

    // assert.equal narrows the discriminated union via its `asserts` overload, so the failure-arm fields below are accessible without an additional if guard.
    assert.equal(result.valid, false);
    assert.equal(result.statusCode, 404);
    assert.equal(result.body, "Channel is disabled.");
  });

  test("returns valid: true with the resolved channel and resolvedKey for a known enabled predefined channel (success path)", () => {

    /* The discriminated-union success arm: a predefined channel that is not disabled, has at least one variant whose service tag is enabled, and is not
     * blocked by login mode. With CONFIG.channels.disabledPredefined empty, no service filter active, and login mode off, "abc" resolves cleanly. The success
     * branch returns { channel, resolvedKey, valid: true }; we lock both projections so a future change that drops a field surfaces here.
     */
    const result = validateChannel("abc");

    assert.equal(result.valid, true);
    assert.ok(result.channel, "resolved channel surfaced");
    assert.equal(result.resolvedKey, "abc", "no service selection -> resolvedKey is the canonical key");
  });

  test("warns and falls back to the canonical channel when a service selection points at a missing variant", () => {

    /* The missing-variant fallback warning in validateChannel. When serviceSelections maps a canonical to a variant key that no longer resolves to a real channel
     * (e.g., the variant was removed after the selection was saved), validateChannel logs a LOG.warn and falls back to getAllChannels()[channelName] -
     * effectively serving the canonical's URL instead of failing the request. The test seeds a synthetic missing variant via setServiceSelections, captures
     * LOG.warn via mock.method, and asserts both the fallback channel surface AND the warning. Without the warning assertion, a refactor that quietly dropped
     * the log line would leave operators without the diagnostic signal that originally motivated the warning.
     */
    setServiceSelections({ abc: "abc-nonexistent-variant" });

    const warnSpy = mock.method(LOG, "warn", () => undefined);

    try {

      const result = validateChannel("abc");

      assert.equal(result.valid, true, "fallthrough yields a valid result, not a 404");
      assert.equal(result.resolvedKey, "abc-nonexistent-variant", "resolvedKey reflects the user's selection even though it didn't resolve");
      assert.ok(result.channel, "channel populated from the canonical fallback via getAllChannels()");

      assert.equal(warnSpy.mock.callCount(), 1, "exactly one warning emitted for the missing-variant fallback");

      // The warning is emitted as LOG.warn(format, ...args). The first argument is the format string which contains the diagnostic copy we want to lock.
      const message = warnSpy.mock.calls[0]!.arguments[0]!;

      assert.match(message, /not found/i, "warning message identifies the missing-service condition");
    } finally {

      warnSpy.mock.restore();
    }
  });
});

describe("sendValidationError", () => {

  test("uses res.send for plain-string body", () => {

    // The handler dispatches based on typeof body. A string body must go through res.send, not res.json.
    const { res, send, status } = makeReqRes();

    sendValidationError({ body: "Channel is disabled.", statusCode: 404 }, res);

    assert.equal(status.mock.calls[0]?.arguments[0], 404);
    assert.equal(send.mock.calls[0]?.arguments[0], "Channel is disabled.");
  });

  test("uses res.json for object body", () => {

    // Login-mode rejection bodies are objects. They must go through res.json so Channels DVR clients can parse the structured error.
    const { json, res, status } = makeReqRes();
    const body = { error: "Login in progress", message: "Please complete authentication before starting new streams." };

    sendValidationError({ body, statusCode: 503 }, res);

    assert.equal(status.mock.calls[0]?.arguments[0], 503);
    assert.deepEqual(json.mock.calls[0]?.arguments[0], body);
  });

  test("forwards the statusCode from the validation result verbatim", () => {

    const { res, status } = makeReqRes();

    sendValidationError({ body: "Forbidden.", statusCode: 403 }, res);

    assert.equal(status.mock.calls[0]?.arguments[0], 403);
  });

  test("does NOT crash when the response double is non-fluent (does not return self)", () => {

    // Defensive: the canonical makeReqRes spy returns res from status() so handlers can chain. A future regression could break that, or the production code
    // could be invoked against a stub that does not chain - verify sendValidationError tolerates a response whose setters return undefined. We construct a
    // minimal inline double here rather than going through makeReqRes precisely because makeReqRes IS fluent.
    const captured = { body: undefined as unknown, status: 0 };

    const res = {

      json: (b: unknown) => { captured.body = b; },
      send: (b: unknown) => { captured.body = b; },
      status: (s: number) => {

        captured.status = s;

        return res;
      }
    } as unknown as Response;

    assert.doesNotThrow(() => {

      sendValidationError({ body: "x", statusCode: 418 }, res);
    });

    assert.equal(captured.status, 418);
    assert.equal(captured.body, "x");
  });
});

describe("hasStreamCapacity", () => {

  /* hasStreamCapacity is the pure single source of truth for the concurrent-stream capacity decision, extracted so the boundary arithmetic is pinnable without a
   * browser. The reservation it backs (reserveStreamSlot) is evaluated at the registration site BEFORE the new stream's pending entry is registered, so the count
   * passed in always excludes the stream being admitted. Because the decision is centralized here and evaluated on that self-excluded count, the final free slot
   * is always admitted rather than rejected.
   */

  test("admits the final slot - activeCount one below the limit returns true (the boundary regression)", () => {

    // The regression case: with the limit at 4 and three streams already active, a fourth stream (excluded from this count) must be admitted rather than
    // rejected with a 503 after a preroll has already been sent.
    assert.equal(hasStreamCapacity(3, 4), true, "the last free slot is admitted, not rejected");

    // The same boundary at a limit of 1: zero active streams must admit the first stream.
    assert.equal(hasStreamCapacity(0, 1), true, "the first stream is admitted when the limit is 1");
  });

  test("rejects when the registry is already full - activeCount equal to the limit returns false", () => {

    // At the limit with no slot to spare, the new stream cannot be admitted (the caller then attempts an idle reclaim before issuing a 503).
    assert.equal(hasStreamCapacity(4, 4), false, "a full registry rejects the new stream");
    assert.equal(hasStreamCapacity(1, 1), false, "a single-slot limit rejects a second stream");
  });

  test("rejects when the registry is over the limit - activeCount above the limit returns false", () => {

    // Over-capacity is reachable when the operator lowers maxConcurrentStreams while streams are running. The predicate must keep rejecting until the count drains
    // back below the limit.
    assert.equal(hasStreamCapacity(6, 4), false, "an over-capacity registry keeps rejecting");
  });

  test("admits freely below the limit", () => {

    // Well below the limit, every new stream is admitted.
    assert.equal(hasStreamCapacity(0, 10), true);
    assert.equal(hasStreamCapacity(5, 10), true);
  });
});

describe("handleHLSSegment: named init segments (T5)", () => {

  test("serves a relay-minted init segment through the generic segment route", () => {

    /* The names a native fMP4 relay mints for upstream initialization segments are requested on the same route as media segments. Without the init store in the
     * generic lookup chain, every one of those references would 404 and the playlist would point at nothing.
     */
    const entry = makeRegistryEntry({ channelName: "named-init-channel" });

    registerStream(entry);
    setChannelStreamId("named-init-channel", entry.id);

    const videoInit = Buffer.from("relay-video-init");

    storeNamedInitSegment(entry.id, "video", "init-v0.mp4", videoInit);

    try {

      const { req, res, send, setHeader } = makeReqRes({ params: { name: "named-init-channel", segment: "init-v0.mp4" } });

      handleHLSSegment(req, res);

      assert.deepEqual(send.mock.calls[0]?.arguments[0], videoInit, "the relay's init is served");
      assert.ok(setHeader.mock.calls.some((call) => (call.arguments[0] === "Content-Type") && (call.arguments[1] === "video/mp4")),
        "an init segment is served as video/mp4");
    } finally {

      terminateStream(entry.id, "named-init-channel", "test cleanup");
      unregisterStream(entry.id);
    }
  });

  test("the capture path's literal init.mp4 branch still resolves the single slot", () => {

    // The literal branch belongs to capture's own init slot and must keep answering independently of the relay store.
    const entry = makeRegistryEntry({ channelName: "capture-init-channel" });

    registerStream(entry);
    setChannelStreamId("capture-init-channel", entry.id);

    const captureInit = Buffer.from("capture-moov");

    storeInitSegment(entry.id, captureInit);

    try {

      const { req, res, send } = makeReqRes({ params: { name: "capture-init-channel", segment: "init.mp4" } });

      handleHLSSegment(req, res);

      assert.deepEqual(send.mock.calls[0]?.arguments[0], captureInit, "the capture slot answers the literal name");
    } finally {

      terminateStream(entry.id, "capture-init-channel", "test cleanup");
      unregisterStream(entry.id);
    }
  });

  test("serves .m4s media segments as video/mp4 and .ts media segments as video/MP2T", () => {

    // Container-true naming only helps if the Content-Type follows it, so both extensions are asserted on the same route.
    const entry = makeRegistryEntry({ channelName: "content-type-channel" });

    registerStream(entry);
    setChannelStreamId("content-type-channel", entry.id);

    storeSegment(entry.id, "segment0.m4s", Buffer.from("fmp4-fragment"));
    storeSegment(entry.id, "segment1.ts", Buffer.from("ts-fragment"));

    try {

      const fmp4 = makeReqRes({ params: { name: "content-type-channel", segment: "segment0.m4s" } });

      handleHLSSegment(fmp4.req, fmp4.res);

      assert.ok(fmp4.setHeader.mock.calls.some((call) => (call.arguments[0] === "Content-Type") && (call.arguments[1] === "video/mp4")),
        "an .m4s fragment is served as video/mp4");

      const ts = makeReqRes({ params: { name: "content-type-channel", segment: "segment1.ts" } });

      handleHLSSegment(ts.req, ts.res);

      assert.ok(ts.setHeader.mock.calls.some((call) => (call.arguments[0] === "Content-Type") && (call.arguments[1] === "video/MP2T")),
        "a .ts fragment keeps video/MP2T");
    } finally {

      terminateStream(entry.id, "content-type-channel", "test cleanup");
      unregisterStream(entry.id);
    }
  });

  test("returns 404 for a name that matches no segment and no init", () => {

    const entry = makeRegistryEntry({ channelName: "missing-name-channel" });

    registerStream(entry);
    setChannelStreamId("missing-name-channel", entry.id);

    try {

      const { req, res, send, status } = makeReqRes({ params: { name: "missing-name-channel", segment: "init-v9.mp4" } });

      handleHLSSegment(req, res);

      assert.equal(status.mock.calls[0]?.arguments[0], 404);
      assert.equal(send.mock.calls[0]?.arguments[0], "Segment not found.");
    } finally {

      terminateStream(entry.id, "missing-name-channel", "test cleanup");
      unregisterStream(entry.id);
    }
  });
});
describe("buildResumeContinuity", () => {

  /* The derivation reads as a set of conditional arms and the rows walk every one of them, because the segmenter reads this object by key presence rather than
   * by value: a member that is absent and a member set to a zero or empty stand-in mean different things to it. So every row reads the returned object's own keys
   * with the "in" operator rather than comparing values, which is the only way to tell an absent member from one present and undefined.
   */

  // The prior session a resume carries back from disk. One literal, reused by the rows that resume, so a shape change lands in one place.
  const resumed: ResumeData = {

    initSegment: Buffer.from([ 1, 2, 3 ]),
    initVersion: 7,
    segmentIndex: 42,
    trackTimestamps: new Map([[ 1, 90000n ]])
  };

  test("a resume with no preroll ahead of it carries the whole prior session, and no session statistics", () => {

    /* The both-spreads arm, and the row the absence assertion is really for. A resume from disk has no session statistics at all, so the object must not carry
     * the member: present-but-empty would tell the segmenter a live prior session is continuing and mint a tab replacement that never happened into the resumed
     * stream's summary.
     */
    const result = buildResumeContinuity({ baseSegmentIndex: 42, prerollSegmentCount: 0, resumeData: resumed });

    assert.equal(result.initialTrackTimestamps, resumed.trackTimestamps, "the prior session's timestamps, by reference");
    assert.equal(result.previousInitSegment, resumed.initSegment, "and its init segment, because no preroll window precedes this one");
    assert.equal(result.startingInitVersion, 7, "and its init version");
    assert.equal(result.startingSegmentIndex, 42, "starting where the resume left off, with no preroll range to add");
    assert.equal("priorSessionStats" in result, false, "and no session statistics member at all, not an empty one");
  });

  test("a resume behind a preroll window drops the init segment and offsets the index by the preroll range", () => {

    /* The inner-conditional arm. The preroll init differs from the real one, so handing the prior init segment across a preroll boundary would let a byte match
     * suppress a discontinuity the boundary genuinely needs - the member is withheld rather than nulled.
     */
    const result = buildResumeContinuity({ baseSegmentIndex: 42, prerollSegmentCount: 5, resumeData: resumed });

    assert.equal("previousInitSegment" in result, false, "the init segment is withheld across a preroll boundary");
    assert.equal(result.startingSegmentIndex, 47, "and the index accounts for the resume offset and the preroll range together");
    assert.equal(result.initialTrackTimestamps, resumed.trackTimestamps, "the timestamps still continue");
    assert.equal(result.startingInitVersion, 7, "and so does the init version");
    assert.equal("priorSessionStats" in result, false, "with no session statistics here either");
  });

  test("a fresh stream behind a preroll window carries the preroll offset and nothing else", () => {

    // The index-only arm: there is no prior session to continue, but the preroll segments still occupy the range the real ones start after.
    const result = buildResumeContinuity({ baseSegmentIndex: 0, prerollSegmentCount: 5, resumeData: null });

    assert.deepEqual(Object.keys(result), ["startingSegmentIndex"], "exactly one member, and it is the index");
    assert.equal(result.startingSegmentIndex, 5, "which is the preroll range the real segments follow");
  });

  test("a fresh stream with no preroll continues from nothing at all", () => {

    // The empty arm. A stream that starts from nothing must hand the segmenter an object with no members, so every default the segmenter owns stays in force.
    const result = buildResumeContinuity({ baseSegmentIndex: 0, prerollSegmentCount: 0, resumeData: null });

    assert.equal(Object.keys(result).length, 0, "nothing to continue from, so nothing is carried");
  });
});
