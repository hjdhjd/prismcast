/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * registry.helpers.test.ts: Tests for the makeRegistryEntry factory in registry.helpers.ts. The factory is consumed across streaming/ and hdhr/ test files;
 * a bug in defaults or override-merging here would silently affect every dependent suite. Tests pin: default-shape correctness, id auto-allocation, streamIdStr
 * derivation, override merge semantics, hls always being a fresh HLSState (not a shared mutable reference), and the "capture" streamingMode default.
 */
import { STREAM_REGISTRY_ENTRY_KEYS, makeRegistryEntry } from "./registry.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { assertSameShape } from "../testing.helpers.ts";

describe("makeRegistryEntry", () => {

  test("populates every StreamRegistryEntry key (parity check against the type's complete key set)", () => {

    /* Two-layer drift catch:
     *
     * - STREAM_REGISTRY_ENTRY_KEYS is built via declareKeysOf<StreamRegistryEntry>(), whose compile-time check forces the array to be updated when the type
     *   gains a key (otherwise the call to declareKeysOf fails to compile).
     * - assertSameShape then compares the factory's runtime output keys against the array. If the array is updated but the factory isn't, this assertion fails.
     *
     * Together they make silent drift impossible: the type, the key list, and the factory must all stay in lockstep, and a regression in any one of the three
     * surfaces immediately.
     */
    const entry = makeRegistryEntry();
    const reference = Object.fromEntries(STREAM_REGISTRY_ENTRY_KEYS.map((k) => [ k, undefined ]));

    assertSameShape(entry, reference, "makeRegistryEntry vs StreamRegistryEntry's declared key set");
  });

  test("auto-allocates a fresh id from getNextStreamId when not overridden", () => {

    const a = makeRegistryEntry();
    const b = makeRegistryEntry();

    assert.notEqual(a.id, b.id, "consecutive entries get distinct auto-allocated ids");
    assert.equal(typeof a.id, "number");
  });

  test("respects an explicit id override", () => {

    const entry = makeRegistryEntry({ id: 42 });

    assert.equal(entry.id, 42);
  });

  test("derives streamIdStr from the resolved channelName/url via the production formatter", () => {

    // The factory delegates to generateStreamId so the fixture shape stays in lockstep with production. Without a channelName, the format derives from the URL
    // host: "<domain>-<6charRequestId>". The prefix may contain dots (e.g., "example.test"); we assert on the shape rather than the exact string because the
    // request-id suffix is random.
    const entry = makeRegistryEntry();

    assert.match(entry.streamIdStr, /^[\w.-]+-[a-z0-9]{6}$/, "streamIdStr must follow the production '<prefix>-<requestId>' shape");
  });

  test("uses the channelName as the streamIdStr prefix when provided", () => {

    const entry = makeRegistryEntry({ channelName: "abc" });

    assert.match(entry.streamIdStr, /^abc-[a-z0-9]{6}$/, "channelName takes precedence over the URL host as the prefix");
  });

  test("respects an explicit streamIdStr override", () => {

    const entry = makeRegistryEntry({ id: 7, streamIdStr: "custom-stream-id" });

    assert.equal(entry.streamIdStr, "custom-stream-id");
  });

  test("returns a fresh HLSState on each call (not a shared mutable reference)", () => {

    // Two calls share defaults but the hls field must be a fresh object - otherwise tests that mutate hls state on one entry would observe leaks into others.
    const a = makeRegistryEntry();
    const b = makeRegistryEntry();

    assert.notEqual(a.hls, b.hls, "hls is not a shared reference across calls");
  });

  test("sets safe neutral defaults (null/empty/zero) for every nullable field", () => {

    const entry = makeRegistryEntry();

    assert.equal(entry.captureCodec, null);
    assert.equal(entry.channelName, null);
    assert.equal(entry.clientAddress, null);
    assert.equal(entry.ffmpegProcess, null);
    assert.equal(entry.hardwareAccelerated, false);
    assert.equal(entry.mpegTsClientCount, 0);
    assert.equal(entry.nativeBandwidth, 0);
    assert.equal(entry.nativeProxy, null);
    assert.equal(entry.nativeResolution, null);
    assert.equal(entry.page, null);
    assert.equal(entry.preTuned, false);
    assert.equal(entry.profile, null);
    assert.equal(entry.rawCaptureStream, null);
    assert.equal(entry.segmenter, null);
    assert.equal(entry.stopMonitor, null);
  });

  test("defaults to 'capture' streamingMode and a stable test URL", () => {

    const entry = makeRegistryEntry();

    assert.equal(entry.streamingMode, "capture");
    assert.equal(entry.url, "https://example.test/stream");
  });

  test("defaults info.storeKey to 'test-channel' with a zeroed lastPlaylistRequest", () => {

    const entry = makeRegistryEntry();

    assert.deepEqual(entry.info, { lastPlaylistRequest: 0, storeKey: "test-channel" });
  });

  test("startTime is a Date instance close to now", () => {

    // The factory uses new Date() as the default. Tests can override with a fixed value if they need deterministic timestamps.
    const before = Date.now();
    const entry = makeRegistryEntry();
    const after = Date.now();

    assert.ok(entry.startTime instanceof Date);
    assert.ok((entry.startTime.getTime() >= before) && (entry.startTime.getTime() <= after), "startTime is current");
  });

  test("merges overrides shallowly on top of defaults", () => {

    const entry = makeRegistryEntry({

      channelName: "ABC",
      info: { lastPlaylistRequest: 12345, storeKey: "custom-key" },
      streamingMode: "native"
    });

    assert.equal(entry.channelName, "ABC");
    assert.deepEqual(entry.info, { lastPlaylistRequest: 12345, storeKey: "custom-key" });
    assert.equal(entry.streamingMode, "native");
    // Non-overridden fields retain defaults.
    assert.equal(entry.captureCodec, null);
    assert.equal(entry.url, "https://example.test/stream");
  });

  test("returns a fresh entry on each call (no shared mutable state across calls)", () => {

    const a = makeRegistryEntry();
    const b = makeRegistryEntry();

    a.captureCodec = "h264";

    assert.equal(b.captureCodec, null, "mutating one entry does not affect another");
  });
});
