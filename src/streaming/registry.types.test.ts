/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * registry.types.test.ts: Compile-time tests for the stream identity discriminated union in registry.ts.
 *
 * What the union buys is the states it makes impossible, and that guarantee lives entirely in the type checker - no runtime assertion can observe a shape that
 * cannot be written. So the rows below assert it the way this codebase asserts every other type-level relationship: the legal shapes are written plainly and
 * the illegal ones sit under @ts-expect-error, where a directive that stops being needed is itself a failure. Each row still carries a runtime assertion so the
 * suite reports a real test rather than an empty one.
 */
import type { CaptureStreamIdentity, NativeStreamIdentity, StreamIdentity } from "./registry.ts";
import { describe, test } from "node:test";
import { makeNativeIdentity, makeRegistryEntry } from "./registry.helpers.ts";
import type { NativeProxy } from "../native/proxy.ts";
import type { StreamingMode } from "../types/index.ts";
import assert from "node:assert/strict";
import { makePendingCaptureIdentity } from "./registry.ts";

describe("the stream identity union", () => {

  test("a whole-identity replacement is how a stream changes mode", () => {

    // The sanctioned mutation: one assignment of a complete literal. Nothing partial is representable, so no frame can observe a half-flipped stream.
    const entry = makeRegistryEntry();

    assert.equal(entry.identity.mode, "capture", "a stream is born capturing");

    entry.identity = makeNativeIdentity({ nativeBandwidth: 5000000 });

    assert.equal(entry.identity.mode, "native");
    assert.equal(entry.identity.nativeBandwidth, 5000000, "the narrowed member is reachable off the replaced identity");
  });

  test("a within-variant refresh may spread its own identity", () => {

    // Same variant in, same variant out: there is no other variant's member for the spread to carry across, which is why this form stays allowed where a
    // mode-changing spread does not.
    const identity: StreamIdentity = { ...makeNativeIdentity(), captureCodec: "HEVC", nativeResolution: "1920x1080" };

    assert.equal(identity.mode, "native");
    assert.equal(identity.captureCodec, "HEVC");
  });

  test("rejects an in-place member write on a narrowed identity", () => {

    const identity = makePendingCaptureIdentity();

    // @ts-expect-error - every member is readonly, which is what forces a change to go through a whole-identity replacement rather than a field poke.
    identity.captureCodec = "H264";

    assert.equal(identity.captureCodec, "H264", "the runtime write still lands - the rejection is the compiler's, which is the point");
  });

  test("rejects a capture literal carrying a native member", () => {

    const proxy = {} as NativeProxy;

    // @ts-expect-error - the excess-property check is what stops a native member being smuggled onto a capture identity.
    const smuggled: CaptureStreamIdentity = { captureCodec: null, captureSession: null, hardwareAccelerated: false, mode: "capture", nativeProxy: proxy };

    assert.equal(smuggled.mode, "capture");
  });

  test("rejects a native literal carrying a capture member", () => {

    // @ts-expect-error - the same check in the other direction: a native identity has no capture pipeline to hold.
    const smuggled: NativeStreamIdentity = { ...makeNativeIdentity(), captureSession: null };

    assert.equal(smuggled.mode, "native");
  });

  test("rejects a native identity without its proxy", () => {

    // @ts-expect-error - a native stream always has a proxy, which is what makes the permanently-no-op health check unrepresentable rather than merely guarded.
    const proxyless: NativeStreamIdentity = { ...makeNativeIdentity(), nativeProxy: null };

    assert.equal(proxyless.mode, "native");
  });

  test("the identity's mode members are exactly the StreamingMode the flat DTOs speak", () => {

    /* The variants spell their mode as literals rather than deriving it, so these two assignments are what holds the union and the wire vocabulary to the same
     * closed set: a StreamingMode the union cannot express, or a mode the DTOs cannot carry, fails here rather than at some distant projection.
     */
    const fromIdentity: StreamingMode = makePendingCaptureIdentity().mode;
    const wireMode: StreamingMode = "native";
    const fromDto: StreamIdentity["mode"] = wireMode;

    assert.equal(fromIdentity, "capture");
    assert.equal(fromDto, "native");
  });
});
