/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * ffmpeg.helpers.test.ts: Tests for the makeFakeFFmpeg double in ffmpeg.helpers.ts. Every suite whose rows reach the FFmpeg spawn takes its FFmpeg child from this
 * double, so a wrong teardown flag or a shared stream here would silently change what those suites prove. The rows assert the fresh state, the kill bookkeeping,
 * the streams' independence, and the double's parity with the FFmpegProcess shape.
 */
import { FAKE_FFMPEG_KEYS, makeFakeFFmpeg } from "./ffmpeg.helpers.ts";
import { describe, test } from "node:test";
import { PassThrough } from "node:stream";
import assert from "node:assert/strict";
import { assertSameShape } from "../testing.helpers.ts";

describe("makeFakeFFmpeg", () => {

  test("a fresh double is not shutting down and has counted no kill", () => {

    const ffmpeg = makeFakeFFmpeg();

    assert.equal(ffmpeg.isShuttingDown(), false, "no teardown has been requested on a fresh double");
    assert.equal(ffmpeg.kills(), 0, "a fresh double has counted no kill");
  });

  test("kill() sets the teardown flag and counts each call", () => {

    const ffmpeg = makeFakeFFmpeg();

    ffmpeg.kill();

    assert.equal(ffmpeg.isShuttingDown(), true, "the first kill sets the flag, as the real wrapper sets it unconditionally");
    assert.equal(ffmpeg.kills(), 1, "the first kill is counted");

    ffmpeg.kill();

    assert.equal(ffmpeg.isShuttingDown(), true, "a later kill leaves the flag set");
    assert.equal(ffmpeg.kills(), 2, "every kill is counted");
  });

  test("each double's stdin and stdout are live streams of its own", () => {

    const first = makeFakeFFmpeg();
    const second = makeFakeFFmpeg();

    assert.ok(first.stdin instanceof PassThrough, "stdin is a real stream the capture pipeline can write into");
    assert.ok(first.stdout instanceof PassThrough, "stdout is a real stream the segmenter can read from");
    assert.equal(first.stdin.writable, true, "stdin accepts writes");
    assert.equal(first.stdout.readable, true, "stdout can be read");
    assert.notEqual(first.stdin, first.stdout, "a double's stdin and stdout are distinct streams");
    assert.notEqual(first.stdin, second.stdin, "separate doubles never share a stdin");
    assert.notEqual(first.stdout, second.stdout, "separate doubles never share a stdout");
  });

  test("carries every FFmpegProcess member", () => {

    const reference = Object.fromEntries(FAKE_FFMPEG_KEYS.map((key) => [ key, undefined ]));

    assertSameShape(makeFakeFFmpeg(), reference, "makeFakeFFmpeg vs FakeFFmpeg's declared key set");
  });
});
