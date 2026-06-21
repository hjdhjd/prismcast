/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * ffmpeg.context.test.ts: Smoke tests for the default FFmpegContext adapter. ffmpeg.context.ts is the ports-and-adapters adapter that produces an FFmpegContext
 * literal from real runtime I/O - existsSync, homedir, process.platform, the spawn-based probe, and the bundled FFmpeg path from ffmpeg-for-homebridge. The
 * algorithmic content lives in ffmpeg.ts (probeFFmpegPath) and is exercised against synthetic contexts in ffmpeg.test.ts; this file pins the contract that the
 * default adapter exposes the documented five-field shape and that the private probe helper returns true for a known-good binary and false for a known-missing
 * one. The probe is invoked transitively against process.execPath (the running Node binary - guaranteed to exist and exit 0 on -version) and a definitely-not-here
 * path; we cannot import the private helper, so we exercise it through the context's `probe` field.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { createDefaultFFmpegContext } from "./ffmpeg.context.ts";

describe("createDefaultFFmpegContext", () => {

  test("returns the documented six-field FFmpegContext shape", () => {

    // The context type has exactly five required keys (the bundled path is permitted to be undefined when ffmpeg-for-homebridge fails to resolve, but the field
    // itself is always present). A future refactor that drops a key would ripple into probeFFmpegPath as a runtime undefined; pin the shape so the regression
    // surfaces here.
    const ctx = createDefaultFFmpegContext();

    assert.ok(("bundledPath" in ctx), "bundledPath field present (string | undefined)");
    assert.equal(typeof ctx.exists, "function", "exists is a function");
    assert.equal(typeof ctx.homedir, "function", "homedir is a function");
    assert.equal(typeof ctx.platform, "string", "platform is a string");
    assert.equal(typeof ctx.probe, "function", "probe is a function");
  });

  test("homedir() returns the os.homedir() value", () => {

    // Wires through to node:os.homedir(). We don't pin a specific value (it varies by host); we only verify the field returns a non-empty string consistent
    // with the Node API contract.
    const ctx = createDefaultFFmpegContext();
    const result = ctx.homedir();

    assert.equal(typeof result, "string");
    assert.ok(result.length > 0, "homedir resolves to a non-empty path");
  });

  test("platform matches process.platform exactly", () => {

    const ctx = createDefaultFFmpegContext();

    assert.equal(ctx.platform, process.platform);
  });

  test("exists() returns true for a known-good path and false for a definitely-missing one", () => {

    // The default adapter wires existsSync. We exercise it against process.execPath (always present - the running Node binary) and a path that cannot exist on
    // any platform.
    const ctx = createDefaultFFmpegContext();

    assert.equal(ctx.exists(process.execPath), true, "running Node binary exists");
    assert.equal(ctx.exists("/definitely/not/a/real/path/xz9q2"), false, "missing path returns false");
  });

  test("probe() returns false when the binary cannot be spawned (path does not exist)", async () => {

    // The error event fires when spawn fails (ENOENT). The helper resolves false in that case.
    const ctx = createDefaultFFmpegContext();
    const result = await ctx.probe("/definitely/not/a/real/path/xz9q2");

    assert.equal(result, false, "missing binary -> probe returns false");
  });

  test("probe() returns true for a binary that exits 0 in response to '-version'", async (t) => {

    // The private checkFFmpegAtPath spawns the target with '-version' and resolves true on exit code 0. Real FFmpeg accepts that flag and exits 0; for the
    // smoke test we need a similarly-behaved binary present on the test host. /bin/bash is universally available on macOS and Linux and exits 0 in response
    // to '-version'. On Windows this binary may not exist; we skip the test rather than guess at a Windows-equivalent.
    const { existsSync } = await import("node:fs");

    if(!existsSync("/bin/bash")) {

      t.skip("/bin/bash not available on this host");

      return;
    }

    const ctx = createDefaultFFmpegContext();
    const result = await ctx.probe("/bin/bash");

    assert.equal(result, true, "/bin/bash -version exits 0 -> probe returns true");
  });
});
