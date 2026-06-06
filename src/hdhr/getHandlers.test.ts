/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * getHandlers.test.ts: Unit tests for the HDHR UDP Get-dispatch table. The module is pure, so the tests are mechanical: build a synthetic GetContext, call
 * resolveGet, and assert the returned result. Three families of behavior:
 *
 *   1. /sys/* keys resolve to constants (model, hwmodel, copyright) or to the runtimeVersion captured in the context (/sys/version).
 *
 *   2. /tuner&lt;N&gt;/* keys resolve to per-slot formatted strings. Active slots produce channel-bound output; idle slots produce the documented "none" forms.
 *
 *   3. Unknown keys (including out-of-range tuner indices) produce the canonical HDHR error string.
 */
import { HDHR_COPYRIGHT, HDHR_HW_MODEL, HDHR_MODEL } from "./identity.ts";
import { describe, test } from "node:test";
import type { GetContext } from "./getHandlers.ts";
import type { TunerState } from "./tunerState.ts";
import assert from "node:assert/strict";
import { resolveGet } from "./getHandlers.ts";

// makeContext builds a synthetic GetContext with the supplied tuners and a fixed runtime version. Tests pass their own tuner array; defaulting it to empty
// keeps system-key tests minimal.
function makeContext(tuners: readonly TunerState[] = []): GetContext {

  return { runtimeVersion: "1.10.3-test", tuners };
}

// makeIdleSlot constructs a TunerState record representing an idle slot at the given index.
function makeIdleSlot(slot: number): TunerState {

  return {

    active: false,
    channelName: null,
    channelNumber: null,
    clientAddress: null,
    resource: "tuner" + String(slot),
    slot
  };
}

// makeActiveSlot constructs a TunerState record representing an active stream on the given slot with the given channel info.
function makeActiveSlot(slot: number, channelName: string, channelNumber: number, clientAddress: string | null = "10.0.0.5"): TunerState {

  return {

    active: true,
    channelName,
    channelNumber,
    clientAddress,
    resource: "tuner" + String(slot),
    slot
  };
}

describe("resolveGet - system keys", () => {

  test("/sys/version returns the runtime version captured in context", () => {

    const result = resolveGet("/sys/version", makeContext());

    assert.deepEqual(result, { kind: "value", value: "1.10.3-test" });
  });

  test("/sys/model returns the identity SSOT value", () => {

    assert.deepEqual(resolveGet("/sys/model", makeContext()), { kind: "value", value: HDHR_MODEL });
  });

  test("/sys/hwmodel returns the identity SSOT value", () => {

    assert.deepEqual(resolveGet("/sys/hwmodel", makeContext()), { kind: "value", value: HDHR_HW_MODEL });
  });

  test("/sys/copyright returns the identity SSOT value", () => {

    assert.deepEqual(resolveGet("/sys/copyright", makeContext()), { kind: "value", value: HDHR_COPYRIGHT });
  });

  test("/sys/debug returns an empty string", () => {

    assert.deepEqual(resolveGet("/sys/debug", makeContext()), { kind: "value", value: "" });
  });
});

describe("resolveGet - per-tuner keys for an active slot", () => {

  // Shared active-slot fixture; every test in this block references slot 0 bound to "CNN" channel 1000.
  const ctx = makeContext([ makeActiveSlot(0, "CNN", 1000), makeIdleSlot(1) ]);

  test("/tuner0/channel returns 'auto:<number>' for an active slot", () => {

    assert.deepEqual(resolveGet("/tuner0/channel", ctx), { kind: "value", value: "auto:1000" });
  });

  test("/tuner0/status reports channel binding and synthetic 100% signal metrics", () => {

    const result = resolveGet("/tuner0/status", ctx);

    // Match the structural shape first so the narrowed branch below is type-safe; then assert the value contents via regex so the ordering or padding of the
    // key=value pairs can evolve without breaking the test. The load-bearing claim is "all fields present with their expected values for an active stream".
    assert.equal(result.kind, "value");
    assert.match((result as { value: string }).value, /ch=auto:1000/);
    assert.match((result as { value: string }).value, /lock=8vsb/);
    assert.match((result as { value: string }).value, /ss=100/);
  });

  test("/tuner0/streaminfo returns the channel name for an active slot", () => {

    assert.deepEqual(resolveGet("/tuner0/streaminfo", ctx), { kind: "value", value: "CNN" });
  });

  test("/tuner0/target returns 'none' regardless of state (PrismCast does not forward RTP)", () => {

    assert.deepEqual(resolveGet("/tuner0/target", ctx), { kind: "value", value: "none" });
  });

  test("/tuner0/vchannel returns the bare numeric channel number for an active slot", () => {

    assert.deepEqual(resolveGet("/tuner0/vchannel", ctx), { kind: "value", value: "1000" });
  });

  test("/tuner0/vstatus reports virtual channel binding for an active slot", () => {

    const result = resolveGet("/tuner0/vstatus", ctx);

    assert.equal(result.kind, "value");
    assert.match((result as { value: string }).value, /vch=1000/);
    assert.match((result as { value: string }).value, /auth=success/);
  });

  test("/tuner0/channelmap returns an empty string (PrismCast has no RF tuning band)", () => {

    assert.deepEqual(resolveGet("/tuner0/channelmap", ctx), { kind: "value", value: "" });
  });

  test("/tuner0/filter returns the wide-open default PID range", () => {

    assert.deepEqual(resolveGet("/tuner0/filter", ctx), { kind: "value", value: "0x0000-0x1FFF" });
  });

  test("/tuner0/lockkey reports 'none' (PrismCast has no exclusivity model)", () => {

    assert.deepEqual(resolveGet("/tuner0/lockkey", ctx), { kind: "value", value: "none" });
  });

  test("/tuner0/program returns '0' (single-program MPEG-TS remux)", () => {

    assert.deepEqual(resolveGet("/tuner0/program", ctx), { kind: "value", value: "0" });
  });
});

describe("resolveGet - per-tuner keys for an idle slot", () => {

  const ctx = makeContext([makeIdleSlot(0)]);

  test("/tuner0/channel returns 'none' for an idle slot", () => {

    assert.deepEqual(resolveGet("/tuner0/channel", ctx), { kind: "value", value: "none" });
  });

  test("/tuner0/status reports the no-lock baseline for an idle slot", () => {

    const result = resolveGet("/tuner0/status", ctx);

    assert.equal(result.kind, "value");
    assert.match((result as { value: string }).value, /ch=none/);
    assert.match((result as { value: string }).value, /lock=none/);
  });

  test("/tuner0/streaminfo returns 'none' for an idle slot", () => {

    assert.deepEqual(resolveGet("/tuner0/streaminfo", ctx), { kind: "value", value: "none" });
  });

  test("/tuner0/vchannel returns 'none' for an idle slot", () => {

    assert.deepEqual(resolveGet("/tuner0/vchannel", ctx), { kind: "value", value: "none" });
  });
});

describe("resolveGet - error cases", () => {

  test("unknown system key returns the canonical 'ERROR: unknown getset variable' string", () => {

    assert.deepEqual(resolveGet("/sys/unknown-key", makeContext()), { error: "ERROR: unknown getset variable", kind: "error" });
  });

  test("unknown per-tuner sub-key returns the canonical error string", () => {

    const ctx = makeContext([makeIdleSlot(0)]);

    assert.deepEqual(resolveGet("/tuner0/unknown-sub-key", ctx), { error: "ERROR: unknown getset variable", kind: "error" });
  });

  test("out-of-range tuner index returns 'ERROR: unknown tuner'", () => {

    // ctx has one slot (index 0); /tuner1/* is out of range.
    const ctx = makeContext([makeIdleSlot(0)]);

    assert.deepEqual(resolveGet("/tuner1/channel", ctx), { error: "ERROR: unknown tuner", kind: "error" });
  });

  test("entirely unknown path returns the canonical error string", () => {

    assert.deepEqual(resolveGet("/totally/unknown/path", makeContext()), { error: "ERROR: unknown getset variable", kind: "error" });
  });
});
