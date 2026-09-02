/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * pretuneTimers.test.ts: Unit tests for the pretune safety-timer registry. This is the leaf module that owns the per-stream reaper timers; it has no browser or CDP
 * dependency, so the cancel-a-real-pending-timer property - dropping a claimed pretuned stream's safety timer so it does not linger and
 * fire against an already-gone stream - is asserted directly here with node:test fake timers. A regression that removed the clearTimeout from clearPretuneSafetyTimer
 * (reintroducing that exact leak) would fail the first test below.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { clearAllPretuneSafetyTimers, clearPretuneSafetyTimer, forgetPretuneSafetyTimer, setPretuneSafetyTimer } from "./pretuneTimers.ts";
import assert from "node:assert/strict";

describe("pretuneTimers", () => {

  beforeEach(() => {

    mock.timers.enable({ apis: ["setTimeout"] });
  });

  afterEach(() => {

    // Cancel any timers the test registered and restore real timers so no reaper survives into the next test.
    clearAllPretuneSafetyTimers();
    mock.timers.reset();
  });

  test("clearPretuneSafetyTimer cancels a registered timer so its reaper never fires", () => {

    let fired = false;
    const timer = setTimeout(() => { fired = true; }, 90000);

    setPretuneSafetyTimer(7, timer);
    clearPretuneSafetyTimer(7);

    // Advance well past the original delay; the cancelled reaper must not run.
    mock.timers.tick(120000);

    assert.equal(fired, false, "the cancelled reaper does not fire after its delay elapses");

    // A second clear for the same stream is a harmless no-op.
    assert.doesNotThrow(() => { clearPretuneSafetyTimer(7); });
  });

  test("clearPretuneSafetyTimer is a no-op for a stream that never registered a timer", () => {

    assert.doesNotThrow(() => { clearPretuneSafetyTimer(999999); });
  });

  test("a fired reaper forgets its own entry, so a later clear is a harmless no-op", () => {

    let fired = false;
    const timer = setTimeout(() => {

      fired = true;
      forgetPretuneSafetyTimer(4);
    }, 30000);

    setPretuneSafetyTimer(4, timer);
    mock.timers.tick(40000);

    assert.equal(fired, true, "the reaper fired and ran its own cleanup");

    // The fired timer already dropped its bookkeeping entry; a defensive clear afterward must not double-cancel or throw.
    assert.doesNotThrow(() => { clearPretuneSafetyTimer(4); });
  });

  test("clearAllPretuneSafetyTimers cancels every registered reaper", () => {

    let firedA = false;
    let firedB = false;

    setPretuneSafetyTimer(1, setTimeout(() => { firedA = true; }, 50000));
    setPretuneSafetyTimer(2, setTimeout(() => { firedB = true; }, 60000));

    clearAllPretuneSafetyTimers();
    mock.timers.tick(120000);

    assert.equal(firedA, false, "the first reaper was cancelled");
    assert.equal(firedB, false, "the second reaper was cancelled");
  });

  test("re-registering a stream's safety timer cancels the prior reaper so the registry holds at most one live timer", () => {

    let firedOld = false;
    let firedNew = false;

    setPretuneSafetyTimer(5, setTimeout(() => { firedOld = true; }, 30000));

    // Re-registering for the same stream cancels the prior reaper before tracking the new one, so no stale handle survives in the registry.
    setPretuneSafetyTimer(5, setTimeout(() => { firedNew = true; }, 45000));
    clearPretuneSafetyTimer(5);
    mock.timers.tick(120000);

    assert.equal(firedOld, false, "the prior reaper was cancelled when a new one was registered for the same stream");
    assert.equal(firedNew, false, "the current reaper was cancelled by clear");
  });
});
