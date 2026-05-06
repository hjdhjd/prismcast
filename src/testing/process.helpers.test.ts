/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * process.helpers.test.ts: Tests for assertNoUnhandledRejections and expectAt. The first wires a per-test handler against an injected emitter (so synthetic
 * events don't conflict with the test runner); the second polls a microtask-yielding predicate within a bounded iteration budget. Coverage pins the rejection
 * accumulation, listener detachment, deterministic re-throw, and the off-by-one boundaries on the polling budget.
 */
import { assertNoUnhandledRejections, expectAt } from "./process.helpers.ts";
import { describe, test } from "node:test";
import { EventEmitter } from "node:events";
import assert from "node:assert/strict";

describe("assertNoUnhandledRejections", () => {

  /* All tests pass a fresh EventEmitter rather than the default process. Real rejections on process would be picked up by Node's test runner and fail the test
   * before the helper sees them; the injected emitter keeps the helper's listener wiring identical while letting tests dispatch synthetic events safely.
   */

  test("cleanup function returns silently when no rejections occurred", () => {

    const emitter = new EventEmitter();
    const cleanup = assertNoUnhandledRejections(emitter);

    assert.doesNotThrow(cleanup, "cleanup should not throw when no rejections were captured");
  });

  test("cleanup function throws when an unhandled rejection was captured", () => {

    const emitter = new EventEmitter();
    const cleanup = assertNoUnhandledRejections(emitter);

    emitter.emit("unhandledRejection", new Error("oops"), Promise.resolve());

    assert.throws(cleanup, /Unhandled rejection during test/, "cleanup should report the captured rejection");
  });

  test("the thrown error surfaces every rejection reason via AggregateError.errors", () => {

    const emitter = new EventEmitter();
    const cleanup = assertNoUnhandledRejections(emitter);
    const reason = new Error("origin");

    emitter.emit("unhandledRejection", reason, Promise.resolve());

    try {

      cleanup();
      assert.fail("cleanup should have thrown");
    } catch(err) {

      assert.ok(err instanceof AggregateError, "thrown value should be an AggregateError");
      assert.deepEqual(err.errors, [reason], "AggregateError.errors should contain every captured rejection reason");
    }
  });

  test("reports every captured reason via AggregateError.errors when multiple rejections occurred", () => {

    const emitter = new EventEmitter();
    const cleanup = assertNoUnhandledRejections(emitter);
    const first = new Error("first");
    const second = new Error("second");
    const third = new Error("third");

    emitter.emit("unhandledRejection", first, Promise.resolve());
    emitter.emit("unhandledRejection", second, Promise.resolve());
    emitter.emit("unhandledRejection", third, Promise.resolve());

    try {

      cleanup();
      assert.fail("cleanup should have thrown");
    } catch(err) {

      assert.ok(err instanceof AggregateError, "thrown value should be an AggregateError");
      assert.match(err.message, /3 total/, "message should mention all three captured rejections");
      assert.deepEqual(err.errors, [ first, second, third ], "errors should preserve every captured reason in capture order");
    }
  });

  test("does NOT throw when the cleanup is invoked before any rejection drains", () => {

    // Running cleanup immediately after registration captures no rejections. This is the common path - tests that produce no rejections call cleanup at the end
    // and it must be a no-op.
    const emitter = new EventEmitter();

    assert.doesNotThrow(assertNoUnhandledRejections(emitter), "synchronous cleanup with nothing in flight should be silent");
  });

  test("detaches its listener so subsequent rejections are not captured", () => {

    const emitter = new EventEmitter();
    const cleanup = assertNoUnhandledRejections(emitter);

    cleanup();

    // Post-cleanup, the helper's listener is gone; a fresh emit has nothing to capture. We verify by re-running the helper on the same emitter and confirming
    // only the new helper's listener observes the event - the old listener does not double-record.
    const cleanup2 = assertNoUnhandledRejections(emitter);

    emitter.emit("unhandledRejection", new Error("after-cleanup"), Promise.resolve());

    assert.throws(cleanup2, /1 total/, "only the second helper should have captured the post-cleanup event");
  });

  test("re-invoking cleanup after a clean first call is a safe no-op", () => {

    // Boundary: tests that wrap cleanup in t.after may run it more than once if the test framework or user code double-registers. The second call must not
    // throw - the listener was already detached by the first call, the captured list is still empty, so the contract holds.
    const emitter = new EventEmitter();
    const cleanup = assertNoUnhandledRejections(emitter);

    cleanup();

    assert.doesNotThrow(cleanup, "second cleanup invocation must be a silent no-op");
  });

  test("re-invoking cleanup after a throwing first call still throws on the second call", () => {

    // Boundary: the captured-rejections list is closure-state; cleanup does not clear it. So if the first call threw, the second call sees the same captured
    // entries and throws again. This locks the deterministic re-throw contract - operators get the same error every time, not an inconsistent first-throw-
    // then-silent surprise.
    const emitter = new EventEmitter();
    const cleanup = assertNoUnhandledRejections(emitter);

    emitter.emit("unhandledRejection", new Error("captured"), Promise.resolve());

    assert.throws(cleanup, /Unhandled rejection during test/, "first cleanup invocation throws");
    assert.throws(cleanup, /Unhandled rejection during test/, "second cleanup invocation throws the same way");
  });
});

describe("expectAt", () => {

  test("returns the value when the predicate yields on the first attempt", async () => {

    const result = await expectAt(() => 42);

    assert.equal(result, 42, "predicate yielding immediately should resolve to its value");
  });

  test("returns the value once the predicate yields after several microtasks", async () => {

    let attempts = 0;

    const result = await expectAt(() => {

      attempts++;

      return (attempts >= 5) ? "ready" : undefined;
    });

    assert.equal(result, "ready", "predicate yielding after retries should still resolve");
    assert.equal(attempts, 5, "predicate should have been polled exactly until it yielded");
  });

  test("yields a falsy non-undefined value (0, null, false, '') as a successful result", async () => {

    // The contract is "non-undefined", not "truthy" - a predicate that returns 0 or null or false or empty string has succeeded, and the helper must not keep
    // polling. This is the classic falsy-non-undefined boundary that bites lazy implementations.
    const zero = await expectAt(() => 0);
    const nul = await expectAt<null>(() => null);
    const fls = await expectAt(() => false);
    const empty = await expectAt(() => "");

    assert.equal(zero, 0, "0 should be returned");
    assert.equal(nul, null, "null should be returned");
    assert.equal(fls, false, "false should be returned");
    assert.equal(empty, "", "empty string should be returned");
  });

  test("throws when the predicate never yields within the default budget", async () => {

    await assert.rejects(

      () => expectAt(() => undefined),
      /predicate did not yield/,
      "exhausted budget should reject with a descriptive message"
    );
  });

  test("throws when the predicate never yields within a custom budget", async () => {

    await assert.rejects(

      () => expectAt(() => undefined, { iterations: 5 }),
      /within 5 microtask iterations/,
      "the message should reflect the configured iteration count"
    );
  });

  test("zero iterations throws even if the predicate would yield immediately", async () => {

    // iterations: 0 means "never poll" - the loop body does not execute, the helper goes straight to the timeout error. This is the off-by-one boundary.
    await assert.rejects(

      () => expectAt(() => "would-yield", { iterations: 0 }),
      /within 0 microtask iterations/,
      "zero budget should never call the predicate"
    );
  });

  test("negative iterations behave the same as zero (loop never enters, predicate never called)", async () => {

    // Boundary: iterations < 0 is nonsensical input but the code should fail predictably rather than panic or loop forever. The for(let i = 0; i < max; i++)
    // form short-circuits cleanly when max is negative because the entry condition is false on the first check, so we land on the same throw path as
    // iterations: 0.
    let calls = 0;

    await assert.rejects(

      () => expectAt(() => {

        calls++;

        return "never-reached";
      }, { iterations: -1 }),
      /within -1 microtask iterations/,
      "negative budget should never call the predicate"
    );

    assert.equal(calls, 0, "predicate must not be invoked when iterations is negative");
  });

  test("respects a budget of exactly one iteration", async () => {

    // With iterations: 1 the predicate gets exactly one chance. If it yields, return; if it returns undefined, throw.
    let calls = 0;

    const result = await expectAt(() => {

      calls++;

      return "ok";
    }, { iterations: 1 });

    assert.equal(result, "ok", "single-attempt budget should still allow a successful predicate");
    assert.equal(calls, 1, "predicate should be called exactly once");
  });
});
