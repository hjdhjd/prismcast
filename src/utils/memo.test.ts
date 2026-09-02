/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * memo.test.ts: Unit tests for the memoizeAsync primitive in memo.ts. The primitive is small but its correctness has subtle implications for production
 * caching (resolveFFmpegPath in particular). Tests assert every documented behavior: single-shot probe invocation across concurrent first-callers, cached-value
 * return on subsequent calls, sticky-rejection semantics, and correct handling of probes whose resolved value is itself undefined.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { memoizeAsync } from "./memo.ts";

describe("memoizeAsync", () => {

  test("invokes the probe exactly once on the first call and caches the result", async () => {

    let calls = 0;
    const memoized = memoizeAsync(async () => {

      calls++;

      return "value";
    });

    const result = await memoized();

    assert.equal(result, "value");
    assert.equal(calls, 1, "probe was invoked once");
  });

  test("returns the cached value on subsequent calls without re-probing", async () => {

    let calls = 0;
    const memoized = memoizeAsync(async () => {

      calls++;

      return "value";
    });

    await memoized();
    await memoized();
    await memoized();

    assert.equal(calls, 1, "probe was invoked exactly once across three calls");
  });

  test("dedupes concurrent first-callers via the in-flight promise", async () => {

    /* The rule this dedupes: if N callers arrive before the first probe resolves, all N share the single in-flight promise and the probe is invoked exactly
     * once. This is what prevents a "first-call thundering herd" when the cached resolver is awaited from many places at startup.
     *
     * The probe holds open until we explicitly resolve it. Eight callers enter before that happens. The probe must run exactly once.
     */
    let calls = 0;
    let resolveProbe!: (value: string) => void;

    const probePromise = new Promise<string>((resolve) => {

      resolveProbe = resolve;
    });

    const memoized = memoizeAsync(async () => {

      calls++;

      return probePromise;
    });

    const callerPromises = Array.from({ length: 8 }, async () => memoized());

    // All eight callers are pending. Resolve the probe.
    resolveProbe("shared-result");

    const results = await Promise.all(callerPromises);

    assert.equal(calls, 1, "probe was invoked exactly once across eight concurrent first-callers");
    assert.deepEqual(results, Array.from({ length: 8 }, () => "shared-result"), "all callers received the same value");
  });

  test("caches a probe value of undefined (does not conflate 'not yet resolved' with 'resolved to undefined')", async () => {

    /* Without the "resolved" kind tag, the closure would have to use `state.value !== undefined` as the cache-hit check, which would re-probe whenever the
     * resolved value is undefined. The discriminated union's kind tag distinguishes "resolved to undefined" from "not yet resolved", eliminating that
     * conflation. This test asserts the contract.
     */
    let calls = 0;
    const memoized = memoizeAsync(async () => {

      calls++;

      return undefined;
    });

    // The probe resolves to undefined; we only need to verify the caching rule (probe runs exactly once across multiple awaits) - the resolved value itself
    // is the test's premise, not its assertion.
    await memoized();
    await memoized();

    assert.equal(calls, 1, "probe was invoked exactly once even though the resolved value is undefined");
  });

  test("preserves probe rejection on the first call as the surfaced error", async () => {

    const memoized = memoizeAsync(async () => {

      throw new Error("probe failed");
    });

    await assert.rejects(memoized, /probe failed/);
  });

  test("makes probe rejection sticky: subsequent calls receive the same rejection without re-probing", async () => {

    /* Sticky-rejection is the documented contract. If a caller wants retry-after-failure, they compose memoizeAsync with a separate retry helper. Here we
     * verify both halves: the same rejection surfaces, and probe is not re-invoked.
     */
    let calls = 0;
    const memoized = memoizeAsync(async () => {

      calls++;

      throw new Error("probe failed");
    });

    await assert.rejects(memoized, /probe failed/);
    await assert.rejects(memoized, /probe failed/);
    await assert.rejects(memoized, /probe failed/);

    assert.equal(calls, 1, "probe was invoked exactly once across three rejecting calls");
  });

  test("dedupes concurrent first-callers even when the probe rejects", async () => {

    // Eight concurrent callers, one rejecting probe. All eight callers receive the same rejection; probe runs exactly once.
    let calls = 0;
    let rejectProbe!: (error: Error) => void;

    const probePromise = new Promise<string>((_resolve, reject) => {

      rejectProbe = reject;
    });

    const memoized = memoizeAsync(async () => {

      calls++;

      return probePromise;
    });

    const callerPromises = Array.from({ length: 8 }, async () => memoized());

    // Reject the probe. All eight pending callers should now reject with the same error.
    rejectProbe(new Error("shared rejection"));

    const settled = await Promise.allSettled(callerPromises);

    assert.equal(calls, 1, "probe was invoked exactly once across eight concurrent first-rejecters");

    for(const result of settled) {

      // assert.equal narrows the PromiseSettledResult discriminated union via its assertion-function signature, so result.reason is reachable directly without
      // a cast or an extra type guard.
      assert.equal(result.status, "rejected");
      assert.match(String(result.reason), /shared rejection/);
    }
  });

  test("returns a fresh memoized instance on each call to memoizeAsync (no shared state across instances)", async () => {

    // Two independent memoized resolvers must not share cache. Each invokes its own probe exactly once.
    let aCalls = 0;
    let bCalls = 0;

    const a = memoizeAsync(async (): Promise<string> => {

      aCalls++;

      return "A";
    });
    const b = memoizeAsync(async (): Promise<string> => {

      bCalls++;

      return "B";
    });

    assert.equal(await a(), "A");
    assert.equal(await b(), "B");
    assert.equal(await a(), "A");
    assert.equal(await b(), "B");

    assert.equal(aCalls, 1, "memoized A invoked its own probe once");
    assert.equal(bCalls, 1, "memoized B invoked its own probe once");
  });

  test("preserves the probe's resolved type (smoke check on type-erased boundary)", async () => {

    // The generic carries through. Numeric, object, array - all preserved.
    const num = memoizeAsync(async () => 42);
    const obj = memoizeAsync(async () => ({ id: 1 }));
    const arr = memoizeAsync(async () => [ "a", "b" ]);

    assert.equal(await num(), 42);
    assert.deepEqual(await obj(), { id: 1 });
    assert.deepEqual(await arr(), [ "a", "b" ]);
  });
});
