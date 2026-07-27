/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * streamContext.test.ts: Unit tests for the AsyncLocalStorage-based stream context in streamContext.ts. The exports work together: runWithStreamContext
 * establishes a context via ALS.run(), and the accessors read from the current store. Tests verify happy-path propagation, scope clearing with no leakage
 * on either the resolve or reject path, nested-context shadowing, parallel-run isolation, and the lazy showNameResolver pattern.
 */
import { describe, test } from "node:test";
import { getStreamContext, getStreamId, resolveContextShowName, runWithStreamContext } from "./streamContext.ts";
import type { StreamContext } from "./streamContext.ts";
import assert from "node:assert/strict";

describe("getStreamContext outside of any context", () => {

  test("returns undefined when no context is active", () => {

    // Boundary: calling the accessor outside ALS.run() must return undefined, not throw.
    assert.equal(getStreamContext(), undefined);
  });
});

describe("getStreamId outside of any context", () => {

  test("returns undefined when no context is active", () => {

    assert.equal(getStreamId(), undefined);
  });
});

describe("resolveContextShowName outside of any context", () => {

  test("returns the empty string when no context is active", () => {

    // Negative test: the helper guards against undefined store and returns "" rather than crashing.
    assert.equal(resolveContextShowName(), "");
  });
});

describe("runWithStreamContext", () => {

  test("makes streamId available inside the callback via getStreamId", async () => {

    let observed: string | undefined;

    await runWithStreamContext({ streamId: "test-123" }, async () => {

      observed = getStreamId();
    });

    assert.equal(observed, "test-123");
  });

  test("makes the full context available via getStreamContext (preserves all fields)", async () => {

    let observed: StreamContext | undefined;

    await runWithStreamContext({ channelName: "NBC", streamId: "nbc-abc", url: "https://example.test/nbc" }, async () => {

      observed = getStreamContext();
    });

    const ctx = observed!;

    assert.equal(ctx.streamId, "nbc-abc");
    assert.equal(ctx.channelName, "NBC");
    assert.equal(ctx.url, "https://example.test/nbc");
  });

  test("returns the callback's resolved value", async () => {

    const result = await runWithStreamContext({ streamId: "x" }, async () => "result-value");

    assert.equal(result, "result-value");
  });

  test("propagates the callback's rejection", async () => {

    // Negative test: errors from inside the context must bubble out of runWithStreamContext, not be swallowed.
    await assert.rejects(
      () => runWithStreamContext({ streamId: "x" }, async () => {

        throw new Error("inner failure");
      }),
      /inner failure/
    );
  });

  test("clears the context after the callback resolves (no leakage to outer scope)", async () => {

    await runWithStreamContext({ streamId: "scoped" }, async () => {

      assert.equal(getStreamId(), "scoped");
    });

    // After the run completes, the outer scope must not see the inner streamId.
    assert.equal(getStreamId(), undefined, "outer scope sees no context after the run resolved");
  });

  test("clears the context after the callback rejects (no leakage on error path)", async () => {

    try {

      await runWithStreamContext({ streamId: "scoped" }, async () => {

        throw new Error("boom");
      });
    } catch {

      // Swallow - we only care about the leak check below.
    }

    assert.equal(getStreamId(), undefined, "outer scope sees no context even after rejection");
  });

  test("propagates context through nested async/await chains", async () => {

    // The whole point of AsyncLocalStorage. Chained async calls should all see the same streamId.
    async function inner(): Promise<string | undefined> {

      await Promise.resolve();
      await Promise.resolve();

      return getStreamId();
    }

    let observed: string | undefined;

    await runWithStreamContext({ streamId: "deep" }, async () => {

      observed = await inner();
    });

    assert.equal(observed, "deep");
  });

  test("nested runWithStreamContext shadows the parent context inside the inner callback", async () => {

    let outerInside: string | undefined;
    let innerInside: string | undefined;
    let outerAfter: string | undefined;

    await runWithStreamContext({ streamId: "outer" }, async () => {

      outerInside = getStreamId();

      await runWithStreamContext({ streamId: "inner" }, async () => {

        innerInside = getStreamId();
      });

      outerAfter = getStreamId();
    });

    assert.equal(outerInside, "outer");
    assert.equal(innerInside, "inner", "inner context shadows the outer streamId");
    assert.equal(outerAfter, "outer", "outer context restored after inner returns");
  });

  test("two parallel runs maintain independent contexts (no cross-talk)", async () => {

    // Negative test: ALS isolation between parallel async tasks. The two runs must not see each other's streamId.
    const observations: string[] = [];

    await Promise.all([

      runWithStreamContext({ streamId: "a" }, async () => {

        await Promise.resolve();
        observations.push("a-saw-" + (getStreamId() ?? "none"));
      }),
      runWithStreamContext({ streamId: "b" }, async () => {

        await Promise.resolve();
        observations.push("b-saw-" + (getStreamId() ?? "none"));
      })
    ]);

    assert.ok(observations.includes("a-saw-a"), "context A saw streamId a");
    assert.ok(observations.includes("b-saw-b"), "context B saw streamId b");
  });
});

describe("resolveContextShowName", () => {

  test("returns the empty string when no resolver is set in the context", async () => {

    // Boundary: the resolver field is optional. When absent, the helper returns "" rather than calling undefined() and throwing.
    let observed: string | undefined;

    await runWithStreamContext({ streamId: "no-resolver" }, async () => {

      observed = resolveContextShowName();
    });

    assert.equal(observed, "");
  });

  test("invokes the resolver and returns its current value", async () => {

    let observed: string | undefined;

    await runWithStreamContext({ showNameResolver: () => "Today Show", streamId: "with-resolver" }, async () => {

      observed = resolveContextShowName();
    });

    assert.equal(observed, "Today Show");
  });

  test("re-invokes the resolver on each call (lazy reads stay current)", async () => {

    // The resolver pattern is "lazy" specifically so the value reflects the current program even as shows change during long-running streams.
    let counter = 0;
    const resolver = (): string => "show-" + String(++counter);

    let first: string | undefined;
    let second: string | undefined;
    let third: string | undefined;

    await runWithStreamContext({ showNameResolver: resolver, streamId: "lazy" }, async () => {

      first = resolveContextShowName();
      second = resolveContextShowName();
      third = resolveContextShowName();
    });

    assert.equal(first, "show-1");
    assert.equal(second, "show-2");
    assert.equal(third, "show-3");
  });

  test("returns the resolver's empty-string output verbatim (not the no-resolver fallback)", async () => {

    // Negative test: when the resolver returns "" intentionally, the helper must return that "" - not coerce to anything else, and must distinguish it from the
    // no-resolver-set case (which also returns "" but via a different code path).
    let observed: string | undefined;

    await runWithStreamContext({ showNameResolver: () => "", streamId: "empty-resolver" }, async () => {

      observed = resolveContextShowName();
    });

    assert.equal(observed, "", "empty-string resolver result is preserved");
  });
});
