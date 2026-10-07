/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * reactivity.test.ts: Unit tests for the config-change reactivity primitive. The module's observable contracts include:
 *
 *   1. computeConfigDiff produces one ConfigChange per leaf-value difference between two snapshots, in deterministic order.
 *
 *   2. registerConfigChangeHandler is single-shot per prefix (duplicate registration throws).
 *
 *   3. partitionConfigChanges splits a diff by the class an injected classifier gives each path, keeping the diff's order within each list, and its exhaustive
 *      switch refuses a class outside the union.
 *
 *   4. applyConfigChanges hands the live and next-stream changes to the handler of their longest matching prefix together with the candidate running
 *      configuration, realizes every change no handler refused (a change with no handler among them), rejects each refused change with its reason, rejects a
 *      throwing handler's bucket and no other, ignores a rejection for a path the handler was not given, and never hands a held change to a handler.
 *
 * Extend this enumeration alongside any new exported behavior the module gains.
 *
 * The tests below cover each contract directly without leaning on integration plumbing - the primitive's correctness is mechanical, so the tests are also. The
 * candidate configuration the dispatch rows pass is DEFAULTS cloned, because the primitive only forwards it.
 */
import type { Config, ReactivityClass } from "../types/index.ts";
import type { ConfigChange, ConfigChangeHandler, ConfigChangePartition, ReactivityClassifier } from "./reactivity.ts";
import { afterEach, beforeEach, describe, test } from "node:test";
import { applyConfigChanges, computeConfigDiff, partitionConfigChanges, registerConfigChangeHandler, resetConfigChangeHandlers } from "./reactivity.ts";
import { DEFAULTS } from "./userConfig.ts";
import assert from "node:assert/strict";

// The candidate running configuration the dispatch rows hand the primitive. Its contents are opaque to the primitive, which forwards it to each handler.
const NEXT: Config = structuredClone(DEFAULTS);

/**
 * Builds a partition of live changes alone, the shape most dispatch rows need.
 * @param live - The live changes.
 * @returns A partition holding only those live changes.
 */
function livePartition(live: readonly ConfigChange[]): ConfigChangePartition {

  return { held: [], live, nextStream: [] };
}

describe("computeConfigDiff", () => {

  test("returns no changes for deeply-equal snapshots", () => {

    const snapshot = { hdhr: { enabled: true, port: 5004 }, server: { host: "0.0.0.0", port: 5589 } };

    // structuredClone guarantees a separate identity so any false positive in reference comparison would surface.
    assert.deepEqual(computeConfigDiff(snapshot, structuredClone(snapshot)), []);
  });

  test("emits one change per scalar leaf that differs", () => {

    const previous = { hdhr: { enabled: false, port: 5004 } };
    const current = { hdhr: { enabled: true, port: 5005 } };

    assert.deepEqual(computeConfigDiff(previous, current), [
      { current: true, path: "hdhr.enabled", previous: false },
      { current: 5005, path: "hdhr.port", previous: 5004 }
    ]);
  });

  test("treats arrays as opaque leaves (no per-element recursion)", () => {

    const previous = { streaming: { captureCodecs: [ "h264", "vp9" ] } };
    const current = { streaming: { captureCodecs: ["h264"] } };

    // The array is compared whole, so the change is one entry at the array's own path rather than an element-level entry for the dropped "vp9".
    assert.deepEqual(computeConfigDiff(previous, current), [
      { current: ["h264"], path: "streaming.captureCodecs", previous: [ "h264", "vp9" ] }
    ]);
  });

  test("captures additions (previous undefined) and removals (current undefined)", () => {

    const previous = { hdhr: { enabled: true } };
    const current = { hdhr: { discoveryEnabled: true, enabled: true } };

    assert.deepEqual(computeConfigDiff(previous, current), [
      { current: true, path: "hdhr.discoveryEnabled", previous: undefined }
    ]);

    // Reversing the inputs flips the addition to a removal.
    assert.deepEqual(computeConfigDiff(current, previous), [
      { current: undefined, path: "hdhr.discoveryEnabled", previous: true }
    ]);
  });

  test("emits changes in alphabetical path order regardless of input key order", () => {

    // Intentionally non-alphabetical input to prove the walker sorts on output. The eslint suppressions document that the disorder is the test's whole point.
    /* eslint-disable sort-keys */
    const previous = { z: 1, a: { c: 1, b: 1 } };
    const current = { z: 2, a: { c: 2, b: 2 } };
    /* eslint-enable sort-keys */
    const diff = computeConfigDiff(previous, current);

    assert.deepEqual(diff.map((c) => c.path), [ "a.b", "a.c", "z" ]);
  });
});

describe("registerConfigChangeHandler", () => {

  beforeEach(() => {

    resetConfigChangeHandlers();
  });

  afterEach(() => {

    resetConfigChangeHandlers();
  });

  test("accepts a single handler per prefix", () => {

    assert.doesNotThrow(() => { registerConfigChangeHandler("hdhr.", async () => []); });
  });

  test("throws when a second handler is registered for the same prefix", () => {

    registerConfigChangeHandler("hdhr.", async () => []);

    assert.throws(() => { registerConfigChangeHandler("hdhr.", async () => []); }, /already registered/);
  });

  test("permits distinct prefixes that overlap as parent and child", () => {

    // A coarse "server." handler and a finer "server.advanced." handler can both exist if a subsystem ever needs nested routing. Longest-match semantics in
    // applyConfigChanges ensures children go to the more-specific handler.
    assert.doesNotThrow(() => {

      registerConfigChangeHandler("server.", async () => []);
      registerConfigChangeHandler("server.advanced.", async () => []);
    });
  });
});

describe("partitionConfigChanges", () => {

  // The synthetic classifier: the path's first segment names its class, so a row states each change's class in its path.
  const classify: ReactivityClassifier = (path) => path.split(".")[0] as ReactivityClass;

  test("splits a diff into held, live, and next-stream changes by the class the classifier gives each path, keeping the diff's order within each", () => {

    const diff = [
      { current: 1, path: "live.b", previous: 0 },
      { current: 1, path: "restart.a", previous: 0 },
      { current: 1, path: "next-stream.a", previous: 0 },
      { current: 1, path: "live.a", previous: 0 },
      { current: 1, path: "restart.b", previous: 0 }
    ];

    const partition = partitionConfigChanges(diff, classify);

    assert.deepEqual(partition.held.map((c) => c.path), [ "restart.a", "restart.b" ]);
    assert.deepEqual(partition.live.map((c) => c.path), [ "live.b", "live.a" ]);
    assert.deepEqual(partition.nextStream.map((c) => c.path), ["next-stream.a"]);
  });

  test("an empty diff partitions into empty lists", () => {

    assert.deepEqual(partitionConfigChanges([], classify), { held: [], live: [], nextStream: [] });
  });

  test("a class outside the union reaches the exhaustive switch's guard and throws rather than landing in any list", () => {

    assert.throws(() => partitionConfigChanges([{ current: 1, path: "bogus.a", previous: 0 }], classify), /Unhandled value: "bogus"/);
  });
});

describe("applyConfigChanges", () => {

  beforeEach(() => {

    resetConfigChangeHandlers();
  });

  afterEach(() => {

    resetConfigChangeHandlers();
  });

  test("returns empty lists when the partition has nothing to dispatch", async () => {

    assert.deepEqual(await applyConfigChanges({ held: [], live: [], nextStream: [] }, NEXT), { realized: [], rejected: [] });
  });

  test("realizes a live change with no handler registered for its path", async () => {

    const change = { current: 0.2, path: "playback.stallThreshold", previous: 0.1 };

    assert.deepEqual(await applyConfigChanges(livePartition([change]), NEXT), { realized: [change], rejected: [] });
  });

  test("hands the handler its changes and the candidate running configuration, and realizes what it does not refuse", async () => {

    const change = { current: true, path: "hdhr.enabled", previous: false };
    let received: readonly ConfigChange[] = [];
    let receivedNext: Readonly<Config> | undefined;

    registerConfigChangeHandler("hdhr.", async (changes, next) => {

      received = changes;
      receivedNext = next;

      return [];
    });

    const result = await applyConfigChanges(livePartition([change]), NEXT);

    assert.deepEqual(received, [change]);
    assert.equal(receivedNext, NEXT, "the handler receives the candidate the caller passed, by reference");
    assert.deepEqual(result, { realized: [change], rejected: [] });
  });

  test("a handler refusing one path of its batch rejects that path alone, with the handler's reason", async () => {

    registerConfigChangeHandler("hdhr.", async () => [{ path: "hdhr.port", reason: "HDHomeRun could not bind port 5005, so the change was not applied." }]);

    const enabled = { current: true, path: "hdhr.enabled", previous: false };
    const port = { current: 5005, path: "hdhr.port", previous: 5004 };
    const result = await applyConfigChanges(livePartition([ enabled, port ]), NEXT);

    assert.deepEqual(result.realized, [enabled]);
    assert.deepEqual(result.rejected, [{ change: port, reason: "HDHomeRun could not bind port 5005, so the change was not applied." }]);
  });

  test("batches multiple changes that share a prefix into a single handler invocation", async () => {

    let invocations = 0;
    let receivedPaths: string[] = [];

    registerConfigChangeHandler("hdhr.", async (changes) => {

      invocations += 1;
      receivedPaths = changes.map((c) => c.path);

      return [];
    });

    const result = await applyConfigChanges(livePartition([
      { current: true, path: "hdhr.discoveryEnabled", previous: false },
      { current: true, path: "hdhr.enabled", previous: false },
      { current: 5005, path: "hdhr.port", previous: 5004 }
    ]), NEXT);

    assert.equal(invocations, 1, "handler is invoked once for the batch");
    assert.deepEqual(receivedPaths, [ "hdhr.discoveryEnabled", "hdhr.enabled", "hdhr.port" ]);
    assert.equal(result.realized.length, 3);
  });

  test("routes to the longest matching prefix, and a sibling prefix's handler receives nothing", async () => {

    const received = new Map<string, string[]>();

    function record(prefix: string): ConfigChangeHandler {

      return async (changes) => {

        received.set(prefix, changes.map((c) => c.path));

        return [];
      };
    }

    registerConfigChangeHandler("a.", record("a."));
    registerConfigChangeHandler("a.b.", record("a.b."));
    registerConfigChangeHandler("c.", record("c."));

    await applyConfigChanges(livePartition([ { current: 1, path: "a.b.c", previous: 0 }, { current: 1, path: "a.x", previous: 0 } ]), NEXT);

    assert.deepEqual(received.get("a.b."), ["a.b.c"], "a.b.c routes to the longer prefix");
    assert.deepEqual(received.get("a."), ["a.x"], "a.x routes to the shorter prefix");
    assert.equal(received.has("c."), false, "a handler whose prefix matches no change is never called");
  });

  test("a thrown handler rejects every change in its bucket, with a sentence naming the prefix and the error, and no other bucket", async () => {

    registerConfigChangeHandler("a.", async () => { throw new Error("boom"); });
    registerConfigChangeHandler("b.", async () => []);

    const ax = { current: 1, path: "a.x", previous: 0 };
    const ay = { current: 1, path: "a.y", previous: 0 };
    const bz = { current: 1, path: "b.z", previous: 0 };
    const result = await applyConfigChanges(livePartition([ ax, ay, bz ]), NEXT);
    const reason = "The configuration handler for \"a.\" failed: boom.";

    assert.deepEqual(result.realized, [bz], "the other bucket is realized");
    assert.deepEqual(result.rejected, [ { change: ax, reason }, { change: ay, reason } ]);
  });

  test("ignores a rejection for a path outside the handler's batch, so it cannot refuse another handler's change", async () => {

    registerConfigChangeHandler("a.", async () => []);
    registerConfigChangeHandler("b.", async () => [{ path: "a.x", reason: "A foreign rejection that must be ignored." }]);

    const ax = { current: 1, path: "a.x", previous: 0 };
    const by = { current: 1, path: "b.y", previous: 0 };
    const result = await applyConfigChanges(livePartition([ ax, by ]), NEXT);

    assert.deepEqual(result.realized, [ ax, by ]);
    assert.equal(result.rejected.length, 0, "the foreign rejection for a.x was ignored");
  });

  test("dispatches parallel handlers independently", async () => {

    const aGate = Promise.withResolvers<null>();
    const bGate = Promise.withResolvers<null>();

    // Both handlers block until the test releases them. Promise.allSettled in applyConfigChanges holds open until both resolve.
    registerConfigChangeHandler("a.", async () => {

      await aGate.promise;

      return [];
    });
    registerConfigChangeHandler("b.", async () => {

      await bGate.promise;

      return [];
    });

    const dispatch = applyConfigChanges(livePartition([ { current: 1, path: "a.x", previous: 0 }, { current: 1, path: "b.y", previous: 0 } ]), NEXT);

    // Release the handlers in reverse registration order to prove independence: nothing serializes their execution.
    bGate.resolve(null);
    aGate.resolve(null);

    assert.equal((await dispatch).realized.length, 2);
  });

  test("keeps the partition's order, live changes before next-stream changes, whatever order handlers settle in", async () => {

    registerConfigChangeHandler("x.", async () => []);

    const partition: ConfigChangePartition = {

      held: [],
      live: [ { current: 1, path: "x.c", previous: 0 }, { current: 1, path: "x.a", previous: 0 } ],
      nextStream: [{ current: 1, path: "x.b", previous: 0 }]
    };

    assert.deepEqual((await applyConfigChanges(partition, NEXT)).realized.map((c) => c.path), [ "x.c", "x.a", "x.b" ]);
  });

  test("never hands a held change to a handler and never reports it", async () => {

    let received: readonly ConfigChange[] = [];

    registerConfigChangeHandler("server.", async (changes) => {

      received = changes;

      return [];
    });

    const held = { current: 6000, path: "server.port", previous: 5589 };
    const result = await applyConfigChanges({ held: [held], live: [], nextStream: [] }, NEXT);

    assert.deepEqual(received, [], "the handler was not called");
    assert.deepEqual(result, { realized: [], rejected: [] });
  });
});
