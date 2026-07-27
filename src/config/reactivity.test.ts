/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * reactivity.test.ts: Unit tests for the config-change reactivity primitive. The module's observable contracts include:
 *
 *   1. computeConfigDiff produces one ConfigChange per leaf-value difference between two snapshots, in deterministic order.
 *
 *   2. registerConfigChangeHandler is single-shot per prefix (duplicate registration throws).
 *
 *   3. applyConfigChanges matches each changed path to its handler by longest-prefix, dispatches the diff to handlers, partitions the result into
 *      applied/deferred/rejected, falls back to the NO_HANDLER_REASON deferral for unhandled paths, and falls back to MISSING_OUTCOME_REASON when a handler
 *      omits a path from its outcome list.
 *
 * Extend this enumeration alongside any new exported behavior the module gains.
 *
 * The tests below cover each contract directly without leaning on integration plumbing - the primitive's correctness is mechanical, so the tests are also.
 */
import { MISSING_OUTCOME_REASON, NO_HANDLER_REASON, applyConfigChanges, computeConfigDiff, registerConfigChangeHandler,
  resetConfigChangeHandlers } from "./reactivity.ts";
import { afterEach, beforeEach, describe, test } from "node:test";
import type { ChangeOutcome } from "./reactivity.ts";
import assert from "node:assert/strict";

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

    // The array swap is a single change, not three individual deletes.
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

    // Future-proofing: a coarse "server." handler and a finer "server.advanced." handler can both exist if a subsystem ever needs nested routing. Longest-match
    // semantics in applyConfigChanges ensures children go to the more-specific handler.
    assert.doesNotThrow(() => {

      registerConfigChangeHandler("server.", async () => []);
      registerConfigChangeHandler("server.advanced.", async () => []);
    });
  });
});

describe("applyConfigChanges", () => {

  beforeEach(() => {

    resetConfigChangeHandlers();
  });

  afterEach(() => {

    resetConfigChangeHandlers();
  });

  test("returns empty buckets for an empty diff", async () => {

    const result = await applyConfigChanges([]);

    assert.deepEqual(result, { applied: [], deferred: [], rejected: [] });
  });

  test("defers paths with no matching handler using NO_HANDLER_REASON", async () => {

    const change = { current: 5005, path: "browser.port", previous: 5004 };
    const result = await applyConfigChanges([change]);

    assert.equal(result.applied.length, 0);
    assert.equal(result.rejected.length, 0);
    assert.deepEqual(result.deferred, [{ change, reason: NO_HANDLER_REASON }]);
  });

  test("routes a change to the registered handler and records the reported outcome", async () => {

    const change = { current: true, path: "hdhr.enabled", previous: false };
    let received: readonly { path: string }[] = [];

    registerConfigChangeHandler("hdhr.", async (changes) => {

      received = changes;

      return [{ kind: "applied", path: change.path }];
    });

    const result = await applyConfigChanges([change]);

    assert.deepEqual(received.map((c) => c.path), ["hdhr.enabled"]);
    assert.deepEqual(result.applied, [change]);
    assert.equal(result.deferred.length, 0);
    assert.equal(result.rejected.length, 0);
  });

  test("batches multiple changes that share a prefix into a single handler invocation", async () => {

    let invocations = 0;
    let receivedPaths: string[] = [];

    registerConfigChangeHandler("hdhr.", async (changes) => {

      invocations += 1;
      receivedPaths = changes.map((c) => c.path);

      return changes.map((c) => ({ kind: "applied", path: c.path } satisfies ChangeOutcome));
    });

    const diff = [
      { current: true, path: "hdhr.discoveryEnabled", previous: false },
      { current: true, path: "hdhr.enabled", previous: false },
      { current: 5005, path: "hdhr.port", previous: 5004 }
    ];

    const result = await applyConfigChanges(diff);

    assert.equal(invocations, 1, "handler is invoked once for the batch");
    assert.deepEqual(receivedPaths, [ "hdhr.discoveryEnabled", "hdhr.enabled", "hdhr.port" ]);
    assert.equal(result.applied.length, 3);
  });

  test("routes to the longest matching prefix when nested handlers are registered", async () => {

    let coarseInvocations = 0;
    let fineInvocations = 0;

    registerConfigChangeHandler("a.", async (changes) => {

      coarseInvocations += 1;

      return changes.map((c) => ({ kind: "applied", path: c.path } satisfies ChangeOutcome));
    });
    registerConfigChangeHandler("a.b.", async (changes) => {

      fineInvocations += 1;

      return changes.map((c) => ({ kind: "applied", path: c.path } satisfies ChangeOutcome));
    });

    await applyConfigChanges([
      { current: 1, path: "a.b.c", previous: 0 },
      { current: 1, path: "a.x", previous: 0 }
    ]);

    assert.equal(fineInvocations, 1, "a.b.c should route to the longer prefix");
    assert.equal(coarseInvocations, 1, "a.x should route to the shorter prefix");
  });

  test("defers paths with MISSING_OUTCOME_REASON when the handler omits them", async () => {

    registerConfigChangeHandler("hdhr.", async () => []);

    const change = { current: true, path: "hdhr.enabled", previous: false };
    const result = await applyConfigChanges([change]);

    assert.deepEqual(result.deferred, [{ change, reason: MISSING_OUTCOME_REASON }]);
  });

  test("records deferred and rejected outcomes from the handler verbatim", async () => {

    registerConfigChangeHandler("hdhr.", async (changes) => changes.map((c) => {

      // Use the path to choose the outcome shape so the test exercises all three kinds in one dispatch.
      if(c.path === "hdhr.enabled") {

        return { kind: "applied", path: c.path } satisfies ChangeOutcome;
      }

      if(c.path === "hdhr.port") {

        return { kind: "deferred", path: c.path, reason: "port change requires HTTP server restart" } satisfies ChangeOutcome;
      }

      return { kind: "rejected", path: c.path, reason: "FFmpeg unavailable" } satisfies ChangeOutcome;
    }));

    const enabledChange = { current: true, path: "hdhr.enabled", previous: false };
    const portChange = { current: 5005, path: "hdhr.port", previous: 5004 };
    const deviceIdChange = { current: "XXXXXXXX", path: "hdhr.deviceId", previous: "YYYYYYYY" };

    const result = await applyConfigChanges([ enabledChange, portChange, deviceIdChange ]);

    assert.deepEqual(result.applied, [enabledChange]);
    assert.deepEqual(result.deferred, [{ change: portChange, reason: "port change requires HTTP server restart" }]);
    assert.deepEqual(result.rejected, [{ change: deviceIdChange, reason: "FFmpeg unavailable" }]);
  });

  test("preserves input order in applied/deferred/rejected buckets", async () => {

    registerConfigChangeHandler("x.", async (changes) => changes.map((c) => ({ kind: "applied", path: c.path } satisfies ChangeOutcome)));

    const changes = [
      { current: 1, path: "x.c", previous: 0 },
      { current: 1, path: "x.a", previous: 0 },
      { current: 1, path: "x.b", previous: 0 }
    ];

    const result = await applyConfigChanges(changes);

    // The handler may reorder internally, but the aggregate result must iterate in the order the caller supplied.
    assert.deepEqual(result.applied.map((c) => c.path), [ "x.c", "x.a", "x.b" ]);
  });

  test("dispatches parallel handlers independently", async () => {

    let aResolve: (() => void) | undefined;
    let bResolve: (() => void) | undefined;

    // Both handlers block until the test releases them. Promise.allSettled in applyConfigChanges should hold open until both resolve.
    const aGate = new Promise<void>((resolve) => { aResolve = resolve; });
    const bGate = new Promise<void>((resolve) => { bResolve = resolve; });

    registerConfigChangeHandler("a.", async (changes) => {

      await aGate;

      return changes.map((c) => ({ kind: "applied", path: c.path } satisfies ChangeOutcome));
    });
    registerConfigChangeHandler("b.", async (changes) => {

      await bGate;

      return changes.map((c) => ({ kind: "applied", path: c.path } satisfies ChangeOutcome));
    });

    const dispatch = applyConfigChanges([
      { current: 1, path: "a.x", previous: 0 },
      { current: 1, path: "b.y", previous: 0 }
    ]);

    // Release the handlers in reverse registration order to prove independence: nothing serializes their execution.
    bResolve?.();
    aResolve?.();

    const result = await dispatch;

    assert.equal(result.applied.length, 2);
  });

  test("ignores a handler outcome for a path outside its input batch (no cross-handler override)", async () => {

    // Two handlers run in parallel. The "b." handler misbehaves and also reports an outcome for "a.x" - a path it was never given. The primitive must ignore
    // that foreign outcome so it cannot override the authoritative classification from the "a." handler via last-write-wins into the outcome map.
    registerConfigChangeHandler("a.", async () => [{ kind: "applied", path: "a.x" }]);
    registerConfigChangeHandler("b.", async () => [
      { kind: "applied", path: "b.y" },
      { kind: "rejected", path: "a.x", reason: "foreign outcome that must be ignored" }
    ]);

    const ax = { current: 1, path: "a.x", previous: 0 };
    const by = { current: 1, path: "b.y", previous: 0 };
    const result = await applyConfigChanges([ ax, by ]);

    // a.x reflects its own handler's "applied", not b.'s foreign "rejected".
    assert.deepEqual(result.applied.map((c) => c.path), [ "a.x", "b.y" ]);
    assert.equal(result.rejected.length, 0, "the foreign rejection for a.x was ignored");
    assert.equal(result.deferred.length, 0);
  });
});
