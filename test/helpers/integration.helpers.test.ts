/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * integration.helpers.test.ts: Tests for the integration test harness itself. Every integration suite depends on its disposal contract holding correctly,
 * so a regression here cascades into every dependent suite. The tests below assert the language-level guarantees we
 * rely on (Symbol.asyncDispose runs exactly once at scope exit, propagates body errors, surfaces cleanup errors) plus the harness-level guarantees we add on
 * top (LIFO cleanup ordering, AggregateError on multiple cleanup failures, temp dir removed at disposal, production resolvers point at the temp dir).
 *
 * These tests live under test/ because they exercise the integration tier and need the same runtime as integration tests (the npm run test:integration
 * harness runs with a 120s timeout). They run in the same e2e batch as the suites they support.
 */
import { access, readFile, writeFile } from "node:fs/promises";
import { createIntegrationContext, pathInDataDir, readPersistedJson, writePersistedJson } from "./integration.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { getDataDir } from "../../src/config/paths.ts";

describe("createIntegrationContext - lifecycle", () => {

  test("provides a temp dataDir that exists during the binding scope", async () => {

    let captured = "";

    {

      await using ctx = await createIntegrationContext();

      captured = ctx.dataDir;
      await assert.doesNotReject(() => access(ctx.dataDir), "dataDir should exist during the binding scope");
    }

    // After scope exit, the temp dir must be gone. The disposer ran rm -rf as part of the scope-exit sequence.
    await assert.rejects(() => access(captured), /ENOENT/, "dataDir should be removed after the binding scope exits");
  });

  test("removes the temp dir even when the body throws (language-guaranteed disposer call)", async () => {

    let captured = "";

    /* The body throws after capturing the dataDir path. The language MUST still call [Symbol.asyncDispose] before the throw propagates - that is the entire
     * point of `await using`. After the catch, the temp dir must be removed.
     */
    await assert.rejects(async () => {

      await using ctx = await createIntegrationContext();

      captured = ctx.dataDir;
      throw new Error("body failed");
    }, /body failed/);

    assert.notEqual(captured, "", "the body should have captured a dataDir before throwing");
    await assert.rejects(() => access(captured), /ENOENT/, "dataDir should be removed even though the body threw");
  });

  test("points the production data-dir resolver at the temp dir", async () => {

    /* createIntegrationContext calls initializeDataDir with the temp path. Production code that resolves the data dir (every config module does, lazily, via
     * the path resolver) must see the temp path - otherwise mutateChannels and friends would write to the user's real ~/.prismcast.
     */
    await using ctx = await createIntegrationContext();

    assert.equal(getDataDir(), ctx.dataDir, "production resolver should return the temp dataDir");
  });
});

describe("createIntegrationContext - cleanup ordering", () => {

  test("registered cleanups run in LIFO order at disposal", async () => {

    /* LIFO is the contract: a cleanup registered later (e.g., a server built on top of a temp dir) tears down before its dependencies. We push values into an
     * order array from each cleanup; the resulting order must be the reverse of the registration order.
     */
    const order: number[] = [];

    {

      await using ctx = await createIntegrationContext();

      ctx.registerCleanup(() => { order.push(1); });
      ctx.registerCleanup(() => { order.push(2); });
      ctx.registerCleanup(() => { order.push(3); });
    }

    assert.deepEqual(order, [ 3, 2, 1 ], "cleanups should drain in LIFO order");
  });

  test("registered cleanups run when the body throws (drain regardless of body outcome)", async () => {

    /* The disposer runs whether the body resolved or threw - that is the language guarantee. Cleanups inside the disposer run unconditionally; a body throw
     * does not skip them.
     */
    let cleanupRan = false;

    await assert.rejects(async () => {

      await using ctx = await createIntegrationContext();

      ctx.registerCleanup(() => { cleanupRan = true; });
      throw new Error("body failed");
    }, /body failed/);

    assert.equal(cleanupRan, true, "cleanup should run even though body threw");
  });

  test("a cleanup that throws does not prevent subsequent cleanups from running", async () => {

    /* The disposer accumulates cleanup errors rather than aborting the drain on the first one. A bad cleanup must not strand the rest - resource leaks are
     * exactly what the harness exists to prevent.
     */
    const ran: string[] = [];

    /* The first cleanup throws; the next two should still run. The cleanup-failure surfaces from the disposer (we assert on it via the rejects below) but the
     * other two cleanups still produce their side effects.
     */
    await assert.rejects(async () => {

      await using ctx = await createIntegrationContext();

      // Registered first -> runs LAST during LIFO drain.
      ctx.registerCleanup(() => { ran.push("first-registered"); });
      ctx.registerCleanup(() => { ran.push("middle"); });
      // Registered last -> runs FIRST during LIFO drain. This one throws.
      ctx.registerCleanup(() => { ran.push("last-registered-throws"); throw new Error("boom"); });
    }, /boom/);

    assert.deepEqual(ran, [ "last-registered-throws", "middle", "first-registered" ], "all cleanups should have run despite the first throwing");
  });

  test("async cleanups are awaited (subsequent cleanups do not run until the prior one resolves)", async () => {

    /* The drain awaits each cleanup before moving on. A cleanup whose async work resolves out-of-order in real time would otherwise let later cleanups race
     * past it. We use a microtask delay to force each cleanup to be sequenced; the order array proves they ran in LIFO not in interleaved/parallel order.
     */
    const order: string[] = [];

    {

      await using ctx = await createIntegrationContext();

      ctx.registerCleanup(async () => { await Promise.resolve(); order.push("a"); });
      ctx.registerCleanup(async () => { await Promise.resolve(); order.push("b"); });
      ctx.registerCleanup(async () => { await Promise.resolve(); order.push("c"); });
    }

    assert.deepEqual(order, [ "c", "b", "a" ], "async cleanups should drain in LIFO order with each awaited before the next");
  });
});

describe("createIntegrationContext - error surfacing", () => {

  test("a single cleanup failure surfaces directly (not wrapped in AggregateError)", async () => {

    /* A test-author looking at output should see the actual cause when only one thing failed. Wrapping a sole error in AggregateError would be noise. We
     * accumulate errors internally and unwrap when the count is exactly one.
     */
    await assert.rejects(async () => {

      await using ctx = await createIntegrationContext();

      ctx.registerCleanup(() => { throw new Error("only one cleanup failed"); });
    }, /only one cleanup failed/);
  });

  test("multiple cleanup failures surface as AggregateError with every reason captured", async () => {

    /* When more than one cleanup fails, no failure should be silently dropped. AggregateError carries every reason in its .errors array.
     */
    let captured: unknown;

    try {

      await using ctx = await createIntegrationContext();

      ctx.registerCleanup(() => { throw new Error("first"); });
      ctx.registerCleanup(() => { throw new Error("second"); });
      ctx.registerCleanup(() => { throw new Error("third"); });
    } catch(err) {

      captured = err;
    }

    assert.ok(captured instanceof AggregateError, "multiple cleanup failures should surface as AggregateError");
    assert.equal(captured.errors.length, 3, "every cleanup failure should be captured");

    // The reasons should be present in the .errors array. Order reflects the LIFO drain (third registered -> drained first -> captured first).
    const messages = (captured.errors as Error[]).map((e) => e.message);

    assert.deepEqual(messages, [ "third", "second", "first" ], "cleanup errors should appear in LIFO drain order");
  });

  test("body failure + cleanup failure both surface (language wraps in SuppressedError)", async () => {

    /* The language disposal protocol wraps body+disposer dual failures in SuppressedError. Per the ECMAScript spec, the disposer's error is the "new" error
     * that suppresses the body's "previously-pending" error - so .error holds the cleanup failure and .suppressed holds the body failure. The test asserts the
     * spec-correct mapping so a future refactor does not silently invert it.
     */
    let captured: unknown;

    try {

      await using ctx = await createIntegrationContext();

      ctx.registerCleanup(() => { throw new Error("cleanup failed"); });
      throw new Error("body failed");
    } catch(err) {

      captured = err;
    }

    // SuppressedError is the ES2024-native wrapper.
    assert.ok(captured instanceof SuppressedError, "body+cleanup dual failure should produce SuppressedError");
    assert.match((captured.error as Error).message, /cleanup failed/, "the suppressing (cleanup) error should be on .error per the ES2024 spec");
    assert.match((captured.suppressed as Error).message, /body failed/, "the suppressed (body) error should be on .suppressed per the ES2024 spec");
  });

  test("a non-Error thrown from cleanup is wrapped in Error before propagation (only-throw-error contract)", async () => {

    /* A cleanup that throws a string (legal in JS but unusual) should not propagate as a raw string - the test runner expects Error instances and
     * only-throw-error enforces that contract. The disposer wraps non-Error values in Error preserving the stringified value as the message.
     */
    await assert.rejects(async () => {

      await using ctx = await createIntegrationContext();

      ctx.registerCleanup(() => {

        // eslint-disable-next-line @typescript-eslint/only-throw-error -- intentional: testing the harness's wrap behavior for non-Error throws.
        throw "raw string thrown from cleanup";
      });
    }, /raw string thrown from cleanup/, "non-Error throw should be wrapped while preserving the value");
  });
});

describe("readPersistedJson / writePersistedJson", () => {

  test("write then read round-trips the value", async () => {

    await using ctx = await createIntegrationContext();

    await writePersistedJson(ctx, "round-trip.json", { hello: "world", n: 42 });

    const data = await readPersistedJson(ctx, "round-trip.json");

    assert.deepEqual(data, { hello: "world", n: 42 });
  });

  test("read returns Promise<unknown> - caller must narrow at the assertion site", async () => {

    /* The signature returns unknown deliberately. Callers cannot pretend the on-disk shape matches a TypeScript type they declared at the call site - that
     * would be a lie because production code controls what gets written. This test verifies the contract by performing a runtime narrow rather than an unsafe
     * cast: assert object-ness, assert key presence via `in`, then assert the value's type. No `as { value: number }` cast - we narrow at runtime.
     */
    await using ctx = await createIntegrationContext();

    await writePersistedJson(ctx, "narrow-me.json", { value: 7 });

    const data = await readPersistedJson(ctx, "narrow-me.json");

    assert.ok((typeof data === "object") && (data !== null), "the helper returns whatever JSON.parse produced; we expect an object here");
    assert.ok("value" in data, "the persisted shape carried a 'value' key");

    // After the `in` narrow, TypeScript widens data["value"] to unknown (record-of-unknown semantics). Narrow again to number before equality compares.
    const value = (data as Record<string, unknown>)["value"];

    assert.equal(typeof value, "number", "value should be a number");
    assert.equal(value, 7);
  });

  test("write creates parent directories when the filename includes a subdirectory", async () => {

    await using ctx = await createIntegrationContext();

    await writePersistedJson(ctx, "nested/under/here.json", { ok: true });

    const data = await readPersistedJson(ctx, "nested/under/here.json");

    assert.deepEqual(data, { ok: true });
  });

  test("read of a missing file rejects with ENOENT", async () => {

    await using ctx = await createIntegrationContext();

    await assert.rejects(() => readPersistedJson(ctx, "definitely-not-here.json"), /ENOENT/);
  });

  test("read of a file with invalid JSON rejects with a parse error", async () => {

    await using ctx = await createIntegrationContext();

    await writeFile(pathInDataDir(ctx, "broken.json"), "{ this is not valid json", "utf8");

    await assert.rejects(() => readPersistedJson(ctx, "broken.json"), /JSON/i, "JSON.parse failure should propagate");
  });
});

describe("pathInDataDir", () => {

  test("composes path segments under the context's dataDir", async () => {

    await using ctx = await createIntegrationContext();

    const composed = pathInDataDir(ctx, "sub", "dir", "file.json");

    assert.ok(composed.startsWith(ctx.dataDir + "/"), "composed path should start with the dataDir");
    assert.ok(composed.endsWith("/sub/dir/file.json"), "composed path should preserve the supplied segments");
  });

  test("returns the dataDir itself when no segments are supplied", async () => {

    await using ctx = await createIntegrationContext();

    assert.equal(pathInDataDir(ctx), ctx.dataDir);
  });
});

describe("isolation across contexts", () => {

  test("two sequential contexts get distinct dataDirs", async () => {

    let firstDir = "";
    let secondDir = "";

    {

      await using a = await createIntegrationContext();

      firstDir = a.dataDir;
    }

    {

      await using b = await createIntegrationContext();

      secondDir = b.dataDir;
    }

    assert.notEqual(firstDir, secondDir, "consecutive contexts should not share a dataDir");

    // Both should have been removed at their respective scope exits.
    await assert.rejects(() => access(firstDir), /ENOENT/);
    await assert.rejects(() => access(secondDir), /ENOENT/);
  });

  test("file written in one context is not visible to a subsequent context", async () => {

    let firstDir = "";

    {

      await using a = await createIntegrationContext();

      firstDir = a.dataDir;
      await writePersistedJson(a, "leak-test.json", { fromFirst: true });
    }

    // After the first context is disposed, its dataDir is gone. A second context gets a fresh dataDir with no leftover files.
    await using b = await createIntegrationContext();

    assert.notEqual(b.dataDir, firstDir, "second context should not reuse the first's dataDir");
    await assert.rejects(() => readFile(pathInDataDir(b, "leak-test.json"), "utf8"), /ENOENT/, "no file leaked across contexts");
  });
});
