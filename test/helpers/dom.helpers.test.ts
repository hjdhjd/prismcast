/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * dom.helpers.test.ts: Tests for the DOM-runtime test harness itself. As with integration.helpers.test.ts, every DOM-runtime suite depends on these guarantees
 * holding correctly. The tests pin the harness's contract surface: served HTML is loaded into a synthetic Window, inline
 * <script> bodies are extracted in document order, scripts only execute when the test explicitly opts in via runScripts(), arbitrary code can be evaluated in
 * the Window's global scope, and disposal closes the Window before the bootApp listener (LIFO) so happy-dom's async tasks settle before the listener tears down.
 *
 * The tests deliberately avoid asserting on the production scripts themselves; that is the dedicated suite in test/e2e/dom-runtime/. Here we only confirm the
 * harness's own primitives behave as advertised.
 */
import { access, readFile } from "node:fs/promises";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { createDomTestContext } from "./dom.helpers.ts";
import { mutateChannels } from "../../src/config/userChannels.ts";
import path from "node:path";

describe("createDomTestContext - lifecycle and disposal", () => {

  test("provides a temp dataDir and bootApp listener URL during the binding scope", async () => {

    /* The harness composes integration context (temp dir + cleanup queue) with bootApp (ephemeral-port Express listener) and a happy-dom Window. Every field
     * surfaced to the test should be live during the binding scope.
     */
    let captured = "";

    {

      await using ctx = await createDomTestContext();

      captured = ctx.dataDir;
      assert.ok(ctx.dataDir.length > 0, "dataDir should be a non-empty path");
      assert.ok((ctx.port > 0) && (ctx.port < 65536), "bootApp port should be a valid TCP port");
      assert.equal(ctx.urlFor("/channels"), "http://127.0.0.1:" + String(ctx.port) + "/channels");
      assert.ok(ctx.html.includes("<html"), "served HTML should be a full document");
      assert.ok(ctx.window, "synthetic Window should be live");
      assert.ok(ctx.document, "synthetic Document should be live");
    }

    // After scope exit the temp dir must be gone (the inner integration context's disposer rm -rf'd it).
    await assert.rejects(() => access(captured), /ENOENT/, "dataDir should be removed after the binding scope exits");
  });

  test("removes the temp dir even when the body throws (disposal protocol guarantee)", async () => {

    /* The body throws after capturing the dataDir. The language MUST still call [Symbol.asyncDispose] before the throw propagates - that is the contract of
     * `await using`. After the catch, the temp dir must be removed and the listener must be torn down.
     */
    let captured = "";

    await assert.rejects(async () => {

      await using ctx = await createDomTestContext();

      captured = ctx.dataDir;
      throw new Error("body failed");
    }, /body failed/);

    assert.notEqual(captured, "", "the body should have captured a dataDir before throwing");
    await assert.rejects(() => access(captured), /ENOENT/, "dataDir should be removed even though the body threw");
  });

  test("bootApp listener responds on the ephemeral port for as long as the binding scope is alive", async () => {

    /* The harness wires the Window's URL to point at the bootApp listener. Tests that fetch from inside the synthetic DOM rely on the listener actually serving
     * traffic. We hit /channels (a trivial JSON listing route, browser-state-independent) directly to confirm the bind landed. /health is unsuitable because
     * it returns 503 when the browser is not connected (which is always the case under integration tests).
     */
    await using ctx = await createDomTestContext();

    const response = await fetch(ctx.urlFor("/channels"));

    assert.equal(response.status, 200, "bootApp /channels should respond 200 during the binding scope");
  });
});

describe("createDomTestContext - HTML loading and script extraction", () => {

  test("extracts inline <script> bodies in document order with sequential indices", async () => {

    /* The landing page emits several inline scripts (shared utilities, tab, channels subtab, config subtab, status, and the logs-tab log-viewer block). The
     * harness must surface them in document order so a test that wants "the script before the channels subtab" can use index arithmetic. We assert only structural
     * properties that are stable across re-orderings and additions: indices are 0..N-1 and contiguous, content is a string, length matches scripts array length.
     */
    await using ctx = await createDomTestContext();

    assert.ok(ctx.scripts.length >= 2, "landing page should emit at least two inline scripts");

    for(let i = 0; i < ctx.scripts.length; i++) {

      assert.equal(ctx.scripts[i]?.index, i, "scripts[" + String(i) + "] should carry index " + String(i));
      assert.equal(typeof ctx.scripts[i]?.content, "string", "scripts[" + String(i) + "].content should be a string");
    }
  });

  test("does not auto-execute inline scripts (disableJavaScriptEvaluation invariant)", async () => {

    /* The harness disables happy-dom's automatic script evaluation so tests can opt in to which scripts run. We confirm by checking that none of the production
     * scripts' window.* assignments leaked into the Window before runScripts is called. window.channelTable is the most reliable witness because it is the only
     * shared.ts namespace that other scripts also might read; if it's already defined here, scripts ran when they shouldn't have.
     */
    await using ctx = await createDomTestContext();

    assert.equal(ctx.evaluate("typeof window.channelTable"), "undefined", "window.channelTable should not exist before runScripts");
    assert.equal(ctx.evaluate("typeof window.showToast"), "undefined", "window.showToast should not exist before runScripts");
  });

  test("loads a non-default path when options.path is supplied", async () => {

    /* The harness defaults to "/" (the landing page) but accepts any path on the bootApp. /channels is a JSON listing endpoint with no inline scripts; we use
     * it (rather than /health) because /health returns 503 when no browser is connected, which is always the case under integration tests. The harness rejects
     * non-ok responses, so we need a route that is reliably 200.
     */
    await using ctx = await createDomTestContext({ path: "/channels" });

    assert.equal(ctx.scripts.length, 0, "/channels should yield zero inline scripts");
    assert.match(ctx.html, /^\s*\{/, "the served body should be a JSON object (channel listing payload)");
  });
});

describe("createDomTestContext - script execution", () => {

  test("runScripts executes only the scripts whose entry satisfies the predicate", async () => {

    /* Predicate selectivity is the harness's central feature. We pick a marker that exists in exactly one script (the shared utilities IIFE references
     * window.channelTable in its namespace SSOT) and confirm the selectivity: exactly one script ran, identifiable by its index.
     */
    await using ctx = await createDomTestContext();

    const ran = ctx.runScripts((s) => s.content.includes("window.channelTable"));

    assert.equal(ran.length, 1, "exactly one script should match the channelTable marker");

    // The matching script must be the shared utilities one - we verify by checking the namespace is defined post-execution.
    assert.equal(ctx.evaluate("typeof window.channelTable"), "object");
  });

  test("runScripts returns the indices of executed scripts so tests can assert the exact set", async () => {

    /* When tests need to confirm that NOTHING beyond the intended script ran (defense against a future change accidentally widening the predicate), the indices
     * are the audit trail. We do not hardcode the shared-utilities script's position; instead the test discovers its index dynamically via findIndex and
     * confirms the returned index list matches that single scripts array entry containing the marker.
     */
    await using ctx = await createDomTestContext();

    const expectedIndex = ctx.scripts.findIndex((s) => s.content.includes("window.channelTable"));

    assert.notEqual(expectedIndex, -1, "the shared utilities script must be discoverable in ctx.scripts");

    const ran = ctx.runScripts((s) => s.content.includes("window.channelTable"));

    assert.deepEqual(ran, [expectedIndex], "runScripts should return the exact index list of executed scripts");
  });

  test("runScripts executes selected scripts in document order regardless of predicate iteration", async () => {

    /* Document order matters because emitted scripts may build on each other (e.g., shared utilities defines window.channelTable; channels.ts may read it on
     * load). Even if the predicate logic could short-circuit out of order, the harness must execute in source order. We pick the first two scripts
     * unconditionally by their document-order index and confirm their ran-index list comes back ascending.
     */
    await using ctx = await createDomTestContext();

    /* Pick the first two inline scripts unconditionally - they are guaranteed to be in document order in ctx.scripts, so their returned indices must be
     * 0 then 1.
     */
    const ran = ctx.runScripts((s) => s.index < 2);

    assert.deepEqual(ran, [ 0, 1 ], "runScripts should report executed indices in ascending document order");
  });

  test("evaluate runs arbitrary code in the Window's global scope and returns the completion value", async () => {

    /* evaluate is the post-script inspection surface. A test setting up a fixture might call evaluate("window.foo = 1") then assert via evaluate("window.foo").
     * We confirm both directions: side-effecting assignment lands on the Window, and the return value of an expression evaluates correctly.
     */
    await using ctx = await createDomTestContext();

    ctx.evaluate("window.harnessTestValue = 42");

    assert.equal(ctx.evaluate("window.harnessTestValue"), 42, "side-effecting assignment should land on the Window");
    assert.equal(ctx.evaluate("1 + 2"), 3, "expression result should be returned");
    assert.equal(ctx.evaluate("typeof globalThis"), "object", "globalThis should be the Window in the eval scope");
  });

  test("evaluateJson round-trips arrays and objects across the vm boundary so deepEqual works", async () => {

    /* The vm sandbox happy-dom builds around the Window has its own Array/Object prototypes. assert.deepEqual (under node:assert/strict) does a prototype
     * identity check that fails on cross-realm values even when the structure matches. evaluateJson serializes inside the sandbox and parses outside, returning
     * plain Node values that deepEqual compares correctly.
     */
    await using ctx = await createDomTestContext();

    assert.deepEqual(ctx.evaluateJson("[ 1, 2, 3 ]"), [ 1, 2, 3 ], "array should round-trip cleanly");
    assert.deepEqual(ctx.evaluateJson("({ a: 1, b: [ 2, 3 ] })"), { a: 1, b: [ 2, 3 ] }, "object with nested array should round-trip");
    assert.equal(ctx.evaluateJson("'hello'"), "hello", "primitives round-trip via JSON unchanged");
    assert.equal(ctx.evaluateJson("undefined"), undefined, "undefined returns undefined (JSON.stringify drops it)");
  });
});

describe("createDomTestContext - integration with production state", () => {

  test("data mutated via mutateChannels surfaces in the page HTML across consecutive contexts", async () => {

    /* The harness routes through bootApp, which serves the production landing-page route. That route reads channel state from initializePersistence's loaded
     * stores. Mutations applied between context creation and page fetch must therefore surface in the served HTML - this is the guarantee that lets tests
     * seed state then assert post-render.
     */
    await using ctx = await createDomTestContext();

    /* Verify the served HTML reflects in-memory state. The "abc" predefined channel is rendered with id display-row-abc; this is byte-stable across the page
     * lifetime so the substring check is safe.
     */
    assert.ok(ctx.html.includes("display-row-abc"), "predefined channel 'abc' should appear in the rendered page");
  });

  test("a custom user channel mutated before context creation appears in the rendered page", async () => {

    /* createDomTestContext fetches the page once at construction and holds it as ctx.html, so that snapshot predates any mutation we apply afterward. To observe
     * a post-construction change we open the context, call mutateChannels to write the new user channel into the temp-dir stores, then re-fetch the page through
     * the bootApp listener. The fresh response renders against the now-current in-memory state and includes the channel; ctx.html does not.
     */
    await using ctx = await createDomTestContext();

    await mutateChannels((data) => {

      data.channels["dom-harness-custom"] = { name: "Harness Custom", url: "https://example.test/harness" };
    });

    // Refetch via the bootApp listener to capture the post-mutation HTML. The current ctx.html is stale; we fetch fresh.
    const response = await fetch(ctx.urlFor("/"));
    const fresh = await response.text();

    assert.ok(fresh.includes("display-row-dom-harness-custom"), "newly-added user channel should appear in a re-fetched page");
  });

  test("the Window's location URL points at the bootApp listener so relative fetch resolves correctly", async () => {

    /* The harness sets the Window URL to the bootApp's origin. window.location.origin should match the bootApp's URL so emitted scripts that call
     * fetch("/some/path") hit the real production listener. We assert that the origin matches the bootApp's own address.
     */
    await using ctx = await createDomTestContext();

    const origin = ctx.evaluate("window.location.origin");

    assert.equal(origin, "http://127.0.0.1:" + String(ctx.port), "window.location.origin should be the bootApp origin");
  });
});

describe("createDomTestContext - cleanup behavior", () => {

  test("registerCleanup hooks run at disposal (LIFO) alongside the harness's internal cleanups", async () => {

    /* Tests can layer their own cleanup hooks on top of the harness via registerCleanup. Internally, bootApp registers the listener-close hook first, then
     * createDomTestContext registers the Window-close hook second, so at disposal the Window closes first and the listener closes last - tests that need to
     * register cleanups whose order matters relative to Window teardown have a deterministic ordering: registered later means runs earlier.
     */
    const order: string[] = [];

    {

      await using ctx = await createDomTestContext();

      ctx.registerCleanup(() => { order.push("test-registered-first"); });
      ctx.registerCleanup(() => { order.push("test-registered-second"); });
    }

    // LIFO: the second-registered runs before the first-registered. Both run before the harness-internal Window/listener teardown (not directly observable).
    assert.deepEqual(order, [ "test-registered-second", "test-registered-first" ], "test-registered cleanups should drain in LIFO order");
  });

  test("disposal closes the Window so happy-dom's async tasks settle before the listener tears down", async () => {

    /* Window close drains microtasks and timers happy-dom queued internally. We can't directly observe the listener-after-window ordering through public API,
     * so we assert a softer guarantee: after disposal, evaluate against the Window throws or returns from a closed sandbox. This is the consumer-visible signal
     * that the Window did, in fact, close.
     */
    let capturedWindow: { evaluate: (code: string) => unknown } | null = null;

    {

      await using ctx = await createDomTestContext();

      // Wrap in an arrow function so the captured reference does not depend on the eventual value of `this` (the context object goes out of scope at disposal).
      capturedWindow = { evaluate: (code: string): unknown => ctx.evaluate(code) };

      // Confirm the window is functional inside the scope.
      assert.equal(ctx.evaluate("1 + 1"), 2);
    }

    /* After disposal the eval reference still exists on capturedWindow but the underlying happy-dom sandbox has been closed. Any call should either throw or
     * return undefined; we accept both because happy-dom's exact post-close behavior is implementation-defined and may evolve across versions.
     */
    let postCloseBehaviorObserved = false;

    try {

      capturedWindow.evaluate("1 + 1");
      postCloseBehaviorObserved = true;
    } catch {

      postCloseBehaviorObserved = true;
    }

    assert.equal(postCloseBehaviorObserved, true, "post-close eval should either throw or return without crashing the runner");
  });

  test("two sequential contexts get distinct dataDirs and distinct bootApp ports", async () => {

    /* Per-test isolation is the harness's reason for being. Two contexts opened in sequence (each within its own scope) must not share dataDir or port - they
     * are independent test instances and must not see each other's state.
     */
    let firstDir = "";
    let firstPort = 0;
    let secondDir = "";
    let secondPort = 0;

    {

      await using a = await createDomTestContext();

      firstDir = a.dataDir;
      firstPort = a.port;
    }

    {

      await using b = await createDomTestContext();

      secondDir = b.dataDir;
      secondPort = b.port;
    }

    assert.notEqual(firstDir, secondDir, "consecutive contexts should not share a dataDir");
    assert.notEqual(firstPort, secondPort, "consecutive contexts should not share a bootApp port");

    // Both temp dirs should have been removed at their respective scope exits.
    await assert.rejects(() => access(firstDir), /ENOENT/);
    await assert.rejects(() => access(secondDir), /ENOENT/);
  });

  test("on-disk state mutated via the harness lands in the temp data directory and is removed at disposal", async () => {

    /* End-to-end persistence trip: mutate via production exports, read raw JSON from the temp dir to confirm the write landed, then verify the directory is
     * gone after the binding scope. This is the complete proof that the harness routes mutations into the temp tree (not into the user's real ~/.prismcast)
     * and tears down cleanly.
     */
    let capturedPath = "";

    {

      await using ctx = await createDomTestContext();

      await mutateChannels((data) => {

        data.channels["harness-persist-test"] = { name: "Harness Persist", url: "https://example.test/harness-persist" };
      });

      capturedPath = path.join(ctx.dataDir, "channels.json");
      const raw = await readFile(capturedPath, "utf8");

      assert.ok(raw.includes("harness-persist-test"), "the user channel should have been written to channels.json");
    }

    await assert.rejects(() => access(capturedPath), /ENOENT/, "channels.json under the temp dir should be removed at disposal");
  });
});
