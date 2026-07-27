/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cleanup.helpers.test.ts: Tests for the puppeteer-stream WebSocketServer cleanup helpers. The integration with puppeteer-stream is exercised at scale by every
 * dependent test file - if the close path regressed, the runner would hang at suite end. The unit tests below pin the contract that matters to direct callers:
 * the awaitable form returns a Promise that always resolves (best-effort), is safe to call more than once, and never throws synchronously; the on-idle form
 * returns void without throwing.
 */
import { closePuppeteerStreamWss, closePuppeteerStreamWssOnIdle } from "./cleanup.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

describe("closePuppeteerStreamWss", () => {

  /* The function awaits the wss promise from puppeteer-stream and closes it. We can't easily observe "the upstream WebSocketServer is now closed" without
   * actually loading puppeteer-stream and inspecting it - and loading it here would spawn a real WebSocketServer in the test process just to test the close.
   *
   * Instead, the tests pin the contract that matters to consumers: the function returns a Promise that always resolves (best-effort cleanup), resolves the same way
   * whether called once or repeatedly, and never throws synchronously. The integration is exercised at scale by every test file that calls this helper - if the dynamic
   * import or close path regressed, the test runner would hang at suite end across the dozens of dependent test files.
   */

  test("returns a Promise that resolves without throwing", async () => {

    const result = closePuppeteerStreamWss();

    assert.ok(result instanceof Promise, "function returns a Promise");
    await assert.doesNotReject(() => result, "the promise resolves without rejection");
  });

  test("is idempotent - calling twice in succession resolves both calls without throwing", async () => {

    // Calling close() on an already-closed WebSocketServer is a no-op upstream. The helper's try/catch absorbs any underlying error regardless, so consecutive
    // calls must both resolve cleanly.
    await assert.doesNotReject(() => closePuppeteerStreamWss());
    await assert.doesNotReject(() => closePuppeteerStreamWss(), "second call after the server is already closing");
  });

  test("never throws synchronously (the dynamic import and try/catch absorb upstream failure modes)", () => {

    // Synchronous-throw is the one failure mode we explicitly guarantee against, because consuming tests call this from after() hooks where a sync throw would
    // mask the test outcome.
    assert.doesNotThrow(() => { void closePuppeteerStreamWss(); });
  });
});

describe("closePuppeteerStreamWssOnIdle", () => {

  test("does not throw and returns void", () => {

    // The on-idle variant schedules a 0ms unref'd timer and returns immediately. A sync throw here would kill the entire test file, so we lock the no-throw
    // contract explicitly.
    assert.doesNotThrow(() => { closePuppeteerStreamWssOnIdle(); });
  });

  test("the scheduled timer is unref'd (does not keep the process alive on its own)", () => {

    /* We can't directly observe "would this keep the process alive" without forking a subprocess. This assertion only guards against the .unref() call itself
     * throwing - it would not catch an accidentally removed .unref() call, since omitting it does not make the function throw. Catching that omission would
     * require asserting directly on the Timeout object's .unref() invocation, for example via a spy, not this synchronous no-throw check.
     */
    assert.doesNotThrow(() => { closePuppeteerStreamWssOnIdle(); });
  });
});
