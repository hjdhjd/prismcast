/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cleanup.helpers.ts: puppeteer-stream WebSocketServer cleanup helpers. Production modules under browser/ transitively pull in puppeteer-stream, which
 * IIFE-spawns a long-lived WebSocketServer at module load to drive its screen-capture extension. That server is not unref'd, so Node's test runner won't exit
 * while it's listening. Tests that import puppeteer-stream-pulling-in production modules call one of these helpers during teardown to close the specific known
 * handle.
 *
 * Upstream status (as of 2026-05): the root cause is that puppeteer-stream's wss IIFE in src/PuppeteerStream.ts does not call .unref() on the WebSocketServer
 * after binding (see the source at https://github.com/SamuelScheit/puppeteer-stream/blob/main/src/PuppeteerStream.ts). A search of the upstream issue tracker
 * for "WebSocketServer", "unref", and "exit hang" returned no relevant matches; the related issue #131 ("Shutdown is taking more than 5 seconds") is about a
 * separate setTimeout in the stream-stop path. The pristine fix would be a one-line .unref() upstream; we work around the limitation here so PrismCast tests
 * exit cleanly without depending on an upstream change.
 *
 * We close the specific exported handle rather than scanning process._getActiveHandles() (an undocumented Node internal) and duck-typing by
 * constructor.name === "Server", which would force-close any unrelated Server handle and depend on internal API surface. puppeteer-stream exports its
 * WebSocketServer as the awaitable `wss` symbol from the package entry. We import it dynamically inside the helper so this module itself doesn't force every
 * consuming test file to load puppeteer-stream - tests that do not transitively need it never trigger the load.
 *
 * Two call patterns are supported because they serve genuinely different test styles:
 *
 * - closePuppeteerStreamWss() is awaitable. Tests that boot their own HTTP server invoke it from an after() hook AFTER closing their own server, because a
 *   module-level timer would race against the test's still-listening server.
 *
 * - closePuppeteerStreamWssOnIdle() schedules the close on a 0ms unref'd timer that fires when the event loop becomes idle (i.e., after all synchronous test
 *   execution). Tests that don't manage their own server lifecycle call this once at module scope; the timer is itself unref'd so it never keeps the runner
 *   alive on its own but does fire just before the runner would otherwise hang on the stranded WebSocketServer.
 */

/**
 * Closes the WebSocketServer that puppeteer-stream lazily spawns at module load. Awaitable. Best-effort: failures during cleanup are swallowed because the test
 * runner will surface any underlying issue on its own, and a failed close should not fail the test that called this helper. Idempotent: calling close() on an
 * already-closed WebSocketServer is a no-op.
 *
 * The dynamic import is deliberate - this helper is widely re-exported through testing.helpers.ts, and a static import here would force every consuming test
 * to load puppeteer-stream transitively (which itself spawns the WebSocketServer). With a dynamic import, the load only happens when this helper is actually
 * called - and tests that call it have almost always already loaded puppeteer-stream transitively, so the dynamic import is a cache hit.
 */
export async function closePuppeteerStreamWss(): Promise<void> {

  try {

    const { wss } = await import("puppeteer-stream");
    const server = await wss;

    server.close();
  } catch {

    // Best-effort cleanup; nothing to do if the wss promise rejected (port enumeration failed) - there is no server to close.
  }
}

/**
 * Schedules closePuppeteerStreamWss on a 0ms unref'd timer that fires once the event loop becomes idle. Tests that load puppeteer-stream-pulling-in modules
 * but don't manage their own teardown call this once at module scope; the unref'd timer never keeps Node alive on its own, but it does run the cleanup just
 * before the runner would otherwise hang on the stranded WebSocketServer.
 *
 * Idempotent in practice: if multiple test files in the same process call this, each schedules an independent unref'd timer; all of them run the close
 * harmlessly because closePuppeteerStreamWss is itself a fixed-point operation.
 */
export function closePuppeteerStreamWssOnIdle(): void {

  setTimeout(() => { void closePuppeteerStreamWss(); }, 0).unref();
}
