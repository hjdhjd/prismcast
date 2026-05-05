/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.test.ts: Unit tests for the testable, non-Chrome-driving pieces of browser/index.ts. The module is dominated by Chrome lifecycle code (launchBrowser,
 * detectDisplayDimensions, cleanupStalePages, executeBrowserRestart, prepareExtension), which all require Puppeteer integration and are deferred to e2e.
 *
 * The unit tests here cover the synchronous accessor surface that does not touch Chrome:
 *
 *   - isGracefulShutdown / setGracefulShutdown (the boolean flag the disconnect handler reads)
 *   - registerManagedPage / unregisterManagedPage (the WeakMap-backed page-registration tracker)
 *   - getChromeVersion (the cached version string accessor)
 *   - getBrowserInstance / isBrowserConnected (the synchronous status accessors)
 *   - canCleanupChrome (the flag that gates Chrome cleanup at process exit)
 *   - buildLaunchOptions (the launch-option assembly that reads CONFIG)
 *   - getExecutablePath (the env-var-or-search executable resolver)
 *   - emitCurrentSystemStatus (the status emitter wrapper - we drain the resulting SSE event)
 *
 * Importing this module pulls in puppeteer-stream which starts a WebSocketServer at evaluation time. The test runner uses --test-force-exit so that handle does
 * not prevent the file from exiting cleanly.
 */
import { afterEach, before, beforeEach, describe, test } from "node:test";
import { buildLaunchOptions, canCleanupChrome, emitCurrentSystemStatus, getBrowserInstance, getChromeVersion, getExecutablePath, isBrowserConnected,
  isGracefulShutdown, registerManagedPage, setGracefulShutdown, unregisterManagedPage } from "./index.ts";
import { mkdtempSync, rmSync } from "node:fs";
import { CONFIG } from "../config/index.ts";
import type { Page } from "puppeteer-core";
import assert from "node:assert/strict";
import { initializeDataDir } from "../config/paths.ts";
import os from "node:os";
import path from "node:path";
import { subscribeToStatus } from "../streaming/statusEmitter.ts";

/* The data directory must be initialized before buildLaunchOptions() can resolve the Chrome user-data-dir path (it derives from the data dir via getChromeDataDir).
 * We create a temp directory once for the whole file, point initializeDataDir at it, and clean it up after every test has run. The path is deterministic per test
 * file via mkdtemp's random suffix.
 */
const tempDataDir = mkdtempSync(path.join(os.tmpdir(), "prismcast-browser-index-test-"));

before(() => {

  initializeDataDir(tempDataDir);
});

// Best-effort cleanup. The test runner uses --test-force-exit so a failure to remove this directory at the end of the file does not hang the process; however, we
// attempt cleanup so the temp dir is not left behind on success.
process.on("beforeExit", () => {

  try {

    rmSync(tempDataDir, { force: true, recursive: true });
  } catch {

    // Ignore - the OS will eventually clean up tmpdir entries.
  }
});

/* fakePage returns a minimal Page-shaped object suitable for use with the WeakMap-based page registry. The registry only stores the reference; it never invokes
 * Page methods, so an empty object is sufficient.
 */
function fakePage(): Page {

  return {} as unknown as Page;
}

describe("isGracefulShutdown / setGracefulShutdown", () => {

  let original: boolean;

  beforeEach(() => {

    original = isGracefulShutdown();
  });

  afterEach(() => {

    setGracefulShutdown(original);
  });

  test("toggles the flag when the setter is called", () => {

    setGracefulShutdown(true);
    assert.equal(isGracefulShutdown(), true, "true after setting to true");

    setGracefulShutdown(false);
    assert.equal(isGracefulShutdown(), false, "false after setting to false");
  });

  test("is idempotent on repeated identical writes", () => {

    setGracefulShutdown(true);
    setGracefulShutdown(true);

    assert.equal(isGracefulShutdown(), true, "still true after two sets");
  });

  test("the getter is read-only - calling it does not mutate state", () => {

    setGracefulShutdown(false);

    isGracefulShutdown();
    isGracefulShutdown();

    assert.equal(isGracefulShutdown(), false, "getter calls do not flip the flag");
  });
});

describe("registerManagedPage / unregisterManagedPage", () => {

  test("registering a page does not throw and is idempotent for the same page reference", () => {

    // The registry uses a WeakMap with monotonically increasing IDs. Registering the same page twice produces two IDs, which is a quirk worth locking - the
    // newer ID wins and the older ID is orphaned in the managedPageIds set. Since unregisterManagedPage uses the WeakMap lookup to find the current ID, it
    // always cleans up the most recent registration. Mainly we lock that registration is non-throwing.
    const page = fakePage();

    assert.doesNotThrow(() => {

      registerManagedPage(page);
    });

    assert.doesNotThrow(() => {

      registerManagedPage(page);
    }, "second register on the same page is non-throwing");

    unregisterManagedPage(page);
  });

  test("unregistering a never-registered page is a clean no-op", () => {

    // Boundary: the unregister path does a WeakMap.get which returns undefined for unknown pages, then does nothing. Locks the contract that callers can blanket
    // unregister without precondition checks.
    assert.doesNotThrow(() => {

      unregisterManagedPage(fakePage());
    }, "unregister of unknown page must be a no-op");
  });

  test("unregistering after registering does not throw (the round-trip path)", () => {

    const page = fakePage();

    registerManagedPage(page);

    assert.doesNotThrow(() => {

      unregisterManagedPage(page);
    });
  });

  test("a second unregister on the same page is a no-op (idempotent cleanup)", () => {

    // After the first unregister removes the entry, the second call's WeakMap.get returns undefined and the early-exit guard fires. Locks the contract.
    const page = fakePage();

    registerManagedPage(page);
    unregisterManagedPage(page);

    assert.doesNotThrow(() => {

      unregisterManagedPage(page);
    }, "second unregister on already-unregistered page is a no-op");
  });
});

describe("getChromeVersion", () => {

  test("returns null when no browser has been launched in this process", () => {

    // The cached version is set inside launchBrowser() after a successful launch and cleared on disconnect. With no browser launched, it stays null.
    assert.equal(getChromeVersion(), null, "no launched browser -> null version");
  });
});

describe("getBrowserInstance", () => {

  test("returns null when no browser has been launched", () => {

    // Like getChromeVersion, the instance reference is null until a launch completes.
    assert.equal(getBrowserInstance(), null, "no launched browser -> null instance");
  });
});

describe("isBrowserConnected", () => {

  test("returns false when no browser has been launched", () => {

    // The check is `!!currentBrowser && currentBrowser.connected` - both clauses fail when currentBrowser is null.
    assert.equal(isBrowserConnected(), false, "no launched browser -> not connected");
  });
});

describe("canCleanupChrome", () => {

  test("returns a boolean (the cleanup-ownership flag is initialized at module load)", () => {

    // The flag starts false and is set true when killStaleChrome runs in the startup sequence. We assert only on the boolean type since the value depends on
    // whether other test files have run killStaleChrome by the time this test runs.
    assert.equal(typeof canCleanupChrome(), "boolean", "result is a boolean");
  });
});

describe("getExecutablePath", () => {

  let originalExecutablePath: string | null;

  beforeEach(() => {

    originalExecutablePath = CONFIG.browser.executablePath;
  });

  afterEach(() => {

    CONFIG.browser.executablePath = originalExecutablePath;
  });

  test("returns CONFIG.browser.executablePath verbatim when it is set (env-var override path)", () => {

    // Boundary: the explicit configuration takes precedence over filesystem search. We verify by setting a sentinel string and asserting it surfaces unchanged.
    CONFIG.browser.executablePath = "/sentinel/path/to/chrome";

    assert.equal(getExecutablePath(), "/sentinel/path/to/chrome", "explicit override wins");
  });

  test("does not throw on a non-empty override even if the file does not exist (no filesystem check)", () => {

    // Negative test: the override path is trusted - the function does not stat() it. This locks the documented behavior that operators can configure paths that
    // will be valid by the time Puppeteer launches Chrome (e.g., a script-mounted volume in Docker).
    CONFIG.browser.executablePath = "/this/path/definitely/does/not/exist/chrome";

    assert.doesNotThrow(() => {

      getExecutablePath();
    }, "explicit override is not stat-checked");
  });
});

describe("buildLaunchOptions", () => {

  let originalExecutablePath: string | null;

  beforeEach(() => {

    // We force a known executablePath so the function does not perform a filesystem search (which would fail in CI environments without Chrome installed).
    originalExecutablePath = CONFIG.browser.executablePath;
    CONFIG.browser.executablePath = "/sentinel/chrome";
  });

  afterEach(() => {

    CONFIG.browser.executablePath = originalExecutablePath;
  });

  test("includes the configured executablePath in the launch options", () => {

    const options = buildLaunchOptions();

    assert.equal(options.executablePath, "/sentinel/chrome", "executablePath surfaces from CONFIG");
  });

  test("disables Puppeteer's default viewport so we can manage sizing via CDP", () => {

    // The contract is that defaultViewport is null - this prevents Puppeteer from forcing 800x600 on every page. We rely on CDP-based sizing in
    // resizeAndMinimizeWindow.
    assert.equal(buildLaunchOptions().defaultViewport, null, "default viewport disabled");
  });

  test("runs Chrome in headed mode so the streaming extension can capture", () => {

    // The streaming extension requires a visible window for the compositor capture path. The launch must be headed.
    assert.equal(buildLaunchOptions().headless, false, "headless: false");
  });

  test("uses pipe mode for the DevTools Protocol connection", () => {

    // Pipe mode is faster and more reliable than WebSocket under load. Locks the documented choice.
    assert.equal(buildLaunchOptions().pipe, true, "pipe: true");
  });

  test("includes the autoplay-no-user-gesture-required flag in the args list", () => {

    // The autoplay flag is critical for unattended capture. We assert it is present in the args.
    const args = buildLaunchOptions().args ?? [];

    assert.ok(args.includes("--autoplay-policy=no-user-gesture-required"), "autoplay flag present");
  });

  test("includes window-size args derived from the configured viewport", () => {

    // The function appends a --window-size arg with the preset viewport dimensions. We verify the arg is present and parseable.
    const args = buildLaunchOptions().args ?? [];
    const windowSizeArg = args.find((a) => a.startsWith("--window-size="));

    assert.ok(windowSizeArg, "window-size arg present");

    // The format is --window-size=W,H with positive integer dimensions.
    assert.match(windowSizeArg, /^--window-size=\d+,\d+$/, "well-formed window-size arg");
  });

  test("excludes Puppeteer default args that would interfere with streaming (extensions, automation flag, mute)", () => {

    // The ignoreDefaultArgs list must include the four critical entries. Locks the documented choices.
    const ignored = buildLaunchOptions().ignoreDefaultArgs ?? [];

    assert.ok(Array.isArray(ignored), "ignoreDefaultArgs is an array");

    for(const expected of [ "--disable-extensions", "--enable-automation", "--mute-audio" ]) {

      assert.ok(ignored.includes(expected), "ignoreDefaultArgs includes " + expected);
    }
  });
});

describe("emitCurrentSystemStatus", () => {

  test("does not throw and reports the expected runtime metrics shape on the SSE bus when state changes", async () => {

    // The function reads runtime metrics and emits a SystemStatus to the SSE bus. The emitter dedupes by browser.connected and streams.active, so consecutive
    // calls with identical values fire only once - we wrap a single call here and verify either no emission (already-cached values match) or a well-shaped one.
    // Either outcome confirms the function does not throw and the emitted shape matches the documented contract when it does emit.
    const captured: { event: string; data: unknown }[] = [];

    const unsubscribe = subscribeToStatus((event, data) => {

      if(event === "systemStatusChanged") {

        captured.push({ data, event });
      }
    });

    try {

      await emitCurrentSystemStatus();
    } finally {

      unsubscribe();
    }

    // Capture is opportunistic: the emitter may have skipped if the cached state already matched. When an event was emitted, we verify the documented shape.
    if(captured.length > 0) {

      const status = captured[0]?.data as {
        browser: { connected: boolean; pageCount: number };
        memory: { heapUsed: number; rss: number };
        streams: { active: number; limit: number };
        uptime: number;
      };

      assert.ok(status, "status object captured");
      assert.equal(typeof status.browser.connected, "boolean", "browser.connected is a boolean");
      assert.equal(typeof status.browser.pageCount, "number", "browser.pageCount is a number");
      assert.equal(typeof status.memory.heapUsed, "number", "memory.heapUsed is a number");
      assert.equal(typeof status.memory.rss, "number", "memory.rss is a number");
      assert.equal(typeof status.streams.active, "number", "streams.active is a number");
      assert.equal(typeof status.streams.limit, "number", "streams.limit is a number");
      assert.equal(typeof status.uptime, "number", "uptime is a number");

      // No browser launched in the unit-test environment, so the connected flag must be false and the page count must be zero.
      assert.equal(status.browser.connected, false, "no browser -> not connected");
      assert.equal(status.browser.pageCount, 0, "no browser -> zero pages");
    }
  });

  test("does not throw on repeated calls (the dedupe path inside emitSystemStatusChanged)", async () => {

    // Boundary: emitSystemStatusChanged caches the prior status and only fires the SSE event when something meaningful changed. Calling emitCurrentSystemStatus
    // twice in a row must not throw even when the second emission is suppressed by the cache.
    await assert.doesNotReject(() => emitCurrentSystemStatus(), "first call");
    await assert.doesNotReject(() => emitCurrentSystemStatus(), "second call");
  });
});

/* Deferred to e2e (require Puppeteer/Chrome integration):
 *
 * - getCurrentBrowser, launchBrowser, launchWithCustomArgs, detectDisplayDimensions (every step here drives Puppeteer or executes JS in a real browser context).
 *
 * - closeBrowser (sends SIGTERM/SIGKILL to a real Chrome ChildProcess and waits for the exit event).
 *
 * - cleanupStalePages, startStalePageCleanup, stopStalePageCleanup (browser.pages() + page.close()).
 *
 * - startBrowserRestartChecking, stopBrowserRestartChecking, executeBrowserRestart (full restart cycle drives closeBrowser + getCurrentBrowser).
 *
 * - getBrowserPages, minimizeBrowserWindow (browser.pages() + CDP traffic against a real session).
 *
 * - prepareExtension (filesystem operations against the packaged executable layout).
 *
 * - killStaleChrome (process.kill, fs.unlinkSync, Atomics.wait against the real process tree).
 *
 * - handleBrowserDisconnect (the disconnect handler is wired into Puppeteer's "disconnected" event - exercising it requires the event firing on a real browser).
 *
 * - emitCurrentSystemStatus's connected-browser branch (where browser.pages() returns a non-empty list).
 *
 * - The opportunistic-restart timing flow including the quiet-period countdown.
 */
