/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.test.ts: Unit tests for the testable, non-Chrome-driving pieces of browser/index.ts. The module is dominated by Chrome lifecycle code (launchReadyBrowser,
 * detectDisplayDimensions, cleanupStalePages, executeBrowserRestart, prepareExtension), which all require Puppeteer integration and are deferred to e2e.
 *
 * The unit tests here cover the synchronous accessor surface that does not touch Chrome:
 *
 *   - isGracefulShutdown / setGracefulShutdown (the boolean flag the disconnect handler reads)
 *   - registerManagedPage / unregisterManagedPage (the WeakMap-backed page-registration tracker)
 *   - getChromeVersion (the cached version string accessor)
 *   - getBrowserInstance / isBrowserConnected (the synchronous status accessors)
 *   - findChromeProcessesUsingProfile (the pure discovery filter killStaleChrome composes)
 *   - buildLaunchOptions (the launch-option assembly that reads CONFIG)
 *   - getExecutablePath (the env-var-or-search executable resolver)
 *   - emitCurrentSystemStatus (the status emitter wrapper - we drain the resulting SSE event)
 *   - seedProfilePreferences (the profile Preferences merge that enables Chrome's extension developer mode)
 *
 * Importing this module pulls in puppeteer-stream which starts a WebSocketServer at evaluation time. The test runner uses --test-force-exit so that handle does
 * not prevent the file from exiting cleanly.
 */
import { afterEach, before, beforeEach, describe, test } from "node:test";
import { buildLaunchOptions, emitCurrentSystemStatus, ensureDataDirectory, findChromeProcessesUsingProfile, getBrowserInstance, getChromeVersion,
  getExecutablePath, isBrowserConnected, isGracefulShutdown, registerManagedPage, seedProfilePreferences, setGracefulShutdown,
  unregisterManagedPage } from "./index.ts";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { CONFIG } from "../config/index.ts";
import { LOG } from "../utils/index.ts";
import type { Page } from "puppeteer-core";
import assert from "node:assert/strict";
import { initializeDataDir } from "../config/paths.ts";
import os from "node:os";
import path from "node:path";
import { subscribeToStatus } from "../streaming/statusEmitter.ts";
import { withTempDir } from "../testing.helpers.ts";

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

  test("registering with the in-flight setup option round-trips through unregister", () => {

    // Stream setup registers its page with the in-flight option so stale page cleanup leaves it alone until the registry records the ownership. The option adds
    // membership in the in-flight collection; what we lock here is that the flagged registration and its unregister are non-throwing, since the collections are
    // private and the behavioral pins for the exemption live in pageStaleness.test.ts.
    const page = fakePage();

    assert.doesNotThrow(() => {

      registerManagedPage(page, { inFlightSetup: true });
    }, "flagged registration is non-throwing");

    assert.doesNotThrow(() => {

      unregisterManagedPage(page);
    }, "unregistering a flagged page is non-throwing");
  });

  test("a second unregister on an in-flight-registered page is a no-op", () => {

    // Boundary: the flagged path must clean up on the first unregister exactly like the unflagged one, so the second call finds nothing and exits early.
    const page = fakePage();

    registerManagedPage(page, { inFlightSetup: true });
    unregisterManagedPage(page);

    assert.doesNotThrow(() => {

      unregisterManagedPage(page);
    }, "second unregister on an already-unregistered flagged page is a no-op");
  });
});

describe("getChromeVersion", () => {

  test("returns null when no browser has been launched in this process", () => {

    // The cached version is set inside launchReadyBrowser() after a successful launch and cleared on disconnect. With no browser launched, it stays null.
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

    // The check reads supervisor.current() and returns `!!browser && browser.connected` - both clauses fail when no browser is published.
    assert.equal(isBrowserConnected(), false, "no launched browser -> not connected");
  });
});

describe("findChromeProcessesUsingProfile", () => {

  // A live-PID predicate that treats every PID in a set as alive. Tests parameterize the set to drive the ppid liveness branch deterministically.
  const aliveSet = (pids: ReadonlySet<number>) => (pid: number): boolean => pids.has(pid);

  test("matches Chrome processes whose command line carries our --user-data-dir flag", () => {

    const processes = [

      { commandLine: "/usr/sbin/syslogd", pid: 1, ppid: 0 },
      { commandLine: "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome --user-data-dir=/home/x/.prismcast/chromedata --no-sandbox",
        pid: 100, ppid: 50 },
      { commandLine: "node /app/dist/index.js", pid: 50, ppid: 1 }
    ];

    // Our process is PID 50 (the node parent of Chrome). Chrome (PID 100) has ppid 50 = us, so ownership is ours. SIGTERM.
    assert.deepEqual(findChromeProcessesUsingProfile(processes, "/home/x/.prismcast/chromedata", 50, aliveSet(new Set([50]))), [100]);
  });

  test("ignores processes whose command line does not reference our profile dir", () => {

    const processes = [

      { commandLine: "/usr/bin/chrome --user-data-dir=/some/other/path", pid: 200, ppid: 1 }
    ];

    // The match is by profile dir. A Chrome instance for a different profile is none of our business.
    assert.deepEqual(findChromeProcessesUsingProfile(processes, "/home/x/.prismcast/chromedata", 50, aliveSet(new Set())), []);
  });

  test("excludes Chrome whose parent is a live unrelated PID (rejected-duplicate safety)", () => {

    // A legitimate PrismCast (PID 999) owns a Chrome (PID 100) with our profile. We are a rejected duplicate (PID 50). Chrome 100's ppid is 999, which is
    // alive and is not us - so it belongs to 999, not us, and we must leave it alone.
    const processes = [

      { commandLine: "/usr/bin/chrome --user-data-dir=/home/x/.prismcast/chromedata", pid: 100, ppid: 999 }
    ];

    assert.deepEqual(findChromeProcessesUsingProfile(processes, "/home/x/.prismcast/chromedata", 50, aliveSet(new Set([999]))), []);
  });

  test("includes orphaned Chrome whose parent is no longer alive", () => {

    // A previous PrismCast died without killing its Chrome. The OS reparented Chrome to init (or it has a dead-PID parent on Windows). We launched fresh and
    // claimed the instance slot; this orphan needs to go before we spawn our own Chrome.
    const processes = [

      { commandLine: "/usr/bin/chrome --user-data-dir=/home/x/.prismcast/chromedata", pid: 100, ppid: 1 }
    ];

    // PID 1 (init) is alive but is not us; without further info this would be excluded. The test injects ppid=1 as NOT alive in the predicate, which captures
    // the typical orphan signature on systems where the original parent died and the kernel reparented to init. The discovery filter trusts the predicate.
    assert.deepEqual(findChromeProcessesUsingProfile(processes, "/home/x/.prismcast/chromedata", 50, aliveSet(new Set())), [100]);
  });

  test("rejects substring matches that extend past the profile dir (no false positives on path prefixes)", () => {

    // The user-data-dir flag's value is "/home/x/data-backup". Our profile dir is "/home/x/data". Naive substring matching would treat this as a hit; the
    // boundary check (next char must be whitespace or end-of-string) rejects it.
    const processes = [

      { commandLine: "/usr/bin/chrome --user-data-dir=/home/x/data-backup --no-sandbox", pid: 100, ppid: 50 }
    ];

    assert.deepEqual(findChromeProcessesUsingProfile(processes, "/home/x/data", 50, aliveSet(new Set([50]))), []);
  });

  test("accepts the match when the user-data-dir is the last argument (end-of-string boundary)", () => {

    const processes = [

      { commandLine: "/usr/bin/chrome --user-data-dir=/home/x/data", pid: 100, ppid: 50 }
    ];

    assert.deepEqual(findChromeProcessesUsingProfile(processes, "/home/x/data", 50, aliveSet(new Set([50]))), [100]);
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

    // The ignoreDefaultArgs list must include the three critical entries. Locks the documented choices.
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

describe("ensureDataDirectory legacy-artifact purge", () => {

  test("removes a pre-existing chrome.pid file (legacy artifact from PrismCast < 1.10.3)", async () => {

    // Simulate an upgraded install: the user has a stale chrome.pid file from a previous version. We create one in the tempDataDir, run ensureDataDirectory,
    // and assert the file is gone. This is the masterclass test for "don't litter on upgrade" - the purge happens transparently as part of normal startup.
    const legacyChromePid = path.join(tempDataDir, "chrome.pid");

    writeFileSync(legacyChromePid, "12345");

    assert.equal(existsSync(legacyChromePid), true, "sentinel chrome.pid is in place before ensureDataDirectory runs");

    await ensureDataDirectory();

    assert.equal(existsSync(legacyChromePid), false, "ensureDataDirectory purged the legacy chrome.pid file");
  });

  test("is a no-op when chrome.pid is absent (steady state)", async () => {

    // The common case after the first purge: ensureDataDirectory runs cleanly with no chrome.pid to remove. ENOENT is silenced inside the purge helper, so
    // there is nothing to assert beyond "does not throw."
    const legacyChromePid = path.join(tempDataDir, "chrome.pid");

    if(existsSync(legacyChromePid)) {

      rmSync(legacyChromePid, { force: true });
    }

    await assert.doesNotReject(async () => ensureDataDirectory());
  });
});

describe("seedProfilePreferences", () => {

  test("creates the Default directory and a seeded Preferences file on a profile Chrome has never launched", async () => {

    // The fresh-install case: no Default directory, no Preferences file. The seed has to create both, because Chrome reads the profile before it writes one and
    // an unpacked extension is refused without the flag already in place.
    await withTempDir(async (dir) => {

      const profileDir = path.join(dir, "chromedata");

      seedProfilePreferences(profileDir);

      const preferencesPath = path.join(profileDir, "Default", "Preferences");

      assert.equal(existsSync(preferencesPath), true, "the seed created the Preferences file");

      const written = JSON.parse(readFileSync(preferencesPath, "utf8")) as { extensions: { ui: { developer_mode: boolean } } };

      assert.equal(written.extensions.ui.developer_mode, true);
    });
  });

  test("preserves every existing preference when it seeds the flag", () => {

    // The upgrade case: a profile that has been in use carries the user's whole Chrome configuration, including an extensions branch of its own. Losing any of
    // it would silently reset the user's browser, so the seed merges rather than replaces.
    const profileDir = path.join(tempDataDir, "merge-profile");

    mkdirSync(path.join(profileDir, "Default"), { recursive: true });
    writeFileSync(path.join(profileDir, "Default", "Preferences"), JSON.stringify({

      extensions: { settings: { abcdef: { state: 1 } }, ui: { "other_flag": 7 } },
      profile: { name: "Person 1" }
    }));

    seedProfilePreferences(profileDir);

    const written = JSON.parse(readFileSync(path.join(profileDir, "Default", "Preferences"), "utf8")) as {
      extensions: { settings: { abcdef: { state: number } }; ui: { developer_mode: boolean; other_flag: number } };
      profile: { name: string };
    };

    assert.equal(written.extensions.ui.developer_mode, true, "the flag was seeded");
    assert.equal(written.extensions.ui.other_flag, 7, "sibling keys inside extensions.ui survive");
    assert.equal(written.extensions.settings.abcdef.state, 1, "sibling branches inside extensions survive");
    assert.equal(written.profile.name, "Person 1", "unrelated top-level branches survive");
  });

  test("does not rewrite the file when the flag is already set", () => {

    // Every launch calls the seed, so the steady state has to cost nothing. We write the file in a shape JSON.stringify would never reproduce (indented, with a
    // key order the seed does not preserve) and assert the bytes come back identical - a rewrite of any kind would normalize them.
    const profileDir = path.join(tempDataDir, "already-seeded-profile");
    const preferencesPath = path.join(profileDir, "Default", "Preferences");
    const original = JSON.stringify({ extensions: { ui: { "developer_mode": true } } }, null, 2);

    mkdirSync(path.join(profileDir, "Default"), { recursive: true });
    writeFileSync(preferencesPath, original);

    seedProfilePreferences(profileDir);

    assert.equal(readFileSync(preferencesPath, "utf8"), original, "an already-seeded profile is left byte-identical");
  });

  test("warns and leaves the file alone when the Preferences file is not parseable", (t) => {

    // A truncated or corrupt Preferences file must never take a launch down with it. Chrome regenerates a file it cannot parse, so the right response is one
    // warning and no write...the next launch seeds the replacement.
    const warn = t.mock.method(LOG, "warn", () => { /* Captured via the mock. */ });
    const profileDir = path.join(tempDataDir, "corrupt-profile");
    const preferencesPath = path.join(profileDir, "Default", "Preferences");

    mkdirSync(path.join(profileDir, "Default"), { recursive: true });
    writeFileSync(preferencesPath, "{ this is not json");

    assert.doesNotThrow(() => seedProfilePreferences(profileDir));

    assert.equal(readFileSync(preferencesPath, "utf8"), "{ this is not json", "the unparseable file is left untouched");
    assert.equal(warn.mock.callCount(), 1, "exactly one warning is emitted");
  });

  test("warns and leaves the file alone when the Preferences file parses to something other than an object", (t) => {

    // JSON.parse happily returns null, a number, or an array for a file that is valid JSON but not a settings object. Merging into any of those is impossible,
    // so the seed treats them the same way it treats corruption.
    const warn = t.mock.method(LOG, "warn", () => { /* Captured via the mock. */ });
    const profileDir = path.join(tempDataDir, "array-profile");
    const preferencesPath = path.join(profileDir, "Default", "Preferences");

    mkdirSync(path.join(profileDir, "Default"), { recursive: true });
    writeFileSync(preferencesPath, "[]");

    assert.doesNotThrow(() => seedProfilePreferences(profileDir));

    assert.equal(readFileSync(preferencesPath, "utf8"), "[]", "the unusable file is left untouched");
    assert.equal(warn.mock.callCount(), 1, "exactly one warning is emitted");
  });

  test("replaces a non-object extensions branch rather than abandoning the seed", () => {

    // Defensive: a Preferences file whose extensions key holds a scalar cannot be descended into. Chrome never writes such a file, but the seed still has to
    // reach its target rather than throw on a property assignment against a primitive.
    const profileDir = path.join(tempDataDir, "scalar-branch-profile");
    const preferencesPath = path.join(profileDir, "Default", "Preferences");

    mkdirSync(path.join(profileDir, "Default"), { recursive: true });
    writeFileSync(preferencesPath, JSON.stringify({ extensions: "unexpected", profile: { name: "Person 1" } }));

    assert.doesNotThrow(() => seedProfilePreferences(profileDir));

    const written = JSON.parse(readFileSync(preferencesPath, "utf8")) as { extensions: { ui: { developer_mode: boolean } }; profile: { name: string } };

    assert.equal(written.extensions.ui.developer_mode, true, "the flag was seeded into a rebuilt extensions branch");
    assert.equal(written.profile.name, "Person 1", "unrelated branches still survive");
  });
});

/* Deferred to e2e (require Puppeteer/Chrome integration):
 *
 * - getCurrentBrowser, launchReadyBrowser, launchWithCustomArgs, detectDisplayDimensions (every step here drives Puppeteer or executes JS in a real browser context).
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
