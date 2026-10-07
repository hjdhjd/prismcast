/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.test.ts: Unit tests for the testable, non-Chrome-driving pieces of browser/index.ts. The module is dominated by Chrome lifecycle code (launchReadyBrowser,
 * detectBrowserCapabilities, cleanupStalePages, executeBrowserRestart, prepareExtension), which all require Puppeteer integration and are deferred to e2e.
 *
 * The unit tests here cover the synchronous accessor surface that does not touch Chrome:
 *
 *   - isGracefulShutdown / setGracefulShutdown (the boolean flag the disconnect handler reads)
 *   - registerManagedPage / unregisterManagedPage (the WeakMap-backed page-registration tracker)
 *   - getChromeVersion (the cached version string accessor)
 *   - getBrowserInstance / getCaptureImpairment / isBrowserConnected (the synchronous status accessors)
 *   - findChromeProcessesUsingProfile (the pure discovery filter killStaleChrome composes)
 *   - findProfileHolder (the pure holder test killStaleChrome's lock-file removal reads)
 *   - cleanStaleProfileFiles (the one lock-file removal the sweep and the teardown reach, held to the holder test, driven on a temp profile directory)
 *   - buildLaunchOptions (the launch-option assembly that reads CONFIG)
 *   - emulateCaptureSurface (the per-capture-page surface declaration, driven through a recording page double)
 *   - emulateLayoutSurface (the per-layout-page surface declaration, driven through the same double)
 *   - reaffirmCaptureSurface (the re-issue of a capture page's standing surface declaration, driven through a recording double that reports the standing record)
 *   - pickCarrierPage / isCarrierPage (the one rule for a page whose session may carry a command aimed at the shared window)
 *   - noteSharedWindow / resolveSharedWindowCarrier / confirmSharedWindowPlacement (the shared window's recorded identity, and the two topology answers the
 *     tab-selection executor is given so an opener-anchored tab can be placed and confirmed)
 *   - mirrorPlacement (the pure derivation from a window's placement to the bounds a window opened beside it is created with)
 *   - createDiscoveryPage (the own-window guide page, driven through a recording browser double)
 *   - makeFocusReaffirmCallback (the pure factory behind the tab-activation heal, driven with an injected re-issue and an injected clock)
 *   - healActivatedCaptureTab (the heal's report-side trigger, driven against pages enrolled through installActivationHeal with injected collaborators)
 *   - getExecutablePath (the env-var-or-search executable resolver)
 *   - emitCurrentSystemStatus (the status emitter wrapper - we drain the resulting SSE event)
 *   - seedProfilePreferences (the profile Preferences merge that enables Chrome's extension developer mode)
 *   - isBrowserIdleForRestart (the pure cause-specific idleness decision both restart guards read)
 *   - applyStalePageCleanupChanges (a saved cleanup interval re-arming the running sweep on the clock its start received, and arming nothing while it is stopped)
 *
 * Importing this module pulls in puppeteer-stream which starts a WebSocketServer at evaluation time. The test runner uses --test-force-exit so that handle does
 * not prevent the file from exiting cleanly.
 */
import type { Browser, Page } from "puppeteer-core";
import type { Config, Nullable, StreamingMode } from "../types/index.ts";
import { TestClock, drainClock } from "homebridge-plugin-utils/testing";
import { afterEach, before, beforeEach, describe, test } from "node:test";
import { applyStalePageCleanupChanges, buildLaunchOptions, cleanStaleProfileFiles, confirmSharedWindowPlacement, createDiscoveryPage, emitCurrentSystemStatus,
  emulateCaptureSurface, emulateLayoutSurface, ensureDataDirectory, findChromeProcessesUsingProfile, findProfileHolder, getBrowserInstance, getCaptureImpairment,
  getChromeVersion, getExecutablePath, healActivatedCaptureTab, installActivationHeal, isBrowserConnected, isBrowserIdleForRestart, isCarrierPage, isGracefulShutdown,
  makeFocusReaffirmCallback, mirrorPlacement, noteSharedWindow, pickCarrierPage, reaffirmCaptureSurface, registerManagedPage, resolveSharedWindowCarrier,
  seedProfilePreferences, setGracefulShutdown, startBrowserRestartChecking, startStalePageCleanup, stopBrowserRestartChecking, stopStalePageCleanup,
  unregisterManagedPage } from "./index.ts";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { firstOf, withTempDir } from "../testing.helpers.ts";
import { CONFIG } from "../config/index.ts";
import type { Clock } from "homebridge-plugin-utils";
import type { ConfigChange } from "../config/reactivity.ts";
import { LOG } from "../utils/index.ts";
import type { StreamRegistryEntry } from "../streaming/registry.ts";
import { applyConfigChanges } from "../config/reactivity.ts";
import assert from "node:assert/strict";
import { getPresetViewport } from "../config/presets.ts";
import { setImmediate as immediate } from "node:timers/promises";
import { initializeDataDir } from "../config/paths.ts";
import { makeNativeIdentity } from "../streaming/registry.helpers.ts";
import { makePendingCaptureIdentity } from "../streaming/registry.ts";
import os from "node:os";
import path from "node:path";
import { subscribeToStatus } from "../streaming/statusEmitter.ts";
import { systemClock } from "homebridge-plugin-utils";

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

// Exactly the device-metrics fields emulateCaptureSurface declares, so a row can assert the whole override with one deepEqual rather than field by field.
interface DeclaredSurface {

  deviceScaleFactor: number;
  height: number;
  width: number;
}

/* The page double the capture-surface rows run against: evaluate answers with whatever the row's display reports (or rejects, or stays pending on a deferred),
 * and setViewport records what was declared, in call order, so a row can read both the count and the content. This file's literal-double convention covers it;
 * a single consumer does not earn a shared helper.
 * @param evaluate - The reader standing in for the page's devicePixelRatio read.
 * @returns The recorded declarations and the page to hand to emulateCaptureSurface.
 */
function makeCapturePage(evaluate: () => Promise<number>): { declared: DeclaredSurface[]; page: Page } {

  const declared: DeclaredSurface[] = [];

  return {

    declared,
    page: { evaluate, setViewport: async (viewport: DeclaredSurface): Promise<void> => { declared.push(viewport); } } as unknown as Page
  };
}

/* The readings a page can hand back that are not usable scale factors, each with the check that identifies it inside the recorded warning: NaN and a missing
 * value need a predicate rather than an equality, while zero and a negative are exact.
 */
const UNUSABLE_DENSITIES: readonly { assertReported: (reported: unknown) => void; label: string; reported: number }[] = [

  { assertReported: (reported): void => assert.ok(Number.isNaN(reported), "the warning names the NaN reading"), label: "NaN", reported: NaN },
  { assertReported: (reported): void => assert.equal(reported, 0, "the warning names the zero reading"), label: "a density of zero", reported: 0 },
  { assertReported: (reported): void => assert.equal(reported, -1, "the warning names the negative reading"), label: "a negative density", reported: -1 },
  { assertReported: (reported): void => assert.equal(reported, undefined, "the warning names the missing reading"), label: "no density at all",
    reported: undefined as unknown as number }
];

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

  test("repeated identical writes change nothing", () => {

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

  test("registering a page does not throw and a repeat registration of the same page reference changes nothing", () => {

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

  test("a second unregister on the same page is a no-op (repeat-safe cleanup)", () => {

    // After the first unregister removes the entry, the second call's WeakMap.get returns undefined and the early-exit guard fires. Locks the contract.
    const page = fakePage();

    registerManagedPage(page);
    unregisterManagedPage(page);

    assert.doesNotThrow(() => {

      unregisterManagedPage(page);
    }, "second unregister on already-unregistered page is a no-op");
  });

  test("registering with the in-flight option round-trips through unregister", () => {

    // An operation that owns a page for its duration - a stream setup, a discovery walk - registers it with the in-flight option so stale page cleanup leaves it
    // alone for that window. The option adds membership in the in-flight collection; what we lock here is that the flagged registration and its unregister are
    // non-throwing, since the collections are private and the behavioral assertions for the exemption live in pageStaleness.test.ts.
    const page = fakePage();

    assert.doesNotThrow(() => {

      registerManagedPage(page, { inFlight: true });
    }, "flagged registration is non-throwing");

    assert.doesNotThrow(() => {

      unregisterManagedPage(page);
    }, "unregistering a flagged page is non-throwing");
  });

  test("a second unregister on an in-flight-registered page is a no-op", () => {

    // Boundary: the flagged path must clean up on the first unregister exactly like the unflagged one, so the second call finds nothing and exits early.
    const page = fakePage();

    registerManagedPage(page, { inFlight: true });
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

describe("getCaptureImpairment", () => {

  test("returns null when no browser has been launched", () => {

    // The read derives from the supervisor's ready state, which holds the only place an impairment can live, so with nothing published there is nothing to report.
    assert.equal(getCaptureImpairment(), null, "no launched browser -> no impairment");
  });
});

describe("isBrowserConnected", () => {

  test("returns false when no browser has been launched", () => {

    // The check reads supervisor.current() and returns `!!browser && browser.connected` - both clauses fail when no browser is published.
    assert.equal(isBrowserConnected(), false, "no launched browser -> not connected");
  });
});

// A live-PID predicate that treats every PID in a set as alive. Tests parameterize the set to drive the ppid liveness branch deterministically.
const aliveSet = (pids: ReadonlySet<number>) => (pid: number): boolean => pids.has(pid);

describe("findChromeProcessesUsingProfile", () => {

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

describe("findProfileHolder", () => {

  // Every row's profile, with the command lines of a Chrome main and its helpers on it. The tables list a main before its helpers, each helper carrying the
  // profile flag with the main as its parent, the shape a live Chrome tree takes on macOS. Our own process is PID 50 throughout.
  const profile = "/home/x/.prismcast/chromedata";
  const mainCommand = "/usr/bin/chrome --remote-debugging-pipe --user-data-dir=" + profile;
  const gpuCommand = "/usr/bin/chrome --type=gpu-process --user-data-dir=" + profile;
  const rendererCommand = "/usr/bin/chrome --type=renderer --user-data-dir=" + profile;

  test("returns the main of a live holder's Chrome tree", () => {

    // A legitimate PrismCast (PID 999) runs a Chrome tree on our profile. Its main's parent is alive and is not us, so the main holds the profile; its helpers'
    // parent, the main, is on the profile too, so no helper reads as a root of its own.
    const processes = [

      { commandLine: "node /app/dist/index.js", pid: 999, ppid: 1 },
      { commandLine: mainCommand, pid: 100, ppid: 999 },
      { commandLine: gpuCommand, pid: 101, ppid: 100 },
      { commandLine: rendererCommand, pid: 102, ppid: 100 }
    ];

    assert.equal(findProfileHolder(processes, profile, 50, aliveSet(new Set([ 1, 999, 100, 101, 102 ]))), 100);
  });

  test("returns null for an orphaned Chrome tree whose parent is no longer alive", () => {

    // The PrismCast that launched this tree died, so the kill filter ends the main and nothing holds the profile. A helper's parent, the main, is alive, which
    // is why the test reads only the tree's root.
    const processes = [

      { commandLine: mainCommand, pid: 100, ppid: 999 },
      { commandLine: gpuCommand, pid: 101, ppid: 100 },
      { commandLine: rendererCommand, pid: 102, ppid: 100 }
    ];

    assert.equal(findProfileHolder(processes, profile, 50, aliveSet(new Set([ 100, 101, 102 ]))), null);
  });

  test("returns null for this process's own Chrome tree", () => {

    // We launched this tree, so its main's parent is us and the kill filter ends it. Its helpers' parent is a live process that is not us, which reads as a
    // holder to a liveness test alone.
    const processes = [

      { commandLine: mainCommand, pid: 100, ppid: 50 },
      { commandLine: gpuCommand, pid: 101, ppid: 100 },
      { commandLine: rendererCommand, pid: 102, ppid: 100 }
    ];

    assert.equal(findProfileHolder(processes, profile, 50, aliveSet(new Set([ 50, 100, 101, 102 ]))), null);
  });

  test("returns null when no Chrome runs on the profile", () => {

    const processes = [

      { commandLine: "node /app/dist/index.js", pid: 999, ppid: 1 },
      { commandLine: "/usr/bin/chrome --user-data-dir=/some/other/path", pid: 200, ppid: 999 }
    ];

    assert.equal(findProfileHolder(processes, profile, 50, aliveSet(new Set([ 200, 999 ]))), null);
  });

  test("returns the first helper of a live tree whose main lacks the profile flag", () => {

    // A Chromium app can launch its main without the flag and hand it to its helpers. The first helper's parent is then off the profile, so the helper reads
    // as the tree's root and holds the profile, which keeps the lock files rather than removing them.
    const processes = [

      { commandLine: "/usr/bin/chrome", pid: 100, ppid: 1 },
      { commandLine: gpuCommand, pid: 101, ppid: 100 },
      { commandLine: rendererCommand, pid: 102, ppid: 100 }
    ];

    assert.equal(findProfileHolder(processes, profile, 50, aliveSet(new Set([ 1, 100, 101, 102 ]))), 101);
  });
});

describe("cleanStaleProfileFiles", () => {

  // Every row seeds the lock files and the DevTools port file in a profile under a fresh temp directory and hands the removal a literal process table. The
  // removal reads our own PID and the liveness of a root's parent from the host, so the root of a tree on the profile takes our own parent as its parent, a
  // live process that is not us, and no other PID's liveness is read.
  const profileFiles = [ "DevToolsActivePort", "SingletonCookie", "SingletonLock", "SingletonSocket" ];

  const seedProfile = (dir: string): string => {

    const profileDir = path.join(dir, "chromedata");

    mkdirSync(profileDir);

    for(const file of profileFiles) {

      writeFileSync(path.join(profileDir, file), "");
    }

    return profileDir;
  };

  test("keeps the lock files when a live Chrome outside this process holds the profile", async () => {

    // The scan a teardown reads once its own Chrome has exited, with another instance's Chrome launched on the shared profile in its place. That main's parent
    // is alive and is not us, so the main holds the profile and the files are its own.
    await withTempDir(async (dir) => {

      const profileDir = seedProfile(dir);
      const processes = [

        { commandLine: "/usr/bin/chrome --remote-debugging-pipe --user-data-dir=" + profileDir, pid: 100, ppid: process.ppid },
        { commandLine: "/usr/bin/chrome --type=gpu-process --user-data-dir=" + profileDir, pid: 101, ppid: 100 },
        { commandLine: "/usr/bin/chrome --type=renderer --user-data-dir=" + profileDir, pid: 102, ppid: 100 }
      ];

      cleanStaleProfileFiles(processes, profileDir);

      for(const file of profileFiles) {

        assert.equal(existsSync(path.join(profileDir, file)), true, file + " is kept for the live holder");
      }
    });
  });

  test("removes the lock files when no Chrome runs on the profile", async () => {

    // The scan a teardown reads once its own Chrome has exited and no other Chrome has taken the profile, so nothing holds it and the files are stale.
    await withTempDir(async (dir) => {

      const profileDir = seedProfile(dir);
      const processes = [

        { commandLine: "/usr/bin/chrome --user-data-dir=/some/other/path", pid: 200, ppid: process.ppid }
      ];

      cleanStaleProfileFiles(processes, profileDir);

      for(const file of profileFiles) {

        assert.equal(existsSync(path.join(profileDir, file)), false, file + " is removed");
      }
    });
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

  test("declares no launch viewport at all, as an explicit null", () => {

    /* The null disables two derivations at once. Puppeteer reads an absent option as its own 800x600 default and applies that override to every page it creates,
     * so a page would carry an emulation nothing asked for; and puppeteer-stream reads a sized default to push Chrome's --window-size and
     * --ozone-override-screen-size flags, where the window-size flag is what stops Chrome restoring the placement it persisted. Undefined would silently be a
     * viewport, so the assertion is on the null itself rather than on the key's absence.
     */
    assert.equal(buildLaunchOptions().defaultViewport, null, "no page-wide emulation and no window-dimension flags are derived from the launch");
  });

  test("runs Chrome in headed mode so the streaming extension can capture", () => {

    // The streaming extension requires a visible window for the compositor capture path. The launch must be headed.
    assert.equal(buildLaunchOptions().headless, false, "headless: false");
  });

  test("uses pipe mode for the DevTools Protocol connection", () => {

    // Pipe mode is faster and more reliable than WebSocket under load. Locks the documented choice.
    assert.equal(buildLaunchOptions().pipe, true, "pipe: true");
  });

  test("turns off the launcher's own SIGHUP, SIGINT, and SIGTERM listeners so PrismCast owns process signals", () => {

    /* All three are asserted because each one hands a different signal back to PrismCast's handlers, and a launch that carried only two would still let the third
     * signal end Chrome's process group with SIGKILL before the graceful path could run. Asserting false rather than the keys' absence is the point: the launcher
     * defaults every one of them to true, so an omitted key is silently the wrong behavior.
     */
    const options = buildLaunchOptions();

    assert.equal(options.handleSIGHUP, false, "handleSIGHUP: false");
    assert.equal(options.handleSIGINT, false, "handleSIGINT: false");
    assert.equal(options.handleSIGTERM, false, "handleSIGTERM: false");
  });

  test("includes the autoplay-no-user-gesture-required flag in the args list", () => {

    // The autoplay flag is critical for unattended capture. We assert it is present in the args.
    const args = buildLaunchOptions().args ?? [];

    assert.ok(args.includes("--autoplay-policy=no-user-gesture-required"), "autoplay flag present");
  });

  test("carries no manual window-size flag, leaving the window's dimensions to Chrome", () => {

    // The window's size and placement are Chrome's and the user's, restored from the profile. A window-size flag assembled here would override both, and with the
    // null viewport deriving none, this is the one place such a flag could enter the launch.
    const args = buildLaunchOptions().args ?? [];

    assert.equal(args.filter((a) => a.startsWith("--window-size=")).length, 0, "no window-size arg assembled here");
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

describe("emulateCaptureSurface", () => {

  test("declares the preset's dimensions at the pixel density the page reports", async () => {

    /* The row that tells a real read apart from a hardcoded declaration: the page reports a density of 2, so the override has to carry 2 - an implementation
     * declaring NATIVE_DENSITY, or any literal, fails here - at exactly the configured preset's dimensions, and the caller gets those same dimensions back to
     * constrain capture with.
     */
    const { declared, page } = makeCapturePage(async (): Promise<number> => 2);
    const viewport = getPresetViewport(CONFIG);

    const surface = await emulateCaptureSurface(page);

    assert.deepEqual(declared, [{ deviceScaleFactor: 2, height: viewport.height, width: viewport.width }], "one override, at the preset size and the reported density");
    assert.deepEqual(surface, { height: viewport.height, width: viewport.width }, "the declared dimensions come back for the capture constraints");
  });

  test("declares a fractional density exactly as the page reported it", async () => {

    // Fractional scale factors are ordinary on Windows and Linux desktops, where 150% display scaling reports 1.5. Rounding or truncating to an integer would
    // declare a density the display does not have, which is the disagreement the explicit declaration exists to prevent.
    const { declared, page } = makeCapturePage(async (): Promise<number> => 1.5);

    await emulateCaptureSurface(page);

    assert.equal(declared[0]?.deviceScaleFactor, 1.5, "1.5 is declared unchanged");
  });

  for(const unusable of UNUSABLE_DENSITIES) {

    test("declares a density of 1 and warns once when the page reports " + unusable.label, async (t) => {

      /* A page that cannot report a usable density leaves nothing to measure, and the compositor still needs an explicit value...so the declaration falls back to
       * 1 and says so. The warning has to carry the reading it rejected, not just fire: an operator reading the log needs to know what the page said.
       */
      const warn = t.mock.method(LOG, "warn", () => { /* Captured via the mock. */ });
      const { declared, page } = makeCapturePage(async (): Promise<number> => unusable.reported);
      const viewport = getPresetViewport(CONFIG);

      const surface = await emulateCaptureSurface(page);

      assert.deepEqual(declared, [{ deviceScaleFactor: 1, height: viewport.height, width: viewport.width }], "the fallback still declares an explicit density");
      assert.deepEqual(surface, { height: viewport.height, width: viewport.width }, "the returned dimensions are the ones declared");
      assert.equal(warn.mock.callCount(), 1, "exactly one warning covers the fallback");

      const args = warn.mock.calls[0]?.arguments ?? [];

      unusable.assertReported(args[1]);
      assert.equal(args[2], 1, "the warning names the density that was declared instead");
    });
  }

  test("warns about nothing when the page reports a usable density", async (t) => {

    // The negative control for the fallback rows: a good reading is declared silently, so a warning on this path would mean the fallback branch is misfiring.
    const warn = t.mock.method(LOG, "warn", () => { /* Captured via the mock. */ });
    const { page } = makeCapturePage(async (): Promise<number> => 2);

    await emulateCaptureSurface(page);

    assert.equal(warn.mock.callCount(), 0, "a usable reading needs no warning");
  });

  test("declares nothing when the density read fails", async () => {

    // The read runs through evaluateWithAbort, so a terminated stream or an unresponsive page rejects here. The establishment unwinds through its DisposableStack
    // on that path; declaring a guessed density on a page we could not measure would be the wrong answer to a failed measurement.
    const failure = new Error("The page went away mid-read.");
    const { declared, page } = makeCapturePage(async (): Promise<number> => { throw failure; });

    await assert.rejects(emulateCaptureSurface(page), (error: Error): boolean => error === failure, "the read's failure reaches the caller");
    assert.equal(declared.length, 0, "no override is declared when the density is unknown");
  });

  test("reads the density before it declares the surface", async () => {

    /* Ordering is the whole mechanism: a declaration issued ahead of the read would re-declare the launch default, leaving the page reporting a density the
     * surface was never emulated at. Holding the read open on a deferred forces the question rather than leaving it to microtask coincidence.
     */
    const { promise, resolve } = Promise.withResolvers<number>();
    const { declared, page } = makeCapturePage(() => promise);
    const pending = emulateCaptureSurface(page);

    await immediate();

    assert.equal(declared.length, 0, "nothing is declared while the read is still pending");

    resolve(2);

    await pending;

    assert.deepEqual(declared.map((viewport) => viewport.deviceScaleFactor), [2], "the declaration lands exactly once, after the read resolved, at the density read");
  });

  test("derives the declared dimensions from a non-default preset rather than a fixed pair", async () => {

    /* The configured default preset and the getter's own fallback resolve to the same dimensions, so those numbers alone cannot tell a config-driven declaration
     * from a hardcoded one. Configuring 4K moves the expected dimensions away from both and makes the distinction observable.
     */
    const originalPreset = CONFIG.streaming.qualityPreset;

    CONFIG.streaming.qualityPreset = "4k";

    try {

      const { declared, page } = makeCapturePage(async (): Promise<number> => 2);

      const surface = await emulateCaptureSurface(page);

      assert.deepEqual(declared, [{ deviceScaleFactor: 2, height: 2160, width: 3840 }], "the 4K preset is declared at the reported density");
      assert.deepEqual(surface, { height: 2160, width: 3840 }, "the returned dimensions follow the preset");
    } finally {

      CONFIG.streaming.qualityPreset = originalPreset;
    }
  });
});

describe("emulateLayoutSurface", () => {

  test("declares the preset's dimensions at the display's own density", async () => {

    /* The layout declaration is the whole surface a page that is laid out but never captured needs: the preset's dimensions, with the density left at Chrome's
     * disable value so the display's own is what the page renders at. The declaration is read back rather than assumed, because the returned dimensions are what
     * the capture probe holds its constraints to.
     */
    const { declared, page } = makeCapturePage(async (): Promise<number> => 2);
    const viewport = getPresetViewport(CONFIG);

    const surface = await emulateLayoutSurface(page);

    assert.deepEqual(declared, [{ deviceScaleFactor: 0, height: viewport.height, width: viewport.width }],
      "one override, at the preset size with the density override disabled");
    assert.deepEqual(surface, { height: viewport.height, width: viewport.width }, "the declared dimensions come back for the caller to hold its capture to");
  });

  test("derives the declared dimensions from a non-default preset rather than a fixed pair", async () => {

    /* The configured default preset and the getter's own fallback resolve to the same dimensions, so those numbers alone cannot tell a config-driven declaration
     * from a hardcoded one. Configuring 4K moves the expected dimensions away from both and makes the distinction observable.
     */
    const originalPreset = CONFIG.streaming.qualityPreset;

    CONFIG.streaming.qualityPreset = "4k";

    try {

      const { declared, page } = makeCapturePage(async (): Promise<number> => 2);

      const surface = await emulateLayoutSurface(page);

      assert.deepEqual(declared, [{ deviceScaleFactor: 0, height: 2160, width: 3840 }], "the 4K preset is declared, still at the display's own density");
      assert.deepEqual(surface, { height: 2160, width: 3840 }, "the returned dimensions follow the preset");
    } finally {

      CONFIG.streaming.qualityPreset = originalPreset;
    }
  });
});

/* The page double the re-affirmation rows run against, in makeCapturePage's shape with a viewport reader beside the recording setter: viewport answers with the
 * standing record a row hands it, and setViewport records each declaration in call order, or rejects with the failure a row hands it. The page answers nothing
 * else, because the re-issue reads the standing record and declares it back, and that is all it does.
 * @param options - The standing record the page reports and the setter failure to simulate.
 * @param options.setViewportError - The error setViewport rejects with, when the row wants a failing declaration.
 * @param options.viewport - The record page.viewport() answers with. Null models a page carrying no emulation at all.
 * @returns The recorded declarations and the page to hand to reaffirmCaptureSurface.
 */
function makeReaffirmPage(options: { setViewportError?: Error; viewport: Nullable<DeclaredSurface> }): { declared: DeclaredSurface[]; page: Page } {

  const declared: DeclaredSurface[] = [];

  return {

    declared,
    page: {

      setViewport: async (viewport: DeclaredSurface): Promise<void> => {

        if(options.setViewportError) {

          throw options.setViewportError;
        }

        declared.push(viewport);
      },
      viewport: (): Nullable<DeclaredSurface> => options.viewport
    } as unknown as Page
  };
}

describe("reaffirmCaptureSurface", () => {

  test("re-issues the page's standing viewport once through the viewport setter", async () => {

    /* Deliberately not a preset size. No quality preset is 1400x788, so this declaration can only come from an implementation that re-declares the page's own
     * standing record - one that reached for the configured preset would declare a preset's dimensions and fail here.
     */
    const { declared, page } = makeReaffirmPage({ viewport: { deviceScaleFactor: 2, height: 788, width: 1400 } });

    await reaffirmCaptureSurface(page);

    assert.deepEqual(declared, [{ deviceScaleFactor: 2, height: 788, width: 1400 }], "one declaration, carrying exactly the standing record the page reports");
  });

  test("declares nothing for a page whose standing density is zero", async () => {

    // A layout page declares a density of 0, Chrome's disable value, which marks it as a page nobody captures, and the guard is what makes the re-issue safe to
    // fire at any page from any trigger.
    const { declared, page } = makeReaffirmPage({ viewport: { deviceScaleFactor: 0, height: 1080, width: 1920 } });

    await reaffirmCaptureSurface(page);

    assert.deepEqual(declared, [], "nothing is declared on a page carrying no explicit density");
  });

  test("declares nothing for a page carrying no viewport at all", async () => {

    // The launch declares no default viewport, so a page PrismCast never declares a surface on reports a null record. The record itself has to be tested, not only
    // the density it would carry.
    const { declared, page } = makeReaffirmPage({ viewport: null });

    await reaffirmCaptureSurface(page);

    assert.deepEqual(declared, [], "nothing is declared on an un-emulated page");
  });

  test("rejects with the viewport setter's own rejection", async () => {

    // Establishment's own re-affirmation runs on a resource stack that unwinds on any throw, so the failure has to reach the caller unaltered rather than being
    // swallowed here.
    const failure = new Error("synthetic viewport rejection");
    const { page } = makeReaffirmPage({ setViewportError: failure, viewport: { deviceScaleFactor: 2, height: 788, width: 1400 } });

    await assert.rejects(reaffirmCaptureSurface(page), (error: Error): boolean => error === failure, "the setter's own rejection reaches the caller");
  });
});

/* The two doubles the discovery-window rows share. A page here answers only what the carrier rule and the placement read actually call - whether it is closed,
 * and a CDP session serving the two commands readWindowPlacement issues - and it counts the bounds reads that session served, which is how a row tells the page
 * a placement was read from apart from the pages it was not. What the reader makes of a given response is asserted in cdp.test.ts; the count here is only about
 * which page was asked. A browser records the options of every creation and the pages it handed back.
 */
interface WindowPageStub {

  // The browser the page names as its own, which is the identity the placement confirmation reads its recorded window from. Set by the browser double below.
  browser: Nullable<Browser>;

  // How many times the page was closed, so a row can tell a page the creator cleaned up from one it handed back.
  closeCalls: number;

  // Every focus-emulation flag the page was handed, in order, so a row can assert what the creator did to the page it made.
  focusEmulations: boolean[];

  page: Page;

  // How many times the page's session was asked which window it sits in, so a row can tell a fresh lookup from a cached answer.
  windowLookups: number;
  windowReads: number;
}

/**
 * Builds a page double for the carrier, creator, and topology rows. A page given no bounds stands in for one whose bounds cannot be read at all, and one marked
 * unreadable stands in for a target that yields no window id - the answer a closed page and a CDP failure both come back as.
 * @param options - What the page reports about itself and what its CDP session answers with.
 * @param options.bounds - The bounds the page's session answers Browser.getWindowBounds with. Omitted means the response carries none.
 * @param options.closed - Whether the page reports itself closed.
 * @param options.focusEmulationError - The error the page's focus emulation rejects with, for a row that wants a page refusing to be presented.
 * @param options.unreadableWindow - Whether the page's session answers Browser.getWindowForTarget with no window id at all.
 * @param options.url - The URL the page reports. Defaults to a plain-origin one.
 * @param options.windowId - The window the page's session says it sits in.
 * @returns The double plus the counts of the lookups and reads its session served.
 */
function makeWindowPage(options: { bounds?: Record<string, number | string>; closed?: boolean; focusEmulationError?: Error;
  unreadableWindow?: boolean; url?: string; windowId?: number; } = {}): WindowPageStub {

  const stub: WindowPageStub = { browser: null, closeCalls: 0, focusEmulations: [], page: null as unknown as Page, windowLookups: 0, windowReads: 0 };

  stub.page = {

    browser: (): Nullable<Browser> => stub.browser,
    close: async (): Promise<void> => {

      stub.closeCalls++;
    },
    createCDPSession: async (): Promise<unknown> => ({

      send: async (method: string): Promise<unknown> => {

        if(method === "Browser.getWindowForTarget") {

          stub.windowLookups++;

          return options.unreadableWindow ? {} : { windowId: options.windowId ?? 7 };
        }

        if(method === "Browser.getWindowBounds") {

          stub.windowReads++;

          return options.bounds ? { bounds: options.bounds } : {};
        }

        return undefined;
      }
    }),
    emulateFocusedPage: async (enabled: boolean): Promise<void> => {

      stub.focusEmulations.push(enabled);

      if(options.focusEmulationError) {

        throw options.focusEmulationError;
      }
    },
    isClosed: (): boolean => options.closed ?? false,
    url: (): string => options.url ?? "https://example.test/page"
  } as unknown as Page;

  return stub;
}

/**
 * Builds a browser double over a set of page doubles and tells each page which browser owns it, which is what lets the placement confirmation reach the window
 * this process recorded for that browser. Each row builds its own, so one row's recorded identity is never another's.
 * @param stubs - The page doubles the browser reports, in the order it reports them.
 * @returns The browser double.
 */
function makeTopologyBrowser(stubs: WindowPageStub[]): Browser {

  const browser = { pages: async (): Promise<Page[]> => stubs.map((stub) => stub.page) } as unknown as Browser;

  for(const stub of stubs) {

    stub.browser = browser;
  }

  return browser;
}

/**
 * Builds a browser double whose page list is fixed and whose creations are recorded.
 * @param pages - The pages browser.pages() answers with, in order.
 * @param pageOptions - What every page this browser creates is built with, for a row that needs the created page to behave a particular way.
 * @returns The double, the doubles behind the pages it created, and the options each creation received.
 */
function makeWindowBrowser(pages: Page[], pageOptions?: Parameters<typeof makeWindowPage>[0]): { browser: Browser; created: WindowPageStub[];
  newPageOptions: unknown[]; } {

  const created: WindowPageStub[] = [];
  const newPageOptions: unknown[] = [];

  const browser = {

    newPage: async (options?: unknown): Promise<Page> => {

      newPageOptions.push(options);

      const stub = makeWindowPage(pageOptions);

      created.push(stub);

      return stub.page;
    },
    pages: async (): Promise<Page[]> => pages
  } as unknown as Browser;

  return { browser, created, newPageOptions };
}

/**
 * Creates a page in a window of its own against a throwaway browser double, for the rows that need one to hand to the carrier rule.
 * @returns The own-window page and the double behind it.
 */
async function makeOwnWindowPage(): Promise<WindowPageStub> {

  const { browser, created } = makeWindowBrowser([]);

  await createDiscoveryPage(browser);

  // The creator has run, so the double recorded exactly one page.
  return created[0]!;
}

describe("pickCarrierPage", () => {

  test("takes the first open page", () => {

    // The common case, and the reason the order matters: the browser reports its pages oldest first, so the first qualifying one is the extension's options page
    // or the blank tab the launch left behind, rather than a page some later feature happened to open.
    const first = makeWindowPage().page;
    const second = makeWindowPage().page;

    assert.equal(pickCarrierPage([ first, second ]), first, "the first candidate wins");
  });

  test("skips a closed page and takes the next open one", () => {

    // A command aimed at a closed page's session goes nowhere, so a closed page is no candidate at all.
    const closed = makeWindowPage({ closed: true }).page;
    const open = makeWindowPage().page;

    assert.equal(pickCarrierPage([ closed, open ]), open, "a closed page is passed over");
  });

  test("skips a page living in its own window and takes the next one", async () => {

    /* The rule the discovery window rests on. A window command travels on a page's session and acts on that page's window, so borrowing the discovery page to
     * minimize or restore "the window" would move the wrong one. The own-window page here comes from the creator rather than from a hand-set mark, so the row
     * proves the mark the creator sets is the mark the rule reads.
     */
    const ownWindow = await makeOwnWindowPage();
    const open = makeWindowPage().page;

    assert.equal(pickCarrierPage([ ownWindow.page, open ]), open, "an own-window page is passed over");
  });

  test("reports no carrier when every page is closed or lives in its own window", async () => {

    // The state the creator answers by opening its window without bounds, and the state the window sync answers with a temporary page of its own.
    const ownWindow = await makeOwnWindowPage();

    assert.equal(pickCarrierPage([ ownWindow.page, makeWindowPage({ closed: true }).page ]), null, "no candidate qualifies");
    assert.equal(pickCarrierPage([]), null, "an empty list has no candidate either");
  });
});

/* The identity is a WeakMap keyed on the browser, so every row here builds its own browser double and its own pages: one row's recorded window can never be
 * another's, and there is nothing to reset between them.
 */
describe("noteSharedWindow, resolveSharedWindowCarrier, and confirmSharedWindowPlacement", () => {

  test("takes the first plain-origin page of the recorded window, passing over own-window, closed, and other-window pages", async () => {

    /* The resolver's whole rule in one row. The open is evaluated on whichever page this answers with, so a page living in a window of its own would anchor the
     * tab to that window - the exact placement the identity exists to prevent - and a page that has since moved to another window is no better.
     */
    const ownWindow = await makeOwnWindowPage();
    const closed = makeWindowPage({ closed: true });
    const elsewhere = makeWindowPage({ windowId: 11 });
    const carrier = makeWindowPage({ url: "https://example.test/live" });
    const resting = makeWindowPage();
    const browser = makeTopologyBrowser([ ownWindow, closed, elsewhere, carrier, resting ]);

    await noteSharedWindow(browser, resting.page);

    assert.equal(await resolveSharedWindowCarrier(browser), carrier.page, "the first open, shared-window, plain-origin page is the carrier");
    assert.equal(ownWindow.windowLookups, 0, "a page in a window of its own is never even asked which window that is");
    assert.equal(closed.windowLookups, 0, "and neither is a closed one");
    assert.equal(elsewhere.windowLookups, 1, "the page in another window was asked, and passed over on the answer");
  });

  test("prefers a plain-origin page and takes the capture extension's own options page only when there is no other", async () => {

    /* An open evaluated on the extension's options page is territory nothing has measured, so it is the carrier of last resort rather than the first candidate -
     * and it would be the first candidate otherwise, because the library opens it at launch and the browser reports its pages oldest first.
     */
    const optionsPage = makeWindowPage({ url: "chrome-extension://jjndjgheafjngoipoacpjgeicjeomjli/options.html" });
    const plain = makeWindowPage();
    const withPlain = makeTopologyBrowser([ optionsPage, plain ]);

    await noteSharedWindow(withPlain, plain.page);

    assert.equal(await resolveSharedWindowCarrier(withPlain), plain.page, "the plain-origin page wins even though the options page came first");

    const lonely = makeWindowPage({ url: "chrome-extension://jjndjgheafjngoipoacpjgeicjeomjli/options.html" });
    const withoutPlain = makeTopologyBrowser([lonely]);

    await noteSharedWindow(withoutPlain, lonely.page);

    assert.equal(await resolveSharedWindowCarrier(withoutPlain), lonely.page, "and it is still a carrier when it is the only page there is");
  });

  test("reports no carrier when every page is closed, lives in its own window, or sits in another one", async () => {

    // The state the open answers by creating its page the plain way. The identity is read from a page that is not among the browser's own, which is what a
    // resting tab the user closed leaves behind: a recorded window with nothing left in it to open from.
    const ownWindow = await makeOwnWindowPage();
    const closed = makeWindowPage({ closed: true });
    const elsewhere = makeWindowPage({ windowId: 11 });
    const departed = makeWindowPage();
    const browser = makeTopologyBrowser([ ownWindow, closed, elsewhere ]);

    await noteSharedWindow(browser, departed.page);

    assert.equal(await resolveSharedWindowCarrier(browser), null, "no candidate qualifies");
    assert.equal(await resolveSharedWindowCarrier(makeTopologyBrowser([])), null, "and a browser reporting no pages has none either");
  });

  test("takes the first qualifying page when no window was ever recorded, and confirms any placement", async () => {

    /* With nothing recorded there is nothing to contradict a placement with, so both answers go advisory: an anchor to some window of the browser's is still
     * better than letting Chrome choose one, and a confirmation that could only ever answer "unknown" would refuse every tab there is.
     */
    const elsewhere = makeWindowPage({ windowId: 11 });
    const browser = makeTopologyBrowser([elsewhere]);

    assert.equal(await resolveSharedWindowCarrier(browser), elsewhere.page, "the first qualifying page is the carrier, whatever window it is in");
    assert.equal(elsewhere.windowLookups, 0, "and it is not asked which window that is");
    assert.equal(await confirmSharedWindowPlacement(elsewhere.page), true, "a placement there is nothing to check against is confirmed");
  });

  test("asks a candidate's session which window it is in once, and answers from the cache afterwards", async () => {

    /* The lookup attaches a CDP session the shared helper does not detach, so a resolver that re-read every candidate on every tune would leave one behind on
     * each launch-era page every time. The count is what makes that a fact rather than an intention.
     */
    const elsewhere = makeWindowPage({ windowId: 11 });
    const carrier = makeWindowPage();
    const browser = makeTopologyBrowser([ elsewhere, carrier ]);

    await noteSharedWindow(browser, carrier.page);

    await resolveSharedWindowCarrier(browser);
    await resolveSharedWindowCarrier(browser);

    assert.equal(elsewhere.windowLookups, 1, "the page that did not qualify was asked once across both resolves");
    assert.equal(carrier.windowLookups, 1, "and the one that did was asked when the identity was recorded and never again");
  });

  test("confirms a page in the recorded window, and refuses one in another window or one whose window cannot be read", async () => {

    /* The refusal on an unreadable window is the deliberate half. The shared lookup reports nothing for a closed page, an empty response, and a CDP failure
     * alike, and reading any of those as a confirmation would confirm a wrong-window tab in exactly the case this check exists for.
     */
    const anchor = makeWindowPage();
    const inWindow = makeWindowPage();
    const elsewhere = makeWindowPage({ windowId: 11 });
    const unreadable = makeWindowPage({ unreadableWindow: true });
    const browser = makeTopologyBrowser([ anchor, inWindow, elsewhere, unreadable ]);

    await noteSharedWindow(browser, anchor.page);

    assert.equal(await confirmSharedWindowPlacement(inWindow.page), true, "a page in the recorded window is confirmed");
    assert.equal(await confirmSharedWindowPlacement(elsewhere.page), false, "a page in another window is refused");
    assert.equal(await confirmSharedWindowPlacement(unreadable.page), false, "and so is a page whose window cannot be read at all");
  });

  test("records nothing when the page it reads the identity from cannot name a window", async () => {

    // An identity recorded from a reading that never happened would refuse every placement afterwards, so an unreadable page leaves both answers advisory.
    const unreadable = makeWindowPage({ unreadableWindow: true });
    const elsewhere = makeWindowPage({ windowId: 11 });
    const browser = makeTopologyBrowser([ unreadable, elsewhere ]);

    await noteSharedWindow(browser, unreadable.page);

    assert.equal(await confirmSharedWindowPlacement(elsewhere.page), true, "with nothing recorded the confirmation stays advisory");
    assert.equal(await resolveSharedWindowCarrier(browser), unreadable.page, "and the resolver takes the first qualifying page");
  });
});

describe("mirrorPlacement", () => {

  // Four distinct numbers, so a mirror that transposed a pair or dropped one into another's slot fails here rather than passing on a uniform frame.
  const FRAME = { height: 400, left: 10, top: 20, width: 300 };

  test("carries the frame and no state for a normal window", () => {

    const bounds = mirrorPlacement({ ...FRAME, windowState: "normal" });

    assert.equal(bounds.height, 400, "the height is mirrored");
    assert.equal(bounds.left, 10, "the left is mirrored");
    assert.equal(bounds.top, 20, "the top is mirrored");
    assert.equal(bounds.width, 300, "the width is mirrored");
    assert.ok(!("windowState" in bounds), "a normal window contributes no state key");
  });

  test("carries the maximized state alongside the frame", () => {

    // The one state worth mirroring: Chrome saves the maximized flag with the placement, so a maximized shared window mirrored as a normal one would drop it.
    const bounds = mirrorPlacement({ ...FRAME, windowState: "maximized" });

    assert.equal(bounds.height, 400, "the height is mirrored");
    assert.equal(bounds.left, 10, "the left is mirrored");
    assert.equal(bounds.top, 20, "the top is mirrored");
    assert.equal(bounds.width, 300, "the width is mirrored");
    assert.equal(bounds.windowState, "maximized", "the maximized state travels with the frame");
  });

  test("carries no state for a fullscreen or a minimized window", () => {

    /* A fullscreen window's frame is its whole screen and a normal window at that frame is the right stand-in for it; a minimized window reports the frame it
     * will return to, which is a placement worth inheriting without the minimization - a window created minimized would render nothing at all.
     */
    for(const windowState of [ "fullscreen", "minimized" ]) {

      const bounds = mirrorPlacement({ ...FRAME, windowState });

      assert.equal(bounds.height, 400, windowState + ": the height is mirrored");
      assert.equal(bounds.left, 10, windowState + ": the left is mirrored");
      assert.equal(bounds.top, 20, windowState + ": the top is mirrored");
      assert.equal(bounds.width, 300, windowState + ": the width is mirrored");
      assert.ok(!("windowState" in bounds), windowState + ": no state key travels");
    }
  });
});

describe("createDiscoveryPage", () => {

  test("opens a background window at the carrier's placement, read through the carrier's own session", async () => {

    /* The creation contract in one row. The page list leads with a page the creator must pass over and a closed one, so a creator reading the placement from
     * whatever page came first would read a window that is not the shared one - and because each page's session counts its own reads, where the read went is
     * observable rather than inferred.
     */
    const ownWindow = await makeOwnWindowPage();
    const closed = makeWindowPage({ closed: true });
    const carrier = makeWindowPage({ bounds: { height: 400, left: 10, top: 20, width: 300, windowState: "normal" } });
    const { browser, created, newPageOptions } = makeWindowBrowser([ ownWindow.page, closed.page, carrier.page ]);

    const page = await createDiscoveryPage(browser);

    assert.deepEqual(newPageOptions, [{ background: true, type: "window", windowBounds: { height: 400, left: 10, top: 20, width: 300 } }],
      "the window is created in the background at the carrier's frame");
    assert.equal(carrier.windowReads, 1, "the placement was read through the carrier's session");
    assert.equal(ownWindow.windowReads, 0, "the own-window page's session was never asked");
    assert.equal(closed.windowReads, 0, "the closed page's session was never asked");
    assert.equal(page, created[0]?.page, "the created page is what comes back");
  });

  test("mirrors a maximized carrier as maximized", async () => {

    const carrier = makeWindowPage({ bounds: { height: 400, left: 10, top: 20, width: 300, windowState: "maximized" } });
    const { browser, newPageOptions } = makeWindowBrowser([carrier.page]);

    await createDiscoveryPage(browser);

    assert.deepEqual(newPageOptions, [{ background: true, type: "window", windowBounds: { height: 400, left: 10, top: 20, width: 300,
      windowState: "maximized" } }], "the maximized state travels into the creation options");
  });

  test("creates the window without bounds when the browser has no page to read a placement from", async () => {

    /* Unreachable in practice - the extension's options page exists from launch and is never swept - but the creator still has to name a behavior for it, and
     * letting Chrome cascade the window from the profile's saved placement is the right one. The three fields are read one at a time so an options object
     * carrying an undefined placement is told apart from one carrying no placement key at all.
     */
    const { browser, newPageOptions } = makeWindowBrowser([]);

    await createDiscoveryPage(browser);

    assert.equal(newPageOptions.length, 1, "exactly one creation");

    const options = newPageOptions[0] as { background?: boolean; type?: string; windowBounds?: unknown };

    assert.equal(options.background, true, "the window still opens in the background");
    assert.equal(options.type, "window", "the page still gets a window of its own");
    assert.equal(options.windowBounds, undefined, "no placement is passed when none could be read");
  });

  test("marks the page it creates as living in its own window", async () => {

    // The mark is what every shared-window command site reads, so the creator setting it is what makes the rule hold at all. A plain page from the same browser
    // double is the control: it qualifies as a carrier, which is what shows the mark rather than the double is doing the work.
    const carrier = makeWindowPage({ bounds: { height: 400, left: 10, top: 20, width: 300, windowState: "normal" } });
    const { browser } = makeWindowBrowser([carrier.page]);

    const created = await createDiscoveryPage(browser);

    assert.equal(isCarrierPage(created), false, "the discovery page never carries a shared-window command");
    assert.equal(pickCarrierPage([created]), null, "and it is never picked as one");
    assert.equal(isCarrierPage(await browser.newPage()), true, "a plain page from the same double still qualifies");
    assert.equal(isCarrierPage(null), false, "an absent page is no carrier");
  });

  test("presents the page it creates as visible and focused, and leaves the carrier alone", async () => {

    /* Chrome does not paint a background window's document on its own, and every wait a walk makes - a visible selector, a condition polled from inside the
     * page - needs a painted document to make progress. Presenting the page is what buys those waits. The mechanism is Puppeteer's own page method, so what the
     * row observes is the flag the creator handed it and which page it handed it to, not protocol traffic.
     */
    const carrier = makeWindowPage({ bounds: { height: 400, left: 10, top: 20, width: 300, windowState: "normal" } });
    const { browser, created } = makeWindowBrowser([carrier.page]);

    await createDiscoveryPage(browser);

    const discovery = firstOf(created, "created page");

    assert.deepEqual(discovery.focusEmulations, [true], "the created page is presented once, with the emulation turned on");
    assert.deepEqual(carrier.focusEmulations, [], "the carrier the placement was read from is left exactly as it was");
    assert.equal(discovery.closeCalls, 0, "a page that accepted the emulation is handed back rather than closed");
  });

  test("closes the page it created when the emulation is refused, and fails with the refusal", async () => {

    /* A page that will not be presented is one no walk can use, and nothing else holds it at this point: the caller never receives it and the managed-page
     * sweep has never seen it. So the creator closes it, which closes its window, and lets the refusal through as it stands.
     */
    const focusEmulationError = new Error("Focus emulation refused.");
    const { browser, created } = makeWindowBrowser([], { focusEmulationError });

    await assert.rejects(createDiscoveryPage(browser), (error: unknown) => error === focusEmulationError,
      "the creation fails with the refusal itself rather than a wrapping of it");

    const discovery = firstOf(created, "created page");

    assert.equal(discovery.closeCalls, 1, "the page the creator made is closed exactly once");
    assert.equal(isCarrierPage(discovery.page), false, "and the mark set before the failure leaves the closed page no carrier");
  });
});

describe("makeFocusReaffirmCallback", () => {

  /* Every row below drives the factory with an injected re-issue and an injected clock, and reads one shared timeline: the re-issue pushes "shot" as it is
   * called and the clock pushes "sleep:<ms>" as each wait is requested, so a row asserts the interleaving of shots and waits directly instead of inferring it
   * from two arrays that were filled independently. A second array records the page each re-issue was handed, which is what tells one page's shots from
   * another's - page doubles are bare objects, so identity is the only comparison that means anything about them.
   *
   * The durations and counts here are written as literals on purpose. Deriving them from the ladder constant the module holds would make every row follow any
   * edit to it, a wrong one included; literals make a deliberate change to the rungs a conscious update here.
   */
  const recordShot = (timeline: string[], targets: Page[]): ((target: Page) => Promise<void>) => async (target: Page): Promise<void> => {

    timeline.push("shot");
    targets.push(target);
  };

  /* A Clock that records each wait into a shared timeline as the ladder asks for it and forwards every call to a TestClock of its own. Recording at registration
   * is what keeps a row's literal timeline exact: the entry lands in the order the ladder requested the wait, not the order the drain later releases it. The
   * inner clock is handed back so a row drains exactly the schedule it recorded.
   */
  const recordWaits = (timeline: string[], prefix = "sleep:"): Clock & { inner: TestClock } => {

    const inner = new TestClock();

    return {

      delay: async (ms: number, init?: { signal?: AbortSignal }): Promise<void> => {

        timeline.push(prefix + String(ms));

        return inner.delay(ms, init);
      },
      inner,
      now: (): number => inner.now(),
      schedule: (callback: () => void, ms: number, init?: { repeat?: boolean }): Disposable => inner.schedule(callback, ms, init),
      timeout: (ms: number): AbortSignal => inner.timeout(ms)
    };
  };

  test("runs the full interleaved ladder from one invocation, every shot against its own page", async () => {

    /* The ladder's whole purpose is that its shots straddle the compositor's switch, and only the interleaving of waits and re-issues shows that: an immediate
     * shot, then a wait for each rung's remaining distance from the activation with a shot behind it. The page identity is carried along because a callback that
     * ignored its page would re-issue against whatever page the caller happened to hold, which on a multi-stream browser is somebody else's capture.
     */
    const page = {} as unknown as Page;
    const targets: Page[] = [];
    const timeline: string[] = [];
    const clock = recordWaits(timeline);
    const running = makeFocusReaffirmCallback(page, recordShot(timeline, targets), clock)();

    await drainClock(clock.inner);
    await running;

    assert.deepEqual(timeline, [ "shot", "sleep:250", "shot", "sleep:500", "shot", "sleep:750", "shot", "sleep:1500", "shot" ],
      "an immediate shot and four rungs, each waiting only the distance left to its own offset from the activation");
    assert.equal(targets.length, 5, "five shots, one per rung plus the immediate one");
    assert.ok(targets.every((target) => target === page), "every shot re-issued against the callback's own page");
  });

  test("a later invocation supersedes an in-flight ladder and runs a full schedule of its own", async () => {

    /* A second activation landing inside an older ladder's window needs rungs timed from its own moment - the compositor switch it has to outlast is its own,
     * not the earlier one's. The generation counter is what hands it that: the older ladder returns at its next check and the newer one owns the schedule.
     */
    const page = {} as unknown as Page;
    const targets: Page[] = [];
    const timeline: string[] = [];

    // The virtual clock parks the first ladder on its first rung, so the second activation lands squarely inside it - the case the generation counter exists for.
    const clock = recordWaits(timeline);
    const callback = makeFocusReaffirmCallback(page, recordShot(timeline, targets), clock);
    const superseded = callback();

    await immediate();

    assert.equal(targets.length, 1, "the first activation fired its immediate shot and parked on its first rung");
    assert.equal(clock.inner.pending, 1, "and that rung is the only wait registered");

    const newer = callback();

    await drainClock(clock.inner);
    await newer;
    await superseded;

    assert.equal(targets.length, 6, "the second activation fired its own immediate shot and its own four rungs, and the superseded ladder returned at its " +
      "generation check rather than firing again");
    assert.deepEqual(timeline, [ "shot", "sleep:250", "shot", "sleep:250", "shot", "sleep:500", "shot", "sleep:750", "shot", "sleep:1500", "shot" ],
      "the second activation's rungs are spaced from its own moment, not from what the first activation had left to run");
  });

  test("near-simultaneous invocations fire both immediate shots but only the newer one's rung schedule", async () => {

    /* One tab activation fires the window's focus event more than once. Both invocations take their immediate shot, which costs nothing and is the shot most
     * likely to matter, and then the older schedule collapses into the newer one a few milliseconds behind it rather than the two running side by side.
     */
    const page = {} as unknown as Page;
    const targets: Page[] = [];
    const timeline: string[] = [];

    const clock = recordWaits(timeline);
    const callback = makeFocusReaffirmCallback(page, recordShot(timeline, targets), clock);
    const both = Promise.all([ callback(), callback() ]);

    await drainClock(clock.inner);
    await both;

    assert.equal(targets.length, 6, "two immediate shots and exactly one full rung schedule");

    // The two ladders interleave microtask by microtask, so the waits are compared as a set: four from the schedule that survived, one from the one that
    // returned at its first check.
    assert.deepEqual(timeline.filter((entry) => entry.startsWith("sleep:")).toSorted(), [ "sleep:1500", "sleep:250", "sleep:250", "sleep:500", "sleep:750" ],
      "the superseded ladder waited once before returning, and the surviving one waited its four rungs");
  });

  test("a rejecting shot is swallowed and the rest of the ladder still fires", async () => {

    // A focus event races page teardown by nature - the tab a user selects can be the one a terminating stream is closing - and the page-side caller has nowhere
    // to put a rejection. A rung that fails also says nothing about the next one, which fires a moment later against a page that may well have settled by then.
    const page = {} as unknown as Page;
    const targets: Page[] = [];
    const timeline: string[] = [];

    const record = recordShot(timeline, targets);
    const clock = recordWaits(timeline);

    const callback = makeFocusReaffirmCallback(page, async (target: Page): Promise<void> => {

      await record(target);

      if(targets.length === 1) {

        throw new Error("synthetic re-issue rejection");
      }
    }, clock);

    const running = callback();

    // The expectation is attached before the clock is driven, so a rejection escaping mid-drive is observed rather than left unhandled.
    const settled = assert.doesNotReject(() => running, "the callback resolves rather than rejecting into the page's focus handler");

    await drainClock(clock.inner);
    await settled;

    assert.equal(targets.length, 5, "the immediate shot's rejection did not stop the four rungs behind it");
  });

  test("an all-rejecting ladder never rejects outward, and a fresh activation still gets a fresh ladder", async () => {

    /* Nothing here gives up early: a shot that fails is one the next rung retries a moment later, and the count below is exactly what a consecutive-failure
     * guard would fall short of. Recovery is structural rather than earned - the next activation runs a full ladder whether or not this one accomplished
     * anything - so a regression that tied a fresh schedule to a past success would surface here.
     */
    const page = {} as unknown as Page;
    const targets: Page[] = [];
    const timeline: string[] = [];

    const record = recordShot(timeline, targets);
    const clock = recordWaits(timeline);

    const callback = makeFocusReaffirmCallback(page, async (target: Page): Promise<void> => {

      await record(target);

      throw new Error("synthetic re-issue rejection");
    }, clock);

    const first = callback();
    const firstSettled = assert.doesNotReject(() => first, "the first activation resolves even though every one of its shots failed");

    await drainClock(clock.inner);
    await firstSettled;

    assert.equal(targets.length, 5, "every rung fired, none of them skipped over the failures ahead of it");

    const second = callback();
    const secondSettled = assert.doesNotReject(() => second, "so does the activation after it");

    await drainClock(clock.inner);
    await secondSettled;

    assert.equal(targets.length, 10, "a fresh activation runs a fresh full ladder");
  });

  test("one page's activation leaves another page's in-flight ladder alone", async () => {

    /* The counter lives inside each callback's own closure, so two capture pages cannot reach each other's schedules. Were it shared, a second stream's tab
     * give-back would cut the first stream's heal short and leave that capture composing the window's view of the page.
     */
    const firstPage = {} as unknown as Page;
    const secondPage = {} as unknown as Page;
    const targets: Page[] = [];
    const timeline: string[] = [];

    // Each page's ladder runs on a clock of its own, so the first page's stays parked on its first rung while the second page's activation runs to completion -
    // which is what makes the second landing genuinely mid-schedule rather than after the fact.
    const firstClock = recordWaits(timeline, "first:sleep:");
    const secondClock = recordWaits(timeline);
    const record = recordShot(timeline, targets);
    const firstRun = makeFocusReaffirmCallback(firstPage, record, firstClock)();

    await immediate();

    assert.equal(firstClock.inner.pending, 1, "the first page's ladder is parked on its first rung");

    const secondRun = makeFocusReaffirmCallback(secondPage, record, secondClock)();

    await drainClock(secondClock.inner);
    await secondRun;

    await drainClock(firstClock.inner);
    await firstRun;

    assert.equal(targets.filter((target) => target === firstPage).length, 5, "the first page's ladder ran its full schedule across the second page's activation");
    assert.equal(targets.filter((target) => target === secondPage).length, 5, "the second page's ladder ran a full schedule of its own");
    assert.equal(targets.length, 10, "neither page received a shot meant for the other");
  });

  test("a shot that never settles parks its own ladder without blocking the next activation", async () => {

    /* A CDP send against a page on its way out can hang rather than reject. The ladder holding it parks mid-schedule, which costs nothing: it holds no timer, it
     * blocks no later activation, and if its shot ever does settle, the generation check ahead of the next rung sends it home.
     */
    const page = {} as unknown as Page;
    const targets: Page[] = [];
    const timeline: string[] = [];

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise: hung, resolve: settleHungShot } = Promise.withResolvers<void>();

    const record = recordShot(timeline, targets);
    const clock = recordWaits(timeline);

    const callback = makeFocusReaffirmCallback(page, async (target: Page): Promise<void> => {

      await record(target);

      // The very first shot never settles on its own; every shot after it behaves normally.
      if(targets.length === 1) {

        await hung;
      }
    }, clock);

    void callback();

    await immediate();

    assert.equal(targets.length, 1, "the first activation is parked inside its immediate shot");
    assert.equal(clock.inner.pending, 0, "a ladder parked inside a shot holds no wait at all");

    const second = callback();
    const secondSettled = assert.doesNotReject(() => second, "a second activation runs to completion regardless");

    await drainClock(clock.inner);
    await secondSettled;

    assert.equal(targets.length, 6, "the parked ladder cost the second activation nothing");

    settleHungShot();

    await immediate();

    assert.equal(targets.length, 6, "the parked ladder's late shot found a newer generation and stopped there");
  });
});

describe("healActivatedCaptureTab", () => {

  /* Every row here enrolls its pages through installActivationHeal itself rather than around it, because the enrollment and the exposed binding holding the same
   * callback instance is the whole of the two-triggers-one-ladder guarantee - an enrollment built beside the install point would prove nothing about it. The page
   * double records the binding it is handed, and the injected collaborators are what make the enrolled callback observable: the recording re-issue counts the
   * shots a report produced, and the virtual clock the row drains lets a ladder run to its end without waiting out its rungs.
   */
  const enroll = async (): Promise<{ binding: () => Promise<void>; clock: TestClock; page: Page; shots: Page[] }> => {

    const bindings: (() => Promise<void>)[] = [];
    const clock = new TestClock();
    const shots: Page[] = [];

    const page = {

      evaluateOnNewDocument: async (): Promise<void> => undefined,
      exposeFunction: async (_name: string, handler: () => Promise<void>): Promise<void> => { bindings.push(handler); }
    } as unknown as Page;

    await installActivationHeal(page, { clock, reaffirm: async (target: Page): Promise<void> => { shots.push(target); } });

    const binding = bindings[0];

    assert.ok(binding, "the install point handed the page a binding");
    assert.equal(bindings.length, 1, "and exactly one");

    return { binding, clock, page, shots };
  };

  // A registry entry as the match reads one: an identity and a page. Nothing else on an entry is looked at, so nothing else is built.
  const streamEntry = (page: Nullable<Page>, mode: StreamingMode): StreamRegistryEntry => {

    return { identity: (mode === "native") ? makeNativeIdentity() : makePendingCaptureIdentity(), page } as unknown as StreamRegistryEntry;
  };

  // The collaborators a report is delivered through, in the shape the match reads them: a registry walk and a page-to-tab lookup.
  interface ReportDeps {

    readonly getAllStreams: () => StreamRegistryEntry[];
    readonly getCachedTabId: (page: Page) => number | undefined;
  }

  /* The tab lookup refuses a null page rather than answering undefined for one, because an answer would let a match that skipped the registry's own nullability
   * guard pass this file unremarked.
   */
  const makeDeps = (entries: StreamRegistryEntry[], tabIds: Map<Page, number>): ReportDeps => ({

    getAllStreams: (): StreamRegistryEntry[] => entries,
    getCachedTabId: (page: Page): number | undefined => {

      assert.notEqual(page, null, "the match narrows an entry's nullable page before asking which tab it is");

      return tabIds.get(page);
    }
  });

  test("fires the enrolled page's own ladder, which the page's binding then supersedes rather than doubling", async () => {

    /* The report is the trigger a captured page cannot raise for itself, so what it has to reach is the same callback the binding reaches. The proof is
     * behavioral rather than structural: the report starts a ladder, the binding's invocation lands inside it, and what follows is the collapse signature of two
     * invocations of one callback - both immediate shots and a single rung schedule - rather than the ten shots two independent ladders would have produced.
     */
    const { binding, clock, page, shots } = await enroll();
    const deps = makeDeps([streamEntry(page, "capture")], new Map([[ page, 42 ]]));

    healActivatedCaptureTab(42, deps);

    assert.equal(shots.length, 1, "the report's immediate shot is away before anything yields");

    const bound = binding();

    await drainClock(clock);
    await bound;

    assert.equal(shots.length, 6, "two immediate shots and one rung schedule: the binding superseded the report's ladder instead of running beside it");
    assert.ok(shots.every((target) => target === page), "and every shot went to the page the report named");
  });

  test("fires nothing for a native stream, an unmatched id, a null page, or a page that was never enrolled", async () => {

    /* The negative controls, all read through the recording re-issue rather than through the absence of a throw: a report that quietly did nothing and a report
     * that fired the wrong page's ladder look identical to a row that only asserts it survived. The native entry is enrolled and its id matches, so the
     * streaming-mode test is the only thing standing between it and a shot.
     */
    const native = await enroll();
    const capture = await enroll();
    const unenrolled = {} as unknown as Page;

    const deps = makeDeps([

      streamEntry(native.page, "native"),
      streamEntry(null, "capture"),
      streamEntry(unenrolled, "capture"),
      streamEntry(capture.page, "capture")
    ], new Map([ [ native.page, 1 ], [ unenrolled, 4 ], [ capture.page, 9 ] ]));

    for(const tabId of [ 1, 4, 777 ]) {

      healActivatedCaptureTab(tabId, deps);
    }

    await immediate();

    assert.equal(native.clock.pending, 0, "no ladder started, so neither page's clock carries a rung");
    assert.equal(capture.clock.pending, 0, "and the enrolled capture page's clock carries none either");
    assert.equal(native.shots.length, 0, "a native stream's page is never re-issued against, enrolled and matched by id though it is");
    assert.equal(capture.shots.length, 0, "and the one enrolled capture page went unnamed by all three reports");
  });

  test("fires only the page the report named, leaving another enrolled page's heal alone", async () => {

    /* Two capture streams share a browser and a report carries one tab id, so the match has to be an identity question rather than a "some capture page was
     * activated" one. Each page carries its own recorder, which is what tells a shot meant for one from a shot that reached the other.
     */
    const first = await enroll();
    const second = await enroll();
    const deps = makeDeps([ streamEntry(first.page, "capture"), streamEntry(second.page, "capture") ], new Map([ [ first.page, 1 ], [ second.page, 2 ] ]));

    healActivatedCaptureTab(1, deps);

    await drainClock(first.clock);

    assert.equal(first.shots.length, 5, "the named page ran its full ladder");
    assert.equal(second.shots.length, 0, "and the other page's heal never ran");

    healActivatedCaptureTab(2, deps);

    await drainClock(second.clock);

    assert.equal(second.shots.length, 5, "the second report ran the second page's ladder");
    assert.equal(first.shots.length, 5, "without adding anything to the first page's");
    assert.ok(first.shots.every((target) => target === first.page) && second.shots.every((target) => target === second.page),
      "each ladder's shots went to its own page");
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
        browser: { captureImpaired: boolean; connected: boolean; pageCount: number };
        memory: { heapUsed: number; rss: number };
        streams: { active: number; limit: number };
        uptime: number;
      };

      assert.ok(status, "status object captured");
      assert.equal(typeof status.browser.captureImpaired, "boolean", "browser.captureImpaired is a boolean");
      assert.equal(typeof status.browser.connected, "boolean", "browser.connected is a boolean");
      assert.equal(typeof status.browser.pageCount, "number", "browser.pageCount is a number");
      assert.equal(typeof status.memory.heapUsed, "number", "memory.heapUsed is a number");
      assert.equal(typeof status.memory.rss, "number", "memory.rss is a number");
      assert.equal(typeof status.streams.active, "number", "streams.active is a number");
      assert.equal(typeof status.streams.limit, "number", "streams.limit is a number");
      assert.equal(typeof status.uptime, "number", "uptime is a number");

      // No browser launched in the unit-test environment, so the connected flag must be false, the page count must be zero, and there is no ready browser for a
      // capture-impairment mark to live on.
      assert.equal(status.browser.captureImpaired, false, "no browser -> no impairment mark");
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

describe("the lifecycle timers arm on the injected clock", () => {

  /* Both owners hold module-level registry bindings, so a row that ended without stopping its owner would leave the next start a no-op. Stopping both here is a
   * no-op when nothing is running and keeps each row independent of the one before it.
   */
  afterEach(() => {

    stopStalePageCleanup();
    stopBrowserRestartChecking();
  });

  test("the stale-page sweep arms one interval, ignores a second start, and is drained by its stop", () => {

    const clock = new TestClock();

    startStalePageCleanup(clock);

    assert.equal(clock.pending, 1, "the sweep's interval is armed on the injected clock");
    assert.deepEqual(clock.requested, [CONFIG.recovery.stalePageCleanupInterval], "the interval is the configured sweep cadence");

    // A second start while one is running changes nothing: without the running guard it would arm a second interval and orphan the first registry.
    startStalePageCleanup(clock);

    assert.equal(clock.pending, 1, "a second start armed nothing further");
    assert.equal(clock.requested.length, 1, "and asked the clock for nothing further");

    stopStalePageCleanup();

    // The drain is the proof the registry was disposed rather than merely forgotten: a skipped disposal would leave the interval on the clock's pending set.
    assert.equal(clock.pending, 0, "the stop drained the interval off the clock");
  });

  test("the restart check arms one interval, ignores a second start, and is drained by its stop", () => {

    // BROWSER_RESTART_CHECK_INTERVAL is module-private, so the row states its value (30 seconds) rather than reaching for the constant.
    const checkIntervalMs = 30000;
    const clock = new TestClock();

    startBrowserRestartChecking(clock);

    assert.equal(clock.pending, 1, "the eligibility check's interval is armed on the injected clock");
    assert.deepEqual(clock.requested, [checkIntervalMs], "the interval is the 30-second check cadence");

    startBrowserRestartChecking(clock);

    assert.equal(clock.pending, 1, "a second start armed nothing further");
    assert.equal(clock.requested.length, 1, "and asked the clock for nothing further");

    stopBrowserRestartChecking();

    assert.equal(clock.pending, 0, "the stop drained the check off the clock");
  });
});

describe("applyStalePageCleanupChanges", () => {

  /* The handler re-arms the running sweep on the clock its start received, so a row starts the sweep on a test clock and reads what the handler asked of it.
   * CONFIG holds the running interval and the candidate the saved one, so a handler that read CONFIG would re-arm at the interval it replaces.
   */
  const RUNNING_INTERVAL = CONFIG.recovery.stalePageCleanupInterval;
  const SAVED_INTERVAL = RUNNING_INTERVAL * 2;
  const CHANGE: ConfigChange = { current: SAVED_INTERVAL, path: "recovery.stalePageCleanupInterval", previous: RUNNING_INTERVAL };

  // The candidate a save hands the handler: the running configuration with the saved interval applied, while CONFIG still holds the running one.
  const makeNext = (): Config => {

    const next = structuredClone(CONFIG);

    next.recovery.stalePageCleanupInterval = SAVED_INTERVAL;

    return next;
  };

  // The timers armed on the system clock, read from the process's active resources, so a sweep a handler armed outside the row's clock shows up here.
  const countSystemTimers = (): number => process.getActiveResourcesInfo().filter((resource) => resource === "Timeout").length;

  // The sweep's owner holds a module-level binding, so a row that ended with the sweep running would leave the next row's start a no-op.
  afterEach(() => {

    stopStalePageCleanup();
  });

  test("a saved interval re-arms the running sweep at the saved cadence on the clock its start received", async () => {

    const clock = new TestClock();

    startStalePageCleanup(clock);

    assert.deepEqual(await applyStalePageCleanupChanges([CHANGE], makeNext()), [], "the handler refuses nothing");
    assert.deepEqual(clock.requested, [ RUNNING_INTERVAL, SAVED_INTERVAL ], "the start armed the running interval, and the save re-armed the saved one");
    assert.equal(clock.pending, 1, "the re-arm left one sweep interval pending rather than adding a second");
  });

  test("a saved interval arms nothing before the sweep starts or after it stops", async () => {

    const clock = new TestClock();
    const systemTimers = countSystemTimers();

    assert.deepEqual(await applyStalePageCleanupChanges([CHANGE], makeNext()), [], "the handler refuses nothing before a start");
    assert.deepEqual(clock.requested, [], "a save before the start asked the row's clock for nothing");
    assert.equal(countSystemTimers(), systemTimers, "a save before the start armed no sweep on the system clock");

    startStalePageCleanup(clock);
    stopStalePageCleanup();

    assert.deepEqual(await applyStalePageCleanupChanges([CHANGE], makeNext()), [], "the handler refuses nothing after a stop");
    assert.deepEqual(clock.requested, [RUNNING_INTERVAL], "only the start asked the row's clock for an interval");
    assert.equal(clock.pending, 0, "the stopped sweep stays drained");
    assert.equal(countSystemTimers(), systemTimers, "a save after the stop armed no sweep on the system clock");

    // A stop returns the owner to its unstarted state, so a later start arms a fresh sweep rather than finding the stopped one still held.
    const restarted = new TestClock();

    startStalePageCleanup(restarted);

    assert.equal(restarted.pending, 1, "a start after the stop armed a fresh sweep");
  });

  test("a change dispatched through the reactivity registry reaches the handler registered for the interval's path", async () => {

    const clock = new TestClock();

    startStalePageCleanup(clock);

    assert.deepEqual(await applyConfigChanges({ held: [], live: [CHANGE], nextStream: [] }, makeNext()), { realized: [CHANGE], rejected: [] },
      "the dispatch realized the change");
    assert.deepEqual(clock.requested, [ RUNNING_INTERVAL, SAVED_INTERVAL ], "the handler registered for the path re-armed the sweep on the row's clock");
  });

  test("the re-armed sweep reads the clock its start received when it fires", async (t) => {

    const clock = new TestClock();

    startStalePageCleanup(clock);
    await applyStalePageCleanupChanges([CHANGE], makeNext());

    /* No unit row can read the instant the sweep judges staleness against, because the sweep returns before reading it while no browser is ready, so the row reads
     * which clock the fired callback reads, that instant's one source. The test clock runs a due callback synchronously inside its advance and keeps its own time
     * in a private field, so each spy counts the callback's reads alone.
     */
    const sweepReads = t.mock.method(clock, "now");
    const systemReads = t.mock.method(systemClock, "now");

    clock.advance(SAVED_INTERVAL - 1);

    assert.equal(sweepReads.mock.callCount(), 0, "nothing fired before the saved cadence elapsed");
    assert.equal(systemReads.mock.callCount(), 0, "and nothing read the system clock");

    clock.advance(1);

    assert.equal(sweepReads.mock.callCount(), 1, "the re-armed sweep read the clock its start received when it fired");
    assert.equal(systemReads.mock.callCount(), 0, "and never read the system clock");
  });
});

describe("isBrowserIdleForRestart", () => {

  /* Each cause asks a different question of the same registry, and the whole point of extracting the decision is that the difference is readable rather than
   * spelled out at each guard. Maintenance is housekeeping, so it defers to any entry at all - replacing the browser under a tune in progress would fail that
   * tune for nothing better than a fresher instance. Impairment is a repair the tunes are waiting on, so it asks only what a teardown would destroy: a stream
   * that holds its page, or a page an operation still holds in flight. A single decision shared by the causes fails the deciding row below, where a pending entry
   * is present and nothing is established.
   */
  test("maintenance defers to any registered entry, established or not", () => {

    assert.equal(isBrowserIdleForRestart("maintenance", { establishedStreams: false, inFlightPages: 0, streamCount: 0 }), true,
      "an empty registry is the only state a maintenance restart runs in");
    assert.equal(isBrowserIdleForRestart("maintenance", { establishedStreams: false, inFlightPages: 0, streamCount: 1 }), false,
      "a pending entry defers scheduled maintenance");
    assert.equal(isBrowserIdleForRestart("maintenance", { establishedStreams: true, inFlightPages: 0, streamCount: 1 }), false,
      "and so does an established one");
  });

  test("impairment waits for nothing established and no page held in flight", () => {

    assert.equal(isBrowserIdleForRestart("impairment", { establishedStreams: false, inFlightPages: 0, streamCount: 0 }), true,
      "an idle browser is replaced at once");
    assert.equal(isBrowserIdleForRestart("impairment", { establishedStreams: true, inFlightPages: 0, streamCount: 1 }), false,
      "a stream established on this browser would be destroyed by the teardown");
    assert.equal(isBrowserIdleForRestart("impairment", { establishedStreams: false, inFlightPages: 1, streamCount: 0 }), false,
      "and so would a page another operation still holds");
    assert.equal(isBrowserIdleForRestart("impairment", { establishedStreams: true, inFlightPages: 2, streamCount: 3 }), false,
      "both together defer it too");
  });

  test("the deciding row: a pending entry alone defers maintenance but not the repair", () => {

    // The tune waiting on the relaunch is itself a pending entry with its page already released, so a repair that read the raw count could never run for it.
    const waitingTune = { establishedStreams: false, inFlightPages: 0, streamCount: 1 };

    assert.equal(isBrowserIdleForRestart("impairment", waitingTune), true, "the repair the waiting tune needs is not blocked by the waiting tune");
    assert.equal(isBrowserIdleForRestart("maintenance", waitingTune), false, "while housekeeping still defers to it");
  });
});

/* Deferred to e2e (require Puppeteer/Chrome integration):
 *
 * - getCurrentBrowser, launchReadyBrowser, launchWithCustomArgs, detectBrowserCapabilities (every step here drives Puppeteer or executes JS in a real browser context).
 *
 * - closeBrowser (sends SIGTERM/SIGKILL to a real Chrome ChildProcess and waits for the exit event).
 *
 * - cleanupStalePages (browser.pages() + page.close()).
 *
 * - executeBrowserRestart (full restart cycle drives closeBrowser + getCurrentBrowser).
 *
 * - getBrowserPages (browser.pages() against a real session). The window-visibility executor is not deferred: its factory takes injected primitives and an
 *   injected page resolver, so windowSync.test.ts drives the whole loop with fakes and only the resolver wired in here needs a real browser.
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
