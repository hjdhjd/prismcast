/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * detection.test.ts: Unit tests for the install-method detection logic in detection.ts. Strategies are data records, so tests read constant fields directly
 * (HOMEBREW_STRATEGY.upgradeCommand) and exercise predicates by passing constructed DetectionContexts to matches/resolve. The dispatcher is tested separately
 * by calling detectInstallMethod with full contexts and verifying it routes to the right strategy.
 *
 * Cross-platform testing is first-class here. The PathHandle wrapper that currentFile uses accepts an explicit platform override, so a single Linux-hosted test
 * suite can construct Windows-flavored handles (backslash separators, case-insensitive matching) and verify the strategies behave correctly on Windows install
 * layouts, catching path-separator regressions on Windows before they reach users as bug reports.
 */
import type { DetectionContext, InstallStrategy } from "./detection.ts";
import { INSTALL_STRATEGIES, UNKNOWN_INSTALL, detectInstallMethod } from "./detection.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { createPathHandle } from "./pathHandle.ts";

/* ContextOverrides is the test-facing shape for makeContext - currentFile is a string (the test author writes raw paths, never PathHandles directly), and a
 * platform field selects POSIX or Windows semantics for the wrapping. Defaults below keep every other field in a no-match state so only the strategy under test
 * fires.
 */
interface ContextOverrides {

  readonly currentFile?: string;
  readonly fileExists?: (path: string) => boolean;
  readonly isContainer?: boolean;
  readonly platform?: NodeJS.Platform;
  readonly runCommand?: (cmd: string) => string | null;
}

/* makeContext builds a DetectionContext literal with sensible defaults that match no install strategy (path under /tmp/test/, no container, no npm prefix, no
 * file existence). Tests override the fields they care about; everything else stays in the no-match state so only the strategy under test fires. The platform
 * override flows through createPathHandle so a single test can produce a Windows handle on a Linux host (and vice versa) without touching process state.
 */
function makeContext(overrides: ContextOverrides = {}): DetectionContext {

  const platform: NodeJS.Platform = overrides.platform ?? "linux";

  return {

    currentFile: createPathHandle(overrides.currentFile ?? "/tmp/test/dist/upgrade/detection.js", { platform }),
    fileExists: overrides.fileExists ?? ((): boolean => false),
    isContainer: overrides.isContainer ?? false,
    runCommand: overrides.runCommand ?? ((): string | null => null)
  };
}

/* Looks up a strategy from the registry by id and asserts it exists. The registry is the SSOT for strategies, so tests grab references through it rather
 * than importing the per-strategy constants (which are intentionally module-private to keep INSTALL_STRATEGIES the only public surface).
 */
function strategyById(id: string): InstallStrategy {

  const found = INSTALL_STRATEGIES.find((s) => s.id === id);

  assert.ok(found, "strategy '" + id + "' must exist in INSTALL_STRATEGIES");

  return found;
}

describe("INSTALL_STRATEGIES registry", () => {

  test("contains exactly the four expected install methods in priority order", () => {

    // Order encodes priority: docker first (container probe is unambiguous), homebrew (path marker is unambiguous), npm-global (requires successful npm prefix
    // lookup), npm-local (broader path marker). Locking the order ensures a future contributor adding a new strategy thinks about where it belongs.
    const ids = INSTALL_STRATEGIES.map((s) => s.id);

    assert.deepEqual(ids, [ "docker", "homebrew", "npm-global", "npm-local" ]);
  });

  test("every strategy declares id, displayName, upgradeCommand, upgradeable, and matches", () => {

    // Locks the InstallStrategy contract: a future strategy that forgets one of the constant fields fails this test even before tsc would catch it (e.g., when
    // someone adds a strategy via Object.assign or an `as any` shortcut).
    for(const strategy of INSTALL_STRATEGIES) {

      assert.equal(typeof strategy.id, "string", "strategy must have a string id");
      assert.ok(strategy.id.length > 0, "strategy id must be non-empty");
      assert.equal(typeof strategy.displayName, "string", "strategy '" + strategy.id + "' must have a string displayName");
      assert.ok(strategy.displayName.length > 0, "strategy '" + strategy.id + "' displayName must be non-empty");
      assert.equal(typeof strategy.upgradeCommand, "string", "strategy '" + strategy.id + "' must have a string upgradeCommand");
      assert.ok(strategy.upgradeCommand.length > 0, "strategy '" + strategy.id + "' upgradeCommand must be non-empty");
      assert.equal(typeof strategy.upgradeable, "boolean", "strategy '" + strategy.id + "' must declare upgradeable as boolean");
      assert.equal(typeof strategy.matches, "function", "strategy '" + strategy.id + "' must have a matches predicate");
    }
  });

  test("each strategy declares the documented displayName", () => {

    // Locks the per-method labels that the upgrade command's status output emits. Operators read these strings; locking them here keeps display drift visible.
    const labels = Object.fromEntries(INSTALL_STRATEGIES.map((s) => [ s.id, s.displayName ]));

    assert.equal(labels["docker"], "Docker");
    assert.equal(labels["homebrew"], "Homebrew");
    assert.equal(labels["npm-global"], "npm (global)");
    assert.equal(labels["npm-local"], "npm (local)");
  });
});

describe("DOCKER strategy", () => {

  const strategy = strategyById("docker");

  test("declares id='docker', displayName='Docker', upgradeable=false, and the documented compose command", () => {

    // Locked verbatim: operators copy/paste this string. Drift in image name or compose subcommand would break documented workflows.
    assert.equal(strategy.id, "docker");
    assert.equal(strategy.displayName, "Docker");
    assert.equal(strategy.upgradeable, false, "containers must be replaced, not upgraded");
    assert.equal(strategy.upgradeCommand, "docker pull ghcr.io/hjdhjd/prismcast:latest && docker compose up -d");
  });

  test("matches when isContainer is true", () => {

    assert.equal(strategy.matches(makeContext({ isContainer: true })), true);
  });

  test("does not match when isContainer is false", () => {

    assert.equal(strategy.matches(makeContext({ isContainer: false })), false);
  });

  test("ignores the path - even a homebrew-shaped path is docker when isContainer is true", () => {

    // Negative test: docker takes precedence over path-based markers because inside a container the path-based check is meaningless. The matches predicate must
    // not examine currentFile at all.
    const matched = strategy.matches(makeContext({

      currentFile: "/usr/local/Cellar/prismcast/1.0.0/dist/upgrade/detection.js",
      isContainer: true
    }));

    assert.equal(matched, true, "container context wins regardless of path");
  });

  test("declares no resolver (constant fields are sufficient)", () => {

    // Boundary: docker has no context-derived fields. The resolve hook is intentionally absent so the dispatcher never invokes it for this method.
    assert.equal(strategy.resolve, undefined);
  });

  test("declares the documented manualUpgradeMessage lines", () => {

    // Locks the prose lines printed before the indented upgradeCommand. Operators read these strings; locking them here keeps display drift visible. The
    // upgradeable check uses assert.fail to bail on the unexpected branch, which doubles as type-narrowing past the discriminated union so manualUpgradeMessage
    // is in scope.
    if(strategy.upgradeable) {

      assert.fail("docker strategy must be non-upgradeable; did the registry change?");
    }

    assert.deepEqual(strategy.manualUpgradeMessage, [

      "Docker containers cannot be upgraded in-place.",
      "To upgrade, pull the latest image and recreate the container:"
    ]);
  });
});

describe("HOMEBREW strategy", () => {

  const strategy = strategyById("homebrew");

  test("declares id='homebrew', displayName='Homebrew', upgradeable=true, and the documented brew command", () => {

    assert.equal(strategy.id, "homebrew");
    assert.equal(strategy.displayName, "Homebrew");
    assert.equal(strategy.upgradeable, true);
    assert.equal(strategy.upgradeCommand, "brew update && brew upgrade prismcast");
  });

  test("matches a currentFile under /Cellar/prismcast/ on Intel Homebrew layout", () => {

    assert.equal(strategy.matches(makeContext({ currentFile: "/usr/local/Cellar/prismcast/1.10.0/dist/upgrade/detection.js" })), true);
  });

  test("matches a currentFile under /Cellar/prismcast/ on Apple Silicon Homebrew layout", () => {

    // Boundary: Apple Silicon's Homebrew prefix is /opt/homebrew, not /usr/local. The Cellar marker is the same, so detection works on both layouts.
    assert.equal(strategy.matches(makeContext({ currentFile: "/opt/homebrew/Cellar/prismcast/1.10.0/dist/upgrade/detection.js" })), true);
  });

  test("does not match when currentFile lacks the Cellar marker", () => {

    assert.equal(strategy.matches(makeContext({ currentFile: "/usr/local/lib/node_modules/prismcast/dist/upgrade/detection.js" })), false);
  });

  test("does not false-match an npm-on-Homebrew install at /opt/homebrew/lib/node_modules/", () => {

    // Negative test: npm installed under Homebrew-managed Node lives at /opt/homebrew/lib/node_modules/prismcast/, which contains /homebrew/ but NOT
    // /Cellar/prismcast/. The marker is intentionally narrow to avoid this false positive.
    assert.equal(strategy.matches(makeContext({ currentFile: "/opt/homebrew/lib/node_modules/prismcast/dist/upgrade/detection.js" })), false);
  });
});

describe("NPM_GLOBAL strategy", () => {

  const strategy = strategyById("npm-global");

  test("declares id='npm-global', displayName='npm (global)', upgradeable=true, and the documented npm install command", () => {

    assert.equal(strategy.id, "npm-global");
    assert.equal(strategy.displayName, "npm (global)");
    assert.equal(strategy.upgradeable, true);
    assert.equal(strategy.upgradeCommand, "npm install -g prismcast@latest");
  });

  test("matches when currentFile starts with the npm global prefix", () => {

    const matched = strategy.matches(makeContext({

      currentFile: "/usr/local/lib/node_modules/prismcast/dist/upgrade/detection.js",
      runCommand: (cmd) => (cmd === "npm prefix -g") ? "/usr/local" : null
    }));

    assert.equal(matched, true);
  });

  test("does not match when runCommand fails (npm not on PATH, timeout, etc.)", () => {

    // Defensive: runCommand returns null on any failure. The strategy must defer cleanly rather than throw.
    const matched = strategy.matches(makeContext({

      currentFile: "/usr/local/lib/node_modules/prismcast/dist/upgrade/detection.js",
      runCommand: (): null => null
    }));

    assert.equal(matched, false);
  });

  test("does not match when runCommand returns an empty string", () => {

    // Boundary: empty stdout is treated the same as a failure - we cannot anchor a path against an empty prefix.
    const matched = strategy.matches(makeContext({

      currentFile: "/usr/local/lib/node_modules/prismcast/dist/upgrade/detection.js",
      runCommand: (): string => ""
    }));

    assert.equal(matched, false);
  });

  test("does not match when currentFile is outside the reported prefix", () => {

    // Negative: npm prefix succeeds and returns a valid path, but our currentFile is somewhere else (e.g., a dev checkout). Strategy must defer.
    const matched = strategy.matches(makeContext({

      currentFile: "/Users/me/projects/prismcast/dist/upgrade/detection.js",
      runCommand: (): string => "/usr/local"
    }));

    assert.equal(matched, false);
  });

  test("queries npm with the exact 'npm prefix -g' command", () => {

    // Locks the command string. A future change to use `npm config get prefix` would be a deliberate decision and should fail this test until updated.
    const seenCommands: string[] = [];

    strategy.matches(makeContext({

      currentFile: "/usr/local/lib/node_modules/prismcast/dist/upgrade/detection.js",
      runCommand: (cmd: string): string => {

        seenCommands.push(cmd);

        return "/usr/local";
      }
    }));

    assert.deepEqual(seenCommands, ["npm prefix -g"]);
  });

  test("matches a Windows path under the reported npm global prefix (the v1.10.2 detection-on-Windows regression)", () => {

    // This Windows install layout has Node's global prefix at "%AppData%\\npm" with PrismCast living under that directory. hasSegmentChain matches this
    // layout because isUnder dispatches through path.win32's separator, so the npm-global strategy fires correctly on the backslash-delimited path and
    // returns the matching InstallInfo instead of falling through to the unknown sentinel.
    const matched = strategy.matches(makeContext({

      currentFile: "C:\\Users\\jp\\AppData\\Roaming\\npm\\node_modules\\prismcast\\dist\\upgrade\\detection.js",
      platform: "win32",
      runCommand: (cmd) => (cmd === "npm prefix -g") ? "C:\\Users\\jp\\AppData\\Roaming\\npm" : null
    }));

    assert.equal(matched, true, "Windows npm-global must detect when prefix and currentFile agree under backslash semantics");
  });

  test("matches a Windows path even when prefix and currentFile use mismatched casing (case-insensitive Windows comparison)", () => {

    // Windows filesystems are case-insensitive at the OS layer, so "C:\\users\\jp" and "C:\\Users\\jp" denote the same directory. The PathHandle isUnder check
    // case-folds both sides on win32 to match that OS semantic. A future change that re-introduced a case-sensitive comparison would silently break installs
    // whose npm prefix casing happens to differ from the canonical casing in import.meta.url; this test locks the behavior.
    const matched = strategy.matches(makeContext({

      currentFile: "C:\\Users\\JP\\AppData\\Roaming\\npm\\node_modules\\prismcast\\dist\\upgrade\\detection.js",
      platform: "win32",
      runCommand: (): string => "c:\\users\\jp\\appdata\\roaming\\npm"
    }));

    assert.equal(matched, true);
  });

  test("does not match when the Windows path is on a different drive than the reported prefix", () => {

    // Negative: an npm prefix on C: with a currentFile on D: cannot be an npm-global install. The structural is-under check rejects this cleanly because the
    // drive segment is part of the segment list and never folds across drives.
    const matched = strategy.matches(makeContext({

      currentFile: "D:\\dev\\node_modules\\prismcast\\dist\\upgrade\\detection.js",
      platform: "win32",
      runCommand: (): string => "C:\\Users\\jp\\AppData\\Roaming\\npm"
    }));

    assert.equal(matched, false);
  });
});

describe("NPM_LOCAL strategy", () => {

  const strategy = strategyById("npm-local");

  test("declares id='npm-local', displayName='npm (local)', upgradeable=true, and the documented npm install command", () => {

    assert.equal(strategy.id, "npm-local");
    assert.equal(strategy.displayName, "npm (local)");
    assert.equal(strategy.upgradeable, true);
    assert.equal(strategy.upgradeCommand, "npm install prismcast@latest");
  });

  test("matches when currentFile contains /node_modules/prismcast/", () => {

    assert.equal(strategy.matches(makeContext({ currentFile: "/Users/me/myproject/node_modules/prismcast/dist/upgrade/detection.js" })), true);
  });

  test("does not match when currentFile lacks the node_modules marker", () => {

    assert.equal(strategy.matches(makeContext({ currentFile: "/Users/me/dev/prismcast/dist/upgrade/detection.js" })), false);
  });

  test("resolver populates packageDir when the extracted project root has a package.json", () => {

    // The resolver is invoked only after matches returns true; it computes the consumer's project root - the directory above node_modules - which the upgrade
    // runner uses as cwd for `npm install`. Verify the strategy hands the right path to fileExists and surfaces the matched root.
    const queriedPaths: string[] = [];

    const ctx = makeContext({

      currentFile: "/Users/me/myproject/node_modules/prismcast/dist/upgrade/detection.js",
      fileExists: (p) => {

        queriedPaths.push(p);

        return p === "/Users/me/myproject/package.json";
      }
    });

    const partial = strategy.resolve!(ctx);

    assert.equal(partial.packageDir, "/Users/me/myproject");
    assert.deepEqual(queriedPaths, ["/Users/me/myproject/package.json"], "fileExists must be queried for the manifest at the project root");
  });

  test("resolver leaves packageDir undefined when the project root has no package.json", () => {

    // Boundary: an orphaned node_modules without a manifest still classifies as npm-local (so the user gets the right command), but packageDir stays undefined
    // so the runner uses cwd=process.cwd() instead.
    const partial = strategy.resolve!(makeContext({

      currentFile: "/orphan/node_modules/prismcast/dist/upgrade/detection.js",
      fileExists: (): boolean => false
    }));

    assert.equal(partial.packageDir, undefined);
  });

  test("matches a Windows path containing node_modules\\prismcast", () => {

    // hasSegmentChain matches the "node_modules" + "prismcast" segment chain against a backslash-separated path because it dispatches through path.win32's
    // separator, so the strategy fires correctly on this Windows layout. Locking the behavior with an explicit win32 fixture.
    const matched = strategy.matches(makeContext({

      currentFile: "C:\\Users\\jp\\my-app\\node_modules\\prismcast\\dist\\upgrade\\detection.js",
      platform: "win32"
    }));

    assert.equal(matched, true);
  });

  test("resolver extracts a Windows project root with backslash separators and queries the manifest at the right path", () => {

    // Locks the Windows analogue of the POSIX resolver test above. parentBefore on a win32 PathHandle returns a handle whose raw path uses backslashes; the
    // resolver's join("package.json") composes the manifest path with the same separator; fileExists sees a clean Windows path. This is exactly the contract the
    // upgrade runner needs because npm install on Windows expects a Windows-style cwd.
    const queriedPaths: string[] = [];

    const ctx = makeContext({

      currentFile: "C:\\Users\\jp\\my-app\\node_modules\\prismcast\\dist\\upgrade\\detection.js",
      fileExists: (p) => {

        queriedPaths.push(p);

        return p === "C:\\Users\\jp\\my-app\\package.json";
      },
      platform: "win32"
    });

    const partial = strategy.resolve!(ctx);

    assert.equal(partial.packageDir, "C:\\Users\\jp\\my-app");
    assert.deepEqual(queriedPaths, ["C:\\Users\\jp\\my-app\\package.json"], "fileExists must be queried with a Windows-style manifest path");
  });
});

describe("detectInstallMethod (dispatcher)", () => {

  test("returns the docker InstallInfo when isContainer is true", () => {

    const info = detectInstallMethod(makeContext({ isContainer: true }));

    assert.equal(info.method, "docker");
    assert.equal(info.displayName, "Docker");
    assert.equal(info.upgradeCommand, "docker pull ghcr.io/hjdhjd/prismcast:latest && docker compose up -d");

    // Locks the dispatcher's contract that manualUpgradeMessage is copied off the matching strategy onto the resulting InstallInfo. assert.fail on the
    // unexpected branch narrows the InstallInfo union past the upgradeable tag so the message field is in scope below.
    if(info.upgradeable) {

      assert.fail("docker InstallInfo must be non-upgradeable");
    }

    assert.deepEqual(info.manualUpgradeMessage, [

      "Docker containers cannot be upgraded in-place.",
      "To upgrade, pull the latest image and recreate the container:"
    ]);
  });

  test("returns the homebrew InstallInfo for a Homebrew-shaped path", () => {

    const info = detectInstallMethod(makeContext({ currentFile: "/opt/homebrew/Cellar/prismcast/1.10.0/dist/upgrade/detection.js" }));

    assert.equal(info.method, "homebrew");
    assert.equal(info.displayName, "Homebrew");
    assert.equal(info.upgradeable, true);
    assert.equal(info.upgradeCommand, "brew update && brew upgrade prismcast");
  });

  test("returns the npm-global InstallInfo when runCommand confirms the prefix", () => {

    const info = detectInstallMethod(makeContext({

      currentFile: "/usr/local/lib/node_modules/prismcast/dist/upgrade/detection.js",
      runCommand: (cmd) => (cmd === "npm prefix -g") ? "/usr/local" : null
    }));

    assert.equal(info.method, "npm-global");
    assert.equal(info.upgradeable, true);
    assert.equal(info.upgradeCommand, "npm install -g prismcast@latest");
  });

  test("returns the npm-local InstallInfo with packageDir resolved when the project root has a package.json", () => {

    const info = detectInstallMethod(makeContext({

      currentFile: "/Users/me/proj/node_modules/prismcast/dist/upgrade/detection.js",
      fileExists: (): boolean => true
    }));

    assert.equal(info.method, "npm-local");
    assert.equal(info.upgradeable, true);
    assert.equal(info.upgradeCommand, "npm install prismcast@latest");
    assert.equal(info.packageDir, "/Users/me/proj");
  });

  test("returns the npm-local InstallInfo without packageDir when the project root has no package.json", () => {

    const info = detectInstallMethod(makeContext({

      currentFile: "/orphan/node_modules/prismcast/dist/upgrade/detection.js",
      fileExists: (): boolean => false
    }));

    assert.equal(info.method, "npm-local");
    assert.equal(info.packageDir, undefined);
  });

  test("returns UNKNOWN_INSTALL when no strategy matches", () => {

    const info = detectInstallMethod(makeContext());

    assert.equal(info, UNKNOWN_INSTALL, "unknown sentinel is returned by reference");
    assert.equal(info.method, "unknown");
    assert.equal(info.displayName, "Unknown");
    assert.equal(info.upgradeable, false);
    assert.equal(info.upgradeCommand, "npm install -g prismcast@latest", "unknown surfaces the npm-global command as a manual fallback");
  });

  test("respects priority order: docker wins over a homebrew-shaped path", () => {

    // Negative test for priority: even with a homebrew-shaped currentFile, isContainer=true takes precedence because docker is first in the registry.
    const info = detectInstallMethod(makeContext({

      currentFile: "/opt/homebrew/Cellar/prismcast/1.10.0/dist/upgrade/detection.js",
      isContainer: true
    }));

    assert.equal(info.method, "docker");
  });

  test("respects priority order: homebrew wins over an npm-global-shaped path", () => {

    // currentFile starts with /opt/homebrew which would also match an npm-global probe rooted at /opt/homebrew, but homebrew fires first because it's listed
    // before npm-global in the registry and short-circuits via the /Cellar/prismcast/ marker.
    const info = detectInstallMethod(makeContext({

      currentFile: "/opt/homebrew/Cellar/prismcast/1.10.0/dist/upgrade/detection.js",
      runCommand: (cmd) => (cmd === "npm prefix -g") ? "/opt/homebrew" : null
    }));

    assert.equal(info.method, "homebrew", "Cellar marker wins over npm-global prefix match");
  });

  test("respects priority order: npm-global wins over an npm-local-shaped path", () => {

    // Both strategies would match a path like /usr/local/lib/node_modules/prismcast/...; npm-global fires first because the dispatcher walks the registry in
    // order and npm-global is listed before npm-local.
    const info = detectInstallMethod(makeContext({

      currentFile: "/usr/local/lib/node_modules/prismcast/dist/upgrade/detection.js",
      runCommand: (cmd) => (cmd === "npm prefix -g") ? "/usr/local" : null
    }));

    assert.equal(info.method, "npm-global");
  });

  test("upgradeable InstallInfos carry no manualUpgradeMessage (union-shape invariant)", () => {

    // Locks the guarantee the discriminated union buys us: the dispatcher cannot accidentally attach manualUpgradeMessage to an upgradeable variant.
    // Verified at runtime by reading the (typed-undefined) field; the property name is permitted in the cast because the union explicitly forbids it on the
    // upgradeable branch, so this test is the runtime mirror of that compile-time guarantee.
    const upgradeableContexts: Record<string, DetectionContext> = {

      "homebrew": makeContext({ currentFile: "/opt/homebrew/Cellar/prismcast/1.10.0/x.js" }),
      "npm-global": makeContext({ currentFile: "/usr/local/lib/node_modules/prismcast/x.js", runCommand: () => "/usr/local" }),
      "npm-local": makeContext({ currentFile: "/p/node_modules/prismcast/x.js", fileExists: () => true })
    };

    for(const [ id, ctx ] of Object.entries(upgradeableContexts)) {

      const info = detectInstallMethod(ctx);

      assert.equal(info.upgradeable, true, "context for '" + id + "' must produce an upgradeable InstallInfo");
      assert.equal((info as { manualUpgradeMessage?: readonly string[] }).manualUpgradeMessage, undefined,
        "upgradeable strategy '" + id + "' must not carry manualUpgradeMessage");
    }
  });

  test("dispatcher's method field always equals the strategy's id", () => {

    // This locks the guarantee the strategy-registry design buys us: the dispatcher writes method:strategy.id once, so a strategy cannot accidentally
    // produce an InstallInfo with a method that disagrees with its own id.
    const ctxByStrategy: Record<string, DetectionContext> = {

      "docker": makeContext({ isContainer: true }),
      "homebrew": makeContext({ currentFile: "/opt/homebrew/Cellar/prismcast/1.10.0/x.js" }),
      "npm-global": makeContext({

        currentFile: "/usr/local/lib/node_modules/prismcast/x.js",
        runCommand: () => "/usr/local"
      }),
      "npm-local": makeContext({

        currentFile: "/p/node_modules/prismcast/x.js",
        fileExists: () => true
      })
    };

    for(const strategy of INSTALL_STRATEGIES) {

      const ctx = ctxByStrategy[strategy.id]!;
      const info = detectInstallMethod(ctx);

      assert.equal(info.method, strategy.id, "method field for strategy '" + strategy.id + "' must equal its id");
    }
  });

  test("uses the default context when called with no argument", () => {

    // The default-argument calls createDefaultDetectionContext() which reads import.meta.url. We don't assert on the resulting method (it depends on where the
    // test runtime lives), but we verify the call succeeds and returns a documented method.
    const info = detectInstallMethod();
    const allowed = new Set([ "docker", "homebrew", "npm-global", "npm-local", "unknown" ]);

    assert.ok(allowed.has(info.method), "default-context detection produces a documented method");
    assert.equal(typeof info.upgradeable, "boolean");
    assert.ok(info.upgradeCommand.length > 0, "upgradeCommand is always non-empty");
  });

  test("routes a Windows npm-global layout to the npm-global InstallInfo end-to-end (v1.10.2 regression lock)", () => {

    // End-to-end coverage for a Windows npm-global layout: hasSegmentChain and isUnder dispatch through path.win32's separator, so the predicate matches
    // this backslash-delimited path correctly and the dispatcher routes it to the npm-global InstallInfo. Locking the dispatcher's output (not just the
    // predicate's boolean) catches any future regression that breaks the wiring at a different layer (e.g., if the dispatcher stopped composing the
    // resolved fields correctly).
    const info = detectInstallMethod(makeContext({

      currentFile: "C:\\Users\\jp\\AppData\\Roaming\\npm\\node_modules\\prismcast\\dist\\upgrade\\detection.js",
      platform: "win32",
      runCommand: (cmd) => (cmd === "npm prefix -g") ? "C:\\Users\\jp\\AppData\\Roaming\\npm" : null
    }));

    assert.equal(info.method, "npm-global");
    assert.equal(info.displayName, "npm (global)");
    assert.equal(info.upgradeable, true);
    assert.equal(info.upgradeCommand, "npm install -g prismcast@latest");
  });

  test("routes a Windows npm-local layout to the npm-local InstallInfo with a Windows-style packageDir", () => {

    // Companion to the regression test above. Verifies that an npm-local layout on Windows (a developer working on a project that depends on prismcast)
    // resolves packageDir as a backslash-separated Windows path...the npm install runner uses this as cwd, so it must round-trip through fileExists and into
    // the InstallInfo without any cross-platform normalization that would confuse npm on Windows.
    const info = detectInstallMethod(makeContext({

      currentFile: "C:\\Users\\jp\\my-app\\node_modules\\prismcast\\dist\\upgrade\\detection.js",
      fileExists: (p) => p === "C:\\Users\\jp\\my-app\\package.json",
      platform: "win32"
    }));

    assert.equal(info.method, "npm-local");
    assert.equal(info.upgradeable, true);
    assert.equal(info.packageDir, "C:\\Users\\jp\\my-app");
  });
});

describe("UNKNOWN_INSTALL sentinel", () => {

  test("declares method 'unknown', displayName 'Unknown', upgradeable=false", () => {

    assert.equal(UNKNOWN_INSTALL.method, "unknown");
    assert.equal(UNKNOWN_INSTALL.displayName, "Unknown");
    assert.equal(UNKNOWN_INSTALL.upgradeable, false);
  });

  test("upgradeCommand mirrors the npm-global instruction as a manual fallback", () => {

    assert.equal(UNKNOWN_INSTALL.upgradeCommand, "npm install -g prismcast@latest");
  });

  test("has no packageDir", () => {

    assert.equal(UNKNOWN_INSTALL.packageDir, undefined);
  });

  test("declares the documented manualUpgradeMessage", () => {

    // The unknown sentinel carries its own one-line message because it has no registered strategy to inherit from. Locked because operators see this string
    // verbatim when the tool cannot figure out how PrismCast was installed.
    assert.deepEqual(UNKNOWN_INSTALL.manualUpgradeMessage, ["Unable to detect installation method. Please upgrade manually:"]);
  });
});
