/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * generators.paths.test.ts: Unit tests for getServicePaths (happy paths + null/error branches + injected-IO variants) and detectStalePaths (default + injected-IO
 * variants). Pure leaves (collectServiceEnvironment, buildServiceDefinition, getServiceGenerator dispatch, ServiceDefinition shape contract) live in
 * generators.test.ts; platform-specific generators live in generators.launchAgent.test.ts / generators.systemd.test.ts / generators.windowsTask.test.ts.
 */
import { afterEach, before, describe, mock, test } from "node:test";
import { detectStalePaths, getServicePaths } from "./generators.ts";
import assert from "node:assert/strict";
import fs from "node:fs";
import { initializeDataDir } from "../config/paths.ts";
import { makeFakeIO } from "./generators.helpers.ts";
import os from "node:os";
import path from "node:path";

const ORIGINAL_PLATFORM = process.platform;

function setPlatform(value: string): void {

  Object.defineProperty(process, "platform", {

    configurable: true,
    value
  });
}

/* withParserStubs runs the callback with mock.method-stubbed fs.existsSync and fs.readFileSync. null content means existsSync should report false (file missing).
 * An Error means readFileSync should throw it. A string is the file body.
 */
function withParserStubs<T>(content: string | Error | null, run: () => T): T {

  const exists = (content !== null);
  const existsStub = mock.method(fs, "existsSync", () => exists);
  let readStub: ReturnType<typeof mock.method> | null = null;

  if(exists) {

    readStub = mock.method(fs, "readFileSync", () => {

      if(content instanceof Error) {

        throw content;
      }

      return content;
    });
  }

  try {

    return run();
  } finally {

    existsStub.mock.restore();
    readStub?.mock.restore();
  }
}

const FAKE_PLIST = [
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
  "<plist version=\"1.0\">",
  "<dict>",
  "  <key>Label</key>",
  "  <string>com.github.hjdhjd.prismcast</string>",
  "  <key>ProgramArguments</key>",
  "  <array>",
  "    <string>/opt/homebrew/bin/node</string>",
  "    <string>/opt/homebrew/lib/node_modules/prismcast/dist/index.js</string>",
  "  </array>",
  "</dict>",
  "</plist>"
].join("\n");

const FAKE_UNIT = [
  "[Unit]",
  "Description=PrismCast Streaming Server",
  "[Service]",
  "Type=simple",
  "ExecStart=/usr/bin/node /opt/prismcast/dist/index.js",
  "WorkingDirectory=/opt/prismcast",
  ""
].join("\n");

const FAKE_LAUNCHER = [
  "\u{FEFF}# PrismCast service launcher.",
  "# node: C:\\Program Files\\nodejs\\node.exe",
  "# entry: C:\\Users\\test\\AppData\\Roaming\\npm\\node_modules\\prismcast\\dist\\index.js",
  "",
  "$env:PRISMCAST_SERVICE = '1'",
  "Start-Process node.exe"
].join("\r\n");

before(() => {

  initializeDataDir(path.join(os.tmpdir(), "prismcast-generators-paths-test"));
});

describe("getServicePaths happy paths (per platform)", () => {

  afterEach(() => {

    setPlatform(ORIGINAL_PLATFORM);
  });

  test("darwin: extracts the first two ProgramArguments strings as node and entry point", () => {

    setPlatform("darwin");

    const result = withParserStubs(FAKE_PLIST, () => getServicePaths());

    assert.ok(result, "parser should produce a non-null result");
    assert.equal(result.nodePath, "/opt/homebrew/bin/node");
    assert.equal(result.entryPoint, "/opt/homebrew/lib/node_modules/prismcast/dist/index.js");
  });

  test("linux: extracts node and entry-point paths from ExecStart", () => {

    setPlatform("linux");

    const result = withParserStubs(FAKE_UNIT, () => getServicePaths());

    assert.ok(result, "parser should produce a non-null result");
    assert.equal(result.nodePath, "/usr/bin/node");
    assert.equal(result.entryPoint, "/opt/prismcast/dist/index.js");
  });

  test("windows: extracts node and entry from the # node: and # entry: comment metadata", () => {

    setPlatform("win32");

    const result = withParserStubs(FAKE_LAUNCHER, () => getServicePaths());

    assert.ok(result, "parser should produce a non-null result");
    assert.equal(result.nodePath, "C:\\Program Files\\nodejs\\node.exe");
    assert.equal(result.entryPoint, "C:\\Users\\test\\AppData\\Roaming\\npm\\node_modules\\prismcast\\dist\\index.js");
  });
});

describe("getServicePaths null/error branches (per platform)", () => {

  afterEach(() => {

    setPlatform(ORIGINAL_PLATFORM);
  });

  test("darwin: returns null when ProgramArguments key is absent", () => {

    setPlatform("darwin");
    assert.equal(withParserStubs("<?xml version=\"1.0\"?><plist><dict></dict></plist>", () => getServicePaths()), null);
  });

  test("linux: returns null when ExecStart is absent (malformed unit)", () => {

    setPlatform("linux");
    assert.equal(withParserStubs("[Unit]\nDescription=Broken\n", () => getServicePaths()), null);
  });

  test("windows: returns null when both metadata comments are absent", () => {

    setPlatform("win32");
    assert.equal(withParserStubs("# Some other comment\nStart-Process node.exe", () => getServicePaths()), null);
  });

  test("windows: returns null when only one of the two metadata lines is present", () => {

    // Boundary: the parser requires both # node: and # entry: lines - either alone is a malformed launcher.
    setPlatform("win32");
    assert.equal(withParserStubs("# node: C:\\node.exe\nStart-Process node.exe", () => getServicePaths()), null);
  });

  test("returns null when the service file does not exist (existsSync=false)", () => {

    setPlatform("linux");
    assert.equal(withParserStubs(null, () => getServicePaths()), null);
  });

  test("returns null when readFileSync throws (e.g., permission denied)", () => {

    setPlatform("linux");
    assert.equal(withParserStubs(new Error("EACCES: permission denied"), () => getServicePaths()), null, "read failure must produce null, not throw");
  });
});

/* withStaleStubs orchestrates the multi-call existsSync sequence detectStalePaths needs. The first existsSync call inside getServicePaths checks the service file
 * itself; the next two calls check the nodePath and entryPoint extracted from it. We script the three boolean returns so each test can assert a specific stale
 * topology (both stale, neither stale, only entry stale, etc.).
 */

function withStaleStubs<T>(unit: string, existsResults: boolean[], run: () => T): T {

  let callCount = 0;
  const existsStub = mock.method(fs, "existsSync", () => existsResults[callCount++] ?? false);
  const readStub = mock.method(fs, "readFileSync", () => unit);

  try {

    return run();
  } finally {

    existsStub.mock.restore();
    readStub.mock.restore();
  }
}

describe("detectStalePaths", () => {

  afterEach(() => {

    setPlatform(ORIGINAL_PLATFORM);
  });

  test("returns null when the service file does not exist", () => {

    setPlatform("linux");

    const existsStub = mock.method(fs, "existsSync", () => false);

    try {

      assert.equal(detectStalePaths(), null);
    } finally {

      existsStub.mock.restore();
    }
  });

  test("reports stale=true when both node and entry are missing on disk", () => {

    setPlatform("linux");

    // existsSync calls: [service file=true, nodePath=false, entryPoint=false].
    const result = withStaleStubs("ExecStart=/missing/node /missing/index.js\n", [ true, false, false ], () => detectStalePaths());

    assert.ok(result, "stale detection should produce a non-null result");
    assert.equal(result.stale, true);
    assert.equal(result.nodePath, "/missing/node");
    assert.equal(result.entryPoint, "/missing/index.js");
  });

  test("reports stale=false when both paths exist on disk", () => {

    setPlatform("linux");

    const result = withStaleStubs("ExecStart=/present/node /present/index.js\n", [ true, true, true ], () => detectStalePaths());

    assert.ok(result, "stale detection should produce a non-null result");
    assert.equal(result.stale, false);
    assert.equal(result.nodePath, undefined, "nodePath only present when stale");
    assert.equal(result.entryPoint, undefined, "entryPoint only present when stale");
  });

  test("reports only the entryPoint as stale when node exists but entry does not", () => {

    setPlatform("linux");

    // existsSync calls: [service file=true, nodePath=true, entryPoint=false].
    const result = withStaleStubs("ExecStart=/present/node /missing/index.js\n", [ true, true, false ], () => detectStalePaths());

    assert.ok(result, "stale detection should produce a non-null result");
    assert.equal(result.stale, true);
    assert.equal(result.nodePath, undefined, "node was present, so nodePath field is undefined");
    assert.equal(result.entryPoint, "/missing/index.js", "entry was missing, so it surfaces");
  });
});

describe("getServicePaths - with injected IO", () => {

  test("returns null when the service file does not exist", () => {

    const { io } = makeFakeIO({ existing: {}, platform: "darwin" });

    assert.equal(getServicePaths(io), null);
  });

  test("parses the launchd plist and returns nodePath + entryPoint", () => {

    const installPath = "/Users/test/Library/LaunchAgents/com.prismcast.plist";
    const plist = [
      "<?xml version=\"1.0\"?>",
      "<plist>",
      "<dict>",
      "  <key>ProgramArguments</key>",
      "  <array>",
      "    <string>/opt/homebrew/bin/node</string>",
      "    <string>/opt/homebrew/lib/prismcast/dist/index.js</string>",
      "  </array>",
      "</dict>",
      "</plist>"
    ].join("\n");
    const { io } = makeFakeIO({

      existing: { [installPath]: true },
      files: { [installPath]: plist },
      platform: "darwin",
      serviceFilePath: installPath
    });

    const result = getServicePaths(io);

    assert.deepEqual(result, {

      entryPoint: "/opt/homebrew/lib/prismcast/dist/index.js",
      nodePath: "/opt/homebrew/bin/node"
    });
  });

  test("returns null when a launchd plist lacks the ProgramArguments block", () => {

    const installPath = "/Users/test/Library/LaunchAgents/com.prismcast.plist";
    const { io } = makeFakeIO({

      existing: { [installPath]: true },
      files: { [installPath]: "<?xml version=\"1.0\"?><plist><dict></dict></plist>" },
      platform: "darwin",
      serviceFilePath: installPath
    });

    assert.equal(getServicePaths(io), null);
  });

  test("parses the systemd unit's ExecStart line", () => {

    const installPath = "/Users/test/.config/systemd/user/prismcast.service";
    const unit = [
      "[Unit]",
      "Description=PrismCast",
      "[Service]",
      "ExecStart=/usr/local/bin/node /usr/local/lib/prismcast/dist/index.js",
      "Restart=always"
    ].join("\n");
    const { io } = makeFakeIO({

      existing: { [installPath]: true },
      files: { [installPath]: unit },
      platform: "linux",
      serviceFilePath: installPath
    });

    const result = getServicePaths(io);

    assert.deepEqual(result, {

      entryPoint: "/usr/local/lib/prismcast/dist/index.js",
      nodePath: "/usr/local/bin/node"
    });
  });

  test("parses the Windows launcher's '# node:' and '# entry:' metadata comments", () => {

    const launcherPath = "C:\\ProgramData\\PrismCast\\prismcast-service.ps1";
    const launcher = [
      "# PrismCast service launcher.",
      "# node: C:\\Program Files\\nodejs\\node.exe",
      "# entry: C:\\Users\\test\\AppData\\Roaming\\npm\\node_modules\\prismcast\\dist\\index.js",
      "$env:PRISMCAST_SERVICE = '1'",
      "$startArgs = @{}",
      "Start-Process @startArgs"
    ].join("\r\n");
    const { io } = makeFakeIO({

      existing: { [launcherPath]: true },
      files: { [launcherPath]: launcher },
      platform: "windows",
      serviceFilePath: launcherPath
    });

    const result = getServicePaths(io);

    assert.deepEqual(result, {

      entryPoint: "C:\\Users\\test\\AppData\\Roaming\\npm\\node_modules\\prismcast\\dist\\index.js",
      nodePath: "C:\\Program Files\\nodejs\\node.exe"
    });
  });
});

describe("detectStalePaths - with injected IO", () => {

  test("returns null when there is no service file", () => {

    const { io } = makeFakeIO({ existing: {}, platform: "darwin" });

    assert.equal(detectStalePaths(io), null);
  });

  test("returns stale: false when both paths still exist", () => {

    const installPath = "/Users/test/Library/LaunchAgents/com.prismcast.plist";
    const nodePath = "/usr/local/bin/node";
    const entryPoint = "/usr/local/lib/prismcast/dist/index.js";
    const plist = "<key>ProgramArguments</key><array><string>" + nodePath + "</string><string>" + entryPoint + "</string></array>";
    const { io } = makeFakeIO({

      existing: { [entryPoint]: true, [installPath]: true, [nodePath]: true },
      files: { [installPath]: plist },
      platform: "darwin",
      serviceFilePath: installPath
    });

    const result = detectStalePaths(io);

    assert.ok(result, "detectStalePaths returned a result");
    assert.equal(result.stale, false);
    assert.equal(result.entryPoint, undefined, "no stale entryPoint surfaced when path exists");
    assert.equal(result.nodePath, undefined, "no stale nodePath surfaced when path exists");
  });

  test("returns stale: true with the nodePath surfaced when the node binary is gone", () => {

    const installPath = "/Users/test/Library/LaunchAgents/com.prismcast.plist";
    const nodePath = "/old/node";
    const entryPoint = "/usr/local/lib/prismcast/dist/index.js";
    const plist = "<key>ProgramArguments</key><array><string>" + nodePath + "</string><string>" + entryPoint + "</string></array>";
    const { io } = makeFakeIO({

      existing: { [entryPoint]: true, [installPath]: true },
      files: { [installPath]: plist },
      platform: "darwin",
      serviceFilePath: installPath
    });

    const result = detectStalePaths(io);

    assert.ok(result, "detectStalePaths should return a result when paths are stale");
    assert.equal(result.stale, true);
    assert.equal(result.nodePath, nodePath, "stale node path is reported");
    assert.equal(result.entryPoint, undefined, "live entry point is not flagged");
  });
});
