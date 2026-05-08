/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * generators.windowsTask.test.ts: Unit tests for createWindowsSchedulerGenerator (the Windows Task Scheduler generator). Pure leaves live in generators.test.ts;
 * getServicePaths and detectStalePaths variants live in generators.paths.test.ts; macOS LaunchAgent lives in generators.launchAgent.test.ts; Linux systemd lives
 * in generators.systemd.test.ts.
 */
import { definitionFixture, makeFakeIO } from "./generators.helpers.ts";
import { describe, test } from "node:test";
import { execFileAlwaysSucceeds, firstOf, makeExecFileError, nthOf } from "../testing.helpers.ts";
import assert from "node:assert/strict";
import { getServiceGenerator } from "./generators.ts";

describe("createWindowsSchedulerGenerator (via getServiceGenerator on windows)", () => {

  test("install: writes the launcher .ps1 with BOM, registers the task, and starts it", async () => {

    const launcherPath = "C:\\ProgramData\\PrismCast\\prismcast-service.ps1";
    const { calls, io, writes } = makeFakeIO({

      // PowerShell invocations during install carry full script bodies whose exact text depends on runtime data; keying them in a record would be brittle. The
      // wildcard execFile lets every powershell.exe call succeed uniformly while makeFakeIO records the call for sequence assertions below.
      execFile: execFileAlwaysSucceeds(),
      platform: "windows",
      serviceFileDirectory: "C:\\ProgramData\\PrismCast",
      serviceFilePath: launcherPath
    });

    await getServiceGenerator(io)?.install(definitionFixture());

    // Verify the launcher was written with a BOM and contains the metadata comments + Start-Process invocation.
    assert.equal(writes.length, 1);

    const launcherWrite = firstOf(writes, "launcher write");

    assert.equal(launcherWrite.path, launcherPath);
    assert.equal(launcherWrite.content.charCodeAt(0), 0xFEFF, "launcher starts with UTF-8 BOM");
    assert.match(launcherWrite.content, /^# node: \/usr\/local\/bin\/node$/m);
    assert.match(launcherWrite.content, /^# entry: \/usr\/local\/lib\/prismcast\/dist\/index\.js$/m);
    assert.match(launcherWrite.content, /Start-Process @startArgs/);

    // Verify two PowerShell invocations: register, then start. (The legacy artifact removal happens via rm, not execFile.)
    const psCalls = calls.filter((c) => (c.method === "execFile") && (c.args[0] === "powershell.exe"));

    assert.equal(psCalls.length, 2);

    const registerCall = nthOf(psCalls, 0, "PowerShell call");
    const startCall = nthOf(psCalls, 1, "PowerShell call");

    // The full command string is the last argv element; it should reference Register-ScheduledTask in the first call.
    assert.match(String(registerCall.args.at(-1)), /Register-ScheduledTask/);
    assert.match(String(startCall.args.at(-1)), /Start-ScheduledTask/);
  });

  test("isInstalled: returns true when the IS_INSTALLED PowerShell script exits 0", async () => {

    const { io } = makeFakeIO({ execFile: execFileAlwaysSucceeds(), platform: "windows" });

    assert.equal(await getServiceGenerator(io)?.isInstalled(), true);
  });

  test("isInstalled: returns false when the IS_INSTALLED PowerShell script throws (non-zero exit)", async () => {

    // The wildcard execFile here always throws an Error in the shape promisified execFile produces on a non-zero exit (utf8 strings for stdout/stderr).
    const { io } = makeFakeIO({

      execFile: async (): Promise<{ stderr: string; stdout: string }> => {

        throw makeExecFileError("fake-throw", "", "");
      },
      platform: "windows"
    });

    assert.equal(await getServiceGenerator(io)?.isInstalled(), false);
  });

  test("isRunning: returns true when the IS_RUNNING script exits 0", async () => {

    const { io } = makeFakeIO({ execFile: execFileAlwaysSucceeds(), platform: "windows" });

    assert.equal(await getServiceGenerator(io)?.isRunning(), true);
  });
});

