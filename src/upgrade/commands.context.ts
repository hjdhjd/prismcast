/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * commands.context.ts: The default adapter that produces an UpgradeContext from real runtime I/O. The handler in commands.ts is a pure orchestrator over an
 * UpgradeContext; this file is the only place in the upgrade-commands module that calls execSync, fetches the npm registry, or invokes process.exit. Tests
 * bypass this file entirely by constructing UpgradeContext literals inline.
 */
import type { UpgradeContext, UpgradeResult } from "./commands.ts";
import { print, printError } from "../utils/cliOutput.ts";
import { detectInstallMethod } from "./detection.ts";
import { execSync } from "node:child_process";
import { fetchLatestVersion } from "../utils/version.ts";
import { isRunningAsService } from "../utils/platform.ts";


/**
 * Builds the default UpgradeContext from real runtime I/O.
 * @returns An UpgradeContext populated from the live process.
 */
export function createDefaultUpgradeContext(): UpgradeContext {

  return {

    detect: detectInstallMethod,
    exit: (code) => process.exit(code),
    fetchLatestVersion,
    isService: isRunningAsService(),
    runUpgradeCommand: (cmd: string, options: { cwd?: string }): UpgradeResult => {

      try {

        execSync(cmd, { cwd: options.cwd, encoding: "utf-8", stdio: "inherit" });

        return { success: true };
      } catch {

        return { success: false };
      }
    },
    stderr: printError,
    stdout: print
  };
}
