/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * commands.context.ts: The default adapter that produces an UpgradeContext from real runtime I/O. The handler in commands.ts is a pure orchestrator over an
 * UpgradeContext; this file is the only place in the upgrade-commands module that fetches the npm registry, invokes process.exit, or wires the platform-aware
 * lifecycle through to performUpgrade. Tests bypass this file entirely by constructing UpgradeContext literals inline.
 *
 * The lifecycle context is built when performUpgrade is invoked, not at UpgradeContext construction time. Eager construction would call
 * getDataDir() before the upgrade-execution branch is reached, which is wrong for the --help and --check paths (they never invoke performUpgrade) and brittle
 * for tests that exercise those paths against the default context without initializing the data directory. Lazy construction keeps the cost paid exactly when
 * the work happens.
 */
import { print, printError } from "../utils/cliOutput.ts";
import type { InstallInfo } from "./detection.ts";
import type { UpgradeContext } from "./commands.ts";
import type { UpgradeStep } from "./lifecycle.ts";
import { createDefaultLifecycleContext } from "./lifecycle.context.ts";
import { detectInstallMethod } from "./detection.ts";
import { fetchLatestVersion } from "../utils/version.ts";
import { isRunningAsService } from "../utils/platform.ts";
import { performUpgrade } from "./lifecycle.ts";

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
    performUpgrade: async (info: InstallInfo): Promise<UpgradeStep> => performUpgrade(createDefaultLifecycleContext(), info),
    stderr: printError,
    stdout: print
  };
}
