/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * lifecycle.context.ts: The default adapter that produces an UpgradeLifecycleContext from real runtime I/O. The strategies in lifecycle.ts are pure functions
 * over the context; this file is the only place in the upgrade-lifecycle module that calls spawn, execSync, getDataDir, or reads process.pid / process.platform.
 * Tests bypass this file entirely by constructing UpgradeLifecycleContext literals inline.
 */
import type { UpgradeLifecycleContext, UpgradeRunResult } from "./lifecycle.ts";
import { execSync, spawn } from "node:child_process";
import { SERVICE_NAME } from "../identity.ts";
import { getDataDir } from "../config/paths.ts";
import path from "node:path";

/**
 * Builds the default UpgradeLifecycleContext from real runtime I/O.
 *
 * runCommand inherits the user's terminal so npm/brew output flows live; it captures only the exit outcome, not the captured stdio. spawnDetached launches a
 * fully decoupled child process - detached, stdio ignored, windowsHide on - then unref's so the child can outlive the parent. The Windows handoff strategy is
 * the sole caller; on POSIX the strategy never invokes it.
 *
 * serviceTaskName resolves to the documented PrismCast service identifier when running on Windows. It is the empty string on other platforms because the field
 * is only consumed by the Windows handoff helper. We always send the identifier (rather than gating on isRunningAsService) because the helper itself probes
 * Get-ScheduledTask for the running task and skips the stop/start steps cleanly when none is registered...the user may be running a global npm install of
 * prismcast on Windows without having registered a service yet, and the upgrade flow must still work.
 *
 * upgradeLogPath sits inside the PrismCast data directory at `upgrade.log`. The helper appends timestamped lines and the upgrade command's captured stdout to
 * this file; the user is told its location in the "handed-off" UpgradeStep so they can tail or read it.
 *
 * @returns An UpgradeLifecycleContext populated from the live process.
 */
export function createDefaultLifecycleContext(): UpgradeLifecycleContext {

  return {

    parentPid: process.pid,
    platform: process.platform,
    runCommand: (cmd: string, options: { readonly cwd?: string }): UpgradeRunResult => {

      try {

        execSync(cmd, { cwd: options.cwd, encoding: "utf-8", stdio: "inherit" });

        return { success: true };
      } catch {

        return { success: false };
      }
    },
    serviceTaskName: (process.platform === "win32") ? SERVICE_NAME : "",
    spawnDetached: (command: string, args: readonly string[]): void => {

      const child = spawn(command, [...args], {

        detached: true,
        stdio: "ignore",
        windowsHide: true
      });

      child.unref();
    },
    upgradeLogPath: path.join(getDataDir(), "upgrade.log")
  };
}
