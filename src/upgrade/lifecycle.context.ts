/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * lifecycle.context.ts: The default adapter that produces an UpgradeLifecycleContext from real runtime I/O. The strategies in lifecycle.ts are pure functions
 * over the context; this file is the only place in the upgrade-lifecycle module that calls spawn, getDataDir, or reads process.pid / process.platform.
 * Tests bypass this file entirely by constructing UpgradeLifecycleContext literals inline.
 */
import type { UpgradeLifecycleContext, UpgradeRunResult } from "./lifecycle.ts";
import { SERVICE_NAME } from "../identity.ts";
import { getDataDir } from "../config/paths.ts";
import path from "node:path";
import { spawn } from "node:child_process";

/**
 * Options the caller uses to shape the context it gets back. The only knob is the command deadline, because that is the one policy the two callers disagree
 * about: the CLI runs unbounded with a user watching the terminal, while the web UI bounds the command so a stalled install cannot hold an HTTP request open
 * indefinitely.
 */
export interface LifecycleContextOptions {

  // The deadline, in milliseconds, applied to the in-process upgrade command. Omitted runs the command unbounded.
  readonly commandTimeoutMs?: number;
}

/**
 * Runs a shell command to completion, inheriting the caller's stdio so npm and brew output flows live, and resolves with nothing but the success verdict.
 *
 * The command arrives as a shell-style line ("npm install -g prismcast@latest", "brew update && brew upgrade prismcast"), so it runs through the platform shell
 * rather than being split into an argv. Inheriting stdio is why this uses spawn rather than a promisified exec: exec captures output into buffers it hands back
 * at the end, which would hide a long install's progress from the user entirely.
 *
 * The optional deadline kills a command that stops making progress. SIGTERM gives the package manager a chance to unwind; the exit that follows carries a signal
 * rather than a zero code, so the killed command reports failure through the normal path with no special case.
 *
 * @param cmd - The shell command line to run.
 * @param options.cwd - The working directory for the command, when the install method has one.
 * @param options.timeoutMs - The deadline in milliseconds, or undefined to run unbounded.
 * @returns Promise resolving to the run outcome.
 */
async function runShellCommand(cmd: string, options: { readonly cwd?: string; readonly timeoutMs?: number }): Promise<UpgradeRunResult> {

  const { promise, resolve } = Promise.withResolvers<UpgradeRunResult>();
  const child = spawn(cmd, { cwd: options.cwd, shell: true, stdio: "inherit" });
  const deadline = (options.timeoutMs === undefined) ? undefined : setTimeout(() => { child.kill("SIGTERM"); }, options.timeoutMs);

  // Both terminal events route through one settle so the deadline timer is always cleared, whichever way the command ends. A second call is harmless: the first
  // resolution wins and the rest are dropped by the promise itself.
  const settle = (success: boolean): void => {

    clearTimeout(deadline);
    resolve({ success });
  };

  // A spawn error (no shell, permission denied) is a failed upgrade like any other. The user sees the package manager's own diagnostics on the inherited stdio,
  // so the outcome carries only the verdict.
  child.on("error", () => { settle(false); });
  child.on("exit", (code) => { settle(code === 0); });

  return promise;
}

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
 * @param options - Caller policy for the context; today only the in-process command deadline.
 * @returns An UpgradeLifecycleContext populated from the live process.
 */
export function createDefaultLifecycleContext(options: LifecycleContextOptions = {}): UpgradeLifecycleContext {

  return {

    commandTimeoutMs: options.commandTimeoutMs,
    parentPid: process.pid,
    platform: process.platform,
    runCommand: runShellCommand,
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
