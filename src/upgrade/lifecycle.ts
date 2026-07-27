/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * lifecycle.ts: Platform-strategy port for performing an upgrade. Detection (detection.ts) answers "what install method is this"; this module answers "given
 * that method, what is the right way to actually run the upgrade on this OS." The two responsibilities are intentionally separated because they vary along
 * different axes: detection is keyed by install path, lifecycle is keyed by OS-level execution model.
 *
 * The architecture mirrors detection.ts: a registry of platform-strategy records is the single source of truth, every strategy is a pure function over a
 * UpgradeLifecycleContext, and the adapter (lifecycle.context.ts) is the only place that touches process state, spawns subprocesses, or reads the filesystem.
 *
 *   - UpgradeLifecycleContext (this file) - the runtime-input port; what every strategy needs to make and execute its decision
 *   - UpgradeStep (this file) - the discriminated-union outcome; consumers in commands.ts narrow on `kind` to choose between "we ran it" and "a detached helper
 *     is running it for us"
 *   - UpgradeLifecycleStrategy (this file) - a strategy record with the platforms it serves and a `perform` function that returns an UpgradeStep
 *   - UPGRADE_LIFECYCLES (this file) - the registry, in priority order
 *   - performUpgrade (this file) - the dispatcher; picks a strategy by platform and runs it
 *   - createDefaultLifecycleContext (lifecycle.context) - the adapter; the only place that calls spawn, execSync, getDataDir, or process.pid
 *
 * Why a strategy port at all? Windows introduces three facts that POSIX strategies never confront: (1) Windows file locks prevent
 * `npm install -g` from renaming the prismcast directory while any node.exe holds it open, so the upgrade cannot run in-process; (2) Windows Task Scheduler does
 * not re-spawn a task because its process exited, so `ctx.exit(0)` does not restart the service the way launchd's KeepAlive or systemd's Restart=always do; and
 * (3) the upgrade tool cannot wait for the helper to finish because it would never finish (the helper is waiting for us to exit). The right architectural shape
 * is a detached helper that runs the upgrade after the parent dies; modeling that as a platform strategy keeps the divergence isolated to one record on the
 * registry and lets the POSIX strategies stay simple in-process runners.
 *
 * Adding a new platform-specific strategy is one record on the registry. The dispatcher and the adapter never change. Tests construct UpgradeLifecycleContext
 * literals inline; production calls performUpgrade(ctx, info) and the adapter wires the context to real I/O.
 */
import type { InstallInfo } from "./detection.ts";

/**
 * Outcome of a single upgrade attempt. Discriminated union over `kind`. Consumers narrow on it to choose between "the upgrade ran in-process, here is the
 * outcome" and "a detached helper is taking it from here, exit immediately and trust the helper to finish the job."
 *
 * The "handed-off" variant is structurally permissible only because the strategy that produced it has already spawned the helper before returning; the field
 * `logPath` is the path the helper writes its progress to so the user has a place to look while the upgrade is running.
 */
export type UpgradeStep = {

  // The strategy ran the upgrade in-process synchronously. The success flag is the runner's outcome; the caller decides what to print and whether to exit
  // (e.g., to trigger the service manager's restart on POSIX).
  readonly kind: "ran";
  readonly success: boolean;
} | {

  // The strategy spawned a detached helper that will perform the upgrade after the current process exits. The caller MUST exit immediately after observing
  // this variant; the helper is waiting on the current process's PID to release file locks before it runs npm install. logPath is the file the helper writes
  // its progress to and is surfaced to the user so they can check status.
  readonly kind: "handed-off";
  readonly logPath: string;
};

/**
 * Result of running an upgrade subprocess. The in-process strategy passes this through verbatim into the UpgradeStep "ran" variant, so its success flag is what
 * commands.ts narrows on and the outcome shape stays consistent across the two layers. The success flag distinguishes ran-without-error from
 * threw-or-exited-non-zero; the runner does not surface the actual error because the user sees subprocess stderr directly on the terminal.
 */
export interface UpgradeRunResult {

  readonly success: boolean;
}

/**
 * Runtime context the lifecycle strategies consume. Each field is one narrow capability some strategy needs. POSIX strategies use only `runCommand`; the
 * Windows handoff strategy uses `parentPid`, `serviceTaskName`, `spawnDetached`, `upgradeLogPath`. Splitting them onto one struct keeps the adapter (and the
 * UpgradeLifecycleContext interface it satisfies for tests) a single object, even though no strategy uses every field.
 */
export interface UpgradeLifecycleContext {

  // The PID of the running prismcast-upgrade process. The Windows handoff helper waits on this PID before running npm install, so the parent's open file
  // handles release before npm tries to rename the prismcast directory.
  readonly parentPid: number;

  // The OS-level platform identifier. The dispatcher matches this against each strategy's `platforms` list to pick the right strategy. POSIX strategies match
  // darwin and linux; the Windows strategy matches win32; unknown platforms fall through to the POSIX default.
  readonly platform: NodeJS.Platform;

  // The subprocess runner for in-process strategies. The implementation is expected to inherit the user's terminal so npm/brew output flows live; the return
  // value reports only success or failure, not captured stdio.
  readonly runCommand: (cmd: string, options: { readonly cwd?: string }) => UpgradeRunResult;

  // The name of the registered service task, when one exists. The Windows handoff helper queries this name via Get-ScheduledTask to know whether a running
  // service instance must be stopped before npm install and re-started afterward. Empty string when PrismCast is not registered as a service; the helper
  // detects that and skips the stop/start steps cleanly.
  readonly serviceTaskName: string;

  // Spawns a detached child process. Returns nothing because the caller (a handoff strategy) does not - and must not - wait for it. The detached helper is
  // intended to outlive the current process by design. The contract requires the implementation to call .unref() and route stdio to "ignore" so the child is
  // fully decoupled from the parent's lifecycle.
  readonly spawnDetached: (command: string, args: readonly string[]) => void;

  // The path the Windows handoff helper writes its progress and final exit code to. The path is surfaced to the user as part of the "handed-off" outcome so
  // they can tail or read the file while the upgrade runs in the background.
  readonly upgradeLogPath: string;
}

/**
 * A platform-strategy record. The registry is a flat tuple of these; the dispatcher picks the first whose `platforms` list includes the current OS. Pure
 * function over UpgradeLifecycleContext + InstallInfo; no state is owned by the strategy itself.
 */
export interface UpgradeLifecycleStrategy {

  // Human-readable identifier for this strategy. Surfaces in tests and error messages. Distinct from any platform name because future variants on the same
  // platform (e.g., "windows-foreground" for an interactive non-service developer flow) would need their own ids.
  readonly id: string;

  // The set of platform identifiers this strategy serves. The dispatcher walks the registry in order and picks the first strategy whose list includes the
  // running platform. Listing them explicitly (rather than via a predicate) keeps the registry declarative and the test surface small.
  readonly platforms: readonly NodeJS.Platform[];

  // Performs the upgrade for one InstallInfo on one context, and returns the outcome. Synchronous because both implementations (in-process execSync, detached
  // spawn-and-return) are themselves synchronous from the caller's perspective.
  readonly perform: (ctx: UpgradeLifecycleContext, info: InstallInfo) => UpgradeStep;
}

/**
 * The PowerShell helper that the Windows handoff strategy spawns. Reads five positional parameters: the parent process ID, the optional service task name
 * (empty string when no service is registered), the upgrade command exactly as detection.ts produced it, the optional working directory for npm-local
 * installs (empty string for npm-global), and the log path the helper appends its progress to.
 *
 * Sequence:
 *
 *   1. Wait for the parent process to exit. Wait-Process with -ErrorAction SilentlyContinue tolerates the (theoretical) case where the parent has already exited
 *      between us being spawned and this line running.
 *   2. If a service task is registered, disable it (so RestartOnFailure does not interfere) and stop it. Stop-ScheduledTask is non-blocking, so we
 *      poll for the task to transition out of Running with a 30-second ceiling - the same poll pattern as service/generators.ts's WINDOWS_REGISTER_SCRIPT.
 *   3. Run the upgrade command via cmd.exe /c so npm/brew shell-style command lines parse correctly. Output is captured for the log; the exit code is captured
 *      so we can re-raise it as our own exit code at the very end.
 *   4. If a service task is registered, re-enable it and start it. The re-enable is necessary because Disable-ScheduledTask persists across restarts; without
 *      this step, the next reboot would silently leave the service disabled.
 *   5. Exit with the upgrade's exit code so the helper's own exit reflects the upgrade outcome (visible in the log via $LASTEXITCODE).
 *
 * Logging is best-effort: a failed Add-Content call (e.g., the directory does not exist) is swallowed by SilentlyContinue so the helper still completes its
 * primary job. The log itself is the only user-facing artifact of the helper, and we surface its path back to commands.ts as part of the UpgradeStep.
 */
const WINDOWS_HANDOFF_SCRIPT = [

  "param($ParentPid, $TaskName, $UpgradeCommand, $WorkingDir, $LogPath)",
  "function Write-UpgradeLog($message) {",
  "  $stamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'",
  "  try { Add-Content -Path $LogPath -Value ('[' + $stamp + '] ' + $message) -Encoding utf8 -ErrorAction SilentlyContinue } catch {}",
  "}",
  "Write-UpgradeLog ('Helper started; waiting for parent PID ' + $ParentPid + ' to exit.')",
  "try { Wait-Process -Id $ParentPid -ErrorAction SilentlyContinue } catch {}",
  "Write-UpgradeLog 'Parent exited.'",
  "if($TaskName) {",
  "  $existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue",
  "  if($existing) {",
  "    Write-UpgradeLog ('Disabling and stopping task ' + $TaskName + '.')",
  "    Disable-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue | Out-Null",
  "    Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue | Out-Null",
  "    $deadline = (Get-Date).AddSeconds(30)",
  "    while((Get-Date) -lt $deadline) {",
  "      $state = (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue).State",
  "      if(($null -eq $state) -or ($state -ne 'Running')) { break }",
  "      Start-Sleep -Milliseconds 250",
  "    }",
  "    Write-UpgradeLog ('Task state after stop: ' + $state + '.')",
  "  } else {",
  "    Write-UpgradeLog 'No matching scheduled task found; skipping stop/start.'",
  "  }",
  "}",
  "if($WorkingDir) { Set-Location -Path $WorkingDir }",
  "Write-UpgradeLog ('Running upgrade command: ' + $UpgradeCommand)",
  "$upgradeOutput = (& cmd.exe /c $UpgradeCommand 2>&1) | Out-String",
  "$upgradeExit = $LASTEXITCODE",
  "try { Add-Content -Path $LogPath -Value $upgradeOutput -Encoding utf8 -ErrorAction SilentlyContinue } catch {}",
  "Write-UpgradeLog ('Upgrade exit code: ' + $upgradeExit + '.')",
  "if($TaskName) {",
  "  $existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue",
  "  if($existing) {",
  "    Write-UpgradeLog ('Re-enabling and starting task ' + $TaskName + '.')",
  "    Enable-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue | Out-Null",
  "    Start-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue | Out-Null",
  "  }",
  "}",
  "Write-UpgradeLog 'Helper finished.'",
  "exit $upgradeExit"
].join("\n");

/**
 * Escapes a value for embedding inside a single-quoted PowerShell string. PowerShell's single-quoted strings are literal (no interpolation, no backslash
 * escapes); the only character that requires escaping is the single quote itself, written as two consecutive single quotes. Mirrors the same helper in
 * service/generators.ts so the two PowerShell-emitting modules agree on the one escape rule that matters.
 *
 * @param value - The value to escape.
 * @returns The value with internal single quotes doubled.
 */
function escapeSingleQuoted(value: string): string {

  return value.replaceAll("'", "''");
}

/**
 * Wraps a value in a PowerShell single-quoted literal.
 * @param value - The value to quote.
 * @returns The literal including its surrounding quotes.
 */
function powerShellLiteral(value: string): string {

  return "'" + escapeSingleQuoted(value) + "'";
}

/**
 * Builds the PowerShell command string that the Windows handoff strategy passes to powershell.exe via -Command. The script body is wrapped in `& { ... }` and
 * the positional arguments are appended after the closing brace, exactly as PowerShell's parser expects. Each argument is single-quote-escaped via
 * powerShellLiteral, so there is one - and only one - escape surface (the single-quote doubling rule).
 *
 * @param args - The positional arguments to pass to the helper script's param() block, in declaration order.
 * @returns The fully composed `-Command` string.
 */
function buildHandoffCommand(args: readonly string[]): string {

  const quoted = args.map((value) => powerShellLiteral(value)).join(" ");

  return "& { " + WINDOWS_HANDOFF_SCRIPT + " } " + quoted;
}

/**
 * POSIX in-process strategy. macOS and Linux can run `npm install -g` (or `brew upgrade`) directly from inside the running prismcast process because POSIX
 * filesystems do not lock open files against directory rename. The strategy is a thin pass-through over ctx.runCommand. The caller (commands.ts) decides
 * whether to exit afterward so the service manager (launchd KeepAlive, systemd Restart=) brings PrismCast back up.
 */
const POSIX_IN_PROCESS_LIFECYCLE: UpgradeLifecycleStrategy = {

  id: "posix-in-process",
  perform: (ctx: UpgradeLifecycleContext, info: InstallInfo): UpgradeStep => {

    const result = ctx.runCommand(info.upgradeCommand, { cwd: info.packageDir });

    return { kind: "ran", success: result.success };
  },
  platforms: [ "darwin", "linux" ]
};

/**
 * Windows handoff strategy. Composes the PowerShell helper command line, hands it to ctx.spawnDetached, and returns "handed-off." The helper waits for the
 * current process to exit (so the prismcast directory's file locks release), runs the upgrade, then restarts the scheduled task if one exists. This is the
 * Windows EBUSY + auto-restart fix: the parent process exits cleanly with an "upgrade running in the background" message; the detached helper handles the rest.
 *
 * The PowerShell launch is identical to the rest of the codebase: -NoProfile / -NonInteractive for a clean shell, -ExecutionPolicy Bypass so installer-style
 * scripted scenarios are not gated by the interactive-use policy, -WindowStyle Hidden so the user sees no console window pop up, and -Command with our
 * single-quote-escaped script string. The launcher is spawned detached, with stdio ignored and windowsHide set, so it survives the parent's exit.
 */
const WINDOWS_HANDOFF_LIFECYCLE: UpgradeLifecycleStrategy = {

  id: "windows-handoff",
  perform: (ctx: UpgradeLifecycleContext, info: InstallInfo): UpgradeStep => {

    const handoffCommand = buildHandoffCommand([

      String(ctx.parentPid),
      ctx.serviceTaskName,
      info.upgradeCommand,
      info.packageDir ?? "",
      ctx.upgradeLogPath
    ]);

    ctx.spawnDetached("powershell.exe", [

      "-NoProfile",
      "-NonInteractive",
      "-ExecutionPolicy", "Bypass",
      "-WindowStyle", "Hidden",
      "-Command", handoffCommand
    ]);

    return { kind: "handed-off", logPath: ctx.upgradeLogPath };
  },
  platforms: ["win32"]
};

/* The strategy registry as a narrow tuple. Declared `as const` so each entry retains its literal shape...the dispatcher walks this in order and picks the
 * first match. POSIX comes first because it serves more platforms; Windows comes second because it serves only one. The default-fallback semantics live in
 * selectLifecycle so an unknown platform (e.g., freebsd) falls back to the POSIX strategy because it serves the broadest platform set and keeps the
 * dispatcher total.
 */
const STRATEGY_TUPLE = [

  POSIX_IN_PROCESS_LIFECYCLE,
  WINDOWS_HANDOFF_LIFECYCLE
] as const;

/**
 * The upgrade-lifecycle strategy registry. Same runtime data as STRATEGY_TUPLE; the public name carries the wide UpgradeLifecycleStrategy type so consumers
 * and tests see one uniform shape. Adding a new platform-specific strategy is appending an entry to STRATEGY_TUPLE; the dispatcher and the adapter never
 * change.
 */
export const UPGRADE_LIFECYCLES: readonly UpgradeLifecycleStrategy[] = STRATEGY_TUPLE;

/**
 * Selects the lifecycle strategy for a given platform. Falls back to the POSIX in-process strategy when no strategy lists the platform...this covers any
 * Unix-like host that is not specifically called out and keeps the function total (no thrown error from a one-off platform name).
 *
 * @param platform - The OS platform identifier.
 * @returns The matching strategy, or the POSIX default when no strategy serves the platform.
 */
export function selectLifecycle(platform: NodeJS.Platform): UpgradeLifecycleStrategy {

  return UPGRADE_LIFECYCLES.find((strategy) => strategy.platforms.includes(platform)) ?? POSIX_IN_PROCESS_LIFECYCLE;
}

/**
 * Performs an upgrade by selecting the platform-appropriate strategy and invoking its `perform` callback. Pure dispatcher; all I/O happens inside the
 * strategy's own callbacks (which themselves receive an UpgradeLifecycleContext that the adapter wires from real I/O). Tests inject context literals and
 * exercise each strategy through this entry point.
 *
 * @param ctx - The lifecycle context.
 * @param info - The detected install info.
 * @returns The upgrade outcome.
 */
export function performUpgrade(ctx: UpgradeLifecycleContext, info: InstallInfo): UpgradeStep {

  return selectLifecycle(ctx.platform).perform(ctx, info);
}
