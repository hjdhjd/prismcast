/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * generators.ts: Platform-specific service file generators for PrismCast.
 */
import type { Platform, ServiceManager } from "../utils/platform.js";
import { SERVICE_ID, SERVICE_NAME, getLogsDirectory, getNodeExecutablePath, getPlatform, getPrismCastEntryPoint, getPrismCastWorkingDirectory, getServiceFileDirectory,
  getServiceFilePath } from "../utils/platform.js";
import { CONFIG_METADATA } from "../config/userConfig.js";
import type { Nullable } from "../types/index.js";
import { execFile as execFileCallback } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { promisify } from "node:util";

const { promises: fsPromises } = fs;

/* Promisified execFile is used for every external tool invocation in this module. execFile (as opposed to exec) passes arguments directly to the OS spawn call,
 * bypassing the shell and eliminating every class of shell-quoting hazard. The Windows PowerShell invocations build a single -Command string inside a scriptblock
 * with single-quote-escaped positional args (see invokePowerShell); macOS and Linux pass each argument as its own array element.
 */
const execFile = promisify(execFileCallback);

/* These generators create platform-specific service definitions that allow PrismCast to run as a managed service. Each generator produces the appropriate
 * configuration format for its service manager (launchd plist for macOS, systemd unit for Linux, Task Scheduler task for Windows), and owns every file it writes
 * and every external call it makes.
 *
 * The interface exposes a single install(definition) entry point. Callers build a ServiceDefinition from the platform helpers and hand it to the generator; the
 * generator decides how to realize it. This keeps the caller free of platform-specific file-count or file-format concerns, and lets each generator remain
 * internally cohesive.
 *
 * Key features of the generated services:
 * - Auto-start at user login (user-level service, no root or UAC required).
 * - Auto-restart on crash (KeepAlive/Restart=always/RestartOnFailure).
 * - PRISMCAST_SERVICE=1 environment variable for service detection.
 * - Stdout/stderr capture to the data directory (launchd StandardOutPath/StandardErrorPath, Windows Start-Process stream redirection). Linux defers to the systemd
 *   journal, which is the native logging surface on that platform.
 *
 * All external tool invocations are genuinely asynchronous (promisified execFile, fsPromises) so that install/start/stop/uninstall honor their Promise<void>
 * contracts and do not block the event loop during multi-second operations such as the Windows task-state poll.
 */

/**
 * Structured description of the service to install. Each generator consumes this and realizes it in whatever file(s) and registration calls its platform requires.
 */
export interface ServiceDefinition {

  // The absolute path to PrismCast's entry point (dist/index.js).
  readonly entryPoint: string;

  // Environment variables to set when the service runs. PRISMCAST_SERVICE=1 is always included.
  readonly envVars: Readonly<Record<string, string>>;

  // Absolute path to the directory where service stdout/stderr logs should be written (where the platform supports redirection).
  readonly logsDir: string;

  // Absolute path to the Node.js executable.
  readonly nodePath: string;

  // Absolute working directory for the service process.
  readonly workingDir: string;
}

/**
 * Interface for platform-specific service generators. Each implementation owns all I/O and external tool invocations for its platform.
 */
export interface ServiceGenerator {

  // Install the service from its structured definition. The generator writes any support files and registers the service with its platform's service manager.
  install(definition: ServiceDefinition): Promise<void>;

  // Check if the service is currently installed.
  isInstalled(): Promise<boolean>;

  // Check if the service is currently running.
  isRunning(): Promise<boolean>;

  // The platform this generator is for.
  platform: Platform;

  // The service manager type.
  serviceManager: ServiceManager;

  // Start the service.
  start(): Promise<void>;

  // Stop the service.
  stop(): Promise<void>;

  // Uninstall the service (deregister and remove any support files).
  uninstall(): Promise<void>;
}

/**
 * Escapes a string for use in XML by replacing special characters with entities. Used by the macOS plist generator; the other platforms do not emit XML.
 * @param str - The string to escape.
 * @returns The escaped string safe for XML.
 */
function escapeXml(str: string): string {

  return str.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll("\"", "&quot;").replaceAll("'", "&apos;");
}

/**
 * Runs an asynchronous child_process call and, on failure, re-throws an Error whose message includes the trimmed stderr text captured from the failed child and
 * whose cause chain points back at the original Error. Without the stderr enrichment, execFile/exec failures surface to users as a generic "Command failed: ..."
 * line while the actual diagnostic - written by the child to stderr - is left on the Error's unread stderr Buffer. Attaching the original via Error.cause
 * preserves the original stack and structured properties (status, signal, stdout, stderr) for any programmatic consumer.
 * @param description - A short human-readable label describing what was attempted; prepended to the error message.
 * @param run - The child_process thunk to execute.
 */
async function runAndSurfaceStderr(description: string, run: () => Promise<unknown>): Promise<void> {

  try {

    await run();
  } catch(error) {

    if(!(error instanceof Error)) {

      throw error;
    }

    const raw = (error as { stderr?: unknown }).stderr;
    const detail = (Buffer.isBuffer(raw) ? raw.toString("utf8") : ((typeof raw === "string") ? raw : "")).trim();

    throw new Error(description + ": " + ((detail.length > 0) ? detail : error.message), { cause: error });
  }
}

/**
 * Returns the service definition's environment variables as a deterministically ordered array of [key, value] entries. Consumed by every generator to produce
 * byte-stable output across regenerations (essential for reliable stale-path detection and clean diffs when users inspect the generated files).
 * @param envVars - The environment variable map to order.
 * @returns The entries sorted alphabetically by key.
 */
function sortedEnvEntries(envVars: Readonly<Record<string, string>>): [string, string][] {

  return Object.entries(envVars).toSorted(([a], [b]) => a.localeCompare(b));
}

/**
 * Checks whether a file exists using the async fs API. Replaces fs.existsSync in async method bodies so the Promise<boolean> return type is backed by real I/O
 * rather than a synchronous call papered over with an eslint-disable comment.
 * @param filePath - The absolute path to check.
 * @returns True if the file exists and is accessible, false otherwise.
 */
async function fileExists(filePath: string): Promise<boolean> {

  try {

    await fsPromises.access(filePath);

    return true;
  } catch {

    return false;
  }
}

/* Generates a launchd property list (plist) file for macOS. The plist is installed to ~/Library/LaunchAgents/ and configured with:
 * - RunAtLoad: Start when user logs in.
 * - KeepAlive: Restart automatically if the process exits.
 * - StandardOutPath/StandardErrorPath: Capture stdout/stderr to the data directory.
 */

/**
 * Creates a launchd service generator for macOS.
 * @returns A ServiceGenerator for launchd.
 */
function createLaunchdGenerator(): ServiceGenerator {

  /**
   * Builds the plist XML from a service definition.
   * @param definition - The service definition to serialize.
   * @returns The plist content as a UTF-8 string.
   */
  function generatePlist(definition: ServiceDefinition): string {

    const envEntries = sortedEnvEntries(definition.envVars)
      .map(([ key, value ]) => [ "      <key>" + escapeXml(key) + "</key>", "      <string>" + escapeXml(value) + "</string>" ].join("\n"))
      .join("\n");

    return [
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
      "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">",
      "<plist version=\"1.0\">",
      "<dict>",
      "  <key>Label</key>",
      "  <string>" + escapeXml(SERVICE_ID) + "</string>",
      "",
      "  <key>ProgramArguments</key>",
      "  <array>",
      "    <string>" + escapeXml(definition.nodePath) + "</string>",
      "    <string>" + escapeXml(definition.entryPoint) + "</string>",
      "  </array>",
      "",
      "  <key>WorkingDirectory</key>",
      "  <string>" + escapeXml(definition.workingDir) + "</string>",
      "",
      "  <key>EnvironmentVariables</key>",
      "  <dict>",
      envEntries,
      "  </dict>",
      "",
      "  <key>RunAtLoad</key>",
      "  <true/>",
      "",
      "  <key>KeepAlive</key>",
      "  <true/>",
      "",
      "  <key>StandardOutPath</key>",
      "  <string>" + escapeXml(path.join(definition.logsDir, "service-stdout.log")) + "</string>",
      "",
      "  <key>StandardErrorPath</key>",
      "  <string>" + escapeXml(path.join(definition.logsDir, "service-stderr.log")) + "</string>",
      "</dict>",
      "</plist>",
      ""
    ].join("\n");
  }

  return {

    async install(definition: ServiceDefinition): Promise<void> {

      const installPath = getServiceFilePath();
      const installDir = getServiceFileDirectory();

      // Ensure directories exist.
      await fsPromises.mkdir(installDir, { recursive: true });
      await fsPromises.mkdir(definition.logsDir, { recursive: true });

      // Write the plist.
      await fsPromises.writeFile(installPath, generatePlist(definition), "utf8");

      // Load the service with launchctl. If load fails because the definition is already loaded (common on reinstall), unload first then reload.
      try {

        await execFile("launchctl", [ "load", "-w", installPath ]);
      } catch {

        try {

          await execFile("launchctl", [ "unload", installPath ]);
        } catch {

          // Ignore unload errors.
        }

        await runAndSurfaceStderr("launchctl load failed", async () => execFile("launchctl", [ "load", "-w", installPath ]));
      }
    },

    async isInstalled(): Promise<boolean> {

      return fileExists(getServiceFilePath());
    },

    async isRunning(): Promise<boolean> {

      try {

        // launchctl list emits tab-separated rows: "PID\tStatus\tLabel". We find the row for this service and parse the PID from the first column. A PID of "-"
        // means the service is loaded but not actively running.
        const { stdout } = await execFile("launchctl", ["list"]);
        const line = stdout.split("\n").find((row) => row.includes(SERVICE_ID));

        if(!line) {

          return false;
        }

        const pid = line.trim().split("\t")[0];

        return (pid !== "-") && !isNaN(Number(pid));
      } catch {

        return false;
      }
    },

    platform: "darwin",

    serviceManager: "launchd",

    async start(): Promise<void> {

      const installPath = getServiceFilePath();

      // Unload first to clear any stale loaded-but-not-running state. Without this, `launchctl load -w` is a no-op when the definition is already loaded (e.g.,
      // after a crash or upgrade with changed paths), and the cached stale definition is reused.
      try {

        await execFile("launchctl", [ "unload", installPath ]);
      } catch {

        // Ignore - may not be loaded.
      }

      await runAndSurfaceStderr("launchctl load failed", async () => execFile("launchctl", [ "load", "-w", installPath ]));
      await runAndSurfaceStderr("launchctl start failed", async () => execFile("launchctl", [ "start", SERVICE_ID ]));
    },

    async stop(): Promise<void> {

      await runAndSurfaceStderr("launchctl unload failed", async () => execFile("launchctl", [ "unload", getServiceFilePath() ]));
    },

    async uninstall(): Promise<void> {

      const installPath = getServiceFilePath();

      // Unload the service first.
      try {

        await execFile("launchctl", [ "unload", installPath ]);
      } catch {

        // Ignore errors if the service was not loaded.
      }

      // Remove the plist file.
      await fsPromises.rm(installPath, { force: true });
    }
  };
}

/* Generates a systemd user service unit file for Linux. The unit is installed to ~/.config/systemd/user/ and configured with:
 * - Restart=always: Restart automatically if the process exits.
 * - RestartSec=5: Wait 5 seconds before restarting.
 * - WantedBy=default.target: Start when user session begins.
 *
 * Linux deliberately does not redirect stdout/stderr to files - systemd captures them to the journal, which is the native Linux logging surface (journalctl
 * --user -u prismcast).
 */

/**
 * Creates a systemd service generator for Linux.
 * @returns A ServiceGenerator for systemd.
 */
function createSystemdGenerator(): ServiceGenerator {

  /**
   * Builds the systemd unit file from a service definition.
   * @param definition - The service definition to serialize.
   * @returns The unit file content as a UTF-8 string.
   */
  function generateUnit(definition: ServiceDefinition): string {

    const envLines = sortedEnvEntries(definition.envVars).map(([ key, value ]) => "Environment=\"" + key + "=" + value + "\"").join("\n");

    return [
      "[Unit]",
      "Description=" + SERVICE_NAME + " Streaming Server",
      "After=network.target",
      "",
      "[Service]",
      "Type=simple",
      "ExecStart=" + definition.nodePath + " " + definition.entryPoint,
      "WorkingDirectory=" + definition.workingDir,
      "Restart=always",
      "RestartSec=5",
      envLines,
      "",
      "[Install]",
      "WantedBy=default.target",
      ""
    ].join("\n");
  }

  return {

    async install(definition: ServiceDefinition): Promise<void> {

      const installPath = getServiceFilePath();
      const installDir = getServiceFileDirectory();

      // Ensure directories exist.
      await fsPromises.mkdir(installDir, { recursive: true });
      await fsPromises.mkdir(definition.logsDir, { recursive: true });

      // Write the unit file.
      await fsPromises.writeFile(installPath, generateUnit(definition), "utf8");

      // Reload systemd to pick up the new unit file.
      try {

        await execFile("systemctl", [ "--user", "daemon-reload" ]);
      } catch {

        // Ignore if systemctl isn't available (shouldn't happen on systemd systems).
      }

      // Enable and start the service.
      await runAndSurfaceStderr("systemctl enable failed", async () => execFile("systemctl", [ "--user", "enable", "prismcast.service" ]));
      await runAndSurfaceStderr("systemctl start failed", async () => execFile("systemctl", [ "--user", "start", "prismcast.service" ]));
    },

    async isInstalled(): Promise<boolean> {

      return fileExists(getServiceFilePath());
    },

    async isRunning(): Promise<boolean> {

      try {

        const { stdout } = await execFile("systemctl", [ "--user", "is-active", "prismcast.service" ]);

        return stdout.trim() === "active";
      } catch {

        return false;
      }
    },

    platform: "linux",

    serviceManager: "systemd",

    async start(): Promise<void> {

      await runAndSurfaceStderr("systemctl start failed", async () => execFile("systemctl", [ "--user", "start", "prismcast.service" ]));
    },

    async stop(): Promise<void> {

      await runAndSurfaceStderr("systemctl stop failed", async () => execFile("systemctl", [ "--user", "stop", "prismcast.service" ]));
    },

    async uninstall(): Promise<void> {

      const installPath = getServiceFilePath();

      // Stop and disable the service.
      try {

        await execFile("systemctl", [ "--user", "stop", "prismcast.service" ]);
      } catch {

        // Ignore if not running.
      }

      try {

        await execFile("systemctl", [ "--user", "disable", "prismcast.service" ]);
      } catch {

        // Ignore if not enabled.
      }

      // Remove the unit file.
      await fsPromises.rm(installPath, { force: true });

      // Reload systemd.
      try {

        await execFile("systemctl", [ "--user", "daemon-reload" ]);
      } catch {

        // Ignore.
      }
    }
  };
}

/* Registers a Windows Task Scheduler task for PrismCast. The architecture is:
 *
 * - One support file: a PowerShell launcher (.ps1) that sets environment variables and spawns node with stdout/stderr redirected to the data directory. The
 *   launcher serves the same role as the macOS plist's EnvironmentVariables dict and the systemd unit's Environment= lines - it is the persistent, human-readable
 *   definition of the service's runtime.
 *
 * - One registration mechanism: the PowerShell ScheduledTasks module (Register-ScheduledTask, Start-ScheduledTask, etc.), which is the Task Scheduler 2.0 COM
 *   surface exposed as typed cmdlets. We do not emit Task Scheduler XML and we do not invoke schtasks.exe. That choice eliminates the MSXML encoding dialect, the
 *   shell-quoting hazards of schtasks /TR, and the need to get the Task XML schema's element order exactly right.
 *
 * - Task Scheduler spawns `powershell.exe -WindowStyle Hidden -File <launcher.ps1>`. PowerShell with -WindowStyle Hidden suppresses the console window natively on
 *   Windows 10+, so no .vbs launcher is needed. VBScript was deprecated by Microsoft in 2024.
 *
 * All PowerShell invocations from Node go through invokePowerShell(), which documents the single argument-escape rule in its docstring. Values flow through Node's
 * execFile without shell interpretation, and inside the PowerShell command string the one escape surface is single-quote doubling - see invokePowerShell below.
 */

/**
 * Escapes a value for embedding inside a single-quoted PowerShell string. PowerShell's single-quoted strings are literal (no interpolation, no backslash escapes);
 * the only character that requires escaping is the single quote itself, which is written as two consecutive single quotes.
 * @param value - The value to escape.
 * @returns The value with internal single quotes doubled.
 */
function escapePowerShellSingleQuoted(value: string): string {

  return value.replaceAll("'", "''");
}

/**
 * Wraps a value in a PowerShell single-quoted string literal, escaping any internal single quotes.
 * @param value - The value to quote.
 * @returns The PowerShell literal (including the surrounding quotes).
 */
function powerShellLiteral(value: string): string {

  return "'" + escapePowerShellSingleQuoted(value) + "'";
}

/**
 * Invokes powershell.exe with a script block and a set of positional arguments. The scriptBody is expected to declare its inputs via a param() clause at the top;
 * each argument is serialized through powerShellLiteral() and appended to the command after the scriptblock, so PowerShell's own parser binds them to the declared
 * parameters.
 *
 * The only escape surface in this path is the single-quote doubling performed by powerShellLiteral(). That one rule is total: inside a PowerShell single-quoted
 * string, no other character has meaning - there is no interpolation, no backslash escape, no subshell, no variable expansion - and Windows filesystem rules forbid
 * the double-quote character that could let a path escape the outer literal quoting we use inside the launcher's ArgumentList. Node's execFile then passes the
 * composed command string to powershell.exe as a single UTF-16 argv element, bypassing cmd.exe entirely and eliminating shell-level quoting concerns.
 *
 * On failure, any stderr text that PowerShell wrote (e.g., a cmdlet's error record) is folded into the thrown Error's message by runAndSurfaceStderr, and the
 * original Error is attached via the cause chain so programmatic consumers retain access to the structured failure details.
 * @param scriptBody - The PowerShell script body, including any param() declaration.
 * @param args - Positional arguments to pass to the script block.
 */
async function invokePowerShell(scriptBody: string, args: string[] = []): Promise<void> {

  const quotedArgs = args.map((value) => powerShellLiteral(value)).join(" ");
  const command = "& { " + scriptBody + " } " + quotedArgs;

  await runAndSurfaceStderr("PowerShell invocation failed",
    async () => execFile("powershell.exe", [ "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", command ]));
}

/* Script bodies for Task Scheduler operations. Each script is a self-contained PowerShell block that reads its inputs from positional parameters - never from string
 * interpolation or inline variables - so that Node-side argument quoting is always the single-quoted-literal escape and nothing else.
 */

/* Registers (or replaces) the PrismCast Task Scheduler task. Takes two positional parameters: the task name and the absolute path to the launcher .ps1 file.
 *
 * The first step stops any currently-running instance of the existing task and waits for its process to actually terminate before we replace the definition. Two
 * Microsoft-documented facts make this wait necessary: Stop-ScheduledTask initiates the stop but returns before the process has exited (there is no -Wait
 * switch and no built-in completion primitive), and "You can make changes to a task definition even if an instance of the task is running. The changes do not
 * affect the current instance." (Set-ScheduledTask docs, which applies equally to Register-ScheduledTask -Force). Without the poll loop, `prismcast service
 * install --force` on a running service would leave the old node process holding port 5589 while Start-ScheduledTask spawns a new node that fails to bind,
 * triggering the 1-minute RestartOnFailure backoff and silently delaying the service becoming available for up to three minutes.
 *
 * The poll checks State every 250 ms against a 30-second ceiling. The ceiling is a belt-and-suspenders fallback - if the old process is pathologically stuck,
 * Task Scheduler's own TerminateProcess path will clean it up later. A null state means the task was unregistered concurrently, which is also a valid "not
 * running anymore" exit.
 *
 * The action invokes powershell.exe with -WindowStyle Hidden and -File pointing at the launcher. -ExecutionPolicy Bypass is the Microsoft-documented pattern for
 * installer-scripted scenarios - it disables the interactive-use execution policy gate without globally weakening script security on the machine.
 *
 * The trigger fires at user logon. The principal runs the task as the current user with Interactive logon and Limited (LeastPrivilege) run level, mirroring the
 * user-scoped semantics of launchd user agents and systemd --user units. On workgroup machines where USERDOMAIN is not set, we fall back to COMPUTERNAME.
 *
 * Settings express unlimited execution time (PT0S), battery-friendly behavior, ignore-new-instance policy, and automatic restart on failure (3 attempts, 1-minute
 * interval).
 */
const WINDOWS_REGISTER_SCRIPT = [
  "param($TaskName, $Launcher)",
  "$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue",
  "if($existing) {",
  "  Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue | Out-Null",
  "  $deadline = (Get-Date).AddSeconds(30)",
  "  while((Get-Date) -lt $deadline) {",
  "    $state = (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue).State",
  "    if(($null -eq $state) -or ($state -ne 'Running')) { break }",
  "    Start-Sleep -Milliseconds 250",
  "  }",
  "}",
  "$userId = $env:USERDOMAIN + '\\' + $env:USERNAME",
  "if(-not $env:USERDOMAIN) { $userId = $env:COMPUTERNAME + '\\' + $env:USERNAME }",
  "$taskArg = '-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File \"' + $Launcher + '\"'",
  "$action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument $taskArg",
  "$trigger = New-ScheduledTaskTrigger -AtLogOn -User $userId",
  "$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Seconds 0) " +
    "-MultipleInstances IgnoreNew -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)",
  "$principal = New-ScheduledTaskPrincipal -UserId $userId -LogonType Interactive -RunLevel Limited",
  "Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null"
].join("\n");

/* Unregisters the task. Takes the task name as its sole parameter.
 */
const WINDOWS_UNREGISTER_SCRIPT = [
  "param($TaskName)",
  "Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue | Out-Null"
].join("\n");

/* Starts the task, re-enabling it first in case a previous stop() call left it disabled. Takes the task name as its sole parameter.
 */
const WINDOWS_START_SCRIPT = [
  "param($TaskName)",
  "Enable-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue | Out-Null",
  "Start-ScheduledTask -TaskName $TaskName | Out-Null"
].join("\n");

/* Stops the task. Disables it first to prevent the RestartOnFailure policy from re-launching the process after we stop it. Start-ScheduledTask in WINDOWS_START_SCRIPT
 * re-enables the task. Takes the task name as its sole parameter.
 */
const WINDOWS_STOP_SCRIPT = [
  "param($TaskName)",
  "Disable-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue | Out-Null",
  "Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue | Out-Null"
].join("\n");

/* Exits 0 if the task is registered, 1 otherwise. Takes the task name as its sole parameter.
 */
const WINDOWS_IS_INSTALLED_SCRIPT = [
  "param($TaskName)",
  "if(Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) { exit 0 } else { exit 1 }"
].join("\n");

/* Exits 0 if the task's State property is 'Running', 1 otherwise. Takes the task name as its sole parameter.
 */
const WINDOWS_IS_RUNNING_SCRIPT = [
  "param($TaskName)",
  "$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue",
  "if($task -and ($task.State -eq 'Running')) { exit 0 } else { exit 1 }"
].join("\n");

/**
 * Creates a Windows Task Scheduler generator.
 * @returns A ServiceGenerator for Windows Task Scheduler.
 */
function createWindowsSchedulerGenerator(): ServiceGenerator {

  const taskName = SERVICE_NAME;

  /**
   * Generates the PowerShell launcher script (.ps1) that Task Scheduler will invoke at user logon. The launcher sets environment variables and spawns node via
   * Start-Process, with stdout and stderr redirected to separate log files in the data directory. Start-Process binds the child's stream file descriptors directly
   * to the output files, so PowerShell's host-level encoding conversions never touch the stream - node's raw UTF-8 bytes land unchanged in the log file.
   *
   * Path and value embedding uses PowerShell single-quoted string literals, which are fully literal (no interpolation, no backslash escapes). The only escape
   * needed is doubling any internal single-quote character. Windows filesystem rules forbid double quotes in paths, so wrapping the Start-Process ArgumentList in
   * literal double quotes safely handles paths with spaces.
   *
   * The node and entry point paths are also written as comment metadata near the top of the file, so that stale-path detection can recover them without having to
   * parse the Start-Process invocation.
   * @param definition - The service definition to serialize.
   * @returns The launcher content as a UTF-8 string with a leading BOM.
   */
  function generateLauncher(definition: ServiceDefinition): string {

    const stdoutLog = path.join(definition.logsDir, "service-stdout.log");
    const stderrLog = path.join(definition.logsDir, "service-stderr.log");
    const envLines = sortedEnvEntries(definition.envVars).map(([ key, value ]) => "$env:" + key + " = " + powerShellLiteral(value));

    // The Start-Process ArgumentList is a PowerShell single-quoted string that contains a literal double-quoted entry-point path. Windows disallows double quotes
    // in paths, so this quoting is always well-formed.
    const argumentList = "'\"" + escapePowerShellSingleQuoted(definition.entryPoint) + "\"'";

    // PowerShell 5.1's default file encoding is ambiguous. A leading UTF-8 BOM (U+FEFF) ensures every supported PowerShell version parses the launcher as UTF-8.
    // Windows line endings (CRLF) are conventional for .ps1 files and match what tools like Set-Content produce.
    return "\uFEFF" + [
      "# PrismCast service launcher.",
      "#",
      "# This file is auto-generated by `prismcast service install`. Manual edits will be overwritten the next time the service is installed or restarted.",
      "#",
      "# The following metadata lines are read back by `prismcast service status` to detect stale paths after a PrismCast upgrade. Do not remove them.",
      "# node: " + definition.nodePath,
      "# entry: " + definition.entryPoint,
      "",
      "# Environment variables for the service process.",
      ...envLines,
      "",
      "# Spawn node with stdout and stderr redirected to separate log files. Start-Process binds the child's file descriptors directly, so byte-level content flows",
      "# through unmodified. WindowStyle 'Hidden' gives node its own hidden console - the reliable pattern for combining stream redirection with a suppressed window",
      "# across every supported PowerShell version. Since the outer PowerShell is already launched hidden by Task Scheduler, the user sees no window at any point.",
      "# -Wait keeps the PowerShell host alive until node exits, so Task Scheduler's RestartOnFailure policy can observe non-zero exit codes and restart as intended.",
      "$startArgs = @{",
      "  ArgumentList = " + argumentList,
      "  FilePath = " + powerShellLiteral(definition.nodePath),
      "  PassThru = $true",
      "  RedirectStandardError = " + powerShellLiteral(stderrLog),
      "  RedirectStandardOutput = " + powerShellLiteral(stdoutLog),
      "  Wait = $true",
      "  WindowStyle = 'Hidden'",
      "  WorkingDirectory = " + powerShellLiteral(definition.workingDir),
      "}",
      "",
      "$process = Start-Process @startArgs",
      "exit $process.ExitCode",
      ""
    ].join("\r\n");
  }

  /**
   * Removes the legacy three-file service artifacts (.cmd launcher, .vbs wrapper, .xml task definition) and the long-obsolete service-installed.marker file from
   * prior PrismCast versions. Safe to call unconditionally; missing files are silently ignored.
   * @param installDir - The service file directory.
   */
  async function removeLegacyWindowsArtifacts(installDir: string): Promise<void> {

    const legacyFiles = [
      path.join(installDir, "prismcast-service.cmd"),
      path.join(installDir, "prismcast-service.vbs"),
      path.join(installDir, "prismcast-task.xml"),
      path.join(installDir, "service-installed.marker")
    ];

    await Promise.all(legacyFiles.map(async (filePath) => fsPromises.rm(filePath, { force: true })));
  }

  return {

    async install(definition: ServiceDefinition): Promise<void> {

      const launcherPath = getServiceFilePath();
      const installDir = getServiceFileDirectory();

      // Ensure directories exist.
      await fsPromises.mkdir(installDir, { recursive: true });
      await fsPromises.mkdir(definition.logsDir, { recursive: true });

      // Remove legacy artifacts from pre-PowerShell versions so the data directory contains only the current launcher.
      await removeLegacyWindowsArtifacts(installDir);

      // Write the launcher .ps1 with a UTF-8 BOM for unambiguous PowerShell parsing across versions.
      await fsPromises.writeFile(launcherPath, generateLauncher(definition), "utf8");

      // Register (or replace) the scheduled task. -Force in the registration script handles reinstall.
      await invokePowerShell(WINDOWS_REGISTER_SCRIPT, [ taskName, launcherPath ]);

      // Start the task immediately so the user does not need to log out and back in.
      try {

        await invokePowerShell(WINDOWS_START_SCRIPT, [taskName]);
      } catch {

        // Best-effort start. The task is registered and will fire at next logon regardless.
      }
    },

    async isInstalled(): Promise<boolean> {

      try {

        await invokePowerShell(WINDOWS_IS_INSTALLED_SCRIPT, [taskName]);

        return true;
      } catch {

        return false;
      }
    },

    async isRunning(): Promise<boolean> {

      try {

        await invokePowerShell(WINDOWS_IS_RUNNING_SCRIPT, [taskName]);

        return true;
      } catch {

        return false;
      }
    },

    platform: "windows",

    serviceManager: "windows-scheduler",

    async start(): Promise<void> {

      await invokePowerShell(WINDOWS_START_SCRIPT, [taskName]);
    },

    async stop(): Promise<void> {

      await invokePowerShell(WINDOWS_STOP_SCRIPT, [taskName]);
    },

    async uninstall(): Promise<void> {

      const launcherPath = getServiceFilePath();
      const installDir = getServiceFileDirectory();

      // Deregister the task. SilentlyContinue in the script makes this a no-op if the task was never registered.
      try {

        await invokePowerShell(WINDOWS_UNREGISTER_SCRIPT, [taskName]);
      } catch {

        // Ignore - task may not exist.
      }

      // Remove the launcher and any residual legacy artifacts.
      await fsPromises.rm(launcherPath, { force: true });
      await removeLegacyWindowsArtifacts(installDir);
    }
  };
}

/**
 * Paths extracted from an existing service file.
 */
export interface ServicePaths {

  // The entry point path (e.g., /opt/homebrew/lib/node_modules/prismcast/dist/index.js).
  entryPoint: string;

  // The node binary path (e.g., /opt/homebrew/bin/node).
  nodePath: string;
}

/**
 * Result of stale path detection.
 */
export interface StalePathResult {

  // The entry point from the service file (present when stale).
  entryPoint?: string;

  // The node binary from the service file (present when stale).
  nodePath?: string;

  // True if one or more paths in the service file no longer exist on disk.
  stale: boolean;
}

/**
 * Reads the existing service file and extracts the node binary and PrismCast entry point paths. Each platform has its own format: launchd plist XML, systemd unit
 * ExecStart line, or Windows PowerShell launcher with comment metadata.
 * @returns The extracted paths, or null if the file does not exist or cannot be parsed.
 */
export function getServicePaths(): Nullable<ServicePaths> {

  const filePath = getServiceFilePath();

  if(!fs.existsSync(filePath)) {

    return null;
  }

  let content: string;

  try {

    content = fs.readFileSync(filePath, "utf8");
  } catch {

    return null;
  }

  switch(getPlatform()) {

    // Launchd plist: the ProgramArguments array contains two <string> elements - the first is the node path, the second is the entry point. We slice to the section
    // after the ProgramArguments key, then take the first two string values via matchAll + iterator helpers (lazy, early-terminating, no intermediate arrays).
    case "darwin": {

      const programArgsIndex = content.indexOf("<key>ProgramArguments</key>");

      if(programArgsIndex === -1) {

        return null;
      }

      const [ nodePath, entryPoint ] = content.slice(programArgsIndex)
        .matchAll(/<string>([^<]+)<\/string>/g)
        .take(2)
        .map((match) => match[1])
        .toArray();

      if(!nodePath || !entryPoint) {

        return null;
      }

      return { entryPoint, nodePath };
    }

    // Systemd unit: ExecStart=<node> <entrypoint> on one line.
    case "linux": {

      const execStart = /^ExecStart=(.+)$/m.exec(content)?.[1];

      if(!execStart) {

        return null;
      }

      const [ nodePath, entryPoint ] = execStart.split(" ");

      if(!nodePath || !entryPoint) {

        return null;
      }

      return { entryPoint, nodePath };
    }

    // Windows: paths are stored as "# node:<path>" and "# entry:<path>" metadata comments near the top of the PowerShell launcher. The .trim() handles CRLF line
    // endings where \r would otherwise be captured by the regex.
    case "windows": {

      const nodePath = /^# node:(.+)$/m.exec(content)?.[1]?.trim();
      const entryPoint = /^# entry:(.+)$/m.exec(content)?.[1]?.trim();

      if(!nodePath || !entryPoint) {

        return null;
      }

      return { entryPoint, nodePath };
    }

    default: {

      return null;
    }
  }
}

/**
 * Checks whether the paths in the existing service file still exist on disk. This detects the common post-upgrade scenario where Homebrew or npm has moved the
 * installation to a new versioned directory and the old paths no longer resolve.
 * @returns A StalePathResult indicating which paths are missing, or null if the service file does not exist or cannot be parsed.
 */
export function detectStalePaths(): Nullable<StalePathResult> {

  const paths = getServicePaths();

  if(!paths) {

    return null;
  }

  const nodeStale = !fs.existsSync(paths.nodePath);
  const entryStale = !fs.existsSync(paths.entryPoint);

  return {

    entryPoint: entryStale ? paths.entryPoint : undefined,
    nodePath: nodeStale ? paths.nodePath : undefined,
    stale: nodeStale || entryStale
  };
}

/**
 * Returns the service generator for the current platform.
 * @returns The appropriate ServiceGenerator, or null if the platform is not supported.
 */
export function getServiceGenerator(): Nullable<ServiceGenerator> {

  switch(getPlatform()) {

    case "darwin": {

      return createLaunchdGenerator();
    }

    case "linux": {

      return createSystemdGenerator();
    }

    case "windows": {

      return createWindowsSchedulerGenerator();
    }

    default: {

      return null;
    }
  }
}

/* Env vars that belong in the service environment but are not declared in CONFIG_METADATA. PRISMCAST_DATA_DIR resolves before config.json is read
 * (chicken-and-egg bootstrap), and PRISMCAST_DEBUG is a runtime-only setting parsed in the entry point - see src/index.ts where both are called out as special
 * cases for the same reason.
 */
const BOOTSTRAP_ENV_VARS = [ "PRISMCAST_DATA_DIR", "PRISMCAST_DEBUG" ] as const;

/**
 * Returns the full set of env var names that represent user-configurable PrismCast settings. Derived from CONFIG_METADATA (the documented single source of truth
 * for configurable settings) plus the bootstrap-only variables enumerated above. Deriving from CONFIG_METADATA means new settings are automatically captured by
 * the service layer the moment they are declared in config metadata - no second list to maintain.
 * @returns An array of env var names.
 */
function getConfigurableEnvVarNames(): string[] {

  const fromMetadata = Object.values(CONFIG_METADATA).flat().map((setting) => setting.envVar).filter((envVar) => envVar !== null);

  return [ ...fromMetadata, ...BOOTSTRAP_ENV_VARS ];
}

/**
 * Collects environment variables that should be persisted in the service definition. Captures every CONFIG_METADATA-declared env var (plus the bootstrap
 * variables that cannot live there) that is currently set in process.env, so anything the user configured via env for this run is preserved into the installed
 * service.
 * @returns A record of environment variable names to values.
 */
export function collectServiceEnvironment(): Record<string, string> {

  const envVars: Record<string, string> = {};

  // Always capture PATH so that FFmpeg and other tools can be found. Service managers like launchd use a minimal PATH by default (/usr/bin:/bin:/usr/sbin:/sbin)
  // which does not include Homebrew or other common tool locations.
  if(process.env.PATH) {

    envVars.PATH = process.env.PATH;
  }

  for(const key of getConfigurableEnvVarNames()) {

    const value = process.env[key];

    if(value !== undefined) {

      envVars[key] = value;
    }
  }

  return envVars;
}

/**
 * Builds a ServiceDefinition from the current runtime context. Composes the platform helpers (node path, entry point, working directory, logs directory) with the
 * collected service environment, and stamps PRISMCAST_SERVICE=1 for service-mode detection.
 * @returns The structured service definition.
 */
export function buildServiceDefinition(): ServiceDefinition {

  const envVars: Record<string, string> = { PRISMCAST_SERVICE: "1", ...collectServiceEnvironment() };

  return {

    entryPoint: getPrismCastEntryPoint(),
    envVars,
    logsDir: getLogsDirectory(),
    nodePath: getNodeExecutablePath(),
    workingDir: getPrismCastWorkingDirectory()
  };
}
