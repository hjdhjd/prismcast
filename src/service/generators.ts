/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * generators.ts: Platform-specific service file generators for PrismCast.
 */
import type { Platform, ServiceManager } from "../utils/platform.js";
import { SERVICE_ID, SERVICE_NAME, getLogsDirectory, getNodeExecutablePath, getPlatform, getPrismCastEntryPoint, getPrismCastWorkingDirectory, getServiceFileDirectory,
  getServiceFilePath } from "../utils/platform.js";
import { execFileSync, execSync } from "node:child_process";
import type { Nullable } from "../types/index.js";
import fs from "node:fs";
import path from "node:path";

const { promises: fsPromises } = fs;

/* These generators create platform-specific service definitions that allow PrismCast to run as a managed service. Each generator produces the appropriate
 * configuration format for its service manager (launchd plist for macOS, systemd unit for Linux, Task Scheduler task for Windows).
 *
 * Key features of generated services:
 * - Auto-start at user login (user-level service, no root required)
 * - Auto-restart on crash (KeepAlive/Restart=always)
 * - PRISMCAST_SERVICE=1 environment variable for service detection
 * - Stdout/stderr capture for backup logging
 */

/**
 * Options for generating a service file.
 */
export interface ServiceOptions {

  // Environment variables to include in the service (in addition to PRISMCAST_SERVICE=1).
  envVars?: Record<string, string>;
}

/**
 * Interface for platform-specific service generators.
 */
export interface ServiceGenerator {

  // Generate the service file content.
  generate(options: ServiceOptions): string;

  // Get the path where the service file should be installed.
  getInstallPath(): string;

  // Install the service (write file and enable).
  install(content: string): Promise<void>;

  // Check if the service is currently installed.
  isInstalled(): Promise<boolean>;

  // Check if the service is currently running.
  isRunning(): Promise<boolean>;

  // Get the platform this generator is for.
  platform: Platform;

  // Get the service manager type.
  serviceManager: ServiceManager;

  // Start the service.
  start(): Promise<void>;

  // Stop the service.
  stop(): Promise<void>;

  // Uninstall the service (disable and remove file).
  uninstall(): Promise<void>;
}

/* Generates a launchd property list (plist) file for macOS. The plist is installed to ~/Library/LaunchAgents/ and configured with:
 * - RunAtLoad: Start when user logs in
 * - KeepAlive: Restart automatically if the process exits
 * - StandardOutPath/StandardErrorPath: Capture stdout/stderr to the data directory
 */

/**
 * Escapes a string for use in XML by replacing special characters with entities.
 * @param str - The string to escape.
 * @returns The escaped string safe for XML.
 */
function escapeXml(str: string): string {

  return str.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll("\"", "&quot;").replaceAll("'", "&apos;");
}

/**
 * Creates a launchd service generator for macOS.
 * @returns A ServiceGenerator for launchd.
 */
function createLaunchdGenerator(): ServiceGenerator {

  return {

    generate(options: ServiceOptions): string {

      const nodePath = getNodeExecutablePath();
      const entryPoint = getPrismCastEntryPoint();
      const workingDir = getPrismCastWorkingDirectory();
      const logsDir = getLogsDirectory();

      // Build environment variables section. Always include PRISMCAST_SERVICE=1 for service detection.
      const envVars: Record<string, string> = { PRISMCAST_SERVICE: "1", ...options.envVars };

      // Generate the environment dictionary entries.
      const envEntries = Object.entries(envVars)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([ key, value ]) => [
          "      <key>" + escapeXml(key) + "</key>",
          "      <string>" + escapeXml(value) + "</string>"
        ].join("\n"))
        .join("\n");

      // Generate the plist content.
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
        "    <string>" + escapeXml(nodePath) + "</string>",
        "    <string>" + escapeXml(entryPoint) + "</string>",
        "  </array>",
        "",
        "  <key>WorkingDirectory</key>",
        "  <string>" + escapeXml(workingDir) + "</string>",
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
        "  <string>" + escapeXml(logsDir + "/service-stdout.log") + "</string>",
        "",
        "  <key>StandardErrorPath</key>",
        "  <string>" + escapeXml(logsDir + "/service-stderr.log") + "</string>",
        "</dict>",
        "</plist>",
        ""
      ].join("\n");
    },

    getInstallPath(): string {

      return getServiceFilePath();
    },

    async install(content: string): Promise<void> {

      const installPath = this.getInstallPath();
      const installDir = getServiceFileDirectory();
      const logsDir = getLogsDirectory();

      // Ensure directories exist.
      await fsPromises.mkdir(installDir, { recursive: true });
      await fsPromises.mkdir(logsDir, { recursive: true });

      // Write the plist file.
      await fsPromises.writeFile(installPath, content, "utf8");

      // Load the service with launchctl.
      try {

        execSync("launchctl load -w \"" + installPath + "\"", { stdio: "pipe" });
      } catch {

        // If load fails, try to unload first then reload (handles reinstall case).
        try {

          execSync("launchctl unload \"" + installPath + "\"", { stdio: "pipe" });
        } catch {

          // Ignore unload errors.
        }

        execSync("launchctl load -w \"" + installPath + "\"", { stdio: "pipe" });
      }
    },

    // eslint-disable-next-line @typescript-eslint/require-await
    async isInstalled(): Promise<boolean> {

      return fs.existsSync(this.getInstallPath());
    },

    // eslint-disable-next-line @typescript-eslint/require-await
    async isRunning(): Promise<boolean> {

      try {

        // Use launchctl list | grep to get tab-separated output: "PID\tStatus\tLabel". The grep exits non-zero if not found.
        const result = execSync("launchctl list | grep " + SERVICE_ID, { encoding: "utf8", stdio: "pipe" });

        // Parse the PID from the first column. Format: "12345\t0\tcom.github.hjdhjd.prismcast" or "-\t0\t..." if loaded but not running.
        const pid = result.trim().split("\t")[0];

        // PID is "-" when loaded but process not running, or a number when actually running.
        return (pid !== "-") && !isNaN(Number(pid));
      } catch {

        return false;
      }
    },

    platform: "darwin",

    serviceManager: "launchd",

    // eslint-disable-next-line @typescript-eslint/require-await
    async start(): Promise<void> {

      const installPath = this.getInstallPath();

      // Unload first to clear any stale loaded-but-not-running state. Without this, `launchctl load -w` is a no-op when the definition is already loaded
      // (e.g., after a crash or upgrade with changed paths), and the cached stale definition is reused.
      try {

        execSync("launchctl unload \"" + installPath + "\"", { stdio: "pipe" });
      } catch {

        // Ignore — may not be loaded.
      }

      // Load the plist with the current (possibly updated) definition.
      execSync("launchctl load -w \"" + installPath + "\"", { stdio: "pipe" });

      // Explicitly start the service. This handles the case where RunAtLoad doesn't trigger (e.g., load-then-start sequence).
      execSync("launchctl start " + SERVICE_ID, { stdio: "pipe" });
    },

    // eslint-disable-next-line @typescript-eslint/require-await
    async stop(): Promise<void> {

      const installPath = this.getInstallPath();

      execSync("launchctl unload \"" + installPath + "\"", { stdio: "pipe" });
    },

    async uninstall(): Promise<void> {

      const installPath = this.getInstallPath();

      // Unload the service first.
      try {

        execSync("launchctl unload \"" + installPath + "\"", { stdio: "pipe" });
      } catch {

        // Ignore errors if service wasn't loaded.
      }

      // Remove the plist file.
      await fsPromises.rm(installPath, { force: true });
    }
  };
}

/* Generates a systemd user service unit file for Linux. The unit is installed to ~/.config/systemd/user/ and configured with:
 * - Restart=always: Restart automatically if the process exits
 * - RestartSec=5: Wait 5 seconds before restarting
 * - WantedBy=default.target: Start when user session begins
 */

/**
 * Creates a systemd service generator for Linux.
 * @returns A ServiceGenerator for systemd.
 */
function createSystemdGenerator(): ServiceGenerator {

  return {

    generate(options: ServiceOptions): string {

      const nodePath = getNodeExecutablePath();
      const entryPoint = getPrismCastEntryPoint();
      const workingDir = getPrismCastWorkingDirectory();

      // Build environment variables. Always include PRISMCAST_SERVICE=1 for service detection.
      const envVars: Record<string, string> = { PRISMCAST_SERVICE: "1", ...options.envVars };

      // Generate Environment= lines, sorted alphabetically.
      const envLines = Object.entries(envVars).sort(([a], [b]) => a.localeCompare(b)).map(([ key, value ]) => "Environment=\"" + key + "=" + value + "\"").join("\n");

      // Generate the unit file content.
      return [
        "[Unit]",
        "Description=" + SERVICE_NAME + " Streaming Server",
        "After=network.target",
        "",
        "[Service]",
        "Type=simple",
        "ExecStart=" + nodePath + " " + entryPoint,
        "WorkingDirectory=" + workingDir,
        "Restart=always",
        "RestartSec=5",
        envLines,
        "",
        "[Install]",
        "WantedBy=default.target",
        ""
      ].join("\n");
    },

    getInstallPath(): string {

      return getServiceFilePath();
    },

    async install(content: string): Promise<void> {

      const installPath = this.getInstallPath();
      const installDir = getServiceFileDirectory();
      const logsDir = getLogsDirectory();

      // Ensure directories exist.
      await fsPromises.mkdir(installDir, { recursive: true });
      await fsPromises.mkdir(logsDir, { recursive: true });

      // Write the unit file.
      await fsPromises.writeFile(installPath, content, "utf8");

      // Reload systemd to pick up the new unit file.
      try {

        execSync("systemctl --user daemon-reload", { stdio: "pipe" });
      } catch {

        // Ignore if systemctl isn't available (shouldn't happen on systemd systems).
      }

      // Enable and start the service.
      execSync("systemctl --user enable prismcast.service", { stdio: "pipe" });
      execSync("systemctl --user start prismcast.service", { stdio: "pipe" });
    },

    // eslint-disable-next-line @typescript-eslint/require-await
    async isInstalled(): Promise<boolean> {

      return fs.existsSync(this.getInstallPath());
    },

    // eslint-disable-next-line @typescript-eslint/require-await
    async isRunning(): Promise<boolean> {

      try {

        const result = execSync("systemctl --user is-active prismcast.service", { encoding: "utf8", stdio: "pipe" });

        return result.trim() === "active";
      } catch {

        return false;
      }
    },

    platform: "linux",

    serviceManager: "systemd",

    // eslint-disable-next-line @typescript-eslint/require-await
    async start(): Promise<void> {

      execSync("systemctl --user start prismcast.service", { stdio: "pipe" });
    },

    // eslint-disable-next-line @typescript-eslint/require-await
    async stop(): Promise<void> {

      execSync("systemctl --user stop prismcast.service", { stdio: "pipe" });
    },

    async uninstall(): Promise<void> {

      const installPath = this.getInstallPath();

      // Stop and disable the service.
      try {

        execSync("systemctl --user stop prismcast.service", { stdio: "pipe" });
      } catch {

        // Ignore if not running.
      }

      try {

        execSync("systemctl --user disable prismcast.service", { stdio: "pipe" });
      } catch {

        // Ignore if not enabled.
      }

      // Remove the unit file.
      await fsPromises.rm(installPath, { force: true });

      // Reload systemd.
      try {

        execSync("systemctl --user daemon-reload", { stdio: "pipe" });
      } catch {

        // Ignore.
      }
    }
  };
}

/* Uses Windows Task Scheduler to run PrismCast at user logon. Three files in the data directory define the service: a batch startup script (.cmd) with environment
 * setup and the node invocation, a VBScript wrapper (.vbs) that launches it with a hidden console window, and a Task Scheduler XML definition (.xml) imported via
 * schtasks /Create /XML. The XML format avoids the shell quoting issues inherent in schtasks /TR and enables advanced task settings (restart on failure, unlimited
 * execution time, battery policy) that command-line flags cannot express. All schtasks calls use execFileSync to bypass cmd.exe shell interpretation entirely.
 */

/**
 * Escapes a string for use in a Windows batch (.cmd) file. Literal percent characters must be doubled because batch interprets % as variable expansion even inside
 * quoted strings.
 * @param value - The string to escape.
 * @returns The escaped string safe for batch files.
 */
function escapeBatchValue(value: string): string {

  return value.replaceAll("%", "%%");
}

/**
 * Creates a Windows Task Scheduler generator.
 * @returns A ServiceGenerator for Windows Task Scheduler.
 */
function createWindowsSchedulerGenerator(): ServiceGenerator {

  const taskName = SERVICE_NAME;

  /**
   * Returns the path to the VBScript wrapper file that launches PrismCast with a hidden console window.
   * @returns The absolute path to the .vbs file in the data directory.
   */
  function getVbsPath(): string {

    return path.join(getServiceFileDirectory(), "prismcast-service.vbs");
  }

  /**
   * Returns the path to the Task Scheduler XML definition file.
   * @returns The absolute path to the .xml file in the data directory.
   */
  function getXmlPath(): string {

    return path.join(getServiceFileDirectory(), "prismcast-task.xml");
  }

  /**
   * Generates the Task Scheduler XML definition. The XML format provides structured task configuration without shell quoting and supports advanced settings
   * (restart on failure, no execution time limit, battery policy) that schtasks command-line flags cannot express. Element ordering follows the Task Scheduler
   * XML schema.
   * @param vbsFilePath - The absolute path to the VBScript wrapper that launches PrismCast.
   * @returns The XML content for the task definition.
   */
  function generateTaskXml(vbsFilePath: string): string {

    return [
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
      "<Task version=\"1.2\" xmlns=\"http://schemas.microsoft.com/windows/2004/02/mit/task\">",
      "  <RegistrationInfo>",
      "    <Description>" + escapeXml(SERVICE_NAME + " Streaming Server") + "</Description>",
      "  </RegistrationInfo>",
      "  <Triggers>",
      "    <LogonTrigger>",
      "      <Enabled>true</Enabled>",
      "    </LogonTrigger>",
      "  </Triggers>",
      "  <Principals>",
      "    <Principal id=\"Author\">",
      "      <LogonType>InteractiveToken</LogonType>",
      "      <RunLevel>HighestAvailable</RunLevel>",
      "    </Principal>",
      "  </Principals>",
      "  <Settings>",
      "    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>",
      "    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>",
      "    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>",
      "    <Enabled>true</Enabled>",
      "    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>",
      "    <RestartOnFailure>",
      "      <Interval>PT5S</Interval>",
      "      <Count>3</Count>",
      "    </RestartOnFailure>",
      "  </Settings>",
      "  <Actions Context=\"Author\">",
      "    <Exec>",
      "      <Command>wscript.exe</Command>",
      "      <Arguments>" + escapeXml("\"" + vbsFilePath + "\"") + "</Arguments>",
      "    </Exec>",
      "  </Actions>",
      "</Task>",
      ""
    ].join("\n");
  }

  return {

    generate(options: ServiceOptions): string {

      const nodePath = getNodeExecutablePath();
      const entryPoint = getPrismCastEntryPoint();
      const workingDir = getPrismCastWorkingDirectory();

      // Build environment variables. Always include PRISMCAST_SERVICE=1 for service detection.
      const envVars: Record<string, string> = { PRISMCAST_SERVICE: "1", ...options.envVars };

      // Generate set commands for environment variables, sorted alphabetically. Values are escaped for batch interpretation where literal % must be doubled.
      const envLines = Object.entries(envVars)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([ key, value ]) => "set \"" + key + "=" + escapeBatchValue(value) + "\"");

      // Generate the batch file content. Each line uses exactly one level of quoting because the batch file is a standalone script, not an inline argument embedded
      // in another command. The rem lines provide machine-readable path metadata for stale path detection, parsed by getServicePaths(). CRLF line endings follow
      // Windows batch file convention.
      return [
        "@echo off",
        "rem node:" + nodePath,
        "rem entry:" + entryPoint,
        "cd /d \"" + escapeBatchValue(workingDir) + "\"",
        ...envLines,
        "\"" + escapeBatchValue(nodePath) + "\" \"" + escapeBatchValue(entryPoint) + "\""
      ].join("\r\n") + "\r\n";
    },

    getInstallPath(): string {

      return getServiceFilePath();
    },

    async install(content: string): Promise<void> {

      const cmdPath = getServiceFilePath();
      const vbsPath = getVbsPath();
      const xmlPath = getXmlPath();
      const installDir = getServiceFileDirectory();
      const logsDir = getLogsDirectory();

      // Ensure directories exist.
      await fsPromises.mkdir(installDir, { recursive: true });
      await fsPromises.mkdir(logsDir, { recursive: true });

      // Delete existing task if it exists.
      try {

        execFileSync("schtasks", [ "/Delete", "/TN", taskName, "/F" ], { stdio: "pipe" });
      } catch {

        // Ignore if task doesn't exist.
      }

      // Write the batch startup script.
      await fsPromises.writeFile(cmdPath, content, "utf8");

      // Write the VBScript wrapper that launches the batch file with a hidden console window. The WshShell.Run second parameter (0) specifies the vbHide window
      // style, and the third parameter (False) means don't wait for the script to finish.
      const vbsContent = "Set WshShell = CreateObject(\"WScript.Shell\")\r\nWshShell.Run \"\"\"" + cmdPath + "\"\"\", 0, False\r\n";

      await fsPromises.writeFile(vbsPath, vbsContent, "utf8");

      // Write the Task Scheduler XML definition and import it. Using XML import instead of schtasks /TR avoids all shell quoting issues and enables advanced task
      // settings that command-line flags cannot express.
      const xmlContent = generateTaskXml(vbsPath);

      await fsPromises.writeFile(xmlPath, xmlContent, "utf8");

      // Import the task definition. execFileSync passes arguments directly to the Windows API, bypassing cmd.exe shell interpretation entirely.
      execFileSync("schtasks", [ "/Create", "/XML", xmlPath, "/TN", taskName, "/F" ], { stdio: "pipe" });

      // Clean up legacy marker file from previous versions that used a separate metadata file for path tracking.
      await fsPromises.rm(path.join(installDir, "service-installed.marker"), { force: true });

      // Start the task immediately.
      try {

        execFileSync("schtasks", [ "/Run", "/TN", taskName ], { stdio: "pipe" });
      } catch {

        // Ignore if start fails.
      }
    },

    // eslint-disable-next-line @typescript-eslint/require-await
    async isInstalled(): Promise<boolean> {

      try {

        execFileSync("schtasks", [ "/Query", "/TN", taskName ], { stdio: "pipe" });

        return true;
      } catch {

        return false;
      }
    },

    // eslint-disable-next-line @typescript-eslint/require-await
    async isRunning(): Promise<boolean> {

      try {

        const result = execFileSync("schtasks", [ "/Query", "/TN", taskName, "/FO", "CSV", "/NH" ], { encoding: "utf8", stdio: "pipe" });

        return result.includes("Running");
      } catch {

        return false;
      }
    },

    platform: "windows",

    serviceManager: "windows-scheduler",

    // eslint-disable-next-line @typescript-eslint/require-await
    async start(): Promise<void> {

      // Re-enable the task (it may have been disabled by stop() to prevent RestartOnFailure from restarting the process) and run it.
      try {

        execFileSync("schtasks", [ "/Change", "/TN", taskName, "/Enable" ], { stdio: "pipe" });
      } catch {

        // Ignore if already enabled or task doesn't exist.
      }

      execFileSync("schtasks", [ "/Run", "/TN", taskName ], { stdio: "pipe" });
    },

    // eslint-disable-next-line @typescript-eslint/require-await
    async stop(): Promise<void> {

      // Disable the task first to prevent RestartOnFailure from automatically restarting the process after we terminate it.
      try {

        execFileSync("schtasks", [ "/Change", "/TN", taskName, "/Disable" ], { stdio: "pipe" });
      } catch {

        // Ignore if task doesn't exist.
      }

      execFileSync("schtasks", [ "/End", "/TN", taskName ], { stdio: "pipe" });
    },

    async uninstall(): Promise<void> {

      const cmdPath = getServiceFilePath();
      const vbsPath = getVbsPath();
      const xmlPath = getXmlPath();

      // Delete the scheduled task.
      try {

        execFileSync("schtasks", [ "/Delete", "/TN", taskName, "/F" ], { stdio: "pipe" });
      } catch {

        // Ignore if task doesn't exist.
      }

      // Remove all service files and legacy marker file from previous versions.
      await Promise.all(
        [ cmdPath, vbsPath, xmlPath, path.join(getServiceFileDirectory(), "service-installed.marker") ].map(async (filePath) => fsPromises.rm(filePath, { force: true }))
      );
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
 * ExecStart line, or Windows batch startup script with rem-prefixed path comments.
 * @returns The extracted paths, or null if the file doesn't exist or can't be parsed.
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

    // Launchd plist: ProgramArguments contains two <string> elements — first is the node path, second is the entry point.
    case "darwin": {

      const stringPattern = /<string>([^<]+)<\/string>/g;
      const matches: string[] = [];
      let match: Nullable<RegExpExecArray>;

      // Walk the ProgramArguments array. We look for the section after the ProgramArguments key and extract the first two string values.
      const programArgsIndex = content.indexOf("<key>ProgramArguments</key>");

      if(programArgsIndex === -1) {

        return null;
      }

      // Extract strings from the <array> section following ProgramArguments.
      const arraySection = content.slice(programArgsIndex);

      while((match = stringPattern.exec(arraySection)) !== null) {

        matches.push(match[1]);

        if(matches.length === 2) {

          break;
        }
      }

      if(matches.length < 2) {

        return null;
      }

      return { entryPoint: matches[1], nodePath: matches[0] };
    }

    // Systemd unit: ExecStart=<node> <entrypoint> on one line.
    case "linux": {

      const execStartMatch = /^ExecStart=(.+)$/m.exec(content);

      if(!execStartMatch) {

        return null;
      }

      const parts = execStartMatch[1].split(" ");

      if(parts.length < 2) {

        return null;
      }

      return { entryPoint: parts[1], nodePath: parts[0] };
    }

    // Windows: paths are stored as "rem node:<path>" and "rem entry:<path>" comments in the batch startup script (.cmd file). The .trim() handles CRLF line
    // endings in batch files where \r would otherwise be captured by the regex.
    case "windows": {

      const nodeMatch = /^rem node:(.+)$/m.exec(content);
      const entryMatch = /^rem entry:(.+)$/m.exec(content);

      if(!nodeMatch || !entryMatch) {

        return null;
      }

      return { entryPoint: entryMatch[1].trim(), nodePath: nodeMatch[1].trim() };
    }

    default: {

      return null;
    }
  }
}

/**
 * Checks whether the paths in the existing service file still exist on disk. This detects the common post-upgrade scenario where Homebrew or npm has moved the
 * installation to a new versioned directory and the old paths no longer resolve.
 * @returns A StalePathResult indicating which paths are missing, or null if the service file doesn't exist or can't be parsed.
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

/* Returns the appropriate service generator for the current platform.
 */

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

/**
 * Collects environment variables that should be persisted in the service file. This includes settings that differ from defaults or have been explicitly configured.
 * @returns A record of environment variable names to values.
 */
export function collectServiceEnvironment(): Record<string, string> {

  const envVars: Record<string, string> = {};

  // Always capture PATH so that FFmpeg and other tools can be found. Service managers like launchd use a minimal PATH by default (/usr/bin:/bin:/usr/sbin:/sbin)
  // which doesn't include Homebrew or other common tool locations.
  if(process.env.PATH) {

    envVars.PATH = process.env.PATH;
  }

  // Include key settings if they're set via environment. These are the settings most likely to be intentionally configured.
  const keysToCapture = [
    "AUDIO_BITRATE",
    "CAPTURE_MODE",
    "CHROME_BIN",
    "FRAME_RATE",
    "HOST",
    "LOG_MAX_SIZE",
    "PORT",
    "PRISMCAST_CHROME_DATA_DIR",
    "PRISMCAST_DATA_DIR",
    "PRISMCAST_DEBUG",
    "PRISMCAST_LOG_FILE",
    "QUALITY_PRESET",
    "VIDEO_BITRATE"
  ];

  for(const key of keysToCapture) {

    const value = process.env[key];

    if(value !== undefined) {

      envVars[key] = value;
    }
  }

  return envVars;
}
