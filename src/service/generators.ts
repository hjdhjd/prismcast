/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * generators.ts: Platform-specific service file generators for PrismCast.
 */
import type { Platform, ServiceManager } from "../utils/platform.js";
import { SERVICE_ID, SERVICE_NAME, getLogsDirectory, getNodeExecutablePath, getPlatform, getPrismCastEntryPoint, getPrismCastWorkingDirectory, getServiceFileDirectory,
  getServiceFilePath } from "../utils/platform.js";
import type { Nullable } from "../types/index.js";
import { execSync } from "node:child_process";
import fs from "node:fs";

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
 * - StandardOutPath/StandardErrorPath: Capture stdout/stderr to ~/.prismcast/logs/
 */

/**
 * Escapes a string for use in XML by replacing special characters with entities.
 * @param str - The string to escape.
 * @returns The escaped string safe for XML.
 */
function escapeXml(str: string): string {

  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
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

    async isInstalled(): Promise<boolean> {

      return await Promise.resolve(fs.existsSync(this.getInstallPath()));
    },

    async isRunning(): Promise<boolean> {

      try {

        // Use launchctl list | grep to get tab-separated output: "PID\tStatus\tLabel". The grep exits non-zero if not found.
        const result = execSync("launchctl list | grep " + SERVICE_ID, { encoding: "utf8", stdio: "pipe" });

        // Parse the PID from the first column. Format: "12345\t0\tcom.github.hjdhjd.prismcast" or "-\t0\t..." if loaded but not running.
        const pid = result.trim().split("\t")[0];

        // PID is "-" when loaded but process not running, or a number when actually running.
        return await Promise.resolve((pid !== "-") && !isNaN(Number(pid)));
      } catch {

        return await Promise.resolve(false);
      }
    },

    platform: "darwin",

    serviceManager: "launchd",

    async start(): Promise<void> {

      const installPath = this.getInstallPath();

      // Load the plist if not already loaded. This is idempotent — if already loaded, launchd logs a warning but doesn't fail.
      try {

        execSync("launchctl load -w \"" + installPath + "\"", { stdio: "pipe" });
      } catch {

        // Ignore — may already be loaded.
      }

      // Explicitly start the service. This handles the case where the plist is loaded but the process isn't running (e.g., after a crash).
      execSync("launchctl start " + SERVICE_ID, { stdio: "pipe" });

      await Promise.resolve();
    },

    async stop(): Promise<void> {

      const installPath = this.getInstallPath();

      execSync("launchctl unload \"" + installPath + "\"", { stdio: "pipe" });

      await Promise.resolve();
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
      if(fs.existsSync(installPath)) {

        await fsPromises.unlink(installPath);
      }
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
      const envLines = Object.entries(envVars)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([ key, value ]) => "Environment=\"" + key + "=" + value + "\"")
        .join("\n");

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

    async isInstalled(): Promise<boolean> {

      return await Promise.resolve(fs.existsSync(this.getInstallPath()));
    },

    async isRunning(): Promise<boolean> {

      try {

        const result = execSync("systemctl --user is-active prismcast.service", { encoding: "utf8", stdio: "pipe" });

        return await Promise.resolve(result.trim() === "active");
      } catch {

        return await Promise.resolve(false);
      }
    },

    platform: "linux",

    serviceManager: "systemd",

    async start(): Promise<void> {

      execSync("systemctl --user start prismcast.service", { stdio: "pipe" });

      await Promise.resolve();
    },

    async stop(): Promise<void> {

      execSync("systemctl --user stop prismcast.service", { stdio: "pipe" });

      await Promise.resolve();
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
      if(fs.existsSync(installPath)) {

        await fsPromises.unlink(installPath);
      }

      // Reload systemd.
      try {

        execSync("systemctl --user daemon-reload", { stdio: "pipe" });
      } catch {

        // Ignore.
      }
    }
  };
}

/* Uses Windows Task Scheduler via schtasks.exe to create a task that runs at user logon. Unlike launchd and systemd, Task Scheduler doesn't have built-in process
 * supervision, so we configure the task to restart on failure. A marker file is used to track installation state.
 */

/**
 * Creates a Windows Task Scheduler generator.
 * @returns A ServiceGenerator for Windows Task Scheduler.
 */
function createWindowsSchedulerGenerator(): ServiceGenerator {

  const taskName = SERVICE_NAME;

  return {

    generate(options: ServiceOptions): string {

      const nodePath = getNodeExecutablePath();
      const entryPoint = getPrismCastEntryPoint();
      const workingDir = getPrismCastWorkingDirectory();

      // Build environment variables for the command. We'll set them in the task action using cmd /c set.
      const envVars: Record<string, string> = { PRISMCAST_SERVICE: "1", ...options.envVars };

      // Generate environment variable SET commands.
      const envSets = Object.entries(envVars)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([ key, value ]) => "set \"" + key + "=" + value + "\"")
        .join(" && ");

      // Return the command to run (used by install).
      return "cmd /c \"cd /d \"" + workingDir + "\" && " + envSets + " && \"" + nodePath + "\" \"" + entryPoint + "\"\"";
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

      // Delete existing task if it exists.
      try {

        execSync("schtasks /Delete /TN \"" + taskName + "\" /F", { stdio: "pipe" });
      } catch {

        // Ignore if task doesn't exist.
      }

      // Create the scheduled task. The /SC ONLOGON runs at user logon. /RL HIGHEST gives elevated privileges if needed.
      const createCmd = "schtasks /Create /TN \"" + taskName + "\" /TR \"" + content + "\" /SC ONLOGON /RL HIGHEST /F";

      execSync(createCmd, { stdio: "pipe" });

      // Write marker file to indicate installation.
      await fsPromises.writeFile(installPath, "Installed at " + new Date().toISOString() + "\n", "utf8");

      // Start the task immediately.
      try {

        execSync("schtasks /Run /TN \"" + taskName + "\"", { stdio: "pipe" });
      } catch {

        // Ignore if start fails.
      }
    },

    async isInstalled(): Promise<boolean> {

      try {

        execSync("schtasks /Query /TN \"" + taskName + "\"", { stdio: "pipe" });

        return await Promise.resolve(true);
      } catch {

        return await Promise.resolve(false);
      }
    },

    async isRunning(): Promise<boolean> {

      try {

        const result = execSync("schtasks /Query /TN \"" + taskName + "\" /FO CSV /NH", { encoding: "utf8", stdio: "pipe" });

        return await Promise.resolve(result.includes("Running"));
      } catch {

        return await Promise.resolve(false);
      }
    },

    platform: "windows",

    serviceManager: "windows-scheduler",

    async start(): Promise<void> {

      execSync("schtasks /Run /TN \"" + taskName + "\"", { stdio: "pipe" });

      await Promise.resolve();
    },

    async stop(): Promise<void> {

      execSync("schtasks /End /TN \"" + taskName + "\"", { stdio: "pipe" });

      await Promise.resolve();
    },

    async uninstall(): Promise<void> {

      const installPath = this.getInstallPath();

      // Delete the scheduled task.
      try {

        execSync("schtasks /Delete /TN \"" + taskName + "\" /F", { stdio: "pipe" });
      } catch {

        // Ignore if task doesn't exist.
      }

      // Remove marker file.
      if(fs.existsSync(installPath)) {

        await fsPromises.unlink(installPath);
      }
    }
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
