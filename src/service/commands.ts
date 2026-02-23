/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * commands.ts: Service command handlers for PrismCast CLI.
 */
import { DEFAULTS, loadUserConfig } from "../config/userConfig.js";
import { SERVICE_NAME, getNodeExecutablePath, getPlatform, getPrismCastEntryPoint, getServiceFilePath } from "../utils/platform.js";
import { collectServiceEnvironment, detectStalePaths, getServiceGenerator, getServicePaths } from "./generators.js";
import type { Nullable } from "../types/index.js";
import type { ServiceGenerator } from "./generators.js";
import { getDataDir } from "../config/paths.js";
import path from "node:path";

// Types.

/**
 * Response from the /streams endpoint.
 */
interface StreamsResponse {

  count: number;
  limit: number;
  streams: {
    channel: Nullable<string>;
    duration: number;
    id: number;
    url: string;
  }[];
}

/* These handlers implement the `prismcast service` subcommands for installing, uninstalling, and checking the status of PrismCast as a system service. Each handler
 * prints its output directly to the console and exits with an appropriate status code.
 */

/**
 * Prints a message to stdout.
 * @param message - The message to print.
 */
function print(message: string): void {

  // eslint-disable-next-line no-console
  console.log(message);
}

/**
 * Prints an error message to stderr.
 * @param message - The error message to print.
 */
function printError(message: string): void {

  // eslint-disable-next-line no-console
  console.error(message);
}

/**
 * Prints a stale path warning block to stderr. Called by handleStatus() when the service file references paths that no longer exist on disk.
 */
function printStalePathWarning(): void {

  const staleResult = detectStalePaths();

  if(!staleResult?.stale) {

    return;
  }

  print("");
  printError("Warning: Service file contains stale paths that no longer exist on disk.");

  if(staleResult.nodePath) {

    printError("  Node binary:  " + staleResult.nodePath + " (not found)");
  }

  if(staleResult.entryPoint) {

    printError("  Entry point:  " + staleResult.entryPoint + " (not found)");
  }

  printError("Run 'prismcast service restart' to update the service file.");
}

/**
 * Formats a duration in seconds to a human-readable string.
 * @param seconds - The duration in seconds.
 * @returns Formatted duration (e.g., "45s", "2m 15s", "1h 23m").
 */
function formatDuration(seconds: number): string {

  if(seconds < 60) {

    return String(seconds) + "s";
  }

  if(seconds < 3600) {

    const minutes = Math.floor(seconds / 60);
    const secs = seconds % 60;

    return secs > 0 ? String(minutes) + "m " + String(secs) + "s" : String(minutes) + "m";
  }

  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);

  return minutes > 0 ? String(hours) + "h " + String(minutes) + "m" : String(hours) + "h";
}

/**
 * Fetches active stream information from the running server.
 * @param port - The server port to connect to.
 * @returns Stream data on success, null if the server is unreachable.
 */
async function fetchActiveStreams(port: number): Promise<Nullable<StreamsResponse>> {

  try {

    const controller = new AbortController();
    const timeoutId = setTimeout(() => { controller.abort(); }, 3000);

    const response = await fetch("http://127.0.0.1:" + String(port) + "/streams", { signal: controller.signal });

    clearTimeout(timeoutId);

    if(!response.ok) {

      return null;
    }

    return await response.json() as StreamsResponse;
  } catch {

    return null;
  }
}

/**
 * Gets the configured server port from user config or defaults.
 * @returns The server port.
 */
async function getServerPort(): Promise<number> {

  const result = await loadUserConfig();

  return result.config.server?.port ?? DEFAULTS.server.port;
}

/**
 * Prints usage information for the service subcommand.
 */
export function printServiceUsage(): void {

  print("Usage: prismcast service <command> [options]");
  print("");
  print("Commands:");
  print("  install [--force]   Install " + SERVICE_NAME + " as a user service");
  print("  uninstall           Remove " + SERVICE_NAME + " service");
  print("  start               Start the " + SERVICE_NAME + " service");
  print("  stop                Stop the " + SERVICE_NAME + " service");
  print("  restart             Restart the " + SERVICE_NAME + " service");
  print("  status              Show service installation and running status");
  print("");
  print("Options:");
  print("  --force             Force reinstall even if already installed");
  print("");
  print("The service runs as the current user and starts automatically at login.");
  print("Configuration is read from " + path.join(getDataDir(), "config.json") + ".");
}

/**
 * Handles the `service install` command. Generates and installs a service file for the current platform, then starts the service.
 * @param force - If true, reinstall even if already installed.
 * @returns Exit code (0 for success, 1 for error).
 */
export async function handleInstall(force: boolean): Promise<number> {

  const generator = getServiceGenerator();

  if(!generator) {

    printError("Error: Service installation is not supported on this platform (" + getPlatform() + ").");

    return 1;
  }

  // Check if already installed.
  const isInstalled = await generator.isInstalled();

  if(isInstalled && !force) {

    printError("Error: " + SERVICE_NAME + " service is already installed.");
    printError("Use --force to reinstall, or run 'prismcast service uninstall' first.");

    return 1;
  }

  print("Installing " + SERVICE_NAME + " service...");
  print("");

  // Collect environment variables to persist in the service file.
  const envVars = collectServiceEnvironment();

  // Generate and install the service.
  try {

    const content = generator.generate({ envVars });

    await generator.install(content);
  } catch(error) {

    printError("Error: Failed to install service.");

    if(error instanceof Error) {

      printError(error.message);
    }

    return 1;
  }

  print("Service installed successfully.");
  print("");
  print("Service file: " + getServiceFilePath());
  print("Service manager: " + generator.serviceManager);
  print("");

  // Print platform-specific instructions.
  switch(generator.serviceManager) {

    case "launchd": {

      print("The service is now running and will start automatically at login.");
      print("");
      print("Useful commands:");
      print("  View logs:        tail -f " + path.join(getDataDir(), "prismcast.log"));
      print("  View service log: tail -f " + path.join(getDataDir(), "service-stdout.log"));
      print("  Stop service:     prismcast service stop");
      print("  Start service:    prismcast service start");
      print("  Status:           prismcast service status");

      break;
    }

    case "systemd": {

      print("The service is now running and will start automatically at login.");
      print("");
      print("Useful commands:");
      print("  View logs:     tail -f " + path.join(getDataDir(), "prismcast.log"));
      print("  View journal:  journalctl --user -u prismcast -f");
      print("  Stop service:  prismcast service stop");
      print("  Start service: prismcast service start");
      print("  Status:        prismcast service status");

      break;
    }

    case "windows-scheduler": {

      print("The service is now running and will start automatically at login.");
      print("");
      print("Useful commands:");
      print("  View logs:     type " + path.join(getDataDir(), "prismcast.log"));
      print("  Stop service:  prismcast service stop");
      print("  Start service: prismcast service start");
      print("  Status:        prismcast service status");

      break;
    }
  }

  return 0;
}

/**
 * Handles the `service uninstall` command. Stops and removes the service.
 * @returns Exit code (0 for success, 1 for error).
 */
export async function handleUninstall(): Promise<number> {

  const generator = getServiceGenerator();

  if(!generator) {

    printError("Error: Service management is not supported on this platform (" + getPlatform() + ").");

    return 1;
  }

  // Check if installed.
  const isInstalled = await generator.isInstalled();

  if(!isInstalled) {

    printError("Error: " + SERVICE_NAME + " service is not installed.");

    return 1;
  }

  print("Uninstalling " + SERVICE_NAME + " service...");

  try {

    await generator.uninstall();
  } catch(error) {

    printError("Error: Failed to uninstall service.");

    if(error instanceof Error) {

      printError(error.message);
    }

    return 1;
  }

  print("Service uninstalled successfully.");
  print("");
  print("Note: Configuration and data files in " + getDataDir() + " have been preserved.");
  print("To remove all data, delete the " + getDataDir() + " directory manually.");

  return 0;
}

/**
 * Core restart logic shared by handleStart() and handleRestart(). Compares paths in the existing service file against the current runtime paths. If the paths have
 * changed (e.g., after a Homebrew or npm upgrade), the service file is regenerated before starting. Otherwise, a normal start is performed. The caller is
 * responsible for validation (platform support, installed check) and for stopping the service if needed before calling this function.
 * @param generator - The validated service generator for the current platform.
 * @param action - The user-facing action verb for success messages ("started" or "restarted").
 * @returns Exit code (0 for success, 1 for error).
 */
async function restartService(generator: ServiceGenerator, action: string): Promise<number> {

  // Compare the paths in the existing service file against the current runtime paths to determine whether regeneration is needed. All supported platforms (launchd,
  // systemd, Windows) store paths in their service files, so getServicePaths() only returns null for corrupt or legacy marker files.
  const existingPaths = getServicePaths();
  const currentNodePath = getNodeExecutablePath();
  const currentEntryPoint = getPrismCastEntryPoint();
  const pathsMatch = (existingPaths?.nodePath === currentNodePath) && (existingPaths.entryPoint === currentEntryPoint);

  // Regenerate the service file if paths have changed or couldn't be extracted.
  if(!pathsMatch) {

    if(existingPaths) {

      print("Detected path changes in service file:");

      if(existingPaths.nodePath !== currentNodePath) {

        print("  Node binary:  " + existingPaths.nodePath + " -> " + currentNodePath);
      }

      if(existingPaths.entryPoint !== currentEntryPoint) {

        print("  Entry point:  " + existingPaths.entryPoint + " -> " + currentEntryPoint);
      }
    }

    print("Regenerating service file...");

    try {

      const envVars = collectServiceEnvironment();
      const content = generator.generate({ envVars });

      await generator.install(content);
    } catch(error) {

      printError("Error: Failed to regenerate service file.");

      if(error instanceof Error) {

        printError(error.message);
      }

      return 1;
    }

    print("Updated service file with current paths.");
  } else {

    // Paths match — normal start cycle without file rewrite.
    print("Starting " + SERVICE_NAME + " service...");

    try {

      await generator.start();
    } catch(error) {

      printError("Error: Failed to start service.");

      if(error instanceof Error) {

        printError(error.message);
      }

      return 1;
    }
  }

  print("Service " + action + " successfully.");

  return 0;
}

/**
 * Handles the `service start` command. Starts the service if it is installed but not running. If the service file contains stale paths (e.g., after a Homebrew
 * upgrade), they are fixed automatically before starting.
 * @returns Exit code (0 for success, 1 for error).
 */
export async function handleStart(): Promise<number> {

  const generator = getServiceGenerator();

  if(!generator) {

    printError("Error: Service management is not supported on this platform (" + getPlatform() + ").");

    return 1;
  }

  // Check if installed.
  const isInstalled = await generator.isInstalled();

  if(!isInstalled) {

    printError("Error: " + SERVICE_NAME + " service is not installed.");
    printError("Run 'prismcast service install' first.");

    return 1;
  }

  // Check if already running.
  const isRunning = await generator.isRunning();

  if(isRunning) {

    print(SERVICE_NAME + " service is already running.");

    return 0;
  }

  return restartService(generator, "started");
}

/**
 * Handles the `service stop` command. Always attempts to stop the service regardless of whether it appears to be running, ensuring that loaded-but-not-running
 * state (common on launchd after a crash) is properly cleared.
 * @returns Exit code (0 for success, 1 for error).
 */
export async function handleStop(): Promise<number> {

  const generator = getServiceGenerator();

  if(!generator) {

    printError("Error: Service management is not supported on this platform (" + getPlatform() + ").");

    return 1;
  }

  // Check if installed.
  const isInstalled = await generator.isInstalled();

  if(!isInstalled) {

    printError("Error: " + SERVICE_NAME + " service is not installed.");

    return 1;
  }

  // Always attempt to stop the service regardless of whether isRunning() reports it as running. On launchd, a service can be in a "loaded but not running" state
  // (e.g., after a crash) where isRunning() returns false but the stale definition remains loaded. Skipping the stop in that case prevents launchctl unload from
  // clearing the loaded state, which causes subsequent start attempts to reuse the cached (potentially stale) definition.
  print("Stopping " + SERVICE_NAME + " service...");

  try {

    await generator.stop();
  } catch(error) {

    // The stop call may fail if the service wasn't actually loaded or running. This is expected — each platform's stop() can throw when there's nothing to stop.
    // We log the error but don't treat it as a failure since the end state (service stopped) is what we wanted.
    if(error instanceof Error) {

      printError("Note: " + error.message);
    }
  }

  print("Service stopped successfully.");

  return 0;
}

/**
 * Handles the `service restart` command. Stops the service, then compares paths and regenerates the service file if needed before starting.
 * @returns Exit code (0 for success, 1 for error).
 */
export async function handleRestart(): Promise<number> {

  const generator = getServiceGenerator();

  if(!generator) {

    printError("Error: Service management is not supported on this platform (" + getPlatform() + ").");

    return 1;
  }

  // Check if installed.
  const isInstalled = await generator.isInstalled();

  if(!isInstalled) {

    printError("Error: " + SERVICE_NAME + " service is not installed.");
    printError("Run 'prismcast service install' first.");

    return 1;
  }

  // Stop the service. We call generator.stop() directly rather than handleStop() to avoid the duplicate "not installed" check and to ensure the stop is always
  // attempted regardless of isRunning() state.
  print("Stopping " + SERVICE_NAME + " service...");

  try {

    await generator.stop();
  } catch {

    // The service may not be running or loaded. This is fine — we just need it stopped before restart.
  }

  print("");

  return restartService(generator, "restarted");
}

/**
 * Handles the `service status` command. Shows the current service installation and running status.
 * @returns Exit code (0 for success).
 */
export async function handleStatus(): Promise<number> {

  const generator = getServiceGenerator();

  if(!generator) {

    print(SERVICE_NAME + " Service Status");
    print("─".repeat(40));
    print("Platform:        " + getPlatform());
    print("Service support: Not available");
    print("");
    print("Service installation is not supported on this platform.");

    return 0;
  }

  const isInstalled = await generator.isInstalled();
  const isRunning = isInstalled ? await generator.isRunning() : false;

  print(SERVICE_NAME + " Service Status");
  print("─".repeat(40));
  print("Platform:        " + getPlatform());
  print("Service manager: " + generator.serviceManager);
  print("Service file:    " + getServiceFilePath());
  print("Installed:       " + (isInstalled ? "Yes" : "No"));
  print("Running:         " + (isRunning ? "Yes" : "No"));

  // Warn about stale paths if the service is installed. This is the most common scenario users encounter after upgrading — the status shows "not running" with no
  // explanation. The warning gives them a clear next step.
  if(isInstalled) {

    printStalePathWarning();
  }

  // If the service is running, fetch and display active streams.
  if(isRunning) {

    const port = await getServerPort();
    const streamsData = await fetchActiveStreams(port);

    if(streamsData === null) {

      print("Active streams:  (server not responding)");
    } else if(streamsData.count === 0) {

      print("Active streams:  0/" + String(streamsData.limit));
    } else {

      print("Active streams:  " + String(streamsData.count) + "/" + String(streamsData.limit));

      for(const stream of streamsData.streams) {

        // Use channel name if available, otherwise extract hostname from URL.
        let name = stream.channel;

        if(!name) {

          try {

            name = new URL(stream.url).hostname.replace(/^www\./, "");
          } catch {

            name = "Stream " + String(stream.id);
          }
        }

        print("  • " + name + " (" + formatDuration(stream.duration) + ")");
      }
    }
  }

  if(!isInstalled) {

    print("");
    print("Run 'prismcast service install' to install the service.");
  }

  return 0;
}

/**
 * Main handler for the `service` subcommand. Parses arguments and delegates to the appropriate handler.
 * @param args - Arguments after 'service' (e.g., ['install', '--force']).
 * @returns Exit code (0 for success, 1 for error).
 */
export async function handleServiceCommand(args: string[]): Promise<number> {

  const command = args[0] as string | undefined;

  // Handle empty args (show help).
  if(command === undefined) {

    printServiceUsage();

    return 0;
  }

  switch(command) {

    case "install": {

      const force = args.includes("--force") || args.includes("-f");

      return handleInstall(force);
    }

    case "uninstall": {

      return handleUninstall();
    }

    case "start": {

      return handleStart();
    }

    case "stop": {

      return handleStop();
    }

    case "restart": {

      return handleRestart();
    }

    case "status": {

      return handleStatus();
    }

    case "help":
    case "--help":
    case "-h": {

      printServiceUsage();

      return 0;
    }

    default: {

      printError("Error: Unknown service command '" + command + "'.");
      printError("");
      printServiceUsage();

      return 1;
    }
  }
}
