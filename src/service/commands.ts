/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * commands.ts: Service command handlers for PrismCast CLI.
 *
 * Each handler is a pure orchestrator over a ServiceContext. The context bundles the platform-specific generator factory, runtime path and platform queries, the
 * server-port and active-streams probes, the stale-path detector, the service-definition builder, and stdout/stderr writers; production wires all of them through
 * createDefaultServiceContext (in commands.context.ts), tests pass a context literal. The decision logic - install gating, restart vs. start, status display,
 * dispatcher routing - is fully testable without touching real launchctl/systemctl/powershell.exe state, real config files, or real HTTP.
 */
import type { ServiceDefinition, ServiceGenerator, ServicePaths, StalePathResult } from "./generators.ts";
import type { Nullable } from "../types/index.ts";
import type { Platform } from "../utils/platform.ts";
import { SERVICE_NAME } from "../identity.ts";
import { createDefaultServiceContext } from "./commands.context.ts";
import { formatDuration } from "../utils/format.ts";
import path from "node:path";

// Types.

/**
 * Response from the /streams endpoint.
 */
export interface StreamsResponse {

  count: number;
  limit: number;
  streams: {
    channel: Nullable<string>;
    duration: number;
    id: number;
    showName: string;
    url: string;
  }[];
}

/**
 * The runtime context the service handlers consume. Each field models one capability the handlers need - generator selection, path/platform queries, server probe,
 * stale-path detection, service-definition build, and stdout/stderr output. Decision logic is a pure function of this shape; production wires it through
 * createDefaultServiceContext (in commands.context.ts), tests pass a context literal.
 */
export interface ServiceContext {

  // Builds a fresh ServiceDefinition reflecting current runtime paths and environment.
  readonly buildServiceDefinition: () => ServiceDefinition;

  // Detects whether the on-disk service file references paths that no longer exist (e.g. after a Homebrew or npm upgrade).
  readonly detectStalePaths: () => Nullable<StalePathResult>;

  // Probes the running server for active streams. Returns null when the server is unreachable.
  readonly fetchActiveStreams: (port: number) => Promise<Nullable<StreamsResponse>>;

  // The data directory where logs and config live.
  readonly getDataDir: () => string;

  // Absolute path to PrismCast's entry point (dist/index.js).
  readonly getEntryPoint: () => string;

  // The platform-specific service generator, or null if unsupported. Tests pass a literal generator to bypass real I/O.
  readonly getGenerator: () => Nullable<ServiceGenerator>;

  // Absolute path to the Node.js executable.
  readonly getNodePath: () => string;

  // The detected platform name (used for unsupported-platform error messages).
  readonly getPlatform: () => Platform;

  // Reads the configured server port from user config or defaults.
  readonly getServerPort: () => Promise<number>;

  // Absolute path to the platform-specific service file (plist, unit, etc.).
  readonly getServiceFilePath: () => string;

  // Reads the paths recorded in the existing service file. Returns null when the file is missing or unparseable.
  readonly getServicePaths: () => Nullable<ServicePaths>;

  // stderr writer.
  readonly stderr: (line: string) => void;

  // stdout writer.
  readonly stdout: (line: string) => void;
}

/* These handlers implement the `prismcast service` subcommands for installing, uninstalling, and checking the status of PrismCast as a system service. Each handler
 * prints its output through the context's stdout/stderr writers and returns an exit code; the dispatcher returns to the CLI bootstrap which calls process.exit.
 */

/**
 * Prints a stale path warning block to stderr. Called by handleStatus() when the service file references paths that no longer exist on disk.
 * @param ctx - The service context.
 */
function printStalePathWarning(ctx: ServiceContext): void {

  const staleResult = ctx.detectStalePaths();

  if(!staleResult?.stale) {

    return;
  }

  ctx.stdout("");
  ctx.stderr("Warning: Service file contains stale paths that no longer exist on disk.");

  if(staleResult.nodePath) {

    ctx.stderr("  Node binary:  " + staleResult.nodePath + " (not found)");
  }

  if(staleResult.entryPoint) {

    ctx.stderr("  Entry point:  " + staleResult.entryPoint + " (not found)");
  }

  ctx.stderr("Run 'prismcast service restart' to update the service file.");
}

/**
 * Prints usage information for the service subcommand.
 * @param ctx - The service context. Defaults to createDefaultServiceContext().
 */
export function printServiceUsage(ctx: ServiceContext = createDefaultServiceContext()): void {

  ctx.stdout("Usage: prismcast service <command> [options]");
  ctx.stdout("");
  ctx.stdout("Commands:");
  ctx.stdout("  install [--force]   Install " + SERVICE_NAME + " as a user service");
  ctx.stdout("  uninstall           Remove " + SERVICE_NAME + " service");
  ctx.stdout("  start               Start the " + SERVICE_NAME + " service");
  ctx.stdout("  stop                Stop the " + SERVICE_NAME + " service");
  ctx.stdout("  restart             Restart the " + SERVICE_NAME + " service");
  ctx.stdout("  status              Show service installation and running status");
  ctx.stdout("");
  ctx.stdout("Options:");
  ctx.stdout("  --force             Force reinstall even if already installed");
  ctx.stdout("");
  ctx.stdout("The service runs as the current user and starts automatically at login.");
  ctx.stdout("Configuration is read from " + path.join(ctx.getDataDir(), "config.json") + ".");
}

/**
 * Handles the `service install` command. Generates and installs a service file for the current platform, then starts the service.
 * @param force - If true, reinstall even if already installed.
 * @param ctx - The service context. Defaults to createDefaultServiceContext().
 * @returns Exit code (0 for success, 1 for error).
 */
export async function handleInstall(force: boolean, ctx: ServiceContext = createDefaultServiceContext()): Promise<number> {

  const generator = ctx.getGenerator();

  if(!generator) {

    ctx.stderr("Error: Service installation is not supported on this platform (" + ctx.getPlatform() + ").");

    return 1;
  }

  // Check if already installed.
  const isInstalled = await generator.isInstalled();

  if(isInstalled && !force) {

    ctx.stderr("Error: " + SERVICE_NAME + " service is already installed.");
    ctx.stderr("Use --force to reinstall, or run 'prismcast service uninstall' first.");

    return 1;
  }

  ctx.stdout("Installing " + SERVICE_NAME + " service...");
  ctx.stdout("");

  // Install the service. Each generator takes a structured ServiceDefinition and decides how to realize it on its platform (launchd plist, systemd unit, or
  // Windows Task Scheduler task).
  try {

    await generator.install(ctx.buildServiceDefinition());
  } catch(error) {

    ctx.stderr("Error: Failed to install service.");

    if(error instanceof Error) {

      ctx.stderr(error.message);
    }

    return 1;
  }

  ctx.stdout("Service installed successfully.");
  ctx.stdout("");
  ctx.stdout("Service file: " + ctx.getServiceFilePath());
  ctx.stdout("Service manager: " + generator.serviceManager);
  ctx.stdout("");

  // Print platform-specific instructions.
  switch(generator.serviceManager) {

    case "launchd": {

      ctx.stdout("The service is now running and will start automatically at login.");
      ctx.stdout("");
      ctx.stdout("Useful commands:");
      ctx.stdout("  View logs:        tail -f " + path.join(ctx.getDataDir(), "prismcast.log"));
      ctx.stdout("  View service log: tail -f " + path.join(ctx.getDataDir(), "service-stdout.log"));
      ctx.stdout("  Stop service:     prismcast service stop");
      ctx.stdout("  Start service:    prismcast service start");
      ctx.stdout("  Status:           prismcast service status");

      break;
    }

    case "systemd": {

      ctx.stdout("The service is now running and will start automatically at login.");
      ctx.stdout("");
      ctx.stdout("Useful commands:");
      ctx.stdout("  View logs:     tail -f " + path.join(ctx.getDataDir(), "prismcast.log"));
      ctx.stdout("  View journal:  journalctl --user -u prismcast -f");
      ctx.stdout("  Stop service:  prismcast service stop");
      ctx.stdout("  Start service: prismcast service start");
      ctx.stdout("  Status:        prismcast service status");

      break;
    }

    case "windows-scheduler": {

      ctx.stdout("The service is now running and will start automatically at login.");
      ctx.stdout("");
      ctx.stdout("Useful commands:");
      ctx.stdout("  View logs:     type " + path.join(ctx.getDataDir(), "prismcast.log"));
      ctx.stdout("  Stop service:  prismcast service stop");
      ctx.stdout("  Start service: prismcast service start");
      ctx.stdout("  Status:        prismcast service status");

      break;
    }
  }

  return 0;
}

/**
 * Handles the `service uninstall` command. Stops and removes the service.
 * @param ctx - The service context. Defaults to createDefaultServiceContext().
 * @returns Exit code (0 for success, 1 for error).
 */
export async function handleUninstall(ctx: ServiceContext = createDefaultServiceContext()): Promise<number> {

  const generator = ctx.getGenerator();

  if(!generator) {

    ctx.stderr("Error: Service management is not supported on this platform (" + ctx.getPlatform() + ").");

    return 1;
  }

  // Check if installed.
  const isInstalled = await generator.isInstalled();

  if(!isInstalled) {

    ctx.stderr("Error: " + SERVICE_NAME + " service is not installed.");

    return 1;
  }

  ctx.stdout("Uninstalling " + SERVICE_NAME + " service...");

  try {

    await generator.uninstall();
  } catch(error) {

    ctx.stderr("Error: Failed to uninstall service.");

    if(error instanceof Error) {

      ctx.stderr(error.message);
    }

    return 1;
  }

  ctx.stdout("Service uninstalled successfully.");
  ctx.stdout("");
  ctx.stdout("Note: Configuration and data files in " + ctx.getDataDir() + " have been preserved.");
  ctx.stdout("To remove all data, delete the " + ctx.getDataDir() + " directory manually.");

  return 0;
}

/**
 * Core restart logic shared by handleStart() and handleRestart(). Compares paths in the existing service file against the current runtime paths. If the paths have
 * changed (e.g., after a Homebrew or npm upgrade), the service file is regenerated before starting. Otherwise, a normal start is performed. The caller is
 * responsible for validation (platform support, installed check) and for stopping the service if needed before calling this function.
 * @param ctx - The service context.
 * @param generator - The validated service generator for the current platform.
 * @param action - The user-facing action verb for success messages ("started" or "restarted").
 * @returns Exit code (0 for success, 1 for error).
 */
async function restartService(ctx: ServiceContext, generator: ServiceGenerator, action: string): Promise<number> {

  // Compare the paths in the existing service file against the current runtime paths to determine whether regeneration is needed. The caller has already confirmed
  // the service is installed and all supported platforms (launchd plist, systemd unit, Windows PowerShell launcher) store paths in their service files, so in this
  // gated context getServicePaths() returns null only for a corrupt or unparseable service file.
  const existingPaths = ctx.getServicePaths();
  const currentNodePath = ctx.getNodePath();
  const currentEntryPoint = ctx.getEntryPoint();
  const pathsMatch = (existingPaths?.nodePath === currentNodePath) && (existingPaths.entryPoint === currentEntryPoint);

  // Regenerate the service file if paths have changed or couldn't be extracted.
  if(!pathsMatch) {

    if(existingPaths) {

      ctx.stdout("Detected path changes in service file:");

      if(existingPaths.nodePath !== currentNodePath) {

        ctx.stdout("  Node binary:  " + existingPaths.nodePath + " -> " + currentNodePath);
      }

      if(existingPaths.entryPoint !== currentEntryPoint) {

        ctx.stdout("  Entry point:  " + existingPaths.entryPoint + " -> " + currentEntryPoint);
      }
    }

    ctx.stdout("Regenerating service file...");

    try {

      // install() re-registers and starts the service on every platform (launchd load -w, systemctl enable+start, Windows Register+Start-ScheduledTask), so the
      // regeneration path needs no separate start() call; the success message below reflects a service that install() has already brought up.
      await generator.install(ctx.buildServiceDefinition());
    } catch(error) {

      ctx.stderr("Error: Failed to regenerate service file.");

      if(error instanceof Error) {

        ctx.stderr(error.message);
      }

      return 1;
    }

    ctx.stdout("Updated service file with current paths.");
  } else {

    // Paths match - normal start cycle without file rewrite.
    ctx.stdout("Starting " + SERVICE_NAME + " service...");

    try {

      await generator.start();
    } catch(error) {

      ctx.stderr("Error: Failed to start service.");

      if(error instanceof Error) {

        ctx.stderr(error.message);
      }

      return 1;
    }
  }

  ctx.stdout("Service " + action + " successfully.");

  return 0;
}

/**
 * Handles the `service start` command. Starts the service if it is installed but not running. If the service file contains stale paths (e.g., after a Homebrew
 * upgrade), they are fixed automatically before starting.
 * @param ctx - The service context. Defaults to createDefaultServiceContext().
 * @returns Exit code (0 for success, 1 for error).
 */
export async function handleStart(ctx: ServiceContext = createDefaultServiceContext()): Promise<number> {

  const generator = ctx.getGenerator();

  if(!generator) {

    ctx.stderr("Error: Service management is not supported on this platform (" + ctx.getPlatform() + ").");

    return 1;
  }

  // Check if installed.
  const isInstalled = await generator.isInstalled();

  if(!isInstalled) {

    ctx.stderr("Error: " + SERVICE_NAME + " service is not installed.");
    ctx.stderr("Run 'prismcast service install' first.");

    return 1;
  }

  // Check if already running.
  const isRunning = await generator.isRunning();

  if(isRunning) {

    ctx.stdout(SERVICE_NAME + " service is already running.");

    return 0;
  }

  return restartService(ctx, generator, "started");
}

/**
 * Handles the `service stop` command. Always attempts to stop the service regardless of whether it appears to be running, ensuring that loaded-but-not-running
 * state (common on launchd after a crash) is properly cleared.
 * @param ctx - The service context. Defaults to createDefaultServiceContext().
 * @returns Exit code (0 for success, 1 for error).
 */
export async function handleStop(ctx: ServiceContext = createDefaultServiceContext()): Promise<number> {

  const generator = ctx.getGenerator();

  if(!generator) {

    ctx.stderr("Error: Service management is not supported on this platform (" + ctx.getPlatform() + ").");

    return 1;
  }

  // Check if installed.
  const isInstalled = await generator.isInstalled();

  if(!isInstalled) {

    ctx.stderr("Error: " + SERVICE_NAME + " service is not installed.");

    return 1;
  }

  // Always attempt to stop the service regardless of whether isRunning() reports it as running. On launchd, a service can be in a "loaded but not running" state
  // (e.g., after a crash) where isRunning() returns false but the stale definition remains loaded. Skipping the stop in that case prevents launchctl unload from
  // clearing the loaded state, which causes subsequent start attempts to reuse the cached (potentially stale) definition.
  ctx.stdout("Stopping " + SERVICE_NAME + " service...");

  try {

    await generator.stop();
  } catch(error) {

    // The stop call may fail if the service wasn't actually loaded or running. This is expected - each platform's stop() can throw when there's nothing to stop.
    // We log the error but don't treat it as a failure since the end state (service stopped) is what we wanted.
    if(error instanceof Error) {

      ctx.stderr("Note: " + error.message);
    }
  }

  ctx.stdout("Service stopped successfully.");

  return 0;
}

/**
 * Handles the `service restart` command. Stops the service, then compares paths and regenerates the service file if needed before starting.
 * @param ctx - The service context. Defaults to createDefaultServiceContext().
 * @returns Exit code (0 for success, 1 for error).
 */
export async function handleRestart(ctx: ServiceContext = createDefaultServiceContext()): Promise<number> {

  const generator = ctx.getGenerator();

  if(!generator) {

    ctx.stderr("Error: Service management is not supported on this platform (" + ctx.getPlatform() + ").");

    return 1;
  }

  // Check if installed.
  const isInstalled = await generator.isInstalled();

  if(!isInstalled) {

    ctx.stderr("Error: " + SERVICE_NAME + " service is not installed.");
    ctx.stderr("Run 'prismcast service install' first.");

    return 1;
  }

  // Stop the service. We call generator.stop() directly rather than handleStop() to avoid the duplicate "not installed" check and to ensure the stop is always
  // attempted regardless of isRunning() state.
  ctx.stdout("Stopping " + SERVICE_NAME + " service...");

  try {

    await generator.stop();
  } catch {

    // The service may not be running or loaded. This is fine - we just need it stopped before restart.
  }

  ctx.stdout("");

  return restartService(ctx, generator, "restarted");
}

/**
 * Handles the `service status` command. Shows the current service installation and running status.
 * @param ctx - The service context. Defaults to createDefaultServiceContext().
 * @returns Exit code (0 for success).
 */
export async function handleStatus(ctx: ServiceContext = createDefaultServiceContext()): Promise<number> {

  const generator = ctx.getGenerator();

  if(!generator) {

    ctx.stdout(SERVICE_NAME + " Service Status");
    ctx.stdout("─".repeat(40));
    ctx.stdout("Platform:        " + ctx.getPlatform());
    ctx.stdout("Service support: Not available");
    ctx.stdout("");
    ctx.stdout("Service installation is not supported on this platform.");

    return 0;
  }

  const isInstalled = await generator.isInstalled();
  const isRunning = isInstalled ? await generator.isRunning() : false;

  ctx.stdout(SERVICE_NAME + " Service Status");
  ctx.stdout("─".repeat(40));
  ctx.stdout("Platform:        " + ctx.getPlatform());
  ctx.stdout("Service manager: " + generator.serviceManager);
  ctx.stdout("Service file:    " + ctx.getServiceFilePath());
  ctx.stdout("Installed:       " + (isInstalled ? "Yes" : "No"));
  ctx.stdout("Running:         " + (isRunning ? "Yes" : "No"));

  // Warn about stale paths if the service is installed. This is the most common scenario users encounter after upgrading - the status shows "not running" with no
  // explanation. The warning gives them a clear next step.
  if(isInstalled) {

    printStalePathWarning(ctx);
  }

  // If the service is running, fetch and display active streams.
  if(isRunning) {

    const port = await ctx.getServerPort();
    const streamsData = await ctx.fetchActiveStreams(port);

    if(streamsData === null) {

      ctx.stdout("Active streams:  (server not responding)");
    } else if(streamsData.count === 0) {

      ctx.stdout("Active streams:  0/" + String(streamsData.limit));
    } else {

      ctx.stdout("Active streams:  " + String(streamsData.count) + "/" + String(streamsData.limit));

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

        const suffix = stream.showName ? " - " + stream.showName : "";

        ctx.stdout("  • " + name + " (" + formatDuration(stream.duration, "s") + ")" + suffix);
      }
    }
  }

  if(!isInstalled) {

    ctx.stdout("");
    ctx.stdout("Run 'prismcast service install' to install the service.");
  }

  return 0;
}

/**
 * Main handler for the `service` subcommand. Parses arguments and delegates to the appropriate handler.
 * @param args - Arguments after 'service' (e.g., ['install', '--force']).
 * @param ctx - The service context. Defaults to createDefaultServiceContext().
 * @returns Exit code (0 for success, 1 for error).
 */
export async function handleServiceCommand(args: string[], ctx: ServiceContext = createDefaultServiceContext()): Promise<number> {

  const command = args[0];

  // Handle empty args (show help).
  if(command === undefined) {

    printServiceUsage(ctx);

    return 0;
  }

  switch(command) {

    case "install": {

      const force = args.includes("--force") || args.includes("-f");

      return handleInstall(force, ctx);
    }

    case "uninstall": {

      return handleUninstall(ctx);
    }

    case "start": {

      return handleStart(ctx);
    }

    case "stop": {

      return handleStop(ctx);
    }

    case "restart": {

      return handleRestart(ctx);
    }

    case "status": {

      return handleStatus(ctx);
    }

    case "help":
    case "--help":
    case "-h": {

      printServiceUsage(ctx);

      return 0;
    }

    default: {

      ctx.stderr("Error: Unknown service command '" + command + "'.");
      ctx.stderr("");
      printServiceUsage(ctx);

      return 1;
    }
  }
}
