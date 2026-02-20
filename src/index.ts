/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Entry point for PrismCast.
 */
import { LOG, formatError, getPackageVersion, initDebugFilter, setDebugLogging } from "./utils/index.js";
import { flushLogBufferSync } from "./utils/fileLogger.js";
import { handleServiceCommand } from "./service/index.js";
import { initializeDataDir } from "./config/paths.js";
import { killStaleChrome } from "./browser/index.js";
import path from "node:path";
import { startServer } from "./app.js";

/* These handlers catch unhandled promise rejections and uncaught exceptions to prevent the process from crashing. For a livestreaming server, process stability is
 * critical - a single unhandled error should not terminate all active streams. The handlers log the error and allow the process to continue. Individual stream
 * failures are handled by circuit breakers, and actual browser crashes are handled by the browser's disconnected event.
 */

process.on("unhandledRejection", (reason: unknown): void => {

  LOG.error("Unhandled promise rejection: %s.", formatError(reason));
});

process.on("uncaughtException", (error: Error): void => {

  LOG.error("Uncaught exception: %s.", formatError(error));
});

/* The entry point supports basic command-line arguments for common operations like changing the port, showing help, and displaying the version.
 */

/**
 * Prints usage information to the console.
 */
function printUsage(): void {

  /* eslint-disable no-console */
  console.log("Usage: prismcast [command] [options]");
  console.log("");
  console.log("Commands:");
  console.log("  service                         Manage PrismCast as a system service");
  console.log("                                  Run 'prismcast service --help' for details");
  console.log("  upgrade                         Upgrade PrismCast to the latest version");
  console.log("                                  Run 'prismcast upgrade --help' for details");
  console.log("");
  console.log("Options:");
  console.log("  -c, --console                   Log to console instead of file (for Docker or debugging)");
  console.log("  -d, --debug                     Enable debug logging (verbose output for troubleshooting)");
  console.log("  -h, --help                      Show this help message");
  console.log("  -p, --port <port>               Set server port (default: 5589)");
  console.log("  -v, --version                   Show version number");
  console.log("  --chrome-data-dir <path>        Set Chrome profile data directory (default: <data-dir>/chromedata)");
  console.log("  --data-dir <path>               Set data directory (default: ~/.prismcast)");
  console.log("  --list-env                      List all environment variables");
  console.log("  --log-file <path>               Set log file path (default: <data-dir>/prismcast.log)");
  console.log("");
  console.log("If no command is specified, starts the PrismCast server.");
  console.log("");
  console.log("Common Environment Variables:");
  console.log("  AUDIO_BITRATE                   Audio bitrate (bps)");
  console.log("  CAPTURE_MODE                    Capture mode: ffmpeg (recommended) or native");
  console.log("  CHROME_BIN                      Path to Chrome executable");
  console.log("  FRAME_RATE                      Target frame rate");
  console.log("  HOST                            HTTP server bind address");
  console.log("  LOG_MAX_SIZE                    Maximum log file size in bytes (default: 1048576)");
  console.log("  PORT                            HTTP server port");
  console.log("  PRISMCAST_CHROME_DATA_DIR       Chrome profile data directory path");
  console.log("  PRISMCAST_DATA_DIR              Data directory path (default: ~/.prismcast)");
  console.log("  PRISMCAST_DEBUG                 Debug category filter (e.g., 'tuning:hulu', 'recovery', '*,-streaming:segmenter')");
  console.log("  PRISMCAST_LOG_FILE              Log file path");
  console.log("  QUALITY_PRESET                  Video quality preset (480p, 720p, 720p-high, 1080p, 1080p-high, 4k)");
  console.log("  VIDEO_BITRATE                   Video bitrate (bps)");
  console.log("");
  console.log("  Run 'prismcast --list-env' for a complete list of all environment variables.");
  /* eslint-enable no-console */
}

/**
 * Prints a complete listing of all environment variables organized by category. Generates output dynamically from CONFIG_METADATA so it is always accurate. Uses a
 * dynamic import to avoid the circular dependency: index.ts -> config/userConfig.js -> utils/index.js -> utils/retry.js -> config/index.js -> config/userConfig.js.
 */
async function printEnvironmentVariables(): Promise<void> {

  const { CONFIG_METADATA, DEFAULTS, getNestedValue } = await import("./config/userConfig.js");

  /* eslint-disable no-console */

  // Category ordering: server first (most commonly configured), then alphabetical, with Special last.
  const categoryOrder: { displayName: string; key: string }[] = [
    { displayName: "Server", key: "server" },
    { displayName: "Browser", key: "browser" },
    { displayName: "HDHomeRun", key: "hdhr" },
    { displayName: "HLS", key: "hls" },
    { displayName: "Logging", key: "logging" },
    { displayName: "Paths", key: "paths" },
    { displayName: "Playback", key: "playback" },
    { displayName: "Recovery", key: "recovery" },
    { displayName: "Streaming", key: "streaming" }
  ];

  // Dynamic default descriptions for null path settings that resolve at runtime rather than from DEFAULTS.
  const dynamicDefaults: Record<string, string> = {

    "browser.executablePath": "autodetect",
    "paths.chromeDataDir": "<data-dir>/chromedata",
    "paths.logFile": "<data-dir>/prismcast.log"
  };

  console.log("PrismCast Environment Variables");
  console.log("");
  console.log("All settings can also be configured via the web UI at /config or config.json.");
  console.log("Priority: CLI flags > environment variables > config.json > defaults.");

  for(const category of categoryOrder) {

    const settings = CONFIG_METADATA[category.key];

    // Filter to settings that have an environment variable.
    const envSettings = settings.filter((s) => s.envVar !== null);

    if(envSettings.length === 0) {

      continue;
    }

    console.log("");
    console.log(category.displayName + ":");

    let first = true;

    for(const setting of envSettings) {

      // Type narrowing: envSettings is already filtered to non-null envVar values, but TypeScript can't narrow through .filter() callbacks.
      const envVar = setting.envVar;

      if(!envVar) {

        continue;
      }

      if(!first) {

        console.log("");
      }

      first = false;

      console.log("  " + envVar);

      // Truncate description to first sentence for brevity. Full descriptions are available in the web UI.
      const desc = setting.description;
      const periodSpace = desc.indexOf(". ");
      const firstSentence = (periodSpace !== -1) ? desc.slice(0, periodSpace + 1) : desc;

      console.log("    " + firstSentence);

      // Format default value with appropriate context for the setting type.
      const dynamicDefault = dynamicDefaults[setting.path];
      let defaultStr: string;

      if(dynamicDefault) {

        defaultStr = dynamicDefault;
      } else {

        const defaultValue = getNestedValue(DEFAULTS, setting.path);

        defaultStr = String(defaultValue);

        if((typeof defaultValue === "number") && setting.unit) {

          defaultStr = defaultStr + " (" + setting.unit + ")";
        }
      }

      console.log("    Default: " + defaultStr);
    }
  }

  // Special environment variables that are not part of CONFIG_METADATA. PRISMCAST_DATA_DIR is resolved before config.json is loaded (chicken-and-egg), so it cannot
  // be in config.json. PRISMCAST_DEBUG is a runtime-only setting parsed in the entry point.
  console.log("");
  console.log("Special:");
  console.log("  PRISMCAST_DATA_DIR");
  console.log("    Data directory path. Must be an absolute path.");
  console.log("    Default: ~/.prismcast");
  console.log("");
  console.log("  PRISMCAST_DEBUG");
  console.log("    Debug category filter (e.g., 'tuning:hulu', 'recovery', '*,-streaming:segmenter').");
  console.log("    Default: (disabled)");

  /* eslint-enable no-console */
}

/**
 * Result of parsing command-line arguments. CLI flags have the highest priority in the configuration merge order.
 */
export interface ParsedArgs {

  chromeDataDir?: string;
  consoleLogging: boolean;
  dataDir?: string;
  debugLogging: boolean;
  logFile?: string;
  port?: number;
}

/**
 * Validates that a path argument is absolute. Prints an error and exits if relative.
 * @param flag - The CLI flag name for the error message.
 * @param value - The path value to validate.
 */
function requireAbsolutePath(flag: string, value: string): void {

  // Guard against missing values when the flag is the last argument. TypeScript types array index access as `string`, but at runtime `args[++i]` returns `undefined`
  // when out of bounds. Checking for falsy catches both `undefined` and empty string.
  if(!value) {

    // eslint-disable-next-line no-console
    console.error("Error: " + flag + " requires a path argument.");

    process.exit(1);
  }

  if(!path.isAbsolute(value)) {

    // eslint-disable-next-line no-console
    console.error("Error: " + flag + " requires an absolute path, got: " + value);

    process.exit(1);
  }
}

/**
 * Parses command-line arguments into a structured result. Values are stored in ParsedArgs rather than written directly to CONFIG, so that the configuration merge
 * system can apply CLI overrides at the correct priority level (CLI > env > config.json > defaults).
 * @returns Parsed argument flags and values.
 */
function parseArgs(): ParsedArgs {

  const args = process.argv.slice(2);
  let chromeDataDir: string | undefined;
  let consoleLogging = false;
  let dataDir: string | undefined;
  let debugLogging = false;
  let logFile: string | undefined;
  let port: number | undefined;

  for(let i = 0; i < args.length; i++) {

    const arg = args[i];

    if((arg === "-c") || (arg === "--console")) {

      consoleLogging = true;
    }

    if((arg === "-d") || (arg === "--debug")) {

      debugLogging = true;
    }

    if((arg === "-h") || (arg === "--help")) {

      printUsage();

      process.exit(0);
    }

    if((arg === "-p") || (arg === "--port")) {

      const parsed = parseInt(args[++i]);

      if(!isNaN(parsed)) {

        port = parsed;
      }
    }

    if(arg === "--data-dir") {

      dataDir = args[++i];

      requireAbsolutePath("--data-dir", dataDir);
    }

    if(arg === "--chrome-data-dir") {

      chromeDataDir = args[++i];

      requireAbsolutePath("--chrome-data-dir", chromeDataDir);
    }

    if(arg === "--log-file") {

      logFile = args[++i];

      requireAbsolutePath("--log-file", logFile);
    }

    if((arg === "-v") || (arg === "--version")) {

      // eslint-disable-next-line no-console
      console.log("PrismCast v" + getPackageVersion());

      process.exit(0);
    }
  }

  return { chromeDataDir, consoleLogging, dataDir, debugLogging, logFile, port };
}

/* Check for subcommands before parsing flags. Subcommands are handled separately and exit after completion.
 */

const rawArgs = process.argv.slice(2);
const subcommand = rawArgs[0];

// Initialize the data directory early so that all code paths (server and service subcommands) can resolve paths. For service subcommands, only the env var and
// default are available since --data-dir is a server flag. For the server path, parseArgs() may override this with a CLI flag.
initializeDataDir();

// Handle the 'service' subcommand for managing PrismCast as a system service.
if(subcommand === "service") {

  handleServiceCommand(rawArgs.slice(1)).then((exitCode) => {

    process.exit(exitCode);
  }).catch((error: unknown) => {

    // eslint-disable-next-line no-console
    console.error("Service command failed: " + formatError(error));

    process.exit(1);
  });
} else if(subcommand === "upgrade") {

  // Handle the 'upgrade' subcommand for self-upgrading PrismCast. Uses a dynamic import to keep the upgrade module out of the main server's import graph.
  import("./upgrade/index.js").then(async ({ handleUpgradeCommand }) => handleUpgradeCommand(rawArgs.slice(1))).then((exitCode) => {

    process.exit(exitCode);
  }).catch((error: unknown) => {

    // eslint-disable-next-line no-console
    console.error("Upgrade command failed: " + formatError(error));

    process.exit(1);
  });
} else if(rawArgs.includes("--list-env")) {

  // Handle --list-env at the top level (like the service subcommand) to avoid starting the server. The dynamic import inside printEnvironmentVariables() is
  // required because a static import of config/userConfig.js creates a circular dependency through the utils barrel.
  printEnvironmentVariables().then(() => {

    process.exit(0);
  }).catch((error: unknown) => {

    // eslint-disable-next-line no-console
    console.error("Error: " + formatError(error));

    process.exit(1);
  });
} else {

  /* The main entry point parses command-line arguments, starts the server, and handles any fatal errors that occur during initialization. If startup fails, we exit
   * with a non-zero code to signal the failure to process managers.
   */

  const parsedArgs = parseArgs();

  // Re-initialize the data directory with the CLI flag if provided. The initial initializeDataDir() call above used only the env var / default, but the CLI
  // --data-dir flag takes precedence over both.
  if(parsedArgs.dataDir) {

    initializeDataDir(parsedArgs.dataDir);
  }

  // Enable debug logging before starting the server so debug messages during startup are captured. The PRISMCAST_DEBUG environment variable takes precedence over
  // the --debug CLI flag, allowing fine-grained category selection.
  const debugEnv = process.env.PRISMCAST_DEBUG;

  if(debugEnv) {

    initDebugFilter(debugEnv);
  } else if(parsedArgs.debugLogging) {

    setDebugLogging(true);
  }

  /* Safety net for server exit paths. When the process exits — whether via process.exit(1) from a fatal startup error, an unrecoverable exception, or any other
   * termination — we ensure Chrome processes are cleaned up and buffered log entries are flushed to disk. Without this, fatal exits during startup (e.g., capture
   * probe timeout) silently orphan Chrome processes and lose diagnostic messages that are still in the file logger's write buffer.
   *
   * The 'exit' event runs synchronously, so only synchronous operations are safe here. killStaleChrome() uses execSync internally, and flushLogBufferSync() writes
   * directly to the filesystem. The graceful shutdown path (SIGTERM/SIGINT) handles cleanup via async closeBrowser() and shutdownFileLogger() — this handler is a
   * fallback for paths that bypass graceful shutdown.
   *
   * This is registered only in the server branch — not for service subcommands like `prismcast service status`. Running killStaleChrome() from a service
   * subcommand would kill Chrome belonging to the running PrismCast server instance.
   */
  process.on("exit", (): void => {

    // Flush logs first — this is the critical operation. The error messages from a failed startup are sitting in the write buffer and must reach disk before the
    // process terminates. Chrome will die when its pipes break since the parent is exiting; killing it explicitly is belt-and-suspenders.
    flushLogBufferSync();

    try {

      killStaleChrome();
    } catch {

      // Best-effort cleanup. If pkill fails for any reason, the process is exiting anyway.
    }
  });

  startServer(parsedArgs).catch((error: unknown): void => {

    LOG.error("Fatal startup error occurred: %s.", formatError(error));

    process.exit(1);
  });
}
