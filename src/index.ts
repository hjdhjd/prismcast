/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Entry point for PrismCast.
 */
import { LOG, formatError, getPackageVersion, initDebugFilter, setDebugLogging } from "./utils/index.js";
import { CONFIG } from "./config/index.js";
import { handleServiceCommand } from "./service/index.js";
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
  console.log("  service             Manage PrismCast as a system service");
  console.log("                      Run 'prismcast service --help' for details");
  console.log("");
  console.log("Options:");
  console.log("  -c, --console       Log to console instead of file (for Docker or debugging)");
  console.log("  -d, --debug         Enable debug logging (verbose output for troubleshooting)");
  console.log("  -h, --help          Show this help message");
  console.log("  -p, --port <port>   Set server port (default: 5589)");
  console.log("  -v, --version       Show version number");
  console.log("");
  console.log("If no command is specified, starts the PrismCast server.");
  console.log("");
  console.log("Environment Variables:");
  console.log("  PORT                HTTP server port");
  console.log("  HOST                HTTP server bind address");
  console.log("  QUALITY_PRESET      Video quality preset (480p, 720p, 1080p, 1080p-high, 4k)");
  console.log("  VIDEO_BITRATE       Video bitrate (bps)");
  console.log("  AUDIO_BITRATE       Audio bitrate (bps)");
  console.log("  FRAME_RATE          Target frame rate");
  console.log("  CHROME_BIN          Path to Chrome executable");
  console.log("  LOG_MAX_SIZE        Maximum log file size in bytes (default: 1048576)");
  console.log("  PRISMCAST_DEBUG     Debug category filter (e.g., 'tuning:hulu', 'recovery', '*,-streaming:segmenter')");
  /* eslint-enable no-console */
}

/**
 * Result of parsing command-line arguments.
 */
interface ParsedArgs {

  consoleLogging: boolean;
  debugLogging: boolean;
}

/**
 * Parses command-line arguments and applies overrides to the configuration.
 * @returns Parsed argument flags.
 */
function parseArgs(): ParsedArgs {

  const args = process.argv.slice(2);
  let consoleLogging = false;
  let debugLogging = false;

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

      const port = parseInt(args[++i]);

      if(!isNaN(port)) {

        CONFIG.server.port = port;
      }
    }

    if((arg === "-v") || (arg === "--version")) {

      // eslint-disable-next-line no-console
      console.log("PrismCast v" + getPackageVersion());

      process.exit(0);
    }
  }

  return { consoleLogging, debugLogging };
}

/* Check for subcommands before parsing flags. Subcommands are handled separately and exit after completion.
 */

const rawArgs = process.argv.slice(2);
const subcommand = rawArgs[0];

// Handle the 'service' subcommand for managing PrismCast as a system service.
if(subcommand === "service") {

  handleServiceCommand(rawArgs.slice(1)).then((exitCode) => {

    process.exit(exitCode);
  }).catch((error: Error) => {

    // eslint-disable-next-line no-console
    console.error("Service command failed: " + (error.message || String(error)));

    process.exit(1);
  });
} else {

  /* The main entry point parses command-line arguments, starts the server, and handles any fatal errors that occur during initialization. If startup fails, we exit
   * with a non-zero code to signal the failure to process managers.
   */

  const parsedArgs = parseArgs();

  // Enable debug logging before starting the server so debug messages during startup are captured. The PRISMCAST_DEBUG environment variable takes precedence over
  // the --debug CLI flag, allowing fine-grained category selection.
  const debugEnv = process.env.PRISMCAST_DEBUG;

  if(debugEnv) {

    initDebugFilter(debugEnv);
  } else if(parsedArgs.debugLogging) {

    setDebugLogging(true);
  }

  startServer(parsedArgs.consoleLogging).catch((error: Error): void => {

    LOG.error("Fatal startup error occurred: %s.", formatError(error));

    process.exit(1);
  });
}
