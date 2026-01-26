/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * app.ts: Express application builder for PrismCast.
 */
import { CONFIG, displayConfiguration, initializeConfiguration, validateConfiguration } from "./config/index.js";
import type { Express, NextFunction, Request, Response } from "express";
import { LOG, createMorganStream, formatError, getPackageVersion, resolveFFmpegPath, setConsoleLogging, startUpdateChecking, stopUpdateChecking } from "./utils/index.js";
import { closeBrowser, ensureDataDirectory, getCurrentBrowser, killStaleChrome, prepareExtension, setGracefulShutdown, startStalePageCleanup,
  stopStalePageCleanup } from "./browser/index.js";
import { initializeFileLogger, shutdownFileLogger } from "./utils/fileLogger.js";
import { startShowInfoPolling, stopShowInfoPolling } from "./streaming/showInfo.js";
import type { Nullable } from "./types/index.js";
import type { Server } from "http";
import { cleanupIdleStreams } from "./streaming/hls.js";
import consoleStamp from "console-stamp";
import express from "express";
import { getAllStreams } from "./streaming/registry.js";
import { initializeUserChannels } from "./config/userChannels.js";
import morgan from "morgan";
import { setupRoutes } from "./routes/index.js";
import { terminateStream } from "./streaming/lifecycle.js";
import { validateProfiles } from "./config/profiles.js";

/*
 * LOGGING MODE
 *
 * The logging mode is set at startup based on the --console CLI flag. When console logging is enabled, timestamps are added via console-stamp and output goes to
 * stdout/stderr. When file logging is used (the default), output goes to ~/.prismcast/prismcast.log.
 */

// Track whether console logging is enabled, set during startServer().
let usingConsoleLogging = false;

/*
 * APPLICATION STATE
 *
 * The HTTP server instance is stored globally so it can be closed during graceful shutdown.
 */

let server: Nullable<Server> = null;

// Interval for idle stream cleanup.
let idleCleanupInterval: Nullable<ReturnType<typeof setInterval>> = null;

/**
 * Starts the idle cleanup interval. Runs every 10 seconds to check for idle streams and terminate them.
 */
function startIdleCleanup(): void {

  if(idleCleanupInterval) {

    return;
  }

  // Check for idle streams every 10 seconds.
  idleCleanupInterval = setInterval(() => {

    cleanupIdleStreams();
  }, 10000);
}

/**
 * Stops the idle cleanup interval.
 */
function stopIdleCleanup(): void {

  if(idleCleanupInterval) {

    clearInterval(idleCleanupInterval);
    idleCleanupInterval = null;
  }
}

/*
 * GRACEFUL SHUTDOWN
 *
 * When the process receives a termination signal, we close all active streams and the browser before exiting. This ensures resources are released cleanly.
 */

/**
 * Sets up signal handlers for graceful shutdown. When SIGINT or SIGTERM is received, we close all streams, the browser, and the HTTP server before exiting.
 */
function setupGracefulShutdown(): void {

  let shutdownInProgress = false;

  async function shutdown(): Promise<void> {

    // Prevent multiple shutdown attempts if multiple signals are received.
    if(shutdownInProgress) {

      return;
    }

    shutdownInProgress = true;

    LOG.info("Shutting down.");

    // Set the graceful shutdown flag early so that page close errors are suppressed during stream termination.
    setGracefulShutdown(true);

    // Stop cleanup and polling intervals.
    stopStalePageCleanup();
    stopIdleCleanup();
    stopShowInfoPolling();
    stopUpdateChecking();

    // Terminate all streams. terminateStream() handles all cleanup including page closure and registry removal.
    const streams = getAllStreams();

    for(const stream of streams) {

      terminateStream(stream.id, stream.info.storeKey, "server shutdown");
    }

    // Close the browser.
    await closeBrowser();

    // Close the HTTP server.
    try {

      if(server) {

        server.close((): void => {

          LOG.info("HTTP server closed successfully.");
        });
      }
    } catch(error) {

      LOG.error("Error closing server during shutdown: %s.", formatError(error));
    }

    // Shut down file logger if in use.
    if(!usingConsoleLogging) {

      shutdownFileLogger();
    }

    process.exit(0);
  }

  process.on("SIGINT", (): void => {

    void shutdown();
  });

  process.on("SIGTERM", (): void => {

    void shutdown();
  });
}

/*
 * APPLICATION BUILDER
 *
 * The buildApp function creates and configures the Express application with all middleware and routes. This is separated from the server startup to allow for
 * testing and flexibility in deployment.
 */

/**
 * Creates and configures the Express application with all middleware and routes.
 * @returns The configured Express application.
 */
async function buildApp(): Promise<Express> {

  try {

    await prepareExtension();
  } catch(error) {

    LOG.error("Cannot build app without extension: %s.", formatError(error));

    throw error;
  }

  const app = express();

  // Trust proxy headers (X-Forwarded-Proto, X-Forwarded-Host) so that req.protocol and req.hostname reflect what the client actually used when accessing through
  // a reverse proxy. This ensures playlist URLs match the client's connection.
  app.set("trust proxy", true);

  // Add body parsing middleware for form submissions (configuration page).
  app.use(express.urlencoded({ extended: true }));
  app.use(express.json());

  // Configure Morgan for HTTP request logging based on httpLogLevel configuration. Morgan output goes through morganStream which handles timestamp formatting
  // consistently for both console and file logging modes.
  if(CONFIG.logging.httpLogLevel !== "none") {

    const morganFormat = ":method :url from :remote-addr responded :status in :response-time ms.";
    const morganStream = createMorganStream();

    // Patterns for browser-initiated asset requests that return 404. These are noise from browsers automatically requesting files that don't exist.
    const browserAssetPatterns = [ "/apple-touch-icon", "/favicon", "/robots.txt", "/site.webmanifest" ];

    if(CONFIG.logging.httpLogLevel === "errors") {

      // Log requests with 4xx or 5xx status codes, but skip 404s for common browser asset requests.
      app.use(morgan(morganFormat, {

        skip: (req, res): boolean => {

          // Log all non-error responses.
          if(res.statusCode < 400) {

            return true;
          }

          // Skip 404s for browser asset requests (favicon, apple-touch-icon, etc.).
          if(res.statusCode === 404) {

            const url = req.originalUrl || req.url;

            if(browserAssetPatterns.some((pattern) => url.startsWith(pattern))) {

              return true;
            }
          }

          // Skip 503s with Retry-After header. These indicate expected temporary unavailability (e.g., stream starting up) rather than a real error.
          if((res.statusCode === 503) && res.getHeader("Retry-After")) {

            return true;
          }

          return false;
        },

        stream: morganStream
      }));
    } else if(CONFIG.logging.httpLogLevel === "filtered") {

      // Log important requests while skipping high-frequency polling endpoints. We always log errors, slow requests, and critical endpoints.
      const skipPatterns = [ "/logs", "/health", "/favicon", "/logo.png", "/logo.svg" ];

      app.use(morgan(morganFormat, {

        skip: (req, res) => {

          // Always log errors.
          if(res.statusCode >= 400) {

            return false;
          }

          // Always log slow requests (over 1 second).
          const responseTime = parseFloat(res.getHeader("X-Response-Time") as string || "0");

          if(responseTime > 1000) {

            return false;
          }

          // Always log streaming and management endpoints.
          const url = req.originalUrl || req.url;
          const importantPatterns = [ "/stream", "/streams", "/config", "/playlist", "/debug" ];

          if(importantPatterns.some((pattern) => url.startsWith(pattern))) {

            return false;
          }

          // Skip high-frequency endpoints when successful.
          if(skipPatterns.some((pattern) => url.startsWith(pattern))) {

            return true;
          }

          // Skip successful requests to the root landing page.
          if((url === "/") && (res.statusCode < 400)) {

            return true;
          }

          // Log everything else.
          return false;
        },

        stream: morganStream
      }));
    } else {

      // Log all requests.
      app.use(morgan(morganFormat, { stream: morganStream }));
    }
  }

  // Set up all HTTP endpoints.
  setupRoutes(app);

  // Global error handler. Express error handlers require 4 parameters even if unused.
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  app.use((err: Error, _req: Request, res: Response, _next: NextFunction): void => {

    LOG.error("Unhandled error in request: %s.", formatError(err));

    if(!res.headersSent) {

      res.status(500).send("Internal server error");
    }
  });

  return app;
}

/*
 * SERVER STARTUP
 *
 * The startServer function initializes and starts the HTTP server. It validates configuration, cleans up stale processes, warms up the browser, and starts the
 * Express application.
 */

/**
 * Initializes and starts the HTTP server. Before accepting connections, we validate configuration, clean up stale Chrome processes, and warm up the browser
 * instance.
 * @param useConsoleLogging - Whether to log to console instead of file. Defaults to false (file logging).
 */
export async function startServer(useConsoleLogging = false): Promise<void> {

  // Set logging mode early before any log calls.
  usingConsoleLogging = useConsoleLogging;
  setConsoleLogging(useConsoleLogging);

  // Apply console-stamp for timestamps only when using console logging.
  if(useConsoleLogging) {

    consoleStamp(console, { format: ":date(yyyy/mm/dd HH:MM:ss.l)" });
  }

  // Initialize configuration from file and environment variables, then validate.
  try {

    await initializeConfiguration();
    validateConfiguration();
    validateProfiles();
  } catch(error) {

    LOG.error(formatError(error));

    process.exit(1);
  }

  displayConfiguration();
  setupGracefulShutdown();

  // Ensure the data directory exists before any operations that depend on it.
  await ensureDataDirectory();

  // Initialize file logger if not using console logging.
  if(!useConsoleLogging) {

    await initializeFileLogger(CONFIG.logging.maxSize);
  }

  // Check FFmpeg availability if using FFmpeg capture mode. This must be after file logger initialization so the log message is captured.
  if(CONFIG.streaming.captureMode === "ffmpeg") {

    const ffmpegPath = await resolveFFmpegPath();

    if(!ffmpegPath) {

      LOG.error("FFmpeg is not available. FFmpeg capture mode requires FFmpeg to be installed and in the system PATH.");
      LOG.error("Either install FFmpeg or change the capture mode to 'native' in the configuration.");

      process.exit(1);
    }

    LOG.info("Using FFmpeg at: %s", ffmpegPath);
  }

  // Load user channels from ~/.prismcast/channels.json if it exists.
  await initializeUserChannels();

  killStaleChrome();

  // Warm up browser.
  try {

    await getCurrentBrowser();

    LOG.info("Chrome ready.");
  } catch(error) {

    LOG.error("Failed to initialize browser during startup: %s.", formatError(error));

    throw error;
  }

  // Start stale page cleanup.
  startStalePageCleanup();

  // Start idle cleanup.
  startIdleCleanup();

  // Start show info polling for Channels DVR integration.
  startShowInfoPolling();

  // Start checking for updates.
  startUpdateChecking(getPackageVersion());

  // Build and start Express application.
  try {

    const app = await buildApp();

    server = app.listen(CONFIG.server.port, CONFIG.server.host, (): void => {

      LOG.info("PrismCast is now listening on %s:%s.", CONFIG.server.host, CONFIG.server.port);
    });
  } catch(error) {

    LOG.error("Failed to build application: %s.", formatError(error));

    throw error;
  }
}
