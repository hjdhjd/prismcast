/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * app.ts: Express application builder for PrismCast.
 */
import { CONFIG, displayConfiguration, initializeConfiguration, persistCoercedConfig, validateConfiguration } from "./config/index.ts";
import type { Express, NextFunction, Request, Response } from "express";
import type { IncomingMessage, Server } from "node:http";
import { LOG, claim, createMorganStream, formatError, formatTimestamp, getCurrentPattern, getPackageVersion, isDebugLogging, release, resolveFFmpegPath,
  setConsoleLogging, startUpdateChecking, stopUpdateChecking } from "./utils/index.ts";
import { closeBrowser, ensureDataDirectory, getCurrentBrowser, killStaleChrome, prepareExtension, setGracefulShutdown, setLoginModeEndObserver,
  startBrowserRestartChecking, startStalePageCleanup, stopBrowserRestartChecking, stopStalePageCleanup, syncWindowVisibility } from "./browser/index.ts";
import { ensureAllMigrated, snapshotAllForRelease } from "./config/persistence.ts";
import { flushHealthStateNow, loadHealthState } from "./config/health.ts";
import { getAllStreams, isCaptureIdentity } from "./streaming/registry.ts";
import { getDebugEnv, getLogFilePath, getServerPidFilePath } from "./config/paths.ts";
import { initializeFileLogger, shutdownFileLogger } from "./utils/fileLogger.ts";
import { loadResumeState, saveResumeState } from "./streaming/hlsResume.ts";
import { revalidateDomainAuth, stopPrecaching } from "./browser/precaching.ts";
import { startHdhrServer, stopHdhrServer } from "./hdhr/index.ts";
import { startPretunePolling, stopPretunePolling } from "./streaming/pretune.ts";
import { startShowInfoPolling, stopShowInfoPolling } from "./streaming/showInfo.ts";
import type { CliOverrides } from "./config/index.ts";
import type { Nullable } from "./types/index.ts";
import type { ParsedArgs } from "./index.ts";
import type { ResumeStreamData } from "./streaming/hlsResume.ts";
import { attachCdpUpgradeHandler } from "./routes/cdp.ts";
import { cleanupIdleStreams } from "./streaming/hls.ts";
import compression from "compression";
import express from "express";
import { generatePreroll } from "./streaming/preroll.ts";
import { initializeUserChannels } from "./config/userChannels.ts";
import { initializeUserProfiles } from "./config/userProfiles.ts";
import { installHealthBridge } from "./routes/config/channels/healthBridge.ts";
import { loadProviderLineups } from "./config/providerLineups.ts";
import morgan from "morgan";
import { runConsistencyProbeAtStartup } from "./config/consistencyProbe.ts";
import { setupRoutes } from "./routes/index.ts";
import { terminateStream } from "./streaming/lifecycle.ts";
import { validateProfiles } from "./config/profiles.ts";

/* The logging mode is set at startup based on the --console CLI flag. When console logging is enabled, the standard console output methods are wrapped to
 * prepend a formatTimestamp() prefix so console output matches the file logger's timestamp format. When file logging is used (the default), output goes to the
 * configured log file (default: prismcast.log in the data directory).
 */

// Track whether console logging is enabled, set during startServer().
let usingConsoleLogging = false;

/* The HTTP server instance is stored globally so it can be closed during graceful shutdown.
 */

let server: Nullable<Server> = null;

/* The background-service teardown stack. Each long-lived background service (stale-page sweep, browser-restart watchdog, idle cleanup, show-info/pretune polling,
 * update check) registers its stop here at the moment it starts in startServer(); graceful shutdown disposes the stack wholesale, so a future service cannot be
 * started without also being torn down. An AsyncDisposableStack is used (rather than the synchronous DisposableStack) so the teardown awaits each registered stop -
 * today's registered stops are all synchronous, but a future service whose stop must complete (e.g. flushing to disk) would be silently fire-and-forgotten by a
 * sync stack.
 */

let backgroundServices: Nullable<AsyncDisposableStack> = null;

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

/* When the process receives a termination signal, we close all active streams and the browser before exiting. This ensures resources are released cleanly.
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

    // Tear down the background services. HDHR is torn down first and awaited so its HTTP and UDP sockets fully release before the rest of shutdown proceeds. The
    // long-lived pollers are disposed wholesale via the AsyncDisposableStack they registered on at startup. The browser-launch-scoped precache cycle is cancelled
    // separately - its graceful-shutdown guard has already neutralized it via setGracefulShutdown above, and this clears the pending timer.
    await stopHdhrServer();
    await backgroundServices?.disposeAsync();
    stopPrecaching();

    // Terminate all streams. terminateStream() handles all cleanup including page closure and registry removal.
    const streams = getAllStreams();

    // Collect resume state from active streams before termination destroys the segmenters. Each entry captures sequence numbers and timestamps so the next startup
    // can seed from them, preventing HLS sequence resets that confuse Channels DVR. The segment index is decremented by one so the resumed playlist still includes
    // the last completed segment. Channels DVR's HLS fetcher will have already cached that segment, so its recorder consumes the cached data instead of dropping it
    // via drop_until_next_sequence when the playlist range would otherwise jump past it.
    const resumeEntries: ResumeStreamData[] = [];

    for(const stream of streams) {

      const segmenter = isCaptureIdentity(stream) ? stream.identity.captureSession?.segmenter : undefined;

      if(stream.info.storeKey && segmenter) {

        resumeEntries.push({

          channelName: stream.info.storeKey,
          initSegment: segmenter.getInitSegment(),
          initVersion: segmenter.getInitVersion(),
          segmentIndex: Math.max(0, segmenter.getSegmentIndex() - 1),
          trackTimestamps: segmenter.getTrackTimestamps()
        });
      }
    }

    saveResumeState(resumeEntries);

    for(const stream of streams) {

      terminateStream(stream.id, stream.info.storeKey, "server shutdown");
    }

    // Flush any pending debounced health-state write so the final channel/domain health is durably persisted even when shutdown lands inside the FLUSH_DELAY window.
    await flushHealthStateNow();

    // Close the browser.
    await closeBrowser();

    // Release the server instance slot so the next startup does not see a stale identity file.
    releaseInstanceSlot();

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

// HTTP request logging.

/* Every request's elapsed time comes from one stamp taken by the middleware below, and both readers of that time - the log line's token and the filtered mode's
 * slow-request rule - derive from it. A single stamp is what lets the two agree: a rule that consulted a different clock than the line it decides to print could
 * hold back a request the line would have reported as slow.
 *
 * process.hrtime.bigint() is the monotonic source, so a system clock adjustment mid-request cannot produce a negative or wildly inflated elapsed reading. The
 * stamps are held in a WeakMap keyed on the request object rather than on a property of it, so a request that never reaches the logger is collected with no
 * bookkeeping and no risk of a stale entry outliving its connection.
 */
const requestStartStamps = new WeakMap<IncomingMessage, bigint>();

// Requests slower than this are always logged in filtered mode, whatever their path. A second is the threshold at which a response stops feeling immediate to a
// client, which is the point at which it is worth a log line even on an endpoint the filter otherwise suppresses.
const SLOW_REQUEST_MS = 1000;

// Browser-initiated asset requests that return 404. These are noise from browsers automatically requesting files that do not exist.
const BROWSER_ASSET_PATTERNS = [ "/apple-touch-icon", "/favicon", "/robots.txt", "/site.webmanifest" ];

// High-frequency polling endpoints, skipped in filtered mode when they succeed.
const FILTERED_SKIP_PATTERNS = [ "/logs", "/health", "/favicon", "/logo.png", "/logo.svg" ];

// Streaming and management endpoints, always logged in filtered mode because they mark what the server is actually doing.
const FILTERED_IMPORTANT_PATTERNS = [ "/stream", "/streams", "/config", "/playlist", "/debug" ];

/**
 * Records a request's arrival time. Registered ahead of the logger so both readers of the elapsed time measure from the moment the request reached the app rather
 * than from whenever the logger happened to see it.
 * @param req - The incoming request.
 * @param _res - The response, unused.
 * @param next - The next middleware in the chain.
 */
export function stampRequestStart(req: IncomingMessage, _res: unknown, next: () => void): void {

  requestStartStamps.set(req, process.hrtime.bigint());

  next();
}

/**
 * Renders a request's elapsed time in milliseconds with three decimals, matching the shape morgan's own timing token produces, or an empty string when the request
 * carries no stamp. An unstamped request is the ordering case: a request that reached morgan without passing the stamping middleware has no time to report, and an
 * empty rendering leaves the log line's shape intact rather than printing a fabricated zero.
 * @param req - The incoming request.
 * @returns The elapsed milliseconds as a fixed-3 string, or an empty string.
 */
export function elapsedMillis(req: IncomingMessage): string {

  const elapsed = elapsedMillisValue(req);

  return (elapsed === null) ? "" : elapsed.toFixed(3);
}

/**
 * Reads a request's elapsed time in milliseconds from its start stamp, or null when the request carries no stamp.
 * @param req - The incoming request.
 * @returns The elapsed milliseconds, or null.
 */
function elapsedMillisValue(req: IncomingMessage): Nullable<number> {

  const startedAt = requestStartStamps.get(req);

  if(startedAt === undefined) {

    return null;
  }

  return Number(process.hrtime.bigint() - startedAt) / 1000000;
}

/**
 * Decides whether errors mode skips a request. Errors mode logs 4xx and 5xx only, minus the expected non-errors: a 404 for an asset the browser asked
 * for on its own, and a 503 carrying Retry-After, which announces a temporary unavailability the server chose rather than a fault.
 * @param request - The response status, the request URL, and whether a Retry-After header is present.
 * @returns True when the request should not be logged.
 */
export function skipInErrorsMode(request: { hasRetryAfter: boolean; statusCode: number; url: string }): boolean {

  // Skip all non-error responses. In errors-only mode we log only 4xx and 5xx statuses, so any successful response is skipped here.
  if(request.statusCode < 400) {

    return true;
  }

  // Skip 404s for browser asset requests (favicon, apple-touch-icon, etc.).
  if((request.statusCode === 404) && BROWSER_ASSET_PATTERNS.some((pattern) => request.url.startsWith(pattern))) {

    return true;
  }

  // Skip 503s with Retry-After header. These indicate expected temporary unavailability (e.g., stream starting up) rather than a real error.
  return (request.statusCode === 503) && request.hasRetryAfter;
}

/**
 * Decides whether filtered mode skips a request. Errors, slow requests, and the streaming and management endpoints are always logged; the high-frequency polling
 * endpoints and the landing page are skipped when they succeed; everything else is logged. A request with no elapsed reading is treated as not slow, so the
 * remaining rules decide it.
 * @param request - The elapsed milliseconds (null when unstamped), the response status, and the request URL.
 * @returns True when the request should not be logged.
 */
export function skipInFilteredMode(request: { elapsedMs: Nullable<number>; statusCode: number; url: string }): boolean {

  // Always log errors.
  if(request.statusCode >= 400) {

    return false;
  }

  // Always log slow requests.
  if((request.elapsedMs !== null) && (request.elapsedMs > SLOW_REQUEST_MS)) {

    return false;
  }

  // Always log streaming and management endpoints.
  if(FILTERED_IMPORTANT_PATTERNS.some((pattern) => request.url.startsWith(pattern))) {

    return false;
  }

  // Skip high-frequency endpoints when successful.
  if(FILTERED_SKIP_PATTERNS.some((pattern) => request.url.startsWith(pattern))) {

    return true;
  }

  // Skip successful requests to the root landing page, and log everything else.
  return request.url === "/";
}

/* The token is defined once for the process, at module load. morgan's token registry is global to the module, so registering inside the app builder would re-register
 * on every build - harmless today because the definition is constant, but a registration whose count tracks how often the app is assembled is the kind of coupling
 * that only shows up once a second build carries different state.
 */
morgan.token("elapsed", elapsedMillis);

/* The buildApp function creates and configures the Express application with all middleware and routes. This is separated from the server startup to allow for
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

  // Enable response compression for HTML, JSON, CSS, JavaScript, and text responses. SSE is excluded because compression buffers output for better ratios, which
  // conflicts with SSE's need to deliver events immediately. Binary video data (HLS segments, MPEG-TS) is skipped automatically by the compressible MIME type check.
  app.use(compression({

    filter: (req, res) => {

      if(res.getHeader("Content-Type") === "text/event-stream") {

        return false;
      }

      return compression.filter(req, res);
    }
  }));

  // Add body parsing middleware for form submissions (configuration page).
  app.use(express.urlencoded({ extended: true }));
  app.use(express.json());

  // Configure Morgan for HTTP request logging based on httpLogLevel configuration. Morgan output goes through morganStream which handles timestamp formatting
  // consistently for both console and file logging modes.
  if(CONFIG.logging.httpLogLevel !== "none") {

    const morganFormat = ":method :url from :remote-addr responded :status in :elapsed ms.";
    const morganStream = createMorganStream();

    // Stamp each request's arrival before morgan sees it, so the elapsed token and the filtered mode's slow-request rule read the same start time.
    app.use(stampRequestStart);

    if(CONFIG.logging.httpLogLevel === "errors") {

      // Log requests with 4xx or 5xx status codes, but skip 404s for common browser asset requests.
      app.use(morgan(morganFormat, {

        skip: (req, res): boolean => skipInErrorsMode({

          hasRetryAfter: res.getHeader("Retry-After") !== undefined,
          statusCode: res.statusCode,
          url: req.originalUrl || req.url
        }),

        stream: morganStream
      }));
    } else if(CONFIG.logging.httpLogLevel === "filtered") {

      // Log important requests while skipping high-frequency polling endpoints. We always log errors, slow requests, and critical endpoints.
      app.use(morgan(morganFormat, {

        skip: (req, res): boolean => skipInFilteredMode({

          elapsedMs: elapsedMillisValue(req),
          statusCode: res.statusCode,
          url: req.originalUrl || req.url
        }),

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
  app.use((err: Error, _req: Request, res: Response, _next: NextFunction): void => {

    LOG.error("Unhandled error in request: %s.", formatError(err));

    if(!res.headersSent) {

      res.status(500).send("Internal server error");
    }
  });

  return app;
}

/* PrismCast instance guard. Only one server instance should run at a time - a second instance would launch a competing Chrome process, bind to the same port,
 * and corrupt shared state. The guard delegates to the runtime-identity primitive in utils/runtimeIdentity.ts, which composes PID liveness with the current
 * boot session ID to classify the on-disk identity file. Stale records from reboots, container restarts, crashes, and ungraceful shutdowns are recovered
 * transparently: the next claim() overwrites them. A held-live record (same boot, alive PID) is the only state that rejects a startup.
 *
 * Ownership is verified structurally inside release(): the function reads the on-disk record and only removes the file when its PID matches this process's
 * PID. The file content is itself the source of truth, so a rejected duplicate startup's exit handler cannot accidentally delete the legitimate holder's file.
 */

/**
 * Claims the server instance slot for this process. On success, the identity file at getServerPidFilePath() now holds our PID and boot session ID; subsequent
 * starts of PrismCast will recover the file as stale on the next reboot/crash. On failure, a live instance already holds the slot - we surface a precise
 * diagnostic (PID + version + start time of the holder) and exit.
 */
function claimInstanceSlot(): void {

  const result = claim(getServerPidFilePath(), { version: getPackageVersion() });

  if(result.ok) {

    return;
  }

  const conflict = result.conflict;

  // eslint-disable-next-line no-console
  console.error("Error: another PrismCast instance is already running (PID " + String(conflict.pid) + ", version " + conflict.version + ", started " +
    conflict.startedAt + "). Stop it before starting a new one.");

  process.exit(1);
}

/**
 * Releases the server instance slot if this process owns it. Called during graceful shutdown and from the process exit handler. The ownership check is
 * structural (release() reads the on-disk record and refuses to remove a file belonging to a different PID), so a rejected-duplicate startup whose exit
 * handler runs this function will correctly leave the legitimate holder's file in place. Tolerates an unresolved data directory: a startup that fails before
 * initializeDataDir() runs still reaches the exit handler, and a slot we never claimed has nothing to release.
 */
export function releaseInstanceSlot(): void {

  try {

    release(getServerPidFilePath());
  } catch {

    // The data directory may not have been initialized when this runs (early-startup failure paths). Without a resolvable path there is no slot to release; the
    // legitimate holder's file, if any, is in the configured data directory which we cannot address - so we never could have touched it. Silent no-op is safe.
  }
}

/**
 * Starts the main HTTP server and resolves only once the bind has actually succeeded. Express's app.listen callback fires even when the bind fails (it resolves
 * with a non-listening server on EADDRINUSE / EADDRNOTAVAIL), so it cannot be trusted to signal success; we detect the outcome through the explicit "listening"
 * and "error" events instead, mirroring the HDHomeRun HTTP server and the UDP responder. A bind failure rejects rather than lingering, so startServer's caller
 * logs the error and exits cleanly instead of leaving a process that printed a false "listening" line while its HTTP surface never came up.
 * @param app - The built Express application.
 * @returns The listening http.Server.
 */
async function listenMainServer(app: Express): Promise<Server> {

  const { promise, reject, resolve } = Promise.withResolvers<Server>();
  const httpServer = app.listen(CONFIG.server.port, CONFIG.server.host);

  // On a bind failure, reject with a clear, operator-actionable message. Exactly one of "error" or "listening" fires for a given bind attempt, so the promise
  // always settles.
  const onBindError = (error: NodeJS.ErrnoException): void => {

    if(error.code === "EADDRINUSE") {

      LOG.error("Cannot start PrismCast: port %s on %s is already in use. Stop the conflicting service or choose a different port.",
        CONFIG.server.port, CONFIG.server.host);
    } else {

      LOG.error("Failed to start the PrismCast HTTP server: %s.", formatError(error));
    }

    reject(error);
  };

  httpServer.once("error", onBindError);

  httpServer.once("listening", (): void => {

    // Bind succeeded: retire the bind-failure handler and attach a long-lived runtime handler so a later socket error is logged rather than crashing the process
    // on an unhandled "error" event.
    httpServer.removeListener("error", onBindError);
    httpServer.on("error", (error: NodeJS.ErrnoException): void => {

      LOG.error("PrismCast HTTP server encountered a socket error: %s.", formatError(error));
    });

    LOG.info("PrismCast is now listening on %s:%s.", CONFIG.server.host, CONFIG.server.port);

    resolve(httpServer);
  });

  return promise;
}

/* The startServer function initializes and starts the HTTP server. It validates configuration, cleans up stale processes, warms up the browser, and starts the
 * Express application.
 */

/**
 * Initializes and starts the HTTP server. Before accepting connections, we validate configuration, clean up stale Chrome processes, and warm up the browser
 * instance.
 * @param parsedArgs - Parsed command-line arguments containing flags and override values.
 */
export async function startServer(parsedArgs: ParsedArgs): Promise<void> {

  // Set logging mode early before any log calls.
  usingConsoleLogging = parsedArgs.consoleLogging;
  setConsoleLogging(parsedArgs.consoleLogging);

  // Apply timestamps to console output only when using console logging. Wraps the standard console output methods so each call prepends a timestamp matching the
  // file logger's format, ensuring console output and prismcast.log share identical timestamps. The wrappers are installed once at startup and affect every log
  // call from anywhere in the process - ours, Node's internal warnings, third-party library output - without distributing the responsibility across hundreds of
  // call sites. The no-console suppressions are intentional: the entire purpose of this block is to replace console's standard methods with timestamped wrappers.
  if(parsedArgs.consoleLogging) {

    const methods = [ "log", "info", "warn", "error" ] as const;

    for(const method of methods) {

      // eslint-disable-next-line no-console
      const original = console[method].bind(console);

      // eslint-disable-next-line no-console
      console[method] = (...args: unknown[]): void => {

        original("[" + formatTimestamp() + "]", ...args);
      };
    }
  }

  // Build CLI overrides from parsed arguments. These have the highest priority in the merge order (CLI > env > config.json > defaults).
  const cliOverrideMap: [keyof ParsedArgs, string][] = [
    [ "chromeDataDir", "paths.chromeDataDir" ],
    [ "logFile", "paths.logFile" ],
    [ "port", "server.port" ]
  ];

  const cliOverrides: CliOverrides = {};

  for(const [ argKey, configPath ] of cliOverrideMap) {

    if(parsedArgs[argKey] !== undefined) {

      cliOverrides[configPath] = parsedArgs[argKey];
    }
  }

  // Release boot coordinator: snapshot every persistence-managed file before any reads or migrations run, then apply any pending schema migrations across all
  // stores. Both operations are safe to run more than once within a release (snapshots skip when the labeled copy already exists; ensureMigrated skips when the
  // file is at the current schema version). Running them up front guarantees a restore point exists for every file at the start of every release boot, even if a
  // subsequent initialize* function or migration discovers a problem and aborts startup.
  try {

    await snapshotAllForRelease("pre-v" + getPackageVersion());
    await ensureAllMigrated();
  } catch(error) {

    LOG.error("Failed during release boot coordinator: %s.", formatError(error));

    process.exit(1);
  }

  // Initialize configuration from file and environment variables, then validate. CLI overrides are applied as the highest-priority merge pass. User profiles are
  // loaded after configuration validation but before profile validation, so that user-defined profiles and domain mappings are available for the validation pass.
  try {

    await initializeConfiguration(cliOverrides);
    validateConfiguration();

    // Write any startup capture coercion back to disk so the on-disk state matches the coerced live CONFIG. This keeps a config file that holds an unsupported
    // capture value (native mode) from staying divergent forever, which would otherwise make the reload-validation path reject every later save on the phantom
    // capture diff. A no-op unless validateConfiguration actually coerced something.
    await persistCoercedConfig();

    await initializeUserProfiles();
    validateProfiles();
  } catch(error) {

    LOG.error(formatError(error));

    process.exit(1);
  }

  setupGracefulShutdown();

  // Ensure the data directory exists before any operations that depend on it.
  await ensureDataDirectory();

  // Claim the server instance slot before launching Chrome or binding the port. This must come after ensureDataDirectory() since the identity file lives there.
  // claimInstanceSlot() exits with a precise diagnostic when a live holder is detected; on success the identity file holds our PID and boot session ID. If
  // startup subsequently fails, the exit handler calls releaseInstanceSlot() to remove the stale file.
  claimInstanceSlot();

  // Initialize file logger if not using console logging. This must happen after config loading (to resolve the log file path) and after ensureDataDirectory()
  // (to create the parent directory). All startup log messages that should appear in the log file must come after this point.
  if(!parsedArgs.consoleLogging) {

    await initializeFileLogger(getLogFilePath(CONFIG), CONFIG.logging.maxSize);
  }

  // Log the version and active configuration as the first messages captured by the file logger.
  displayConfiguration();

  // Log the debug filter status after the file logger is ready so the message is captured.
  if(isDebugLogging()) {

    const debugEnv = getDebugEnv();

    if(debugEnv) {

      LOG.info("Debug logging enabled with filter: %s (from PRISMCAST_DEBUG).", debugEnv);
    } else if((CONFIG.logging.debugFilter.length > 0) && (getCurrentPattern() === CONFIG.logging.debugFilter)) {

      LOG.info("Debug logging enabled with filter: %s (from config).", CONFIG.logging.debugFilter);
    } else {

      LOG.info("Debug logging enabled for all categories.");
    }
  }

  // Check FFmpeg availability if using FFmpeg capture mode. This must be after file logger initialization so the log message is captured.
  if(CONFIG.streaming.captureMode === "ffmpeg") {

    const ffmpegPath = await resolveFFmpegPath();

    if(!ffmpegPath) {

      LOG.error("FFmpeg is not available. FFmpeg capture mode requires FFmpeg to be installed and in the system PATH.");
      LOG.error("Either install FFmpeg or change the capture mode to 'native' in the configuration.");

      process.exit(1);
    }

    LOG.info("Using FFmpeg at: %s.", ffmpegPath);
  }

  // Load user channels from channels.json in the data directory if it exists.
  await initializeUserChannels();

  // Load persisted health state (channel health + domain auth) from health.json.
  await loadHealthState();

  // Load the provider channel lineups persisted by earlier sessions from provider-lineups.json. They are verify-on-use hints: they load before the browser does,
  // so a boot whose own precache walk comes back empty can still tune a channel directly instead of failing at a guide page it cannot read.
  await loadProviderLineups();

  // Install the reactive bridge that translates health/auth state changes into channel table patches over SSE. Channel row HTML has a single source of truth -
  // generateChannelRowHtml on the server - and every reactive update flows through this bridge so the client never composes channel row state imperatively.
  installHealthBridge();

  // Wire the login-end observer: when login mode ends, a domain currently marked needs-sign-in gets an automatic revalidation discovery so fresh success evidence
  // can clear the flag without waiting for the next precache cycle. Composition-root wiring - login.ts stays free of discovery knowledge, and revalidateDomainAuth
  // never rejects, so voiding the promise is safe.
  setLoginModeEndObserver((url) => void revalidateDomainAuth(url));

  // Run the cross-store consistency probe now that every store is loaded. Validates foreign-key-style rules spanning multiple stores (service selections, variant
  // canonicalKey targets, domain profile mappings, service tag filter) and auto-fixes warnings where safe. Errors do not block startup.
  await runConsistencyProbeAtStartup();

  // Load HLS resume state from the previous shutdown. This seeds sequence numbers so streams resume forward instead of resetting to 0.
  loadResumeState();

  killStaleChrome();

  // Warm up the browser. getCurrentBrowser() launches Chrome through the capture-readiness supervisor, whose launch gate runs the real capture probe (the
  // capability tier) at every launch - including this one. So a successful warm-up already means capture is verified. A browser that cannot capture rejects the
  // warm-up and aborts startup.
  try {

    await getCurrentBrowser("page");
  } catch(error) {

    LOG.error("Failed to initialize browser during startup: %s.", formatError(error));

    throw error;
  }

  // Generate the preroll fMP4 segment for immediate HLS response during stream startup. This runs after browser launch because the preroll has to be encoded in the
  // codec capture will actually produce, and that choice reads the GPU capabilities the launch-time capability probe caches.
  await generatePreroll();

  // Settle the window against the visibility policy. Nothing is streaming yet, so this leaves it minimized: the desktop stays clear and the GPU stays idle until a
  // capture stream needs the window on screen. We defer the sync until after the browser capability probe and the launch-gate capture probe complete, because the
  // GPU probe wants an environment representative of the one capture runs in and the capture probe acquires a real capture, both of which want the window as the
  // launch left it.
  await syncWindowVisibility();

  // Start the background services and register each one's stop on an AsyncDisposableStack the moment it starts, so graceful shutdown can dispose them all wholesale
  // and a future service cannot be started without also being torn down. These are order-independent background loops/timers, so LIFO disposal order is immaterial.
  // HDHR is intentionally not a member (it is torn down first in shutdown so its sockets release before the rest), and the browser-launch-scoped precache cycle is
  // started elsewhere and stopped separately.
  backgroundServices = new AsyncDisposableStack();

  startStalePageCleanup();
  backgroundServices.defer(stopStalePageCleanup);

  startBrowserRestartChecking();
  backgroundServices.defer(stopBrowserRestartChecking);

  startIdleCleanup();
  backgroundServices.defer(stopIdleCleanup);

  startShowInfoPolling();
  backgroundServices.defer(stopShowInfoPolling);

  startPretunePolling();
  backgroundServices.defer(stopPretunePolling);

  startUpdateChecking(getPackageVersion());
  backgroundServices.defer(stopUpdateChecking);

  // Build and start Express application.
  try {

    const app = await buildApp();

    // Bind the main HTTP server, detecting bind success or failure through explicit events rather than the unreliable listen callback. A bind failure throws out
    // of here so the catch below surfaces it and the process exits cleanly rather than lingering with no HTTP surface.
    server = await listenMainServer(app);

    // Attach the CDP proxy upgrade handler once the underlying http.Server exists. The handler is gated on the `cdp` debug category at request time, so it sits
    // dormant until the user enables CDP via /debug. We attach unconditionally so the toggle takes effect without requiring a restart.
    attachCdpUpgradeHandler(server);
  } catch(error) {

    // Covers both a buildApp failure and a listenMainServer bind failure; the latter already logged the precise, operator-actionable line before rejecting.
    LOG.error("Failed to build or start the HTTP server: %s.", formatError(error));

    throw error;
  }

  // Start HDHomeRun emulation server if enabled. This runs independently of the main server and handles EADDRINUSE gracefully without affecting PrismCast's
  // primary functionality.
  await startHdhrServer();
}
