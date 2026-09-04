/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fileLogger.ts: File-based logging with automatic size-based rotation for PrismCast.
 */
import type { Nullable } from "../types/index.ts";
import { boundedWait } from "./delay.ts";
import { formatTimestamp } from "./format.ts";
import fs from "node:fs";
import { isAnyDebugEnabled } from "./debugFilter.ts";
import path from "node:path";
import { styleText } from "node:util";

const { promises: fsPromises } = fs;

/* Color names accepted by node:util.styleText for log output. The logger uses cyan for debug, yellow for warnings, and red for errors. A null value indicates the
 * default terminal color (used by info-level messages).
 */
export type LogColor = "cyan" | "red" | "yellow" | null;

/* The file logger provides persistent logging to a configurable log file with automatic size-based trimming. When the log file exceeds the configured maximum
 * size, it is trimmed to half the maximum size, keeping only complete lines (the most recent logs are preserved). This approach prevents unbounded log growth while
 * maintaining recent history for troubleshooting.
 *
 * Design decisions:
 *
 * 1. Asynchronous buffered writes - Logs are collected in a buffer and flushed periodically to avoid blocking the event loop during high-frequency logging.
 * 2. Periodic size checking - File size is checked every N writes rather than on each write to minimize syscall overhead.
 * 3. Atomic trim operations - Trimming writes to a temp file then renames, preventing data loss if the process crashes during trim.
 * 4. Timestamps - Delegates to formatTimestamp() in utils/format.ts, which emits yyyy/mm/dd hh:mm:ss.mmm AM/PM. The same helper feeds the console method
 *    wrappers in app.ts and the Morgan HTTP request logger, so file, console, and request logs share identical timestamps.
 * 5. Terminal coloring - SGR escape codes are baked into the file output via styleText so that viewing the log with terminal commands (tail -f, less -R, cat)
 *    shows the same color scheme as console output. styleText emits both the opening color code and the trailing reset in one call, with validateStream disabled
 *    because the log file is not a TTY but we still want the codes preserved for downstream terminal viewers.
 * 6. Startup window - The logger cannot open its file until the configuration that names the path and the size cap has been read, and reading it is itself
 *    worth logging. Entries emitted before the one initialization are held in the write buffer and appended by the first flush after it, so the log file
 *    opens with the boot messages that preceded it. The window is bounded and closes at that initialization whether it succeeds or fails.
 */

/* The file logger maintains state for the log file path, write buffer, and size tracking. State is initialized when initializeFileLogger() is called during server
 * startup.
 */

// Path to the log file, set during initialization.
let logFilePath: Nullable<string> = null;

/* The file the logger shut down, kept so a line logged after that shutdown - the exit handler's Chrome cleanup, a late process-level handler - is written to
 * the run's own file rather than dropped. A new initialization supersedes it, and a single failed append abandons it.
 */
let closedFilePath: Nullable<string> = null;

// Buffer for collecting log entries before flushing to disk.
let writeBuffer: string[] = [];

// Approximate file size tracked in memory between actual file size checks.
let approximateSize = 0;

// Counter for tracking writes since last file size check.
let writeCount = 0;

// Timer for periodic buffer flushing.
let flushTimer: Nullable<ReturnType<typeof setInterval>> = null;

// Flag indicating whether the file logger is initialized and operational.
let isInitialized = false;

/* Whether the startup window is open. The window is the interval between process start and the logger's one initialization, and entries logged inside it are
 * held in writeBuffer for the first flush after that initialization to append. The first initializeFileLogger call closes the window - success or failure -
 * and nothing reopens it, and what happens to a later entry follows from that: once the logger has shut down the entry goes straight to the file that shutdown
 * closed, and after an initialization that failed there is no file to take it, so it is dropped. Holding it for a later initialization instead - something only
 * the test suite performs - would leak the entry into a different run's file.
 */
let startupWindowOpen = true;

/* Tail of the write-ordering chain. Every asynchronous mutation of the log file (buffer flushes and trims) is appended to this promise so that the operations run
 * strictly one after another. Without this serialization a flush's appendFile could land between a trim's readFile and its rename, and the rename would then
 * overwrite the just-appended lines with stale trimmed content - silently dropping log entries. Routing both paths through a single ordering primitive makes
 * "read-modify-rename" and "append" mutually exclusive without an explicit lock. The tail never rejects: the helper that enqueues work swallows the settled
 * outcome before chaining the next operation, so one failing operation cannot poison the chain for subsequent writes.
 */
let writeChain: Promise<void> = Promise.resolve();

// How long shutdown waits for the outstanding write chain to drain. Generous enough for a trim's read, write, and rename on a busy disk, and short enough that a
// wedged filesystem operation cannot hold the process open through its own shutdown.
const SHUTDOWN_DRAIN_BOUND_MS = 5000;

// Flag to temporarily disable logging on write errors, preventing error cascades.
let isDisabled = false;

// Timestamp when logging was disabled due to error, for retry timing.
let disabledAt = 0;

// Maximum log file size, set during initialization.
let maxLogSize = 1048576;

// Configuration Constants.

// Interval in milliseconds between buffer flushes.
const FLUSH_INTERVAL_MS = 1000;

// Number of writes between file size checks.
const SIZE_CHECK_FREQUENCY = 100;

// Duration in milliseconds to disable logging after a write error before retrying.
const ERROR_RETRY_DELAY_MS = 60000;

// The most entries the startup window holds. A push past the limit drops the oldest entry, because the newest lines are the ones that explain an exit.
const STARTUP_BUFFER_LIMIT = 1000;

// Initialization.

/**
 * Initializes the file logger. Creates the log file if it does not exist. Must be called after the data directory is ensured to exist. A call is the one
 * initialization of the process: it closes the startup window either way, on success leaving the window's entries in the buffer for the first flush to append
 * and counting their bytes toward the tracked file size, and on failure discarding them.
 * @param logPath - Absolute path to the log file, resolved by the caller via getLogFilePath().
 * @param maxSize - Maximum log file size in bytes from CONFIG.logging.maxSize.
 */
export async function initializeFileLogger(logPath: string, maxSize: number): Promise<void> {

  // A new initialization supersedes the file the last shutdown closed, so every line from here on belongs to this run's file.
  closedFilePath = null;
  logFilePath = logPath;
  maxLogSize = maxSize;

  try {

    // Ensure the parent directory of the log file exists.
    await fsPromises.mkdir(path.dirname(logFilePath), { recursive: true });

    // Check if log file exists and get its size.
    try {

      const stats = await fsPromises.stat(logFilePath);

      approximateSize = stats.size;
    } catch(error) {

      // File does not exist, create it.
      if((error as NodeJS.ErrnoException).code === "ENOENT") {

        await fsPromises.writeFile(logFilePath, "", "utf-8");
        approximateSize = 0;
      } else {

        throw error;
      }
    }

    /* The entries the startup window holds are appended by the first flush, so their bytes belong to the size the trim heuristic works from. Adding them after
     * the stat above keeps approximateSize describing what the file holds once that flush lands.
     */
    for(const entry of writeBuffer) {

      approximateSize += entry.length;
    }

    // Start the periodic flush timer.
    flushTimer = setInterval((): void => {

      void flushLogBuffer();
    }, FLUSH_INTERVAL_MS);

    isInitialized = true;
    startupWindowOpen = false;
  } catch(error) {

    // A failed initialization closes the startup window and discards what it held: no file will ever take those entries, and holding them would grow the
    // buffer for the life of the process.
    startupWindowOpen = false;
    writeBuffer = [];

    // Log to console since file logging failed, but do not throw - file logging is a best-effort feature.
    // eslint-disable-next-line no-console
    console.error("Failed to initialize file logger: %s. File logging disabled.", (error instanceof Error) ? error.message : String(error));
  }
}

// Log Entry Writing.

/**
 * Formats one log entry as it appears in the file: the timestamp, the level tag (carrying the debug category when one is supplied), and the message, colored
 * and newline-terminated. The buffered path and the post-shutdown append both call it, so the shape of a line is stated in one place. See the file's design
 * block for the rationale behind baking SGR codes into the file output.
 * @param level - Log level ("info", "warn", "error", "debug").
 * @param message - The formatted log message.
 * @param color - Color name accepted by node:util.styleText, or null for the default terminal color.
 * @param categoryTag - Optional debug category tag (e.g., "recovery:tab"). Appended to the level prefix as [DEBUG:category].
 * @returns The entry text, terminated by a newline.
 */
function formatLogEntry(level: string, message: string, color: LogColor, categoryTag?: string): string {

  const timestamp = formatTimestamp();
  const levelTag = categoryTag ? level.toUpperCase() + ":" + categoryTag : level.toUpperCase();
  const levelPrefix = (level === "info") ? "" : "[" + levelTag + "] ";
  const body = levelPrefix + message;
  const coloredBody = color ? styleText(color, body, { validateStream: false }) : body;

  return "[" + timestamp + "] " + coloredBody + "\n";
}

/**
 * Writes a log entry to the buffer. Entries are flushed to disk periodically.
 * @param level - Log level ("info", "warn", "error", "debug").
 * @param message - The formatted log message.
 * @param color - Color name accepted by node:util.styleText, or null for the default terminal color.
 * @param categoryTag - Optional debug category tag (e.g., "recovery:tab"). Appended to the level prefix as [DEBUG:category].
 */
export function writeLogEntry(level: string, message: string, color: LogColor, categoryTag?: string): void {

  const isWritingToFile = isInitialized && (logFilePath !== null);

  if(isWritingToFile) {

    // Check if logging is disabled due to previous error and whether retry delay has passed.
    if(isDisabled) {

      if((Date.now() - disabledAt) < ERROR_RETRY_DELAY_MS) {

        return;
      }

      // Re-enable logging and try again.
      isDisabled = false;
    }
  } else if(closedFilePath !== null) {

    /* The logger has shut down and this line still belongs to the run that just ended, so it goes to that run's file. The append is synchronous because
     * nothing asynchronous is left to carry it: the flush timer is stopped, the write chain has drained, and the process may exit on the next tick.
     */
    try {

      fs.appendFileSync(closedFilePath, formatLogEntry(level, message, color, categoryTag), "utf-8");
    } catch(error) {

      // eslint-disable-next-line no-console
      console.error("Failed to write a log entry after shutdown: %s.", (error instanceof Error) ? error.message : String(error));

      // Abandon the closed file so a path that cannot be written is reported once rather than once per line.
      closedFilePath = null;
    }

    return;
  } else if(!startupWindowOpen) {

    // Nothing holds the entry: no file is open, no closed file is waiting for it, and the startup window has closed for good.
    return;
  }

  const entry = formatLogEntry(level, message, color, categoryTag);

  // Add to buffer.
  writeBuffer.push(entry);

  /* The size accounting and the periodic size check below describe an open file, which the startup window does not have, so the window skips them and bounds
   * itself instead. The check runs on every push, so the oldest entry makes room as soon as one entry too many arrives.
   */
  if(!isWritingToFile) {

    if(writeBuffer.length > STARTUP_BUFFER_LIMIT) {

      writeBuffer.shift();
    }

    return;
  }

  approximateSize += entry.length;
  writeCount++;

  // Check if we should verify actual file size.
  if((writeCount % SIZE_CHECK_FREQUENCY) === 0) {

    void checkAndTrimFile();
  }
}

// Write Ordering.

/**
 * Serializes an asynchronous log-file mutation against every other such mutation. The supplied operation is appended to the module-scoped write chain so that
 * flushes and trims execute one at a time, never interleaving their read/append/rename steps. The returned promise resolves when this specific operation has
 * settled. The chain itself is advanced with the settled (caught) result so a rejecting operation cannot poison subsequent writes - each operation is responsible
 * for its own error handling, exactly as the inline try/catch blocks did before serialization.
 * @param operation - The async mutation to run after all previously enqueued mutations have completed.
 */
async function serializeWrite(operation: () => Promise<void>): Promise<void> {

  // Chain this operation after the current tail, then advance the tail to a promise that always resolves so a single failure does not break ordering for the next
  // caller. We capture the run promise to return it, while the tail intentionally swallows the outcome.
  const run = writeChain.then(operation);

  writeChain = run.then((): void => undefined, (): void => undefined);

  return run;
}

// Buffer Flushing.

/**
 * Flushes the write buffer to disk asynchronously. Called periodically by the flush timer. The append is routed through the write-ordering chain so it cannot
 * interleave with an in-flight trim, which would otherwise discard the appended lines when the trim renames its stale snapshot over the file.
 */
export async function flushLogBuffer(): Promise<void> {

  if(!isInitialized || !logFilePath || (writeBuffer.length === 0)) {

    return;
  }

  // Take the current buffer and reset. We snapshot the path as well so the serialized operation is not affected by a concurrent shutdown clearing logFilePath.
  const entries = writeBuffer;
  const targetPath = logFilePath;

  writeBuffer = [];

  const content = entries.join("");

  return serializeWrite(async (): Promise<void> => {

    try {

      await fsPromises.appendFile(targetPath, content, "utf-8");
    } catch(error) {

      // Disable logging temporarily to prevent error cascade.
      isDisabled = true;
      disabledAt = Date.now();

      // Log to console as fallback.
      // eslint-disable-next-line no-console
      console.error("Failed to write to log file: %s. File logging disabled for %s seconds.",
        (error instanceof Error) ? error.message : String(error), ERROR_RETRY_DELAY_MS / 1000);
    }
  });
}

/**
 * Flushes the write buffer to disk synchronously. Shutdown reaches it with the logger initialized and writes to the configured log file, and the process exit
 * handler reaches it on either side of initialization. The fallback path serves that second position - an exit before initialization, where the buffer holds
 * the startup window's entries and no log file was ever opened - and an initialized logger ignores it in favor of its own file.
 * @param fallbackPath - Absolute path to write to when the logger never initialized. Shutdown omits it, and so does console mode, whose lines never enter the buffer.
 */
export function flushLogBufferSync(fallbackPath?: string): void {

  if(writeBuffer.length === 0) {

    return;
  }

  // Before initialization the fallback path is the only thing that says where the startup window's entries should land, and without one there is nowhere to
  // put them.
  const isWritingToFile = isInitialized && (logFilePath !== null);
  const targetPath = isWritingToFile ? logFilePath : fallbackPath;

  if(!targetPath) {

    return;
  }

  const content = writeBuffer.join("");

  writeBuffer = [];

  try {

    /* Only the fallback needs its parent directory created, and it shares the catch below so a read-only data directory yields the same console message a
     * failed append does. An initialized logger created its directory at initialization, so repeating the syscall on its shutdown path would buy nothing.
     */
    if(!isWritingToFile) {

      fs.mkdirSync(path.dirname(targetPath), { recursive: true });
    }

    fs.appendFileSync(targetPath, content, "utf-8");
  } catch(error) {

    // Log to console as fallback.
    // eslint-disable-next-line no-console
    console.error("Failed to write final log entries: %s.", (error instanceof Error) ? error.message : String(error));
  }
}

// Size Management.

/**
 * Checks the actual file size and trims if it exceeds the maximum.
 */
async function checkAndTrimFile(): Promise<void> {

  if(!logFilePath) {

    return;
  }

  try {

    const stats = await fsPromises.stat(logFilePath);

    approximateSize = stats.size;

    // Skip trimming when debug logging is active. Debug sessions generate high-volume output that is valuable for diagnosis - trimming mid-session would discard
    // the very data we are trying to capture.
    if((approximateSize > maxLogSize) && !isAnyDebugEnabled()) {

      await trimLogFile();
    }
  } catch(error) {

    // File might have been deleted externally - reset size tracking.
    if((error as NodeJS.ErrnoException).code === "ENOENT") {

      approximateSize = 0;
    }

    // Log to console but continue operating.
    // eslint-disable-next-line no-console
    console.warn("Error checking log file size: %s.", (error instanceof Error) ? error.message : String(error));
  }
}

/**
 * Pure trim logic: given the current log file content and the configured maximum size, returns the trimmed content (keeping complete lines from the file's tail)
 * or null when no trim is needed (file is already at or below half maxSize). Extracted from trimLogFile so the cut-at-newline algorithm is testable in isolation
 * without orchestrating real filesystem I/O - the surrounding read/write/rename chain is small enough to be exercised at the integration level.
 *
 * Algorithm: target half of maxSize as the post-trim file size, cut from (content.length - targetSize), then advance to the next newline so the trimmed file
 * begins on a complete line. If no newline exists past the cut, the cut position is taken as-is (the trim still drops earlier content even if the kept tail is
 * a single fragment line).
 *
 * @param content - The current log file content as a UTF-8 string.
 * @param maxSize - The configured maximum log file size in bytes.
 * @returns The trimmed content, or null when no trim is needed.
 */
export function computeTrimmedLogContent(content: string, maxSize: number): Nullable<string> {

  // Calculate target size (half of max).
  const targetSize = Math.floor(maxSize / 2);

  // We want to keep the END of the file (most recent logs). Find where to cut.
  const cutPosition = content.length - targetSize;

  if(cutPosition <= 0) {

    // File is smaller than target, no trimming needed.
    return null;
  }

  // Find the next newline after the cut position to keep complete lines.
  let lineStart = content.indexOf("\n", cutPosition);

  if(lineStart === -1) {

    // No newline found after cut position, keep from cut position.
    lineStart = cutPosition;
  } else {

    // Start after the newline.
    lineStart += 1;
  }

  return content.substring(lineStart);
}

/**
 * Trims the log file to half the maximum size, keeping only complete lines. The most recent logs are preserved. Pure cut logic lives in
 * computeTrimmedLogContent; this function is the I/O orchestration shell around it.
 *
 * The entire read/write/rename critical section runs inside the write-ordering chain so it is serialized against buffer flushes. This closes the trim/flush race:
 * an appendFile cannot land between this function's readFile and its rename, so the rename never overwrites freshly appended lines with a stale snapshot.
 */
async function trimLogFile(): Promise<void> {

  if(!logFilePath) {

    return;
  }

  // Snapshot the path so a concurrent shutdown clearing logFilePath cannot redirect the rename target mid-operation.
  const targetPath = logFilePath;

  return serializeWrite(async (): Promise<void> => {

    try {

      const content = await fsPromises.readFile(targetPath, "utf-8");
      const trimmedContent = computeTrimmedLogContent(content, maxLogSize);

      if(trimmedContent === null) {

        return;
      }

      // Write to temp file, then rename (atomic replace).
      const tempPath = targetPath + ".tmp";

      await fsPromises.writeFile(tempPath, trimmedContent, "utf-8");
      await fsPromises.rename(tempPath, targetPath);

      approximateSize = trimmedContent.length;
    } catch(error) {

      // Log to console but continue operating - trim will be retried on next check.
      // eslint-disable-next-line no-console
      console.warn("Error trimming log file: %s.", (error instanceof Error) ? error.message : String(error));
    }
  });
}

// Shutdown.

/**
 * Shuts down the file logger: stops the flush timer, waits for the write chain to drain, then flushes any remaining buffer synchronously and clears module state.
 *
 * The drain is what keeps shutdown from racing the writes it is meant to finish. A trim holds the log file across a read, a temp write, and a rename; a synchronous
 * final flush issued while that rename is still pending appends to the file the rename is about to replace, so the last entries of the run are discarded along with
 * the stale snapshot. Awaiting the chain first puts the final flush strictly after every mutation already in flight.
 *
 * The wait is bounded because a wedged filesystem operation must not hold the process open through its own shutdown. When the bound lapses the flush and the state
 * reset proceed anyway, which is the same outcome an unbounded wait would eventually reach minus the hang.
 *
 * The reset keeps the path of the file it closed, so a line logged after this point - the exit handler's Chrome cleanup, a late process-level handler - is
 * appended to that file synchronously instead of falling into the gap between the reset and the process exit.
 */
export async function shutdownFileLogger(): Promise<void> {

  if(!isInitialized) {

    return;
  }

  // Stop the flush timer.
  if(flushTimer) {

    clearInterval(flushTimer);
    flushTimer = null;
  }

  // Drain the outstanding write chain so the synchronous flush below lands after every mutation already in flight rather than into a file a pending rename is
  // about to replace. The chain never rejects - serializeWrite swallows each operation's outcome - so this only ever resolves or lapses.
  await boundedWait(writeChain, SHUTDOWN_DRAIN_BOUND_MS);

  // Flush remaining buffer synchronously.
  flushLogBufferSync();

  // Reset all module state so a subsequent initializeFileLogger() starts from a clean slate. The disabled-on-write-error flag in particular must not survive
  // shutdown - if a prior run hit a write failure and entered the 60-second retry window, that window should not silently drop writes from the next run.
  isInitialized = false;
  isDisabled = false;
  disabledAt = 0;
  writeBuffer = [];
  approximateSize = 0;
  writeCount = 0;

  // Keep the file this run wrote to, so a line logged from here on still reaches it rather than falling into the gap between the reset and the process exit.
  closedFilePath = logFilePath;
  logFilePath = null;

  // Reset the write-ordering chain so the next run starts from a fresh, already-resolved tail rather than chaining onto the previous run's last operation.
  writeChain = Promise.resolve();
}
