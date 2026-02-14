/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fileLogger.ts: File-based logging with automatic size-based rotation for PrismCast.
 */
import type { Nullable } from "../types/index.js";
import df from "dateformat";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const { promises: fsPromises } = fs;

/* The file logger provides persistent logging to ~/.prismcast/prismcast.log with automatic size-based trimming. When the log file exceeds the configured maximum
 * size, it is trimmed to half the maximum size, keeping only complete lines (the most recent logs are preserved). This approach prevents unbounded log growth while
 * maintaining recent history for troubleshooting.
 *
 * Design decisions:
 *
 * 1. Asynchronous buffered writes - Logs are collected in a buffer and flushed periodically to avoid blocking the event loop during high-frequency logging.
 * 2. Periodic size checking - File size is checked every N writes rather than on each write to minimize syscall overhead.
 * 3. Atomic trim operations - Trimming writes to a temp file then renames, preventing data loss if the process crashes during trim.
 * 4. Timestamps - Uses the same format as console-stamp for consistency: yyyy/mm/dd HH:MM:ss.l
 */

/* The file logger maintains state for the log file path, write buffer, and size tracking. State is initialized when initializeFileLogger() is called during server
 * startup.
 */

// Path to the log file, set during initialization.
let logFilePath: Nullable<string> = null;

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

/* Terminal color codes for log file output. These match the colors used in console mode so that viewing the log file with terminal commands (tail -f, less -R, cat)
 * shows the same color scheme as console output.
 */

const ANSI_RESET = "\x1b[0m";

// Initialization.

/**
 * Initializes the file logger. Creates the log file if it does not exist. Must be called after the data directory is ensured to exist.
 * @param maxSize - Maximum log file size in bytes from CONFIG.logging.maxSize.
 */
export async function initializeFileLogger(maxSize: number): Promise<void> {

  const dataDir = path.join(os.homedir(), ".prismcast");

  logFilePath = path.join(dataDir, "prismcast.log");
  maxLogSize = maxSize;

  try {

    // Ensure the data directory exists.
    await fsPromises.mkdir(dataDir, { recursive: true });

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

    // Start the periodic flush timer.
    flushTimer = setInterval((): void => {

      void flushLogBuffer();
    }, FLUSH_INTERVAL_MS);

    isInitialized = true;
  } catch(error) {

    // Log to console since file logging failed, but do not throw - file logging is a best-effort feature.
    // eslint-disable-next-line no-console
    console.error("Failed to initialize file logger: %s. File logging disabled.", (error instanceof Error) ? error.message : String(error));
  }
}

// Log Entry Writing.

/**
 * Writes a log entry to the buffer. Entries are flushed to disk periodically.
 * @param level - Log level ("info", "warn", "error", "debug").
 * @param message - The formatted log message.
 * @param color - Optional ANSI color code to apply to the level prefix and message.
 * @param categoryTag - Optional debug category tag (e.g., "recovery:tab"). Appended to the level prefix as [DEBUG:category].
 */
export function writeLogEntry(level: string, message: string, color?: string, categoryTag?: string): void {

  if(!isInitialized || !logFilePath) {

    return;
  }

  // Check if logging is disabled due to previous error and whether retry delay has passed.
  if(isDisabled) {

    if((Date.now() - disabledAt) < ERROR_RETRY_DELAY_MS) {

      return;
    }

    // Re-enable logging and try again.
    isDisabled = false;
  }

  // Format the log entry with timestamp and level. Apply ANSI color if provided.
  const timestamp = df(new Date(), "yyyy/mm/dd HH:MM:ss.l");
  const levelTag = categoryTag ? [ level.toUpperCase(), ":", categoryTag ].join("") : level.toUpperCase();
  const levelPrefix = (level === "info") ? "" : [ "[", levelTag, "] " ].join("");
  const colorStart = color ?? "";
  const colorEnd = color ? ANSI_RESET : "";
  const entry = [ "[", timestamp, "] ", colorStart, levelPrefix, message, colorEnd, "\n" ].join("");

  // Add to buffer.
  writeBuffer.push(entry);
  approximateSize += entry.length;
  writeCount++;

  // Check if we should verify actual file size.
  if((writeCount % SIZE_CHECK_FREQUENCY) === 0) {

    void checkAndTrimFile();
  }
}

// Buffer Flushing.

/**
 * Flushes the write buffer to disk asynchronously. Called periodically by the flush timer.
 */
export async function flushLogBuffer(): Promise<void> {

  if(!isInitialized || !logFilePath || (writeBuffer.length === 0)) {

    return;
  }

  // Take the current buffer and reset.
  const entries = writeBuffer;

  writeBuffer = [];

  const content = entries.join("");

  try {

    await fsPromises.appendFile(logFilePath, content, "utf-8");
  } catch(error) {

    // Disable logging temporarily to prevent error cascade.
    isDisabled = true;
    disabledAt = Date.now();

    // Log to console as fallback.
    // eslint-disable-next-line no-console
    console.error("Failed to write to log file: %s. File logging disabled for %s seconds.",
      (error instanceof Error) ? error.message : String(error), ERROR_RETRY_DELAY_MS / 1000);
  }
}

/**
 * Flushes the write buffer to disk synchronously. Used during shutdown to ensure final logs are written.
 */
export function flushLogBufferSync(): void {

  if(!isInitialized || !logFilePath || (writeBuffer.length === 0)) {

    return;
  }

  const content = writeBuffer.join("");

  writeBuffer = [];

  try {

    fs.appendFileSync(logFilePath, content, "utf-8");
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

    if(approximateSize > maxLogSize) {

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
 * Trims the log file to half the maximum size, keeping only complete lines. The most recent logs are preserved.
 */
async function trimLogFile(): Promise<void> {

  if(!logFilePath) {

    return;
  }

  try {

    const content = await fsPromises.readFile(logFilePath, "utf-8");

    // Calculate target size (half of max).
    const targetSize = Math.floor(maxLogSize / 2);

    // We want to keep the END of the file (most recent logs). Find where to cut.
    const cutPosition = content.length - targetSize;

    if(cutPosition <= 0) {

      // File is smaller than target, no trimming needed.
      return;
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

    const trimmedContent = content.substring(lineStart);

    // Write to temp file, then rename (atomic replace).
    const tempPath = logFilePath + ".tmp";

    await fsPromises.writeFile(tempPath, trimmedContent, "utf-8");
    await fsPromises.rename(tempPath, logFilePath);

    approximateSize = trimmedContent.length;
  } catch(error) {

    // Log to console but continue operating - trim will be retried on next check.
    // eslint-disable-next-line no-console
    console.warn("Error trimming log file: %s.", (error instanceof Error) ? error.message : String(error));
  }
}

// Shutdown.

/**
 * Shuts down the file logger, flushing any remaining buffer synchronously.
 */
export function shutdownFileLogger(): void {

  if(!isInitialized) {

    return;
  }

  // Stop the flush timer.
  if(flushTimer) {

    clearInterval(flushTimer);
    flushTimer = null;
  }

  // Flush remaining buffer synchronously.
  flushLogBufferSync();

  isInitialized = false;
}
