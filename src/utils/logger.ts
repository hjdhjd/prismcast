/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * logger.ts: Logging utilities with color-coded output for PrismCast.
 */
import { initDebugFilter, isAnyDebugEnabled, isCategoryEnabled } from "./debugFilter.js";
import type { LogEntry } from "./logEmitter.js";
import { emitLogEntry } from "./logEmitter.js";
import { format } from "util";
import { getStreamId } from "./streamContext.js";
import { writeLogEntry } from "./fileLogger.js";

/* Terminal color codes for log output formatting. Warnings appear in yellow and errors in red, making it easy to spot issues when scanning log output. The reset
 * code restores the default color after each colored message to prevent color bleeding into subsequent output.
 */

const ANSI_COLORS = {

  cyan: "\x1b[36m",
  red: "\x1b[31m",
  reset: "\x1b[0m",
  yellow: "\x1b[33m"
};

/* The logger can operate in two modes: console mode (output to stdout/stderr with colors) or file mode (output to ~/.prismcast/prismcast.log). By default, file mode
 * is used. Console mode is enabled via the --console CLI flag for Docker deployments or interactive debugging.
 */

// Flag indicating whether to use console logging instead of file logging.
let useConsoleLogging = false;

/**
 * Sets the logging mode. When true, logs go to console with colors. When false, logs go to the file logger.
 * @param enabled - True to enable console logging, false for file logging.
 */
export function setConsoleLogging(enabled: boolean): void {

  useConsoleLogging = enabled;
}

/**
 * Returns whether console logging is currently enabled.
 * @returns True if using console logging, false if using file logging.
 */
export function isConsoleLogging(): boolean {

  return useConsoleLogging;
}

/* Debug logging is controlled by the category-based filter system in debugFilter.ts. The --debug CLI flag enables all categories (equivalent to
 * PRISMCAST_DEBUG=*), while the PRISMCAST_DEBUG environment variable allows fine-grained category selection.
 */

/**
 * Enables or disables debug logging. When called with true, initializes the debug filter with wildcard (*) to enable all categories.
 * @param enabled - True to enable all debug logging, false to disable.
 */
export function setDebugLogging(enabled: boolean): void {

  initDebugFilter(enabled ? "*" : "");
}

/**
 * Returns whether any debug logging is currently enabled.
 * @returns True if any debug categories are enabled, false otherwise.
 */
export function isDebugLogging(): boolean {

  return isAnyDebugEnabled();
}

/* The LOG object provides a centralized logging interface with color-coded output and printf-style format strings. All methods accept a format string followed by
 * optional arguments, using Node's util.format() for interpolation. Supported format specifiers include %s (string), %d (number), %j (JSON), and %o (object).
 *
 * Stream context is automatically detected via AsyncLocalStorage. When running within a stream context (established by runWithStreamContext()), log messages are
 * automatically prefixed with the stream ID for correlation across concurrent streaming sessions.
 *
 * For logging outside a stream context (e.g., iterating over streams in a disconnect handler), use LOG.withStreamId() to create a bound logger.
 */

/**
 * Formats the current time as a timestamp string matching the log file format.
 * @returns A timestamp string in the format "YYYY/MM/DD HH:MM:ss.lll".
 */
function formatTimestamp(): string {

  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  const hours = String(now.getHours()).padStart(2, "0");
  const minutes = String(now.getMinutes()).padStart(2, "0");
  const seconds = String(now.getSeconds()).padStart(2, "0");
  const ms = String(now.getMilliseconds()).padStart(3, "0");

  return [ year, "/", month, "/", day, " ", hours, ":", minutes, ":", seconds, ".", ms ].join("");
}

/**
 * Emits a log entry to SSE subscribers for real-time streaming.
 * @param level - The log level.
 * @param message - The formatted message.
 * @param categoryTag - Optional debug category tag for category-filtered debug messages.
 */
function emitToSubscribers(level: LogEntry["level"], message: string, categoryTag?: string): void {

  const entry: LogEntry = {

    level,
    message,
    timestamp: formatTimestamp()
  };

  if(categoryTag) {

    entry.categoryTag = categoryTag;
  }

  emitLogEntry(entry);
}

/**
 * Core logging implementation shared by all log levels. Handles stream ID prefixing, SSE emission, and output routing.
 * @param level - The log level (error, warn, info, debug).
 * @param color - ANSI color code for console output (empty string for no color).
 * @param message - The format string.
 * @param args - Format arguments.
 * @param explicitStreamId - Optional explicit stream ID (used by withStreamId helper).
 * @param categoryTag - Optional debug category tag for category-filtered debug messages.
 */
function logWithLevel(level: LogEntry["level"], color: string, message: string, args: unknown[], explicitStreamId?: string, categoryTag?: string): void {

  const streamId = explicitStreamId ?? getStreamId();
  const formatted = args.length > 0 ? format(message, ...args) : message;
  const logMessage = streamId ? [ "[", streamId, "] ", formatted ].join("") : formatted;

  // Emit to SSE subscribers for real-time streaming.
  emitToSubscribers(level, logMessage, categoryTag);

  if(useConsoleLogging) {

    /* eslint-disable no-console */
    let consoleMethod;

    switch(level) {

      case "error": {

        consoleMethod = console.error;

        break;
      }

      case "warn": {

        consoleMethod = console.warn;

        break;
      }

      default: {

        consoleMethod = console.log;

        break;
      }
    }
    /* eslint-enable no-console */

    if(streamId && color) {

      consoleMethod("%s[%s] %s%s", color, streamId, formatted, ANSI_COLORS.reset);
    } else if(streamId) {

      consoleMethod("[%s] %s", streamId, formatted);
    } else if(color) {

      consoleMethod("%s%s%s", color, formatted, ANSI_COLORS.reset);
    } else {

      consoleMethod(formatted);
    }
  } else {

    writeLogEntry(level, logMessage, color || undefined, categoryTag);
  }
}

/**
 * Bound logger interface returned by LOG.withStreamId(). Provides the same logging methods but with a fixed stream ID.
 */
interface BoundLogger {

  debug: (category: string, message: string, ...args: unknown[]) => void;
  error: (message: string, ...args: unknown[]) => void;
  info: (message: string, ...args: unknown[]) => void;
  warn: (message: string, ...args: unknown[]) => void;
}

export const LOG = {

  /**
   * Logs a debug message in cyan, filtered by category. Debug messages are only output when the specified category is enabled via the PRISMCAST_DEBUG environment
   * variable or the --debug CLI flag (which enables all categories). Use this for verbose diagnostic information that would clutter normal logs.
   *
   * Stream ID is automatically included if running within a stream context (established by runWithStreamContext()).
   * @param category - The debug category (e.g., "tuning:hulu", "recovery:tab", "streaming:segmenter").
   * @param message - The format string (supports %s, %d, %j, %o).
   * @param args - Values to interpolate into the format string.
   */
  debug: function(category: string, message: string, ...args: unknown[]): void {

    if(!isAnyDebugEnabled() || !isCategoryEnabled(category)) {

      return;
    }

    logWithLevel("debug", ANSI_COLORS.cyan, message, args, undefined, category);
  },

  /**
   * Logs an error message in red. Use this for critical failures that prevent normal operation, such as browser crashes, navigation failures after all retries, or
   * stream initialization errors. The red color provides immediate visual indication of serious problems requiring attention.
   *
   * Stream ID is automatically included if running within a stream context (established by runWithStreamContext()).
   * @param message - The format string (supports %s, %d, %j, %o).
   * @param args - Values to interpolate into the format string.
   */
  error: function(message: string, ...args: unknown[]): void {

    logWithLevel("error", ANSI_COLORS.red, message, args);
  },

  /**
   * Logs an informational message in the default terminal color. Use this for normal operational messages like startup notifications, successful operations, and
   * status updates.
   *
   * Stream ID is automatically included if running within a stream context (established by runWithStreamContext()).
   * @param message - The format string (supports %s, %d, %j, %o).
   * @param args - Values to interpolate into the format string.
   */
  info: function(message: string, ...args: unknown[]): void {

    logWithLevel("info", "", message, args);
  },

  /**
   * Logs a warning message in yellow. Use this for non-critical issues that do not prevent operation but indicate potential problems, such as timeouts that were
   * recovered from, missing optional features, or degraded functionality.
   *
   * Stream ID is automatically included if running within a stream context (established by runWithStreamContext()).
   * @param message - The format string (supports %s, %d, %j, %o).
   * @param args - Values to interpolate into the format string.
   */
  warn: function(message: string, ...args: unknown[]): void {

    logWithLevel("warn", ANSI_COLORS.yellow, message, args);
  },

  /**
   * Creates a bound logger with a fixed stream ID. Use this when logging about a stream from outside its async context, such as when iterating over multiple streams
   * in a disconnect handler or cleanup routine.
   *
   * Example:
   *   const streamLog = LOG.withStreamId(streamId);
   *   streamLog.warn("Terminating stream due to browser disconnect.");
   *   streamLog.info("Cleanup complete.");
   *
   * @param streamId - The stream ID to include in all log messages.
   * @returns A logger object with debug, error, warn, and info methods that include the specified stream ID.
   */
  withStreamId: function(streamId: string): BoundLogger {

    return {

      debug: (category: string, message: string, ...args: unknown[]): void => {

        if(isAnyDebugEnabled() && isCategoryEnabled(category)) {

          logWithLevel("debug", ANSI_COLORS.cyan, message, args, streamId, category);
        }
      },
      error: (message: string, ...args: unknown[]): void => { logWithLevel("error", ANSI_COLORS.red, message, args, streamId); },
      info: (message: string, ...args: unknown[]): void => { logWithLevel("info", "", message, args, streamId); },
      warn: (message: string, ...args: unknown[]): void => { logWithLevel("warn", ANSI_COLORS.yellow, message, args, streamId); }
    };
  }
};
