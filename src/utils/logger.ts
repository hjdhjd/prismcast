/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * logger.ts: Logging utilities with color-coded output for PrismCast.
 */
import { format, styleText } from "node:util";
import { getStreamId, resolveContextShowName } from "./streamContext.ts";
import { initDebugFilter, isAnyDebugEnabled, isCategoryEnabled } from "./debugFilter.ts";
import type { LogColor } from "./fileLogger.ts";
import type { LogEntry } from "./logEmitter.ts";
import { emitLogEntry } from "./logEmitter.ts";
import { formatTimestamp } from "./format.ts";
import { writeLogEntry } from "./fileLogger.ts";

/* Terminal color choices for log output. Warnings appear in yellow and errors in red, making it easy to spot issues when scanning log output. Coloring is delegated
 * to node:util.styleText so the SGR sequences are managed by the platform rather than hand-written escape codes.
 */

/* The logger can operate in two modes: console mode (output to stdout/stderr with colors) or file mode (output to the configured log file). By default, file mode
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

/* The logger commits to emitting exactly one sentence terminator on every non-debug line so callers do not have to reason about whether the format string or an
 * interpolated value carries the punctuation. This encodes the "non-debug logs are complete sentences" project rule as logger behavior rather than as per-call-site
 * discipline - a producer changing its message punctuation can no longer silently regress an interpolated log line, and the historical split between formatError
 * (which strips trailing punctuation) and userMessage/validator strings (which carry it) becomes invisible to callers. Debug stays raw because debug is fragments
 * by convention.
 */

/**
 * Normalizes a non-debug log message to exactly one terminal sentence terminator. Runs of trailing periods collapse to a single period (the double-period
 * regression class); existing "?" and "!" terminators pass through unchanged because they are producer-intentional and have no run-collision class to defend
 * against; messages without any terminator gain a period.
 * @param message - The composed message body.
 * @returns The message with a single, well-formed terminator.
 */
function normalizeSentence(message: string): string {

  // Empty input stays empty. Forcing a bare "." into a zero-length message would be a worse outcome than leaving it - and in practice no caller passes an
  // empty format string, so this guard is defensive rather than load-bearing.
  if(!message) {

    return message;
  }

  // Collapse runs of trailing periods to one. We only collapse periods because they are the only terminator that the format-string + value composition can
  // double up (a value ending in "." plus a format string ending in "." yields ".."); "?" and "!" never compose the same way.
  const collapsed = message.replace(/\.+$/, ".");

  // If the collapsed message ends with any terminator now, keep it; otherwise append a period so the line is a complete sentence.
  return (/[.?!]$/).test(collapsed) ? collapsed : collapsed + ".";
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
 * @param color - Color name accepted by node:util.styleText, or null for the default terminal color.
 * @param message - The format string.
 * @param args - Format arguments.
 * @param explicitStreamId - Optional explicit stream ID (used by withStreamId helper).
 * @param categoryTag - Optional debug category tag for category-filtered debug messages.
 */
function logWithLevel(level: LogEntry["level"], color: LogColor, message: string, args: unknown[], explicitStreamId?: string, categoryTag?: string): void {

  const rawFormatted = args.length > 0 ? format(message, ...args) : message;

  // Non-debug levels are guaranteed sentence-terminated; debug stays raw because debug is fragments by convention. The contract lives here, not at the call site.
  const formatted = (level === "debug") ? rawFormatted : normalizeSentence(rawFormatted);

  emitFormatted(level, color, formatted, explicitStreamId, categoryTag);
}

/**
 * Emits an already-formatted message body through the full logger pipeline: stream-ID prefix composition, SSE subscriber emission, and console-or-file routing.
 * Shared between logWithLevel (which normalizes first) and displayLine (which deliberately bypasses normalization for tabular display). Factoring this out keeps
 * the two callers from drifting on prefix shape, SSE routing, or color handling.
 * @param level - The log level (drives console method routing and category tagging).
 * @param color - Color name for styleText, or null for the default terminal color.
 * @param formatted - The fully-prepared message body (post-normalization if applicable).
 * @param explicitStreamId - Optional explicit stream ID (used by withStreamId).
 * @param categoryTag - Optional debug category tag.
 */
function emitFormatted(level: LogEntry["level"], color: LogColor, formatted: string, explicitStreamId?: string, categoryTag?: string): void {

  const streamId = explicitStreamId ?? getStreamId();

  // Build the log prefix. Stream ID is always included when available. The show name (resolved lazily from the stream context) is appended when present,
  // giving log readers immediate context for correlating issues with DVR recordings without cross-referencing timestamps against the guide.
  let logMessage: string;

  if(streamId) {

    const showName = explicitStreamId ? "" : resolveContextShowName();
    const showPrefix = showName ? " [" + showName + "]" : "";

    logMessage = "[" + streamId + "]" + showPrefix + " " + formatted;
  } else {

    logMessage = formatted;
  }

  // SSE emission.
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

    if(color) {

      // styleText emits both the SGR opening code and the trailing reset, so callers do not need to manage the reset themselves. We disable validateStream because
      // we want colors regardless of TTY detection - downstream consumers (Docker log drivers, file viewers with -R) handle the codes correctly.
      consoleMethod(styleText(color, logMessage, { validateStream: false }));
    } else {

      consoleMethod(logMessage);
    }
  } else {

    writeLogEntry(level, logMessage, color, categoryTag);
  }
}

/**
 * Emits a non-sentence line at info level through the same SSE / file / console pipeline as LOG.info, but without the sentence-normalization contract. Use this
 * for structured display output where the line is not a prose sentence and forcing a terminal period would degrade readability - the canonical case is the
 * startup configuration dump (header + indented "label: value" rows). For ordinary log messages, use LOG.info; this function is the explicit escape hatch for
 * display-style output, named so misuse stands out at a glance during review.
 * @param message - The format string emitted verbatim (after util.format interpolation), with no terminator appended.
 * @param args - Format arguments interpolated via util.format.
 */
export function displayLine(message: string, ...args: unknown[]): void {

  const formatted = args.length > 0 ? format(message, ...args) : message;

  emitFormatted("info", null, formatted);
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

    logWithLevel("debug", "cyan", message, args, undefined, category);
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

    logWithLevel("error", "red", message, args);
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

    logWithLevel("info", null, message, args);
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

    logWithLevel("warn", "yellow", message, args);
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

          logWithLevel("debug", "cyan", message, args, streamId, category);
        }
      },
      error: (message: string, ...args: unknown[]): void => { logWithLevel("error", "red", message, args, streamId); },
      info: (message: string, ...args: unknown[]): void => { logWithLevel("info", null, message, args, streamId); },
      warn: (message: string, ...args: unknown[]): void => { logWithLevel("warn", "yellow", message, args, streamId); }
    };
  }
};
