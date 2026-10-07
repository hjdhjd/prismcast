/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * logger.ts: Logging utilities with color-coded output for PrismCast.
 */
import { format, inspect, styleText } from "node:util";
import { getStreamId, resolveContextShowName } from "./streamContext.ts";
import { initDebugFilter, isAnyDebugEnabled, isCategoryEnabled } from "./debugFilter.ts";
import type { LogColor } from "./fileLogger.ts";
import type { LogEntry } from "./logEmitter.ts";
import { emitLogEntry } from "./logEmitter.ts";
import { formatTimestamp } from "./format.ts";
import { isPlainObject } from "./plainObject.ts";
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
 * optional arguments, using Node's util.format() for interpolation. The supported format specifiers are the ones FORMAT_SPECIFIER_PATTERN holds: %s (string),
 * %d (number), %i (integer), %f (floating point), %j (JSON), %o and %O (object), and %c (CSS, consumed and ignored).
 *
 * Stream context is automatically detected via AsyncLocalStorage. When running within a stream context (established by runWithStreamContext()), log messages are
 * automatically prefixed with the stream ID for correlation across concurrent streaming sessions.
 *
 * For logging outside a stream context (e.g., iterating over streams in a disconnect handler), use LOG.withStreamId() to create a bound logger.
 */

/* The format specifiers util.format() consumes an argument for. An escaped percent consumes nothing, so the count strips every escaped percent before it matches,
 * and what remains mirrors util.format()'s own consumption: a message whose arguments outnumber its specifiers leaves its trailing arguments unconsumed, which is
 * how logWithLevel tells a trailing context object from an argument a specifier renders. The pattern is global so String.prototype.match returns every
 * occurrence, and it is read only through match, because a global pattern's own exec and test carry their position from one call to the next in lastIndex.
 */
const FORMAT_SPECIFIER_PATTERN = /%[sdifjoOc]/g;

// An escaped percent. util.format() renders it as one literal percent sign, and it consumes no argument.
const ESCAPED_PERCENT = "%%";

/**
 * Counts the specifiers in a message that each consume an argument, with escaped percents removed first so "%%s" reads as a literal percent sign and an "s".
 * @param message - The format string.
 * @returns The number of argument-consuming specifiers in the message.
 */
function countFormatSpecifiers(message: string): number {

  return message.replaceAll(ESCAPED_PERCENT, "").match(FORMAT_SPECIFIER_PATTERN)?.length ?? 0;
}

/**
 * Formats the sentence of a line whose trailing context object has been set aside, from the message and the arguments that precede the context.
 * @param message - The format string.
 * @param args - The arguments that precede the context object.
 * @returns The formatted sentence, before normalization.
 */
function formatSentence(message: string, args: readonly unknown[]): string {

  if(args.length > 0) {

    return format(message, ...args);
  }

  // util.format() renders an escaped percent only when it is given an argument, and the context object is such an argument. A message that leaves no argument
  // for the sentence has no specifier, so rendering its escaped percents is all util.format() would do to the sentence if the context went through it as well.
  return message.replaceAll(ESCAPED_PERCENT, "%");
}

/* The logger commits to emitting exactly one sentence terminator on every non-debug line so callers do not have to reason about whether the format string or an
 * interpolated value carries the punctuation. This encodes the "non-debug logs are complete sentences" project rule as logger behavior rather than as per-call-site
 * discipline - a producer changing its message punctuation can no longer silently regress an interpolated log line, and the differing punctuation conventions
 * between formatError (which strips trailing punctuation) and userMessage/validator strings (which carry it) become invisible to callers. Debug stays raw because
 * debug is fragments by convention. The terminator closes the sentence, and a trailing context object follows it: a line in the house form, a complete sentence
 * with its details in a context object, ends with the object rather than with a period after it.
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
  // empty format string, so this guard exists defensively rather than to prevent a real regression.
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
 * Interpolates the format string against its arguments and normalizes non-debug messages to a single sentence terminator, then hands the result to emitFormatted
 * for prefixing, SSE emission, and output routing. A non-debug line whose last argument is a plain object that no specifier consumes carries that object as its
 * context: the sentence is formatted from the message and the other arguments and normalized on its own, and the context follows its terminator as
 * util.inspect() renders it. Debug messages skip normalization because debug output is fragments by convention.
 * @param level - The log level (error, warn, info, debug).
 * @param color - Color name accepted by node:util.styleText, or null for the default terminal color.
 * @param message - The format string.
 * @param args - Format arguments.
 * @param explicitStreamId - Optional explicit stream ID (used by withStreamId helper).
 * @param categoryTag - Optional debug category tag for category-filtered debug messages.
 */
function logWithLevel(level: LogEntry["level"], color: LogColor, message: string, args: unknown[], explicitStreamId?: string, categoryTag?: string): void {

  const context = args.at(-1);

  // A trailing plain object that the message's specifiers leave unconsumed is the line's context. On a non-debug line the terminator closes the sentence, formatted
  // from the message and the other arguments, and the context follows it as util.inspect() renders it, the rendering util.format() gives an argument it does not
  // consume. The plain-object test runs before the specifier count because it is the cheaper check and a line without a trailing object fails it at once.
  if((level !== "debug") && isPlainObject(context) && (args.length > countFormatSpecifiers(message))) {

    emitFormatted(level, color, normalizeSentence(formatSentence(message, args.slice(0, -1))) + " " + inspect(context), explicitStreamId, categoryTag);

    return;
  }

  const formatted = args.length > 0 ? format(message, ...args) : message;

  // Non-debug levels are guaranteed sentence-terminated; debug stays raw because debug is fragments by convention. The contract lives here, not at the call site.
  emitFormatted(level, color, (level === "debug") ? formatted : normalizeSentence(formatted), explicitStreamId, categoryTag);
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

    // An explicit stream ID means this call came through the withStreamId bound logger, which runs outside any AsyncLocalStorage stream context. Skip the
    // ambient show-name resolver in that case so a bound logger for one stream can never surface another stream's show name from whatever context happens to
    // be active when the call is made.
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
   * @param message - The format string (supports %s, %d, %i, %f, %j, %o, %O, %c).
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
   * @param message - The format string (supports %s, %d, %i, %f, %j, %o, %O, %c).
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
   * @param message - The format string (supports %s, %d, %i, %f, %j, %o, %O, %c).
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
   * @param message - The format string (supports %s, %d, %i, %f, %j, %o, %O, %c).
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
