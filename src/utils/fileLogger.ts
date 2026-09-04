/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fileLogger.ts: File-based logging with automatic size-based rotation for PrismCast.
 */
import type { Nullable } from "../types/index.ts";
import { assertNever } from "./never.ts";
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

/* Where the logger is in its lifecycle, which is the one thing that says where a line goes. The logger occupies exactly one of these states at a time, and each
 * arm carries only what that state needs, so a combination that cannot happen cannot be written down either.
 *
 * window - Process start, before the one initialization. Entries are held in the write buffer for the first flush after that initialization to append, because
 *   they are the boot messages that explain how the run began.
 * open - The initialization succeeded and the file is the logger's to write. The timer flushes the buffer periodically, and pausedSince is set while the file is
 *   refusing writes: a flush failure sets or refreshes it, entries are dropped while it is set and the retry delay has not elapsed, and the first write past the
 *   delay clears it. The pause is what keeps a failing disk from turning every periodic flush into another failed syscall.
 * closing - Shutdown, while the drain runs. The timer is stopped and any pause is lifted, and entries are buffered for the final flush: that flush is the run's
 *   one last attempt at the file, and a pause bounds a cascade of periodic retries, which a single final flush cannot start.
 * closed - After shutdown. A line logged from here is appended synchronously to the file the run wrote, rather than falling into the gap between the shutdown
 *   and the process exit.
 * off - No file can take a line: after an initialization that failed, after a post-shutdown append that failed, and inside a later initialization's own awaits,
 *   where the previous run's closed file has been superseded and this run's file is not open yet.
 *
 * An initialization moves every state but the window to off before its first await and then enters open or off; the window is the one state that keeps holding
 * lines across an initialization. Every assignment goes through transition(), which disposes a flush timer the next state does not carry.
 */
type LoggerState =
  { readonly kind: "window" } |
  { readonly kind: "open"; readonly path: string; readonly pausedSince: Nullable<number>; readonly timer: ReturnType<typeof setInterval> } |
  { readonly kind: "closing"; readonly path: string } |
  { readonly kind: "closed"; readonly path: string } |
  { readonly kind: "off" };

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
 *    worth logging. Entries emitted in the window state, before the one initialization, are held in the write buffer and appended by the first flush after it,
 *    so the log file opens with the boot messages that preceded it. The window is bounded, and the initialization leaves it whether it succeeds or fails.
 */

/* The file logger's module state: the lifecycle state that says where a line goes, plus the bookkeeping of the file it is open on. Every one of them is set up
 * by the initializeFileLogger() call during server startup.
 */

// Where the logger is in its lifecycle. Only transition() assigns it, which is what gives the flush timer a single owner.
let state: LoggerState = { kind: "window" };

/* Buffer collecting log entries between flushes. It is the open file's bookkeeping rather than a member of the open state: the write path pushes to it once per
 * line, and rebuilding a state object per line would allocate on every log call.
 */
let writeBuffer: string[] = [];

// Writes since the last file size check, beside the state for the same reason the buffer is - the write path bumps it once per line.
let writeCount = 0;

/* Tail of the write-ordering chain, the open file's bookkeeping rather than a member of its state. Every asynchronous mutation of the log file (buffer flushes
 * and trims) is appended to this promise so that the operations run strictly one after another. Without this serialization a flush's appendFile could land
 * between a trim's readFile and its rename, and the rename would then overwrite the just-appended lines with stale trimmed content - silently dropping log
 * entries. Routing both paths through a single ordering primitive makes "read-modify-rename" and "append" mutually exclusive without an explicit lock. The tail
 * never rejects: the helper that enqueues work swallows the settled outcome before chaining the next operation, so one failing operation cannot poison the chain
 * for subsequent writes.
 */
let writeChain: Promise<void> = Promise.resolve();

// How long shutdown waits for the outstanding write chain to drain. Generous enough for a trim's read, write, and rename on a busy disk, and short enough that a
// wedged filesystem operation cannot hold the process open through its own shutdown.
const SHUTDOWN_DRAIN_BOUND_MS = 5000;

// Maximum log file size, set during initialization. It is configuration the trim reads rather than lifecycle, so it stays beside the state.
let maxLogSize = 1048576;

// Configuration Constants.

// Interval in milliseconds between buffer flushes.
const FLUSH_INTERVAL_MS = 1000;

// Number of writes between file size checks.
const SIZE_CHECK_FREQUENCY = 100;

// Duration in milliseconds a paused file drops entries for before the next write retries it.
const ERROR_RETRY_DELAY_MS = 60000;

// The most entries the startup window holds. A push past the limit drops the oldest entry, because the newest lines are the ones that explain an exit.
const STARTUP_BUFFER_LIMIT = 1000;

// Lifecycle State.

/**
 * Moves the logger to its next state. Every transition goes through here, and this is where the flush timer's lifetime is decided: a timer belongs to the
 * states that carry one, so leaving them for a state that does not carry the same timer stops it, whichever transition is leaving.
 * @param next - The state the logger enters.
 */
function transition(next: LoggerState): void {

  if(("timer" in state) && (!("timer" in next) || (next.timer !== state.timer))) {

    clearInterval(state.timer);
  }

  state = next;
}

/**
 * The path of the file the logger writes through its buffer, while it has one: the open and closing states. The window has no file yet, the off state has
 * none, and the closed state keeps its path for the synchronous append in writeLogEntry rather than for the buffered paths this reader serves.
 * @returns The path, or null when no file is open.
 */
function openFilePath(): Nullable<string> {

  switch(state.kind) {

    case "open":
    case "closing": {

      return state.path;
    }

    case "window":
    case "closed":
    case "off": {

      return null;
    }

    default: {

      return assertNever(state);
    }
  }
}

// Initialization.

/**
 * Initializes the file logger. Creates the log file if it does not exist. Must be called after the data directory is ensured to exist. A call is the one
 * initialization of the process: it enters open, leaving the window's entries in the buffer for the first flush to append, or off, discarding them. Either
 * outcome leaves the startup window behind.
 * @param logPath - Absolute path to the log file, resolved by the caller via getLogFilePath().
 * @param maxSize - Maximum log file size in bytes from CONFIG.logging.maxSize.
 */
export async function initializeFileLogger(logPath: string, maxSize: number): Promise<void> {

  /* This runs before the first await below, because the states it leaves must not survive into that gap. A later initialization supersedes the file the last
   * run closed, so a line landing inside these awaits is dropped rather than appended to a file this run has moved on from; a timer a still-open logger carried
   * is stopped by the chokepoint on the way through; and the window is the one state left alone, because the lines it holds are the boot messages the first
   * flush appends.
   */
  if(state.kind !== "window") {

    transition({ kind: "off" });
  }

  maxLogSize = maxSize;

  try {

    // Ensure the parent directory of the log file exists.
    await fsPromises.mkdir(path.dirname(logPath), { recursive: true });

    // The stat asks one question - does the file exist - and its result is not needed beyond that, because an existing file is appended to as it stands and
    // the periodic check reads the size from the disk when it needs one.
    try {

      await fsPromises.stat(logPath);
    } catch(error) {

      // File does not exist, create it.
      if((error as NodeJS.ErrnoException).code === "ENOENT") {

        await fsPromises.writeFile(logPath, "", "utf-8");
      } else {

        throw error;
      }
    }

    // The timer belongs to the state that holds the file, so it is created with that state and disposed by the chokepoint when the logger leaves it.
    const timer = setInterval((): void => {

      void flushLogBuffer();
    }, FLUSH_INTERVAL_MS);

    transition({ kind: "open", path: logPath, pausedSince: null, timer });
  } catch(error) {

    // A failed initialization discards what the window held and leaves the logger where no state takes a line: no file will ever take those entries, and
    // holding them would grow the buffer for the life of the process.
    writeBuffer = [];
    transition({ kind: "off" });

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

  switch(state.kind) {

    case "window": {

      writeBuffer.push(formatLogEntry(level, message, color, categoryTag));

      /* The window has no open file, so it bounds itself where the open state runs a periodic size check instead. The bound is tested on every push, so the
       * oldest entry makes room as soon as one entry too many arrives.
       */
      if(writeBuffer.length > STARTUP_BUFFER_LIMIT) {

        writeBuffer.shift();
      }

      return;
    }

    case "open":
    case "closing": {

      if((state.kind === "open") && (state.pausedSince !== null)) {

        if((Date.now() - state.pausedSince) < ERROR_RETRY_DELAY_MS) {

          return;
        }

        /* The first write past the retry delay re-opens the file, and the next failure on it sets a fresh pause. Clearing the field here is what keeps it
         * describing the file's standing rather than a moment that has passed, and it has no observable of its own, because every failure on the file the
         * logger holds open refreshes the timestamp.
         */
        transition({ ...state, pausedSince: null });
      }

      writeBuffer.push(formatLogEntry(level, message, color, categoryTag));
      writeCount++;

      // Check if we should verify actual file size.
      if((writeCount % SIZE_CHECK_FREQUENCY) === 0) {

        void checkAndTrimFile();
      }

      return;
    }

    case "closed": {

      /* The logger has shut down and this line still belongs to the run that just ended, so it goes to that run's file. The append is synchronous because
       * nothing asynchronous is left to carry it: the flush timer is stopped, the write chain has drained, and the process may exit on the next tick.
       */
      try {

        fs.appendFileSync(state.path, formatLogEntry(level, message, color, categoryTag), "utf-8");
      } catch(error) {

        // eslint-disable-next-line no-console
        console.error("Failed to write a log entry after shutdown: %s.", (error instanceof Error) ? error.message : String(error));

        // A closed file that cannot be written is where the logger goes off, so the path is reported once rather than once per line.
        transition({ kind: "off" });
      }

      return;
    }

    case "off": {

      // Nothing takes the entry: no file is open, no closed file is waiting for it, and the startup window is behind us.
      return;
    }

    default: {

      assertNever(state);
    }
  }
}

// Write Ordering.

/**
 * Serializes an asynchronous log-file mutation against every other such mutation. The supplied operation is appended to the module-scoped write chain so that
 * flushes and trims execute one at a time, never interleaving their read/append/rename steps. The returned promise resolves when this specific operation has
 * settled. The chain itself is advanced with the settled (caught) result so a rejecting operation cannot poison subsequent writes - each operation is responsible
 * for its own error handling.
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

  // The path is read once here so the serialized operation below writes to the file the logger held when the flush was issued, whatever the state does next.
  const targetPath = openFilePath();

  if((targetPath === null) || (writeBuffer.length === 0)) {

    return;
  }

  // Take the current buffer and reset.
  const entries = writeBuffer;

  writeBuffer = [];

  const content = entries.join("");

  return serializeWrite(async (): Promise<void> => {

    try {

      await fsPromises.appendFile(targetPath, content, "utf-8");
    } catch(error) {

      /* Every failure on the file the logger still holds open sets or refreshes the pause, so the delay before the next attempt runs from the latest failure
       * rather than the first. A failure that settles while the logger is closing that file, or has already left it, touches no state: the final flush is the
       * run's one attempt either way, and the drain bound lets a shutdown complete with an append still in flight, so a run that has since opened its own file
       * must not inherit a pause the run before it earned.
       */
      if((state.kind === "open") && (state.path === targetPath)) {

        transition({ ...state, pausedSince: Date.now() });

        // Log to console as fallback.
        // eslint-disable-next-line no-console
        console.error("Failed to write to log file: %s. File logging disabled for %s seconds.",
          (error instanceof Error) ? error.message : String(error), ERROR_RETRY_DELAY_MS / 1000);
      } else {

        // Log to console as fallback.
        // eslint-disable-next-line no-console
        console.error("Failed to write to the log file: %s.", (error instanceof Error) ? error.message : String(error));
      }
    }
  });
}

/**
 * Flushes the write buffer to disk synchronously. Shutdown reaches it while the logger is closing and writes to the configured log file, and the process exit
 * handler reaches it on either side of initialization. The fallback path serves that second position - an exit in the startup window, where the buffer holds
 * the window's entries and no log file was ever opened - and a logger holding a file of its own ignores it in favor of that file.
 * @param fallbackPath - Absolute path to write to when the logger never initialized. Shutdown omits it, and so does console mode, whose lines never enter the buffer.
 */
export function flushLogBufferSync(fallbackPath?: string): void {

  if(writeBuffer.length === 0) {

    return;
  }

  // With no file of its own, the fallback path is the only thing that says where these entries should land, and without one there is nowhere to put them.
  const openPath = openFilePath();
  const targetPath = openPath ?? fallbackPath;

  if(!targetPath) {

    return;
  }

  const content = writeBuffer.join("");

  writeBuffer = [];

  try {

    /* Only the fallback needs its parent directory created, and it shares the catch below so a read-only data directory yields the same console message a
     * failed append does. A logger holding its own file created that directory at initialization, so repeating the syscall here would buy nothing.
     */
    if(openPath === null) {

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

  const targetPath = openFilePath();

  if(targetPath === null) {

    return;
  }

  try {

    const stats = await fsPromises.stat(targetPath);

    // Skip trimming when debug logging is active. Debug sessions generate high-volume output that is valuable for diagnosis - trimming mid-session would discard
    // the very data we are trying to capture.
    if((stats.size > maxLogSize) && !isAnyDebugEnabled()) {

      await trimLogFile();
    }
  } catch(error) {

    // A file removed externally arrives here as the stat's ENOENT, and the warning is the whole response: the next append recreates the file, so logging
    // continues into an empty one rather than stopping.
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

  // The path is read once here because a shutdown can move the state under the serialized operation below, and the rename has to land on the file the read
  // came from rather than on wherever the logger has since gone.
  const targetPath = openFilePath();

  if(targetPath === null) {

    return;
  }

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
    } catch(error) {

      // Log to console but continue operating - trim will be retried on next check.
      // eslint-disable-next-line no-console
      console.warn("Error trimming log file: %s.", (error instanceof Error) ? error.message : String(error));
    }
  });
}

// Shutdown.

/**
 * Shuts down the file logger: enters the closing state, waits for the write chain to drain, then flushes any remaining buffer synchronously and enters closed.
 *
 * The flush timer stops the moment the logger enters closing, because the chokepoint disposes a timer the next state does not carry, so no periodic flush can
 * fire into the drain.
 *
 * The drain is what keeps shutdown from racing the writes it is meant to finish. A trim holds the log file across a read, a temp write, and a rename; a synchronous
 * final flush issued while that rename is still pending appends to the file the rename is about to replace, so the last entries of the run are discarded along with
 * the stale snapshot. Awaiting the chain first puts the final flush strictly after every mutation already in flight.
 *
 * The wait is bounded because a wedged filesystem operation must not hold the process open through its own shutdown. When the bound lapses the flush and the move
 * to closed proceed anyway, which is the same outcome an unbounded wait would eventually reach minus the hang.
 *
 * The closed state keeps the path of the file it closed, so a line logged after this point - the exit handler's Chrome cleanup, a late process-level handler - is
 * appended to that file synchronously instead of falling into the gap between the shutdown and the process exit.
 */
export async function shutdownFileLogger(): Promise<void> {

  // The window, closed, and off states have no file to shut down, and returning here is what leaves a window's buffered entries for the exit handler's fallback.
  if((state.kind !== "open") && (state.kind !== "closing")) {

    return;
  }

  const targetPath = state.path;

  /* Entering closing stops the flush timer through the chokepoint, so no periodic flush fires into the drain, and it lifts any pause the open file carried: the
   * drain buffers every line logged under it and the final flush is the run's one last attempt at them, which a pause - a bound on a cascade of periodic
   * retries - has no reason to prevent.
   */
  transition({ kind: "closing", path: targetPath });

  // Drain the outstanding write chain so the synchronous flush below lands after every mutation already in flight rather than into a file a pending rename is
  // about to replace. The chain never rejects - serializeWrite swallows each operation's outcome - so this only ever resolves or lapses.
  await boundedWait(writeChain, SHUTDOWN_DRAIN_BOUND_MS);

  // Flush remaining buffer synchronously.
  flushLogBufferSync();

  writeCount = 0;
  writeBuffer = [];

  // The closed state keeps the file this run wrote to, so a line logged from here on still reaches it rather than falling into the gap between the shutdown and
  // the process exit.
  transition({ kind: "closed", path: targetPath });

  // Reset the write-ordering chain so the next run starts from a fresh, already-resolved tail rather than chaining onto the previous run's last operation.
  writeChain = Promise.resolve();
}
