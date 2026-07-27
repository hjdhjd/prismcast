/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * loggers.helpers.ts: Logger doubles passed in place of the production LOG wherever a consumer accepts a logger parameter. silentLog drops every call (used
 * when production code logs but the test isn't asserting on output); capturingLog records every call (used when the test does want to assert on log output).
 * Each satisfies the TestLogger interface, which mirrors the public surface of LOG from src/utils/logger.ts.
 */

/**
 * The minimal logger surface that every test consumer needs. Mirrors the public methods on LOG from src/utils/logger.ts (debug/error/info/warn) plus the bound
 * logger returned by withStreamId. Helpers in this file return an object that satisfies this shape so it can be passed in place of LOG wherever a consumer
 * accepts a logger parameter.
 */
export interface TestLogger {

  debug: (category: string, message: string, ...args: unknown[]) => void;
  error: (message: string, ...args: unknown[]) => void;
  info: (message: string, ...args: unknown[]) => void;
  warn: (message: string, ...args: unknown[]) => void;
  withStreamId: (streamId: string) => Omit<TestLogger, "withStreamId">;
}

/**
 * A captured log line. Mirrors LogEntry from src/utils/logEmitter.ts loosely - we keep our own shape so tests can match against fields without coupling to the
 * production type's evolution.
 */
export interface CapturedLogLine {

  args: unknown[];
  category?: string;
  level: "debug" | "error" | "info" | "warn";
  message: string;
  streamId?: string;
}

/**
 * No-op used as the implementation behind every silentLog method. It satisfies @typescript-eslint/no-empty-function because the body carries an explanatory
 * comment, which is the rule's standard escape hatch for a deliberately empty function. The same reference is shared by every method so silentLog stays cheap to
 * instantiate per test. This parallels the canonical noop exported from fn.helpers.ts; we keep a local copy here so the logger doubles carry no cross-helper
 * import and the shared-reference shape lives next to the silentLog/silentBound consumers that depend on it.
 */
function noop(): void {

  // Intentional no-op: silentLog drops every call.
}

const silentBound: Omit<TestLogger, "withStreamId"> = {

  debug: noop,
  error: noop,
  info: noop,
  warn: noop
};

/**
 * Returns a logger that drops every call. Use this when a test exercises code that logs but the test isn't asserting on log output - the production code wants
 * to call LOG.info(...) and the test just needs the call to be a no-op so it doesn't pollute test runner stdout. Returned object satisfies TestLogger so it can
 * be passed in place of the production LOG wherever a consumer accepts a logger parameter.
 * @returns A logger whose methods all return undefined and record nothing.
 */
export function silentLog(): TestLogger {

  return {

    debug: noop,
    error: noop,
    info: noop,
    warn: noop,
    withStreamId: () => silentBound
  };
}

/**
 * Returns a logger that records every call into an array, plus a snapshot accessor. Use this when a test does want to assert on log output - either to verify
 * that a particular warning fired, or to verify that nothing leaked at error level. The returned object also exposes a clear() so tests can reset between
 * phases without re-instantiating.
 * @returns An object with a logger surface and lines/clear accessors.
 */
export function capturingLog(): { clear: () => void; lines: () => CapturedLogLine[]; logger: TestLogger } {

  const captured: CapturedLogLine[] = [];

  function recordTopLevel(level: CapturedLogLine["level"]): (message: string, ...args: unknown[]) => void {

    return function(message: string, ...args: unknown[]): void {

      captured.push({ args, level, message });
    };
  }

  function recordDebug(category: string, message: string, ...args: unknown[]): void {

    // The production LOG.debug signature accepts a category as its first parameter; the recording reflects that shape so consumers can assert on it.
    captured.push({ args, category, level: "debug", message });
  }

  function recordBound(streamId: string, level: CapturedLogLine["level"]): (message: string, ...args: unknown[]) => void {

    return function(message: string, ...args: unknown[]): void {

      captured.push({ args, level, message, streamId });
    };
  }

  function recordBoundDebug(streamId: string): (category: string, message: string, ...args: unknown[]) => void {

    return function(category: string, message: string, ...args: unknown[]): void {

      captured.push({ args, category, level: "debug", message, streamId });
    };
  }

  const logger: TestLogger = {

    debug: recordDebug,
    error: recordTopLevel("error"),
    info: recordTopLevel("info"),
    warn: recordTopLevel("warn"),
    withStreamId: (streamId: string) => ({

      debug: recordBoundDebug(streamId),
      error: recordBound(streamId, "error"),
      info: recordBound(streamId, "info"),
      warn: recordBound(streamId, "warn")
    })
  };

  return {

    clear: (): void => {

      captured.length = 0;
    },
    lines: (): CapturedLogLine[] => captured.slice(),
    logger
  };
}
