/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * morganStream.ts: Morgan logging stream adapter for PrismCast.
 */
import type { StreamOptions } from "morgan";
import { isConsoleLogging } from "./logger.ts";
import { writeLogEntry } from "./fileLogger.ts";

/* Morgan HTTP request logger needs a writable stream to output log entries. By default, Morgan writes to stdout. This adapter routes Morgan output to either the
 * console or the file logger based on the current logging mode, ensuring HTTP request logs follow the same path as application logs.
 *
 * The adapter stamps nothing itself, because both destinations stamp for it: in console mode the entry path wraps console.log to prepend a timestamp to every
 * console line in the process, and in file mode writeLogEntry() prefixes the entry it appends. A stamp added here would be the second one on the line.
 */

/**
 * Creates a Morgan stream options object that routes log output based on the logging mode. When console logging is active, the payload goes to stdout, which the
 * entry path's console wrapper stamps. When file logging is active, it goes to the file logger, which stamps the entry it appends.
 * @returns StreamOptions object for Morgan configuration.
 */
export function createMorganStream(): StreamOptions {

  return {

    write: (message: string): void => {

      // Remove trailing newline that Morgan adds since our loggers handle newlines.
      const trimmedMessage = message.trim();

      if(isConsoleLogging()) {

        // Console logging mode - the console wrapper installed at startup supplies the timestamp, so the payload goes out as it came in.
        // eslint-disable-next-line no-console
        console.log(trimmedMessage);
      } else {

        // File logging mode - route through the file logger which adds its own timestamp. HTTP request logs use the default terminal color, matching info-level output.
        writeLogEntry("info", trimmedMessage, null);
      }
    }
  };
}
