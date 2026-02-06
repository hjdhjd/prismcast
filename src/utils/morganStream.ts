/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * morganStream.ts: Morgan logging stream adapter for PrismCast.
 */
import type { StreamOptions } from "morgan";
import df from "dateformat";
import { isConsoleLogging } from "./logger.js";
import { writeLogEntry } from "./fileLogger.js";

/* Morgan HTTP request logger needs a writable stream to output log entries. By default, Morgan writes to stdout. This adapter routes Morgan output to either the
 * console or the file logger based on the current logging mode, ensuring HTTP request logs follow the same path as application logs.
 *
 * Timestamps are added here rather than in the Morgan format string to ensure consistency: the file logger adds timestamps via writeLogEntry(), so we add them
 * manually for console mode to match.
 */

/**
 * Creates a Morgan stream options object that routes log output based on the logging mode. When console logging is active, output goes to stdout with a timestamp
 * prefix. When file logging is active, output goes to the file logger which adds its own timestamp.
 * @returns StreamOptions object for Morgan configuration.
 */
export function createMorganStream(): StreamOptions {

  return {

    write: (message: string): void => {

      // Remove trailing newline that Morgan adds since our loggers handle newlines.
      const trimmedMessage = message.trim();

      if(isConsoleLogging()) {

        // Console logging mode - add timestamp prefix and write to stdout.
        const timestamp = df(new Date(), "yyyy/mm/dd HH:MM:ss.l");

        // eslint-disable-next-line no-console
        console.log([ "[", timestamp, "] ", trimmedMessage ].join(""));
      } else {

        // File logging mode - route through the file logger which adds its own timestamp.
        writeLogEntry("info", trimmedMessage);
      }
    }
  };
}
