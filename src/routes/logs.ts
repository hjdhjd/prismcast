/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * logs.ts: Log viewing endpoint for PrismCast.
 */
import type { Express, Request, Response } from "express";
import { isConsoleLogging, subscribeToLogs } from "../utils/index.ts";
import { CONFIG } from "../config/index.ts";
import type { Nullable } from "../types/index.ts";
import fs from "node:fs";
import { getLogFilePath } from "../config/paths.ts";
import { installSseStream } from "./sse.ts";
import { sendErrorResponse } from "./config/http/envelope.ts";

const { promises: fsPromises } = fs;

/* Log entries are parsed from the log file format: [YYYY/MM/DD HH:MM:ss.l] [LEVEL] message
 * The level prefix is present for debug, warn, and error entries; info entries have no prefix.
 */

interface LogEntry {

  categoryTag?: string;
  level: "debug" | "error" | "info" | "warn";
  message: string;
  timestamp: string;
}

interface LogsResponse {

  entries: LogEntry[];
  filtered: number;
  mode: "console" | "file";
  total: number;
}

/* The log file uses a consistent format that can be parsed with a regular expression. Each line starts with a bracketed timestamp, optionally followed by a bracketed
 * level indicator, then the message content. Log files may contain ANSI color codes for terminal viewing, which are stripped before parsing.
 */

// Pattern to match ANSI escape sequences (SGR - Select Graphic Rendition).
// eslint-disable-next-line no-control-regex
const ANSI_PATTERN = /\x1b\[[0-9;]*m/g;

// Pattern to match log entries: [timestamp] optional [LEVEL] or [LEVEL:category] message. The timestamp includes an optional AM/PM suffix to match the 12-hour
// format produced by the file logger. The category suffix handles the DEBUG:category format while remaining backward-compatible with plain [DEBUG] entries.
const LOG_LINE_PATTERN = /^\[(\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}\.\d{3}(?: [AP]M)?)\] (?:\[(WARN|ERROR|DEBUG(?::[^\]]+)?)\] )?(.*)$/;

/**
 * Strips ANSI escape codes from a string. Used to clean log file lines that may contain terminal color codes.
 * @param text - The text that may contain ANSI codes.
 * @returns The text with all ANSI codes removed.
 */
function stripAnsiCodes(text: string): string {

  return text.replace(ANSI_PATTERN, "");
}

/**
 * Parses a single log line into a structured entry.
 * @param line - The raw log line from the file.
 * @returns The parsed log entry, or null if the line does not match the expected format.
 */
function parseLogLine(line: string): Nullable<LogEntry> {

  // Strip ANSI color codes before parsing.
  const cleanLine = stripAnsiCodes(line);
  const match = LOG_LINE_PATTERN.exec(cleanLine);

  if(!match) {

    return null;
  }

  const [ , timestamp, levelStr, message ] = match as unknown as [string, string, string | undefined, string];

  let level: "debug" | "error" | "info" | "warn" = "info";
  let categoryTag: string | undefined;

  if(levelStr?.startsWith("DEBUG")) {

    level = "debug";

    // Extract the category suffix from "DEBUG:tuning:hulu" -> "tuning:hulu". This preserves category information for web UI rendering so file-loaded entries
    // display the same [DEBUG:category] badge as live SSE entries.
    const colonIndex = levelStr.indexOf(":");

    if(colonIndex !== -1) {

      categoryTag = levelStr.substring(colonIndex + 1);
    }
  } else if(levelStr === "WARN") {

    level = "warn";
  } else if(levelStr === "ERROR") {

    level = "error";
  }

  const entry: LogEntry = { level, message, timestamp };

  if(categoryTag) {

    entry.categoryTag = categoryTag;
  }

  return entry;
}

/**
 * Reads and parses the log file, returning the most recent entries.
 * @param lines - Maximum number of lines to return.
 * @param levelFilter - Optional level filter (error, warn, info, or undefined for all).
 * @returns The parsed log entries and metadata.
 */
async function readLogEntries(lines: number, levelFilter?: string): Promise<LogsResponse> {

  // Check if using console logging mode (no file logs available).
  if(isConsoleLogging()) {

    return { entries: [], filtered: 0, mode: "console", total: 0 };
  }

  const logFilePath = getLogFilePath(CONFIG);

  try {

    const content = await fsPromises.readFile(logFilePath, "utf-8");
    const allLines = content.split("\n").filter((line) => line.trim().length > 0);

    // Parse all lines into entries.
    const allEntries: LogEntry[] = [];

    for(const line of allLines) {

      const entry = parseLogLine(line);

      if(entry) {

        allEntries.push(entry);
      }
    }

    const total = allEntries.length;

    // Apply level filter if specified.
    let filteredEntries = allEntries;

    if(levelFilter && [ "error", "info", "warn" ].includes(levelFilter)) {

      filteredEntries = allEntries.filter((entry) => entry.level === levelFilter);
    }

    const filtered = filteredEntries.length;

    // Return the most recent entries (last N lines).
    const recentEntries = filteredEntries.slice(-lines);

    return { entries: recentEntries, filtered, mode: "file", total };
  } catch(error) {

    // File does not exist or is unreadable.
    if((error as NodeJS.ErrnoException).code === "ENOENT") {

      return { entries: [], filtered: 0, mode: "file", total: 0 };
    }

    throw error;
  }
}

/* The /logs endpoint provides access to recent application log entries. It supports query parameters for filtering and limiting results, and returns JSON data
 * suitable for both API consumption and the landing page log viewer.
 */

/**
 * Creates the logs endpoint for viewing application log entries.
 * @param app - The Express application.
 */
export function setupLogsEndpoint(app: Express): void {

  app.get("/logs", async (req: Request, res: Response): Promise<void> => {

    // Parse query parameters.
    const linesParam = parseInt(req.query["lines"] as string, 10);
    const lines = (!isNaN(linesParam) && (linesParam > 0) && (linesParam <= 1000)) ? linesParam : 100;
    const level = req.query["level"] as string | undefined;

    try {

      const logsResponse = await readLogEntries(lines, level);

      // GET /logs success path is a data response, not an envelope - the client (loadLogs in routes/root/content.ts) consumes `data.mode` and `data.entries`
      // directly. We deliberately do not wrap with sendSuccess here, since adding `success: true` would change the response contract from "data shape" to
      // "envelope shape" on a read-only endpoint without any consumer needing the marker. The error path below DOES carry the envelope marker because that is
      // the disambiguator clients (and the cross-tree drift sweep at test/e2e/routes/error-envelope.test.ts) use to detect failure responses across the surface.
      res.json(logsResponse);
    } catch {

      // We ship the failure response with the same field shape the success path uses (entries/filtered/mode/total) plus the canonical envelope marker so the
      // log viewer can render an empty state without branching on response shape. The polymorphic sendErrorResponse rich-payload form attaches `success: false`
      // and ships at 500 verbatim.
      sendErrorResponse(res, { entries: [], error: "Failed to read log file.", filtered: 0, mode: "file", total: 0 }, 500);
    }
  });

  /* The /logs/stream endpoint provides real-time log entries via Server-Sent Events. Connected clients receive log entries as they are written, eliminating the need
   * for polling. The connection remains open until the client disconnects.
   */

  app.get("/logs/stream", (req: Request, res: Response): void => {

    const sse = installSseStream(res);

    // Optional level filter from query parameter.
    const levelFilter = req.query["level"] as string | undefined;
    const validLevels = [ "error", "info", "warn" ];
    const filterLevel = (levelFilter && validLevels.includes(levelFilter)) ? levelFilter : null;

    // Subscribe to log entries and forward them as unnamed SSE data events; the heartbeat is owned by installSseStream.
    const unsubscribe = subscribeToLogs((entry) => {

      if(filterLevel && (entry.level !== filterLevel)) {

        return;
      }

      sse.sendEvent(null, entry);
    });

    req.on("close", () => {

      sse.close();
      unsubscribe();
    });
  });
}
