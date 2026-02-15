/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * logs.ts: Log viewing endpoint for PrismCast.
 */
import type { Express, Request, Response } from "express";
import { isConsoleLogging, subscribeToLogs } from "../utils/index.js";
import type { Nullable } from "../types/index.js";
import fs from "fs";
import os from "os";
import path from "path";

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

// Pattern to match log entries: [timestamp] optional [LEVEL] or [LEVEL:category] message. The category suffix handles the new DEBUG:category format while
// remaining backward-compatible with plain [DEBUG] entries from older log files.
const LOG_LINE_PATTERN = /^\[(\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\] (?:\[(WARN|ERROR|DEBUG(?::[^\]]+)?)\] )?(.*)$/;

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

    // Extract the category suffix from "DEBUG:tuning:hulu" → "tuning:hulu". This preserves category information for web UI rendering so file-loaded entries
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

  const logFilePath = path.join(os.homedir(), ".prismcast", "prismcast.log");

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
    const linesParam = parseInt(req.query.lines as string, 10);
    const lines = (!isNaN(linesParam) && (linesParam > 0) && (linesParam <= 1000)) ? linesParam : 100;
    const level = req.query.level as string | undefined;

    try {

      const logsResponse = await readLogEntries(lines, level);

      res.json(logsResponse);
    } catch(error) {

      res.status(500).json({

        entries: [],
        error: "Failed to read log file.",
        filtered: 0,
        mode: "file",
        total: 0
      });
    }
  });

  /* The /logs/stream endpoint provides real-time log entries via Server-Sent Events. Connected clients receive log entries as they are written, eliminating the need
   * for polling. The connection remains open until the client disconnects.
   */

  app.get("/logs/stream", (req: Request, res: Response): void => {

    // Set SSE headers. The Content-Type must be text/event-stream for the browser to recognize this as an SSE connection. Cache-Control prevents proxies from buffering
    // the stream, and Connection: keep-alive ensures the connection stays open.
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("Content-Type", "text/event-stream");

    // Disable response buffering to ensure events are sent immediately.
    res.flushHeaders();

    // Optional level filter from query parameter.
    const levelFilter = req.query.level as string | undefined;
    const validLevels = [ "error", "info", "warn" ];
    const filterLevel = (levelFilter && validLevels.includes(levelFilter)) ? levelFilter : null;

    // Subscribe to log entries and send them as SSE events.
    const unsubscribe = subscribeToLogs((entry) => {

      // Apply level filter if specified.
      if(filterLevel && (entry.level !== filterLevel)) {

        return;
      }

      // Format the entry as an SSE event. Each event consists of "data:" lines followed by a blank line.
      const eventData = JSON.stringify(entry);

      res.write("data: " + eventData + "\n\n");
    });

    // Send a named heartbeat event every 30 seconds to keep the connection alive through proxies and allow clients to detect staleness.
    const heartbeatInterval = setInterval(() => {

      res.write("event: heartbeat\ndata: \n\n");
    }, 30000);

    // Clean up when the client disconnects.
    req.on("close", () => {

      clearInterval(heartbeatInterval);
      unsubscribe();
    });
  });
}
