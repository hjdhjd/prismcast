/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * pid.ts: Cross-platform PID file management and process checking utilities.
 */
import type { Nullable } from "../types/index.js";
import fs from "node:fs";

/* PID files provide a lightweight mechanism for tracking running processes across restarts and crashes. Each PID file stores a single process ID as a plain text
 * integer. On startup, the stored PID is checked via signal 0 to determine whether the process is still alive. Stale PID files from crashed or terminated processes
 * are handled gracefully — ENOENT on read and ESRCH on signal check are both expected and silently ignored.
 *
 * This module is intentionally synchronous. PID file operations run in contexts where the event loop may not be available (process.on("exit") handlers), so all
 * I/O uses the synchronous fs API and process.kill() for signaling.
 */

/**
 * Checks whether a process with the given PID is still running. Uses the signal 0 technique: process.kill(pid, 0) throws ESRCH if the process does not exist,
 * returns successfully if it does. EPERM (permission denied) means the process exists but belongs to another user — treated as "still running" since we cannot
 * kill it anyway. This is cross-platform and works on macOS, Linux, and Windows without external tools.
 * @param pid - The process ID to check.
 * @returns True if the process is running, false if it has exited.
 */
export function isProcessRunning(pid: number): boolean {

  try {

    process.kill(pid, 0);

    return true;
  } catch(error: unknown) {

    // EPERM means the process exists but we lack permission to signal it. Treat as running.
    return (error as NodeJS.ErrnoException).code === "EPERM";
  }
}

/**
 * Reads a PID from a file on disk. Returns null if the file does not exist, is empty, or contains non-numeric content. ENOENT is expected on first run or after a
 * clean shutdown and is silently ignored.
 * @param filePath - The absolute path to the PID file.
 * @param label - A descriptive label for log messages (e.g., "Chrome", "server").
 * @param log - Optional logger for non-ENOENT read errors. When omitted (e.g., before the logger is initialized), errors are silently ignored.
 * @returns The stored process ID, or null if unavailable.
 */
export function readPidFile(filePath: string, label: string, log?: { warn: (message: string, ...args: unknown[]) => void }): Nullable<number> {

  try {

    const content = fs.readFileSync(filePath, "utf-8").trim();
    const pid = parseInt(content, 10);

    if(!isNaN(pid)) {

      return pid;
    }
  } catch(error: unknown) {

    if(((error as NodeJS.ErrnoException).code !== "ENOENT") && log) {

      log.warn("Failed to read %s PID file: %s.", label, (error as Error).message);
    }
  }

  return null;
}

/**
 * Writes a PID to a file on disk. Used to persist the current process ID so that the next startup can detect a running instance or clean up an orphaned process.
 * @param filePath - The absolute path to the PID file.
 * @param pid - The process ID to write.
 * @param label - A descriptive label for log messages (e.g., "Chrome", "server").
 * @param log - Optional logger for write errors. When omitted, errors are silently ignored.
 */
export function writePidFile(filePath: string, pid: number, label: string, log?: { warn: (message: string, ...args: unknown[]) => void }): void {

  try {

    fs.writeFileSync(filePath, String(pid), "utf-8");
  } catch(error: unknown) {

    if(log) {

      log.warn("Failed to write %s PID file: %s.", label, (error as Error).message);
    }
  }
}

/**
 * Removes a PID file from disk. Called during graceful shutdown and from exit handlers as a fallback. ENOENT is expected when the file has already been removed and
 * is silently ignored.
 * @param filePath - The absolute path to the PID file.
 * @param label - A descriptive label for log messages (e.g., "Chrome", "server").
 * @param log - Optional logger for non-ENOENT removal errors. When omitted, errors are silently ignored.
 */
export function clearPidFile(filePath: string, label: string, log?: { warn: (message: string, ...args: unknown[]) => void }): void {

  try {

    fs.unlinkSync(filePath);
  } catch(error: unknown) {

    if(((error as NodeJS.ErrnoException).code !== "ENOENT") && log) {

      log.warn("Failed to remove %s PID file: %s.", label, (error as Error).message);
    }
  }
}
