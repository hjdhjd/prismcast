/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * pid.ts: Cross-platform PID liveness check and silent file removal. This module holds the low-level OS primitives that the PID-file layer builds on: a
 * liveness check via the signal-0 technique, and file removal that treats ENOENT as success. Structured PID-file lifecycle (write the record, parse it
 * on read, detect a stale boot session) lives in utils/runtimeIdentity.ts; this module is its low-level dependency.
 *
 * This module is intentionally synchronous. PID-file operations run in contexts where the event loop may not be available (process.on("exit") handlers), so
 * all I/O uses the synchronous fs API and process.kill() for signaling.
 */
import fs from "node:fs";

/**
 * Checks whether a process with the given PID is still running. Uses the signal 0 technique: process.kill(pid, 0) throws ESRCH if the process does not exist,
 * returns successfully if it does. EPERM (permission denied) means the process exists but belongs to another user - treated as "still running" since we cannot
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
 * Removes a PID file from disk. ENOENT is expected when the file has already been removed and is silently ignored.
 * @param filePath - The absolute path to the PID file.
 * @param label - A descriptive label for log messages (e.g., "identity", "server").
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
