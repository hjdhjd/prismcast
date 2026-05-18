/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * processInspector.ts: The OS process-table port. Returns a snapshot of every visible process on the host with its full command line so domain code can match
 * by intent (e.g., "Chrome processes using our profile directory") rather than by historical PID. This is the architecturally correct primitive for finding
 * orphaned processes after an ungraceful shutdown - a saved PID has no provenance after a reboot, but a live process's command line is the actual source of
 * truth for what it is and what it is doing.
 *
 * Platform branches live entirely in processInspector.context.ts and the per-platform parsers exported from this file. The orchestrator is a one-line dispatch
 * over a ProcessInspectorContext, which tests construct as a literal with pre-built ProcessInfo arrays. The per-platform parsers are exported so tests can
 * exercise the format-handling code paths against realistic fixtures without spawning subprocesses.
 */
import type { Nullable } from "../types/index.ts";
import { createDefaultProcessInspectorContext } from "./processInspector.context.ts";

/**
 * One row of the OS process table. The triple (pid, ppid, commandLine) lets domain code identify a process by intent and verify its ownership via the
 * parent-child relationship. The ppid in particular is what lets killStaleChrome safely run from any context: a process whose Chrome is matched by profile
 * but whose ppid points at a live unrelated parent is owned by that parent, not by us, and we must leave it alone.
 */
export interface ProcessInfo {

  // The full command line as the OS reports it. On Linux this is the space-joined /proc/<pid>/cmdline (NUL bytes replaced with spaces); on macOS the output of
  // `ps -o command=`; on Windows the CommandLine property from Win32_Process. May be empty for kernel threads or processes that hid their command line.
  readonly commandLine: string;

  // The process ID. Stable for the lifetime of the process.
  readonly pid: number;

  // The parent process ID at the moment of enumeration. After the parent dies, the kernel reparents the child to the init process (PID 1 on Linux/macOS,
  // distinct system process on Windows); a ppid that points at a dead PID or at init signals an orphan.
  readonly ppid: number;
}

/**
 * The runtime capability set listProcesses consumes. Production wires it from real I/O via createDefaultProcessInspectorContext; tests pass a context literal
 * holding the desired ProcessInfo[].
 */
export interface ProcessInspectorContext {

  // Returns the current process table snapshot. Implementations may spawn a subprocess, read /proc, or query a platform API - the orchestrator does not care.
  readonly enumerate: () => ProcessInfo[];
}

/**
 * Returns a snapshot of every visible process. Callers filter the result for the processes they care about.
 * @param ctx - The process inspector context. Defaults to real I/O wiring.
 * @returns The current process table.
 */
export function listProcesses(ctx: ProcessInspectorContext = createDefaultProcessInspectorContext()): ProcessInfo[] {

  return ctx.enumerate();
}

/**
 * Parses a single /proc/<pid>/cmdline payload. The on-disk format separates argv entries with NUL bytes and may or may not include a trailing NUL. Empty
 * payloads (kernel threads, zombie processes) yield an empty command line - callers decide whether to drop those entries.
 * @param raw - The /proc/<pid>/cmdline payload.
 * @returns The decoded command line.
 */
export function parseLinuxProcCmdline(raw: string): string {

  // Drop a trailing NUL if present so the join does not produce a phantom blank arg.
  const trimmed = raw.endsWith("\0") ? raw.slice(0, -1) : raw;

  return trimmed.split("\0").join(" ");
}

/**
 * Parses the ppid (field 4) from a /proc/<pid>/stat payload. The format embeds the executable name in parentheses at field 2; that name can contain
 * whitespace and parentheses itself, so we anchor on the last closing paren before splitting. After the closing paren, the first field is field 3 (state) and
 * the second is field 4 (ppid).
 * @param raw - The /proc/<pid>/stat payload.
 * @returns The parent process ID, or null when the payload is malformed.
 */
export function parseLinuxProcStat(raw: string): Nullable<number> {

  const rparen = raw.lastIndexOf(")");

  if(rparen === -1) {

    return null;
  }

  const fields = raw.slice(rparen + 1).trim().split(/\s+/);

  // We want field 4 (ppid), which lands at index 1 in the post-paren split: index 0 is state (field 3), index 1 is ppid (field 4).
  if(fields.length < 2) {

    return null;
  }

  const ppid = parseInt(fields[1] ?? "", 10);

  return Number.isNaN(ppid) ? null : ppid;
}

/**
 * Parses the output of `ps -axww -o pid=,ppid=,command=` (or equivalent shape). Each non-empty line begins with leading whitespace, then the PID and PPID as
 * integers, then whitespace, then the full command. Lines that do not match this shape are silently dropped.
 * @param raw - The raw ps output.
 * @returns The parsed process table.
 */
export function parseMacOsPsOutput(raw: string): ProcessInfo[] {

  const results: ProcessInfo[] = [];

  for(const line of raw.split("\n")) {

    const trimmed = line.trim();

    if(trimmed === "") {

      continue;
    }

    // Three captures: pid, ppid, and the rest. The command line is the rest of the line verbatim (may contain spaces).
    const match = /^(\d+)\s+(\d+)\s+(.+)$/.exec(trimmed);

    if(match === null) {

      continue;
    }

    const pid = parseInt(match[1] ?? "", 10);
    const ppid = parseInt(match[2] ?? "", 10);
    const commandLine = match[3] ?? "";

    if(Number.isNaN(pid) || Number.isNaN(ppid)) {

      continue;
    }

    results.push({ commandLine, pid, ppid });
  }

  return results;
}

/**
 * Parses the output of PowerShell's `Get-CimInstance Win32_Process | Format-List ProcessId,ParentProcessId,CommandLine` (or equivalent shape). The format
 * places each property on its own line as "Name : Value", with blank lines separating records. CommandLine may be blank (system processes) or contain
 * embedded colons - we split on the first " : " separator only.
 * @param raw - The raw Format-List output.
 * @returns The parsed process table.
 */
export function parseWindowsFormatListOutput(raw: string): ProcessInfo[] {

  const results: ProcessInfo[] = [];

  let currentPid: Nullable<number> = null;
  let currentPpid: Nullable<number> = null;
  let currentCommandLine: Nullable<string> = null;

  const flush = (): void => {

    if((currentPid !== null) && (currentPpid !== null)) {

      results.push({ commandLine: currentCommandLine ?? "", pid: currentPid, ppid: currentPpid });
    }

    currentPid = null;
    currentPpid = null;
    currentCommandLine = null;
  };

  for(const line of raw.split(/\r?\n/)) {

    if(line.trim() === "") {

      flush();

      continue;
    }

    // Format-List output is "Name : Value" with the colon surrounded by spaces. We split on the first " : " so values containing colons survive intact.
    const sep = line.indexOf(" : ");

    if(sep === -1) {

      continue;
    }

    const key = line.slice(0, sep).trim();
    const value = line.slice(sep + 3);

    switch(key) {

      case "ProcessId": {

        const parsed = parseInt(value.trim(), 10);

        currentPid = Number.isNaN(parsed) ? null : parsed;

        break;
      }

      case "ParentProcessId": {

        const parsed = parseInt(value.trim(), 10);

        currentPpid = Number.isNaN(parsed) ? null : parsed;

        break;
      }

      case "CommandLine": {

        currentCommandLine = value;

        break;
      }

      default: {

        // Other Format-List keys (Name, Path, ...) are ignored. Tolerating extra fields keeps the parser robust to upstream additions.
      }
    }
  }

  // Flush the trailing record if the output did not end with a blank line.
  flush();

  return results;
}
