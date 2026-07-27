/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * processInspector.context.ts: The default adapter for ProcessInspectorContext. Enumerates the OS process table per platform: walks /proc on Linux, spawns
 * `ps` on macOS, and spawns PowerShell with Get-CimInstance on Windows. This file is the only place in the process-inspector module that shells out, reads
 * /proc, or branches on process.platform; tests construct ProcessInspectorContext literals inline and bypass this file entirely.
 *
 * Synchronous enumeration is intentional. killStaleChrome (the primary caller) runs in synchronous code paths including process.on("exit") handlers, where the
 * event loop is not guaranteed to be available. The execFileSync / readdirSync / readFileSync calls below are bounded - macOS/Windows shells return in tens of
 * milliseconds for a typical process table, and Linux /proc walks are sub-millisecond.
 */
import type { ProcessInfo, ProcessInspectorContext } from "./processInspector.ts";
import { parseLinuxProcCmdline, parseLinuxProcStat, parseMacOsPsOutput, parseWindowsFormatListOutput } from "./processInspector.ts";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

/**
 * Builds the default ProcessInspectorContext from real runtime I/O. Dispatches to the platform-appropriate enumerator at adapter-creation time so the chosen
 * branch is captured in a closure for every subsequent enumerate() call.
 * @returns A ProcessInspectorContext wired to the current platform's process table reader.
 */
export function createDefaultProcessInspectorContext(): ProcessInspectorContext {

  return { enumerate: pickEnumeratorForPlatform() };
}

/**
 * Selects the platform-appropriate enumerator. Linux uses /proc; macOS and Windows shell out to their native process listing utilities.
 * @returns The enumerator function for the current platform.
 */
function pickEnumeratorForPlatform(): () => ProcessInfo[] {

  switch(process.platform) {

    case "linux": {

      return enumerateLinux;
    }

    case "darwin": {

      return enumerateMacOs;
    }

    case "win32": {

      return enumerateWindows;
    }

    default: {

      // Unsupported platforms return an empty snapshot. Domain code that filters this list (e.g., looking for Chrome) will simply find no matches, which is
      // the safe default - we never SIGTERM anything we did not actually discover.
      return (): ProcessInfo[] => [];
    }
  }
}

/**
 * Enumerates processes on Linux by walking /proc. Each numeric directory under /proc corresponds to a PID; reading its cmdline file yields the command line.
 * Processes that exit between readdir and readFile produce ENOENT, which we silently skip.
 * @returns The current process table.
 */
function enumerateLinux(): ProcessInfo[] {

  const results: ProcessInfo[] = [];

  let entries: string[];

  try {

    entries = fs.readdirSync("/proc");
  } catch {

    return results;
  }

  for(const entry of entries) {

    const pid = parseInt(entry, 10);

    if(Number.isNaN(pid)) {

      continue;
    }

    let cmdlineRaw: string;
    let statRaw: string;

    try {

      cmdlineRaw = fs.readFileSync(path.join("/proc", entry, "cmdline"), "utf-8");
      statRaw = fs.readFileSync(path.join("/proc", entry, "stat"), "utf-8");
    } catch {

      // ENOENT (process exited between readdir and readFile), EACCES (permission denied for some PIDs as a non-root user), and similar are all expected during
      // a /proc walk. Skip silently rather than aborting the whole enumeration.
      continue;
    }

    const ppid = parseLinuxProcStat(statRaw);

    if(ppid === null) {

      continue;
    }

    results.push({ commandLine: parseLinuxProcCmdline(cmdlineRaw), pid, ppid });
  }

  return results;
}

/**
 * Enumerates processes on macOS via `ps -axww -o pid=,ppid=,command=`. The -ax flags select every process on the system; -ww disables column truncation so long
 * command lines (Chrome's are frequently very long) survive intact; the trailing `=` on each -o spec suppresses the column headers.
 * @returns The current process table.
 */
function enumerateMacOs(): ProcessInfo[] {

  try {

    // maxBuffer is bumped to 16MB to accommodate hosts with thousands of processes and Chrome's long --flag-soup command lines.
    const raw = execFileSync("/bin/ps", [ "-axww", "-o", "pid=,ppid=,command=" ], { encoding: "utf-8", maxBuffer: 16 * 1024 * 1024 });

    return parseMacOsPsOutput(raw);
  } catch {

    return [];
  }
}

/**
 * Enumerates processes on Windows via PowerShell's Get-CimInstance Win32_Process pipeline. We choose Format-List output over Format-Table because the latter
 * truncates the CommandLine column, and choose CIM over the legacy wmic.exe because wmic is being deprecated by Microsoft and is absent from Windows Server
 * Core / minimal SKUs.
 * @returns The current process table.
 */
function enumerateWindows(): ProcessInfo[] {

  try {

    // -NoProfile skips loading the user's PowerShell profile (saves hundreds of milliseconds); -Command is the parameter form most resilient to quoting.
    // The same 16MB maxBuffer used for the macOS enumerator comfortably covers Format-List's output here too, since its multi-line, one-property-per-row
    // rendering is more verbose per process than a single ps line but still stays well within that ceiling even on hosts with thousands of processes.
    const raw = execFileSync("powershell.exe",
      [ "-NoProfile", "-Command", "Get-CimInstance Win32_Process | Format-List ProcessId,ParentProcessId,CommandLine" ],
      { encoding: "utf-8", maxBuffer: 16 * 1024 * 1024 });

    return parseWindowsFormatListOutput(raw);
  } catch {

    return [];
  }
}
