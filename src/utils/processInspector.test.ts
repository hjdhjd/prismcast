/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * processInspector.test.ts: Unit tests for the process-inspector port. The orchestrator is a one-liner so its coverage is a smoke test; the meaningful surface
 * is the three platform-specific parsers, each exercised against representative fixtures. Tests construct ProcessInspectorContext literals for the orchestrator
 * and pass synthetic strings to the parsers - no real /proc, ps, or PowerShell is invoked here.
 */
import { describe, test } from "node:test";
import { listProcesses, parseLinuxProcCmdline, parseLinuxProcStat, parseMacOsPsOutput, parseWindowsFormatListOutput } from "./processInspector.ts";
import type { ProcessInspectorContext } from "./processInspector.ts";
import assert from "node:assert/strict";

describe("listProcesses", () => {

  test("delegates to ctx.enumerate", () => {

    const expected = [ { commandLine: "node /app/index.js", pid: 1234, ppid: 1 }, { commandLine: "/usr/bin/chrome --user-data-dir=/x", pid: 5678, ppid: 1234 } ];
    const ctx: ProcessInspectorContext = { enumerate: () => expected };

    assert.deepEqual(listProcesses(ctx), expected);
  });
});

describe("parseLinuxProcCmdline", () => {

  test("joins NUL-separated argv with single spaces", () => {

    // The on-disk format is "argv[0]\0argv[1]\0argv[2]\0". The parser must reconstruct a single shell-readable string.
    const raw = "/usr/bin/chrome\0--user-data-dir=/home/x/.prismcast\0--no-sandbox\0";

    assert.equal(parseLinuxProcCmdline(raw), "/usr/bin/chrome --user-data-dir=/home/x/.prismcast --no-sandbox");
  });

  test("handles cmdline without a trailing NUL", () => {

    // Some kernels do not append the trailing NUL. The parser must produce the same shape either way.
    assert.equal(parseLinuxProcCmdline("/usr/bin/chrome\0--no-sandbox"), "/usr/bin/chrome --no-sandbox");
  });

  test("returns an empty string for an empty cmdline (kernel threads)", () => {

    // Kernel threads expose an empty cmdline. Callers that filter ProcessInfo[] by command-line content will simply not match such entries.
    assert.equal(parseLinuxProcCmdline(""), "");
  });
});

describe("parseLinuxProcStat", () => {

  test("extracts field 4 (ppid) from a representative /proc/<pid>/stat payload", () => {

    // The comm contains an embedded paren ("(bash") to exercise the last-paren anchor. Field 4 (ppid) is 4242; everything else is filler.
    const raw = "1234 ((bash) S 4242 1 1 0 -1 4194304 0 0 0 0 0 0 0 0 20 0 1 0 5678 0 0 18446744073709551615 1 1 0 0 0 0 0 0 0 0 0 0 17 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n";

    assert.equal(parseLinuxProcStat(raw), 4242);
  });

  test("returns null when there is no closing paren", () => {

    assert.equal(parseLinuxProcStat("garbage"), null);
  });

  test("returns null when the ppid field is not a valid integer", () => {

    // The first field after the closing paren is state; the second should be ppid. If the parser saw a non-numeric token there, the record is unusable.
    assert.equal(parseLinuxProcStat("1 (init) S not-a-pid\n"), null);
  });
});

describe("parseMacOsPsOutput", () => {

  test("parses a representative `ps -axww -o pid=,ppid=,command=` payload", () => {

    // The leading whitespace is what real ps emits (column padding for the pid and ppid fields). The parser must tolerate it. Commands with embedded spaces
    // and equals signs (Chrome's --user-data-dir flag) must survive intact.
    const raw =
      "    1     0 /sbin/launchd\n" +
      "  321     1 /usr/sbin/syslogd\n" +
      "12345  4444 /Applications/Google Chrome.app/Contents/MacOS/Google Chrome --user-data-dir=/Users/x/.prismcast/chromedata --no-sandbox\n";

    assert.deepEqual(parseMacOsPsOutput(raw), [

      { commandLine: "/sbin/launchd", pid: 1, ppid: 0 },
      { commandLine: "/usr/sbin/syslogd", pid: 321, ppid: 1 },
      { commandLine: "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome --user-data-dir=/Users/x/.prismcast/chromedata --no-sandbox", pid: 12345,
        ppid: 4444 }
    ]);
  });

  test("silently drops malformed lines (header garbage, blank lines)", () => {

    const raw = "PID PPID COMMAND\n\n  1234   1 /usr/bin/node\n";

    // The header line starts with "PID" which is not numeric; the blank line is empty. Both are dropped, leaving only the numeric row.
    assert.deepEqual(parseMacOsPsOutput(raw), [{ commandLine: "/usr/bin/node", pid: 1234, ppid: 1 }]);
  });
});

describe("parseWindowsFormatListOutput", () => {

  test("parses a representative Format-List payload", () => {

    // Format-List emits one line per property (ProcessId, ParentProcessId, CommandLine) plus blank-line separators. The parser must group records correctly.
    // CommandLine entries can be very long (Chrome's command line) and contain colons - we split on the first " : " separator only.
    const raw =
      "ProcessId       : 4\r\n" +
      "ParentProcessId : 0\r\n" +
      "CommandLine     : \r\n" +
      "\r\n" +
      "ProcessId       : 12345\r\n" +
      "ParentProcessId : 4444\r\n" +
      "CommandLine     : \"C:\\Program Files\\Google\\Chrome\\chrome.exe\" --user-data-dir=C:\\Users\\x\\.prismcast\\chromedata --no-sandbox\r\n" +
      "\r\n";

    assert.deepEqual(parseWindowsFormatListOutput(raw), [

      { commandLine: "", pid: 4, ppid: 0 },
      { commandLine: "\"C:\\Program Files\\Google\\Chrome\\chrome.exe\" --user-data-dir=C:\\Users\\x\\.prismcast\\chromedata --no-sandbox", pid: 12345,
        ppid: 4444 }
    ]);
  });

  test("flushes the trailing record when output does not end with a blank line", () => {

    const raw = "ProcessId       : 7\r\nParentProcessId : 1\r\nCommandLine     : foo\r\n";

    assert.deepEqual(parseWindowsFormatListOutput(raw), [{ commandLine: "foo", pid: 7, ppid: 1 }]);
  });

  test("ignores unrelated Format-List properties", () => {

    // If a future PowerShell version adds more fields to Format-List output, the parser must keep working. Only ProcessId, ParentProcessId, and CommandLine
    // drive the record.
    const raw = "ProcessId       : 100\r\nName            : node.exe\r\nPath            : C:\\Program Files\\nodejs\\node.exe\r\nParentProcessId : 50\r\n" +
      "CommandLine     : node x.js\r\n\r\n";

    assert.deepEqual(parseWindowsFormatListOutput(raw), [{ commandLine: "node x.js", pid: 100, ppid: 50 }]);
  });

  test("drops records without a ParentProcessId field", () => {

    // The record is unusable for ownership decisions if we cannot tell who spawned it. Better to skip than to emit a record with a guessed ppid.
    const raw = "ProcessId       : 50\r\nCommandLine     : something\r\n\r\n";

    assert.deepEqual(parseWindowsFormatListOutput(raw), []);
  });
});
