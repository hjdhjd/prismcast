/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * exec.helpers.test.ts: Tests for the execFile fakes (bufferOrStringToString, makeExecFileError, execFileFromMap, execFileAlwaysSucceeds). The fakes model the
 * promisified node:child_process.execFile contract, including both encoding branches (string default, Buffer for encoding: "buffer" callers) so production
 * unpackers can exercise both. Coverage asserts the success/failure/encoding branches plus the strict-key-miss behavior of execFileFromMap.
 */
import { bufferOrStringToString, execFileAlwaysSucceeds, execFileFromMap, makeExecFileError } from "./exec.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

describe("bufferOrStringToString", () => {

  test("returns the string as-is when given a string", () => {

    assert.equal(bufferOrStringToString("hello"), "hello", "a string should pass through untouched");
  });

  test("decodes a Buffer to utf8 when given a Buffer", () => {

    assert.equal(bufferOrStringToString(Buffer.from("hello", "utf8")), "hello", "a Buffer should decode to its utf8 text");
  });

  test("returns an empty string when given undefined", () => {

    assert.equal(bufferOrStringToString(undefined), "", "undefined should read as the empty string");
  });

  test("returns an empty string when given an empty Buffer", () => {

    assert.equal(bufferOrStringToString(Buffer.alloc(0)), "", "an empty Buffer should read as the empty string");
  });

  test("preserves multi-byte utf8 sequences when decoding a Buffer", () => {

    // The Buffer.toString('utf8') path must handle non-ASCII content correctly. A character outside the BMP exercises the multi-byte sequence handling.
    const text = "Hello, éü中文\u{1F600}";

    assert.equal(bufferOrStringToString(Buffer.from(text, "utf8")), text, "multi-byte sequences should survive the decode");
  });
});

describe("makeExecFileError", () => {

  test("returns an Error whose message matches the supplied text", () => {

    const error = makeExecFileError("Command failed", "", "");

    assert.equal(error.message, "Command failed", "the supplied text should become the error message");
  });

  test("attaches stderr and stdout as own properties on the Error", () => {

    const error = makeExecFileError("boom", "the stderr", "the stdout") as Error & { stderr: string; stdout: string };

    assert.equal(error.stderr, "the stderr", "stderr should be attached to the error");
    assert.equal(error.stdout, "the stdout", "stdout should be attached to the error");
  });

  test("preserves Buffer-shaped stderr without converting it (matches encoding: 'buffer' real shape)", () => {

    // The execFile { encoding: "buffer" } config produces Buffer-shaped stderr/stdout. Helpers downstream that unpack both branches need the Buffer to flow
    // through unmodified.
    const stderr = Buffer.from("Buffer-shaped stderr", "utf8");
    const error = makeExecFileError("boom", stderr, "") as Error & { stderr: Buffer | string };

    assert.ok(Buffer.isBuffer(error.stderr), "stderr is a Buffer");
    assert.equal(error.stderr.toString("utf8"), "Buffer-shaped stderr", "the Buffer contents should be intact");
  });

  test("returned object is structurally an Error (instanceof Error, has stack)", () => {

    const error = makeExecFileError("boom", "", "");

    assert.ok(error instanceof Error, "the fake should be a real Error");
    assert.equal(typeof error.stack, "string", "a real Error carries a stack");
  });
});

describe("execFileFromMap", () => {

  test("returns the configured success result when the keyed command matches", async () => {

    const execFile = execFileFromMap({ "ls -la": { stderr: "warning text", stdout: "file listing" } });

    const result = await execFile("ls", ["-la"]);

    assert.deepEqual(result, { stderr: "warning text", stdout: "file listing" }, "a keyed command should return its configured result");
  });

  test("normalizes Buffer-shaped stdout/stderr to utf8 strings on the success path", async () => {

    // Tests author results in either encoding; the helper normalizes to strings (matching real execFile's default utf8 encoding behavior).
    const execFile = execFileFromMap({

      "echo hi": {

        stderr: Buffer.from("warning", "utf8"),
        stdout: Buffer.from("output", "utf8")
      }
    });

    const result = await execFile("echo", ["hi"]);

    assert.equal(typeof result.stdout, "string", "Buffer stdout should be normalized to a string");
    assert.equal(typeof result.stderr, "string", "Buffer stderr should be normalized to a string");
    assert.deepEqual(result, { stderr: "warning", stdout: "output" }, "the normalized text should match the Buffer contents");
  });

  test("throws fake-exec-fail with empty stderr/stdout when no map entry matches the command", async () => {

    // Strict default: unknown commands throw "no result configured" so test setups missing a command surface immediately rather than silently passing.
    const execFile = execFileFromMap({});

    await assert.rejects(
      () => execFile("missing", ["arg"]),
      (error: Error & { stderr: Buffer | string; stdout: Buffer | string }): boolean => {

        assert.match(error.message, /fake-exec-fail: no result configured for missing arg/, "the message should name the unmapped command");
        assert.equal(error.stderr, "", "an unmapped command reports empty stderr");
        assert.equal(error.stdout, "", "an unmapped command reports empty stdout");

        return true;
      },
      "an unmapped command should reject rather than pass"
    );
  });

  test("throws fake-exec-fail with the configured stderr/stdout when a match has shouldThrow set", async () => {

    const execFile = execFileFromMap({

      "ls -la": {

        shouldThrow: true,
        stderr: "permission denied",
        stdout: "partial output before failure"
      }
    });

    await assert.rejects(
      () => execFile("ls", ["-la"]),
      (error: Error & { stderr: Buffer | string; stdout: Buffer | string }): boolean => {

        assert.match(error.message, /fake-exec-fail: ls -la/, "the message should name the failing command");
        assert.equal(error.stderr, "permission denied", "the configured stderr should reach the caller");
        assert.equal(error.stdout, "partial output before failure", "the configured stdout should reach the caller");

        return true;
      },
      "a shouldThrow entry should reject"
    );
  });

  test("preserves Buffer-shaped stderr on the throw path (so the Buffer branch of unpackers can be exercised)", async () => {

    const stderr = Buffer.from("Buffer stderr", "utf8");
    const execFile = execFileFromMap({ "rm -rf /": { shouldThrow: true, stderr } });

    await assert.rejects(
      () => execFile("rm", [ "-rf", "/" ]),
      (error: Error & { stderr: Buffer | string }): boolean => {

        assert.ok(Buffer.isBuffer(error.stderr), "stderr is preserved as Buffer on the throw path");

        return true;
      },
      "a Buffer-stderr entry should reject"
    );
  });

  test("keys are 'file args.join(\" \")' so multi-arg commands match exactly", async () => {

    // The key construction is documented; this test asserts it so future refactors don't accidentally change the join character or quoting.
    const execFile = execFileFromMap({ "git --git-dir=/x status --porcelain": { stdout: "" } });

    await assert.doesNotReject(() => execFile("git", [ "--git-dir=/x", "status", "--porcelain" ]), "a space-joined multi-arg key should match");
  });

  test("an empty args array yields a key with a trailing space (file + ' ')", async () => {

    // Edge case: zero-arg invocations still need a key. The current implementation produces "file " (file plus a space and an empty join). We assert this so a
    // future refactor doesn't accidentally make zero-arg commands key as just "file".
    const execFile = execFileFromMap({ "ls ": { stdout: "current dir" } });

    const result = await execFile("ls", []);

    assert.equal(result.stdout, "current dir", "a zero-arg command should match its trailing-space key");
  });
});

describe("execFileAlwaysSucceeds", () => {

  test("returns success on every invocation with empty stdout/stderr by default", async () => {

    const execFile = execFileAlwaysSucceeds();

    const a = await execFile("any", ["thing"]);
    const b = await execFile("totally", [ "different", "command" ]);

    assert.deepEqual(a, { stderr: "", stdout: "" }, "the first invocation should succeed with empty output");
    assert.deepEqual(b, { stderr: "", stdout: "" }, "a different command should succeed the same way");
  });

  test("uses the supplied stdout and stderr defaults", async () => {

    const execFile = execFileAlwaysSucceeds("hello", "warning");

    const result = await execFile("any", []);

    assert.deepEqual(result, { stderr: "warning", stdout: "hello" }, "the supplied defaults should be returned");
  });

  test("normalizes Buffer-shaped defaults to utf8 strings", async () => {

    const execFile = execFileAlwaysSucceeds(Buffer.from("output", "utf8"), Buffer.from("warning", "utf8"));

    const result = await execFile("any", []);

    assert.deepEqual(result, { stderr: "warning", stdout: "output" }, "Buffer defaults should be normalized to strings");
  });

  test("never throws regardless of file/args", async () => {

    const execFile = execFileAlwaysSucceeds();

    await assert.doesNotReject(() => execFile("", []), "an empty command should still resolve");
    await assert.doesNotReject(() => execFile("malformed", [ "very", "weird", "args" ]), "an unmapped command should still resolve");
  });
});
