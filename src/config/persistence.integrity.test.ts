/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * persistence.integrity.test.ts: Tests for the file-store framework's integrity-and-recovery branches - the validator severity router, the post-write integrity
 * check that catches encoder bugs and partial writes, and the recovery branches inside tryRecoverFromBackup, doMutate's backup step, and read()'s file-read
 * fallback. This file covers tryRecoverFromBackup's own restore-write-failure contract as exercised through read() (which is what doMutate calls internally
 * before applying a mutation); the mutate-specific recovery-guard behavior - the corrupt-main rotation guard and the distinct temp-path contract between a
 * read-triggered recovery and an in-flight mutate - is covered separately in persistence.test.ts. Each branch is a safety net the framework relies on but the
 * user-facing happy path never visits; the tests assert them so a refactor that breaks the sequence (e.g., dropping the .bak restore on integrity failure,
 * ENOENT-misclassifying a permission error) surfaces here rather than as user data loss.
 *
 * The tests use the in-memory storage backend with override hooks to drive failure modes deterministically. Real-fs reproduction of "writeFile lies about what
 * it wrote" or "readFile fails for a permission reason but not ENOENT" is fragile or impossible; the in-memory backend lets the test author specify the exact
 * failure sequence the branch was designed to handle.
 *
 * How the severity-routing and best-effort-warn assertions observe LOG output. The framework logs through the process-wide LOG, whose entries also flow to the
 * SSE emitter before the console/file branch. We subscribe to that emitter (subscribeToLogs) and assert against the captured level and formatted message - the
 * same observable an operator sees on the Logs tab - rather than swapping the logger. Console logging defaults off under test, so the subscription is silent.
 */
import { afterEach, beforeEach, describe, test } from "node:test";
import { makeMemoryStorageBackend, makeMemoryStore } from "./persistence.helpers.ts";
import type { LogEntry } from "../utils/logEmitter.ts";
import type { ValidationIssue } from "./persistence.ts";
import assert from "node:assert/strict";
import { subscribeToLogs } from "../utils/logEmitter.ts";

// Every emitted log entry for the duration of a test. Populated by the subscribeToLogs subscription installed in beforeEach and reset per test so one test's
// framework output cannot leak into another's assertions. Filtered by level and message substring the same way an operator would scan the Logs tab.
let captured: LogEntry[];

let unsubscribe: () => void;

beforeEach(() => {

  captured = [];
  unsubscribe = subscribeToLogs((entry) => { captured.push(entry); });
});

afterEach(() => {

  unsubscribe();
});

describe("FileStore.mutate - validator severity routing", () => {

  test("warning-severity issues are emitted at warn level (not error)", async () => {

    /* The validator's contract: returned issues are routed by severity. A warning emits at LOG.warn; an error emits at LOG.error. This test asserts the warning route -
     * a warning-severity issue must surface through LOG.warn and never through LOG.error.
     */
    const backend = makeMemoryStorageBackend();
    const filePath = "/data/validator-warn.json";
    const issues: ValidationIssue[] = [
      { category: "test-warn", description: "synthetic warning condition", severity: "warning" }
    ];
    const store = makeMemoryStore<{ value: number }>(backend, filePath, {

      defaultValue: () => ({ value: 0 }),
      validate: () => issues
    });

    await store.mutate((data) => { data.value = 1; });

    const warnings = captured.filter((line) => (line.level === "warn"));
    const errors = captured.filter((line) => (line.level === "error"));
    const matching = warnings.filter((line) => line.message.includes("test-warn"));

    assert.equal(matching.length, 1, "exactly one warn-level line carries the issue category");
    assert.match(matching[0]?.message ?? "", /validator-warn/, "log message includes the store label");
    assert.match(matching[0]?.message ?? "", /synthetic warning condition/, "log message includes the issue description");
    assert.equal(errors.length, 0, "warning-severity issues must NOT route through LOG.error");
  });

  test("error-severity issues are emitted at error level (not warn)", async () => {

    const backend = makeMemoryStorageBackend();
    const filePath = "/data/validator-error.json";
    const issues: ValidationIssue[] = [
      { category: "test-error", description: "synthetic error condition", severity: "error" }
    ];
    const store = makeMemoryStore<{ value: number }>(backend, filePath, {

      defaultValue: () => ({ value: 0 }),
      validate: () => issues
    });

    await store.mutate((data) => { data.value = 1; });

    const warnings = captured.filter((line) => (line.level === "warn"));
    const errors = captured.filter((line) => (line.level === "error"));
    const matching = errors.filter((line) => line.message.includes("test-error"));

    assert.equal(matching.length, 1, "exactly one error-level line carries the issue category");
    assert.match(matching[0]?.message ?? "", /validator-error/, "log message includes the store label");
    assert.match(matching[0]?.message ?? "", /synthetic error condition/, "log message includes the issue description");
    assert.equal(warnings.filter((line) => line.message.includes("test-error")).length, 0,
      "error-severity issues must NOT route through LOG.warn");
  });
});

describe("FileStore.mutate - post-write integrity check", () => {

  test("readback content mismatch triggers .bak restoration and a thrown error", async () => {

    /* The framework writes content X, reads back what landed on disk, and compares byte-for-byte. On mismatch, the file is structurally suspect - the
     * framework restores the prior good state from .bak and throws so the caller knows the write failed.
     *
     * To drive this, we override writeFile so that the FIRST write to <filePath>.tmp stores tampered content rather than what the framework asked for. The
     * subsequent rename moves the tampered tmp to main, the readback returns tampered bytes, byte-comparison fails, .bak is restored, and the mutate rejects.
     * We use a one-shot guard so a second write to the identical <filePath>.tmp path would not re-trigger the tamper injection, should the framework ever
     * reuse that path within a single mutate. The recovery path itself writes through a distinctly-suffixed <filePath>.recover.tmp, so it is unaffected by
     * this guard either way.
     */
    const backend = makeMemoryStorageBackend();
    const filePath = "/data/integrity-fail.json";

    // Seed: a valid existing main and .bak so the framework reads the pre-state cleanly and has a known good state to restore from after integrity fails.
    const priorGoodContent = "{\"value\":1}\n";

    backend.files.set(filePath, priorGoodContent);
    backend.mtimes.set(filePath, 1);
    backend.files.set(filePath + ".bak", priorGoodContent);
    backend.mtimes.set(filePath + ".bak", 1);

    let tamperedOnce = false;
    const realWriteFile = backend.writeFile;

    backend.writeFile = async (p: string, content: string): Promise<void> => {

      // First write to <filePath>.tmp: store tampered content. The integrity check on readback will see the tampered bytes and fail. Subsequent writes (the
      // recovery's own .tmp write while restoring from .bak) flow through the default unchanged so recovery actually completes.
      if(!tamperedOnce && (p === (filePath + ".tmp"))) {

        tamperedOnce = true;
        backend.files.set(p, "TAMPERED-PAYLOAD-NOT-VALID-JSON");
        backend.mtimes.set(p, (backend.mtimes.get(p) ?? 0) + 100);

        return;
      }

      return realWriteFile(p, content);
    };

    const store = makeMemoryStore<{ value: number }>(backend, filePath, {

      defaultValue: () => ({ value: 0 })
    });

    await assert.rejects(() => store.mutate((data) => { data.value = 2; }), /Post-write integrity check failed/,
      "the integrity check rejects with the documented error message");

    // The .bak restore landed: main now byte-matches the prior good state. Without the restore, main would still hold the tampered bytes (the rename had
    // already moved tampered tmp into place by the time the integrity check fired).
    assert.equal(backend.files.get(filePath), priorGoodContent, "main file was restored from .bak after the integrity-check failure");
  });

  test("post-write readback failure (readFile rejects after the write) propagates as the readback error", async () => {

    /* The framework's readback is wrapped in `try { backend.readFile } catch { LOG.error; throw }`. When the readback ITSELF fails (not a content mismatch but
     * an outright read failure), the framework logs and re-throws the original error.
     */
    const backend = makeMemoryStorageBackend();
    const filePath = "/data/readback-fail.json";

    // Seed initial content so the framework's pre-mutate read succeeds; only the post-write readback trips.
    backend.files.set(filePath, "{\"value\":1}\n");
    backend.mtimes.set(filePath, 1);

    let readFileCallCount = 0;
    const realReadFile = backend.readFile;

    backend.readFile = async (p: string): Promise<string> => {

      readFileCallCount += 1;

      // First call is the pre-mutate read; the second call is the post-write integrity readback. Trip on the second.
      if((readFileCallCount >= 2) && (p === filePath)) {

        throw new Error("synthetic-readback-failure");
      }

      return realReadFile(p);
    };

    const store = makeMemoryStore<{ value: number }>(backend, filePath, {

      defaultValue: () => ({ value: 0 })
    });

    await assert.rejects(() => store.mutate((data) => { data.value = 2; }), /synthetic-readback-failure/,
      "the readback's underlying error propagates verbatim - the framework wraps in a log line but rethrows the original");
  });
});

describe("FileStore.read - non-ENOENT file read error", () => {

  test("a permission/IO failure during read falls back to defaults rather than throwing", async () => {

    /* read() distinguishes "file does not exist" (ENOENT, normal first-run path) from "any other read failure" (permission denied, I/O error). The non-ENOENT
     * branch logs a warn and returns defaults rather than throwing - safer than crashing on a transient read failure.
     */
    const backend = makeMemoryStorageBackend({


      readFile: async (): Promise<string> => {

        const err = new Error("EACCES: permission denied") as NodeJS.ErrnoException;

        err.code = "EACCES";

        throw err;
      }
    });
    const filePath = "/data/permission-denied.json";
    const store = makeMemoryStore<{ value: number }>(backend, filePath, {

      defaultValue: () => ({ value: 42 })
    });

    const result = await store.read();

    assert.deepEqual(result.data, { value: 42 }, "read returns defaults on non-ENOENT failure - no throw");
    assert.equal(result.parseError, false, "parseError stays false (the file was unreadable, not corrupt)");
    assert.equal(result.recoveredFromBackup, false, "no backup recovery attempted - the failure is at the file-read layer");
  });
});

describe("FileStore.mutate - tryRecoverFromBackup restore-write failure", () => {

  test("when .bak parses but the restore-write to main fails, the recovered data is returned and a warn is logged", async () => {

    /* tryRecoverFromBackup attempts an atomic temp+rename to restore main from .bak. The restore is best-effort: if the write step fails, the function still
     * returns the recovered data (the in-memory state is good even if the on-disk restore could not complete) and logs a warn. We force the restore to fail
     * by overriding rename for the restore-tmp path; the framework's recovery still surfaces recovered=true with the .bak's content.
     */
    const backend = makeMemoryStorageBackend();
    const filePath = "/data/restore-write-fail.json";

    // Seed: corrupt main, valid .bak. The framework's read will detect the corrupt main, attempt to recover from .bak, and try to restore main.
    backend.files.set(filePath, "{not valid json");
    backend.mtimes.set(filePath, 1);
    backend.files.set(filePath + ".bak", "{\"value\":99}\n");
    backend.mtimes.set(filePath + ".bak", 2);

    // Override rename to fail when called against the recovery restore path. tryRecoverFromBackup restores main via a recovery-specific temp suffix (.recover.tmp,
    // distinct from doMutate's .tmp so the two write paths never collide), so we detect by source matching `<filePath>.recover.tmp` and destination matching
    // `filePath`. Other rename calls (e.g., the post-mutate atomic rename) flow through the default.
    const realRename = backend.rename;

    backend.rename = async (source: string, destination: string): Promise<void> => {

      if((source === (filePath + ".recover.tmp")) && (destination === filePath)) {

        throw new Error("synthetic-rename-failure");
      }

      return realRename(source, destination);
    };

    const store = makeMemoryStore<{ value: number }>(backend, filePath, {

      defaultValue: () => ({ value: 0 })
    });

    // read() drives tryRecoverFromBackup. The recovered data still surfaces despite the failed restore.
    const result = await store.read();

    assert.equal(result.recoveredFromBackup, true, "recoveredFromBackup is true even when the on-disk restore fails - in-memory state is what callers see");
    assert.deepEqual(result.data, { value: 99 }, "the recovered data matches the .bak content");

    // The warn line carries diagnostic context for operator triage.
    const warnings = captured.filter((line) => (line.level === "warn") && line.message.includes("failed to restore the main file"));

    assert.ok(warnings.length >= 1, "the restore-write failure is logged at warn level so operators see it on next read");
  });
});

describe("FileStore.mutate - backup copy non-ENOENT failure", () => {

  test("a non-ENOENT failure during the pre-write backup copy logs a warn and proceeds with the write", async () => {

    /* The pre-write backup copy is best-effort: ENOENT (no main file yet, first run) is silently swallowed; any OTHER error logs a warn and lets the write
     * proceed. We override copyFile to throw a non-ENOENT error and verify the mutate completes successfully and the new content lands on disk.
     */
    const backend = makeMemoryStorageBackend();
    const filePath = "/data/backup-eacces.json";

    backend.files.set(filePath, "{\"value\":1}\n");
    backend.mtimes.set(filePath, 1);

    const realCopyFile = backend.copyFile;

    backend.copyFile = async (source: string, destination: string): Promise<void> => {

      // The pre-write backup copies main to .bak. Detect by destination suffix. Snapshot copies (which use the snapshots/ subdir) are unaffected.
      if(destination === (filePath + ".bak")) {

        const err = new Error("EACCES: permission denied") as NodeJS.ErrnoException;

        err.code = "EACCES";

        throw err;
      }

      return realCopyFile(source, destination);
    };

    const store = makeMemoryStore<{ value: number }>(backend, filePath, {

      defaultValue: () => ({ value: 0 })
    });

    await assert.doesNotReject(() => store.mutate((data) => { data.value = 2; }), "mutate must complete when backup fails for a non-ENOENT reason");

    // The new content landed via the framework's stringifySorted-and-write pipeline. We assert that it is well-formed JSON whose value matches what we set,
    // rather than byte-comparing against an exact serialization shape (the framework's indent/spacing is not under test here).
    const written = backend.files.get(filePath);

    assert.ok(written, "main file has new content after the mutate");

    const parsed = JSON.parse(written) as { value: number };

    assert.equal(parsed.value, 2, "the new content reflects the user's mutation");

    // The warn line surfaces the underlying EACCES so operators see why the .bak rotation failed. ENOENT failures are silently swallowed; non-ENOENT must log.
    const warnings = captured.filter((line) => (line.level === "warn") && line.message.includes("Failed to back up"));

    assert.ok(warnings.length >= 1, "the non-ENOENT backup failure is logged at warn level");
  });
});
