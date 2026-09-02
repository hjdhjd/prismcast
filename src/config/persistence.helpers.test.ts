/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * persistence.helpers.test.ts: Self-tests for the in-memory storage backend used by the persistence framework's test suites. Helpers earn the same coverage
 * rigor as production code - a bug in the backend would cascade through every dependent test, masking or fabricating failures. The tests assert every operation's
 * contract: presence/absence semantics, ENOENT shape, mtime monotonicity, override-takes-precedence-over-default, and the StorageBackend surface conformance.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { makeMemoryStorageBackend } from "./persistence.helpers.ts";

describe("makeMemoryStorageBackend - default behavior", () => {

  test("readFile of a missing path throws ENOENT with the expected code", async () => {

    // The framework's branch detection inspects err.code === "ENOENT" - the memory backend must produce errors with this exact shape.
    const backend = makeMemoryStorageBackend();

    await assert.rejects(() => backend.readFile("/nope"), (err: NodeJS.ErrnoException) => (err.code === "ENOENT"));
  });

  test("writeFile then readFile round-trips the content", async () => {

    const backend = makeMemoryStorageBackend();

    await backend.writeFile("/file.json", "hello");

    const content = await backend.readFile("/file.json");

    assert.equal(content, "hello");
  });

  test("writeFile bumps the mtime monotonically per call", async () => {

    // Two writes to the same path produce strictly increasing mtimeMs values. The framework's snapshot pruning sorts by mtime descending; ties on mtimeMs would
    // produce non-deterministic order. The counter-based mtime guarantees no ties even within the same millisecond.
    const backend = makeMemoryStorageBackend();

    await backend.writeFile("/a", "1");

    const firstMtime = (await backend.stat("/a")).mtimeMs;

    await backend.writeFile("/a", "2");

    const secondMtime = (await backend.stat("/a")).mtimeMs;

    assert.ok(secondMtime > firstMtime, "second writeFile must produce a strictly greater mtime");
  });

  test("copyFile copies content and stamps a new mtime on the destination", async () => {

    const backend = makeMemoryStorageBackend();

    await backend.writeFile("/source", "payload");

    const sourceMtime = (await backend.stat("/source")).mtimeMs;

    await backend.copyFile("/source", "/destination");

    assert.equal(await backend.readFile("/destination"), "payload");

    const destinationMtime = (await backend.stat("/destination")).mtimeMs;

    assert.ok(destinationMtime > sourceMtime, "destination mtime must be strictly greater than source - copy is a write");
  });

  test("copyFile from a missing source throws ENOENT", async () => {

    const backend = makeMemoryStorageBackend();

    await assert.rejects(() => backend.copyFile("/nope", "/whatever"), (err: NodeJS.ErrnoException) => (err.code === "ENOENT"));
  });

  test("rename moves content and mtime atomically; source disappears", async () => {

    // The framework's atomic temp+rename relies on rename preserving content while making the source unreachable. We verify both halves so a regression that
    // copied instead of moved would surface.
    const backend = makeMemoryStorageBackend();

    await backend.writeFile("/tmp", "renamed-content");

    await backend.rename("/tmp", "/final");

    assert.equal(await backend.readFile("/final"), "renamed-content");

    await assert.rejects(() => backend.readFile("/tmp"), (err: NodeJS.ErrnoException) => (err.code === "ENOENT"));
  });

  test("rename preserves the source mtime on the destination", async () => {

    // Unlike copyFile (which is a write and stamps a new mtime), rename moves the file in place - the framework's snapshot test relies on this contract to
    // verify per-file pruning order.
    const backend = makeMemoryStorageBackend();

    await backend.writeFile("/tmp", "x");

    const originalMtime = (await backend.stat("/tmp")).mtimeMs;

    await backend.rename("/tmp", "/final");

    const finalMtime = (await backend.stat("/final")).mtimeMs;

    assert.equal(finalMtime, originalMtime, "rename must preserve mtime - it is a move, not a copy");
  });

  test("unlink removes the file; subsequent reads throw ENOENT", async () => {

    const backend = makeMemoryStorageBackend();

    await backend.writeFile("/doomed", "x");

    await backend.unlink("/doomed");

    await assert.rejects(() => backend.readFile("/doomed"), (err: NodeJS.ErrnoException) => (err.code === "ENOENT"));
  });

  test("unlink of a missing file throws ENOENT", async () => {

    const backend = makeMemoryStorageBackend();

    await assert.rejects(() => backend.unlink("/nope"), (err: NodeJS.ErrnoException) => (err.code === "ENOENT"));
  });

  test("stat of a missing file throws ENOENT", async () => {

    const backend = makeMemoryStorageBackend();

    await assert.rejects(() => backend.stat("/nope"), (err: NodeJS.ErrnoException) => (err.code === "ENOENT"));
  });

  test("mkdir is a no-op (directories are implicit)", async () => {

    // Memory backend does not track directories explicitly; mkdir resolves without recording any state. A subsequent writeFile under the directory works without
    // a prior mkdir, mirroring the framework's implicit-parent assumption.
    const backend = makeMemoryStorageBackend();

    await assert.doesNotReject(() => backend.mkdir("/some/nested/dir"));

    await backend.writeFile("/some/nested/dir/file", "content");

    assert.equal(await backend.readFile("/some/nested/dir/file"), "content");
  });

  test("access succeeds on a known file", async () => {

    const backend = makeMemoryStorageBackend();

    await backend.writeFile("/exists", "x");

    await assert.doesNotReject(() => backend.access("/exists"));
  });

  test("access on a directory prefix succeeds when at least one file lives under it", async () => {

    // The snapshot path (e.g., "/data/snapshots") is a directory; access is used to test for the existence of a snapshot file whose creation is safe to
    // repeat. We mirror that pattern: writing /data/snapshots/foo.json.v1 makes /data/snapshots/foo.json.v1 reachable via access and ALSO makes
    // /data/snapshots reachable as a directory (because at least one file lives under it).
    const backend = makeMemoryStorageBackend();

    await backend.writeFile("/data/snapshots/foo.json.v1", "x");

    await assert.doesNotReject(() => backend.access("/data/snapshots/foo.json.v1"));
    await assert.doesNotReject(() => backend.access("/data/snapshots"));
  });

  test("access on a missing path throws ENOENT", async () => {

    const backend = makeMemoryStorageBackend();

    await assert.rejects(() => backend.access("/nope"), (err: NodeJS.ErrnoException) => (err.code === "ENOENT"));
  });

  test("readdir returns the basenames of files directly under the directory", async () => {

    // The framework's snapshot pruning calls readdir on the snapshots directory and expects to see basenames it can match against a prefix. Subdirectory
    // entries surface as their first-segment name (we are not exercising deeper recursion in the framework itself).
    const backend = makeMemoryStorageBackend();

    await backend.writeFile("/dir/a", "1");
    await backend.writeFile("/dir/b", "2");
    await backend.writeFile("/dir/sub/c", "3");

    const entries = await backend.readdir("/dir");

    assert.deepEqual(entries.toSorted(), [ "a", "b", "sub" ], "readdir lists basenames including subdirectory entries as their first segment");
  });

  test("readdir of an empty directory returns an empty array (no throw)", async () => {

    // Memory backend does not distinguish "empty dir" from "missing dir" - both return []. This deviates slightly from real fs semantics (real fs throws ENOENT
    // for missing dirs) but the framework's pruneSnapshots tolerates [] uniformly via the early-return when matching.length <= retention, so the deviation is
    // benign for framework tests. Tests that need a missing-dir behavior override readdir explicitly.
    const backend = makeMemoryStorageBackend();

    const entries = await backend.readdir("/empty");

    assert.deepEqual(entries, []);
  });

  test("the underlying maps (files, mtimes) are accessible for direct seeding and inspection", async () => {

    // Tests sometimes seed initial state directly into the maps rather than going through the backend operations. We expose them on the returned object so the
    // backend can be used as a pure data structure when convenient.
    const backend = makeMemoryStorageBackend();

    backend.files.set("/seeded", "hello");
    backend.mtimes.set("/seeded", 1);

    assert.equal(await backend.readFile("/seeded"), "hello");
    assert.equal((await backend.stat("/seeded")).mtimeMs, 1);
  });
});

describe("makeMemoryStorageBackend - override hooks", () => {

  test("an override REPLACES the default operation entirely", async () => {

    // When override.readFile is supplied, the default in-memory readFile is bypassed - the override is the only thing that runs. We verify by overriding to
    // throw a custom error and confirming the seeded file is unreachable through readFile (but still reachable via files.get).
    const backend = makeMemoryStorageBackend({


      readFile: async (): Promise<string> => {

        throw new Error("override-ran");
      }
    });

    backend.files.set("/file", "default-content");

    await assert.rejects(() => backend.readFile("/file"), /override-ran/);

    // Direct map access still sees the seeded content - confirms the override only replaces the operation, not the underlying state.
    assert.equal(backend.files.get("/file"), "default-content");
  });

  test("absent overrides keep the default behavior; partial override leaves siblings working", async () => {

    // Overriding writeFile alone must not affect readFile, copyFile, etc. We override writeFile to corrupt the content and verify (a) the corruption lands, and
    // (b) other operations still flow through their defaults.
    const backend = makeMemoryStorageBackend({



      writeFile: async (path: string): Promise<void> => {

        // Corrupt: store a different value than what the caller passed. The framework's post-write integrity check would catch this on a real store.
        backend.files.set(path, "CORRUPT");
      }
    });

    await backend.writeFile("/file", "intended");

    // Default readFile still works against the underlying map.
    assert.equal(await backend.readFile("/file"), "CORRUPT");
  });

  test("an override that throws an ENOENT-shaped error is indistinguishable from the default ENOENT", async () => {

    // Tests that simulate "missing" need to throw with code: "ENOENT" so the framework's branch detection fires. Confirm a hand-built ENOENT propagates with
    // the expected shape.
    const enoent = new Error("synthetic-not-found") as NodeJS.ErrnoException;

    enoent.code = "ENOENT";

    const backend = makeMemoryStorageBackend({


      readFile: async (): Promise<string> => {

        throw enoent;
      }
    });

    await assert.rejects(() => backend.readFile("/anything"), (err: NodeJS.ErrnoException) => (err.code === "ENOENT"));
  });
});
