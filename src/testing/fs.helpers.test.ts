/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fs.helpers.test.ts: Tests for withTempDir. The contract: a fresh directory exists during the callback, the callback's return value (or rejection) is
 * propagated, and the directory is removed regardless of resolve/reject. Coverage pins each leg of that contract plus a "different dir per call" check so
 * tests don't accidentally share state via a shared path.
 */
import { access, readFile, writeFile } from "node:fs/promises";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { withTempDir } from "./fs.helpers.ts";

describe("withTempDir", () => {

  test("invokes the callback with an existing absolute directory path", async () => {

    const observed = await withTempDir(async (dir) => {

      await access(dir);

      return dir;
    });

    assert.match(observed, /prismcast-test-/, "temp dir name should include the prismcast-test- prefix");
  });

  test("removes the directory after the callback resolves", async () => {

    const dir = await withTempDir((d) => Promise.resolve(d));

    await assert.rejects(() => access(dir), /ENOENT/, "directory should be gone after withTempDir resolves");
  });

  test("returns the value the callback resolves with", async () => {

    const result = await withTempDir(() => Promise.resolve(42));

    assert.equal(result, 42, "withTempDir should pass through the callback's return value");
  });

  test("removes the directory even when the callback rejects", async () => {

    let captured = "";

    await assert.rejects(

      () => withTempDir((dir) => {

        captured = dir;

        return Promise.reject(new Error("callback failed"));
      }),
      { message: "callback failed" },
      "the callback's rejection should propagate"
    );

    assert.notEqual(captured, "", "the callback should have run and recorded the dir");
    await assert.rejects(() => access(captured), /ENOENT/, "directory should be gone even though callback rejected");
  });

  test("cleans up a directory containing files", async () => {

    const dir = await withTempDir(async (d) => {

      await writeFile(d + "/file.txt", "content");
      await writeFile(d + "/other.txt", "more");

      return d;
    });

    await assert.rejects(() => access(dir), /ENOENT/, "directory with files should still be removed");
  });

  test("returns a different directory on each invocation", async () => {

    const a = await withTempDir((d) => Promise.resolve(d));
    const b = await withTempDir((d) => Promise.resolve(d));

    assert.notEqual(a, b, "two invocations should produce distinct paths");
  });

  test("preserves the value when the callback writes and reads its own files", async () => {

    const result = await withTempDir(async (dir) => {

      await writeFile(dir + "/payload.txt", "hello");

      return readFile(dir + "/payload.txt", "utf8");
    });

    assert.equal(result, "hello", "callback should be able to round-trip data through its temp dir");
  });
});
