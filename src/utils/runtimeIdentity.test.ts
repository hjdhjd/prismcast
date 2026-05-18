/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * runtimeIdentity.test.ts: Unit tests for the runtime-identity state machine. Each state-machine branch (free, held-live, stale-different-boot, stale-dead-pid,
 * stale-malformed) gets a dedicated test that pins the exact discriminator and (where applicable) the record payload. The file format is covered via round-trip
 * tests over serializeRecord/parseRecord. Tests use withTempDir for filesystem isolation and a hand-rolled RuntimeIdentityContext literal for deterministic
 * control over boot session ID and PID liveness - no real /proc or process.kill is exercised here.
 */
import type { IdentityRecord, RuntimeIdentityContext } from "./runtimeIdentity.ts";
import { claim, forceRelease, inspect, parseRecord, release, serializeRecord } from "./runtimeIdentity.ts";
import { describe, test } from "node:test";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import assert from "node:assert/strict";
import path from "node:path";
import { withTempDir } from "../testing.helpers.ts";

/* A deterministic RuntimeIdentityContext factory. Tests parameterize the boot session ID and a live-PID predicate; the rest of the state machine is pure. */
function makeCtx(opts: { bootId: string; livePids: ReadonlySet<number> }): RuntimeIdentityContext {

  return {

    getBootSessionId: () => opts.bootId,
    isProcessRunning: (pid: number): boolean => opts.livePids.has(pid)
  };
}

describe("inspect state machine", () => {

  test("free: returns kind 'free' when no file exists", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");
      const ctx = makeCtx({ bootId: "session-1", livePids: new Set() });

      assert.deepEqual(inspect(filePath, ctx), { kind: "free" });
    });
  });

  test("held-live: returns kind 'held-live' when bootId matches and pid is alive", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      // Pre-seed the file with a record matching the test's boot session and a "live" PID.
      writeFileSync(filePath, serializeRecord({ bootId: "session-1", pid: 12345, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

      const ctx = makeCtx({ bootId: "session-1", livePids: new Set([12345]) });
      const state = inspect(filePath, ctx);

      // assert.equal narrows state.kind to "held-live" via the asserts-clause type, which discriminates the union and exposes state.record without an extra wrapper.
      assert.equal(state.kind, "held-live");
      assert.equal(state.record.pid, 12345);
      assert.equal(state.record.bootId, "session-1");
      assert.equal(state.record.version, "1.10.3");
    });
  });

  test("stale-different-boot: returns kind 'stale-different-boot' when bootId differs (reboot case)", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      // The on-disk record claims a previous boot session. The current ctx reports a different one.
      writeFileSync(filePath, serializeRecord({ bootId: "previous-boot", pid: 653, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

      // Even if the PID happens to be alive (post-reboot recycling), the boot session mismatch alone classifies as stale.
      const ctx = makeCtx({ bootId: "current-boot", livePids: new Set([653]) });
      const state = inspect(filePath, ctx);

      assert.equal(state.kind, "stale-different-boot");
    });
  });

  test("stale-dead-pid: returns kind 'stale-dead-pid' when bootId matches but pid is not alive (same-boot crash)", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      writeFileSync(filePath, serializeRecord({ bootId: "session-1", pid: 99999, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

      const ctx = makeCtx({ bootId: "session-1", livePids: new Set() });
      const state = inspect(filePath, ctx);

      assert.equal(state.kind, "stale-dead-pid");
    });
  });

  test("stale-malformed: returns kind 'stale-malformed' when file contents cannot be parsed", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      // A pre-runtimeIdentity PID file (bare integer with no bootId line) is malformed under the new schema. The raw payload is preserved for diagnostic.
      writeFileSync(filePath, "12345\n");

      const ctx = makeCtx({ bootId: "session-1", livePids: new Set() });
      const state = inspect(filePath, ctx);

      assert.equal(state.kind, "stale-malformed");
      assert.equal(state.raw, "12345\n");
    });
  });
});

describe("claim", () => {

  test("succeeds when the slot is free and writes a record we can read back", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");
      const ctx = makeCtx({ bootId: "session-1", livePids: new Set([process.pid]) });

      const result = claim(filePath, { version: "1.10.3" }, ctx);

      assert.equal(result.ok, true);
      assert.equal(result.record.bootId, "session-1");
      assert.equal(result.record.pid, process.pid);
      assert.equal(result.record.version, "1.10.3");

      // The file now reports held-live on re-inspect with the same context (our own PID is in the live set).
      const after = inspect(filePath, ctx);

      assert.equal(after.kind, "held-live");
    });
  });

  test("succeeds when the existing record is from a different boot (overwrites silently)", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      writeFileSync(filePath, serializeRecord({ bootId: "previous-boot", pid: 653, startedAt: "2026-05-17T00:00:00Z", version: "1.10.2" }));

      const ctx = makeCtx({ bootId: "current-boot", livePids: new Set([ process.pid, 653 ]) });
      const result = claim(filePath, { version: "1.10.3" }, ctx);

      assert.equal(result.ok, true);

      // The on-disk record was overwritten with our PID and the current boot session.
      const after = inspect(filePath, ctx);

      assert.equal(after.kind, "held-live");
      assert.equal(after.record.bootId, "current-boot");
      assert.equal(after.record.pid, process.pid);
      assert.equal(after.record.version, "1.10.3");
    });
  });

  test("succeeds when the existing record's pid is dead (same-boot crash recovery)", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      writeFileSync(filePath, serializeRecord({ bootId: "session-1", pid: 99999, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

      const ctx = makeCtx({ bootId: "session-1", livePids: new Set([process.pid]) });
      const result = claim(filePath, { version: "1.10.3" }, ctx);

      assert.equal(result.ok, true);
    });
  });

  test("succeeds when the existing record is malformed (legacy / corrupt file)", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      writeFileSync(filePath, "garbage");

      const ctx = makeCtx({ bootId: "session-1", livePids: new Set([process.pid]) });
      const result = claim(filePath, { version: "1.10.3" }, ctx);

      assert.equal(result.ok, true);
    });
  });

  test("fails with the conflicting record when the slot is held-live", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      writeFileSync(filePath, serializeRecord({ bootId: "session-1", pid: 12345, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

      const ctx = makeCtx({ bootId: "session-1", livePids: new Set([12345]) });
      const result = claim(filePath, { version: "1.10.3" }, ctx);

      assert.equal(result.ok, false);
      assert.equal(result.conflict.pid, 12345);
      assert.equal(result.conflict.bootId, "session-1");

      // The on-disk record is unchanged - we did not overwrite the live holder.
      const reread = readFileSync(filePath, "utf-8");

      assert.match(reread, /^12345\n/);
    });
  });
});

describe("release", () => {

  test("removes the file when its record identifies the current process", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      // The file claims OUR PID, in our current boot session, and we are alive. release sees held-live + matching pid and removes.
      writeFileSync(filePath, serializeRecord({ bootId: "session-1", pid: process.pid, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

      const ctx = makeCtx({ bootId: "session-1", livePids: new Set([process.pid]) });

      release(filePath, ctx);

      assert.equal(existsSync(filePath), false);
    });
  });

  test("leaves the file alone when its record identifies a different live process (rejected-duplicate case)", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      // A live instance with a different PID owns the slot. A rejected-duplicate startup's exit handler must not delete this file.
      writeFileSync(filePath, serializeRecord({ bootId: "session-1", pid: 99999, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

      const ctx = makeCtx({ bootId: "session-1", livePids: new Set([ 99999, process.pid ]) });

      release(filePath, ctx);

      // File is unchanged - we did not delete the legitimate holder's record.
      assert.equal(existsSync(filePath), true);
    });
  });

  test("is a no-op when the slot is free", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");
      const ctx = makeCtx({ bootId: "session-1", livePids: new Set() });

      // Calling release twice on a missing file must not throw.
      release(filePath, ctx);
      release(filePath, ctx);

      assert.equal(existsSync(filePath), false);
    });
  });

  test("is a no-op when the existing record is from a different boot (the next startup will overwrite it)", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      writeFileSync(filePath, serializeRecord({ bootId: "previous-boot", pid: process.pid, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

      const ctx = makeCtx({ bootId: "current-boot", livePids: new Set([process.pid]) });

      release(filePath, ctx);

      // We did not own this file (different boot), so we leave it alone. The next claim() will overwrite it as stale-different-boot.
      assert.equal(existsSync(filePath), true);
    });
  });
});

describe("forceRelease", () => {

  test("removes the file unconditionally, even when it identifies a different live process", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      writeFileSync(filePath, serializeRecord({ bootId: "session-1", pid: 99999, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

      forceRelease(filePath);

      // The whole point of forceRelease is to bypass the safety check for explicit recovery flows.
      assert.equal(existsSync(filePath), false);
    });
  });

  test("is idempotent when the file is already absent", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      forceRelease(filePath);
      forceRelease(filePath);

      assert.equal(existsSync(filePath), false);
    });
  });
});

describe("serializeRecord / parseRecord round trip", () => {

  test("round-trip yields an equal record", () => {

    const original: IdentityRecord = { bootId: "boot-1", pid: 4242, startedAt: "2026-05-17T12:34:56Z", version: "1.10.3" };
    const parsed = parseRecord(serializeRecord(original));

    assert.deepEqual(parsed, original);
  });

  test("first line is the bare PID integer (backwards compatibility)", () => {

    // External tooling that grepped the old format expects the PID on its own as the first line. The masterclass invariant is that this never breaks.
    const serialized = serializeRecord({ bootId: "boot-1", pid: 4242, startedAt: "2026-05-17T12:34:56Z", version: "1.10.3" });

    assert.match(serialized, /^4242\n/);
  });

  test("parseRecord returns null when the first line is not a numeric PID", () => {

    assert.equal(parseRecord("not-a-pid\nbootId=x\n"), null);
  });

  test("parseRecord returns null when bootId is missing", () => {

    // A bare-integer file from a pre-runtimeIdentity PrismCast lacks the bootId line. It must be classified as malformed so the state machine overwrites it.
    assert.equal(parseRecord("4242\nstartedAt=2026-05-17\nversion=1.0.0\n"), null);
  });

  test("parseRecord ignores unknown keys for forward compatibility", () => {

    // A future writer could add fields; older readers must not refuse the record on their account.
    const raw = "4242\nbootId=b\nstartedAt=t\nversion=v\nfutureField=ignored\n";
    const parsed = parseRecord(raw);

    assert.deepEqual(parsed, { bootId: "b", pid: 4242, startedAt: "t", version: "v" });
  });

  test("parseRecord ignores blank lines and lines without '='", () => {

    const raw = "4242\n\nbootId=b\nthis-line-has-no-equals\nstartedAt=t\nversion=v\n";
    const parsed = parseRecord(raw);

    assert.deepEqual(parsed, { bootId: "b", pid: 4242, startedAt: "t", version: "v" });
  });
});
