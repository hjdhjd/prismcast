/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * runtimeIdentity.test.ts: Unit tests for the runtime-identity state machine. Each state-machine branch (free, held-live, stale-different-boot, stale-dead-pid,
 * stale-malformed) gets a dedicated test that asserts the exact kind tag and (where applicable) the record payload. The file format is covered via round-trip
 * tests over serializeRecord/parseRecord. Tests use withTempDir for filesystem isolation and a hand-rolled RuntimeIdentityContext literal for deterministic
 * control over boot session ID, PID liveness, and the command line the process table reports - no real /proc or process.kill is exercised here.
 */
import type { IdentityRecord, RuntimeIdentityContext } from "./runtimeIdentity.ts";
import { claim, fingerprintCommandLine, forceRelease, inspect, parseRecord, release, serializeRecord } from "./runtimeIdentity.ts";
import { describe, test } from "node:test";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import type { Nullable } from "../types/index.ts";
import assert from "node:assert/strict";
import path from "node:path";
import { withTempDir } from "../testing.helpers.ts";

// The instant every context this file builds reports, so a claim record's startedAt is a fixed value rather than a reading of the host clock.
const CLAIM_INSTANT_MS = 1700000000000;

// The command line every context this file builds reports for a PID unless a row passes its own reader, in the form a service launch takes.
const HOLDER_COMMAND_LINE = "/usr/local/bin/node /opt/prismcast/dist/index.js";

// The fingerprint of HOLDER_COMMAND_LINE, which a seeded record carries when it stands for a record this release writes, so the default context's live process
// at the record's PID reads as its writer.
const HOLDER_HASH = fingerprintCommandLine(HOLDER_COMMAND_LINE);

/* A deterministic RuntimeIdentityContext factory. Tests parameterize the boot session ID, a live-PID predicate, and an optional command-line reader; the rest of
 * the state machine is pure. The reader defaults to reporting HOLDER_COMMAND_LINE for every PID, so a record seeded with HOLDER_HASH reads as held by its live
 * writer and the common held-live path stays concise; the same-boot PID-reuse cases pass a reader that reports a different command line for a PID, or null when
 * the process table has no row for it. The instant is fixed, so a claim's startedAt is a value the rows assert rather than whatever the host clock read when
 * they ran.
 */
function makeCtx(opts: { bootId: string; commandLineOf?: (pid: number) => Nullable<string>; livePids: ReadonlySet<number> }): RuntimeIdentityContext {

  return {

    commandLineOf: opts.commandLineOf ?? ((): string => HOLDER_COMMAND_LINE),
    getBootSessionId: () => opts.bootId,
    isProcessRunning: (pid: number): boolean => opts.livePids.has(pid),
    now: (): number => CLAIM_INSTANT_MS
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
      writeFileSync(filePath, serializeRecord({ bootId: "session-1", commandHash: HOLDER_HASH, pid: 12345, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

      const ctx = makeCtx({ bootId: "session-1", livePids: new Set([12345]) });
      const state = inspect(filePath, ctx);

      // assert.equal narrows state.kind to "held-live" via the asserts-clause type, which narrows the union and exposes state.record without an extra wrapper.
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
      writeFileSync(filePath, serializeRecord({ bootId: "previous-boot", commandHash: HOLDER_HASH, pid: 653, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

      // Even if the PID happens to be alive (post-reboot recycling), the boot session mismatch alone classifies as stale.
      const ctx = makeCtx({ bootId: "current-boot", livePids: new Set([653]) });
      const state = inspect(filePath, ctx);

      assert.equal(state.kind, "stale-different-boot");
    });
  });

  test("stale-dead-pid: returns kind 'stale-dead-pid' when bootId matches but pid is not alive (same-boot crash)", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      writeFileSync(filePath, serializeRecord({ bootId: "session-1", commandHash: HOLDER_HASH, pid: 99999, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

      const ctx = makeCtx({ bootId: "session-1", livePids: new Set() });
      const state = inspect(filePath, ctx);

      assert.equal(state.kind, "stale-dead-pid");
    });
  });

  test("stale-dead-pid: returns kind 'stale-dead-pid' when the live PID's command line differs from the one its record fingerprints (same-boot PID reuse)", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      // The on-disk record matches the current boot session and its PID is alive, but the live process at that PID is not the writer - the original was
      // SIGKILLed and the kernel reassigned the freed PID to another program within the same boot. That program's command line mentions the product, as a log
      // tail's does, and still fingerprints differently from the writer's, which is what downgrades the slot from held-live to stale.
      writeFileSync(filePath, serializeRecord({ bootId: "session-1", commandHash: HOLDER_HASH, pid: 12345, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

      const ctx = makeCtx({ bootId: "session-1", commandLineOf: (): string => "/usr/bin/tail -f prismcast.log", livePids: new Set([12345]) });
      const state = inspect(filePath, ctx);

      // The reused-PID-but-different-process holder is treated as NOT live: the slot is stale and the next claim() may overwrite it.
      assert.equal(state.kind, "stale-dead-pid");
    });
  });

  test("held-live: keeps kind 'held-live' when the live PID reports the command line its record fingerprints", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      writeFileSync(filePath, serializeRecord({ bootId: "session-1", commandHash: HOLDER_HASH, pid: 12345, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

      // The live process at the record's PID reports the writer's own command line, so the fingerprints match and the slot remains held-live.
      const ctx = makeCtx({ bootId: "session-1", commandLineOf: (): string => HOLDER_COMMAND_LINE, livePids: new Set([12345]) });
      const state = inspect(filePath, ctx);

      assert.equal(state.kind, "held-live");
    });
  });

  test("held-live: keeps kind 'held-live' when the live PID reports its recorded command line with surrounding whitespace", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      writeFileSync(filePath, serializeRecord({ bootId: "session-1", commandHash: HOLDER_HASH, pid: 12345, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

      // A platform can report a command line with whitespace around it that the claim's reading lacked. The fingerprint trims the claim's reading and the live
      // one alike, so the holder still matches its own record.
      const ctx = makeCtx({ bootId: "session-1", commandLineOf: (): string => HOLDER_COMMAND_LINE + " ", livePids: new Set([12345]) });
      const state = inspect(filePath, ctx);

      assert.equal(state.kind, "held-live");
    });
  });

  test("held-live: keeps kind 'held-live' conservatively when the process table reports no command line for the PID", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      writeFileSync(filePath, serializeRecord({ bootId: "session-1", commandHash: HOLDER_HASH, pid: 12345, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

      // commandLineOf returns null: the process table is unavailable on the platform or the PID is absent from it, so identity cannot be determined. We must NOT
      // downgrade a possibly-live holder to stale on weak evidence, because the stale branch is the only one that risks two concurrent instances.
      const ctx = makeCtx({ bootId: "session-1", commandLineOf: (): Nullable<string> => null, livePids: new Set([12345]) });
      const state = inspect(filePath, ctx);

      assert.equal(state.kind, "held-live");
    });
  });

  test("held-live: keeps kind 'held-live' for a record with no fingerprint whatever runs at its live PID", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      // A record with no fingerprint, the form a release before the field writes, holds no commandHash line. Nothing then proves the live process at its PID is
      // another program, so the slot stays held-live and an upgrade started beside that release is refused.
      writeFileSync(filePath, serializeRecord({ bootId: "session-1", commandHash: null, pid: 12345, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

      assert.equal(readFileSync(filePath, "utf-8").includes("commandHash="), false, "a record with no fingerprint writes no commandHash line");

      const ctx = makeCtx({ bootId: "session-1", commandLineOf: (): string => "/usr/bin/unrelated", livePids: new Set([12345]) });
      const state = inspect(filePath, ctx);

      assert.equal(state.kind, "held-live");
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
      assert.equal(result.record.startedAt, new Date(CLAIM_INSTANT_MS).toISOString(), "the record stamps the context's instant");
      assert.equal(result.record.version, "1.10.3");

      // The file now reports held-live on re-inspect with the same context (our own PID is in the live set).
      const after = inspect(filePath, ctx);

      assert.equal(after.kind, "held-live");
    });
  });

  test("succeeds when the existing record is from a different boot (overwrites silently)", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      writeFileSync(filePath, serializeRecord({ bootId: "previous-boot", commandHash: HOLDER_HASH, pid: 653, startedAt: "2026-05-17T00:00:00Z", version: "1.10.2" }));

      const ctx = makeCtx({ bootId: "current-boot", livePids: new Set([ process.pid, 653 ]) });
      const result = claim(filePath, { version: "1.10.3" }, ctx);

      assert.equal(result.ok, true);

      // The on-disk record was overwritten with our PID and the current boot session.
      const after = inspect(filePath, ctx);

      assert.equal(after.kind, "held-live");
      assert.equal(after.record.bootId, "current-boot");
      assert.equal(after.record.pid, process.pid);
      assert.equal(after.record.startedAt, new Date(CLAIM_INSTANT_MS).toISOString(), "the overwriting record stamps the context's instant");
      assert.equal(after.record.version, "1.10.3");
    });
  });

  test("succeeds when the existing record's pid is dead (same-boot crash recovery)", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      writeFileSync(filePath, serializeRecord({ bootId: "session-1", commandHash: HOLDER_HASH, pid: 99999, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

      const ctx = makeCtx({ bootId: "session-1", livePids: new Set([process.pid]) });
      const result = claim(filePath, { version: "1.10.3" }, ctx);

      assert.equal(result.ok, true);
    });
  });

  test("succeeds when the holder PID is alive but recycled to a different process (same-boot PID-reuse residual)", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      // A SIGKILLed PrismCast left this record; the kernel then reassigned its PID to another live program within the same boot. Without the fingerprint this
      // would falsely report "another instance is already running". With it, the recycled PID classifies as stale and the claim succeeds.
      writeFileSync(filePath, serializeRecord({ bootId: "session-1", commandHash: HOLDER_HASH, pid: 12345, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

      const ctx = makeCtx({

        bootId: "session-1",
        commandLineOf: (pid: number): string => (pid === 12345) ? "/usr/bin/tail -f prismcast.log" : HOLDER_COMMAND_LINE,
        livePids: new Set([ 12345, process.pid ])
      });

      const result = claim(filePath, { version: "1.10.3" }, ctx);

      assert.equal(result.ok, true);

      // The slot now belongs to us: the recycled holder's record was overwritten with our identity.
      const after = inspect(filePath, ctx);

      assert.equal(after.kind, "held-live");
      assert.equal(after.record.pid, process.pid);
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

      writeFileSync(filePath, serializeRecord({ bootId: "session-1", commandHash: HOLDER_HASH, pid: 12345, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

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

  test("records the fingerprint of the command line the process table reports for this process", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");
      const ownCommandLine = "node dist/index.js --port 5590";
      const ctx = makeCtx({ bootId: "session-1", commandLineOf: (pid: number): Nullable<string> => (pid === process.pid) ? ownCommandLine : null,
        livePids: new Set([process.pid]) });

      assert.equal(claim(filePath, { version: "1.10.3" }, ctx).ok, true);

      // The record carries the digest of the command line, never the command line itself, as 64 lowercase hex digits.
      const line = readFileSync(filePath, "utf-8").split("\n").find((entry) => entry.startsWith("commandHash=")) ?? "";

      assert.match(line, /^commandHash=[0-9a-f]{64}$/);
      assert.equal(line.slice("commandHash=".length), fingerprintCommandLine(ownCommandLine));
    });
  });

  test("records a command line carrying a newline without letting it forge a field", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      // A Linux command line can carry a newline. A record holding the raw text would read the line after it back as a key line of its own, here replacing the
      // boot session; the fingerprint holds hex digits alone, so the record reads back with the context's boot session.
      const ctx = makeCtx({ bootId: "session-1", commandLineOf: (): string => "node app.js\nbootId=forged", livePids: new Set([process.pid]) });

      assert.equal(claim(filePath, { version: "1.10.3" }, ctx).ok, true);
      assert.equal(parseRecord(readFileSync(filePath, "utf-8"))?.bootId, "session-1");
    });
  });
});

describe("release", () => {

  test("removes the file when its record identifies the current process", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      // The file claims OUR PID, in our current boot session, and we are alive. release sees held-live + matching pid and removes.
      writeFileSync(filePath, serializeRecord({ bootId: "session-1", commandHash: HOLDER_HASH, pid: process.pid, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

      const ctx = makeCtx({ bootId: "session-1", livePids: new Set([process.pid]) });

      release(filePath, ctx);

      assert.equal(existsSync(filePath), false);
    });
  });

  test("removes the file after a claim whose command line does not name the product (an npm start or dev-wrapper launch)", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");
      const ctx = makeCtx({ bootId: "session-1", commandLineOf: (pid: number): Nullable<string> => (pid === process.pid) ? "node dist/index.js --port 5590" : null,
        livePids: new Set([process.pid]) });

      // The claim records the fingerprint of the command line this process was launched with, so release recognizes the record as its own however it was
      // launched.
      assert.equal(claim(filePath, { version: "1.10.3" }, ctx).ok, true);

      release(filePath, ctx);

      assert.equal(existsSync(filePath), false);
    });
  });

  test("leaves the file alone when its record identifies a different live process (rejected-duplicate case)", async () => {

    await withTempDir(async (dir) => {

      const filePath = path.join(dir, "identity");

      // A live instance with a different PID owns the slot. A rejected-duplicate startup's exit handler must not delete this file.
      writeFileSync(filePath, serializeRecord({ bootId: "session-1", commandHash: HOLDER_HASH, pid: 99999, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

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

      writeFileSync(filePath, serializeRecord({ bootId: "previous-boot", commandHash: HOLDER_HASH, pid: process.pid, startedAt: "2026-05-17T00:00:00Z",
        version: "1.10.3" }));

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

      writeFileSync(filePath, serializeRecord({ bootId: "session-1", commandHash: HOLDER_HASH, pid: 99999, startedAt: "2026-05-17T00:00:00Z", version: "1.10.3" }));

      forceRelease(filePath);

      // The whole point of forceRelease is to bypass the safety check for explicit recovery flows.
      assert.equal(existsSync(filePath), false);
    });
  });

  test("is a no-op when the file is already absent", async () => {

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

    const original: IdentityRecord = { bootId: "boot-1", commandHash: HOLDER_HASH, pid: 4242, startedAt: "2026-05-17T12:34:56Z", version: "1.10.3" };
    const parsed = parseRecord(serializeRecord(original));

    assert.deepEqual(parsed, original);
  });

  test("first line is the bare PID integer (backwards compatibility)", () => {

    // External tooling that greps the bare PID convention expects it on its own as the first line. This never breaks, even as future writers add fields.
    const serialized = serializeRecord({ bootId: "boot-1", commandHash: HOLDER_HASH, pid: 4242, startedAt: "2026-05-17T12:34:56Z", version: "1.10.3" });

    assert.match(serialized, /^4242\n/);
  });

  test("parseRecord returns null when the first line is not a numeric PID", () => {

    assert.equal(parseRecord("not-a-pid\nbootId=x\n"), null);
  });

  test("parseRecord returns null when the PID line carries trailing text after its digits", () => {

    assert.equal(parseRecord("4242abc\nbootId=b\n"), null);
  });

  test("parseRecord returns null when the PID is zero", () => {

    assert.equal(parseRecord("0\nbootId=b\n"), null);
  });

  test("parseRecord returns null when bootId is missing", () => {

    // A bare-integer file from a pre-runtimeIdentity PrismCast lacks the bootId line. It must be classified as malformed so the state machine overwrites it.
    assert.equal(parseRecord("4242\nstartedAt=2026-05-17\nversion=1.0.0\n"), null);
  });

  test("parseRecord returns null when the boot session is empty", () => {

    assert.equal(parseRecord("4242\nbootId=\nstartedAt=t\nversion=v\n"), null);
  });

  test("parseRecord reads a record with no commandHash line as a record with no fingerprint", () => {

    // The form a release before the field writes, and the form a writer whose command line the process table could not report writes.
    assert.deepEqual(parseRecord("4242\nbootId=b\nstartedAt=t\nversion=v\n"), { bootId: "b", commandHash: null, pid: 4242, startedAt: "t", version: "v" });
  });

  test("parseRecord reads a commandHash that is not 64 lowercase hex digits as no fingerprint", () => {

    assert.equal(parseRecord("4242\nbootId=b\ncommandHash=" + "A".repeat(64) + "\n")?.commandHash, null, "uppercase hex is not a fingerprint");
    assert.equal(parseRecord("4242\nbootId=b\ncommandHash=" + "a".repeat(63) + "\n")?.commandHash, null, "a short digest is not a fingerprint");
  });

  test("parseRecord ignores unknown keys for forward compatibility", () => {

    // A future writer could add fields; older readers must not refuse the record on their account.
    const raw = "4242\nbootId=b\nstartedAt=t\nversion=v\nfutureField=ignored\n";
    const parsed = parseRecord(raw);

    assert.deepEqual(parsed, { bootId: "b", commandHash: null, pid: 4242, startedAt: "t", version: "v" });
  });

  test("parseRecord ignores blank lines and lines without '='", () => {

    const raw = "4242\n\nbootId=b\nthis-line-has-no-equals\nstartedAt=t\nversion=v\n";
    const parsed = parseRecord(raw);

    assert.deepEqual(parsed, { bootId: "b", commandHash: null, pid: 4242, startedAt: "t", version: "v" });
  });
});
