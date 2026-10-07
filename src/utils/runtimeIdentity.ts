/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * runtimeIdentity.ts: Single source of truth for whether a PID file currently identifies a live PrismCast process. Composes the boot session port (bootSession.ts)
 * with the PID liveness primitives (pid.ts) into a discriminated-union state machine. Every caller that needs to decide "is another instance running?" or "is
 * this stale state I can overwrite?" goes through inspect() or claim() here - no other module makes ad-hoc PID-only judgments.
 *
 * On-disk file format. The format is line-oriented, with no escaping. The first line is the writer's PID as a bare positive integer, so external tools that grep
 * the integer (the universal pidfile convention) still work. Subsequent lines are key=value pairs: the boot session identifier (the field a mismatch or
 * downgrade depends on), commandHash, the fingerprint of the command line the OS process table reported for the writer at its claim, and informational fields
 * (startedAt, version). The fingerprint is a sha256 digest rather than the command line itself, because a command line can carry a newline, which in a format
 * with no escaping would read as a key line of its own. A writer whose command line the table could not report writes no commandHash line, the form a record
 * that a release before the field wrote also takes. The parser is defensive: a line that does not match the format is ignored and an unknown key is skipped,
 * so a reader tolerates the fields a newer writer adds, and a commandHash that is not 64 lowercase hex digits reads as absent. The PID and the boot session are
 * the required fields: a first line that is not a positive integer, or a boot session that is missing or empty, makes the record malformed, and
 * "stale-malformed" is safely overwritten on claim().
 *
 * State machine.
 *   - free                  : No file on disk. claim writes a fresh record.
 *   - held-live             : File exists, boot session matches, the PID is alive, and nothing shows the live process to be another program: its command line
 *                             fingerprints as the record's, the record carries no fingerprint, or the table reports no command line for the PID. claim refuses
 *                             and returns the holder's record.
 *   - stale-different-boot  : File exists but boot session differs. The writing process cannot still exist; safe to overwrite.
 *   - stale-dead-pid        : File exists, boot session matches, but the PID is no longer alive OR the live process at that PID reports a command line whose
 *                             fingerprint differs from the record's, another program that inherited the PID within the same boot. Each means "the writer is
 *                             gone"; safe to overwrite.
 *   - stale-malformed       : File exists but cannot be parsed (unrecognized format, partial write, corruption, a PID line that is not a positive integer, a
 *                             missing or empty boot session), or cannot be read (any read error other than ENOENT), reported with an empty raw. Safe to
 *                             overwrite.
 *
 * Same-boot PID reuse. The bootId check alone catches the cross-reboot case (a reboot mints a new boot session, so a recycled PID classifies as
 * stale-different-boot regardless of liveness). It cannot catch the same-boot residual: a SIGKILL of PrismCast followed by the kernel reassigning the freed PID
 * to an unrelated process within the same boot session leaves bootId matching and the PID alive, which would falsely read as held-live. The record therefore
 * names its writer by the fingerprint of the command line the OS process table reported for it at its claim, read through the processInspector port rather
 * than rebuilt from process.argv, which differs from what the table reports. A live same-boot PID whose command line fingerprints differently is another
 * program that inherited the PID, and the slot is stale. When identity cannot be determined - a record with no fingerprint, or a PID whose command line the
 * table cannot report because the table is unavailable on the platform or the PID is absent from it - we keep the held-live verdict: failing to confirm identity
 * must never downgrade a possibly-live holder to stale, since an overwritable verdict is the only path to two concurrent instances. The fingerprint holds only while the
 * table reports the command line the claim read, so a process that rewrote its title after its claim would read its own record as stale, which is why the server
 * sets no title.
 *
 * Concurrency note. claim() is not atomic against simultaneous startups - two callers racing on the same file may both pass inspect() and both write. Service
 * managers serialize startup so this is not a production concern; if a user manually launches two instances at once, the port-bind step (EADDRINUSE) catches
 * the collision downstream. A future hardening pass could acquire an advisory file lock here; the discriminated-union shape leaves that as a single-point
 * upgrade.
 */
import type { Nullable } from "../types/index.ts";
import { clearPidFile } from "./pid.ts";
import { createDefaultRuntimeIdentityContext } from "./runtimeIdentity.context.ts";
import { createHash } from "node:crypto";
import fs from "node:fs";

/**
 * The structured identity record persisted to disk.
 */
export interface IdentityRecord {

  // The boot session identifier at the moment the record was written. Compared for equality against the current boot session on read.
  readonly bootId: string;

  // The fingerprint of the writer's command line as the OS process table reported it at the claim, from fingerprintCommandLine. Null when the table could not
  // report it, and for a record a release before this field wrote.
  readonly commandHash: Nullable<string>;

  // The process ID of the writer. Combined with bootId, identifies the writing process uniquely across reboots and container restarts.
  readonly pid: number;

  // ISO-8601 timestamp of when the record was written. Informational only; never participates in correctness decisions.
  readonly startedAt: string;

  // The PrismCast version string at the moment the record was written. Informational; useful in held-live conflict diagnostics.
  readonly version: string;
}

/**
 * Discriminated union representing the on-disk state at a given path. Each variant carries only the data its state has. claim() and release() test for
 * held-live alone, so unless they are extended, claim() overwrites a new variant and release() leaves its file in place.
 */
export type IdentityState =
  { kind: "free" } |
  { kind: "held-live"; record: IdentityRecord } |
  { kind: "stale-different-boot"; record: IdentityRecord } |
  { kind: "stale-dead-pid"; record: IdentityRecord } |
  { kind: "stale-malformed"; raw: string };

/**
 * Outcome of a claim attempt. ok: true means we now own the slot; ok: false means another live instance holds it and we should not start.
 */
export type ClaimResult =
  { ok: true; record: IdentityRecord } |
  { ok: false; conflict: IdentityRecord };

/**
 * The runtime capability set inspect/claim consume. Production wires the defaults from real I/O via createDefaultRuntimeIdentityContext; tests pass a context
 * literal to drive each state-machine branch deterministically.
 */
export interface RuntimeIdentityContext {

  // Returns the command line the OS process table reports for a PID, exactly as reported, or null when the table has no row for it. It does no normalization:
  // the claim and inspect fingerprint what it returns through fingerprintCommandLine in this module, which applies one rule to each. Conventionally backed by
  // the processInspector port.
  readonly commandLineOf: (pid: number) => Nullable<string>;

  // Returns the current boot session identifier. Conventionally proxies getBootSessionId() from bootSession.ts.
  readonly getBootSessionId: () => string;

  // Returns whether a given PID belongs to a process that is currently alive. Conventionally proxies isProcessRunning() from pid.ts.
  readonly isProcessRunning: (pid: number) => boolean;

  // Returns the instant the claim record stamps as its start, in epoch milliseconds, so a test asserts the record's startedAt against a fixed value.
  // Conventionally the system clock's reading.
  readonly now: () => number;
}

/**
 * Inspects the identity file at the given path and reports the current state. Reads the file, parses the record, and combines boot session match with PID
 * liveness and process-identity verification to classify which branch of the state machine applies. Never mutates disk state.
 * @param filePath - The absolute path to the identity file.
 * @param ctx - The runtime identity context. Defaults to real I/O wiring.
 * @returns The current state.
 */
export function inspect(filePath: string, ctx: RuntimeIdentityContext = createDefaultRuntimeIdentityContext()): IdentityState {

  let raw: string;

  try {

    raw = fs.readFileSync(filePath, "utf-8");
  } catch(error: unknown) {

    // ENOENT is the canonical "no file" case: the slot is free.
    if((error as NodeJS.ErrnoException).code === "ENOENT") {

      return { kind: "free" };
    }

    // Any other read error (EACCES, EIO, ...) is reported as malformed-with-empty-raw, so claim() takes the overwrite path rather than crashing. Like a record
    // that fails to parse, this downgrade is made without proof that the holder is gone, a deliberate exception to the header's rule that unconfirmed identity
    // keeps held-live: an unreadable file holds no record to confirm, so keeping held-live would refuse every startup until the file was repaired by hand. The
    // overwrite either throws, when the temp write or the rename is refused, or replaces the file, and if a holder is still running behind it, this startup
    // meets the port bind the concurrency note describes.
    return { kind: "stale-malformed", raw: "" };
  }

  const record = parseRecord(raw);

  if(record === null) {

    return { kind: "stale-malformed", raw };
  }

  if(record.bootId !== ctx.getBootSessionId()) {

    return { kind: "stale-different-boot", record };
  }

  if(!ctx.isProcessRunning(record.pid)) {

    return { kind: "stale-dead-pid", record };
  }

  // The PID is alive and the boot session matches, but within a single boot the kernel can reassign a freed PID to another program after PrismCast was
  // SIGKILLed. The record carries the fingerprint of its writer's command line, so a live process at that PID whose command line fingerprints differently is
  // another program, the writer is gone, and the slot is stale. We classify that case as stale-dead-pid rather than a new branch because the operational meaning
  // is identical and callers already overwrite that state. Only that positive proof downgrades the slot: a record with no fingerprint reads no process table, and
  // a PID whose command line the table cannot report keeps the held-live verdict, because downgrading a possibly-live holder is the only path to two concurrent
  // instances.
  const liveHash = (record.commandHash === null) ? null : fingerprintCommandLine(ctx.commandLineOf(record.pid));

  if((liveHash !== null) && (liveHash !== record.commandHash)) {

    return { kind: "stale-dead-pid", record };
  }

  return { kind: "held-live", record };
}

/**
 * Attempts to claim the identity slot at the given path for the current process. If the slot is free or in any "stale-*" state, the record is overwritten
 * with our identity and { ok: true } is returned. If the slot is held-live by another process, no write occurs and { ok: false } is returned with the
 * conflicting holder's record so the caller can surface a precise diagnostic.
 * @param filePath - The absolute path to the identity file.
 * @param metadata - Caller-supplied metadata (the version string at minimum) that ends up in the persisted record.
 * @param ctx - The runtime identity context. Defaults to real I/O wiring.
 * @returns The claim result.
 */
export function claim(filePath: string, metadata: { version: string }, ctx: RuntimeIdentityContext = createDefaultRuntimeIdentityContext()): ClaimResult {

  const state = inspect(filePath, ctx);

  if(state.kind === "held-live") {

    return { conflict: state.record, ok: false };
  }

  const record: IdentityRecord = {

    bootId: ctx.getBootSessionId(),
    commandHash: fingerprintCommandLine(ctx.commandLineOf(process.pid)),
    pid: process.pid,
    startedAt: new Date(ctx.now()).toISOString(),
    version: metadata.version
  };

  writeRecord(filePath, record);

  return { ok: true, record };
}

/**
 * Releases the identity slot by removing the file at the given path - but only if the file's record identifies the current process. The PID-match check
 * inside makes ownership structural: a rejected duplicate startup's exit handler sees a held-live record belonging to a different PID and leaves the file
 * alone, while the legitimate holder sees its own PID and cleans up. Safe to call more than once: a missing file is not an error. Release is purely hygiene -
 * a process that fails to release on death has its file recovered transparently on the next startup via the stale-different-boot or stale-dead-pid branches.
 * @param filePath - The absolute path to the identity file.
 * @param ctx - The runtime identity context. Defaults to real I/O wiring.
 */
export function release(filePath: string, ctx: RuntimeIdentityContext = createDefaultRuntimeIdentityContext()): void {

  const state = inspect(filePath, ctx);

  // Only remove the file when it identifies us. Any other state (free, stale, or held-live by another process) means it is not ours to remove.
  if((state.kind !== "held-live") || (state.record.pid !== process.pid)) {

    return;
  }

  clearPidFile(filePath, "identity");
}

/**
 * Unconditionally removes the identity file at the given path. Intended for an explicit user-invoked reset or recovery flow where the caller has deliberately
 * asked to clear stale state without the safety check that release() provides. Prefer release() for normal lifecycle cleanup.
 * @param filePath - The absolute path to the identity file.
 */
export function forceRelease(filePath: string): void {

  clearPidFile(filePath, "identity");
}

/**
 * Fingerprints a command line as the identity record stores it: the lowercase hex sha256 digest of the trimmed text. It is the one normalization of a command
 * line, applied at the claim and at inspect alike, which is why it lives here rather than in the context: the platforms report surrounding whitespace
 * differently, and trimming the claim's reading and inspect's reading alike keeps a holder's command line matching its own record.
 * @param commandLine - The command line as the process table reported it, or null when the table had no row for the process.
 * @returns The fingerprint, or null when there is no command line to fingerprint.
 */
export function fingerprintCommandLine(commandLine: Nullable<string>): Nullable<string> {

  const text = commandLine?.trim() ?? "";

  if(text === "") {

    return null;
  }

  return createHash("sha256").update(text).digest("hex");
}

/**
 * Serializes a record to its on-disk representation. The first line is the bare PID for backwards compatibility with shell tooling; subsequent lines are
 * key=value pairs in deterministic order, the commandHash line written only when the record carries a fingerprint. The trailing newline keeps the file
 * well-formed for POSIX text-file tools.
 * @param record - The identity record to serialize.
 * @returns The serialized payload.
 */
export function serializeRecord(record: IdentityRecord): string {

  return String(record.pid) + "\n" +
    "bootId=" + record.bootId + "\n" +
    ((record.commandHash === null) ? "" : "commandHash=" + record.commandHash + "\n") +
    "startedAt=" + record.startedAt + "\n" +
    "version=" + record.version + "\n";
}

/**
 * Parses the on-disk representation of an identity record. Returns null when the payload cannot be interpreted as a complete record: a first line that is not a
 * positive integer PID, or a boot session that is missing or empty. A commandHash that is not a well-formed fingerprint reads as absent rather than refusing the
 * record, and unknown key=value pairs are silently ignored so future fields can be added without breaking older readers.
 * @param raw - The raw file contents.
 * @returns The parsed record, or null when the payload is unusable.
 */
export function parseRecord(raw: string): Nullable<IdentityRecord> {

  const lines = raw.split("\n");

  // String.prototype.split always returns at least one element, so this guard can never actually fire - it is belt-and-suspenders defense that documents the
  // empty-payload intent and keeps the array-access below structurally safe even if the split contract were ever to change.
  if(lines.length === 0) {

    return null;
  }

  const firstLine = lines[0];

  if(firstLine === undefined) {

    return null;
  }

  // The PID is a required field, so a first line that is not all digits, or whose value is zero, names no process and the record is malformed. Reading the line
  // whole, rather than by its leading digits, keeps a corrupt line from naming whatever PID its digits happen to spell.
  const pidText = firstLine.trim();
  const pid = Number(pidText);

  if(!/^\d+$/.test(pidText) || (pid <= 0)) {

    return null;
  }

  let bootId: Nullable<string> = null;
  let commandHash: Nullable<string> = null;
  let startedAt = "";
  let version = "";

  for(let i = 1; i < lines.length; i++) {

    const line = lines[i];

    if(line === undefined) {

      continue;
    }

    const trimmed = line.trim();

    if(trimmed === "") {

      continue;
    }

    const eq = trimmed.indexOf("=");

    if(eq === -1) {

      continue;
    }

    const key = trimmed.slice(0, eq);
    const value = trimmed.slice(eq + 1);

    switch(key) {

      case "bootId": {

        bootId = value;

        break;
      }

      // Only a sha256 digest in lowercase hex is a fingerprint fingerprintCommandLine could have written, so any other value reads as no fingerprint at all.
      case "commandHash": {

        commandHash = /^[0-9a-f]{64}$/.test(value) ? value : null;

        break;
      }

      case "startedAt": {

        startedAt = value;

        break;
      }

      case "version": {

        version = value;

        break;
      }

      default: {

        // Unknown keys are silently ignored to support forward compatibility - older readers must tolerate fields added in newer writers.
      }
    }
  }

  // The boot session is the other required field. A file without one, or with an empty one, gives the state machine nothing to compare against the current
  // boot, whether an external tool or a PrismCast that predates the identity record wrote it or it is corrupt, so it is treated as malformed.
  if((bootId === null) || (bootId === "")) {

    return null;
  }

  return { bootId, commandHash, pid, startedAt, version };
}

/**
 * Persists a record to disk via atomic write (write-temp + rename). On POSIX rename is atomic within a filesystem; on Windows the rename is best-effort but
 * the partial-write window remains negligible. A torn write would parse as malformed, which the state machine recovers from on the next inspect - so atomicity
 * here is hygiene, not something callers depend on for correctness.
 * @param filePath - The absolute path to the identity file.
 * @param record - The record to persist.
 */
function writeRecord(filePath: string, record: IdentityRecord): void {

  const payload = serializeRecord(record);
  const tempPath = filePath + ".tmp";

  fs.writeFileSync(tempPath, payload, "utf-8");
  fs.renameSync(tempPath, filePath);
}
