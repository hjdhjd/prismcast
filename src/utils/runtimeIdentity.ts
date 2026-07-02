/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * runtimeIdentity.ts: Single source of truth for whether a PID file currently identifies a live PrismCast process. Composes the boot session port (bootSession.ts)
 * with the PID liveness primitives (pid.ts) into a discriminated-union state machine. Every caller that needs to decide "is another instance running?" or "is
 * this stale state I can overwrite?" goes through inspect() or claim() here - no other module makes ad-hoc PID-only judgments.
 *
 * On-disk file format. The format is line-oriented and backwards-compatible. The first line is the bare PID as an integer so external tools that grep the
 * integer (the universal pidfile convention) still work. Subsequent lines are key=value pairs holding the boot session identifier (the load-bearing invariant)
 * and informational fields (startedAt, version). The parser is defensive: any line that does not match the format is ignored; missing required fields
 * downgrade the state to "stale-malformed", which is safely overwritten on claim().
 *
 * State machine.
 *   - free                  : No file on disk. claim writes a fresh record.
 *   - held-live             : File exists, boot session matches, the PID is alive, and the process-identity probe does not positively identify the live process
 *                             as a non-PrismCast program (it is genuinely the same process). claim refuses and returns the holder's record.
 *   - stale-different-boot  : File exists but boot session differs. The writing process cannot still exist; safe to overwrite.
 *   - stale-dead-pid        : File exists, boot session matches, but the PID is no longer alive OR the PID has been recycled to an unrelated process within the
 *                             same boot (the process-identity probe finds a non-PrismCast command line at that PID). Both are "the writer is gone"; safe to overwrite.
 *   - stale-malformed       : File exists but cannot be parsed (unrecognized format, partial write, corruption). Safe to overwrite.
 *
 * Same-boot PID reuse. The bootId check alone catches the cross-reboot case (a reboot mints a new boot session, so a recycled PID classifies as
 * stale-different-boot regardless of liveness). It cannot catch the same-boot residual: a SIGKILL of PrismCast followed by the kernel reassigning the freed PID
 * to an unrelated process within the same boot session leaves bootId matching and the PID alive, which would falsely read as held-live. We close this with a
 * process-identity probe (isPidOurProcess) that consults the OS process table - via the processInspector seam - and asks whether the live process at that PID is
 * genuinely a PrismCast instance or some unrelated program that inherited the recycled PID. When the table confirms a non-PrismCast command line at that PID, we
 * classify the slot as stale rather than held-live. When identity cannot be determined (the process table is unavailable on the platform, or the PID is absent
 * from it), we conservatively retain the held-live verdict: failing to confirm identity must never downgrade a possibly-live holder to "free", since that is the
 * only branch that risks two concurrent instances.
 *
 * Concurrency note. claim() is not atomic against simultaneous startups - two callers racing on the same file may both pass inspect() and both write. Service
 * managers serialize startup so this is not a production concern; if a user manually launches two instances at once, the port-bind step (EADDRINUSE) catches
 * the collision downstream. A future hardening pass could acquire an advisory file lock here; the discriminated-union shape leaves that as a single-point
 * upgrade.
 */
import type { Nullable } from "../types/index.ts";
import { clearPidFile } from "./pid.ts";
import { createDefaultRuntimeIdentityContext } from "./runtimeIdentity.context.ts";
import fs from "node:fs";

/**
 * The structured identity record persisted to disk.
 */
export interface IdentityRecord {

  // The boot session identifier at the moment the record was written. Compared for equality against the current boot session on read.
  readonly bootId: string;

  // The process ID of the writer. Combined with bootId, identifies the writing process uniquely across reboots and container restarts.
  readonly pid: number;

  // ISO-8601 timestamp of when the record was written. Informational only; never participates in correctness decisions.
  readonly startedAt: string;

  // The PrismCast version string at the moment the record was written. Informational; useful in held-live conflict diagnostics.
  readonly version: string;
}

/**
 * Discriminated union representing the on-disk state at a given path. Every branch is exhaustive at compile time, so callers must handle each variant
 * explicitly and the type system catches any new state added in the future.
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

  // Returns the current boot session identifier. Conventionally proxies getBootSessionId() from bootSession.ts.
  readonly getBootSessionId: () => string;

  // Resolves the process-identity question for a PID whose liveness and boot session have already matched the record: is the live process at this PID genuinely
  // a PrismCast instance, or an unrelated process that inherited a recycled PID within the same boot session? Returns true when the process table confirms a
  // PrismCast command line at that PID, false when it confirms a different (non-PrismCast) command line, and null when identity cannot be determined - the
  // process table is unavailable on the platform, the PID is absent from it, or the marker is empty. The null case is deliberately conservative: callers retain
  // the held-live verdict rather than risk downgrading a possibly-live holder to "free". Conventionally backed by the processInspector seam plus a PrismCast
  // command-line marker.
  readonly isPidOurProcess: (pid: number) => Nullable<boolean>;

  // Returns whether a given PID belongs to a process that is currently alive. Conventionally proxies isProcessRunning() from pid.ts.
  readonly isProcessRunning: (pid: number) => boolean;
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

    // Any other read error (EACCES, EIO, ...) is reported as malformed-with-empty-raw so callers fall through to the overwrite path rather than crashing on
    // transient I/O.
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

  // The PID is alive and the boot session matches, but within a single boot the kernel can reassign a freed PID to an unrelated process after PrismCast was
  // SIGKILLed. We confirm process identity through the seam: a verdict of false means the process table positively identified a non-PrismCast command line at
  // this PID, so the original writer is gone and the slot is stale. A verdict of true (confirmed PrismCast) or null (cannot determine) both keep the held-live
  // verdict - we only downgrade on positive proof of a different process. We classify the recycled case as stale-dead-pid rather than a new branch because the
  // operational meaning is identical - the writing process is gone - and callers already overwrite that state.
  if(ctx.isPidOurProcess(record.pid) === false) {

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
    pid: process.pid,
    startedAt: new Date().toISOString(),
    version: metadata.version
  };

  writeRecord(filePath, record);

  return { ok: true, record };
}

/**
 * Releases the identity slot by removing the file at the given path - but only if the file's record identifies the current process. The PID-match check
 * inside makes ownership structural: a rejected duplicate startup's exit handler sees a held-live record belonging to a different PID and leaves the file
 * alone, while the legitimate holder sees its own PID and cleans up. Idempotent: a missing file is not an error. Release is purely hygiene - a process that
 * fails to release on death has its file recovered transparently on the next startup via the stale-different-boot or stale-dead-pid branches.
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
 * Serializes a record to its on-disk representation. The first line is the bare PID for backwards compatibility with shell tooling; subsequent lines are
 * key=value pairs in deterministic order. The trailing newline keeps the file well-formed for POSIX text-file tools.
 * @param record - The identity record to serialize.
 * @returns The serialized payload.
 */
export function serializeRecord(record: IdentityRecord): string {

  return String(record.pid) + "\n" +
    "bootId=" + record.bootId + "\n" +
    "startedAt=" + record.startedAt + "\n" +
    "version=" + record.version + "\n";
}

/**
 * Parses the on-disk representation of an identity record. Returns null when the payload cannot be interpreted as a complete record (missing or non-integer
 * pid, missing bootId). Unknown key=value pairs are silently ignored so future fields can be added without breaking older readers.
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

  const pid = parseInt(firstLine.trim(), 10);

  if(Number.isNaN(pid)) {

    return null;
  }

  let bootId: Nullable<string> = null;
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

  // bootId is the only required metadata field. A file without one was written by a pre-runtimeIdentity PrismCast (or by an external tool) and is treated as
  // malformed for state-machine purposes.
  if(bootId === null) {

    return null;
  }

  return { bootId, pid, startedAt, version };
}

/**
 * Persists a record to disk via atomic write (write-temp + rename). On POSIX rename is atomic within a filesystem; on Windows the rename is best-effort but
 * the partial-write window remains negligible. A torn write would parse as malformed, which the state machine recovers from on the next inspect - so atomicity
 * here is hygiene rather than load-bearing.
 * @param filePath - The absolute path to the identity file.
 * @param record - The record to persist.
 */
function writeRecord(filePath: string, record: IdentityRecord): void {

  const payload = serializeRecord(record);
  const tempPath = filePath + ".tmp";

  fs.writeFileSync(tempPath, payload, "utf-8");
  fs.renameSync(tempPath, filePath);
}
