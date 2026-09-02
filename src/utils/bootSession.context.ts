/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * bootSession.context.ts: The default adapter for BootSessionContext. Snapshots host boot time and container instance state at adapter-creation time and serves
 * the frozen values for the lifetime of the context. This file is the only place in the boot-session module that calls Date.now(), reads os.uptime(), touches
 * the filesystem, or queries platform helpers; tests construct BootSessionContext literals inline and bypass this file entirely.
 *
 * The snapshot semantics matter. NTP correction or system-clock adjustment mid-process would otherwise let two reads of "host boot minute" disagree by a
 * minute, breaking the guarantee that two calls to getBootSessionId within the same process return equal values. Capturing the math at adapter creation locks
 * the value for the process lifetime.
 */
import type { BootSessionContext } from "./bootSession.ts";
import type { Nullable } from "../types/index.ts";
import fs from "node:fs";
import { isRunningInContainer } from "./platform.ts";
import os from "node:os";

/**
 * Builds the default BootSessionContext from real runtime I/O, snapshotting every reading at construction time so subsequent accessor calls return frozen values.
 * @returns A BootSessionContext whose readings are frozen at the moment of this call.
 */
export function createDefaultBootSessionContext(): BootSessionContext {

  // Snapshot the host boot minute by computing it from system uptime. os.uptime() is sleep-inclusive on Linux (clock_gettime(CLOCK_BOOTTIME), with a /proc/uptime read
  // tried first), macOS (sysctl KERN_BOOTTIME), and Windows (GetTickCount64); each of those primitives keeps counting while the system is suspended, so this value
  // remains stable across sleep/wake cycles. Rounding to the nearest minute absorbs sub-minute NTP drift between writes and reads. Future upgrade: on Linux,
  // /proc/sys/kernel/random/boot_id is a kernel-generated UUID that is regenerated each boot and immune to clock drift; switching this branch to read that file would
  // eliminate the minute-boundary jitter entirely. macOS (sysctl kern.boottime) and Windows (Win32_OS.LastBootUpTime) have equivalent kernel facts but require a
  // subprocess; deferred until a real boundary-jitter incident motivates the work.
  const hostBootMinuteSnapshot = Math.round((Date.now() - (os.uptime() * 1000)) / 60000);

  // Snapshot the container detection result. The project-wide helper reads PRISMCAST_CONTAINER and /.dockerenv; both are stable for the process lifetime.
  const inContainerSnapshot = isRunningInContainer();

  // Snapshot the container instance tag only when in a container. Outside a container /proc/1/stat is the host init, which would not distinguish container
  // restarts within the same host boot; skipping the read avoids spurious work on macOS and Windows where /proc does not exist.
  const containerInstanceTagSnapshot = inContainerSnapshot ? readPid1StartTicks() : null;

  return {

    containerInstanceTag: () => containerInstanceTagSnapshot,
    hostBootMinute: () => hostBootMinuteSnapshot,
    inContainer: () => inContainerSnapshot
  };
}

/**
 * Reads field 22 (starttime, clock ticks since boot) from /proc/1/stat. ENOENT on non-Linux platforms and any read failure resolve to null. The parsing is
 * delegated to parsePid1StartTicks so it can be exercised in tests without an injectable filesystem.
 * @returns The container init's starttime as a string, or null when /proc/1/stat is unavailable or malformed.
 */
function readPid1StartTicks(): Nullable<string> {

  try {

    return parsePid1StartTicks(fs.readFileSync("/proc/1/stat", "utf-8"));
  } catch {

    return null;
  }
}

/**
 * Parses field 22 (starttime) out of a /proc/<pid>/stat payload. The format embeds the executable name in parentheses at field 2; that name can itself contain
 * whitespace and parentheses, so we anchor on the last closing paren before splitting the remainder on whitespace. The first field after the closing paren is
 * field 3 (state), which makes the starttime we want land at index 19 of the post-paren split. Exported for direct unit testing.
 * @param raw - The /proc/<pid>/stat payload as a string.
 * @returns The starttime field as a string, or null when the payload is malformed.
 */
export function parsePid1StartTicks(raw: string): Nullable<string> {

  const rparen = raw.lastIndexOf(")");

  if(rparen === -1) {

    return null;
  }

  const fields = raw.slice(rparen + 1).trim().split(/\s+/);

  if(fields.length < 20) {

    return null;
  }

  // Coalesce to null even though the length guard above proves the index is in range; TypeScript's noUncheckedIndexedAccess still types fields[19] as
  // string | undefined and the explicit ?? null keeps the return type exact.
  return fields[19] ?? null;
}
