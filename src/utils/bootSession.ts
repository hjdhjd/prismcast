/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * bootSession.ts: The boot-session port. Returns a stable identifier for the current kernel boot session so PID files - and any other on-disk state that must
 * not survive a reboot or container restart - can detect when their contents are from a previous session. Callers compare for equality only; the identifier's
 * internal format is intentionally opaque. The composition is "<host-boot-minute>" outside containers and "<host-boot-minute>::<container-init-start>" inside,
 * where the host portion is derived from os.uptime() arithmetic rounded to the nearest minute (to absorb sub-minute NTP drift) and the container portion is the
 * container init process's starttime in clock ticks since boot (from /proc/1/stat field 22). Both signals are sleep-inclusive on every supported platform.
 *
 * Why a port. The boot identifier is the load-bearing invariant for runtime-identity tracking - if two callers in the same process observe different values, the
 * invariant collapses. So the default adapter (in bootSession.context.ts) snapshots its readings ONCE at adapter creation and returns the frozen values on every
 * subsequent call. This is intentionally different from realClock, where each call delegates live: clock readings should not be stable across calls, but boot
 * identifiers must be. Tests bypass the snapshot by constructing context literals.
 */
import type { Nullable } from "../types/index.ts";
import { createDefaultBootSessionContext } from "./bootSession.context.ts";

/**
 * The runtime capability set getBootSessionId consumes. Production wires it from real I/O via createDefaultBootSessionContext; tests pass a context literal.
 */
export interface BootSessionContext {

  // Returns the container init process's instance tag, or null when unavailable. Conventionally /proc/1/stat field 22 (clock ticks since boot) on Linux. The
  // orchestrator only consults this when inContainer() is true.
  readonly containerInstanceTag: () => Nullable<string>;

  // Returns the rounded host boot minute (Unix epoch minute of last system boot). Stable across calls within the same boot session; differs across reboots.
  readonly hostBootMinute: () => number;

  // Returns whether the current process is running inside a container. Conventionally proxies the project-wide isRunningInContainer() helper.
  readonly inContainer: () => boolean;
}

/* The default context is created once at module load. createDefaultBootSessionContext() snapshots its readings, so this constant captures the boot identifier
 * for the entire process lifetime. Tests bypass it by passing their own context literal.
 */
const defaultContext = createDefaultBootSessionContext();

/**
 * Returns the boot session identifier for the current process. Outside a container the identifier is just the host boot minute; inside, it is composed with
 * the container init's instance tag so each container restart yields a distinct value even within the same host boot.
 * @param ctx - The boot session context. Defaults to a module-load snapshot of real runtime I/O.
 * @returns A string that is stable across calls in the same kernel/container session and differs across reboots or container restarts.
 */
export function getBootSessionId(ctx: BootSessionContext = defaultContext): string {

  const host = String(ctx.hostBootMinute());

  if(!ctx.inContainer()) {

    return host;
  }

  const tag = ctx.containerInstanceTag();

  return (tag === null) ? host : (host + "::" + tag);
}
