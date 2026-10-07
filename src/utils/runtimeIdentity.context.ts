/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * runtimeIdentity.context.ts: The default adapter for RuntimeIdentityContext. Wires commandLineOf to the command line the OS process table reports for a PID
 * through the processInspector port, getBootSessionId to the boot-session module, isProcessRunning to the PID primitives, and now() to the system clock. This file
 * is the only place in the runtime-identity module that consumes those defaults, and it makes no identity decision of its own: it hands back the command line
 * exactly as the table reports it, and the fingerprint and the classification live in runtimeIdentity.ts, so the claim and inspect apply one rule. Every call
 * of inspect(), claim() or release() made without a context of its own reaches this adapter through the default parameter, production's and a test's alike.
 */
import type { Nullable } from "../types/index.ts";
import type { RuntimeIdentityContext } from "./runtimeIdentity.ts";
import { getBootSessionId } from "./bootSession.ts";
import { isProcessRunning } from "./pid.ts";
import { listProcesses } from "./processInspector.ts";
import { systemClock } from "homebridge-plugin-utils";

/**
 * Builds the default RuntimeIdentityContext from real runtime I/O.
 * @returns A RuntimeIdentityContext populated from the live process.
 */
export function createDefaultRuntimeIdentityContext(): RuntimeIdentityContext {

  return {

    commandLineOf: (pid: number): Nullable<string> => listProcesses().find((entry) => entry.pid === pid)?.commandLine ?? null,
    getBootSessionId: () => getBootSessionId(),
    isProcessRunning,
    now: (): number => systemClock.now()
  };
}
