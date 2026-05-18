/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * runtimeIdentity.context.ts: The default adapter for RuntimeIdentityContext. Wires getBootSessionId from the boot-session module and isProcessRunning from the
 * PID primitives. This file is the only place in the runtime-identity module that consumes those defaults; tests construct RuntimeIdentityContext literals
 * inline and bypass this file entirely.
 */
import type { RuntimeIdentityContext } from "./runtimeIdentity.ts";
import { getBootSessionId } from "./bootSession.ts";
import { isProcessRunning } from "./pid.ts";

/**
 * Builds the default RuntimeIdentityContext from real runtime I/O.
 * @returns A RuntimeIdentityContext populated from the live process.
 */
export function createDefaultRuntimeIdentityContext(): RuntimeIdentityContext {

  return {

    getBootSessionId: () => getBootSessionId(),
    isProcessRunning
  };
}
