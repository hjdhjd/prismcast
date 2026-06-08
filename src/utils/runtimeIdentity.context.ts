/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * runtimeIdentity.context.ts: The default adapter for RuntimeIdentityContext. Wires getBootSessionId from the boot-session module, isProcessRunning from the
 * PID primitives, and isPidOurProcess from the process-inspector seam. This file is the only place in the runtime-identity module that consumes those defaults;
 * tests construct RuntimeIdentityContext literals inline and bypass this file entirely.
 *
 * Process-identity probe. isPidOurProcess answers the same-boot PID-reuse question: after a SIGKILL frees PrismCast's PID, the kernel can reassign it to an
 * unrelated process within the same boot session, which would otherwise read as a live holder. We resolve identity by enumerating the OS process table through
 * the processInspector seam and inspecting the command line of the row whose pid matches. A genuine PrismCast process is launched with the product token in its
 * command line on every supported install method (the `prismcast` binary, `node .../prismcast/dist/index.js`, the Homebrew wrapper, or the Docker entry point),
 * so a command line that contains the token is treated as PrismCast and one that lacks it as an unrelated process. The match deliberately biases toward the safe
 * direction: an unrelated process whose command line happens to mention the token (e.g. `tail -f prismcast.log`) reads as PrismCast and keeps the conservative
 * held-live verdict, since refusing to start a duplicate is always safer than spawning two concurrent instances.
 */
import type { Nullable } from "../types/index.ts";
import type { RuntimeIdentityContext } from "./runtimeIdentity.ts";
import { getBootSessionId } from "./bootSession.ts";
import { isProcessRunning } from "./pid.ts";
import { listProcesses } from "./processInspector.ts";

/* The product-identity token searched for in a process's command line. This mirrors the package name / SERVICE_NAME identity; it lives as a local constant
 * because the runtime-identity context must remain free of sideways imports into identity.ts. Matching is case-insensitive so launcher casing differences (e.g.
 * the macOS app bundle versus the lowercase binary) do not cause a false negative.
 */
const PRISMCAST_COMMAND_TOKEN = "prismcast";

/**
 * Builds the default RuntimeIdentityContext from real runtime I/O.
 * @returns A RuntimeIdentityContext populated from the live process.
 */
export function createDefaultRuntimeIdentityContext(): RuntimeIdentityContext {

  return {

    getBootSessionId: () => getBootSessionId(),
    isPidOurProcess: (pid: number): Nullable<boolean> => resolvePidIdentity(pid),
    isProcessRunning
  };
}

/**
 * Resolves whether the live process at the given PID is genuinely a PrismCast instance by enumerating the OS process table and matching the product token in the
 * row's command line. Returns true when the row's command line contains the token, false when it contains a non-empty command line without the token, and null
 * when identity cannot be determined - the process table is empty (unsupported platform or enumeration failure), the PID is absent from it, or the matching
 * row reports an empty command line (kernel threads, hidden command lines). The null cases all map upstream to the conservative held-live verdict.
 * @param pid - The PID to identify.
 * @returns True for a confirmed PrismCast process, false for a confirmed different process, null when identity cannot be determined.
 */
function resolvePidIdentity(pid: number): Nullable<boolean> {

  const processes = listProcesses();
  const match = processes.find((entry) => entry.pid === pid);

  // The PID is absent from the snapshot (it exited between the liveness check and this enumeration, or the platform returned an empty table). We cannot prove the
  // process is unrelated, so identity is indeterminate.
  if(match === undefined) {

    return null;
  }

  const commandLine = match.commandLine.trim();

  // An empty command line (kernel thread, process that hid its argv) gives us nothing to match against, so identity remains indeterminate.
  if(commandLine === "") {

    return null;
  }

  return commandLine.toLowerCase().includes(PRISMCAST_COMMAND_TOKEN);
}
