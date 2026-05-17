/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * detection.context.ts: The default adapter that produces a DetectionContext from real runtime I/O. The detection logic in detection.ts is a pure function over
 * a DetectionContext; this file is the only place in the upgrade module that reads import.meta.url, calls execSync, or touches the filesystem. Tests bypass this
 * file entirely by constructing DetectionContext literals inline.
 *
 * The path coming out of import.meta.url is the one place where platform-specific filesystem encoding (forward slash on POSIX, backslash on Windows) enters the
 * system. We wrap it in a PathHandle right here at the boundary so the strategies never see a raw string, which makes the cross-platform path-encoding class of
 * bugs structurally inexpressible at the strategy layer. createPathHandle defaults its platform to process.platform, so production is automatic and tests can
 * still construct platform-specific handles directly when they need to.
 */
import type { DetectionContext } from "./detection.ts";
import { createPathHandle } from "./pathHandle.ts";
import { execSync } from "node:child_process";
import { existsSync } from "node:fs";
import { isRunningInContainer } from "../utils/platform.ts";
import url from "node:url";

/**
 * Builds the default DetectionContext from real runtime I/O.
 * @returns A DetectionContext populated from the live process.
 */
export function createDefaultDetectionContext(): DetectionContext {

  return {

    currentFile: createPathHandle(url.fileURLToPath(import.meta.url)),
    fileExists: existsSync,
    isContainer: isRunningInContainer(),
    runCommand: (cmd: string): string | null => {

      try {

        return execSync(cmd, { encoding: "utf-8", timeout: 5000 }).trim();
      } catch {

        return null;
      }
    }
  };
}
