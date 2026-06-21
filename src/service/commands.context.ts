/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * commands.context.ts: The default adapter that produces a ServiceContext from real runtime I/O. The handlers in commands.ts are pure orchestrators over a
 * ServiceContext; this file is the composition seam that wires those handlers to live runtime I/O - it performs the HTTP probe of the running server directly and
 * delegates the platform-specific generator factory and the path and platform queries to their respective helpers. Tests bypass this file entirely by constructing
 * ServiceContext literals inline.
 */
import { DEFAULTS, readConfig } from "../config/userConfig.ts";
import type { ServiceContext, StreamsResponse } from "./commands.ts";
import { buildServiceDefinition, detectStalePaths, getServiceGenerator, getServicePaths } from "./generators.ts";
import { getDataDir, getServiceFilePath } from "../config/paths.ts";
import { getNodeExecutablePath, getPlatform, getPrismCastEntryPoint } from "../utils/platform.ts";
import { print, printError } from "../utils/cliOutput.ts";
import type { Platform } from "../utils/platform.ts";

/**
 * Builds the default ServiceContext from real runtime I/O.
 * @returns A ServiceContext populated from the live process.
 */
export function createDefaultServiceContext(): ServiceContext {

  return {

    buildServiceDefinition,
    detectStalePaths,
    fetchActiveStreams: async (port: number): Promise<StreamsResponse | null> => {

      try {

        const response = await fetch("http://127.0.0.1:" + String(port) + "/streams", { signal: AbortSignal.timeout(3000) });

        if(!response.ok) {

          return null;
        }

        return await response.json() as StreamsResponse;
      } catch {

        return null;
      }
    },
    getDataDir,
    getEntryPoint: getPrismCastEntryPoint,
    getGenerator: getServiceGenerator,
    getNodePath: getNodeExecutablePath,
    getPlatform: (): Platform => getPlatform(),
    getServerPort: async (): Promise<number> => {

      const result = await readConfig();

      return result.config.server?.port ?? DEFAULTS.server.port;
    },
    getServiceFilePath,
    getServicePaths,
    stderr: printError,
    stdout: print
  };
}
