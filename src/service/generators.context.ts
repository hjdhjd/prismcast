/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * generators.context.ts: The default adapter that produces a GeneratorIO from real runtime I/O. Each platform-specific generator factory in generators.ts is a
 * pure orchestrator over a GeneratorIO; this file is the only place in the service-generators module that actually invokes execFile, mkdir, writeFile, rm, etc.
 * Tests bypass this file entirely by constructing GeneratorIO literals inline so install/uninstall/start/stop pipelines run against fakes rather than spawning
 * launchctl/systemctl/powershell.exe.
 */
import { type Platform, getPlatform } from "../utils/platform.ts";
import { getServiceFileDirectory, getServiceFilePath } from "../config/paths.ts";
import type { GeneratorIO } from "./generators.ts";
import { execFile as execFileCallback } from "node:child_process";
import fs from "node:fs";
import { promisify } from "node:util";

const execFile = promisify(execFileCallback);
const { promises: fsPromises } = fs;

/**
 * Builds the default GeneratorIO from real runtime I/O. Production callers use this; tests construct GeneratorIO literals inline.
 * @returns A GeneratorIO populated from the live process.
 */
export function createDefaultGeneratorIO(): GeneratorIO {

  return {

    access: async (path: string): Promise<void> => {

      await fsPromises.access(path);
    },
    execFile: async (file: string, args: string[]): Promise<{ stderr: string; stdout: string }> => {

      // Node's promisified execFile defaults to utf8 string output when no encoding option is supplied; the type signature reflects that, so stdout and stderr
      // are typed as string here.
      const { stderr, stdout } = await execFile(file, args);

      return { stderr, stdout };
    },
    existsSync: fs.existsSync,
    getPlatform: (): Platform => getPlatform(),
    getServiceFileDirectory,
    getServiceFilePath,
    mkdir: async (path: string, options: { recursive: boolean }): Promise<void> => {

      await fsPromises.mkdir(path, options);
    },
    readFileSync: (path: string): string => fs.readFileSync(path, "utf8"),
    rm: async (path: string, options: { force: boolean }): Promise<void> => {

      await fsPromises.rm(path, options);
    },
    writeFile: async (path: string, content: string): Promise<void> => {

      await fsPromises.writeFile(path, content, "utf8");
    }
  };
}
