/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * generators.helpers.ts: Shared test helpers for the service-generators suite. Hosts the FakeIO* shapes (a GeneratorIO double that captures every IO call into
 * arrays so tests can assert on the exact subprocess invocations and file-write content each generator emits) and the definitionFixture factory (a
 * ServiceDefinition with sensible defaults). Platform-specific tests (generators.launchAgent.test.ts, generators.systemd.test.ts, generators.windowsTask.test.ts),
 * generators.paths.test.ts, and generators.test.ts (getServiceGenerator dispatch) all consume these. The path-resolution stubs (withParserStubs, FAKE_PLIST,
 * FAKE_UNIT, FAKE_LAUNCHER) are scoped to generators.paths.test.ts and live there directly.
 */

import type { GeneratorIO, ServiceDefinition } from "./generators.ts";
import type { FakeExecFile } from "../testing.helpers.ts";
import type { Platform } from "../utils/platform.ts";
import { execFileFromMap } from "../testing.helpers.ts";

export interface FakeIOCall {

  args: unknown[];
  method: string;
}

export interface FakeIOOptions {

  // The execFile implementation injected into the fake IO. Defaults to execFileFromMap({}) - strictly throws "no result configured" on every call so test setups
  // can't silently miss commands. Tests typically pass execFileFromMap({...}) for keyed responses or execFileAlwaysSucceeds() for wildcard responses.
  execFile?: FakeExecFile;

  // Map of file paths to whether they "exist" for access() and existsSync().
  existing?: Record<string, boolean>;

  // Map of paths to file contents readFileSync() should return.
  files?: Record<string, string>;

  platform?: Platform;
  serviceFileDirectory?: string;
  serviceFilePath?: string;
}

export interface FakeIOHarness {

  calls: FakeIOCall[];
  io: GeneratorIO;
  writes: { content: string; path: string }[];
}

/**
 * Builds a fake GeneratorIO adapter that records every call it receives into calls (and every file write into writes), so tests can assert on the exact
 * subprocess invocations and file-write content a generator emits. access/existsSync/readFileSync answer from the existing/files maps in options; execFile
 * delegates to options.execFile (default: execFileFromMap({}), which throws on any command a test forgot to configure); getPlatform/getServiceFileDirectory/
 * getServiceFilePath answer from the matching options field or a darwin-flavored default.
 * @param options - Overrides for the fake's platform, file-system state, and execFile behavior. Defaults to an empty options object.
 * @returns A FakeIOHarness: the GeneratorIO double (io), its recorded call log (calls), and its recorded file writes (writes).
 */
export function makeFakeIO(options: FakeIOOptions = {}): FakeIOHarness {

  const calls: FakeIOCall[] = [];
  const writes: { content: string; path: string }[] = [];
  const existing = options.existing ?? {};
  const files = options.files ?? {};
  const userExecFile = options.execFile ?? execFileFromMap({});

  const io: GeneratorIO = {

    access: async (filePath: string): Promise<void> => {

      calls.push({ args: [filePath], method: "access" });

      if(!existing[filePath]) {

        throw new Error("ENOENT: no such file or directory, access " + filePath);
      }
    },
    execFile: async (file: string, args: string[]): Promise<{ stderr: string; stdout: string }> => {

      // Record the invocation before delegating to the user's impl so calls are observable on every code path - including the failure path where userExecFile
      // throws. This keeps assertions on the call sequence stable regardless of whether commands succeed or fail.
      calls.push({ args: [ file, ...args ], method: "execFile" });

      return userExecFile(file, args);
    },
    existsSync: (filePath: string): boolean => {

      calls.push({ args: [filePath], method: "existsSync" });

      return existing[filePath] ?? false;
    },
    getPlatform: (): Platform => options.platform ?? "darwin",
    getServiceFileDirectory: (): string => options.serviceFileDirectory ?? "/Users/test/Library/LaunchAgents",
    getServiceFilePath: (): string => options.serviceFilePath ?? "/Users/test/Library/LaunchAgents/com.prismcast.plist",
    mkdir: async (filePath: string, opts: { recursive: boolean }): Promise<void> => {

      calls.push({ args: [ filePath, opts ], method: "mkdir" });
    },
    readFileSync: (filePath: string): string => {

      calls.push({ args: [filePath], method: "readFileSync" });

      const content = files[filePath];

      if(content === undefined) {

        throw new Error("ENOENT: no such file or directory, open " + filePath);
      }

      return content;
    },
    rm: async (filePath: string, opts: { force: boolean }): Promise<void> => {

      calls.push({ args: [ filePath, opts ], method: "rm" });
    },
    writeFile: async (filePath: string, content: string): Promise<void> => {

      calls.push({ args: [ filePath, content ], method: "writeFile" });
      writes.push({ content, path: filePath });
    }
  };

  return { calls, io, writes };
}

/**
 * Builds a ServiceDefinition with sensible test defaults (entry point, env vars, logs/working directories, node path) so generator tests can pass a ready-made
 * definition to the generator under test without repeating the same boilerplate in every case.
 * @param overrides - Fields to override on the default ServiceDefinition. Defaults to no overrides.
 * @returns The resulting ServiceDefinition.
 */
export function definitionFixture(overrides: Partial<ServiceDefinition> = {}): ServiceDefinition {

  return {

    entryPoint: "/usr/local/lib/prismcast/dist/index.js",
    envVars: { PATH: "/usr/local/bin:/usr/bin", PRISMCAST_SERVICE: "1" },
    logsDir: "/Users/test/.prismcast",
    nodePath: "/usr/local/bin/node",
    workingDir: "/Users/test/.prismcast",
    ...overrides
  };
}
