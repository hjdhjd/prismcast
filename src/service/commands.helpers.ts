/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * commands.helpers.ts: Shared test helpers for the service command suite. Hosts the FakeGenerator fixture (ServiceGenerator double that records every method
 * invocation), the makeFakeGenerator factory, the ContextHarness shape (captured stdout/stderr arrays + the synthetic ServiceContext), and the makeContextHarness
 * factory. The default-context tests (commands.test.ts) and the literal-context handler tests (commands.install.test.ts, commands.lifecycle.test.ts,
 * commands.status.test.ts) all consume these.
 */

import type { ServiceContext, StreamsResponse } from "./commands.ts";
import type { ServiceDefinition, ServiceGenerator, ServicePaths, StalePathResult } from "./generators.ts";
import type { Nullable } from "../types/index.ts";

export interface FakeGenerator extends ServiceGenerator {

  installed: boolean;
  installs: ServiceDefinition[];
  installShouldThrow: Nullable<Error>;
  isInstalledCalls: number;
  isRunningCalls: number;
  running: boolean;
  startCalls: number;
  startShouldThrow: Nullable<Error>;
  stopCalls: number;
  stopShouldThrow: Nullable<Error>;
  uninstallCalls: number;
  uninstallShouldThrow: Nullable<Error>;
}

export function makeFakeGenerator(overrides: Partial<FakeGenerator> = {}): FakeGenerator {

  const generator: FakeGenerator = {

    install: async (definition: ServiceDefinition): Promise<void> => {

      generator.installs.push(definition);

      if(generator.installShouldThrow) {

        throw generator.installShouldThrow;
      }

      generator.installed = true;
      generator.running = true;
    },
    installShouldThrow: null,
    installed: false,
    installs: [],
    isInstalled: async (): Promise<boolean> => {

      generator.isInstalledCalls++;

      return generator.installed;
    },
    isInstalledCalls: 0,
    isRunning: async (): Promise<boolean> => {

      generator.isRunningCalls++;

      return generator.running;
    },
    isRunningCalls: 0,
    platform: "darwin",
    running: false,
    serviceManager: "launchd",
    start: async (): Promise<void> => {

      generator.startCalls++;

      if(generator.startShouldThrow) {

        throw generator.startShouldThrow;
      }

      generator.running = true;
    },
    startCalls: 0,
    startShouldThrow: null,
    stop: async (): Promise<void> => {

      generator.stopCalls++;

      if(generator.stopShouldThrow) {

        throw generator.stopShouldThrow;
      }

      generator.running = false;
    },
    stopCalls: 0,
    stopShouldThrow: null,
    uninstall: async (): Promise<void> => {

      generator.uninstallCalls++;

      if(generator.uninstallShouldThrow) {

        throw generator.uninstallShouldThrow;
      }

      generator.installed = false;
      generator.running = false;
    },
    uninstallCalls: 0,
    uninstallShouldThrow: null,
    ...overrides
  };

  return generator;
}

export interface ContextHarness {

  context: ServiceContext;
  generator: FakeGenerator | null;
  stderr: string[];
  stdout: string[];
}

export interface ContextOverrides {

  detectStalePaths?: () => Nullable<StalePathResult>;
  fetchActiveStreams?: (port: number) => Promise<Nullable<StreamsResponse>>;
  generator?: FakeGenerator | null;
  getServerPort?: () => Promise<number>;
  getServicePaths?: () => Nullable<ServicePaths>;
  platform?: string;
}

export function makeContextHarness(overrides: ContextOverrides = {}): ContextHarness {

  const stdout: string[] = [];
  const stderr: string[] = [];
  const generator = overrides.generator === null ? null : (overrides.generator ?? makeFakeGenerator());

  const definition: ServiceDefinition = {

    entryPoint: "/usr/local/lib/prismcast/dist/index.js",
    envVars: {},
    logsDir: "/Users/test/.prismcast",
    nodePath: "/usr/local/bin/node",
    workingDir: "/Users/test/.prismcast"
  };

  const context: ServiceContext = {

    buildServiceDefinition: (): ServiceDefinition => definition,
    detectStalePaths: overrides.detectStalePaths ?? ((): Nullable<StalePathResult> => null),
    fetchActiveStreams: overrides.fetchActiveStreams ?? (async (): Promise<Nullable<StreamsResponse>> => null),
    getDataDir: (): string => "/Users/test/.prismcast",
    getEntryPoint: (): string => "/usr/local/lib/prismcast/dist/index.js",
    getGenerator: (): Nullable<ServiceGenerator> => generator,
    getNodePath: (): string => "/usr/local/bin/node",
    getPlatform: () => (overrides.platform ?? "darwin") as ReturnType<ServiceContext["getPlatform"]>,
    getServerPort: overrides.getServerPort ?? (async (): Promise<number> => 5589),
    getServiceFilePath: (): string => "/Users/test/Library/LaunchAgents/com.prismcast.plist",
    getServicePaths: overrides.getServicePaths ?? ((): Nullable<ServicePaths> => null),
    stderr: (line: string): void => { stderr.push(line); },
    stdout: (line: string): void => { stdout.push(line); }
  };

  return { context, generator, stderr, stdout };
}
