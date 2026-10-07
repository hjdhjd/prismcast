/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.helpers.ts: Test-only in-memory double of the configuration store. Co-located with index.ts, the configuration module that declares the ConfigStore port
 * and composes its default from the file store. Consumed by the configuration suites that boot and save through that port. Excluded from the build emit by the
 * *.helpers.ts pattern in tsconfig.build.json.
 */
import type { ConfigStore } from "./index.ts";
import { FileStoreParseError } from "./persistence.ts";
import type { Nullable } from "../types/index.ts";
import type { UserConfig } from "./userConfig.ts";
import { normalizeStoredConfig } from "./userConfig.ts";

/**
 * The error message the double's read failure throws from a mutation, the file store's own wording for a file it could not read.
 */
export const READ_FAILURE_MESSAGE = "The configuration file /memory/config.json could not be read, so nothing was written.";

/**
 * An in-memory ConfigStore whose state a row reads and assigns on the object it holds: the file it stores, the failure a row arms, and how many reads and writes
 * it served.
 */
export interface MemoryConfigStore extends ConfigStore {

  armedFailure: Nullable<"parse" | "read">;
  file: UserConfig;
  reads: number;
  writes: number;
}

/**
 * Builds an in-memory double of the ConfigStore port, typed by the port so the double cannot drift from it. Its operations close over the object it returns, so a
 * row assigns the file, arms a failure, and reads the counters on the object it holds.
 *
 * mutateConfig refuses on an armed failure exactly as the file store does, before the callback runs, and otherwise runs the callback against a copy of the held
 * file, keeping the copy only when the callback returns, so a callback that throws writes nothing. The kept copy passes normalizeStoredConfig first, the function
 * the real store's write hook is, so the held file is what the store would have written. readConfig answers an armed failure the way the file store does, with
 * the defaults and the member that names the failure, and otherwise a copy of the held file.
 * @param file - The stored file the double starts from.
 * @returns The double.
 */
export function makeMemoryConfigStore(file: UserConfig = {}): MemoryConfigStore {

  const store: MemoryConfigStore = {

    armedFailure: null,
    file,
    mutateConfig: async (fn: (current: UserConfig) => void): Promise<void> => {

      switch(store.armedFailure) {

        case "parse": {

          throw new FileStoreParseError("configuration", "/memory/config.json", "Unexpected token.");
        }

        case "read": {

          throw new Error(READ_FAILURE_MESSAGE);
        }

        default: {

          break;
        }
      }

      const working = structuredClone(store.file);

      fn(working);
      store.file = normalizeStoredConfig(working);
      store.writes++;
    },
    readConfig: async (): ReturnType<ConfigStore["readConfig"]> => {

      store.reads++;

      switch(store.armedFailure) {

        case "parse": {

          return { config: {}, parseError: true, parseErrorMessage: "Unexpected token.", readError: false };
        }

        case "read": {

          return { config: {}, parseError: false, readError: true };
        }

        default: {

          return { config: structuredClone(store.file), parseError: false, readError: false };
        }
      }
    },
    reads: 0,
    writes: 0
  };

  return store;
}
