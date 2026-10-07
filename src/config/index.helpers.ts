/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.helpers.ts: Test-only in-memory double of the configuration store. Co-located with index.ts, the configuration module that declares the ConfigStore port
 * and composes its default from the file store. Consumed by the suites that boot and save through that port. Excluded from the build emit by the
 * *.helpers.ts pattern in tsconfig.build.json.
 */
import { FileStoreParseError, FileStoreReadError } from "./persistence.ts";
import type { ConfigStore } from "./index.ts";
import type { Nullable } from "../types/index.ts";
import type { UserConfig } from "./userConfig.ts";
import { normalizeStoredConfig } from "./userConfig.ts";

/**
 * A DeviceID that passes its checksum, the one seed every suite takes when its stored file needs a valid id, so a boot from that file corrects nothing and
 * writes nothing for it.
 */
export const SEEDED_DEVICE_ID = "e2370904";

/**
 * The error message the double's read failure throws from a mutation, the file store's own wording for a file it could not read, carrying the reason the
 * double's permission failure gives. It is written out rather than composed, so a change to the store's wording reddens the rows that match it.
 */
export const READ_FAILURE_MESSAGE = "The configuration file /memory/config.json could not be read (EACCES: permission denied), so nothing was written.";

/**
 * The error message the double's write failure throws from a mutation once its callback has run and before its follow-up, standing in for a write or a readback
 * the file store reports failed.
 */
export const WRITE_FAILURE_MESSAGE = "The configuration file /memory/config.json could not be written.";

/**
 * An in-memory ConfigStore whose state a row reads and assigns on the object it holds: the file it stores, the failure a row arms, and how many reads and writes
 * it served.
 */
export interface MemoryConfigStore extends ConfigStore {

  armedFailure: Nullable<"parse" | "read" | "write">;
  file: UserConfig;
  reads: number;
  writes: number;
}

/**
 * Builds an in-memory double of the ConfigStore port, typed by the port so the double cannot drift from it. Its operations close over the object it returns, so a
 * row assigns the file, arms a failure, and reads the counters on the object it holds.
 *
 * mutateConfigThen refuses on an armed parse or read failure exactly as the file store does, before the callback runs, and otherwise runs the callback against
 * a copy of the held file, keeping the copy only when the callback returns, so a callback that throws writes nothing and runs no follow-up. An armed write
 * failure refuses once the callback has run, keeping nothing, counting no write and running no follow-up, as the file store refuses a write or a readback that
 * fails after the callback ran. The kept copy passes normalizeStoredConfig first, the function the real store's write hook is, so the held file is what the
 * store would have written; the double then counts the write, awaits the follow-up the callback returned, and resolves with its result. readConfig answers an
 * armed parse or read failure the way the file store does, with the defaults and the member that names the failure, and otherwise a copy of the held file,
 * because a store whose write fails still reads.
 *
 * The double models no chain: each call runs its callback when it is made, so overlapping writes are not serialized here the way the file store serializes
 * them. A row about the order of overlapping writes runs on the real store, in index.ordering.test.ts.
 * @param file - The stored file the double starts from.
 * @returns The double.
 */
export function makeMemoryConfigStore(file: UserConfig = {}): MemoryConfigStore {

  const store: MemoryConfigStore = {

    armedFailure: null,
    file,
    mutateConfigThen: async <R>(fn: (current: UserConfig) => () => Promise<R>): Promise<R> => {

      switch(store.armedFailure) {

        case "parse": {

          throw new FileStoreParseError("configuration", "/memory/config.json", "Unexpected token.");
        }

        case "read": {

          throw new FileStoreReadError("configuration", "/memory/config.json", new Error("EACCES: permission denied"));
        }

        default: {

          break;
        }
      }

      const working = structuredClone(store.file);
      const followUp = fn(working);

      if(store.armedFailure === "write") {

        throw new Error(WRITE_FAILURE_MESSAGE);
      }

      store.file = normalizeStoredConfig(working);
      store.writes++;

      return followUp();
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
