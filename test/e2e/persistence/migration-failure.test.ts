/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * migration-failure.test.ts: Pins the file-store framework's contract under migration failure - what happens when a declarative schema migration's `apply`
 * callback throws. The framework runs migrations in memory before any disk write, so a throwing migration cannot corrupt the on-disk file; this suite pins
 * that behavior so a future migration that crashes (logic bug, type error, programmer mistake on a fresh schema bump) cannot silently corrupt user data.
 *
 * INVESTIGATION. Read of src/config/persistence.ts (runMigrations, read, doMutate, mutate, ensureMigrated). We cite these by function name rather than line
 * number so the trail does not rot as the source file shifts:
 *
 *   - runMigrations is called by read(). The migration's `apply` callback runs in memory mutating the parsed data in place, then setSchemaVersion
 *     is stamped, then recordMigration is appended, then applied.push happens, then currentVersion advances. If apply() throws, control jumps out of
 *     runMigrations entirely - none of the post-apply steps (version stamp, record append) executes. The exception propagates out of read().
 *   - read() does not catch the throw (it only catches parse errors, not migration errors). So the throw propagates to whichever caller invoked read.
 *   - mutate's doMutate calls read() before any write. If read() throws, doMutate never reaches the write step. The file is therefore byte-identical to
 *     its pre-mutate state. The mutate's caller sees the thrown exception.
 *   - ensureMigrated calls read() (a peek). Same story: read throws, ensureMigrated propagates the exception, the file is untouched.
 *
 * Conclusion: the framework's behavior on migration throw is correct by construction. The migration runs in memory before any disk write; a throw inside the
 * runner means the disk write never happens; the file's schemaVersion and migrationsApplied therefore cannot advance, and the data on disk is the pre-attempt
 * state. There is no rollback to perform because there was nothing to roll back - the framework's separation of "in-memory upgrade" from "persist the upgrade"
 * is what makes failure transparent. This suite pins that contract end-to-end so a future regression that, say, moved the version stamp ahead of the apply
 * call, or that swallowed the throw and persisted partial state, fails loud.
 *
 * Approach choice. mock.module is the right tool for mocking exported FUNCTIONS that production
 * statically imports - it intercepts at the module-binding boundary. The framework's migration contract is exercised by INSTANTIATING a fresh FileStore via
 * createFileStore with a custom migration map; that surface IS the production API for declaring a store, and the test using it exercises the same code path
 * production stores (config, channels, profiles) traverse on every boot. A mock.module call to swap a real production migration would test "would the
 * production foxcom-rename-migration crash gracefully if it threw" - a narrower assertion than "does the framework's runMigrations contract handle any
 * throwing migration correctly," which is what this suite pins.
 *
 * Why this is the architecturally correct boundary to test at (not a workaround): createFileStore IS a public, exported, framework-level constructor. A
 * test that uses it to instantiate a fresh store with synthetic migrations is using the framework's documented surface to test the framework's documented
 * contract. No production code is mocked, no internal modules are mocked, no module bindings are patched. The store created here is real, registers in the same
 * registeredStores list every production store registers in, runs the same runMigrations the production stores run, and writes via the same atomic-rename
 * path. The fixture is synthetic; the framework code is production.
 */
import { createIntegrationContext, pathInDataDir } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { readFile, writeFile } from "node:fs/promises";
import assert from "node:assert/strict";
import { createFileStore } from "../../../src/config/persistence.ts";

/**
 * Synthetic data shape for the test stores. Carries a payload field plus the framework-managed schema metadata - the same general shape every production store
 * adopts (config, channels, profiles all carry schemaVersion + migrationsApplied + their own payload).
 */
interface TestData {

  migrationsApplied: string[];
  payload: string;
  schemaVersion: number;
}

/**
 * Builds the standard FileStoreOptions surface for a synthetic test store. Every test in this suite varies only the migrations map; everything else (parse,
 * beforeWrite, schema version handling) is identical, so this helper centralizes the boilerplate.
 * @param filePath - Absolute path to the JSON file the store backs.
 * @param migrations - The migration map to install. The runner walks from each file's stored schemaVersion up to currentSchemaVersion=2, so this map should
 *   define migration index 2 - the test's failure-mode subject.
 * @param label - Human-readable store label for log messages and error text.
 * @returns A FileStore instance ready to test.
 */
function makeTestStore(filePath: string, migrations: Parameters<typeof createFileStore<TestData>>[0]["migrations"],
  label: string): ReturnType<typeof createFileStore<TestData>> {

  return createFileStore<TestData>({

    beforeWrite: (data) => ({ migrationsApplied: data.migrationsApplied, payload: data.payload, schemaVersion: data.schemaVersion }),
    currentSchemaVersion: 2,
    defaultValue: () => ({ migrationsApplied: [], payload: "default", schemaVersion: 2 }),
    getSchemaVersion: (data) => data.schemaVersion,
    label,
    migrations,
    parse: (raw) => {

      const parsed = JSON.parse(raw) as Record<string, unknown>;

      return {

        migrationsApplied: Array.isArray(parsed["migrationsApplied"]) ? (parsed["migrationsApplied"] as string[]) : [],
        payload: (typeof parsed["payload"] === "string") ? parsed["payload"] : "",
        schemaVersion: (typeof parsed["schemaVersion"] === "number") ? parsed["schemaVersion"] : 1
      };
    },
    path: () => filePath,
    recordMigration: (data, description) => { data.migrationsApplied.push(description); },
    setSchemaVersion: (data, version) => { data.schemaVersion = version; }
  });
}

describe("file-store framework - migration failure rollback contract", () => {

  test("a migration whose apply callback throws leaves the on-disk file byte-identical to its pre-attempt state", async () => {

    /* The headline contract: migrations run in memory before any disk write, so a throw means the file never gets touched. We seed a v1 fixture, install a
     * migration whose apply throws, attempt to mutate the store (which forces a read + migrate path internally), and assert the on-disk bytes after the throw
     * are byte-equal to the seeded fixture.
     *
     * A regression that moved the disk write ahead of the migration runner, or that caught and swallowed the throw and proceeded to write partial state, would
     * fail this assertion because the file would no longer match the pre-attempt bytes.
     */
    await using ctx = await createIntegrationContext();

    const filePath = pathInDataDir(ctx, "test-failure.json");

    // Seed a v1 fixture. We pre-format the JSON with stable field ordering so the byte-identity assertion below is robust to incidental whitespace - the
    // production stringifySorted writes alphabetized keys with two-space indentation, but the fixture is human-authored and the post-throw read does not write
    // anything back, so the bytes are exactly what we wrote here.
    const fixtureBytes = JSON.stringify({ migrationsApplied: [], payload: "v1-original-bytes", schemaVersion: 1 }, null, 2) + "\n";

    await writeFile(filePath, fixtureBytes, "utf-8");

    // Install a v2 migration that always throws. The framework's runMigrations walks from the file's stored schemaVersion (1, parsed from the fixture) up to
    // currentSchemaVersion (2) and runs migration index 2's apply - which throws. A regression that swallowed the throw or that ran the post-apply steps
    // anyway (version stamp, record append) would not surface this throw to the caller.
    const store = makeTestStore(filePath, {

      2: {

        apply: () => {

          throw new Error("synthetic migration failure for migration-failure.test.ts");
        },
        description: "deliberately throws to exercise the framework's failure path"
      }
    }, "migration-failure-test-1");

    // Attempt the mutate that should force the migration to run in memory. mutate's internal read() invokes runMigrations, which throws; the throw propagates
    // out of mutate. The file is never written because doMutate never reaches the write step.
    await assert.rejects(

      async () => store.mutate((data) => { data.payload = "should-not-land-on-disk"; }),
      /synthetic migration failure/,
      "mutate must propagate the migration apply()'s throw - silent failure here would mean the framework caught and discarded a real bug"
    );

    // The headline assertion: the file's bytes after the failed mutate are byte-identical to the seeded fixture. Same content, same encoding, same whitespace.
    const postThrowBytes = await readFile(filePath, "utf-8");

    assert.equal(postThrowBytes, fixtureBytes,
      "the on-disk file must be byte-identical to its pre-attempt state after a migration throws - any drift indicates the framework wrote partial state");
  });

  test("a migration throw does NOT advance schemaVersion or migrationsApplied on disk", async () => {

    /* The companion contract: schemaVersion and migrationsApplied must remain at their pre-attempt values. A regression that wrote the version stamp BEFORE
     * applying the migration body, or that recorded the migration in migrationsApplied before checking apply() succeeded, would surface here as the post-throw
     * file showing schemaVersion=2 (with the apply that never ran) and migrationsApplied containing the failed migration's description.
     *
     * This is the negative pair to test 1's positive byte-identity. Test 1 pins "the bytes do not change"; this test pins "specifically, schemaVersion does
     * not advance and migrationsApplied does not gain an entry." Together they triangulate any regression that subtly altered the failure path.
     */
    await using ctx = await createIntegrationContext();

    const filePath = pathInDataDir(ctx, "test-version-stability.json");

    await writeFile(filePath, JSON.stringify({ migrationsApplied: [], payload: "v1-payload", schemaVersion: 1 }, null, 2) + "\n", "utf-8");

    const store = makeTestStore(filePath, {

      2: {

        apply: () => { throw new Error("apply throws before any post-apply step runs"); },
        description: "must-not-be-recorded-on-failure"
      }
    }, "migration-failure-test-2");

    await assert.rejects(async () => store.mutate(() => undefined), /apply throws before/, "mutate must surface the throw");

    // Re-read the file directly. The on-disk shape carries schemaVersion and migrationsApplied; the test asserts both are at their pre-attempt values.
    const persistedRaw = await readFile(filePath, "utf-8");
    const persisted = JSON.parse(persistedRaw) as { migrationsApplied?: unknown[]; schemaVersion?: number };

    assert.equal(persisted.schemaVersion, 1, "schemaVersion must remain at the pre-attempt value (1) - a throw in apply must not advance the version stamp");
    assert.deepEqual(persisted.migrationsApplied ?? [], [],
      "migrationsApplied must remain empty - a throw in apply must not append the failed migration's description to the audit trail");
  });

  test("a successful migration after a fix recovers cleanly: the same file that failed under a throwing migration persists correctly under a working one", async () => {

    /* The recovery contract: a failed migration is not a permanent corruption - the file remains valid v1 on disk, so a maintainer who fixes the migration's
     * code (replacing the throwing apply with a working one) can rerun and have the migration apply normally. This test simulates that workflow:
     *
     *   1. Seed a v1 file.
     *   2. Attempt mutate with a throwing migration installed - expect the throw, expect the file unchanged.
     *   3. Build a SECOND store at the same file path with a WORKING migration installed.
     *   4. Mutate via the second store - expect success, expect the file now at v2 with migrationsApplied recording the working migration.
     *
     * A regression that left the file in an inconsistent state after the failure (e.g., schemaVersion bumped to 2 in memory before the throw, leaking back
     * out via some shared data buffer) would fail step 4 because the second store's read would parse the file as already-v2 and skip migrations entirely.
     */
    await using ctx = await createIntegrationContext();

    const filePath = pathInDataDir(ctx, "test-recovery.json");
    const fixtureBytes = JSON.stringify({ migrationsApplied: [], payload: "v1-payload", schemaVersion: 1 }, null, 2) + "\n";

    await writeFile(filePath, fixtureBytes, "utf-8");

    // Step 1: throwing migration; expect failure.
    const failingStore = makeTestStore(filePath, {

      2: {

        apply: () => { throw new Error("first-attempt-fails"); },
        description: "v1 -> v2 (throws on first attempt)"
      }
    }, "migration-failure-test-3-failing");

    await assert.rejects(async () => failingStore.mutate(() => undefined), /first-attempt-fails/, "first attempt's mutate must throw");

    // Sanity: file is still at v1 with original payload after the failure.
    const afterFailureBytes = await readFile(filePath, "utf-8");

    assert.equal(afterFailureBytes, fixtureBytes,
      "the file must remain at v1 fixture state after the failed migration - any drift here would invalidate the recovery scenario below");

    // Step 2: working migration on the same file path. The store registers fresh in the framework's global registeredStores list; that's expected and benign
    // (the registry is process-scoped and the test process disposes when this test ends).
    const workingStore = makeTestStore(filePath, {

      2: {

        apply: (data) => { data.payload = "v2-after-fix"; },
        description: "v1 -> v2 (works after fix)"
      }
    }, "migration-failure-test-3-working");

    await workingStore.mutate(() => undefined);

    // Verify post-recovery state on disk: schemaVersion=2, migrationsApplied carries the working migration's description, payload reflects the migration's
    // mutation.
    const recoveredRaw = await readFile(filePath, "utf-8");
    const recovered = JSON.parse(recoveredRaw) as { migrationsApplied?: string[]; payload?: string; schemaVersion?: number };

    assert.equal(recovered.schemaVersion, 2, "after the working migration runs, schemaVersion must advance to 2");
    assert.deepEqual(recovered.migrationsApplied, ["v1 -> v2 (works after fix)"],
      "after the working migration runs, migrationsApplied records exactly that description - the throwing migration's description must NOT appear because " +
      "it never completed (the runner appends to migrationsApplied AFTER apply returns, never before)");
    assert.equal(recovered.payload, "v2-after-fix",
      "after the working migration runs, the payload must reflect the migration's mutation - confirming the apply ran fully and the result was written to disk");
  });
});
