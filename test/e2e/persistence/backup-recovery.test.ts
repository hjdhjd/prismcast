/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * backup-recovery.test.ts: Pins the file-store framework's auto-recovery contract for the worst-case persistence scenario - a corrupt main JSON file. The
 * framework's safety guarantee (src/config/persistence.ts createFileStore docstring): when the main file fails to parse, read() transparently restores from
 * .bak via atomic temp+rename and surfaces recoveredFromBackup on the result; only when both files are unparseable does the result fall back to defaults with
 * parseError=true. Without integration coverage, the next change to the recovery sequencing (e.g., a refactor that touched the order of restore and write,
 * or that mistakenly fed the corrupt main back into .bak before recovery) ships untested.
 *
 * The scenarios tested here are the user-visible failure modes that recovery exists to handle: a kill-mid-save, an OS-level write failure, a partial flush
 * during shutdown. Each leaves the main file in a corrupt state that the .bak can rescue. The test deliberately corrupts files inside ctx.dataDir so the
 * corruption falls out of scope at disposal - the production data directory is never touched.
 *
 * recoveredFromBackup propagation: the framework's FileStore.read() exposes recoveredFromBackup on the FileStoreReadResult interface and the per-store
 * wrapper read functions (readConfig, readChannels, readProfiles) project it onto their respective UserConfigLoadResult / UserChannelsLoadResult /
 * UserProfilesLoadResult return shapes. Read-path tests (Tests 2 and 3 below) pin the flag directly. Tests 1 and 4 trigger recovery through mutateConfig /
 * mutateChannels - the mutate API does not return a read result, so the flag is not observable from those callers. Their assertions cover separate guarantees
 * (post-recovery disk state, runtime CONFIG via initializeConfiguration, no FileStoreParseError thrown, per-store isolation) that remain meaningful regardless
 * of how the flag is surfaced.
 *
 * Why corrupt the file by writing garbage instead of using mock.module to inject parse failure: the contract under test is "the framework recovers from a
 * corrupt MAIN FILE on disk." A mocked parser failure would skip the framework's actual fs.readFile and fs.copyFile chain - the production code path. Real
 * file corruption exercises every layer: byte read, parse attempt, recovery path, atomic temp+rename of restored content, post-recovery mutate.
 */
import { CONFIG, initializeConfiguration } from "../../../src/config/index.ts";
import { createIntegrationContext, initializePersistence, pathInDataDir, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { mutateConfig, readConfig } from "../../../src/config/userConfig.ts";
import { readFile, writeFile } from "node:fs/promises";
import assert from "node:assert/strict";
import { mutateChannels } from "../../../src/config/userChannels.ts";
import { mutateProfiles } from "../../../src/config/userProfiles.ts";

describe("file-store backup recovery from a corrupt main file", () => {

  test("a corrupt main file with a valid .bak triggers transparent recovery; the recovered data flows through the next mutate", async () => {

    /* The canonical recovery scenario. Steps:
     *   1. Establish two distinct config states via mutateConfig - the second write copies the first to .bak. After this, main carries the v2 host and .bak
     *      carries the v1 host.
     *   2. Snapshot the .bak file's bytes for the pre-corruption sanity check below (Step 1's v1 snapshot lives in .bak). The independent .bak-preservation
     *      guarantee - that recovery never overwrites .bak with the corrupt main - is pinned by Test 3 using its own separately-captured snapshot, not this one.
     *   3. Overwrite main with garbage that does not parse as JSON.
     *   4. Trigger a read by calling mutateConfig with a no-op. This invokes the framework's read(), which fails to parse main, falls through to .bak, parses
     *      it, atomically restores main from .bak, and returns the parsed v1 data. The mutate then proceeds normally - no FileStoreParseError thrown.
     *   5. Assert: the post-mutate disk state shows main with valid JSON (specifically the v1 host - the recovered value, since the no-op mutate did not
     *      change anything), and the in-memory CONFIG (refreshed via initializeConfiguration) reflects v1.
     *
     * A regression that aborted on the corrupt main without consulting .bak would surface here as a FileStoreParseError thrown from mutateConfig. A regression
     * that overwrote .bak before reading it would lose the only good copy and fall through to defaults - the assertion on the recovered host would fail.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Step 1: establish two writes so .bak carries v1 and main carries v2.
    await mutateConfig((data) => { data.channelsDvr = { host: "v1.example.test", port: 8089 }; });
    await mutateConfig((data) => { data.channelsDvr = { host: "v2.example.test", port: 8089 }; });

    // Step 2: snapshot .bak. The recovery path under test must NOT mutate this file - one-deep rotation means .bak is the only safety net.
    const bakBefore = await readFile(pathInDataDir(ctx, "config.json.bak"), "utf-8");

    assert.match(bakBefore, /v1\.example\.test/, "sanity: .bak holds the v1 snapshot before corruption");

    // Step 3: corrupt main with bytes that do not parse as JSON. The leading `{` mimics a partial write that crashed mid-flush - the most realistic shape of a
    // real corruption (a complete-garbage file would also work; the partial-shape garbage exercises the same parse-failure code path more faithfully).
    await writeFile(pathInDataDir(ctx, "config.json"), "{ \"channelsDvr\": { \"host\": \"v3.example.test\"", "utf-8");

    // Sanity: the corrupt main really does fail to parse, so the recovery path is genuinely exercised below.
    const corruptMain = await readFile(pathInDataDir(ctx, "config.json"), "utf-8");

    assert.throws(() => JSON.parse(corruptMain), "sanity: the corrupted main file must fail to parse - otherwise recovery would not be triggered");

    // Step 4: a no-op mutate. The framework's mutate calls read() first; read() recovers from .bak; mutate then writes the (recovered) data back. No throw.
    await mutateConfig(() => undefined);

    // Step 5: post-mutate disk state - main parses, contains v1 (the recovered .bak content, since the mutate was a no-op).
    const recovered = await readPersistedJson(ctx, "config.json");

    assert.equal(typeof recovered, "object", "post-recovery main must parse as JSON");
    assert.equal(((recovered as { channelsDvr?: { host?: string } }).channelsDvr?.host), "v1.example.test",
      "the recovered main file's host must equal v1 - the .bak content - since the no-op mutate did not change anything");

    /* Step 5b: drive the recovered data through the production boot sequence and assert the runtime CONFIG reflects the .bak value. initializeConfiguration()
     * calls readConfig() and feeds the result into mergeConfiguration(); the HYDRATED_FIELDS registry pulls channelsDvr.host through to runtime CONFIG so the
     * recovered host is reachable via the same accessor production code uses. The runtime-CONFIG assertion is the structural pin on registry-driven hydration:
     * a regression that broke it - so that the inline-block merge path skipped channelsDvr.host and left the field preserved on disk but invisible to runtime
     * CONFIG - would surface here as a mismatch between the disk and runtime views, not a silent drop.
     */
    await initializeConfiguration();

    assert.equal(CONFIG.channelsDvr.host, "v1.example.test",
      "runtime CONFIG must reflect the recovered .bak host after initializeConfiguration, confirming HYDRATED_FIELDS bridges disk to runtime");
  });

  test("when both main and .bak are corrupt, readConfig falls through to defaults with parseError=true; no exception thrown at the read boundary", async () => {

    /* The framework's "loud, recoverable, never silent" contract on the worst case: when even .bak is unparseable, the read does not throw - it returns the
     * default value with parseError=true so the caller can banner the failure to operators. Compare with mutate(), which DOES throw FileStoreParseError in the
     * same scenario (doMutate's corruption guard throws FileStoreParseError when result.parseError is set) - the asymmetry is by design: reads are diagnostic,
     * writes are refused to prevent saving over the only-good copy with new data.
     *
     * We seed config first so both files exist on disk. Then corrupt both and call readConfig (the read path) and assert parseError=true with defaults flowing
     * through. A regression that caused an unhandled exception here would propagate up as an uncaught error during boot - a much worse failure mode than the
     * documented "default + parseError" outcome.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateConfig((data) => { data.channelsDvr = { host: "first.example.test", port: 8089 }; });
    await mutateConfig((data) => { data.channelsDvr = { host: "second.example.test", port: 8089 }; });

    // Corrupt both files. The framework's recovery path attempts .bak only when main fails; when .bak ALSO fails, the result is parseError + defaults.
    await writeFile(pathInDataDir(ctx, "config.json"), "completely-not-json", "utf-8");
    await writeFile(pathInDataDir(ctx, "config.json.bak"), "also-not-json", "utf-8");

    const result = await readConfig();

    assert.equal(result.parseError, true, "parseError must be true when both main and .bak are unparseable");
    assert.ok(result.parseErrorMessage, "parseErrorMessage must carry diagnostic context for operator triage");
    assert.equal(result.recoveredFromBackup, false,
      "recoveredFromBackup must be false when both main and .bak are unparseable - recovery was attempted but failed, so the flag stays false");
    assert.equal(typeof result.config, "object", "config must still be an object - the framework returns defaults rather than throwing");
  });

  test("recovery preserves .bak as the snapshot - the rotation does NOT overwrite .bak with the just-corrupted main during the recovery path itself", async () => {

    /* The structural rule that makes recovery safe to call repeatedly: the recovery code path (tryRecoverFromBackup) reads .bak,
     * restores main from .bak via atomic temp+rename, and never touches .bak. This means a corrupt main can be recovered any number of times against the same
     * .bak - the .bak is the snapshot, never replaced by a worse version.
     *
     * Note: when read() recovers the in-memory state from .bak, doMutate skips the .bak rotation entirely - the copy of main to .bak is guarded by
     * `if(!result.recoveredFromBackup)`, so a mutate that triggered recovery leaves .bak untouched (no rotation occurs at all, not a rotation that re-writes
     * identical bytes). To keep this test scoped to the recovery path alone, independent of any later mutate, we use readConfig (which restores main from
     * .bak via the recovery path but performs no main->.bak rotation) instead of mutateConfig - the read-only path exercises tryRecoverFromBackup, which
     * writes only to main.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Establish .bak as the v1 snapshot.
    await mutateConfig((data) => { data.channelsDvr = { host: "snapshot.example.test", port: 8089 }; });
    await mutateConfig((data) => { data.channelsDvr = { host: "current.example.test", port: 8089 }; });

    const bakBefore = await readFile(pathInDataDir(ctx, "config.json.bak"), "utf-8");

    assert.match(bakBefore, /snapshot\.example\.test/, "sanity: .bak holds the snapshot host before corruption");

    // Corrupt main but leave .bak alone.
    await writeFile(pathInDataDir(ctx, "config.json"), "garbage-main-content", "utf-8");

    // Trigger recovery via readConfig (read-only path, no subsequent rotation).
    const recovered = await readConfig();

    assert.equal(recovered.parseError, false, "the read recovered from .bak - parseError must be false");
    assert.equal(recovered.recoveredFromBackup, true,
      "recoveredFromBackup must be true when main was unparseable but .bak was usable - the wrapper projects the framework's flag directly");
    assert.equal(recovered.config.channelsDvr?.host, "snapshot.example.test",
      "the recovered config carries the .bak host - confirming the recovery actually executed");

    // The .bak file's bytes are untouched after recovery. tryRecoverFromBackup's contract is to write to main, never to .bak.
    const bakAfter = await readFile(pathInDataDir(ctx, "config.json.bak"), "utf-8");

    assert.equal(bakAfter, bakBefore, "the .bak file's bytes must be byte-identical after recovery - the recovery path must not mutate .bak");

    // Bonus: the corrupted main has been atomically replaced with the recovered .bak content. tryRecoverFromBackup writes to a .tmp and renames over main.
    const mainAfter = await readFile(pathInDataDir(ctx, "config.json"), "utf-8");

    assert.equal(mainAfter, bakBefore, "the recovered main file's bytes must match .bak's - confirming the temp+rename restore actually completed");
  });

  test("per-store recovery isolation: corrupting only channels.json triggers recovery for channels alone, leaving config and profiles undisturbed", async () => {

    /* Each store owns its own file, its own .bak, and its own recovery path. A corrupt channels.json must not influence config.json or profiles.json reads -
     * the recovery is scoped to the affected store. Without this rule, a single corrupted file could cascade through unrelated stores' loads.
     *
     * Setup: mutate all three stores so each has main+bak on disk. Snapshot config.json and profiles.json. Corrupt channels.json's main (leave channels.json.bak
     * intact). Trigger recovery via mutateChannels with a no-op. Assert: config.json and profiles.json are byte-identical pre/post (their recovery paths were
     * never invoked); channels.json was recovered (parses, contains the bak content).
     *
     * This is the negative pair to the cross-store-isolation suite's positive guarantee ("each store writes to its own file"). Together they pin the per-store
     * boundary in both directions: writes don't leak across stores, and recoveries don't leak across stores either.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Seed all three stores. Each gets two writes so all three have a .bak.
    await mutateConfig((data) => { data.channelsDvr = { host: "config-v1.example.test", port: 8089 }; });
    await mutateConfig((data) => { data.channelsDvr = { host: "config-v2.example.test", port: 8089 }; });

    await mutateChannels((data) => { data.channels["isolation-test-channel"] = { name: "Iso v1", url: "https://example.test/iso-v1" }; });
    await mutateChannels((data) => { data.channels["isolation-test-channel"] = { name: "Iso v2", url: "https://example.test/iso-v2" }; });

    await mutateProfiles((data) => { data.profiles["isolation-test-profile"] = { description: "Profile v1" }; });
    await mutateProfiles((data) => { data.profiles["isolation-test-profile"] = { description: "Profile v2" }; });

    // Snapshot config.json and profiles.json (and their .bak rotations). These must be byte-identical after the unrelated channels recovery.
    const configBefore = await readFile(pathInDataDir(ctx, "config.json"), "utf-8");
    const configBakBefore = await readFile(pathInDataDir(ctx, "config.json.bak"), "utf-8");
    const profilesBefore = await readFile(pathInDataDir(ctx, "profiles.json"), "utf-8");
    const profilesBakBefore = await readFile(pathInDataDir(ctx, "profiles.json.bak"), "utf-8");
    const channelsBakBefore = await readFile(pathInDataDir(ctx, "channels.json.bak"), "utf-8");

    // Corrupt only channels.json. Its .bak holds the v1 snapshot ("Iso v1"); the recovery should restore that.
    await writeFile(pathInDataDir(ctx, "channels.json"), "this-is-not-valid-json", "utf-8");

    // Trigger recovery via a no-op mutate against channels.
    await mutateChannels(() => undefined);

    // The other two stores are untouched. Snapshots must match byte-for-byte - main AND .bak.
    assert.equal(await readFile(pathInDataDir(ctx, "config.json"), "utf-8"), configBefore,
      "config.json must not change when channels recovery fires");
    assert.equal(await readFile(pathInDataDir(ctx, "config.json.bak"), "utf-8"), configBakBefore,
      "config.json.bak must not change when channels recovery fires");
    assert.equal(await readFile(pathInDataDir(ctx, "profiles.json"), "utf-8"), profilesBefore,
      "profiles.json must not change when channels recovery fires");
    assert.equal(await readFile(pathInDataDir(ctx, "profiles.json.bak"), "utf-8"), profilesBakBefore,
      "profiles.json.bak must not change when channels recovery fires");

    // Channels recovered: main is now valid JSON and matches the v1 snapshot's recovered content (the no-op mutate wrote the recovered data back).
    const channelsAfter = await readPersistedJson(ctx, "channels.json");

    assert.equal(typeof channelsAfter, "object", "channels.json must parse after recovery");

    const recoveredEntry = (channelsAfter as Record<string, unknown>)["isolation-test-channel"];

    assert.equal(typeof recoveredEntry, "object", "the recovered channels.json must contain the test entry");
    assert.equal((recoveredEntry as Record<string, unknown>)["name"], "Iso v1",
      "the recovered channel must show the v1 name (.bak content), confirming recovery used .bak rather than falling through to defaults");

    // The channels .bak survived the recovery path itself (same rule as Test 3, scoped to the recovery boundary). The no-op mutate that triggered recovery
    // does NOT rotate .bak: doMutate guards the main->.bak copy with `if(!result.recoveredFromBackup)`, and read() recovered here, so the rotation is skipped
    // entirely. .bak is left untouched because no rotation runs - not because a rotation re-wrote identical bytes.
    assert.equal(await readFile(pathInDataDir(ctx, "channels.json.bak"), "utf-8"), channelsBakBefore,
      "channels.json.bak must remain byte-identical - the recovery path does not touch .bak, and the recovering mutate skips the rotation entirely");
  });
});
