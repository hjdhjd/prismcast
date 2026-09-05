/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userchannels-public-api.test.ts: Integration-tier coverage for the smaller public exports of userChannels.ts that are reachable only after the persistence
 * subsystem is initialized. Each describe block asserts one or more such exports:
 *
 *   - getStoredUserChannels: defensive copy contract.
 *   - isChannelAvailable: in-merged-map predicate.
 *   - getPredefinedScopeCounts: all/east/pacific counts under a service filter.
 *   - mutateChannelDisplayPrefs: partial-update + post-write CONFIG sync.
 *   - markSetupCompleted: one-shot transition.
 *   - disablePredefinedChannels/enablePredefinedChannels: empty-keys early return (mutateDisabledPredefined internal no-op) - no write occurs when the input
 *     array is empty.
 *   - transformChannelTags: no-op skip via isDeepStrictEqual on sorted tags + null-empty-tags branch.
 */
import { createIntegrationContext, initializePersistence, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { disablePredefinedChannels, enablePredefinedChannels, getPredefinedScopeCounts, getStoredUserChannels, initializeUserChannels, isChannelAvailable,
  markSetupCompleted, mutateChannelDisplayPrefs, mutateChannels, transformChannelTags } from "../../../src/config/userChannels.ts";
import { CONFIG } from "../../../src/config/index.ts";
import assert from "node:assert/strict";

describe("getStoredUserChannels: defensive copy", () => {

  test("returns a fresh object reference; mutating the result does not affect subsequent reads", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateChannels((data) => {

      data.channels["mychannel"] = { name: "Mine", url: "https://example.test/" };
    });

    const snapshot1 = getStoredUserChannels();

    /* Mutate the snapshot - the contract is that this does NOT alter the module-level state.
     */
    Reflect.deleteProperty(snapshot1, "mychannel");
    (snapshot1 as Record<string, unknown>)["injected"] = { name: "Injected", url: "https://injected.test/" };

    const snapshot2 = getStoredUserChannels();

    assert.ok(snapshot2["mychannel"], "module state preserved despite mutation on the first snapshot");
    assert.equal("injected" in snapshot2, false, "module state did not pick up the injected key");
  });
});

describe("isChannelAvailable", () => {

  test("returns true for a key in the merged channel map (a known predefined canonical)", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    /* abc is a predefined canonical; with no service filter active and no disable, it appears in getAllChannels.
     */
    assert.equal(isChannelAvailable("abc"), true);
  });

  test("returns false for a key that exists in no source", () => {

    /* No bring-up needed for the negative case - getAllChannels returns the merge result, and a totally-unknown key is structurally absent.
     */
    assert.equal(isChannelAvailable("definitely-not-a-channel-x9z2"), false);
  });

  test("returns false for a predefined channel that has been disabled", async () => {

    /* Disabling a predefined channel removes it from getAllChannels (the merge filters by enabled). isChannelAvailable should reflect that.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await disablePredefinedChannels(["abc"]);

    assert.equal(isChannelAvailable("abc"), false);

    /* Re-enable so the post-test state is clean (other tests share the predefined catalog).
     */
    await enablePredefinedChannels(["abc"]);
  });
});

describe("getPredefinedScopeCounts", () => {

  test("returns all/east/pacific counts with shape { enabled, total }", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const counts = getPredefinedScopeCounts();

    assert.equal(typeof counts.all.enabled, "number");
    assert.equal(typeof counts.all.total, "number");
    assert.equal(typeof counts.east.enabled, "number");
    assert.equal(typeof counts.east.total, "number");
    assert.equal(typeof counts.pacific.enabled, "number");
    assert.equal(typeof counts.pacific.total, "number");
    assert.ok(counts.all.total > 0, "predefined catalog is non-empty so total is > 0");
    assert.ok(counts.all.total >= counts.east.total + counts.pacific.total, "all >= east + pacific (some channels may be neither)");
  });

  test("disabling a predefined channel decrements all.enabled while leaving total unchanged", async () => {

    /* The enabled count filters by !disabled; total counts the universe regardless of disabled state. Disabling a single channel must drop enabled by 1
     * while total stays put.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const before = getPredefinedScopeCounts();

    await disablePredefinedChannels(["abc"]);

    const after = getPredefinedScopeCounts();

    assert.equal(after.all.total, before.all.total, "total is unaffected by enable/disable");
    assert.equal(after.all.enabled, before.all.enabled - 1, "enabled decreases by exactly 1");

    await enablePredefinedChannels(["abc"]);
  });
});

describe("mutateChannelDisplayPrefs: partial update + runtime CONFIG sync", () => {

  test("partial input: absent fields are copied from CONFIG; the explicit field overrides", async () => {

    /* Contract: the function reads absent fields from runtime CONFIG and writes the union back through mutateConfig. After the call, runtime CONFIG reflects
     * the merged shape - this is what subsequent renders / playlist generators read.
     *
     * Note on disk shape: filterDefaults strips fields equal to defaults via the PRESERVED_FIELDS predicates (differsFromStringDefault for direction/field,
     * isNonEmptyArray for visibleColumns). When the explicit override differs from default but the inherited fields still equal defaults, only the override
     * lands on disk. The contract worth asserting is the runtime CONFIG state, not the on-disk shape (which is filterDefaults' contract).
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    /* Snapshot pre-call CONFIG values; the function's "absent fields copied from CONFIG" rule means these survive into the post-call state.
     */
    const preDirection = CONFIG.channels.channelSortDirection;

    await mutateChannelDisplayPrefs({ channelSortField: "channelNumber" });

    assert.equal(CONFIG.channels.channelSortField, "channelNumber", "runtime CONFIG reflects the override");
    assert.equal(CONFIG.channels.channelSortDirection, preDirection, "absent input field copied from runtime CONFIG (pre-call value preserved)");

    /* Disk shape: only fields that differ from defaults survive filterDefaults. channelSortField is "channelNumber" (differs from default "name"), so it
     * persists. channelSortDirection's persistence depends on whether the snapshotted preDirection equals the default "asc".
     */
    const persisted = await readPersistedJson(ctx, "config.json") as { channels?: { channelSortField?: string; channelSortDirection?: string } };

    assert.equal(persisted.channels?.channelSortField, "channelNumber", "explicit override persisted");
  });
});

describe("markSetupCompleted: one-shot transition", () => {

  test("sets the runtime CONFIG flag to true; in-memory state survives the call", async () => {

    /* Contract worth asserting at integration tier: after the call, CONFIG.channels.setupCompleted is true. Subsequent table renders and route handlers read this
     * runtime flag.
     *
     * On-disk persistence note: setupCompleted is NOT in PRESERVED_FIELDS or CONFIG_METADATA, so filterDefaults strips it from the on-disk shape. The flag is
     * effectively in-memory only and gets re-inferred at next startup via initializeUserChannels' "if any services or user channels exist" branch. This is
     * intentional - the function's mutateConfig call is a no-op on disk, but the flag survives across restarts via the inference, not via persistence. The
     * companion suite below asserts the inference path that closes the loop.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    /* Module-level CONFIG state is shared across tests in the same process. We only assert the post-call state is true - the pre-call state may be either
     * false (fresh process) or true (a prior test set it). Either way, after markSetupCompleted the flag must be true.
     */
    await markSetupCompleted();

    assert.equal(CONFIG.channels.setupCompleted, true, "runtime flag flipped to true");
  });
});

describe("setupCompleted re-inference at startup (cross-store: services -> setupCompleted)", () => {

  test("initializeUserChannels sets setupCompleted=true when enabledServices is non-empty even though config.json carries no setupCompleted entry", async () => {

    /* Counterpart to the markSetupCompleted persistence note above. The flag is intentionally not preserved through filterDefaults; the architectural answer
     * is re-inference at boot. This test seeds the precondition (CONFIG carries enabledServices, runtime flag starts false) and asserts that calling the
     * channel-store initializer flips the flag back to true. If a future refactor removes the inference branch in initializeUserChannels, the markSetupCompleted
     * write becomes a real bug (the flag would not survive a restart) - this test fails first.
     *
     * We do not seed config.json with setupCompleted because that's the whole point: the flag is observably absent from disk, the inference rebuilds it from
     * the cross-store signal "user has any services or channels".
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    /* Force the precondition: a configured enabledServices list (the cross-store signal the inference reads) and an explicitly false runtime flag. CONFIG is
     * module-level so we set the slice directly; the inference branch runs only when the runtime flag is currently false.
     */
    CONFIG.channels.enabledServices = ["hulu"];
    CONFIG.channels.setupCompleted = false;

    await initializeUserChannels();

    assert.equal(CONFIG.channels.setupCompleted, true, "inference re-establishes setupCompleted from observed services state");
  });
});

describe("disablePredefinedChannels/enablePredefinedChannels: empty-keys early return (mutateDisabledPredefined internal no-op)", () => {

  test("disablePredefinedChannels([]) is a no-op (no disk write, no exception)", async () => {

    /* Empty input -> the helper returns immediately without entering mutateConfig. Verify by reading the on-disk state before and after and confirming nothing
     * changed.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const before = await readPersistedJson(ctx, "config.json").catch(() => ({}));

    await disablePredefinedChannels([]);
    await enablePredefinedChannels([]);

    const after = await readPersistedJson(ctx, "config.json").catch(() => ({}));

    /* The on-disk state is structurally identical (both sides may legitimately differ in unrelated fields if other startup work touched config.json, so we
     * narrow the assertion to the disabledPredefined slice).
     */
    const beforeDisabled = (before as { channels?: { disabledPredefined?: string[] } }).channels?.disabledPredefined ?? [];
    const afterDisabled = (after as { channels?: { disabledPredefined?: string[] } }).channels?.disabledPredefined ?? [];

    assert.deepEqual(afterDisabled, beforeDisabled, "empty-keys early return does not change disabledPredefined");
  });
});

describe("transformChannelTags", () => {

  test("no-op skip via isDeepStrictEqual: when transform produces an unchanged sorted tag set, no write occurs for that channel", async () => {

    /* The isDeepStrictEqual call on the sorted tags arrays must skip channels whose effective tag set is unchanged after the transform. We
     * exercise via an identity transform: the result should report no affected keys for predefined channels (their tags don't change).
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const result = await transformChannelTags(
      (entry) => entry.key === "abc",
      // The identity transform returns the same tags, so the sorted-equality check skips every selected channel.
      (tags) => tags
    );

    assert.deepEqual(result.affectedKeys, [], "identity transform produces no changes; affectedKeys is empty");
  });

  test("null-empty-tags branch: returning [] from the transform writes null (the clear signal) for the entry", async () => {

    /* When transform returns an empty array, the tags field is set to null (the codebase-wide "clear this field" signal). The next read should reflect the
     * cleared tag set on the affected channel.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    /* Filter to abc canonical (a predefined with predefined tags) and clear all tags via empty-array return.
     */
    const result = await transformChannelTags(
      (entry) => entry.key === "abc",
      () => []
    );

    assert.deepEqual(result.affectedKeys, ["abc"], "abc was affected (its predefined tags were cleared)");

    /* The on-disk shape now has tags: null on abc (the storage normalizer survives the null - it's an explicit clear against the predefined base, which is
     * non-empty).
     */
    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;
    const abc = persisted["abc"] as { tags?: unknown } | undefined;

    assert.equal(abc?.tags, null, "tags cleared via null storage convention");
  });

  test("multiple matching entries: transform applies to every entry the filter selects", async () => {

    /* The filter predicate decides which entries to transform. Applied to two predefined canonicals, we get two affected keys.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const result = await transformChannelTags(
      (entry) => (entry.key === "abc") || (entry.key === "nbc"),
      // The transform appends a Custom tag to every selected entry.
      (tags) => [ ...tags, "Custom" ]
    );

    assert.deepEqual(result.affectedKeys.toSorted(), [ "abc", "nbc" ]);
  });
});
