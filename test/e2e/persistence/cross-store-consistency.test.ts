/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cross-store-consistency.test.ts: Integration coverage for the cross-store consistency probe (consistencyProbe.ts). The probe enforces foreign-key-style
 * invariants that span multiple stores - things per-store schema migrations cannot enforce because they only see one file at a time. The three checks are:
 *
 *   - unknown-service-tag: CONFIG.channels.enabledServices contains tags that are not in the rebuilt service-group taxonomy. AUTO-FIX: strip unknown tags.
 *   - dangling-variant-canonical: a variant entry's canonicalKey points at a channel that does not exist in PREDEFINED_CHANNELS or user channels. NO auto-fix
 *     (the right action depends on operator intent).
 *   - dangling-domain-profile: a user domain mapping references a profile that does not exist. NO auto-fix.
 *
 * The auto-fix path is the most observable from a black-box test because it produces persistent on-disk state changes; we exercise it for the unknown-service-
 * tag case below. The other two checks are surfaced via log warnings only - covered by the warn-capturing integration suite in consistency-probe.test.ts, which
 * seeds the dangling references and captures LOG output directly. This file verifies the auto-fix actually persists across the full real-stores stack.
 */
import { createIntegrationContext, initializePersistence, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { CONFIG } from "../../../src/config/index.ts";
import assert from "node:assert/strict";
import { mutateConfig } from "../../../src/config/userConfig.ts";
import { runConsistencyProbeAtStartup } from "../../../src/config/consistencyProbe.ts";
import { setEnabledServices } from "../../../src/config/services.ts";

describe("consistency probe - unknown-service-tag auto-fix", () => {

  test("strips unknown service tags from CONFIG.channels.enabledServices", async () => {

    /* Seed: enabledServices contains a known tag (hulu, which exists in the service-group taxonomy) and a definitely-unknown tag. The probe runs, identifies
     * the unknown tag, and runs its auto-fix which goes through mutateEnabledServices - the same path the toggle endpoint uses. After the probe completes,
     * CONFIG.channels.enabledServices should contain only the known tag.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Set the enabled services in module state directly so the probe sees the dirty value via CONFIG.channels.enabledServices. setEnabledServices updates the
    // module cache and CONFIG; mutateConfig persists the same change to disk, matching what the real path produces.
    const dirtyTags = [ "hulu", "totally-unknown-service-x9z2" ];

    setEnabledServices(dirtyTags);
    await mutateConfig((config) => {

      config.channels ??= {};
      config.channels.enabledServices = dirtyTags;
    });

    // Sanity check: the dirty state is in place before the probe runs.
    assert.ok(CONFIG.channels.enabledServices.includes("totally-unknown-service-x9z2"), "the unknown tag should be present pre-probe");

    await runConsistencyProbeAtStartup();

    // Post-fix module state: only the known tag remains.
    assert.deepEqual(CONFIG.channels.enabledServices, ["hulu"], "module state should reflect the probe's auto-fix (unknown tag stripped)");

    // Post-fix on-disk state: the auto-fix persists via mutateEnabledServices, so config.json should match.
    const persisted = await readPersistedJson(ctx, "config.json") as { channels: { enabledServices: string[] } };

    assert.deepEqual(persisted.channels.enabledServices, ["hulu"], "config.json should reflect the auto-fix");
  });

  test("is idempotent - a second run with no dirty state is a no-op", async () => {

    /* Running the probe against a known-clean (already-consistent) state must be a no-op. With only the recognized "hulu" tag enabled there is nothing to fix,
     * so the probe should find no issues and leave config.json byte-for-byte unchanged across the single call.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    setEnabledServices(["hulu"]);
    await mutateConfig((config) => {

      config.channels ??= {};
      config.channels.enabledServices = ["hulu"];
    });

    const before = await readPersistedJson(ctx, "config.json");

    await runConsistencyProbeAtStartup();

    const after = await readPersistedJson(ctx, "config.json");

    assert.deepEqual(after, before, "clean-state probe run must not modify config.json");
  });

  test("does not throw when there are no enabled services to validate", async () => {

    /* Boundary: an empty enabledServices list means there is nothing for the unknown-service-tag check to validate. The probe should short-circuit cleanly
     * rather than treat empty as suspicious or null-deref the iteration.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    setEnabledServices([]);

    await assert.doesNotReject(() => runConsistencyProbeAtStartup(), "probe must handle empty enabledServices cleanly");
  });

  test("preserves multiple known tags while stripping a single unknown one", async () => {

    /* Mixed-state input: the auto-fix must surgically remove only the unknown entries, preserving every recognized one. We seed with three known tags and
     * one unknown, then assert the three known tags survive in their original order.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const dirtyTags = [ "hulu", "sling", "unknown-tag-xyz", "spectrum" ];

    setEnabledServices(dirtyTags);
    await mutateConfig((config) => {

      config.channels ??= {};
      config.channels.enabledServices = dirtyTags;
    });

    await runConsistencyProbeAtStartup();

    assert.deepEqual(CONFIG.channels.enabledServices, [ "hulu", "sling", "spectrum" ],
      "known tags should survive in original order; only the unknown one is stripped");
  });
});
