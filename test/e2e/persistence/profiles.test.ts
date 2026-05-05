/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * profiles.test.ts: Integration coverage for the user profiles persistence layer. User profiles are custom site profiles + domain mappings the user creates
 * via the profile builder; they live in profiles.json alongside the channels and config files. This suite verifies CRUD behavior at the integration tier:
 * mutate -> persist -> reload -> retrieve, with the same fidelity production code sees.
 *
 * The unit tier covers the parsing/validation logic in isolation. The integration value here is the cross-layer composition: the persisted shape goes
 * through the file store framework's serialization, the schema migration runner, and the readback verification - any layer breaking the round-trip would
 * silently corrupt user-authored profiles, and operators would only notice via a missing profile in the dropdown.
 */
import { createIntegrationContext, initializePersistence, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { deleteUserProfile, getUserDomains, getUserProfiles, initializeUserProfiles, mutateProfiles } from "../../../src/config/userProfiles.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

describe("user profiles persistence", () => {

  test("a profile written via mutateProfiles surfaces via getUserProfiles after reload", async () => {

    /* The save -> load round-trip. We write a profile, simulate a restart by re-running initializeUserProfiles, and assert the profile is in the loaded
     * in-memory map. Profile shape is intentionally minimal (description only) - we are exercising the persistence boundary, not the profile schema.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateProfiles((data) => {

      data.profiles["test-custom"] = { description: "test-custom-description" };
    });

    // Simulated restart: re-init reloads from disk into module state.
    await initializeUserProfiles();

    const profiles = getUserProfiles();

    assert.ok("test-custom" in profiles, "test-custom profile should be present after reload");
    assert.equal(profiles["test-custom"].description, "test-custom-description", "description preserved");
  });

  test("a domain mapping written via mutateProfiles surfaces via getUserDomains after reload", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateProfiles((data) => {

      data.domains["custom.example.test"] = { profile: "test-custom" };
    });

    await initializeUserProfiles();

    const domains = getUserDomains();

    assert.ok("custom.example.test" in domains, "domain mapping should be present after reload");
    assert.equal(domains["custom.example.test"].profile, "test-custom", "profile reference preserved");
  });

  test("deleteUserProfile removes the profile from disk and the in-memory map", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Seed.
    await mutateProfiles((data) => {

      data.profiles["to-delete"] = { description: "doomed" };
    });

    await deleteUserProfile("to-delete");

    // Module state should reflect the delete immediately.
    assert.equal("to-delete" in getUserProfiles(), false, "profile should be gone from module state");

    // Disk should also reflect it - read raw to confirm.
    const persisted = await readPersistedJson(ctx, "profiles.json") as { profiles?: Record<string, unknown> };
    const onDisk = persisted.profiles ?? {};

    assert.equal("to-delete" in onDisk, false, "profile should be gone from disk");
  });

  test("multiple writes to different profile keys all persist (no last-write-wins overwrite)", async () => {

    /* The mutate path is serialized; consecutive calls each apply their delta against the latest disk state. Three distinct profile additions should all
     * survive a reload.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateProfiles((data) => { data.profiles["profile-a"] = { description: "A" }; });
    await mutateProfiles((data) => { data.profiles["profile-b"] = { description: "B" }; });
    await mutateProfiles((data) => { data.profiles["profile-c"] = { description: "C" }; });

    await initializeUserProfiles();

    const profiles = getUserProfiles();

    assert.ok("profile-a" in profiles, "profile-a survives");
    assert.ok("profile-b" in profiles, "profile-b survives");
    assert.ok("profile-c" in profiles, "profile-c survives");
  });
});
