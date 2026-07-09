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
import { createIntegrationContext, initializePersistence, pathInDataDir, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { deleteUserProfile, getProfilesParseErrorMessage, getUserDomains, getUserProfiles, hasProfilesParseError, initializeUserProfiles,
  mutateProfiles, validateProfileKey } from "../../../src/config/userProfiles.ts";
import { describe, test } from "node:test";
import { getAllServiceTags, getChannelServiceLabel, getResolvedChannel, getServiceTagForChannel } from "../../../src/config/services.ts";
import assert from "node:assert/strict";
import { firstOf } from "../../../src/testing.helpers.ts";
import { getProfiles } from "../../../src/config/profiles.ts";
import { mutateChannels } from "../../../src/config/userChannels.ts";
import { writeFile } from "node:fs/promises";

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

  test("hasProfilesParseError and getProfilesParseErrorMessage surface a corrupt profiles.json after init", async () => {

    /* The accessor surface for the file-store framework's "loud, recoverable, never silent" parse-error contract. When profiles.json is unparseable AND the
     * .bak rotation has nothing usable, initializeUserProfiles loads defaults and stamps the parse-error flag plus an operator-facing message. Web UI banners
     * read these accessors to surface the failure.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    /* Corrupt the on-disk profiles.json directly. The integration harness's framework already initialized the data dir; we overwrite the live file with
     * non-JSON content and re-initialize the user-profiles loader to drive the parse-error path.
     */
    await writeFile(pathInDataDir(ctx, "profiles.json"), "this-is-not-valid-json", "utf-8");
    await initializeUserProfiles();

    assert.equal(hasProfilesParseError(), true, "parse-error flag set after a corrupt file load");

    const message = getProfilesParseErrorMessage();

    assert.ok(message, "parse-error message populated after a corrupt file load");
    assert.equal(typeof message, "string", "message is a string for UI display");

    /* Reset module state for subsequent tests by overwriting the file with valid empty JSON and re-initializing. The accessors should return cleanly. */
    await writeFile(pathInDataDir(ctx, "profiles.json"), JSON.stringify({ schemaVersion: 2 }), "utf-8");
    await initializeUserProfiles();
  });
});

/* The accessors below are not thin persistence round-trips - they compose the loaded user-profile and user-domain state with builtin tables to derive the
 * profile dropdown, the service filter, and per-channel service identity. Each needs the persistence tier to seed real user state (a profile, a domain mapping,
 * or a channel that references one) before the derivation can be exercised, which is why they belong here rather than at the pure unit tier.
 */
describe("profile-derived accessors", () => {

  test("getProfiles emits a user profile as source user with default custom category and a summary that falls back to description, merged and sorted", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Seed a single user profile that carries only a description - no explicit category, no summary. getProfiles must tag it source: "user", default its
    // category to "custom", and fill the summary from the description fallback. The key is deliberately non-builtin so it merges alongside the builtin
    // SITE_PROFILES entries rather than colliding with one.
    await mutateProfiles((data) => {

      data.profiles["myCustomTestProfile"] = { description: "A custom user profile for testing.", extends: "fullscreenApi" };
    });

    await initializeUserProfiles();

    const profiles = getProfiles();
    const mine = profiles.find((p) => (p.name === "myCustomTestProfile"));

    assert.ok(mine, "the seeded user profile should appear in the getProfiles output");
    assert.equal(mine.source, "user", "a user-defined profile is tagged source: user");
    assert.equal(mine.category, "custom", "a user profile with no explicit category defaults to custom");
    assert.equal(mine.description, "A custom user profile for testing.", "the description is carried through verbatim");
    assert.equal(mine.summary, "A custom user profile for testing.", "the summary falls back to the description when no summary is set");

    // The merged list carries builtin profiles too, and the whole result is re-sorted alphabetically by name. A regression that appended user profiles without
    // re-sorting, or dropped the builtins entirely, would break one of these.
    assert.ok(profiles.length > 1, "builtin profiles are merged with the user profile");
    assert.ok(profiles.some((p) => (p.source === "builtin")), "builtin profiles are present in the merged list");

    const names = profiles.map((p) => p.name);

    assert.deepEqual(names, [...names].toSorted((a, b) => a.localeCompare(b)), "the merged builtin + user profile list is sorted alphabetically by name");
  });

  test("getAllServiceTags surfaces a serviceTag declared only in a user domain mapping, keeping direct first", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Seed a user domain mapping that carries a serviceTag and service display name but references no channels. getAllServiceTags scans user domain mappings in
    // addition to channels, so a freshly-created service surfaces in the filter before any channel is assigned to it. The domain is non-builtin, so its metadata
    // is taken from the user mapping rather than shadowed by DOMAIN_CONFIG.
    await mutateProfiles((data) => {

      data.domains["my-user-service.example"] = { profile: "myCustomTestProfile", service: "My User Service", serviceTag: "myuserservice" };
    });

    await initializeUserProfiles();

    const tags = getAllServiceTags();
    const mine = tags.find((t) => (t.tag === "myuserservice"));

    assert.ok(mine, "the user domain's serviceTag surfaces in getAllServiceTags");
    assert.equal(mine.displayName, "My User Service", "the display name comes from the user domain's service field");

    // The direct tag is always emitted first regardless of alphabetical order.
    assert.equal(firstOf(tags).tag, "direct", "the direct tag is always emitted first");

    // Everything after direct is sorted by display name. A regression that stopped sorting, or sorted by tag instead of display name, would break this.
    const remainderNames = tags.slice(1).map((t) => t.displayName);

    assert.deepEqual(remainderNames, [...remainderNames].toSorted((a, b) => a.localeCompare(b)), "non-direct service tags are sorted by display name");
  });

  test("a channel assigned an explicit user profile derives its service tag and label from the profile's domain mapping", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Seed a user profile plus a domain mapping that binds the profile key to a custom serviceTag and service name.
    await mutateProfiles((data) => {

      data.profiles["myProviderProfile"] = { description: "Provider profile.", extends: "fullscreenApi" };
      data.domains["provider.example"] = { profile: "myProviderProfile", service: "My Provider", serviceTag: "myprovider" };
    });

    // Seed a channel whose URL domain has no builtin service identity but which explicitly references the user profile. mutateChannels rebuilds the service
    // groups so the channel resolves through getResolvedChannel afterward.
    await mutateChannels((data) => {

      data.channels["profile-bound"] = { name: "Profile Bound", profile: "myProviderProfile", url: "https://unmapped-domain.example/watch" };
    });

    // The service tag comes from the profile's domain mapping (myprovider), overriding the URL-domain-derived builtin identity - which for an unmapped domain
    // would fall back to "direct".
    assert.equal(getServiceTagForChannel("profile-bound"), "myprovider", "the service tag is taken from the user profile's domain mapping");

    const channel = getResolvedChannel("profile-bound");

    assert.ok(channel, "the seeded channel resolves via the merged channel map");
    assert.equal(getChannelServiceLabel(channel), "My Provider", "the service label is taken from the user profile's domain mapping, not the URL domain");
  });

  test("validateProfileKey rejects a key that already exists among the loaded user profiles", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateProfiles((data) => {

      data.profiles["existingUserProfile"] = { description: "Already here.", extends: "fullscreenApi" };
    });

    await initializeUserProfiles();

    // isNew = true triggers the duplicate check against the loaded user profiles map. The co-located unit test in src/config/userProfiles.test.ts covers the
    // format and required-field branches, but not this duplicate branch, which needs loaded module state to have a key to collide with.
    const message = validateProfileKey("existingUserProfile", true);

    assert.ok(message, "a duplicate key returns an error message");
    assert.match(message, /already exists/, "the duplicate-key message states that the key already exists");
  });
});
