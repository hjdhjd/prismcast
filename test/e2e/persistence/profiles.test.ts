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
  mutateProfiles, validateProfile, validateProfileKey } from "../../../src/config/userProfiles.ts";
import { describe, test } from "node:test";
import { getAllServiceTags, getChannelServiceLabel, getResolvedChannel, getServiceTagForChannel } from "../../../src/config/services.ts";
import { getProfiles, resolveProfile } from "../../../src/config/profiles.ts";
import assert from "node:assert/strict";
import { firstOf } from "../../../src/testing.helpers.ts";
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

/* Resolution has to be total over whatever profiles.json holds. The profile editor rejects a user profile that extends another user profile, but the file is
 * hand-editable and a service pack can be assembled by hand, so the store can carry an extends chain that closes back on itself. Resolution runs on the
 * playlist, channel table, and tune paths, so a stored cycle that resolution could not survive would take the settings page, the M3U feed, or a timer-driven
 * tune down with it.
 *
 * These fixtures live at the integration tier because the resolver reads the module-level user profile map, which is populated only by real file I/O -
 * mutateProfiles writes the store and initializeUserProfiles loads it back, exactly as a restart would.
 */
describe("profile resolution over stored user profiles", () => {

  test("a two-profile cycle resolves to the collected chain over defaults instead of exhausting the stack", async () => {

    /* The mutual-reference case: cycleAlpha extends cycleBeta, which extends cycleAlpha. Both profiles set channelSelector, to different values, so the
     * assertion proves merge ORDER rather than mere presence - the named profile's value has to win over the value of what it extends. cycleBeta also sets a
     * flag cycleAlpha leaves alone, so the assertions prove that both members of the cycle contribute.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateProfiles((data) => {

      data.profiles["cycleAlpha"] = { channelSelector: "alpha-wins", extends: "cycleBeta" };
      data.profiles["cycleBeta"] = { channelSelector: "beta-loses", extends: "cycleAlpha", lockVolumeProperties: true };
    });

    await initializeUserProfiles();

    const resolved = resolveProfile("cycleAlpha");

    assert.equal(resolved.channelSelector, "alpha-wins", "the named profile's value overrides the value of the profile it extends");
    assert.equal(resolved.lockVolumeProperties, true, "the profile reached through the extends hop still contributes its own flags");
    assert.equal(resolved.staticCapture, false, "flags set by neither profile fall back to the default");

    // Resolving the same cycle a second time must produce the same result. The visited set is per-call state; hoisting or memoizing it would leak the first
    // resolution's bookkeeping into the second and silently truncate the chain here.
    assert.deepEqual(resolveProfile("cycleAlpha"), resolved, "a second resolution of the same cyclic profile returns an identical result");
  });

  test("a profile that extends itself resolves to its own flags over defaults", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateProfiles((data) => {

      data.profiles["selfExtending"] = { channelSelector: "self-wins", extends: "selfExtending", waitForNetworkIdle: true };
    });

    await initializeUserProfiles();

    const resolved = resolveProfile("selfExtending");

    assert.equal(resolved.channelSelector, "self-wins", "the profile's own flags apply");
    assert.equal(resolved.waitForNetworkIdle, true, "every flag the profile sets applies");
    assert.equal(resolved.useRequestFullscreen, false, "unset flags fall back to the default");
  });

  test("a user profile extending a builtin resolves to the builtin's flags with the user profile's own flags on top", async () => {

    /* The parity check for the ordinary case: embeddedPlayer contributes needsIframeHandling, and the user profile sets that same flag to the OPPOSITE value, so
     * the assertion proves precedence rather than mere inheritance - a merge applied in the wrong order resolves it to true here. The second flag is the option
     * a profile turns the native Fullscreen API on through, and the user profile and embeddedPlayer both set it, so it reaches the resolved shape whichever of
     * the two the merge takes it from.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateProfiles((data) => {

      data.profiles["userOverBuiltin"] = { channelSelector: "user-value", extends: "embeddedPlayer", needsIframeHandling: false, useRequestFullscreen: true };
    });

    await initializeUserProfiles();

    const resolved = resolveProfile("userOverBuiltin");

    assert.equal(resolved.needsIframeHandling, false, "the user profile's value overrides the value set further up the chain");
    assert.equal(resolved.useRequestFullscreen, true, "the flag both the user profile and the builtin set reaches the resolved shape");
    assert.equal(resolved.channelSelector, "user-value", "the user profile's own flag applies");
    assert.equal(resolved.selectReadyVideo, false, "flags no profile in the chain sets fall back to the default");
  });

  test("a user profile turns the native fullscreen call on over a chain that contributes none of its own", async () => {

    /* The opt-in proof the precedence case above cannot give. clickToPlayApi extends fullscreenApi and neither sets the flag that drives the native
     * requestFullscreen call, so a resolved profile carrying it can only have taken it from the user profile. The second assertion is the control that keeps
     * that true: should the parent chain ever start contributing the flag, it fails here rather than leaving the opt-in assertion to pass on an inherited value.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateProfiles((data) => {

      data.profiles["userNativeFullscreen"] = { channelSelector: "opt-in-value", extends: "clickToPlayApi", useRequestFullscreen: true };
    });

    await initializeUserProfiles();

    const resolved = resolveProfile("userNativeFullscreen");

    assert.equal(resolved.useRequestFullscreen, true, "the user profile's own opt-in reaches the resolved shape");
    assert.equal(resolveProfile("clickToPlayApi").useRequestFullscreen, false, "no profile in the parent chain sets the native fullscreen flag");
    assert.equal(resolved.channelSelector, "opt-in-value", "the user profile's own flag applies");
  });

  test("a builtin chain two hops deep resolves every ancestor's flags and carries no metadata fields", async () => {

    /* embeddedDynamicMultiVideo extends embeddedPlayer, which extends fullscreenApi - flags contributed at every level below the named profile except the root,
     * and metadata carried at every level. Asserting that the metadata keys are structurally ABSENT is what catches an implementation that strips them from only the
     * last entry it merges: object spread accepts the extra properties, so flag equality alone would pass against a resolved profile still carrying a stale
     * category or extends.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);
    await initializeUserProfiles();

    const resolved = resolveProfile("embeddedDynamicMultiVideo");

    assert.equal(resolved.useRequestFullscreen, true, "the intermediate ancestor's native call is inherited, the root ancestor contributing no flag of its own");
    assert.equal(resolved.needsIframeHandling, true, "the intermediate ancestor's flag is inherited");
    assert.equal(resolved.selectReadyVideo, true, "the named profile's own flags apply");
    assert.equal(resolved.waitForNetworkIdle, true, "every flag the named profile sets applies");

    for(const metadataField of [ "category", "description", "extends", "summary" ]) {

      assert.equal(metadataField in resolved, false, "the resolved profile carries no " + metadataField + " field from any level of the chain");
    }
  });

  test("an extends target that exists nowhere ends the walk and the flags collected before it still apply", async () => {

    /* The unresolvable hop sits BEYOND the first one, so the fixture proves the walk keeps what it collected rather than discarding the whole chain. Against a
     * resolver that abandoned the collection on a miss, chainHead would resolve to bare defaults.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateProfiles((data) => {

      data.profiles["chainHead"] = { channelSelector: "head-value", extends: "chainMiddle" };
      data.profiles["chainMiddle"] = { extends: "noSuchProfileAnywhere", lockVolumeProperties: true };
    });

    await initializeUserProfiles();

    const resolved = resolveProfile("chainHead");

    assert.equal(resolved.channelSelector, "head-value", "the named profile's flags apply");
    assert.equal(resolved.lockVolumeProperties, true, "the resolvable hop before the missing one still contributes");
    assert.equal(resolved.waitForNetworkIdle, false, "flags no collected profile sets fall back to the default");
  });

  test("validateProfile names the user-to-user extends rule when the target is a profile in the store", async () => {

    /* The reporting half of the same story: resolution degrades quietly, validation is what tells the user which rule the stored profile breaks. A target that
     * exists as a user profile is the hop every cycle is built from, and calling it non-existent would send the user looking for a profile they can plainly
     * see. validateProfile is called directly here because initializeUserProfiles does not invoke it - that wiring lives in validateProfiles on the boot path.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateProfiles((data) => {

      data.profiles["parentProfile"] = { extends: "fullscreenApi" };
    });

    await initializeUserProfiles();

    const userTargetErrors = validateProfile("childProfile", { extends: "parentProfile" });

    assert.equal(userTargetErrors.length, 1, "a user-profile extends target produces exactly one error");
    assert.match(firstOf(userTargetErrors), /extends references user profile 'parentProfile'/, "the message names the target as a user profile");
    assert.match(firstOf(userTargetErrors), /must extend a builtin profile/, "the message states the rule that was broken");

    // A target that exists in neither the builtin tables nor the store keeps the not-found message, so the sharper wording is scoped to the case it describes.
    const missingTargetErrors = validateProfile("childProfile", { extends: "noSuchProfileAnywhere" });

    assert.match(firstOf(missingTargetErrors), /non-existent builtin profile 'noSuchProfileAnywhere'/, "an unresolvable target keeps the not-found message");
  });
});

/* Builtin profile keys are reserved for names that are not already user data, which makes the rule depend on the loaded user store - module state that only
 * real file I/O populates. The unit tier covers the reservation itself against an empty store; the grandfather clause needs a key to already be there, so its
 * pins live here.
 */
describe("validateProfileKey grandfathering of keys already in the store", () => {

  test("a stored key that shadows a provider profile stays editable", async () => {

    /* A profile saved under a provider-profile name before the reservation covered that table is permanently shadowed at resolution, but it is still the user's
     * data. Rejecting the key on every save would leave it uneditable and undeletable behind a message asking for a name the user cannot change. isNew is false
     * (the edit path) so the pre-existing duplicate-key check cannot be what decides this case - only the builtin-collision clause is in play.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateProfiles((data) => {

      data.profiles["disneyNow"] = { description: "A legacy shadowed profile.", extends: "fullscreenApi" };
    });

    await initializeUserProfiles();

    assert.equal(validateProfileKey("disneyNow", false), undefined, "a stored key that shadows a provider profile is accepted on edit");
  });

  test("a stored key that shadows a general builtin profile stays editable", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateProfiles((data) => {

      data.profiles["fullscreenApi"] = { description: "A legacy shadowed profile.", extends: "keyboardFullscreen" };
    });

    await initializeUserProfiles();

    assert.equal(validateProfileKey("fullscreenApi", false), undefined, "a stored key that shadows a general builtin profile is accepted on edit");
  });

  test("a builtin-colliding key absent from the store is rejected on both the create and the import framing", async () => {

    /* The other side of the clause. The store here holds an unrelated profile, so the key under test is absent from it and the reservation applies whichever
     * value isNew carries - an implementation that keyed the reservation off the isNew flag instead of store membership would accept the false case.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateProfiles((data) => {

      data.profiles["unrelatedProfile"] = { extends: "fullscreenApi" };
    });

    await initializeUserProfiles();

    assert.match(validateProfileKey("disneyNow", true) ?? "", /conflicts with a builtin/, "the create framing is rejected");
    assert.match(validateProfileKey("disneyNow", false) ?? "", /conflicts with a builtin/, "the import framing is rejected");
  });
});
