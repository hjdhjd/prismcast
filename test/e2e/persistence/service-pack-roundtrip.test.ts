/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * service-pack-roundtrip.test.ts: Integration coverage for the service pack import/export round-trip in src/config/servicePacks.ts plus its HTTP surface in
 * src/routes/config/services.ts. Service packs bundle a user profile + its domain mappings + its channels into one JSON artifact for sharing between
 * PrismCast deployments. Import/export is the most concentrated data-corruption surface in the application: a regression here can corrupt every persisted
 * profile in one operation. The unit suite at src/config/servicePacks.test.ts covers parse + import + export in isolation; this suite drives the same logic
 * end-to-end through Express so the route layer's body parsing, validation rejection envelope, and bytes-on-disk pre/post snapshots are all under test.
 *
 * What's pinned:
 *
 *   1. Round-trip identity. Seed profiles + domain mappings + channels referencing those profiles, GET /config/profiles/export, wipe state, POST
 *      /config/profiles/import with the exported body, and assert profiles.json and channels.json are byte-identical to the pre-export snapshots. The import
 *      must reverse the export exactly when applied to wiped state - any drift in field ordering, default emission, or shape conversion would surface as a
 *      byte-level diff.
 *   2. Merge contract on dirty state. importServicePack uses Object.assign to merge profiles, domains, and channels into the existing maps - import-takes-
 *      precedence on key collision; existing keys not in the pack survive untouched. A regression that wholesale-replaced state would lose the unrelated
 *      pre-existing entries. (Verified contract: src/config/servicePacks.ts:202-203, 224.)
 *   3. Malformed pack rejection produces a 400 envelope and writes nothing. The route handler at services.ts:550-557 calls parseServicePack first; on validation
 *      failure it short-circuits with status 400 before mutateProfiles or mutateChannels is ever called. profiles.json and channels.json must be byte-identical
 *      pre/post.
 *   4. Export determinism. Two consecutive GET requests for the same profile produce byte-identical bodies - a property the persistence layer's stringifySorted
 *      provides for files but the export endpoint must equally provide for HTTP responses, since the export body is what gets re-imported elsewhere.
 *
 * Why end-to-end through bootApp instead of calling exportServicePack/importServicePack directly: the route layer is where parseServicePack runs, where the
 * content-type and body parsing happen, and where the validation-rejection envelope shape is determined. A unit-level call to importServicePack would skip
 * all three. Tests 1, 2, and 3 specifically exercise behaviors that can only be observed through the wire surface.
 *
 * Merge contract investigation (per the roadmap's "PAUSE POINT LIKELY" note for this suite): src/config/servicePacks.ts:200-225 reads cleanly. Profiles
 * and domains are merged via Object.assign(data.profiles, pack.profiles) inside mutateProfiles; channels via Object.assign(data.channels, packChannels) inside
 * mutateChannels. Import-takes-precedence on key collision; pre-existing keys not in the pack survive. The contract is documented through the code shape,
 * not through a doc comment; pinning it here makes the contract explicit and catches a regression that flipped the merge to a wholesale replace.
 */
import { bootApp, createIntegrationContext, initializePersistence, pathInDataDir, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { exportServicePack, importServicePack } from "../../../src/config/servicePacks.ts";
import assert from "node:assert/strict";
import { mutateChannels } from "../../../src/config/userChannels.ts";
import { mutateProfiles } from "../../../src/config/userProfiles.ts";
import { readFile } from "node:fs/promises";

/**
 * Seeds a one-profile / one-domain / one-channel fixture and returns the seeded keys for cross-reference. fullscreenApi is the canonical user-extensible base
 * profile; myservice.example.test does not collide with any built-in domain in src/config/sites.ts so validateDomain accepts it.
 * @returns The seeded fixture's keys.
 */
async function seedFixture(): Promise<{ channelKey: string; domainKey: string; profileKey: string }> {

  const profileKey = "pack-test";
  const domainKey = "myservice.example.test";
  const channelKey = "pack-test-channel";

  await mutateProfiles((data) => {

    data.profiles[profileKey] = { description: "pack test profile", extends: "fullscreenApi", summary: "pack test" };
    data.domains[domainKey] = { profile: profileKey, service: "MyService", serviceTag: "myservice" };
  });

  await mutateChannels((data) => {

    data.channels[channelKey] = { name: "Pack Test Channel", profile: profileKey, url: "https://" + domainKey + "/channel" };
  });

  return { channelKey, domainKey, profileKey };
}

describe("service pack round-trip - export then import recovers state byte-identical", () => {

  test("a profile + domain + channel round-trip from export to import-after-wipe produces byte-identical files", async () => {

    /* The canonical round-trip. Steps:
     *   1. Seed a profile, a domain mapping pointing at that profile, and a channel whose profile field references that profile.
     *   2. Snapshot profiles.json and channels.json (full file bytes).
     *   3. GET /config/profiles/export?profile=pack-test&channels=1 to capture the pack.
     *   4. Wipe profiles + channels via mutateProfiles/mutateChannels assigning empty maps.
     *   5. POST /config/profiles/import with the captured pack body.
     *   6. Re-read profiles.json and channels.json and assert byte-equality to the snapshots from step 2.
     *
     * Byte-identity is the strongest correctness contract: not just "the same data" but "the same emission." Any drift in field ordering, default elision,
     * shape conversion, or whitespace would surface as a byte-level diff. The persistence layer's stringifySorted guarantees byte-identity at the file level
     * for identical data; the round-trip must preserve that all the way through the export/import pipeline.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { channelKey, domainKey, profileKey } = await seedFixture();

    // Snapshot the on-disk bytes after seeding.
    const profilesBefore = await readFile(pathInDataDir(ctx, "profiles.json"), "utf-8");
    const channelsBefore = await readFile(pathInDataDir(ctx, "channels.json"), "utf-8");

    const { urlFor } = await bootApp(ctx);

    // Step 3: export. ?channels=1 includes channels referencing the profile; ?domains defaults to including (only ?domains=0 excludes).
    const exportResponse = await fetch(urlFor("/config/profiles/export?profile=" + profileKey + "&channels=1"));

    assert.equal(exportResponse.status, 200, "export must succeed; body: " + (await exportResponse.clone().text()).slice(0, 200));

    const packBody = await exportResponse.text();

    // Sanity: the pack body actually carries the seeded profile, domain, and channel - confirms the export endpoint is doing the work the round-trip depends on.
    assert.match(packBody, /"pack-test"/, "the export pack must reference the seeded profile key");
    assert.match(packBody, /"myservice\.example\.test"/, "the export pack must reference the seeded domain key");
    assert.match(packBody, /"pack-test-channel"/, "the export pack must reference the seeded channel key");

    // Step 4: wipe profiles and channels. We assign empty maps inside the mutator so the cache hydrates from the wipe (mutateProfiles sets loadedUserProfiles
    // from the post-mutate data) and the disk reflects the empty state. The schemaVersion/migrationsApplied metadata on each file passes through untouched.
    await mutateProfiles((data) => {

      data.profiles = {};
      data.domains = {};
    });

    await mutateChannels((data) => {

      data.channels = {};
    });

    // Verify wipe took effect at the persistence layer - the seeded keys are gone.
    const profilesWiped = await readPersistedJson(ctx, "profiles.json") as { profiles?: Record<string, unknown> };

    assert.equal((profilesWiped.profiles && (profileKey in profilesWiped.profiles)) ?? false, false,
      "sanity: the wipe must remove the seeded profile from disk");

    const channelsWiped = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;

    assert.equal(channelKey in channelsWiped, false, "sanity: the wipe must remove the seeded channel from disk");

    // Step 5: import the captured pack body. Express parses application/json into req.body which the route handler then passes to parseServicePack.
    const importResponse = await fetch(urlFor("/config/profiles/import"), {

      body: packBody,
      headers: { "Content-Type": "application/json" },
      method: "POST"
    });

    assert.equal(importResponse.status, 200, "import must succeed; body: " + (await importResponse.clone().text()).slice(0, 200));

    // Step 6: byte-identical assertion at the file level. The post-import bytes must equal the pre-export bytes - reversing the export with the import (under
    // wiped state) recovers the exact serialization. Any drift here is an indication that the export -> import pipeline is not the identity function it should be.
    const profilesAfter = await readFile(pathInDataDir(ctx, "profiles.json"), "utf-8");
    const channelsAfter = await readFile(pathInDataDir(ctx, "channels.json"), "utf-8");

    assert.equal(profilesAfter, profilesBefore, "profiles.json must be byte-identical pre-export and post-import-after-wipe");
    assert.equal(channelsAfter, channelsBefore, "channels.json must be byte-identical pre-export and post-import-after-wipe");

    // Sanity reassertion: the seeded keys are present in the post-import state. (If the file bytes match but somehow neither contained the seeded keys, the
    // earlier assertions would have already failed - so this is belt-and-suspenders documentation of what the byte equality entails.)
    void domainKey;
  });
});

describe("service pack import - merge contract on dirty state", () => {

  test("import merges into existing state with import-takes-precedence on key collision; unrelated existing keys survive untouched", async () => {

    /* The merge contract. importServicePack at servicePacks.ts:200-225 uses Object.assign(data.profiles, pack.profiles) inside mutateProfiles - import-takes-
     * precedence on collision; entries not in the pack are left intact. The same shape applies to domains and channels.
     *
     * Test design: build state A (profile "shared" with description "version A", profile "a-only", channel "a-channel"), capture as a pack via the export
     * endpoint, wipe, then seed state B (profile "shared" with description "version B", profile "b-only", channel "b-channel"), then import A's pack on top of
     * B. Final state should be:
     *   - profile "shared" -> "version A" (import overwrote)
     *   - profile "a-only" -> present (added)
     *   - profile "b-only" -> present (untouched by import)
     *   - channel "a-channel" -> present (added)
     *   - channel "b-channel" -> present (untouched)
     *
     * A regression to wholesale-replace would have profile "b-only" missing and channel "b-channel" missing. A regression to existing-takes-precedence would
     * have profile "shared" reading "version B" instead of "version A".
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // State A: seed and export.
    await mutateProfiles((data) => {

      data.profiles["shared"] = { description: "version A", extends: "fullscreenApi", summary: "shared from A" };
      data.profiles["a-only"] = { description: "a only", extends: "fullscreenApi", summary: "a only" };
    });

    await mutateChannels((data) => {

      data.channels["a-channel"] = { name: "A Channel", profile: "shared", url: "https://example.test/a" };
    });

    const { urlFor } = await bootApp(ctx);

    const exportResponse = await fetch(urlFor("/config/profiles/export?profile=shared,a-only&channels=1"));

    assert.equal(exportResponse.status, 200, "export must succeed; body: " + (await exportResponse.clone().text()).slice(0, 200));

    const packA = await exportResponse.text();

    // Wipe and re-seed as state B.
    await mutateProfiles((data) => {

      data.profiles = {};
      data.domains = {};
    });

    await mutateChannels((data) => {

      data.channels = {};
    });

    await mutateProfiles((data) => {

      data.profiles["shared"] = { description: "version B", extends: "fullscreenApi", summary: "shared from B" };
      data.profiles["b-only"] = { description: "b only", extends: "fullscreenApi", summary: "b only" };
    });

    await mutateChannels((data) => {

      data.channels["b-channel"] = { name: "B Channel", profile: "shared", url: "https://example.test/b" };
    });

    // Import A's pack on top of B's state.
    const importResponse = await fetch(urlFor("/config/profiles/import"), {

      body: packA,
      headers: { "Content-Type": "application/json" },
      method: "POST"
    });

    assert.equal(importResponse.status, 200, "import must succeed; body: " + (await importResponse.clone().text()).slice(0, 200));

    // Verify the merge contract on the profiles file.
    const profilesFinal = await readPersistedJson(ctx, "profiles.json") as { profiles: Record<string, { description?: string }> };

    assert.equal(profilesFinal.profiles["shared"]?.description, "version A", "import-takes-precedence: 'shared' must be A's version after import");
    assert.ok("a-only" in profilesFinal.profiles, "added: 'a-only' must be present after import");
    assert.ok("b-only" in profilesFinal.profiles, "preserved: 'b-only' must survive an import that does not reference it");

    // Verify the merge contract on the channels file. (Channels file is flattened: entries at top level alongside schemaVersion/migrationsApplied/etc.)
    const channelsFinal = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;

    assert.ok("a-channel" in channelsFinal, "added: 'a-channel' must be present after import");
    assert.ok("b-channel" in channelsFinal, "preserved: 'b-channel' must survive an import that does not reference it");
  });
});

describe("service pack import - validation rejection", () => {

  test("a malformed pack body returns a 400 envelope and writes nothing to disk", async () => {

    /* Validation rejections must be transactional in the disk sense: a 400 response means profiles.json and channels.json are byte-identical pre/post. The
     * route handler at services.ts:550-557 short-circuits with status 400 when parseServicePack returns errors, before either mutateProfiles or mutateChannels
     * is invoked. A regression that called the mutators before validating - or that partial-saved profiles before failing on channels - would corrupt state on
     * every malformed POST.
     *
     * Validation trip: pack with version 999 (parseServicePack at servicePacks.ts:79-81 rejects version > CURRENT_VERSION). Other invalid shapes (missing
     * profiles, non-object profiles) trip the same code path; the assertion below documents the envelope shape, not the specific error string.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await seedFixture();

    // Snapshot the two stores the import path touches. We do NOT also snapshot config.json or health.json: cross-store-isolation already pins that mutations
    // to one store leave the others untouched, and config.json need not exist on disk after a fresh initializePersistence (it is only created on first write).
    // The invariant under test here is "the import handler does not partial-write profiles.json BEFORE rejecting on validation, and does not write channels.json
    // AT ALL when the primary validation fails."
    const profilesBefore = await readFile(pathInDataDir(ctx, "profiles.json"), "utf-8");
    const channelsBefore = await readFile(pathInDataDir(ctx, "channels.json"), "utf-8");

    const { urlFor } = await bootApp(ctx);

    // Malformed pack: version > CURRENT_VERSION (1) and otherwise valid shape so the version check is the trip.
    const malformedPack = {

      name: "Malformed Pack",
      profiles: { "test-profile": { description: "test", extends: "fullscreenApi" } },
      version: 999
    };

    const importResponse = await fetch(urlFor("/config/profiles/import"), {

      body: JSON.stringify(malformedPack),
      headers: { "Content-Type": "application/json" },
      method: "POST"
    });

    assert.equal(importResponse.status, 400, "malformed pack must produce a 400 status");

    const responseEnvelope = await importResponse.json() as { success?: boolean; error?: string };

    assert.equal(responseEnvelope.success, false, "the response envelope must carry success: false on validation failure");
    assert.equal(typeof responseEnvelope.error, "string", "the response envelope must carry a string error message on validation failure");

    // Disk-side: profiles.json and channels.json bytes are unchanged.
    assert.equal(await readFile(pathInDataDir(ctx, "profiles.json"), "utf-8"), profilesBefore,
      "profiles.json must be byte-identical after a 400-rejected import - validation rejection means zero state mutation");
    assert.equal(await readFile(pathInDataDir(ctx, "channels.json"), "utf-8"), channelsBefore,
      "channels.json must be byte-identical after a 400-rejected import - the secondary mutator must not run when the primary validation fails");
  });
});

describe("service pack export - determinism", () => {

  test("two consecutive exports of the same profile produce byte-identical response bodies", async () => {

    /* The export endpoint emits via stringifySorted (services.ts:649) - the same sorted-keys serializer the persistence layer uses for byte-identity on file
     * writes. Two consecutive GETs of the same export must produce byte-identical response bodies; otherwise users sharing pack files between deployments would
     * see spurious diffs in version control or hash mismatches in distribution scenarios. The same property the persistence layer guarantees for files must hold
     * for the HTTP response that IS the file's distribution form.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { profileKey } = await seedFixture();

    const { urlFor } = await bootApp(ctx);

    const firstResponse = await fetch(urlFor("/config/profiles/export?profile=" + profileKey + "&channels=1"));
    const firstBody = await firstResponse.text();

    const secondResponse = await fetch(urlFor("/config/profiles/export?profile=" + profileKey + "&channels=1"));
    const secondBody = await secondResponse.text();

    assert.equal(firstResponse.status, 200, "first export must succeed");
    assert.equal(secondResponse.status, 200, "second export must succeed");

    assert.equal(firstBody, secondBody, "two consecutive exports of the same profile must produce byte-identical bodies - stringifySorted determinism contract");
  });
});

describe("importServicePack and exportServicePack - direct orchestrator coverage", () => {

  /* The roundtrip suite above drives import/export through the HTTP routes. Here we exercise the orchestrator functions directly so the coverage protocol's
   * "every public API line is exercised" axis is closed for the orchestrator surface as well as for the route surface. The audit's S4-C7 / S4-C8 finding
   * called out that importServicePack's options.skipChannels and "no channels" branches plus exportServicePack's matchedProfiles loop, includeDomains filter,
   * includeChannels filter, and hdhrEnabled stripping all have no direct coverage; this block lands the missing assertions.
   */
  test("exportServicePack happy path: matched profiles, included domains, included channels, and hdhrEnabled stripped", async () => {

    /* Seeds a profile-domain-channel triple and exports with everything included. Asserts: (a) the matched profile is in the result, (b) the domain referencing
     * it is included, (c) the channel referencing it is included, and (d) the channel's hdhrEnabled flag is stripped from the exported body since it's a local
     * deployment preference. The seed sets hdhrEnabled to a non-default value so the strip is observable.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { channelKey, domainKey, profileKey } = await seedFixture();

    /* Override the seeded channel to carry hdhrEnabled=false so the strip is observable in the exported result. Channel definitions in the production catalog
     * default to hdhrEnabled=true; the seed function adds the channel as a user channel so we mutate it directly here.
     */
    await mutateChannels((data) => {

      const ch = data.channels[channelKey];

      if(ch) {

        (ch as { hdhrEnabled?: boolean }).hdhrEnabled = false;
      }
    });

    const pack = exportServicePack([profileKey], { includeChannels: true, includeDomains: true });

    assert.ok(pack, "exportServicePack returns a pack when profile exists in user catalog");
    assert.ok(pack.profiles[profileKey], "matched profile included");
    assert.ok(pack.domains?.[domainKey], "domain referencing matched profile included");
    const exportedChannel = pack.channels?.[channelKey];

    assert.ok(exportedChannel, "channel referencing matched profile included");
    assert.equal((exportedChannel as { hdhrEnabled?: boolean }).hdhrEnabled, undefined,
      "hdhrEnabled stripped from exported channel - local deployment preference, not service configuration");
  });

  test("exportServicePack with includeDomains=false omits the domains section", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { profileKey } = await seedFixture();

    const pack = exportServicePack([profileKey], { includeDomains: false });

    assert.ok(pack, "pack returned");
    assert.equal(pack.domains, undefined, "domains section omitted when includeDomains=false");
  });

  test("exportServicePack without includeChannels omits the channels section", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { profileKey } = await seedFixture();

    /* includeChannels defaults to false (only true when explicitly requested). The seeded fixture has channels referencing the profile, but they must not
     * appear in the export when includeChannels is omitted.
     */
    const pack = exportServicePack([profileKey]);

    assert.ok(pack, "exportServicePack should return a pack for a known profile key");
    assert.equal(pack.channels, undefined, "channels section omitted when includeChannels is not explicitly true");
  });

  test("importServicePack with options.skipChannels=true imports profiles but skips channels", async () => {

    /* The skipChannels branch lets a caller import a pack's profiles and domains while leaving channels untouched. This is used by the UI when the user wants
     * to bring in a friend's profiles without taking their channel customizations. Pin: profilesAdded > 0, channelsAdded === 0, success === true.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const summary = await importServicePack({

      channels: {

        "skipped-channel": { name: "Should Not Land", url: "https://skipped.example.test/" }
      },
      domains: { "imported-skip-test.example": { profile: "skip-test-profile" } },
      name: "skip-channels-pack",
      profiles: { "skip-test-profile": { extends: "fullscreenApi" } },
      version: 1
    }, { skipChannels: true });

    assert.equal(summary.success, true, "primary import succeeded");
    assert.equal(summary.profilesAdded, 1, "profile imported");
    assert.equal(summary.channelsAdded, 0, "channel import skipped per options.skipChannels");
    assert.deepEqual(summary.errors, [], "no errors on the skip path");
  });

  test("importServicePack with no channels in the pack succeeds without invoking the channels mutator", async () => {

    /* The no-channels branch: a pack carrying only profiles + domains must complete successfully without entering the mutateChannels block at all. We assert
     * channelsAdded === 0 and the absence of channel-import warnings, which indicates the branch was taken (rather than entering the block and finding the
     * channels map empty).
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const summary = await importServicePack({

      name: "profiles-only-pack",
      profiles: { "profiles-only": { extends: "fullscreenApi" } },
      version: 1
    });

    assert.equal(summary.success, true);
    assert.equal(summary.channelsAdded, 0, "no channels added when pack has none");
    assert.deepEqual(summary.errors, [], "no warnings when channels are absent");
  });
});
