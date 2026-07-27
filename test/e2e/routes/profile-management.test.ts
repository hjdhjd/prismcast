/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * profile-management.test.ts: Integration coverage for the profile/domain HTTP route handlers in src/routes/config/services.ts. Phase 1's profiles.test.ts
 * drives mutateProfiles directly - this suite exercises the wire-level surface that the UI actually hits, end-to-end through Express. The route handlers are
 * the concentrated entry point for every user-facing profile and domain change; a 4afa8a0-equivalent regression in any of them (POST that wholesale-replaces
 * profile state instead of merging, DELETE that orphans domain mappings) would slip past Phase 1's per-mutator coverage entirely.
 *
 * What's pinned:
 *
 *   1. POST /config/profiles creates a profile and its domain mappings together as one transaction - both land on disk in the right shape, and the domain's
 *      `profile` reference points at the just-created key (no orphan domain entries).
 *   2. POST /config/profiles is a per-key partial update - posting an update to one profile leaves the other profiles' on-disk bytes byte-identical. This is
 *      the cross-profile analog of cross-store-isolation (which pinned the cross-FILE rule; this suite pins the cross-PROFILE rule inside one file).
 *   3. DELETE /config/profiles/:key cascades to every domain mapping that referenced that profile - no orphan domain entries remain, and other profiles'
 *      domain mappings are untouched.
 *   4. POST /config/profiles with an invalid profile body produces a 400 envelope and zero on-disk state mutation - profiles.json is byte-identical pre/post.
 *   5. Concurrent POSTs to two distinct profile keys both succeed and both land on disk - the per-store mutator queue serializes the writes correctly without
 *      either losing the other's update.
 *   6. GET /config/profiles projects each profile into one summary entry: channelCount, the reverse-looked-up domains that reference it, the extends and
 *      channel-selection-strategy fields (falling back to the "default"/"inherited" sentinels when the profile declares neither), and the whole list sorted
 *      by key.
 *
 * Why bootApp instead of calling mutateProfiles directly: every route handler ships its own validation, sanitization, and merge logic ahead of the mutator
 * call, and those layers ARE under test. Calling the mutator directly would exercise the persistence layer but skip the HTTP-side logic that the UI depends
 * on. Cross-profile isolation, validation rejection, and the delete cascade are all behaviors that live in the route handler, not the persistence layer.
 *
 * Why we use a non-builtin domain ("myservice.example.test"): src/config/userProfiles.ts validateDomain rejects domains that collide with the builtin
 * DOMAIN_CONFIG map. Picking a fictitious .example.test hostname keeps the test self-contained and avoids coupling test fidelity to the builtin catalog.
 */
import { bootApp, createIntegrationContext, initializePersistence, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { firstOf, nthOf } from "../../../src/testing.helpers.ts";
import assert from "node:assert/strict";
import { mutateChannels } from "../../../src/config/userChannels.ts";
import { mutateProfiles } from "../../../src/config/userProfiles.ts";
import { readFile } from "node:fs/promises";

/**
 * Builds the JSON body for a POST /config/profiles request. The route handler expects `{ key, profile, domains? }`; this helper centralizes the shape so each
 * test focuses on the values being exercised rather than re-asserting the request shape on every call. fullscreenApi is the canonical builtin base profile
 * for "extends" since it's the simplest user-extensible base in src/config/sites.ts.
 * @param key - User profile key.
 * @param description - Profile description.
 * @param domains - Optional domain mappings keyed by hostname.
 * @returns The JSON-stringifiable request body.
 */
function makePostBody(key: string, description: string, domains?: Record<string, { profile?: string; service?: string; serviceTag?: string }>): unknown {

  return {

    domains,
    key,
    profile: { description, extends: "fullscreenApi", summary: description }
  };
}

describe("POST /config/profiles - create and update", () => {

  test("a POST that includes both a profile and a domain mapping creates them together in one transaction", async () => {

    /* The wizard's "Save" button ships a single POST carrying the new profile AND every domain mapping the user assigned to it. The route handler must save
     * both atomically so the UI never sees an intermediate state where the profile exists but its domain mappings don't (or vice versa). Without this, a UI
     * refresh between sub-saves would surface a half-built profile to the operator. The integration assertion is that profiles.json - on disk after the POST -
     * carries both the new profile entry under the right key and the new domain entry pointing at that key.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const body = makePostBody("integration-test", "integration test profile", {

      "myservice.example.test": { profile: "integration-test", service: "MyService", serviceTag: "myservice" }
    });

    const response = await fetch(urlFor("/config/profiles"), {

      body: JSON.stringify(body),
      headers: { "Content-Type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "POST /config/profiles must succeed; body: " + (await response.clone().text()).slice(0, 200));

    // Disk-side assertion: profiles.json carries both the new profile and the new domain mapping. We narrow the unknown into the documented on-disk shape
    // (profiles.ts persistence layer emits { profiles: ..., domains: ..., schemaVersion, migrationsApplied? } via prepareProfilesForWrite).
    const persisted = await readPersistedJson(ctx, "profiles.json") as { profiles?: Record<string, unknown>; domains?: Record<string, unknown> };

    assert.equal(typeof persisted.profiles, "object", "profiles.json must persist a profiles map");
    assert.ok(persisted.profiles && ("integration-test" in persisted.profiles), "the new profile must be present under its key");

    assert.equal(typeof persisted.domains, "object", "profiles.json must persist a domains map");
    assert.ok(persisted.domains && ("myservice.example.test" in persisted.domains), "the new domain mapping must be present under its hostname");

    const domainEntry = (persisted.domains as Record<string, { profile?: string }>)["myservice.example.test"];

    assert.equal(domainEntry?.profile, "integration-test", "the domain mapping's profile reference must point at the just-created profile key");
  });

  test("a POST whose domain mapping references a provider profile is accepted", async () => {

    /* Mapping a domain onto a provider profile is coherent - the builtin DOMAIN_CONFIG entries do exactly that. disneyNow is absent from the UI profile
     * catalog, which lists only the general table plus the user's own profiles, so this mapping validates only because the reference is tested against the
     * single builtin lookup rather than that catalog.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const body = makePostBody("provider-ref-test", "provider reference profile", {

      "providerref.example.test": { profile: "disneyNow", service: "MyService", serviceTag: "myservice" }
    });

    const response = await fetch(urlFor("/config/profiles"), {

      body: JSON.stringify(body),
      headers: { "Content-Type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "a domain mapping naming a provider profile must save; body: " + (await response.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "profiles.json") as { domains?: Record<string, { profile?: string }> };

    assert.equal(persisted.domains?.["providerref.example.test"]?.profile, "disneyNow", "the domain mapping persists its provider-profile reference");
  });

  test("a POST update to one profile leaves all other profile entries byte-identical on disk", async () => {

    /* The cross-profile isolation rule. The wizard's edit flow loads one profile, lets the user mutate it, and POSTs the result. The route handler
     * cleans up stale domain mappings for the targeted profile and merges the new profile entry into the existing profiles map. Both happen in-place inside the
     * mutateProfiles callback: the stale-mapping loop deletes domains whose `profile` field matches the key, then the per-key write `data.profiles[key] = profile`
     * adds or replaces just that one entry without disturbing any other. A regression that wholesale-replaced profile state - that re-emitted the profiles map
     * without copying every untouched entry - is precisely the 4afa8a0 class for the profiles surface.
     *
     * We seed three profiles (a, b, c), capture the full profiles.json bytes, POST an edit to profile-b, and assert that profile-a's and profile-c's per-key
     * JSON projections (via stringifySorted-equivalent JSON.stringify with sorted keys) are byte-identical pre/post. We compare per-entry rather than full-file
     * because the targeted entry's bytes change and the metadata (schemaVersion ordering) may shift if the file's overall key set changes.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Seed three distinct profiles via the production mutator (skipping the route layer; the test is about isolation under route-driven update, not initial
    // creation - the seed shape just needs to be on disk).
    await mutateProfiles((data) => {

      data.profiles["profile-a"] = { description: "A", extends: "fullscreenApi", summary: "A summary" };
      data.profiles["profile-b"] = { description: "B", extends: "fullscreenApi", summary: "B summary" };
      data.profiles["profile-c"] = { description: "C", extends: "fullscreenApi", summary: "C summary" };
    });

    // Capture pre-update bytes for each non-targeted entry. JSON.stringify with a sorted-key replacer matches what the persistence layer produces, so this
    // projection is the same one the file store uses end-to-end.
    const persistedBefore = await readPersistedJson(ctx, "profiles.json") as { profiles: Record<string, unknown> };
    const profileABefore = JSON.stringify(persistedBefore.profiles["profile-a"]);
    const profileCBefore = JSON.stringify(persistedBefore.profiles["profile-c"]);

    const { urlFor } = await bootApp(ctx);

    // Post an update to profile-b only.
    const response = await fetch(urlFor("/config/profiles"), {

      body: JSON.stringify(makePostBody("profile-b", "B updated", undefined)),
      headers: { "Content-Type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "POST update to profile-b must succeed; body: " + (await response.clone().text()).slice(0, 200));

    // Per-entry assertion: profile-a and profile-c are byte-identical pre/post. profile-b's bytes have changed (the description was rewritten); the assertion
    // is exclusively on the untouched entries.
    const persistedAfter = await readPersistedJson(ctx, "profiles.json") as { profiles: Record<string, unknown> };

    assert.equal(JSON.stringify(persistedAfter.profiles["profile-a"]), profileABefore,
      "profile-a's on-disk projection must be byte-identical after a POST update targeting only profile-b");
    assert.equal(JSON.stringify(persistedAfter.profiles["profile-c"]), profileCBefore,
      "profile-c's on-disk projection must be byte-identical after a POST update targeting only profile-b");

    // Sanity: profile-b's description did get updated (proves the POST took effect at all - so the byte-identity check above is meaningful).
    const profileB = persistedAfter.profiles["profile-b"] as { description?: string };

    assert.equal(profileB.description, "B updated", "profile-b's description must reflect the POST update");
  });
});

describe("DELETE /config/profiles/:key - cascade", () => {

  test("DELETE removes every domain mapping that referenced the deleted profile while leaving unrelated mappings untouched", async () => {

    /* The cascade contract. deleteUserProfile in config/userProfiles.ts removes the profile entry and walks the domains map removing every entry whose `profile`
     * field references the deleted key. A regression that skipped the cascade would orphan domain entries pointing at a non-existent profile - those
     * entries would surface in the wizard's domain list with a broken reference and silently fail to resolve at request time.
     *
     * Setup: seed two profiles (target, bystander), each with two domain mappings (4 mappings total). DELETE the target profile. Assert: target profile gone,
     * its 2 domains gone, bystander profile and its 2 domains byte-identical pre/post.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateProfiles((data) => {

      data.profiles["target"] = { description: "target", extends: "fullscreenApi", summary: "target" };
      data.profiles["bystander"] = { description: "bystander", extends: "fullscreenApi", summary: "bystander" };

      data.domains["target-1.example.test"] = { profile: "target" };
      data.domains["target-2.example.test"] = { profile: "target" };
      data.domains["bystander-1.example.test"] = { profile: "bystander" };
      data.domains["bystander-2.example.test"] = { profile: "bystander" };
    });

    const persistedBefore = await readPersistedJson(ctx, "profiles.json") as { domains: Record<string, unknown> };
    const bystander1Before = JSON.stringify(persistedBefore.domains["bystander-1.example.test"]);
    const bystander2Before = JSON.stringify(persistedBefore.domains["bystander-2.example.test"]);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/profiles/target"), { method: "DELETE" });

    assert.equal(response.status, 200, "DELETE must succeed; body: " + (await response.clone().text()).slice(0, 200));

    const persistedAfter = await readPersistedJson(ctx, "profiles.json") as { profiles?: Record<string, unknown>; domains?: Record<string, unknown> };

    // Profile gone.
    assert.equal((persistedAfter.profiles && ("target" in persistedAfter.profiles)) ?? false, false, "the target profile must be removed from disk");
    assert.equal((persistedAfter.profiles && ("bystander" in persistedAfter.profiles)) ?? false, true, "the bystander profile must remain on disk");

    // Cascade: both target-* domain mappings gone.
    const domainsAfter = persistedAfter.domains ?? {};

    assert.equal("target-1.example.test" in domainsAfter, false, "target-1 domain mapping must be cascaded away");
    assert.equal("target-2.example.test" in domainsAfter, false, "target-2 domain mapping must be cascaded away");

    // Bystander mappings byte-identical pre/post.
    assert.equal(JSON.stringify(domainsAfter["bystander-1.example.test"]), bystander1Before,
      "bystander-1 domain mapping must be byte-identical pre/post a DELETE targeting an unrelated profile");
    assert.equal(JSON.stringify(domainsAfter["bystander-2.example.test"]), bystander2Before,
      "bystander-2 domain mapping must be byte-identical pre/post a DELETE targeting an unrelated profile");
  });
});

describe("POST /config/profiles - validation rejection", () => {

  test("a POST with an invalid profile body returns a 400 envelope and writes nothing to disk", async () => {

    /* Validation rejections must be transactional in the disk sense: a 400 response must mean profiles.json is byte-identical pre/post. The route handler at
     * routes/config/services.ts calls validateProfile and short-circuits with status 400 when errors come back. A regression that bypassed the early return (or
     * partial-saved before validating) would corrupt state on every malformed POST.
     *
     * Validation trip: extends="" - validateProfile in config/userProfiles.ts rejects "extends is required". The same code path also rejects unknown extends
     * targets, non-generic strategies, and unrecognized flags; the assertion below documents the envelope shape, not the specific error string.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Seed one profile so profiles.json has on-disk content; the byte-identity check is meaningful only against non-empty state.
    await mutateProfiles((data) => {

      data.profiles["seed"] = { description: "seed", extends: "fullscreenApi", summary: "seed" };
    });

    const beforeBytes = await readFile(ctx.dataDir + "/profiles.json", "utf-8");

    const { urlFor } = await bootApp(ctx);

    // Invalid profile: extends is missing/empty. The route handler must surface a 400 with an error message and write nothing.
    const response = await fetch(urlFor("/config/profiles"), {

      body: JSON.stringify({ key: "bad-profile", profile: { description: "no extends" } }),
      headers: { "Content-Type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 400, "an invalid profile body must produce a 400 status");

    const responseBody = await response.json() as { success?: boolean; error?: string };

    assert.equal(responseBody.success, false, "the response envelope must carry success: false on validation failure");
    assert.equal(typeof responseBody.error, "string", "the response envelope must carry a string error message on validation failure");

    // Disk-side: profiles.json bytes are unchanged.
    const afterBytes = await readFile(ctx.dataDir + "/profiles.json", "utf-8");

    assert.equal(afterBytes, beforeBytes, "profiles.json must be byte-identical after a 400-rejected POST - validation rejection means zero state mutation");
  });
});

describe("POST /config/profiles - per-store mutator queue under contention", () => {

  test("concurrent POSTs to different keys both land on disk - the route handler does its read-modify-write inside the mutator callback", async () => {

    /* The serialized RMW contract. The route handler's mutateProfiles callback (services.ts:533-548) does the merge so each write applies against the latest
     * serialized state under the per-store queue's lock. Two concurrent POSTs to different keys serialize correctly: the first mutator writes profile A,
     * the second's callback then sees profiles = { A: ... } as its starting state and adds B alongside it. Both keys land on disk; neither overwrites the other.
     *
     * This pins the rule that any read-modify-write against profiles.json must happen inside the mutator's callback. A regression that lifts the
     * read out of the callback - even partially, e.g., by capturing a snapshot of `data.profiles` before mutating - reintroduces the lost-update bug because the
     * snapshot freezes a baseline that may already be stale by the time the mutate function returns. Pinned here so any such regression fails loud immediately.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const [ responseA, responseB ] = await Promise.all([

      fetch(urlFor("/config/profiles"), {

        body: JSON.stringify(makePostBody("concurrent-a", "concurrent A", undefined)),
        headers: { "Content-Type": "application/json" },
        method: "POST"
      }),
      fetch(urlFor("/config/profiles"), {

        body: JSON.stringify(makePostBody("concurrent-b", "concurrent B", undefined)),
        headers: { "Content-Type": "application/json" },
        method: "POST"
      })
    ]);

    assert.equal(responseA.status, 200, "concurrent POST A must succeed; body: " + (await responseA.clone().text()).slice(0, 200));
    assert.equal(responseB.status, 200, "concurrent POST B must succeed; body: " + (await responseB.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "profiles.json") as { profiles?: Record<string, unknown> };
    const profilesMap = persisted.profiles ?? {};

    assert.ok("concurrent-a" in profilesMap, "concurrent-a must be present on disk after the parallel POSTs settle");
    assert.ok("concurrent-b" in profilesMap, "concurrent-b must be present on disk after the parallel POSTs settle");
  });
});

describe("GET /config/profiles - list projection", () => {

  test("the list maps each profile to its channel count, reverse-looked-up domains, extends/strategy fallbacks, sorted by key", async () => {

    /* The read-side projection contract. The GET handler at services.ts:373-413 is what the Custom Profiles subtab fetches to render (and re-render after a
     * mutation). For every user profile it emits, in one entry: channelCount (scanned from the channel listing), the reverse-looked-up domain mappings that
     * reference the profile, the extends base (falling back to "default" when the profile declares no base), and the channel-selection strategy (falling back to
     * "inherited" when the profile declares no channelSelection block). The whole list is sorted by key via localeCompare. A regression in any one of those five
     * projections - a channel-count that stops scanning the listing, a domain reverse-lookup that attributes a mapping to the wrong profile, a fallback that
     * emits undefined instead of the "default"/"inherited" sentinel, or a list that loses its sort - would surface to the operator as a mis-rendered profile
     * table even though the underlying stores are correct. sendSuccess flattens the payload's data to the envelope top level, so the wire body is
     * { domains, profiles: [...], success: true } rather than a nested data object.
     *
     * We seed two profiles that exercise both sides of the fallbacks: "zebra" declares an explicit base ("fullscreenApi") and an explicit strategy ("tileClick"),
     * while "alpha" declares neither, so it must project extends "default" and strategy "inherited". The keys are chosen so insertion order (alpha seeded after
     * zebra below) is the reverse of sorted order, which makes the sort assertion meaningful. Channels and domains are seeded to distinct profiles so the
     * per-profile counts and reverse-lookups are unambiguous, plus a profile-less channel that must be counted against nobody.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Seed the two profiles. "zebra" is inserted first so that a handler which forgot to sort would emit zebra before alpha; the sort assertion below catches
    // exactly that. "alpha" deliberately omits both extends and channelSelection so its projection must fall back to the "default"/"inherited" sentinels.
    await mutateProfiles((data) => {

      data.profiles["zebra"] = { channelSelection: { strategy: "tileClick" }, description: "Z", extends: "fullscreenApi", summary: "Z summary" };
      data.profiles["alpha"] = { description: "A", summary: "A summary" };

      // Reverse-lookup fixtures: one domain per profile. alpha's mapping carries service/serviceTag; zebra's omits them so the handler's `?? ""` fallback is
      // exercised. A bystander domain references a non-existent profile and must appear in neither profile's domains array.
      data.domains["a1.example.test"] = { profile: "alpha", service: "AlphaTV", serviceTag: "alpha" };
      data.domains["z1.example.test"] = { profile: "zebra" };
      data.domains["orphan.example.test"] = { profile: "does-not-exist" };
    });

    // Seed channels: two bound to alpha, one to zebra, one with no profile at all. countChannelsByProfile scans getChannelListing() and buckets on
    // channel.profile, so these drive channelCount to 2 (alpha) and 1 (zebra); the profile-less channel must not be counted against anyone.
    await mutateChannels((data) => {

      data.channels["alpha-one"] = { name: "Alpha One", profile: "alpha", url: "https://a1.example.test/one" };
      data.channels["alpha-two"] = { name: "Alpha Two", profile: "alpha", url: "https://a1.example.test/two" };
      data.channels["zebra-one"] = { name: "Zebra One", profile: "zebra", url: "https://z1.example.test/one" };
      data.channels["no-profile"] = { name: "No Profile", url: "https://n.example.test/x" };
    });

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/profiles"));

    assert.equal(response.status, 200, "GET /config/profiles must succeed; body: " + (await response.clone().text()).slice(0, 200));

    // The envelope: sendSuccess flattens data to the top level, so success/profiles/domains all sit at the root of the parsed body.
    const body = await response.json() as {
      profiles?: { channelCount?: number; domains?: { config?: Record<string, unknown>; domain?: string; service?: string; serviceTag?: string }[];
        extends?: string; key?: string;
        strategy?: string; }[];
      success?: boolean;
    };

    assert.equal(body.success, true, "the envelope must carry success: true");
    assert.ok(Array.isArray(body.profiles), "the response must carry a profiles array");

    const profiles = body.profiles ?? [];

    // Sort: the two seeded keys must come back in localeCompare order (alpha before zebra) regardless of insertion order.
    assert.deepEqual(profiles.map((p) => p.key), [ "alpha", "zebra" ], "the profile list must be sorted by key ascending");

    const alpha = firstOf(profiles);
    const zebra = nthOf(profiles, 1);

    // Channel counts: alpha has two bound channels, zebra one; the profile-less channel is counted against neither.
    assert.equal(alpha.channelCount, 2, "alpha's channelCount must reflect its two bound channels");
    assert.equal(zebra.channelCount, 1, "zebra's channelCount must reflect its single bound channel");

    // Fallbacks: alpha declared neither base nor strategy, so it must project the "default"/"inherited" sentinels; zebra declared both, so it must project them
    // verbatim.
    assert.equal(alpha.extends, "default", "a profile with no extends must project extends: 'default'");
    assert.equal(alpha.strategy, "inherited", "a profile with no channelSelection must project strategy: 'inherited'");
    assert.equal(zebra.extends, "fullscreenApi", "a profile with an explicit base must project that base verbatim");
    assert.equal(zebra.strategy, "tileClick", "a profile with an explicit strategy must project that strategy verbatim");

    // Reverse-looked-up domains: exactly the mapping that references each profile, with the service/serviceTag `?? ""` fallback applied and the full raw
    // DomainConfig carried as config (the wizard round-trips unrendered domain-level fields from it). The bystander mapping for a non-existent profile must
    // appear in neither array.
    assert.deepEqual(alpha.domains,
      [{ config: { profile: "alpha", service: "AlphaTV", serviceTag: "alpha" }, domain: "a1.example.test", service: "AlphaTV", serviceTag: "alpha" }],
      "alpha's domains must be exactly its one mapping with service/serviceTag preserved");
    assert.deepEqual(zebra.domains, [{ config: { profile: "zebra" }, domain: "z1.example.test", service: "", serviceTag: "" }],
      "zebra's domains must carry the '' fallback for the absent service/serviceTag fields");
  });
});
