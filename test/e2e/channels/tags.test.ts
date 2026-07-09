/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * tags.test.ts: HTTP-level integration coverage for the tag vocabulary endpoints. Tags drive playlist filtering and channel-table grouping; the vocabulary
 * lives in tagRegistry inside channels.json. Tag identity is case-insensitive ("news" and "News" are the same tag) while the displayed casing is whatever the
 * user typed first; that casing-identity invariant lives in the handler and is not exercised here. This suite exercises the full CRUD: create, list, delete (with
 * cascade), rename (cascading across channels).
 */
import { bootApp, createIntegrationContext, initializePersistence, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { mutateChannels } from "../../../src/config/userChannels.ts";
import { stringifySorted } from "../../../src/utils/format.ts";

describe("GET /config/tags - list tag vocabulary", () => {

  test("returns the active vocabulary, predefined list, and registry", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/tags"));

    assert.equal(response.status, 200);

    const body = await response.json() as { active: string[]; predefined: string[]; registry: { deletedTags: string[]; tags: string[] }; success: boolean };

    assert.equal(body.success, true);
    assert.ok(Array.isArray(body.active), "active is an array");
    assert.ok(Array.isArray(body.predefined), "predefined is an array");
    assert.ok(body.registry, "registry is present");
  });
});

describe("POST /config/tags - create tag", () => {

  test("creates a new user tag and persists it to the registry", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/tags"), {

      body: JSON.stringify({ tag: "test-new-tag" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "tag creation should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "channels.json") as { tagRegistry?: { tags: string[] } };

    assert.ok(persisted.tagRegistry, "tagRegistry should be persisted");
    assert.ok(persisted.tagRegistry.tags.includes("test-new-tag"), "tag should be in the registry");
  });

  test("rejects creating a duplicate tag with 409", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // First creation succeeds.
    await fetch(urlFor("/config/tags"), {

      body: JSON.stringify({ tag: "duplicate-test" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    // Second creation rejects.
    const response = await fetch(urlFor("/config/tags"), {

      body: JSON.stringify({ tag: "duplicate-test" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 409, "duplicate tag should reject with 409");
  });

  test("rejects an empty tag with 400", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/tags"), {

      body: JSON.stringify({ tag: "" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 400, "empty tag should reject");
  });
});

describe("DELETE /config/tags/:tag - delete tag", () => {

  test("deletes a user tag and removes it from the registry", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // Create.
    await fetch(urlFor("/config/tags"), {

      body: JSON.stringify({ tag: "to-delete" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    // Delete.
    const response = await fetch(urlFor("/config/tags/to-delete"), { method: "DELETE" });

    assert.equal(response.status, 200);

    const persisted = await readPersistedJson(ctx, "channels.json") as { tagRegistry?: { tags?: string[] } };
    const tags = persisted.tagRegistry?.tags ?? [];

    assert.equal(tags.includes("to-delete"), false, "tag should be removed from registry after delete");
  });

  test("returns 404 when deleting an unknown tag", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/tags/never-existed-tag-x9z2"), { method: "DELETE" });

    assert.equal(response.status, 404, "deleting an unknown tag should 404");
  });
});

describe("POST /config/tags/rename - rename tag", () => {

  test("renames a tag in the registry", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // Create.
    await fetch(urlFor("/config/tags"), {

      body: JSON.stringify({ tag: "old-name" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    // Rename.
    const response = await fetch(urlFor("/config/tags/rename"), {

      body: JSON.stringify({ newTag: "new-name", oldTag: "old-name" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "rename should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "channels.json") as { tagRegistry?: { tags?: string[] } };
    const tags = persisted.tagRegistry?.tags ?? [];

    assert.ok(tags.includes("new-name"), "new tag name should be in the registry");
    assert.equal(tags.includes("old-name"), false, "old tag name should be gone");
  });
});

describe("POST /config/tags/rename - cascade across vocabulary and channel bindings", () => {

  /* The rename cascade invariant: when a tag is renamed in the vocabulary, every channel that referenced the old name must be updated to the new name in lockstep,
   * AND every other tag on those channels must survive untouched. The cascade implementation is shared with DELETE - both call transformChannelTags - so the
   * shape of the cascade contract here mirrors the "DELETE /config/tags/:tag - cascade across vocabulary and channel bindings" describe block below exactly. The
   * conflict path (renaming to an existing name) is the additional pin specific to rename:
   * a 409 must short-circuit before either the registry write or the cascade, so vocabulary and channels stay byte-identical when the rename is rejected.
   *
   * The validation-failure path also short-circuits before the registry write, so a malformed newTag (empty, too long, invalid character) leaves the world
   * unchanged just like the conflict path. This mirrors DELETE's 404 short-circuit pinned in the DELETE-cascade describe block below (the unknown-tag DELETE test).
   */

  test("POST rename on a referenced user tag updates vocabulary AND every channel binding", async () => {

    /* Create a user tag, attach it to two channels alongside an unrelated predefined tag, rename the user tag, and verify: the new tag name is in the
     * vocabulary, the old name is gone, both channels reflect the new name, and the unrelated tag is preserved.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const createResponse = await fetch(urlFor("/config/tags"), {

      body: JSON.stringify({ tag: "old-tag" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(createResponse.status, 200, "creating the rename source tag should succeed");

    await mutateChannels((data) => {

      data.channels["seed-a"] = { name: "Seed A", tags: [ "News", "old-tag" ], url: "https://example.test/a" };
      data.channels["seed-b"] = { name: "Seed B", tags: [ "News", "old-tag" ], url: "https://example.test/b" };
    });

    const renameResponse = await fetch(urlFor("/config/tags/rename"), {

      body: JSON.stringify({ newTag: "renamed-tag", oldTag: "old-tag" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(renameResponse.status, 200, "rename should succeed; body: " + (await renameResponse.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown> & { tagRegistry?: { tags?: string[] } };
    const registryTags = persisted.tagRegistry?.tags ?? [];

    assert.ok(registryTags.includes("renamed-tag"), "registry must carry the new tag name");
    assert.equal(registryTags.includes("old-tag"), false, "registry must no longer carry the old tag name");

    for(const key of [ "seed-a", "seed-b" ] as const) {

      const entry = persisted[key] as { tags?: string[] };

      assert.ok(Array.isArray(entry.tags), key + " should still carry a tags array after cascade rename");
      assert.ok(entry.tags.includes("renamed-tag"), key + " must carry the renamed tag");
      assert.equal(entry.tags.includes("old-tag"), false, key + " must no longer carry the old tag name");
      assert.ok(entry.tags.includes("News"), key + " must still carry the unrelated News tag");
    }
  });

  test("POST rename to an existing tag name rejects with 409 and leaves vocabulary + channels byte-identical", async () => {

    /* Two user tags, A and B, both attached to a channel. Attempt to rename A -> B; the vocabulary already contains B, so the handler must reject with 409 before
     * touching the registry or the cascade. We snapshot per-key bytes pre-rename and assert byte-equality post-rename to prove the short-circuit. We assert only
     * the 409 status, not the envelope body; the real invariant being pinned is the unchanged on-disk state.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    for(const tag of [ "alpha-tag", "beta-tag" ]) {

      // Two sequential POST creations. The tag-create handler reads a defensive copy of the registry via getTagRegistry(), pushes the new tag to that copy, and
      // writes the whole copy back via setTagRegistry(); two concurrent creates both start from the same persisted snapshot, so the second write clobbers the
      // first and one append is lost. We serialize at the test level to avoid that lost-append race.
      // eslint-disable-next-line no-await-in-loop -- read-modify-write race on the registry forces sequential creates here.
      await fetch(urlFor("/config/tags"), {

        body: JSON.stringify({ tag }),
        headers: { "content-type": "application/json" },
        method: "POST"
      });
    }

    await mutateChannels((data) => {

      data.channels["seed-a"] = { name: "Seed A", tags: [ "alpha-tag", "beta-tag" ], url: "https://example.test/a" };
    });

    const beforeFile = await readPersistedJson(ctx, "channels.json") as Record<string, unknown> & { tagRegistry?: unknown };
    const beforeChannelBytes = stringifySorted(beforeFile["seed-a"]);
    const beforeRegistryBytes = stringifySorted(beforeFile.tagRegistry ?? null);

    const response = await fetch(urlFor("/config/tags/rename"), {

      body: JSON.stringify({ newTag: "beta-tag", oldTag: "alpha-tag" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 409, "rename to an existing tag must reject with 409");

    const afterFile = await readPersistedJson(ctx, "channels.json") as Record<string, unknown> & { tagRegistry?: unknown };

    assert.equal(stringifySorted(afterFile["seed-a"]), beforeChannelBytes, "channel binding must be byte-identical when the rename is rejected");
    assert.equal(stringifySorted(afterFile.tagRegistry ?? null), beforeRegistryBytes, "tag registry must be byte-identical when the rename is rejected");
  });

  test("POST rename on an unreferenced tag updates vocabulary; channel state byte-identical", async () => {

    /* A user tag with no channel bindings. Seed unrelated channels, rename the unreferenced tag, snapshot. The transformChannelTags filter (only entries that
     * match) yields zero affected keys, so no channel entry changes and each seed channel's serialized bytes are byte-identical before and after. The registry,
     * of course, must reflect the rename.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await fetch(urlFor("/config/tags"), {

      body: JSON.stringify({ tag: "lonely-tag" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    await mutateChannels((data) => {

      data.channels["seed-a"] = { name: "Seed A", tags: ["News"], url: "https://example.test/a" };
      data.channels["seed-b"] = { name: "Seed B", tags: ["News"], url: "https://example.test/b" };
    });

    const before = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;
    const beforeBytes = { "seed-a": stringifySorted(before["seed-a"]), "seed-b": stringifySorted(before["seed-b"]) };

    const response = await fetch(urlFor("/config/tags/rename"), {

      body: JSON.stringify({ newTag: "less-lonely-tag", oldTag: "lonely-tag" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "rename of an unreferenced tag should succeed");

    const after = await readPersistedJson(ctx, "channels.json") as Record<string, unknown> & { tagRegistry?: { tags?: string[] } };
    const tags = after.tagRegistry?.tags ?? [];

    assert.ok(tags.includes("less-lonely-tag"), "registry must carry the new tag name");
    assert.equal(tags.includes("lonely-tag"), false, "registry must no longer carry the old tag name");
    assert.equal(stringifySorted(after["seed-a"]), beforeBytes["seed-a"], "seed-a must be byte-identical when no cascade was needed");
    assert.equal(stringifySorted(after["seed-b"]), beforeBytes["seed-b"], "seed-b must be byte-identical when no cascade was needed");
  });

  test("POST rename to an invalid name rejects without modifying vocabulary or channel state", async () => {

    /* Validation failure path: rename to a name that fails the tag-name pattern (an empty string after trim). The validateTagName call must short-circuit before
     * the registry write or the cascade. We seed a channel with the tag, snapshot vocabulary + channel bytes, attempt the malformed rename, and assert: 400
     * status, registry unchanged, channel entry byte-identical.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await fetch(urlFor("/config/tags"), {

      body: JSON.stringify({ tag: "valid-source" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    await mutateChannels((data) => {

      data.channels["seed-a"] = { name: "Seed A", tags: ["valid-source"], url: "https://example.test/a" };
    });

    const beforeFile = await readPersistedJson(ctx, "channels.json") as Record<string, unknown> & { tagRegistry?: unknown };
    const beforeChannelBytes = stringifySorted(beforeFile["seed-a"]);
    const beforeRegistryBytes = stringifySorted(beforeFile.tagRegistry ?? null);

    /* Use a name that's both non-empty (so the both-required check passes) AND fails the pattern check. A leading hyphen fails TAG_NAME_PATTERN's anchored
     * alphanumeric-start requirement, exercising the validateTagName branch rather than the both-required branch.
     */
    const response = await fetch(urlFor("/config/tags/rename"), {

      body: JSON.stringify({ newTag: "-invalid", oldTag: "valid-source" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 400, "rename to an invalid name must reject with 400");

    const afterFile = await readPersistedJson(ctx, "channels.json") as Record<string, unknown> & { tagRegistry?: unknown };

    assert.equal(stringifySorted(afterFile["seed-a"]), beforeChannelBytes, "channel binding must be byte-identical when the rename is rejected for validation");
    assert.equal(stringifySorted(afterFile.tagRegistry ?? null), beforeRegistryBytes, "tag registry must be byte-identical when the rename is rejected for validation");
  });
});

describe("DELETE /config/tags/:tag - cascade across vocabulary and channel bindings", () => {

  /* The cascade-on-delete invariant: when a tag is removed from the vocabulary, every channel that referenced it must have the tag stripped from its bindings,
   * AND every other tag on those channels must survive untouched. Two failure modes this protects against:
   *
   *   1. Vocabulary-only delete: the tag disappears from the registry but stays on channel.tags arrays - users see "phantom" tags that no longer filter or
   *      group.
   *   2. Over-broad delete: the cascade strips the right tag but rewrites or re-orders other tags on the same channels, drifting the on-disk shape and breaking
   *      the byte-preservation guarantee that downstream tooling (diffs, merges, exports) depends on.
   *
   * The 404 path tests confirm the rejection short-circuit does not even open the cascade door - on an unknown-tag DELETE, neither vocabulary nor channel state
   * may mutate.
   */

  test("DELETE on a referenced user tag strips it from every channel that referenced it", async () => {

    /* Create a user tag, attach it to two channels alongside an unrelated predefined tag, DELETE the user tag, and verify: the user tag is gone from both
     * channels, the unrelated tag is preserved, and the registry no longer carries the deleted tag.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // Create the user tag in vocabulary (cascade only operates on registered tags - validates the documented contract that delete and binding stay in lockstep).
    const createResponse = await fetch(urlFor("/config/tags"), {

      body: JSON.stringify({ tag: "to-cascade" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(createResponse.status, 200, "creating the cascade tag should succeed");

    // Seed two channels that carry both the about-to-be-deleted tag AND the predefined News tag. The latter is the survival witness: cascade must not touch it.
    await mutateChannels((data) => {

      data.channels["seed-a"] = { name: "Seed A", tags: [ "News", "to-cascade" ], url: "https://example.test/a" };
      data.channels["seed-b"] = { name: "Seed B", tags: [ "News", "to-cascade" ], url: "https://example.test/b" };
    });

    const deleteResponse = await fetch(urlFor("/config/tags/to-cascade"), { method: "DELETE" });

    assert.equal(deleteResponse.status, 200, "delete should succeed; body: " + (await deleteResponse.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown> & { tagRegistry?: { tags?: string[] } };
    const registryTags = persisted.tagRegistry?.tags ?? [];

    assert.equal(registryTags.includes("to-cascade"), false, "registry must no longer carry the deleted tag");

    for(const key of [ "seed-a", "seed-b" ] as const) {

      const entry = persisted[key] as { tags?: string[] };

      assert.ok(Array.isArray(entry.tags), key + " should still carry a tags array after cascade delete");
      assert.equal(entry.tags.includes("to-cascade"), false, key + " must no longer carry the deleted tag");
      assert.ok(entry.tags.includes("News"), key + " must still carry the unrelated News tag");
    }
  });

  test("DELETE on a tag that no channel carries leaves channel state byte-identical", async () => {

    /* Create a user tag with no channel bindings, seed channels carrying a different tag, snapshot, DELETE the unreferenced tag, snapshot again. The
     * transformChannelTags filter (only entries that match) should yield zero affected keys, so no channel entry changes and each seed channel's serialized bytes
     * are byte-identical before and after.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await fetch(urlFor("/config/tags"), {

      body: JSON.stringify({ tag: "no-channels" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    await mutateChannels((data) => {

      data.channels["seed-a"] = { name: "Seed A", tags: ["News"], url: "https://example.test/a" };
      data.channels["seed-b"] = { name: "Seed B", tags: ["News"], url: "https://example.test/b" };
    });

    const before = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;
    const beforeBytes = { "seed-a": stringifySorted(before["seed-a"]), "seed-b": stringifySorted(before["seed-b"]) };

    const response = await fetch(urlFor("/config/tags/no-channels"), { method: "DELETE" });

    assert.equal(response.status, 200, "delete of an unreferenced tag should succeed");

    const after = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;

    assert.equal(stringifySorted(after["seed-a"]), beforeBytes["seed-a"], "seed-a must be byte-identical when no cascade was needed");
    assert.equal(stringifySorted(after["seed-b"]), beforeBytes["seed-b"], "seed-b must be byte-identical when no cascade was needed");
  });

  test("DELETE on an unknown tag rejects without modifying vocabulary or channel state", async () => {

    /* The 404 path must short-circuit before either the registry write or the cascade. We seed a channel with the predefined News tag, snapshot vocabulary +
     * channel bytes, attempt to DELETE a tag that has never existed, and assert: 404 status, registry unchanged, channel entry byte-identical.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await mutateChannels((data) => {

      data.channels["seed-a"] = { name: "Seed A", tags: ["News"], url: "https://example.test/a" };
    });

    const beforeFile = await readPersistedJson(ctx, "channels.json") as Record<string, unknown> & { tagRegistry?: { deletedTags: string[]; tags: string[] } };
    const beforeChannelBytes = stringifySorted(beforeFile["seed-a"]);
    const beforeRegistryBytes = stringifySorted(beforeFile.tagRegistry ?? null);

    const response = await fetch(urlFor("/config/tags/never-existed-tag-x9z2"), { method: "DELETE" });

    assert.equal(response.status, 404, "deleting an unknown tag should reject with 404");

    const afterFile = await readPersistedJson(ctx, "channels.json") as Record<string, unknown> & { tagRegistry?: { deletedTags: string[]; tags: string[] } };

    assert.equal(stringifySorted(afterFile["seed-a"]), beforeChannelBytes, "channel binding must be byte-identical when the delete is rejected");
    assert.equal(stringifySorted(afterFile.tagRegistry ?? null), beforeRegistryBytes, "tag registry must be byte-identical when the delete is rejected");
  });
});

describe("POST /config/tags/restore - restore a deleted predefined tag", () => {

  /* The restore invariant is the inverse of the predefined delete: removing a predefined tag tombstones it in deletedTags (dropping it from the active
   * vocabulary) and cascades a tag-stripping override onto every predefined channel whose definition carries it; restoring the tag reverses both halves. It must
   * (1) remove the tag from deletedTags so it re-enters the active vocabulary, and (2) cascade the canonical-cased tag back onto exactly those predefined channels
   * whose DEFINITION includes it but whose current resolved tags dropped it. The normalizer then strips the now-redundant override, reverting each such channel to
   * its predefined default. We use the real predefined channel "cooking" (Food Network's Cooking Channel), whose definition carries the single predefined tag
   * "Lifestyle", as the cascade witness. The not-deleted-tag path 404s, mirroring the DELETE 404 short-circuit in the sibling delete-cascade block above.
   */

  test("restores a deleted predefined tag to the vocabulary and re-applies it to the channels that define it", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // Delete the predefined Lifestyle tag first so it enters deletedTags and drops from the channels that define it. The "cooking" channel's stored delta becomes
    // an explicit tag-stripping override ({ tags: null }) - a witness that the tag was actively removed rather than simply absent.
    const deleteResponse = await fetch(urlFor("/config/tags/Lifestyle"), { method: "DELETE" });

    assert.equal(deleteResponse.status, 200, "deleting the predefined tag should succeed");

    const afterDelete = await readPersistedJson(ctx, "channels.json") as Record<string, unknown> & { tagRegistry?: { deletedTags: string[]; tags: string[] } };

    assert.deepEqual(afterDelete["cooking"], { tags: null }, "delete must leave a tag-stripping override on the predefined channel that defined Lifestyle");
    assert.ok((afterDelete.tagRegistry?.deletedTags ?? []).includes("Lifestyle"), "delete must record the canonical predefined tag in deletedTags");

    // Restore it. deletedTags must drop Lifestyle so it re-enters the active vocabulary, and the cascade must re-add the canonical tag to cooking, which the
    // normalizer then reverts to its predefined default - the override disappears from the persisted file entirely.
    const restoreResponse = await fetch(urlFor("/config/tags/restore"), {

      body: JSON.stringify({ tag: "Lifestyle" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(restoreResponse.status, 200, "restore should succeed; body: " + (await restoreResponse.clone().text()).slice(0, 200));

    const restoreBody = await restoreResponse.json() as { active: string[]; registry: { deletedTags: string[]; tags: string[] }; success: boolean };

    assert.ok(restoreBody.active.includes("Lifestyle"), "the restored tag must re-enter the active vocabulary bundle");
    assert.equal(restoreBody.registry.deletedTags.includes("Lifestyle"), false, "restore must remove the tag from the deletedTags registry");

    const afterRestore = await readPersistedJson(ctx, "channels.json") as Record<string, unknown> & { tagRegistry?: { deletedTags: string[]; tags: string[] } };

    assert.equal(afterRestore["cooking"], undefined, "restore must revert the predefined channel to its default so its Lifestyle definition tag applies again");
    assert.equal((afterRestore.tagRegistry?.deletedTags ?? []).includes("Lifestyle"), false, "persisted deletedTags must no longer carry the restored tag");
  });

  test("returns 404 when restoring a tag that was never deleted", async () => {

    /* The restore guard: a tag that is not in deletedTags cannot be restored. Lifestyle is a valid predefined tag but has not been deleted here, so restore must
     * 404 rather than silently succeed. This mirrors the not-found short-circuit the delete and rename endpoints enforce.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/tags/restore"), {

      body: JSON.stringify({ tag: "Lifestyle" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 404, "restoring a tag that was never deleted must reject with 404");
  });
});

describe("POST /config/tags/rename - predefined versus user tag registry semantics", () => {

  /* Rename resolves the old tag's provenance and takes one of two registry paths, both cascading identically across channel bindings. For a PREDEFINED old tag,
   * the endpoint tombstones the canonical old name in deletedTags (exactly once) and pushes the new name as a user tag - it cannot mutate the immutable predefined
   * list in place. For a USER old tag, the endpoint maps the name in place within the user tags array and never touches deletedTags. These two tests pin that
   * distinction in the persisted registry while asserting the shared cascade onto channel bindings.
   */

  test("renaming a PREDEFINED tag tombstones the canonical old name and adds the new name as a user tag, cascading to bindings", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // Rename the predefined Lifestyle tag. cooking (which defines Lifestyle) is the cascade witness: its stored delta must carry the new name.
    const response = await fetch(urlFor("/config/tags/rename"), {

      body: JSON.stringify({ newTag: "Wellness", oldTag: "Lifestyle" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "renaming a predefined tag should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown> & { tagRegistry?: { deletedTags: string[]; tags: string[] } };
    const deletedTags = persisted.tagRegistry?.deletedTags ?? [];
    const tags = persisted.tagRegistry?.tags ?? [];

    assert.equal(deletedTags.filter((t) => t === "Lifestyle").length, 1, "the canonical old predefined tag must be tombstoned in deletedTags exactly once");
    assert.ok(tags.includes("Wellness"), "the new name must be added to the user tags");
    assert.equal(tags.includes("Lifestyle"), false, "the old predefined name must not appear in the user tags");
    assert.deepEqual(persisted["cooking"], { tags: ["Wellness"] }, "the rename must cascade onto the predefined channel that defined the old tag");
  });

  test("renaming a USER tag maps it in place and never tombstones anything in deletedTags", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // Create a user tag, bind it to a channel alongside an unrelated predefined tag, then rename it.
    const createResponse = await fetch(urlFor("/config/tags"), {

      body: JSON.stringify({ tag: "custom-a" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(createResponse.status, 200, "creating the user tag should succeed");

    await mutateChannels((data) => {

      data.channels["seed-a"] = { name: "Seed A", tags: [ "News", "custom-a" ], url: "https://example.test/a" };
    });

    const response = await fetch(urlFor("/config/tags/rename"), {

      body: JSON.stringify({ newTag: "custom-b", oldTag: "custom-a" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "renaming a user tag should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown> & { tagRegistry?: { deletedTags: string[]; tags: string[] } };
    const deletedTags = persisted.tagRegistry?.deletedTags ?? [];
    const tags = persisted.tagRegistry?.tags ?? [];

    assert.ok(tags.includes("custom-b"), "the user tags must carry the new name after an in-place rename");
    assert.equal(tags.includes("custom-a"), false, "the user tags must no longer carry the old name");
    assert.deepEqual(deletedTags, [], "a user-tag rename must not tombstone anything in deletedTags");

    const entry = persisted["seed-a"] as { tags?: string[] };

    assert.ok(Array.isArray(entry.tags), "seed-a should still carry a tags array after the cascade rename");
    assert.ok(entry.tags.includes("custom-b"), "seed-a must carry the renamed user tag");
    assert.equal(entry.tags.includes("custom-a"), false, "seed-a must no longer carry the old user tag");
    assert.ok(entry.tags.includes("News"), "seed-a must still carry the unrelated News tag");
  });
});

describe("POST /config/tags - creating a deleted predefined tag conflicts", () => {

  /* A deleted predefined tag is not gone - it is tombstoned in deletedTags and can be restored. Re-creating it as a fresh user tag would fork the identity and
   * lose the restore path, so the create endpoint rejects that case with a 409 that directs the user to restore instead. We delete the predefined Lifestyle tag,
   * then attempt to create it, and pin the 409 plus its restore guidance and the fact that the conflicting create leaves the user vocabulary untouched.
   */

  test("returns 409 directing the user to restore when creating a tag that matches a deleted predefined tag", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const deleteResponse = await fetch(urlFor("/config/tags/Lifestyle"), { method: "DELETE" });

    assert.equal(deleteResponse.status, 200, "deleting the predefined tag should succeed");

    const response = await fetch(urlFor("/config/tags"), {

      body: JSON.stringify({ tag: "Lifestyle" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 409, "creating a tag matching a deleted predefined tag must reject with 409");

    const body = await response.json() as { error: string; success: boolean };

    assert.match(body.error, /restore/i, "the 409 must direct the user to restore rather than re-create");

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown> & { tagRegistry?: { deletedTags: string[]; tags: string[] } };
    const tags = persisted.tagRegistry?.tags ?? [];

    assert.equal(tags.some((t) => t.toLowerCase() === "lifestyle"), false, "the rejected create must not add the deleted predefined tag to the user vocabulary");
  });
});

describe("DELETE /config/tags/:tag - predefined delete and already-deleted no-op", () => {

  /* Deleting a predefined tag differs from deleting a user tag: the user tags array is immutable for predefined names, so the endpoint records the canonical form
   * in deletedTags and cascades a tag-stripping override across bindings. Re-deleting an ALREADY-deleted predefined tag is a no-op: the handler short-circuits
   * before setTagRegistry and the cascade, returning current state without a second write. We pin both halves - the first delete's registry + cascade effect, and
   * the second delete's byte-identical persisted state proving no write occurred.
   */

  test("records the canonical predefined form and cascades on first delete; re-delete is a no-op that leaves persisted state byte-identical", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const first = await fetch(urlFor("/config/tags/Lifestyle"), { method: "DELETE" });

    assert.equal(first.status, 200, "the first predefined delete should succeed");

    const afterFirst = await readPersistedJson(ctx, "channels.json") as Record<string, unknown> & { tagRegistry?: { deletedTags: string[]; tags: string[] } };

    assert.deepEqual(afterFirst["cooking"], { tags: null }, "the first delete must strip the tag from the predefined channel that defined it");
    assert.ok((afterFirst.tagRegistry?.deletedTags ?? []).includes("Lifestyle"), "the first delete must record the canonical predefined tag in deletedTags");

    const afterFirstBytes = stringifySorted(afterFirst);

    const second = await fetch(urlFor("/config/tags/Lifestyle"), { method: "DELETE" });

    assert.equal(second.status, 200, "re-deleting an already-deleted predefined tag returns current state");

    const afterSecond = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;

    assert.equal(stringifySorted(afterSecond), afterFirstBytes, "re-deleting an already-deleted predefined tag must not mutate persisted state");
  });
});
