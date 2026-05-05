/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * tags.test.ts: HTTP-level integration coverage for the tag vocabulary endpoints. Tags drive playlist filtering and channel-table grouping; the vocabulary
 * lives in tagRegistry inside channels.json. The 1.9.0 fix preserved tag casing - lowercase user input ("news") and capitalized form ("News") are the same
 * tag at the identity level but the displayed casing is what the user typed first. This suite exercises the full CRUD: create, list, delete (with cascade),
 * rename (cascading across channels).
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
   * shape of the cascade contract here mirrors Suite 20 exactly. The conflict path (renaming to an existing name) is the additional pin specific to rename:
   * a 409 must short-circuit before either the registry write or the cascade, so vocabulary and channels stay byte-identical when the rename is rejected.
   *
   * The validation-failure path also short-circuits before the registry write, so a malformed newTag (empty, too long, invalid character) leaves the world
   * unchanged just like the conflict path. This mirrors DELETE's 404 short-circuit pinned in the prior describe block.
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
     * touching the registry or the cascade. We snapshot per-key bytes pre-rename and assert byte-equality post-rename to prove the short-circuit. The 409
     * envelope is checked for completeness but the real invariant is the unchanged on-disk state.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    for(const tag of [ "alpha-tag", "beta-tag" ]) {

      // Two sequential POST creations - the tag-create handler reads getTagRegistry() (a shared reference) and pushes; concurrent handlers can race on that read,
      // so we serialize at the test level. eslint-disable-next-line no-await-in-loop -- handler-state race forces sequential creates here.
      // eslint-disable-next-line no-await-in-loop -- see comment above.
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
     * match) yields zero affected keys, so no channel write happens and per-entry channel bytes hold. The registry, of course, must reflect the rename.
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
     * transformChannelTags filter (only entries that match) should yield zero affected keys, so no channel write happens and the per-entry bytes hold.
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
