/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * crud.test.ts: HTTP-level integration coverage for the channel CRUD lifecycle. The unit tier (crud.test.ts under src/) covers the route handlers' branching
 * logic against synthetic Express stubs; this suite exercises the same handlers through a real Express boot, end-to-end with persistence. The pattern across
 * tests: POST creates, PUT edits, DELETE removes, POST/:key/revert restores predefined defaults. Each test verifies on-disk state matches the API response.
 */
import { bootApp, createIntegrationContext, initializePersistence, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { PLAYLIST_HINT } from "../../../src/routes/config/channels/http/playlistHint.ts";
import assert from "node:assert/strict";

/**
 * Builds a complete form-shape body for the channel CRUD endpoints. Defaults to the abc canonical shape; callers override fields they want to test. The keys
 * mirror what the channel edit form submits.
 */
function makeFormBody(overrides: Partial<Record<string, string>> = {}): Record<string, string> {

  return {

    channelNumber: "",
    channelSelector: "",
    guideTitle: "",
    hdhrEnabled: "true",
    logoUrl: "",
    name: "",
    profile: "",
    stationId: "",
    tags: "",
    url: "",
    ...overrides
  };
}

describe("POST /config/channels - create user channel", () => {

  test("creates a new user channel with the submitted fields", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/channels"), {

      body: JSON.stringify(makeFormBody({ key: "my-custom", name: "Custom", url: "https://example.test/custom" })),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "POST should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;

    assert.ok("my-custom" in persisted, "user channel should be persisted on disk");

    const entry = persisted["my-custom"] as { name: string; url: string };

    assert.equal(entry.name, "Custom");
    assert.equal(entry.url, "https://example.test/custom");
  });

  test("rejects creation without a key", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/channels"), {

      body: JSON.stringify(makeFormBody({ name: "Custom", url: "https://example.test/custom" })),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 400, "creation without a key should reject with 400");
  });
});

describe("DELETE /config/channels/:key - delete user channel", () => {

  test("deletes a user channel from disk", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // Seed via POST.
    await fetch(urlFor("/config/channels"), {

      body: JSON.stringify(makeFormBody({ key: "my-custom", name: "Custom", url: "https://example.test/custom" })),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    // Delete.
    const response = await fetch(urlFor("/config/channels/my-custom"), { method: "DELETE" });

    assert.equal(response.status, 200);

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;

    assert.equal("my-custom" in persisted, false, "user channel should be removed from disk");
  });

  test("rejects deletion of a non-user channel (predefined-only)", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // abc is a predefined-only channel - no user override exists.
    const response = await fetch(urlFor("/config/channels/abc"), { method: "DELETE" });

    // Predefined-only channels can't be deleted; the endpoint refuses.
    assert.notEqual(response.status, 200, "predefined-only channels should not be deleteable");
  });
});

describe("POST /config/channels/:key/revert - revert override to predefined", () => {

  test("removes the user override and restores the predefined defaults", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    /* Seed a channelNumber override via PUT, then revert. The on-disk file should no longer carry the override entry. */
    await fetch(urlFor("/config/channels/abc"), {

      body: JSON.stringify(makeFormBody({ channelNumber: "7", channelSelector: "ABC", name: "ABC", url: "https://abc.com/watch-live" })),
      headers: { "content-type": "application/json" },
      method: "PUT"
    });

    const beforePersisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;

    assert.ok("abc" in beforePersisted, "override should be on disk before revert");

    // Revert.
    const response = await fetch(urlFor("/config/channels/abc/revert"), { method: "POST" });

    assert.equal(response.status, 200, "revert should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const afterPersisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;

    assert.equal("abc" in afterPersisted, false, "override entry should be deleted by revert");
  });
});

describe("PUT /config/channels/:key - user-only channel full replace", () => {

  /* The user-only PUT path (no predefined base for the key) rebuilds the entire stored record from the submitted form and replaces the old one wholesale - it is
   * not a field-merge. A field present on the original record but absent from the edit must vanish from disk. The response's playlist-reload hint is derived from
   * the actual old-to-new M3U-field diff (playlistHintForChange over M3U_FIELDS), so an edit that changes an M3U-visible field carries the hint and an edit that
   * touches only non-M3U fields does not. These tests assert both halves: full replacement and diff-derived hinting.
   */

  test("replaces the whole record - a field cleared in the edit is removed from disk - and an M3U change yields the reload hint", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // Seed a user channel carrying both a stationId and a channelNumber (both M3U-visible identity fields).
    await fetch(urlFor("/config/channels"), {

      body: JSON.stringify(makeFormBody({ channelNumber: "7", key: "my-custom", name: "Custom", stationId: "12345", url: "https://example.test/custom" })),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    // Edit: rename the channel and clear both the stationId and channelNumber by submitting them empty. The rebuilt record should carry only name and url.
    const response = await fetch(urlFor("/config/channels/my-custom"), {

      body: JSON.stringify(makeFormBody({ name: "Renamed", url: "https://example.test/custom" })),
      headers: { "content-type": "application/json" },
      method: "PUT"
    });

    assert.equal(response.status, 200, "PUT should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const body = await response.json() as { message: string; success: boolean };

    // The name (an M3U field) changed, so the reload hint must be appended to the message.
    assert.ok(body.message.endsWith(PLAYLIST_HINT), "an M3U field change must append the playlist reload hint");

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;

    assert.ok("my-custom" in persisted, "the edited channel should still be on disk");

    const entry = persisted["my-custom"] as Record<string, unknown>;

    assert.equal(entry["name"], "Renamed", "the name should reflect the edit");
    assert.equal("stationId" in entry, false, "the cleared stationId must be dropped by the full-record replace");
    assert.equal("channelNumber" in entry, false, "the cleared channelNumber must be dropped by the full-record replace");
  });

  test("an edit that touches no M3U field omits the reload hint while still replacing the record", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // Seed with a channelSelector (a non-M3U binding field) and an initial URL.
    await fetch(urlFor("/config/channels"), {

      body: JSON.stringify(makeFormBody({ channelSelector: "OLD", key: "my-custom", name: "Custom", url: "https://example.test/a" })),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    // Edit: keep every M3U field identical (name unchanged, no station/number/logo/guide) and change only the channelSelector and the URL. Neither is an M3U field,
    // so the response must NOT carry the reload hint even though the record genuinely changed.
    const response = await fetch(urlFor("/config/channels/my-custom"), {

      body: JSON.stringify(makeFormBody({ channelSelector: "NEW", name: "Custom", url: "https://example.test/b" })),
      headers: { "content-type": "application/json" },
      method: "PUT"
    });

    assert.equal(response.status, 200, "PUT should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const body = await response.json() as { message: string; success: boolean };

    assert.equal(body.message.includes(PLAYLIST_HINT), false, "a non-M3U-only edit must not append the playlist reload hint");

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;
    const entry = persisted["my-custom"] as Record<string, unknown>;

    assert.equal(entry["channelSelector"], "NEW", "the channelSelector should reflect the edit");
    assert.equal(entry["url"], "https://example.test/b", "the url should reflect the edit");
  });
});

describe("PATCH /config/channels/:key - inline cell edits", () => {

  /* PATCH updates exactly one inline-editable cell per request. These tests assert the storage contract for the value-shaping branches on a predefined channel
   * (abcnews ships with stationId "113380" and tags ["News"]), where an override is stored as a delta against the predefined base so an explicit "clear" survives
   * as an on-disk null rather than being dropped:
   *   - stationId "" clears the stored override to null (the predefined base has a stationId, so the null-clear is preserved).
   *   - hdhrEnabled stores false only when disabling; enabling clears the override (persisting neither true nor false).
   *   - tags are sorted before storage so the normalizer's order-independent equality check sees a canonical order regardless of submitted order.
   */

  test("stationId submitted empty clears the stored override to null", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/channels/abcnews"), {

      body: JSON.stringify({ stationId: "" }),
      headers: { "content-type": "application/json" },
      method: "PATCH"
    });

    assert.equal(response.status, 200, "PATCH should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;
    const entry = persisted["abcnews"] as Record<string, unknown>;

    assert.equal("stationId" in entry, true, "the cleared stationId must persist as an explicit override key");
    assert.equal(entry["stationId"], null, "clearing the stationId against a predefined base stores null, not an absent field");
  });

  test("hdhrEnabled stores false only when disabling; enabling clears the override", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // Disable: false is stored as an explicit override against the predefined base (which has no hdhrEnabled field, defaulting to included).
    const disableResponse = await fetch(urlFor("/config/channels/abcnews"), {

      body: JSON.stringify({ hdhrEnabled: false }),
      headers: { "content-type": "application/json" },
      method: "PATCH"
    });

    assert.equal(disableResponse.status, 200, "disabling PATCH should succeed; body: " + (await disableResponse.clone().text()).slice(0, 200));

    const disabled = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;
    const disabledEntry = disabled["abcnews"] as Record<string, unknown>;

    assert.equal(disabledEntry["hdhrEnabled"], false, "disabling must persist hdhrEnabled false");

    // Enable: true maps to null in the handler, which is a no-op against the base and collapses the override entry entirely - neither true nor false is stored.
    const enableResponse = await fetch(urlFor("/config/channels/abcnews"), {

      body: JSON.stringify({ hdhrEnabled: true }),
      headers: { "content-type": "application/json" },
      method: "PATCH"
    });

    assert.equal(enableResponse.status, 200, "enabling PATCH should succeed; body: " + (await enableResponse.clone().text()).slice(0, 200));

    const enabled = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;

    assert.equal("abcnews" in enabled, false, "enabling must clear the override entirely, storing neither true nor false");
  });

  test("tags are sorted before storage regardless of submitted order", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // Submit three predefined tags in a deliberately unsorted order. They differ from the base's ["News"] so the override is stored rather than collapsed.
    const response = await fetch(urlFor("/config/channels/abcnews"), {

      body: JSON.stringify({ tags: [ "Sports", "News", "Documentary" ] }),
      headers: { "content-type": "application/json" },
      method: "PATCH"
    });

    assert.equal(response.status, 200, "PATCH should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;
    const entry = persisted["abcnews"] as { tags?: unknown };

    assert.deepEqual(entry.tags, [ "Documentary", "News", "Sports" ], "tags must be persisted in canonical sorted order");
  });
});

describe("PUT /config/channels/:key - edit that matches a sibling variant", () => {

  /* handlePredefinedEdit's findMatchingVariant branch: when a stored canonical override is edited so its submitted values exactly match a sibling service
   * variant's predefined definition, the edit is an implicit revert-to-that-variant. The canonical override is deleted and serviceSelections is switched to the
   * matched variant rather than storing a redundant custom override. abc is a real predefined channel whose canonical service is its own site; abc-hulu is a
   * sibling variant (channelSelector "ABC", the Hulu Live URL) that inherits identity from the canonical. We first create a canonical override via an inline
   * channelNumber edit, then PUT values matching the Hulu variant and assert the canonical override is gone and the service selection points at abc-hulu.
   */

  test("deletes the canonical override and switches serviceSelections to the matched variant", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // Create a canonical override on abc via an inline channelNumber edit so isUserChannel("abc") is true when the sibling-match branch is evaluated.
    const seedResponse = await fetch(urlFor("/config/channels/abc"), {

      body: JSON.stringify({ channelNumber: 999 }),
      headers: { "content-type": "application/json" },
      method: "PATCH"
    });

    assert.equal(seedResponse.status, 200, "seeding the canonical override should succeed; body: " + (await seedResponse.clone().text()).slice(0, 200));

    const seeded = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;

    assert.ok("abc" in seeded, "the canonical override should be on disk before the variant-matching edit");

    // Edit abc so every submitted value matches the abc-hulu variant's predefined definition exactly: the Hulu channelSelector and URL, the inherited name and
    // tags, and an empty channelNumber (the variant inherits no channelNumber from the canonical).
    const response = await fetch(urlFor("/config/channels/abc"), {

      body: JSON.stringify(makeFormBody({ channelSelector: "ABC", name: "ABC", tags: "Local", url: "https://www.hulu.com/live" })),
      headers: { "content-type": "application/json" },
      method: "PUT"
    });

    assert.equal(response.status, 200, "the variant-matching PUT should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const body = await response.json() as { success: boolean };

    assert.equal(body.success, true, "the response envelope should report success");

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown> & { serviceSelections?: Record<string, string> };

    assert.equal("abc" in persisted, false, "the canonical override must be deleted by the implicit variant revert");
    assert.ok(persisted.serviceSelections, "serviceSelections should be present after switching the active variant");
    assert.equal(persisted.serviceSelections["abc"], "abc-hulu", "the service selection must switch to the matched sibling variant");
  });
});
