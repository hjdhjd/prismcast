/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * crud.test.ts: HTTP-level integration coverage for the channel CRUD lifecycle. The unit tier (crud.test.ts under src/) covers the route handlers' branching
 * logic against synthetic Express stubs; this suite exercises the same handlers through a real Express boot, end-to-end with persistence. The pattern across
 * tests: POST creates, PUT edits, DELETE removes, POST/:key/revert restores predefined defaults. Each test verifies on-disk state matches the API response.
 */
import { bootApp, createIntegrationContext, initializePersistence, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
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
