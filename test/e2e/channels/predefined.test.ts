/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * predefined.test.ts: HTTP-level integration coverage for the predefined-channel toggle endpoints. Two endpoints: toggle-predefined disables/enables a single
 * channel by key; bulk-toggle-predefined operates on a scope (all, east, pacific). Disabled channels do NOT appear in the playlist or HDHR lineup, so a
 * regression in the toggle path manifests as channels appearing/disappearing from the user's library.
 */
import { bootApp, createIntegrationContext, initializePersistence, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

describe("POST /config/channels/toggle-predefined - single channel toggle", () => {

  test("disables a predefined channel and persists it to disabledPredefined", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/channels/toggle-predefined"), {

      body: JSON.stringify({ enabled: false, key: "abc" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "toggle should succeed");

    const persisted = await readPersistedJson(ctx, "config.json") as { channels?: { disabledPredefined?: string[] } };
    const disabled = persisted.channels?.disabledPredefined ?? [];

    assert.ok(disabled.includes("abc"), "abc should be in disabledPredefined");
  });

  test("re-enables a previously-disabled channel by removing it from disabledPredefined", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // Disable.
    await fetch(urlFor("/config/channels/toggle-predefined"), {

      body: JSON.stringify({ enabled: false, key: "abc" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    // Re-enable.
    const response = await fetch(urlFor("/config/channels/toggle-predefined"), {

      body: JSON.stringify({ enabled: true, key: "abc" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200);

    const persisted = await readPersistedJson(ctx, "config.json") as { channels?: { disabledPredefined?: string[] } };
    const disabled = persisted.channels?.disabledPredefined ?? [];

    assert.equal(disabled.includes("abc"), false, "abc should NOT be in disabledPredefined after re-enable");
  });

  test("rejects toggling a non-predefined channel with 400", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/channels/toggle-predefined"), {

      body: JSON.stringify({ enabled: false, key: "definitely-not-a-predefined-channel-x9z2" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 400, "non-predefined toggle should reject");
  });
});

describe("POST /config/channels/bulk-toggle-predefined - scope-based toggle", () => {

  test("rejects an unknown scope with 400", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/channels/bulk-toggle-predefined"), {

      body: JSON.stringify({ enabled: false, scope: "nonsense" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 400, "unknown scope should reject");
  });

  test("scope=all disables every predefined channel", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/channels/bulk-toggle-predefined"), {

      body: JSON.stringify({ enabled: false, scope: "all" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "bulk-toggle should succeed");

    const persisted = await readPersistedJson(ctx, "config.json") as { channels?: { disabledPredefined?: string[] } };
    const disabled = persisted.channels?.disabledPredefined ?? [];

    // The disabled list should be non-empty - many channels disabled.
    assert.ok(disabled.length > 0, "disabledPredefined should be populated after bulk-toggle all");
  });
});
