/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * import-export.test.ts: HTTP-level integration coverage for the channel import/export endpoints. Three operations: GET /config/channels/export returns the
 * resolved user channels as JSON; POST /config/channels/import replaces the user channels from a JSON payload; POST /config/channels/import-m3u parses an M3U
 * playlist and imports the channels (with conflict modes "skip" or "replace"). The unit tier covers the parser/validator; this suite exercises the full
 * round-trip plus persistence.
 */
import { bootApp, createIntegrationContext, initializePersistence, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

describe("GET /config/channels/export", () => {

  test("returns user channels as a JSON download", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // Create a user channel first so the export has content.
    await fetch(urlFor("/config/channels"), {

      body: JSON.stringify({ channelNumber: "", channelSelector: "", guideTitle: "", hdhrEnabled: "true", key: "exported-channel", logoUrl: "",
        name: "Exported", profile: "", stationId: "", tags: "", url: "https://example.test/exported" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    const response = await fetch(urlFor("/config/channels/export"));

    assert.equal(response.status, 200);
    assert.match(response.headers.get("content-type") ?? "", /application\/json/);
    assert.match(response.headers.get("content-disposition") ?? "", /attachment/);

    const exported = await response.json() as Record<string, unknown>;

    assert.ok("exported-channel" in exported, "exported JSON should contain the user channel we created");
  });
});

describe("POST /config/channels/import", () => {

  test("imports a JSON payload, replacing existing user channels", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    /* The import endpoint accepts a Record<string, Channel>. Each channel needs at minimum name and url. We import two channels and verify both land on disk.
     */
    const response = await fetch(urlFor("/config/channels/import"), {

      body: JSON.stringify({

        "imported-a": { name: "A", url: "https://example.test/a" },
        "imported-b": { name: "B", url: "https://example.test/b" }
      }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "import should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;

    assert.ok("imported-a" in persisted, "imported-a should be on disk");
    assert.ok("imported-b" in persisted, "imported-b should be on disk");
  });

  test("rejects an invalid payload (missing required fields) with 400", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/channels/import"), {

      body: JSON.stringify({

        "broken": {}
      }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 400, "invalid import should reject");
  });
});

describe("POST /config/channels/import-m3u", () => {

  test("imports channels from an M3U playlist (skip conflict mode)", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const m3u = "#EXTM3U\n" +
      "#EXTINF:-1 tvg-id=\"custom-import\" tvg-name=\"Custom Import\",Custom Import\n" +
      "https://example.test/custom-import\n";

    const response = await fetch(urlFor("/config/channels/import-m3u"), {

      body: JSON.stringify({ conflictMode: "skip", content: m3u }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "M3U import should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;

    /* The M3U importer derives a key from the channel name. We accept any key that resolves to an entry whose name matches our import.
     */
    const importedKeys = Object.keys(persisted).filter((k) => {

      const value = persisted[k];

      if((typeof value !== "object") || (value === null)) {

        return false;
      }

      return (value as { name?: string }).name === "Custom Import";
    });

    assert.ok(importedKeys.length > 0, "the imported channel should be on disk");
  });

  test("rejects empty M3U content with 400", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/channels/import-m3u"), {

      body: JSON.stringify({ content: "" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 400, "empty M3U should reject");
  });

  test("rejects an invalid conflictMode with 400", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/channels/import-m3u"), {

      body: JSON.stringify({ conflictMode: "merge-or-something", content: "#EXTM3U\n" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 400, "invalid conflict mode should reject");
  });
});
