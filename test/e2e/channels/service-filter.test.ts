/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * service-filter.test.ts: HTTP-level integration coverage for the service-filter and bulk-assign endpoints. The service filter restricts which channels appear
 * in the table/playlist based on which streaming services the user subscribes to. Bulk-assign sets a specific service variant on every multi-service channel.
 * Together these endpoints keep channel availability in production consistent with what the user actually configures.
 */
import { bootApp, createIntegrationContext, initializePersistence, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

describe("POST /config/service-filter", () => {

  test("sets the enabled services list and persists it to config.json", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/service-filter"), {

      body: JSON.stringify({ enabledServices: [ "hulu", "sling" ] }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "service-filter update should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "config.json") as { channels?: { enabledServices?: string[] } };
    const enabled = persisted.channels?.enabledServices ?? [];

    assert.deepEqual(enabled.toSorted(), [ "hulu", "sling" ].toSorted(), "enabledServices should reflect the request");
  });

  test("rejects a non-array body with 400", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/service-filter"), {

      body: JSON.stringify({ enabledServices: "not-an-array" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 400, "non-array body should reject");
  });

  test("rejects an unknown service tag with 400", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/service-filter"), {

      body: JSON.stringify({ enabledServices: [ "hulu", "totally-unknown-service-x9z2" ] }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 400, "unknown service tag should reject");
  });

  test("an empty enabledServices array means 'no filter active' (clears the filter)", async () => {

    /* The semantic of an empty array is "all services pass." After clearing, channels.enabledServices on disk should be either absent or empty. We don't pin
     * which because filterDefaults may strip an empty array; we just confirm no filter remains.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // Set a filter first.
    await fetch(urlFor("/config/service-filter"), {

      body: JSON.stringify({ enabledServices: ["hulu"] }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    // Clear it.
    const response = await fetch(urlFor("/config/service-filter"), {

      body: JSON.stringify({ enabledServices: [] }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200);

    const persisted = await readPersistedJson(ctx, "config.json") as { channels?: { enabledServices?: string[] } };
    const enabled = persisted.channels?.enabledServices ?? [];

    assert.equal(enabled.length, 0, "enabledServices should be empty (filter cleared)");
  });
});

describe("POST /config/service-bulk-assign", () => {

  test("rejects an empty service tag with 400", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/service-bulk-assign"), {

      body: JSON.stringify({ service: "" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 400, "empty service should reject");
  });
});
