/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * bulk.test.ts: HTTP-level integration coverage for the bulk channel-operation endpoints. Three operations are exposed: auto-number assigns sequential numbers
 * to all visible channels, hdhr-bulk toggles HDHomeRun inclusion across the visible set, and bulk-tags applies tag operations to the visible set. The unit
 * tier covers the validation surface; this suite verifies the bulk operations actually mutate the persisted state across many channels in one transaction.
 */
import { bootApp, createIntegrationContext, initializePersistence, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { mutateEnabledServices } from "../../../src/config/services.ts";

describe("POST /config/channels/auto-number", () => {

  test("assigns sequential channel numbers starting from the requested start value", async () => {

    /* Auto-number takes a starting integer and assigns N, N+1, N+2, ... to the visible channels in sort order. With predefined channels populated, the first
     * few visible channels should receive the sequential numbers we requested.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/channels/auto-number"), {

      body: JSON.stringify({ sortDirection: "asc", sortField: "name", start: 100 }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "auto-number should succeed; body: " + (await response.clone().text()).slice(0, 200));

    /* The first visible channel by name (alphabetical) should have channelNumber: 100. We don't pin a specific channel because the predefined catalog evolves;
     * we just confirm at least one entry has the assigned number.
     */
    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;
    const entries = Object.entries(persisted).filter(([ , v ]) => v && (typeof v === "object") && ("channelNumber" in (v as Record<string, unknown>)));

    assert.ok(entries.length > 0, "at least one channel should have a channelNumber after auto-number");

    const numbers = entries.map(([ , v ]) => (v as { channelNumber: number }).channelNumber).toSorted((a, b) => a - b);

    assert.equal(numbers[0], 100, "the lowest assigned number should match start");
  });

  test("rejects an out-of-range start value with 400", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/channels/auto-number"), {

      body: JSON.stringify({ start: 999999 }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 400, "out-of-range start should reject");
  });

  test("clear mode (start: 0) removes channelNumber from all visible channels", async () => {

    /* Clear mode is a sentinel: start=0 means "remove all channel numbers." After running, no visible channel should carry a channelNumber.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // First assign numbers.
    await fetch(urlFor("/config/channels/auto-number"), {

      body: JSON.stringify({ start: 100 }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    // Then clear.
    const response = await fetch(urlFor("/config/channels/auto-number"), {

      body: JSON.stringify({ start: 0 }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200);

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;
    const withNumbers = Object.entries(persisted).filter(([ , v ]) => v && (typeof v === "object") && ("channelNumber" in (v as Record<string, unknown>)));

    assert.equal(withNumbers.length, 0, "no channel should carry a channelNumber after clear");
  });
});

describe("POST /config/channels/hdhr-bulk", () => {

  test("disables HDHR inclusion for all visible channels when enable=false", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/channels/hdhr-bulk"), {

      body: JSON.stringify({ enable: false }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "hdhr-bulk disable should succeed; body: " + (await response.clone().text()).slice(0, 200));

    /* At least one channel should now carry hdhrEnabled: false. We don't pin all channels because the override may write only those that DIFFER from the
     * predefined default, but the change should land somewhere.
     */
    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;
    const disabled = Object.entries(persisted).filter(([ , v ]) => (v as { hdhrEnabled?: boolean }).hdhrEnabled === false);

    assert.ok(disabled.length > 0, "hdhr-bulk disable should produce at least one channel with hdhrEnabled: false");
  });
});

describe("POST /config/channels/bulk-tags", () => {

  test("rejects an unknown action with 400", async () => {

    /* The endpoint validates action ∈ {"add", "remove"}. An unknown action is rejected with a clear error message before any tag manipulation runs.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/channels/bulk-tags"), {

      body: JSON.stringify({ action: "delete", tag: "any-tag" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 400, "unknown action should reject");
  });

  test("rejects an unknown tag (not in vocabulary) with 400", async () => {

    /* Tags must be in the vocabulary (managed via /config/tags). bulk-tags is a no-op-or-error operation: applying an unknown tag should reject rather than
     * silently no-op so operators see the misconfiguration immediately.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/channels/bulk-tags"), {

      body: JSON.stringify({ action: "add", tag: "totally-unknown-tag-x9z2" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 400, "unknown tag should reject");
  });
});

describe("bulk operations × service filter scoping", () => {

  /* Bulk endpoints operate on getVisibleChannels() - the intersection of "enabled" and "available under the current service filter." A user toggling the filter
   * and then running a bulk operation expects the bulk to apply to what they see, not to the full catalog. This is the UX contract that the d2ee7be / 80ad097
   * regression families violate when filter scoping breaks.
   *
   * The canonical fixture for this suite: enabledServices = ["hulu"]. abcnews has variants { cox, directv, hulu, sling, xfinity, yttv } and no site, so its
   * service tag set excludes "direct" - the filter is load-bearing for it. amcthrillers has only { sling, yttv } and no site, so it has no overlap with the
   * filter and falls out of getVisibleChannels. Channels with a "site" entry (e.g., abc) are excluded from this fixture because their "direct" service tag is
   * structurally always enabled by design, which would mask the filter-scoping rule under test.
   */

  test("bulk auto-number under an active service filter assigns numbers only to visible channels", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // Apply a filter that includes abcnews (has a hulu variant) and excludes amcthrillers (sling + yttv only). mutateEnabledServices owns both the persistence
    // write and the in-memory cache that getVisibleChannels reads through, so isPredefinedChannelDisabled / isChannelAvailableByService take effect immediately.
    await mutateEnabledServices(["hulu"]);

    const response = await fetch(urlFor("/config/channels/auto-number"), {

      body: JSON.stringify({ start: 100 }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "auto-number should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;
    const abcnews = persisted["abcnews"] as { channelNumber?: number } | undefined;
    const amcThrillers = persisted["amcthrillers"] as { channelNumber?: number } | undefined;

    assert.equal(typeof abcnews?.channelNumber, "number", "abcnews (hulu-available) must receive a channelNumber");
    assert.equal(amcThrillers?.channelNumber, undefined, "amcthrillers (filtered out) must not receive a channelNumber");
  });

  test("bulk HDHR toggle under an active service filter applies only to visible channels", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await mutateEnabledServices(["hulu"]);

    const response = await fetch(urlFor("/config/channels/hdhr-bulk"), {

      body: JSON.stringify({ enable: false }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "hdhr-bulk should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;
    const abcnews = persisted["abcnews"] as { hdhrEnabled?: boolean } | undefined;
    const amcThrillers = persisted["amcthrillers"] as { hdhrEnabled?: boolean } | undefined;

    assert.equal(abcnews?.hdhrEnabled, false, "abcnews (hulu-available) must have hdhrEnabled flipped to false");
    assert.equal(amcThrillers?.hdhrEnabled, undefined, "amcthrillers (filtered out) must remain at the predefined default (no entry needed)");
  });

  test("bulk tag assign under an active service filter applies only to visible channels", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await mutateEnabledServices(["hulu"]);

    /* "Sports" is a predefined tag in the active vocabulary. abcnews's predefined tags are ["News"] (Sports absent → would gain Sports if visible).
     * amcthrillers's predefined tags are ["Entertainment", "Movies"] (Sports absent → would gain Sports if visible). With the hulu filter, only abcnews is
     * visible; the bulk-add must touch abcnews and skip amcthrillers.
     */
    const response = await fetch(urlFor("/config/channels/bulk-tags"), {

      body: JSON.stringify({ action: "add", tag: "Sports" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "bulk-tags should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;
    const abcnews = persisted["abcnews"] as { tags?: string[] } | undefined;
    const amcThrillers = persisted["amcthrillers"] as { tags?: string[] } | undefined;

    assert.ok(abcnews?.tags?.includes("Sports"), "abcnews (hulu-available) must have Sports added");
    assert.equal(amcThrillers?.tags?.includes("Sports") ?? false, false, "amcthrillers (filtered out) must not have Sports added");
  });

  test("clearing the service filter after a scoped bulk reveals the original split between touched and untouched channels", async () => {

    /* Run an auto-number under a hulu-only filter, then clear the filter and inspect the listing. The visible channels (abcnews) carry the assigned numbers;
     * the previously-filtered-out channels (amcthrillers) carry no number. No orphan or duplicate entries appear - the bulk's filter scope was the only thing
     * limiting its reach. This pins the cross-cutting state-consistency invariant: filter scoping is a soft window into a coherent global state, not a way to
     * fork the state into two divergent halves.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await mutateEnabledServices(["hulu"]);

    await fetch(urlFor("/config/channels/auto-number"), {

      body: JSON.stringify({ start: 100 }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    // Clear the filter. Subsequent reads see the union of visible (numbered) and previously-filtered-out (un-numbered) channels.
    await mutateEnabledServices([]);

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;
    const abcnews = persisted["abcnews"] as { channelNumber?: number } | undefined;
    const amcThrillers = persisted["amcthrillers"] as { channelNumber?: number } | undefined;

    assert.equal(typeof abcnews?.channelNumber, "number", "abcnews keeps its assigned number after filter clear");
    assert.equal(amcThrillers?.channelNumber, undefined, "amcthrillers still has no channelNumber after filter clear (the bulk never touched it)");
  });
});
