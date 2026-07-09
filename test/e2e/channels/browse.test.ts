/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * browse.test.ts: HTTP-level integration coverage for the browse-modal apply endpoint (POST /config/channels/modify). The browse modal dispatches a batch of
 * entries with per-entry actions (add | enable | switch | remove); this suite exercises the endpoint through a real Express boot and asserts the on-disk shape
 * that each action produces. It is the sibling of crud.test.ts (single-channel form CRUD) and tags.test.ts (tag vocabulary): all three drive the same channels.json
 * store, but this suite pins the browse-modal-specific contract that the others do not touch.
 *
 * Three invariants are pinned. (1) buildUserChannel's variant branch: when an add resolves to a service variant of an existing predefined canonical (canonicalKey
 * set), the stored record carries binding-only fields (canonicalKey, url, channelSelector) and intentionally DROPS identity fields (name, stationId) because
 * identity is canonical-only. (2) A standalone add (no canonicalKey) preserves the submitted stationId on the identity-owning canonical record, and a batch entry
 * whose name yields no generatable key is skipped with a per-entry error while the rest of the batch still applies - the batch is not aborted. (3) The remove
 * action clears the service selection and, via resolveServiceKey, either survives (a multi-service channel with an alternative service is not disabled) or, when the
 * resolved service is still the removed service (a single-service predefined channel), disables the predefined channel by adding it to disabledPredefined.
 *
 * Fixtures use only real predefined channels from src/channels/index.ts: "abc" (multi-service, canonical is its own "site" so the canonical tag is "direct") and
 * "bloombergoriginals" (single-service - only YouTube TV, so its canonical tag is "yttv" and removing "yttv" leaves no alternative).
 */
import { bootApp, createIntegrationContext, initializePersistence, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { mutateChannels } from "../../../src/config/userChannels.ts";

/**
 * Reads the persisted disabledPredefined list from config.json, tolerating the file's absence. The browse endpoint writes config.json only when it actually
 * disables a channel (disablePredefinedChannels short-circuits on an empty key set), so a test that asserts a channel was NOT disabled must treat a missing
 * config.json as an empty disabled list rather than a read error.
 * @param ctx - The integration context whose data directory holds config.json.
 * @returns The persisted disabled-predefined keys, or an empty array when config.json does not exist.
 */
async function readDisabledPredefined(ctx: Parameters<typeof readPersistedJson>[0]): Promise<string[]> {

  try {

    const config = await readPersistedJson(ctx, "config.json") as { channels?: { disabledPredefined?: unknown } };
    const list = config.channels?.disabledPredefined;

    return Array.isArray(list) ? list.filter((key): key is string => typeof key === "string") : [];
  } catch {

    // config.json is absent because nothing wrote it - equivalent to no channels disabled.
    return [];
  }
}

describe("POST /config/channels/modify - add builds variant vs standalone records", () => {

  test("an add resolving to a variant of an existing canonical stores binding-only fields and drops identity", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    /* "ABC" generates the base key "abc", which is an existing predefined canonical. With a serviceSlug set, the endpoint forms the variant key "abc-custom" and
     * calls buildUserChannel with canonicalKey "abc" - the variant branch. The submitted name and stationId are identity fields and must NOT survive onto the
     * variant record; only the binding fields (canonicalKey, url, channelSelector) are stored.
     */
    const response = await fetch(urlFor("/config/channels/modify"), {

      body: JSON.stringify({ channels: [
        { action: "add", channelSelector: "ABCSEL", name: "ABC", serviceSlug: "custom", stationId: "777777", url: "https://example.test/abc-variant" }
      ] }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "modify should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;
    const record = persisted["abc-custom"];

    assert.ok(record && (typeof record === "object"), "the variant record should be persisted under the derived variant key");

    const variant = record as Record<string, unknown>;

    assert.equal(variant["canonicalKey"], "abc", "the variant must bind to its canonical via canonicalKey");
    assert.equal(variant["url"], "https://example.test/abc-variant", "the variant must carry the submitted binding url");
    assert.equal(variant["channelSelector"], "ABCSEL", "the variant must carry the submitted channelSelector");
    assert.equal("name" in variant, false, "the variant must drop the identity name field - identity is canonical-only");
    assert.equal("stationId" in variant, false, "the variant must drop the identity stationId field - identity is canonical-only");
  });

  test("a standalone add preserves the submitted stationId on the identity-owning canonical record", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    /* "Test Standalone News" generates the base key "test-standalone-news", which matches no predefined canonical. With no canonicalKey, the endpoint stores a
     * standalone canonical that owns its own identity, so the submitted stationId is preserved (unlike the variant branch, which drops it).
     */
    const response = await fetch(urlFor("/config/channels/modify"), {

      body: JSON.stringify({ channels: [
        { action: "add", channelSelector: "TSN", name: "Test Standalone News", stationId: "424242", url: "https://example.test/standalone" }
      ] }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "modify should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;
    const record = persisted["test-standalone-news"];

    assert.ok(record && (typeof record === "object"), "the standalone canonical should be persisted under its generated key");

    const channel = record as Record<string, unknown>;

    assert.equal(channel["name"], "Test Standalone News", "the standalone canonical must own its identity name");
    assert.equal(channel["stationId"], "424242", "the standalone canonical must preserve the submitted stationId");
    assert.equal(channel["url"], "https://example.test/standalone", "the standalone canonical must carry the submitted url");
    assert.equal("canonicalKey" in channel, false, "a standalone canonical must not carry a canonicalKey binding");
  });

  test("a batch entry whose name yields no key is skipped with a per-entry error while the sibling good entry still applies", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    /* The bad entry is listed FIRST so that if the baseKey-failure guard aborted the batch, the good sibling that follows would never apply. "!!!" sanitizes to a
     * non-empty name (so it passes the name-required and url checks) but generateChannelKey yields an empty key, tripping the per-entry "Could not generate key"
     * guard, which continues to the next entry. The good sibling must therefore land on disk, and the response must report exactly one add.
     */
    const response = await fetch(urlFor("/config/channels/modify"), {

      body: JSON.stringify({ channels: [
        { action: "add", name: "!!!", url: "https://example.test/nokey" },
        { action: "add", name: "Batch Good Channel", stationId: "555555", url: "https://example.test/good" }
      ] }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "modify should succeed even when one entry is skipped; body: " + (await response.clone().text()).slice(0, 200));

    const body = await response.json() as { message?: string };

    assert.ok(typeof body.message === "string", "the response must carry a summary message");
    assert.ok(body.message.includes("Added 1 channel."), "exactly one channel should be added - the bad entry is skipped, not fatal: " + body.message);

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;
    const good = persisted["batch-good-channel"];

    assert.ok(good && (typeof good === "object"), "the good sibling must apply even though it followed a skipped entry");
    assert.equal((good as Record<string, unknown>)["stationId"], "555555", "the good sibling must carry its submitted stationId");
    assert.equal("" in persisted, false, "the empty-key bad entry must not be persisted under an empty key");
  });
});

describe("POST /config/channels/modify - remove reverts a multi-service channel or disables a single-service one", () => {

  test("removing one service from a multi-service channel clears the selection and does not disable the channel", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    /* Seed an explicit selection to the DirecTV variant of "abc", then remove the Hulu service. During the mutation resolveServiceKey still sees the DirecTV
     * selection (its tag "directv" differs from the removed "hulu"), so the resolved service is an alternative and the channel is NOT disabled. The endpoint always
     * clears the selection, so on disk "abc" reverts to its canonical default. The persisted outcome we pin: the selection is gone and "abc" is not in
     * disabledPredefined.
     */
    await mutateChannels((data) => {

      data.serviceSelections["abc"] = "abc-directv";
    });

    const response = await fetch(urlFor("/config/channels/modify"), {

      body: JSON.stringify({ channels: [
        { action: "remove", canonicalKey: "abc", name: "ABC", serviceSlug: "hulu" }
      ] }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "remove should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const body = await response.json() as { message?: string };
    const message = body.message ?? "";

    assert.ok(message.includes("Reverted 1 channel."), "the response should report one reverted channel: " + message);

    const persisted = await readPersistedJson(ctx, "channels.json") as { serviceSelections?: Record<string, unknown> };
    const selections = persisted.serviceSelections ?? {};

    assert.equal("abc" in selections, false, "the service selection for the multi-service channel must be cleared by remove");

    const disabled = await readDisabledPredefined(ctx);

    assert.equal(disabled.includes("abc"), false, "a multi-service channel with an alternative service must not be disabled by remove");
  });

  test("removing the only service from a single-service predefined channel disables it", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    /* "bloombergoriginals" is a single-service predefined channel offered only via YouTube TV, so its canonical tag is "yttv" and no alternative variant exists.
     * Removing the "yttv" service leaves resolveServiceKey resolving back to the same service, so the endpoint disables the predefined channel by adding it to
     * disabledPredefined in config.json.
     */
    const response = await fetch(urlFor("/config/channels/modify"), {

      body: JSON.stringify({ channels: [
        { action: "remove", canonicalKey: "bloombergoriginals", name: "Bloomberg Originals", serviceSlug: "yttv" }
      ] }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "remove should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const disabled = await readDisabledPredefined(ctx);

    assert.ok(disabled.includes("bloombergoriginals"), "a single-service predefined channel must be disabled when its only service is removed");
  });

  test("removing the currently-selected service of a multi-service channel reverts to its default and stays enabled", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    /* Seed the selection to the Hulu variant of "abc", a multi-service predefined channel (it also has yttv/sling/xfinity/cox variants plus its own direct
     * canonical). The seed mutation commits and repopulates the module service-selection cache with abc -> abc-hulu.
     */
    await mutateChannels((data) => {

      data.serviceSelections["abc"] = "abc-hulu";
    });

    /* Removing the CURRENTLY-selected service must revert "abc" to an alternative variant or its canonical default and leave it enabled. The remove handler clears
     * the selection on the in-transaction draft and resolves against that draft (not the committed module cache), so the resolver observes the cleared selection and
     * falls back to the canonical "direct" service, whose tag differs from the removed "hulu" - the channel is not disabled. This pins the fix for the stale-cache
     * regression recorded in the project bug ledger (class A): reading the stale committed cache here would resolve back to abc-hulu and wrongly disable the channel.
     */
    await fetch(urlFor("/config/channels/modify"), {

      body: JSON.stringify({ channels: [
        { action: "remove", canonicalKey: "abc", name: "ABC", serviceSlug: "hulu" }
      ] }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    const disabled = await readDisabledPredefined(ctx);

    assert.equal(disabled.includes("abc"), false,
      "abc reverts to its canonical default and stays enabled - removing a channel's currently-selected service must not disable a multi-service channel");
  });
});
