/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * browse.test.ts: HTTP-level integration coverage for the browse-modal apply endpoint (POST /config/channels/modify). The browse modal dispatches a batch of
 * entries with per-entry actions (add | enable | switch | remove); this suite exercises the endpoint through a real Express boot and asserts the on-disk shape
 * that each action produces. It is the sibling of crud.test.ts (single-channel form CRUD) and tags.test.ts (tag vocabulary): all three drive the same channels.json
 * store, but this suite asserts the browse-modal-specific contract that the others do not touch.
 *
 * The guarantees asserted are as follows. (1) buildUserChannel's variant branch: when an add resolves to a service variant of an existing predefined canonical
 * (canonicalKey set), the stored record carries binding-only fields (canonicalKey, url, channelSelector) and intentionally DROPS identity fields (name, stationId)
 * because identity is canonical-only. (2) A standalone add (no canonicalKey) preserves the submitted stationId on the identity-owning canonical record, and a batch entry
 * whose name yields no generatable key is skipped with a per-entry error while the rest of the batch still applies - the batch is not aborted. (3) The remove
 * action clears the service selection and, via hasAlternativeService, keeps a multi-service channel with an alternative service enabled or, when the channel
 * then resolves to the removed service (a single-service predefined channel, or a multi-service one whose default service is the removed one), disables the
 * predefined channel by adding it to disabledPredefined. (4) A batch's enables and disables of predefined channels land as one configuration write,
 * dispatched once, which leaves the disabled list sorted. (5) A switch or an enable on the canonical's own service clears the selection and writes no variant,
 * so a user override's custom URL survives the channel store's normalizer, while a switch to any other service selects the variant keyed by that service,
 * writing it as a user channel when no entry holds it.
 *
 * Fixtures are real predefined channels from src/channels/index.ts, with the user entries a row stores beside them: "abc" (multi-service, canonical is its own
 * "site" so the canonical tag is "direct"), "bloombergoriginals" (single-service - only YouTube TV, so its canonical tag is "yttv" and removing "yttv" leaves no
 * alternative) and "amcthrillers" (multi-service, its canonical is its sling service beside a yttv variant, so with no selection removing "sling" leaves it on
 * the removed service, and once overridden on hulu.com its group carries a :predefined entry for the sling service).
 */
import { PREDEFINED_SUFFIX, getServiceGroup, getServiceSelection, getServiceTagForChannel } from "../../../src/config/services.ts";
import { bootApp, createIntegrationContext, initializePersistence, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { disablePredefinedChannels, mutateChannels } from "../../../src/config/userChannels.ts";
import assert from "node:assert/strict";
import { registerConfigChangeHandler } from "../../../src/config/reactivity.ts";

/**
 * Reads the persisted disabledPredefined list from config.json, tolerating the file's absence. The browse endpoint makes its one configuration write through
 * updatePredefinedChannels, which writes nothing for a batch with no key to enable or disable, so a test that asserts a channel was NOT disabled must treat a
 * missing config.json as an empty disabled list rather than a read error.
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

describe("POST /config/channels/modify - remove keeps a channel that resolves to another service and disables one that does not", () => {

  test("removing one service from a multi-service channel clears the selection and does not disable the channel", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    /* Seed an explicit selection to the DirecTV variant of "abc", then remove the Hulu service. The remove handler clears data.serviceSelections["abc"] and
     * resolves the channel with no selection stored, so resolveServiceKey falls back to the canonical key itself, whose own service tag ("direct") differs from
     * the removed service ("hulu"), so the channel is NOT disabled. The endpoint always clears the selection, so on disk "abc" reverts to its canonical default.
     * The persisted outcome we assert: the selection is gone and "abc" is not in disabledPredefined.
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

  test("removing a multi-service channel's default service with no selection disables it, as the browse modal previews", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    /* "amcthrillers" defaults to its sling service beside a yttv variant and carries no selection, so with the sling service removed the channel still resolves
     * to sling. The browse lineup previews that unchecking it disables the channel, and the endpoint disables it.
     */
    assert.deepEqual(getServiceGroup("amcthrillers")?.variants.map((variant) => [ variant.key, variant.tag ]),
      [ [ "amcthrillers", "sling" ], [ "amcthrillers-yttv", "yttv" ] ], "precondition: the catalog's amcthrillers group");

    const response = await fetch(urlFor("/config/channels/modify"), {

      body: JSON.stringify({ channels: [
        { action: "remove", canonicalKey: "amcthrillers", name: "AMC Thrillers", serviceSlug: "sling" }
      ] }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "remove should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const disabled = await readDisabledPredefined(ctx);

    assert.ok(disabled.includes("amcthrillers"), "a multi-service channel must be disabled when its default service is removed with no selection");
  });

  test("removing the currently-selected service of a multi-service channel reverts to its default and stays enabled", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    /* Seed the selection to the Hulu variant of "abc", a multi-service predefined channel with several service variants plus its own direct canonical. The seed
     * mutation commits and repopulates the module service-selection cache with abc -> abc-hulu.
     */
    await mutateChannels((data) => {

      data.serviceSelections["abc"] = "abc-hulu";
    });

    /* Removing the CURRENTLY-selected service must revert "abc" to an alternative variant or its canonical default and leave it enabled. The remove handler clears
     * the selection and resolves the channel with no selection stored rather than through the committed module cache, so it falls back to the canonical "direct"
     * service, whose tag differs from the removed "hulu" - the channel is not disabled. Resolving with no selection is what keeps the channel enabled here:
     * resolving through the committed cache would bring back abc-hulu and wrongly disable the channel.
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

describe("POST /config/channels/modify - a batch's enables and disables land as one configuration write", () => {

  test("a batch that enables one predefined channel and disables another writes the disabled list once, sorted", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await disablePredefinedChannels([ "abc", "nbc" ]);

    let dispatches = 0;

    registerConfigChangeHandler("channels.disabledPredefined", async () => {

      dispatches++;

      return [];
    });

    /* The batch enables "abc" and removes the only service of "bloombergoriginals", so the route's one updatePredefinedChannels call takes "abc" off the
     * disabled list and adds "bloombergoriginals". The writer's set holds the kept "nbc" ahead of the added key, so a sorted list in the file shows the write
     * sorted it.
     */
    const response = await fetch(urlFor("/config/channels/modify"), {

      body: JSON.stringify({ channels: [
        { action: "enable", canonicalKey: "abc", name: "ABC", serviceSlug: "hulu" },
        { action: "remove", canonicalKey: "bloombergoriginals", name: "Bloomberg Originals", serviceSlug: "yttv" }
      ] }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "the batch should succeed; body: " + (await response.clone().text()).slice(0, 200));
    assert.equal(dispatches, 1, "the enables and disables were one write, dispatched once");
    assert.deepEqual(await readDisabledPredefined(ctx), [ "bloombergoriginals", "nbc" ], "the file's disabled list holds the kept and the disabled keys, sorted");
  });
});

describe("POST /config/channels/modify - switch and enable select the canonical on its own service and the variant keyed by any other", () => {

  // The guide URLs a switch from the Hulu, Sling and YouTube TV lineups carries, and a user override's custom URL on the hulu.com domain at a path of its own.
  const HULU_GUIDE_URL = "https://www.hulu.com/live";
  const SLING_GUIDE_URL = "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z";
  const YTTV_GUIDE_URL = "https://tv.youtube.com/live";
  const CUSTOM_URL = "https://www.hulu.com/watch/amc-thrillers";

  // The switch the Hulu lineup's submit sends for amcthrillers.
  const switchToHulu = { action: "switch", canonicalKey: "amcthrillers", channelSelector: "AMC Thrillers", name: "AMC Thrillers", serviceSlug: "hulu",
    url: HULU_GUIDE_URL };

  interface PersistedChannels {

    channels: Record<string, { canonicalKey?: string; url?: string } | undefined>;
    message: string;
    selections: Record<string, string>;
  }

  interface PostOptions {

    ctx: Parameters<typeof readPersistedJson>[0];
    entry: Record<string, string>;
    urlFor: (path: string) => string;
  }

  /**
   * Posts one browse entry to the modify endpoint, asserts the request succeeds, and reads the response's message and the channels.json the endpoint's write
   * leaves on disk, whose channel records stand at the top level beside the service selections.
   * @param options - The request and the app it goes to.
   * @param options.ctx - The integration context whose data directory holds channels.json.
   * @param options.entry - The browse entry, in the shape the modal's submit loop sends.
   * @param options.urlFor - The booted app's URL builder.
   * @returns The response's message and the persisted channel records and service selections.
   */
  async function postEntry({ ctx, entry, urlFor }: PostOptions): Promise<PersistedChannels> {

    const response = await fetch(urlFor("/config/channels/modify"), {

      body: JSON.stringify({ channels: [entry] }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "the request should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const body = await response.json() as { message?: string };
    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;

    return {

      channels: persisted as PersistedChannels["channels"],
      message: body.message ?? "",
      selections: (persisted["serviceSelections"] ?? {}) as Record<string, string>
    };
  }

  /**
   * Overrides amcthrillers with a custom URL on hulu.com, stores a service selection beside it, and asserts the user override group the write builds and the
   * selection the store keeps.
   * @param selection - The service selection to store for amcthrillers.
   */
  async function overrideOnHulu(selection: string): Promise<void> {

    await mutateChannels((data) => {

      data.channels["amcthrillers"] = { url: CUSTOM_URL };
      data.serviceSelections["amcthrillers"] = selection;
    });

    assert.deepEqual(getServiceGroup("amcthrillers")?.variants.map((variant) => [ variant.key, variant.tag ]),
      [ [ "amcthrillers", "hulu" ], [ "amcthrillers" + PREDEFINED_SUFFIX, "sling" ], [ "amcthrillers-yttv", "yttv" ] ], "precondition: the user override group");
    assert.equal(getServiceSelection("amcthrillers"), selection, "precondition: the store keeps the selection");
  }

  test("a switch to a user override's own service keeps its custom URL and clears its variant selection", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    /* The override's yttv variant is selected, so the Hulu lineup shows the channel as a switch. A hulu variant written for it would share the custom URL's
     * domain, and the normalizer would move the selection to that variant and strip the custom URL, so the switch selects the canonical instead.
     */
    await overrideOnHulu("amcthrillers-yttv");

    const { channels, message, selections } = await postEntry({ ctx, entry: switchToHulu, urlFor });

    assert.ok(message.includes("Switched 1 channel."), "the response should report one switched channel: " + message);
    assert.equal(channels["amcthrillers"]?.url, CUSTOM_URL, "the override keeps its custom URL");
    assert.equal("amcthrillers-hulu" in channels, false, "no variant is written on the canonical's own service");
    assert.equal("amcthrillers" in selections, false, "the selection is cleared, which selects the canonical");
  });

  test("a switch to a user override's own service keeps its custom URL and clears a stored :predefined selection", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // A stored :predefined selection reads as the sling service, so the Hulu lineup shows the channel as a switch, which selects the canonical.
    await overrideOnHulu("amcthrillers" + PREDEFINED_SUFFIX);

    const { channels, message, selections } = await postEntry({ ctx, entry: switchToHulu, urlFor });

    assert.ok(message.includes("Switched 1 channel."), "the response should report one switched channel: " + message);
    assert.equal(channels["amcthrillers"]?.url, CUSTOM_URL, "the override keeps its custom URL");
    assert.equal("amcthrillers-hulu" in channels, false, "no variant is written on the canonical's own service");
    assert.equal("amcthrillers" in selections, false, "the selection is cleared, which selects the canonical");
  });

  test("an enable on a disabled channel's own service enables it and writes no variant", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await disablePredefinedChannels(["bloombergoriginals"]);

    assert.equal(getServiceTagForChannel("bloombergoriginals"), "yttv", "precondition: the canonical's own service is YouTube TV");

    const enable = { action: "enable", canonicalKey: "bloombergoriginals", channelSelector: "Bloomberg Originals", name: "Bloomberg Originals", serviceSlug: "yttv",
      url: YTTV_GUIDE_URL };
    const { channels, message, selections } = await postEntry({ ctx, entry: enable, urlFor });

    assert.ok(message.includes("Switched 1 channel."), "the response should report one switched channel: " + message);
    assert.equal((await readDisabledPredefined(ctx)).includes("bloombergoriginals"), false, "the enable takes the channel off the disabled list");
    assert.equal("bloombergoriginals-yttv" in channels, false, "no variant is written on the canonical's own service");
    assert.equal("bloombergoriginals" in selections, false, "no selection is stored, which selects the canonical");
  });

  test("a switch to the canonical's own service clears the selection and leaves a duplicate an earlier switch stored", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    /* An earlier switch wrote amcthrillers-sling, a user variant on the canonical's own sling service, and the yttv variant is the stored selection. A switch from the
     * Sling lineup selects the canonical, not the duplicate the variant key names, and the duplicate stays stored for the channel's dropdown.
     */
    await mutateChannels((data) => {

      data.channels["amcthrillers-sling"] = { canonicalKey: "amcthrillers", channelSelector: "AMC Thrillers", url: SLING_GUIDE_URL };
      data.serviceSelections["amcthrillers"] = "amcthrillers-yttv";
    });

    assert.deepEqual(getServiceGroup("amcthrillers")?.variants.map((variant) => [ variant.key, variant.tag ]),
      [ [ "amcthrillers", "sling" ], [ "amcthrillers-sling", "sling" ], [ "amcthrillers-yttv", "yttv" ] ], "precondition: the group with the duplicate");
    assert.equal(getServiceSelection("amcthrillers"), "amcthrillers-yttv", "precondition: the store keeps the selection");

    const switchToSling = { action: "switch", canonicalKey: "amcthrillers", channelSelector: "AMC Thrillers", name: "AMC Thrillers", serviceSlug: "sling",
      url: SLING_GUIDE_URL };
    const { channels, message, selections } = await postEntry({ ctx, entry: switchToSling, urlFor });

    assert.ok(message.includes("Switched 1 channel."), "the response should report one switched channel: " + message);
    assert.equal("amcthrillers" in selections, false, "the selection is cleared, which selects the canonical");
    assert.equal(channels["amcthrillers-sling"]?.url, SLING_GUIDE_URL, "the duplicate stays stored");
  });

  test("a switch to a service the channel lacks writes that service's variant and selects it", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // The catalog's amcthrillers offers its sling canonical and its yttv variant alone, so a switch from the Hulu lineup writes a hulu variant and selects it.
    assert.deepEqual(getServiceGroup("amcthrillers")?.variants.map((variant) => [ variant.key, variant.tag ]),
      [ [ "amcthrillers", "sling" ], [ "amcthrillers-yttv", "yttv" ] ], "precondition: the catalog's amcthrillers group");

    const { channels, message, selections } = await postEntry({ ctx, entry: switchToHulu, urlFor });
    const variant = channels["amcthrillers-hulu"];

    assert.ok(message.includes("Switched 1 channel."), "the response should report one switched channel: " + message);
    assert.ok(variant, "the switch writes a hulu variant");
    assert.equal(variant.canonicalKey, "amcthrillers", "the new variant binds to its canonical");
    assert.equal(variant.url, HULU_GUIDE_URL, "the new variant carries the switch's guide URL");
    assert.equal(selections["amcthrillers"], "amcthrillers-hulu", "the new variant is selected");
  });

  test("a switch to a service whose variant the catalog holds selects that variant and writes no entry", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // amcthrillers-yttv is a catalog variant in the channel's group, so a switch from the YouTube TV lineup selects it and stores no user entry for it.
    const switchToYttv = { action: "switch", canonicalKey: "amcthrillers", channelSelector: "AMC Thrillers", name: "AMC Thrillers", serviceSlug: "yttv",
      url: YTTV_GUIDE_URL };
    const { channels, message, selections } = await postEntry({ ctx, entry: switchToYttv, urlFor });

    assert.ok(message.includes("Switched 1 channel."), "the response should report one switched channel: " + message);
    assert.equal(selections["amcthrillers"], "amcthrillers-yttv", "the catalog variant is selected");
    assert.equal("amcthrillers-yttv" in channels, false, "no user entry is written for a catalog variant");
  });

  test("a switch selects the variant keyed by the browsed service when a user's own variant on that service stands beside it", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    /* abc-custom is a user variant whose URL is on hulu.com, so abc's group holds it ahead of the catalog's abc-hulu on the hulu service. A switch from the
     * Hulu lineup selects abc-hulu, the variant its key names, and leaves the user's own variant as it stands.
     */
    await mutateChannels((data) => {

      data.channels["abc-custom"] = { canonicalKey: "abc", url: "https://www.hulu.com/watch/abc-custom" };
    });

    assert.deepEqual(getServiceGroup("abc")?.variants.filter((variant) => (variant.tag === "hulu")).map((variant) => variant.key), [ "abc-custom", "abc-hulu" ],
      "precondition: the user's variant stands ahead of the catalog's on the hulu service");

    const switchAbc = { action: "switch", canonicalKey: "abc", channelSelector: "ABC", name: "ABC", serviceSlug: "hulu", url: HULU_GUIDE_URL };
    const { channels, message, selections } = await postEntry({ ctx, entry: switchAbc, urlFor });

    assert.ok(message.includes("Switched 1 channel."), "the response should report one switched channel: " + message);
    assert.equal(selections["abc"], "abc-hulu", "the variant keyed by the browsed service is selected");
    assert.equal(channels["abc-custom"]?.url, "https://www.hulu.com/watch/abc-custom", "the user's own variant stands as it was");
  });

  test("a switch to a service whose guide shares a channel's domain under another service tag writes that service's variant", async () => {

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    /* A user channel on www.youtube.com carries the "direct" tag, while the YouTube TV guide's URL shares its youtube.com domain under the "yttv" tag. The
     * services differ by tag, so a switch from the YouTube TV lineup writes the yttv variant and selects it.
     */
    await mutateChannels((data) => {

      data.channels["examplelive"] = { name: "Example Live", url: "https://www.youtube.com/@example/live" };
    });

    assert.equal(getServiceTagForChannel("examplelive"), "direct", "precondition: the user channel's own tag");

    const switchExample = { action: "switch", canonicalKey: "examplelive", channelSelector: "Example Live", name: "Example Live", serviceSlug: "yttv",
      url: YTTV_GUIDE_URL };
    const { channels, message, selections } = await postEntry({ ctx, entry: switchExample, urlFor });

    assert.ok(message.includes("Switched 1 channel."), "the response should report one switched channel: " + message);
    assert.equal(channels["examplelive-yttv"]?.url, YTTV_GUIDE_URL, "the yttv variant is written");
    assert.equal(selections["examplelive"], "examplelive-yttv", "the yttv variant is selected");
  });
});
