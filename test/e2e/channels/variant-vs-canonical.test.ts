/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * variant-vs-canonical.test.ts: Integration coverage for variant resolution invariants. A predefined channel can have multiple service variants (e.g., abc
 * has abc-hulu, abc-yttv, etc.); the user selects which service to use via setServiceSelection, and the resolved channel layers identity from the canonical
 * with binding from the variant. Switching the active service must update the binding (url, channelSelector, channelSelection) without disturbing the
 * identity (name, channelNumber, stationId, tags, etc.).
 *
 * The unit tier covers the resolver functions in isolation; this suite verifies the full end-to-end flow: set selection -> resolve -> identity preserved,
 * binding updated.
 */
import { bootApp, createIntegrationContext, initializePersistence, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { clearChannelOverrides, getAllChannels, mutateChannels } from "../../../src/config/userChannels.ts";
import { describe, test } from "node:test";
import { getServiceSelections, mutateServiceSelections, setServiceSelection } from "../../../src/config/services.ts";
import { PREDEFINED_CHANNELS } from "../../../src/channels/index.ts";
import assert from "node:assert/strict";

describe("variant resolution invariants", () => {

  test("setting a service selection switches the resolved binding without touching identity", async () => {

    /* Set up: abc canonical has predefined variants. Without a selection, abc resolves to the canonical. After setServiceSelection(abc, abc-hulu), it should
     * resolve to the abc-hulu variant - same identity (name="ABC"), different binding (url points at hulu's player).
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    // Capture the canonical resolution for the identity baseline.
    const beforeMap = getAllChannels();
    const canonicalAbc = beforeMap["abc"];

    assert.ok(canonicalAbc, "abc canonical should exist");
    assert.equal(canonicalAbc.name, "ABC", "canonical name");

    // Switch to the hulu variant.
    await setServiceSelection("abc", "abc-hulu");

    const afterMap = getAllChannels();
    const resolved = afterMap["abc"];

    assert.ok(resolved, "abc still resolves after service selection");
    assert.equal(resolved.name, canonicalAbc.name, "identity (name) preserved across service switch");
    assert.notEqual(resolved.url, canonicalAbc.url, "binding (url) changed after service switch (now hulu's URL)");
    assert.match(resolved.url, /hulu/i, "the new url should reference hulu");
  });

  test("a user identity override on the canonical applies regardless of which variant is active", async () => {

    /* Identity-on-canonical means "applies to every service variant." The user sets channelNumber=7 on the canonical entry; switching the active service to
     * any variant must still report channelNumber=7 in the resolved view because identity inherits from canonical regardless of variant.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    // Set channelNumber=7 on the canonical entry.
    await mutateChannels((data) => {

      data.channels["abc"] = { channelNumber: 7 };
    });

    // Switch to a variant.
    await setServiceSelection("abc", "abc-hulu");

    const resolved = getAllChannels()["abc"];

    assert.ok(resolved, "abc resolves");
    assert.equal(resolved.channelNumber, 7, "user identity override on canonical applies through variant resolution");
  });

  test("clearing a service selection (selection === canonical key) removes the override", async () => {

    /* Selection-equals-canonical-key is the documented "no override" convention; setServiceSelection treats it as removing the entry from serviceSelections.
     * The on-disk file should have no selection for the canonical after the clear.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    // First set a non-canonical selection.
    await setServiceSelection("abc", "abc-hulu");

    // Then clear by setting selection back to canonical.
    await setServiceSelection("abc", "abc");

    // After clearing, abc should resolve as the canonical (no variant binding active).
    const resolved = getAllChannels()["abc"];

    assert.ok(resolved, "abc still resolves after clear");
    assert.doesNotMatch(resolved.url, /hulu/i, "url should no longer reference hulu after clearing the selection");
  });
});

describe("default canonical resolution for multi-service predefined channels", () => {

  /* The canonical resolution rule documented in the "Canonical resolution rules" comment in src/channels/index.ts: if "site" exists in services, the canonical
   * always gets the site URL; otherwise, the alphabetically-first service key (computed via Object.keys().sort(), not source-order) becomes canonical.
   * Re-pinning this as an integration-level invariant means a future flattener change that breaks the rule fails this suite immediately rather than surfacing
   * as a user-visible misrouted canonical URL.
   *
   * Tests work against the public PREDEFINED_CHANNELS surface (the resolved output of flattenChannelDefinitions) - the flattener itself is not exported, but its
   * effective output is what every consumer sees. The chosen channels exercise: (a) site-presence wins; (b) alphabetical-first wins among many services with
   * different starting letters; (c) alphabetical-first wins between services that share starting letters and could be ordered either way - confirming the rule
   * is a sort, not a "first declared" / "first inserted" / "site-prefix-by-coincidence" alternative.
   */

  test("site URL wins canonical when present (abc has site + service variants)", async () => {

    /* abc declares { cox, directv, hulu, site, sling, spectrum, xfinity, yttv } - alphabetically, "cox" would win without the site rule. Asserting the canonical
     * URL is the site's value pins the "site beats alphabetical" branch, which is the highest-leverage rule because it's what makes network-owned URLs
     * (abc.com/watch-live, fox.com, etc.) outrank cable-provider URLs that would otherwise coincidentally sort first.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    const abc = PREDEFINED_CHANNELS["abc"];

    assert.ok(abc, "abc canonical exists in the catalog");
    assert.equal(abc.url, "https://abc.com/watch-live", "abc canonical URL must be the site URL, not any cable-service URL");
  });

  test("alphabetically-first service wins canonical when no site exists (abcnews resolves to cox)", async () => {

    /* abcnews declares { cox, directv, hulu, sling, xfinity, yttv } and has no site. "cox" sorts first across multiple distinct starting letters - if the
     * resolver picked any other service (directv, hulu, etc.) the canonical URL would mismatch. The variants for the non-canonical services exist on the flat
     * map alongside the canonical; we sanity-check one to confirm the variant entries were emitted (and thus that the catalog's flattening path is exercising
     * the multi-service branch).
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    const abcnews = PREDEFINED_CHANNELS["abcnews"];
    const abcnewsHulu = PREDEFINED_CHANNELS["abcnews-hulu"];

    assert.ok(abcnews, "abcnews canonical exists");
    assert.equal(abcnews.url, "https://watchtv.cox.com/listings", "abcnews canonical URL must be cox URL (cox sorts alphabetically first)");

    assert.ok(abcnewsHulu, "non-canonical hulu variant should exist as a separate flat entry");
    assert.equal(abcnewsHulu.canonicalKey, "abcnews", "variant entry must reference its canonical");
  });

  test("alphabetical-first principle holds for two-service channels (amcthrillers picks sling over yttv)", async () => {

    /* amcthrillers declares only { sling, yttv } - the smallest non-trivial multi-service shape. "sling" sorts before "yttv" alphabetically. If the resolver
     * were ever broken to use source-order or a different rule, this case would surface a different canonical URL. Pinning the two-service path explicitly
     * complements the multi-service abcnews case: a regression that breaks for cardinality > 2 but happens to work at exactly 2 (or vice versa) is caught by
     * having both shapes covered.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    const amcThrillers = PREDEFINED_CHANNELS["amcthrillers"];
    const amcThrillersYttv = PREDEFINED_CHANNELS["amcthrillers-yttv"];

    assert.ok(amcThrillers, "amcthrillers canonical exists");
    assert.equal(amcThrillers.url, "https://watch.sling.com/dashboard/grid_guide/grid_guide_a_z",
      "amcthrillers canonical URL must be the sling URL (sling sorts alphabetically before yttv)");

    // The yttv variant must be emitted as a separate flat entry, never overwriting the canonical. A regression that picked yttv as canonical and emitted no
    // variants for the same service would surface here as a missing key.
    assert.ok(amcThrillersYttv, "amcthrillers-yttv variant must exist");
    assert.equal(amcThrillersYttv.canonicalKey, "amcthrillers");
    assert.equal(amcThrillersYttv.url, "https://tv.youtube.com/live", "the non-canonical yttv variant carries the yttv URL on the variant entry");
  });
});

describe("setServiceSelection: persistence and delete branch", () => {

  /* The mutator routes through mutateChannels and writes to channels.json. The canonical-equals-service-key branch deletes the entry from serviceSelections
   * rather than storing a redundant key->same-key mapping. We assert the on-disk shape directly via readPersistedJson.
   */

  test("setting a non-canonical variant persists the selection to channels.json", async () => {

    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await setServiceSelection("abc", "abc-hulu");

    const persisted = await readPersistedJson(ctx, "channels.json") as { serviceSelections?: Record<string, string> };

    assert.deepEqual(persisted.serviceSelections, { abc: "abc-hulu" }, "variant selection persisted to disk");
  });

  test("setting selection to the canonical key DELETES the entry from serviceSelections (no redundant mapping)", async () => {

    /* setServiceSelection's canonical-equals-key delete branch. When the user selects the canonical service explicitly, the function removes the
     * selection rather than storing a redundant key->key entry. Confirm the on-disk entry disappears.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await setServiceSelection("abc", "abc-hulu");

    /* Confirm the prerequisite (entry on disk) before exercising the delete.
     */
    const before = await readPersistedJson(ctx, "channels.json") as { serviceSelections?: Record<string, string> };

    assert.deepEqual(before.serviceSelections, { abc: "abc-hulu" });

    await setServiceSelection("abc", "abc");

    const after = await readPersistedJson(ctx, "channels.json") as { serviceSelections?: Record<string, string> };

    /* The serviceSelections key is omitted from the on-disk shape entirely once empty (per prepareChannelsForWrite's conditional emit).
     */
    assert.equal(after.serviceSelections, undefined, "empty serviceSelections is not emitted to disk");
  });

  test("post-write cache hydration: getServiceSelections reflects the persisted state immediately after the mutate", async () => {

    /* setServiceSelection routes through mutateChannels, which re-hydrates the serviceSelections cache from the same normalized data it just wrote. This pins
     * that after setServiceSelection resolves, the in-memory getServiceSelections() returns the just-written state, demonstrating the cache was hydrated as
     * part of the mutate.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await setServiceSelection("abc", "abc-hulu");

    assert.deepEqual(getServiceSelections(), { abc: "abc-hulu" }, "in-memory cache reflects the persisted write");
  });
});

describe("mutateServiceSelections: bulk variant", () => {

  test("applies multiple selection changes in a single atomic write", async () => {

    /* The bulk variant (mutateServiceSelections in services.ts) is documented to coalesce N changes into one write. We exercise it with two distinct canonical
     * channels and verify both selections land on disk.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await mutateServiceSelections({ abc: "abc-hulu", nbc: "nbc-yttv" });

    const persisted = await readPersistedJson(ctx, "channels.json") as { serviceSelections?: Record<string, string> };

    assert.deepEqual(persisted.serviceSelections, { abc: "abc-hulu", nbc: "nbc-yttv" });
  });

  test("mixed delete/set entries: entries equal to canonical key are removed; non-canonical entries are stored", async () => {

    /* The bulk variant honors the same canonical-equals-service-key delete convention as the single mutator. We seed two prior selections, then run a bulk
     * update that deletes one (selection === canonical) and updates the other.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await mutateServiceSelections({ abc: "abc-hulu", nbc: "nbc-yttv" });

    /* Bulk update: delete abc (selection equals canonical) and update nbc to a different variant.
     */
    await mutateServiceSelections({ abc: "abc", nbc: "nbc-sling" });

    const persisted = await readPersistedJson(ctx, "channels.json") as { serviceSelections?: Record<string, string> };

    assert.deepEqual(persisted.serviceSelections, { nbc: "nbc-sling" }, "abc removed via canonical-equals-key; nbc updated to new variant");
  });
});

describe("clearChannelOverrides: dual-delete with canonical-precedence return", () => {

  /* The helper deletes both the canonical and the active variant entries in one mutateChannels call. The returned StoredChannel preserves canonical-takes-
   * precedence semantics so callers can compute downstream effects (e.g., playlist reload hint).
   */

  test("returns the canonical entry when it exists; deletes both canonical and active variant", async () => {

    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await setServiceSelection("abc", "abc-hulu");

    /* Seed canonical + variant entries directly via mutateChannels so we have something to delete.
     */
    let returned: unknown;

    await mutateChannels((data) => {

      data.channels["abc"] = { channelNumber: 7 };
      data.channels["abc-hulu"] = { canonicalKey: "abc", channelSelector: "ABC-CUSTOM" };
    });

    await mutateChannels((data) => {

      returned = clearChannelOverrides(data.channels, "abc");
    });

    /* Canonical takes precedence in the returned entry: even though both entries existed, the returned value is the canonical (channelNumber=7), not the variant.
     */
    assert.deepEqual(returned, { channelNumber: 7 });

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;

    assert.equal("abc" in persisted, false, "canonical entry deleted");
    assert.equal("abc-hulu" in persisted, false, "active variant entry deleted");
  });

  test("returns the variant entry when canonical is absent (variant-only fallback)", async () => {

    /* Variant-only revert case: the user has no canonical-stored override but does have a variant override. clearChannelOverrides falls back to returning the
     * variant entry so callers still see a non-undefined result.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await setServiceSelection("abc", "abc-hulu");

    let returned: unknown;

    await mutateChannels((data) => {

      data.channels["abc-hulu"] = { canonicalKey: "abc", channelSelector: "ABC-VARIANT-ONLY" };
    });

    await mutateChannels((data) => {

      returned = clearChannelOverrides(data.channels, "abc");
    });

    assert.equal((returned as { channelSelector?: string }).channelSelector, "ABC-VARIANT-ONLY", "variant entry returned when canonical is absent");
  });

  test("returns undefined when neither canonical nor active variant exists", async () => {

    /* No prior overrides for the channel. The helper still runs the deletes (they're no-ops via Reflect.deleteProperty) and returns undefined.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    let returned: unknown = "unset";

    await mutateChannels((data) => {

      returned = clearChannelOverrides(data.channels, "abc");
    });

    assert.equal(returned, undefined);
  });
});

describe("mutateEnabledServices: post-write cache hydration", () => {

  /* After mutateEnabledServices writes config.json, the in-memory enabledServices cache is hydrated from the just-persisted value.
   * This suite pins that hydration via the service-filter HTTP route plus a subsequent in-memory getEnabledServices() read.
   */

  test("after mutateEnabledServices via the service-filter route, getEnabledServices reflects the new value", async () => {

    /* Mutate via the public route, then assert the in-memory cache reads back the new state. If mutateEnabledServices' post-write cache hydration regressed, the cache
     * would lag behind disk and this assertion would fail.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);
    const response = await fetch(urlFor("/config/service-filter"), {

      body: JSON.stringify({ enabledServices: [ "hulu", "yttv" ] }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "service-filter update should succeed");

    /* In-memory cache: getEnabledServices imported from services.ts reads the same module-state cache that mutateEnabledServices hydrates.
     */
    const { getEnabledServices } = await import("../../../src/config/services.ts");

    assert.deepEqual(getEnabledServices().toSorted(), [ "hulu", "yttv" ].toSorted(), "cache reflects the persisted state");
  });
});
