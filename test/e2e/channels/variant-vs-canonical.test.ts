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
import { createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { getAllChannels, mutateChannels } from "../../../src/config/userChannels.ts";
import { PREDEFINED_CHANNELS } from "../../../src/channels/index.ts";
import assert from "node:assert/strict";
import { setServiceSelection } from "../../../src/config/services.ts";

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

  /* The canonical resolution rule documented at src/channels/index.ts lines 23-33: if "site" exists in services, the canonical always gets the site URL;
   * otherwise, the alphabetically-first service key (computed via Object.keys().sort(), not source-order) becomes canonical. This is the rule that 7f82b03
   * regressed - "Fox defaulting to Cox instead of fox.com" - and re-pinning it as an integration-level invariant means a future flattener change that breaks
   * the rule fails this suite immediately rather than waiting for a user-visible report.
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
