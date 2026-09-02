/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * variant-display.test.ts: Integration coverage for variant-dropdown rendering under the service filter. The renderer at
 * src/routes/config/channels/table.ts (line ~951) marks each <option> with the `hidden` attribute when its service tag is filtered out by isServiceTagEnabled,
 * which is the contract that closes the d2ee7be regression class - "provider filter not applied to predefined variant dropdown options" rendered every option
 * regardless of the active filter, leaving users to select services they never enabled. The unit suite for services.ts asserts isServiceTagEnabled in isolation;
 * this suite asserts the renderer's actual emission of `hidden` against a real channel listing assembled by the production buildServiceGroups pipeline.
 *
 * Test 4 verifies directly (rather than assuming) what the dropdown renders as `selected` when the user's stored selection points at a service that is currently
 * filtered out. resolveServiceKey() in services.ts:969-991 falls back via findFirstEnabledVariant to the first variant whose tag is enabled; this is the
 * documented behavior, and the test asserts it. Note: a regression that re-shapes that fallback (e.g., to "leave the stored selection selected even when filtered"
 * or to "fall back to the canonical key without consulting the filter") would surface here as a different option carrying `selected`. The narrative comment in
 * Test 4 documents the contract so a future reader knows the assertion is by design, not chance.
 *
 * Why no harness `bootApp`: the dropdown rendering is a pure server-side string-producing function (generateChannelRowHtml), exactly like channels-table.test.ts.
 * Booting an HTTP listener would obscure the surface under test - we want to see the renderer's output, not Express's response shape.
 */
import { createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { mutateEnabledServices, setServiceSelection } from "../../../src/config/services.ts";
import assert from "node:assert/strict";
import { generateChannelRowHtml } from "../../../src/routes/config/channels/table.ts";
import { getProfiles } from "../../../src/config/profiles.ts";

/* Returns every <option> element from the variant dropdown rendered for the given channel key. Each entry exposes the option's value, its data-provider-tag,
 * the selected flag, and the hidden flag - the attributes the test cares about. Returns an empty array when no <select> is present (single-service channels
 * render a <span> instead, so the helper signals "not a dropdown" by returning empty rather than throwing).
 *
 * Implementation note: we parse the <option> tags via a substring scan on attribute occurrences rather than a full HTML parser. The renderer always emits
 * value, data-provider-tag, selected (optional), and hidden (optional) in a stable structure that the regex can rely on. A real HTML parser would be overkill
 * for a server-rendered string we already author.
 */
function parseDropdownOptions(displayRow: string): { hidden: boolean; selected: boolean; tag: string; value: string }[] {

  // Capture the body of the provider-select element. Anchor to the class attribute so a future renderer that adds another <select> elsewhere does not poison
  // the parse - the variant dropdown is uniquely identified by class="provider-select".
  const selectMatch = /<select class="provider-select"[^>]*>([\s\S]*?)<\/select>/.exec(displayRow);

  if(!selectMatch) {

    return [];
  }

  const body = selectMatch[1] ?? "";
  const optionPattern = /<option ([^>]*)>/g;
  const results: { hidden: boolean; selected: boolean; tag: string; value: string }[] = [];

  let optionMatch;

  while((optionMatch = optionPattern.exec(body)) !== null) {

    const attrs = optionMatch[1] ?? "";
    const valueMatch = /value="([^"]*)"/.exec(attrs);
    const tagMatch = /data-provider-tag="([^"]*)"/.exec(attrs);

    results.push({

      hidden: / hidden(\s|$)/.test(" " + attrs),
      selected: / selected(\s|$)/.test(" " + attrs),
      tag: tagMatch?.[1] ?? "",
      value: valueMatch?.[1] ?? ""
    });
  }

  return results;
}

describe("variant dropdown rendering under the service filter", () => {

  test("variant options outside enabledServices carry the hidden attribute; enabled options do not", async () => {

    /* The d2ee7be rule directly: with enabledServices = [hulu, sling], the dropdown for abcnews (a multi-service channel with cox/directv/hulu/sling/
     * xfinity/yttv variants and no `direct` always-on tag) must mark every variant whose tag is not in {hulu, sling} as hidden, and leave hulu and sling
     * unhidden. We use abcnews specifically because it has no `site` entry - the alphabetically-first service (cox) is its canonical, and cox-tagged variants
     * are subject to the filter exactly like every other variant. Channels with `direct` (e.g., abc, which has a site URL) would short-circuit isServiceTagEnabled
     * for their direct variant - that case is the subject of Test 3, not this one.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await mutateEnabledServices([ "hulu", "sling" ]);

    const { displayRow } = generateChannelRowHtml("abcnews", getProfiles());
    const options = parseDropdownOptions(displayRow);

    assert.ok(options.length > 0, "abcnews must render a variant dropdown - it has multiple service variants");

    // Partition the options into enabled-tag (hulu/sling) and filtered-out-tag groups, then assert hidden is absent on the first and present on the second.
    // Iterating the partitioned arrays surfaces the offending variant in the assertion message rather than a generic "false !== true."
    const enabled = new Set([ "hulu", "sling" ]);

    for(const option of options) {

      if(enabled.has(option.tag)) {

        assert.equal(option.hidden, false, "tag " + option.tag + " is in enabledServices but its option carries hidden");

        continue;
      }

      assert.equal(option.hidden, true, "tag " + option.tag + " is NOT in enabledServices but its option does not carry hidden");
    }
  });

  test("an empty enabledServices list (no filter) renders every variant without hidden", async () => {

    /* Filter cleared - isServiceTagEnabled returns true for every tag. Every variant option, regardless of tag, must render without the hidden attribute. This
     * is the "no filter" baseline that the previous test's hidden-on-some assertion is measured against; a renderer that emitted hidden unconditionally (e.g.,
     * a refactor that flipped the conditional) would pass Test 1 by accident on the filtered-out tags but fail Test 2 by emitting hidden everywhere.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await mutateEnabledServices([]);

    const { displayRow } = generateChannelRowHtml("abcnews", getProfiles());
    const options = parseDropdownOptions(displayRow);

    assert.ok(options.length > 0, "abcnews must render a variant dropdown");

    for(const option of options) {

      assert.equal(option.hidden, false, "with no filter, tag " + option.tag + " must render without hidden");
    }
  });

  test("the direct tag is never hidden even when enabledServices excludes everything else", async () => {

    /* By-design behavior: isServiceTagEnabled returns true for tag "direct" regardless of enabledServices, because direct-streaming sources (network-owned site
     * URLs like abc.com) do not require a subscription and should always be available. abc has a site
     * variant alongside its cox/directv/hulu/etc. variants; the site variant carries the `direct` tag. Even with enabledServices = [hulu] - which would
     * otherwise hide every option - the `direct` option must render without hidden.
     *
     * A regression that "tightened" isServiceTagEnabled to consult only enabledServices.includes() would silently break this: users who set a narrow service
     * filter would suddenly lose access to the direct sources in their dropdowns. This test fails loud at that moment.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await mutateEnabledServices(["hulu"]);

    const { displayRow } = generateChannelRowHtml("abc", getProfiles());
    const options = parseDropdownOptions(displayRow);

    const directOption = options.find((option) => option.tag === "direct");

    assert.ok(directOption, "abc must include a direct variant (the site URL)");
    assert.equal(directOption.hidden, false, "the direct variant must render without hidden regardless of the service filter");
  });

  test("when the stored selection's tag is filtered out, the dropdown selects the first-enabled variant", async () => {

    /* Documented fallback contract from src/config/services.ts resolveServiceKey() (lines 982-1004): when the stored serviceSelection points at a variant whose
     * tag is no longer in enabledServices, the resolver returns findFirstEnabledVariant(canonical) - the first variant in the group whose tag is enabled. The
     * renderer at table.ts:943 calls resolveServiceKey to compute currentSelection, and marks the matching option as `selected`. Therefore, with the user's
     * stored selection on yttv but enabledServices excluding yttv, the rendered dropdown's `selected` option must be the first-enabled variant, not yttv.
     *
     * The yttv option itself still appears in the dropdown - filtered options are hidden, not removed - and it must carry `hidden`. We assert both: the
     * `selected` flag landed on the fallback variant, and yttv's `hidden` flag is set. This asserts the resolver-renderer contract end-to-end so a future
     * refactor that changed either side independently (e.g., a renderer that ignored resolveServiceKey, or a resolver that returned the stale selection) would
     * surface the divergence.
     *
     * Variant-iteration order: buildServiceGroups pushes the canonical entry first, then appends the remaining variant keys sorted alphabetically
     * (services.ts:455, variantKeys.sort()). For abcnews the canonical is the cox entry (keyed as bare abcnews) followed by abcnews-directv, abcnews-hulu,
     * abcnews-sling, abcnews-xfinity, abcnews-yttv. With enabledServices = [hulu], findFirstEnabledVariant scans the variants array and returns the first one
     * whose tag is in enabledServices. The asserted fallback is therefore the hulu variant (abcnews-hulu).
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    // Seed: user selected the yttv variant, then filtered services to hulu only. The yttv selection is now stale relative to the filter.
    await setServiceSelection("abcnews", "abcnews-yttv");
    await mutateEnabledServices(["hulu"]);

    const { displayRow } = generateChannelRowHtml("abcnews", getProfiles());
    const options = parseDropdownOptions(displayRow);

    const selectedOption = options.find((option) => option.selected);

    assert.ok(selectedOption, "exactly one option must carry the selected flag");
    assert.equal(selectedOption.tag, "hulu", "the selected option must be the first-enabled variant (hulu) - resolveServiceKey's fallback");
    assert.equal(selectedOption.value, "abcnews-hulu", "the selected option's value must be the hulu variant key");

    // The user's stored selection (yttv) still appears as an option but must be hidden, since its tag is not in enabledServices.
    const yttvOption = options.find((option) => option.tag === "yttv");

    assert.ok(yttvOption, "the yttv option must still appear in the dropdown - filtered options are hidden, not removed");
    assert.equal(yttvOption.hidden, true, "the yttv option must carry hidden because its tag is filtered out");
    assert.equal(yttvOption.selected, false, "the yttv option must NOT carry selected when its tag is filtered out (selection moved to fallback)");
  });
});

describe("variant fallback contract under service filter", () => {

  /* These tests assert the variant fallback CONTRACT - the rule the renderer uses when the user's stored selection (or the canonical's own service tag) lands
   * outside the active enabledServices filter. The contract is implemented in src/config/services.ts:
   * resolveServiceKey() returns findFirstEnabledVariant(canonicalKey) when the resolved service tag is filtered out, and falls through to the canonical/selection
   * when no variant is enabled (services.ts:982-1004, 1012-1035). The renderer at table.ts:943 passes the resolveServiceKey output to mark `selected` on the matching
   * <option>. The cumulative observation: the variant cell does NOT emit any cell-level indicator (banner, pill, "unavailable" badge) - the only signal of fallback
   * is which option carries `selected` and which carry `hidden`.
   *
   * The tests below assert these aspects:
   *
   *   1. Negative observation - no cell-level "unavailable" indicator is emitted outside the dropdown. The fallback is silent at the cell level; the dropdown's
   *      `selected` flag is the only signal.
   *   2. Positive control - when the stored selection's tag IS enabled, no fallback applies and the stored selection wins.
   *   3. Canonical-tag-filtered, no user selection: alphabetically-first enabled variant wins (the resolveServiceKey "no selection + canonical filtered" branch).
   *   4. Direct-as-fallback - with a filter that excludes every non-direct tag, the canonical's `direct` tag is always enabled and wins as the fallback.
   *
   * Test 1 confirms directly that the cell does NOT emit an "unavailable" badge. If a future redesign adds a badge, the assertion fails and the renderer's
   * contract change must be intentional (the test is updated alongside the renderer).
   *
   * The renderer's behavior matches the design intent: binding follows the service filter while identity persists. There is no cell-level "unavailable"
   * indicator - the dropdown's `selected` and `hidden` flags are the only signal.
   */

  test("no cell-level unavailable indicator: variant cell renders only the dropdown when stored selection is filtered out", async () => {

    /* Investigation assertion. With the user's stored selection on yttv and enabledServices = [hulu], the variant cell <td> emits the dropdown <select>...</select> and
     * a "no available services" <em> placeholder (always present, hidden when isAvailableByService is true), but NO additional badge, banner, or text node
     * indicating "your selection is unavailable." We assert this with negative substring checks on the sliced variant <td>: no class="unavailable", no
     * class="fallback-", and no static <span class="provider-name"> - any of which would signal a cell-level indicator. A future renderer that adds an
     * "unavailable" indicator would trip one of these checks and require the test to be updated alongside the renderer change - which is the point: changes to
     * the cell-level contract should be deliberate.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await setServiceSelection("abcnews", "abcnews-yttv");
    await mutateEnabledServices(["hulu"]);

    const { displayRow } = generateChannelRowHtml("abcnews", getProfiles());

    /* Slice out the service column from the row. We locate the <td> that contains the provider-select element, using the select's class="provider-select" as
     * the structural landmark. The tempered pattern - (?:[^<]|<(?!\/td>))*? on each side - matches anything except a closing </td>, ensuring the slice stays
     * within a single <td>...</td> boundary and never crosses into an adjacent column. Anchoring on the select's class keeps this resilient to incidental
     * column-content changes elsewhere in the row.
     */
    const selectMatch = /<td[^>]*>(?:[^<]|<(?!\/td>))*?<select class="provider-select"[^>]*>[\s\S]*?<\/select>(?:[^<]|<(?!\/td>))*?<\/td>/.exec(displayRow);

    assert.ok(selectMatch, "service column must contain a <select class=\"provider-select\">");

    const td = selectMatch[0];

    /* No cell-level fallback indicator is emitted. The cell carries the no-provider-label <em>, the <select>, and nothing else cell-scoped (no
     * <span class="unavailable">, no badge pills, no inline text indicating fallback). We use negative substring assertions rather than a positive enumeration
     * because the negative is the contract: anything user-visible at the cell level outside the dropdown would be a behavior change worth reviewing.
     */
    assert.equal(td.includes("class=\"unavailable\""), false, "no element carries class=\"unavailable\" inside the variant cell");
    assert.equal(td.includes("class=\"fallback-"), false, "no element carries a class starting with \"fallback-\" inside the variant cell");
    assert.equal(td.includes("<span class=\"provider-name\""), false, "the static provider-name span is not emitted for a multi-service channel");

    // Cross-check: the dropdown still emits both the user's stale selection AND the fallback option (visibility distinguished by hidden attribute, not by removal).
    const options = parseDropdownOptions(displayRow);
    const selected = options.find((o) => o.selected);
    const yttv = options.find((o) => o.tag === "yttv");

    assert.ok(selected, "exactly one option carries selected");
    assert.equal(selected.tag, "hulu", "the selected option is the first-enabled variant");
    assert.ok(yttv, "the user's stored selection still appears in the dropdown");
    assert.equal(yttv.hidden, true, "the user's stored selection is hidden, not removed");
  });

  test("positive control: when the stored selection's tag is enabled, the stored selection wins (no fallback)", async () => {

    /* Counterfactual to the fallback contract. The user has selected abcnews-hulu; enabledServices = [hulu] makes that selection valid. The dropdown's `selected`
     * flag must land on abcnews-hulu, not on any other variant. This asserts the "fallback only fires when the stored selection is filtered out" branch of
     * resolveServiceKey - if the resolver erroneously fell back even when the selection was enabled, this test would fail loudly.
     *
     * Using the same channel and the same enabledServices as the canonical-fallback test (the "no user selection + canonical's tag filtered out" test,
     * enumerated as #3 in the suite header) - only the stored selection differs. The dropdown rendering is otherwise identical, so the contract assertion is precise:
     * selection-enabled vs selection-filtered is the single variable.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await setServiceSelection("abcnews", "abcnews-hulu");
    await mutateEnabledServices(["hulu"]);

    const { displayRow } = generateChannelRowHtml("abcnews", getProfiles());
    const options = parseDropdownOptions(displayRow);
    const selected = options.find((o) => o.selected);

    assert.ok(selected, "exactly one option carries selected");
    assert.equal(selected.value, "abcnews-hulu", "the stored selection wins when its tag is enabled");
    assert.equal(selected.tag, "hulu", "the selected option's tag matches the enabled service");
  });

  test("no user selection + canonical's tag filtered out: alphabetically-first enabled variant wins", async () => {

    /* The "no selection" branch of resolveServiceKey: when serviceSelections.get(canonicalKey) is undefined and the canonical's own tag is filtered out, the
     * resolver returns findFirstEnabledVariant(canonicalKey). For abcnews (no site entry), the canonical's URL is the cox URL (cox is the alphabetically-first
     * service among abcnews's variants); its tag is "cox". With enabledServices = [hulu], the canonical is filtered out. findFirstEnabledVariant scans the
     * group's variants in alphabetical order (set by buildServiceGroups at services.ts:455) and returns the first whose tag is in enabledServices.
     *
     * The variants for abcnews are: abcnews (canonical, cox tag - cox is the alphabetically-first service so it keys the bare canonical and emits no
     * abcnews-cox variant), then abcnews-directv, abcnews-hulu, abcnews-sling, abcnews-xfinity, abcnews-yttv. Iteration encounters abcnews first (cox -
     * filtered), then abcnews-directv (filtered), then abcnews-hulu (enabled). Therefore the dropdown's `selected` lands on abcnews-hulu.
     *
     * Note: this test uses no setServiceSelection call; serviceSelections is empty for abcnews. The resolver branches on the absence of a selection, not on the
     * filter status of an explicit selection - distinct from test 1's "selection-filtered" branch.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await mutateEnabledServices(["hulu"]);

    const { displayRow } = generateChannelRowHtml("abcnews", getProfiles());
    const options = parseDropdownOptions(displayRow);
    const selected = options.find((o) => o.selected);

    assert.ok(selected, "exactly one option carries selected");
    assert.equal(selected.value, "abcnews-hulu", "with no stored selection and canonical filtered out, the first-enabled variant (alphabetical) wins");
    assert.equal(selected.tag, "hulu", "the selected option's tag matches the enabled service");
  });

  test("direct-as-fallback: when the filter excludes every non-direct tag, the canonical's direct tag wins", async () => {

    /* The interaction between the always-on `direct` rule and the fallback resolver. abc has a site URL on its canonical
     * (tag = direct), and the direct tag is enabled regardless of enabledServices content (isServiceTagEnabled returns true for "direct" unconditionally). With a
     * filter that names a tag matching no abc variant (e.g., "nonexistent"), every non-direct variant is filtered out but the canonical (direct) is not. With the
     * user's stored selection on a filtered tag (abc-yttv), resolveServiceKey returns findFirstEnabledVariant which scans alphabetically and returns the canonical
     * abc - the only enabled variant remaining.
     *
     * This asserts the renderer-resolver collaboration around the direct rule: a filter that excludes all paid-service tags does NOT leave the user with no
     * usable variant - the network-owned site URL remains accessible. A regression that "tightened" isServiceTagEnabled to drop the direct carve-out would
     * surface here as the dropdown selecting nothing or selecting the stale yttv (depending on which side of the contract regressed first).
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    await setServiceSelection("abc", "abc-yttv");
    await mutateEnabledServices(["nonexistent-service-tag"]);

    const { displayRow } = generateChannelRowHtml("abc", getProfiles());
    const options = parseDropdownOptions(displayRow);
    const selected = options.find((o) => o.selected);

    assert.ok(selected, "exactly one option carries selected when direct serves as the fallback");
    assert.equal(selected.tag, "direct", "the selected option's tag is direct (the only enabled variant)");
    assert.equal(selected.value, "abc", "the selected option is the canonical abc entry whose URL is the network site");

    // The user's filtered-out yttv selection appears with hidden but does NOT carry selected.
    const yttv = options.find((o) => o.tag === "yttv");

    assert.ok(yttv, "the yttv option still appears in the dropdown (hidden, not removed)");
    assert.equal(yttv.hidden, true, "the yttv option carries hidden because its tag is filtered out");
    assert.equal(yttv.selected, false, "the yttv option does not carry selected; the direct fallback won");
  });
});
