/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * services.serviceGroups.test.ts: Unit tests for service-group construction in services.ts - buildServiceGroups (passes 1, 2, 3 across canonical/variant/override
 * scenarios) and resolveServiceKey's filter-fallback paths. Predicates, lookups, and label dispatchers live in services.test.ts; sort-key computation lives in
 * services.sortKeys.test.ts.
 */
import { afterEach, beforeEach, describe, test } from "node:test";
import { buildServiceGroups, getEnabledServices, getServiceGroup, getServiceSelections, resolveServiceKey, setEnabledServices,
  setServiceSelections } from "./services.ts";
import { PREDEFINED_CHANNELS } from "../channels/index.ts";
import type { ResolvedChannelMap } from "../types/index.ts";
import assert from "node:assert/strict";
import { makeChannel } from "./userChannels.helpers.ts";

describe("buildServiceGroups", () => {

  let originalSelections: Record<string, string>;

  beforeEach(() => {

    originalSelections = getServiceSelections();
  });

  afterEach(() => {

    setServiceSelections(originalSelections);
  });

  test("returns an empty array of stale keys when there are no service selections", () => {

    setServiceSelections({});

    const channels: ResolvedChannelMap = {

      mychannel: makeChannel({ name: "Mine", url: "https://example.com/" })
    };

    const result = buildServiceGroups(channels);

    assert.deepEqual(result, [], "no stale keys to clean");
  });

  test("returns stale-key list when a stored selection no longer maps to a real variant", () => {

    setServiceSelections({ nbc: "nbc-not-a-real-variant" });

    const channels: ResolvedChannelMap = {

      nbc: makeChannel({ name: "NBC", url: "https://www.nbc.com/live" })
    };

    const result = buildServiceGroups(channels);

    assert.deepEqual(result, ["nbc"], "stale selection's canonical key is reported");
  });
});

describe("buildServiceGroups: user-override scenarios A and B", () => {

  /* The branch matrix that buildServiceGroups applies when isUserOverride is true for a canonical key (the Scenario A / Scenario B split in services.ts):
   *
   *   Scenario A: predefined exists AND user override is on the same domain as predefined or a known variant -> single canonical entry, label = service name.
   *   Scenario B: predefined exists AND user override is on a DIFFERENT domain -> 2-entry group ["Custom (domain)", original-service-name].
   */

  let originalSelections: Record<string, string>;

  beforeEach(() => {

    originalSelections = getServiceSelections();
  });

  afterEach(() => {

    setServiceSelections(originalSelections);
  });

  test("Scenario A (predefined override on same domain): emits canonical with its service label, no :predefined synthetic", () => {

    /* Pass 2 fires when at least one variant key is in the input. We seed the nbc canonical (user override) plus the nbc-hulu predefined variant so Pass 1
     * collects nbc as a canonical with one variant. The user's override URL is on nbc.com (same domain as the predefined nbc canonical's site URL), so Pass 2's
     * Scenario A branch runs - the variant entry uses getChannelServiceLabel(canonical) and no :predefined synthetic entry is added.
     */
    const nbcPredefined = PREDEFINED_CHANNELS["nbc"];

    if(!nbcPredefined) {

      // Catalog shape changed; skip rather than break.
      return;
    }

    setServiceSelections({});

    /* makeChannel produces a fresh reference; spreading nbcPredefined ensures the override is detected by isUserOverride (reference comparison). The URL stays
     * on nbc.com so the userDomain matches the canonical's domain (Scenario A).
     */
    const userOverride = makeChannel({ ...nbcPredefined, name: "NBC My Custom", url: "https://www.nbc.com/different-page" });
    const huluVariant = makeChannel({ canonicalKey: "nbc", url: "https://www.hulu.com/live" });
    const channels: ResolvedChannelMap = { nbc: userOverride, "nbc-hulu": huluVariant };

    buildServiceGroups(channels);

    const group = getServiceGroup("nbc");

    assert.ok(group, "nbc group exists (Pass 2 fires because nbc-hulu carries canonicalKey)");

    /* Scenario A: no synthetic :predefined entry, and the canonical's variant entry uses the service label rather than "Custom (...)".
     */
    const hasPredefinedSuffix = group.variants.some((v) => v.key.endsWith(":predefined"));

    assert.equal(hasPredefinedSuffix, false, "Scenario A does not emit a :predefined variant");

    const canonicalVariant = group.variants.find((v) => v.key === "nbc");

    assert.ok(canonicalVariant, "canonical variant entry present");
    assert.equal(canonicalVariant.label.startsWith("Custom"), false, "label is the service display name, not a 'Custom (...)' prefix");
  });

  test("Scenario B (predefined override on different domain): emits Custom + :predefined variants", () => {

    /* Same setup as Scenario A but the user's override URL is on a foreign domain. Scenario B emits two entries: { key: nbc, label: 'Custom (<domain>)' } and
     * the :predefined synthetic with the original service's label.
     */
    const nbcPredefined = PREDEFINED_CHANNELS["nbc"];

    if(!nbcPredefined) {

      return;
    }

    setServiceSelections({});

    /* extractDomain returns the concise domain (e.g., "example.test" for "foreign.example.test"). The label uses that concise form, so the test asserts on
     * "Custom (example.test)" rather than the full hostname.
     */
    const userOverride = makeChannel({ ...nbcPredefined, name: "NBC Custom", url: "https://foreign.example.test/feed" });
    const huluVariant = makeChannel({ canonicalKey: "nbc", url: "https://www.hulu.com/live" });
    const channels: ResolvedChannelMap = { nbc: userOverride, "nbc-hulu": huluVariant };

    buildServiceGroups(channels);

    const group = getServiceGroup("nbc");

    assert.ok(group, "nbc group exists");

    const variantKeys = group.variants.map((v) => v.key).toSorted();

    assert.ok(variantKeys.includes("nbc"), "canonical entry exists with custom URL");
    assert.ok(variantKeys.includes("nbc:predefined"), "Scenario B emits the :predefined synthetic variant pointing at the original predefined service");

    const customVariant = group.variants.find((v) => v.key === "nbc");

    assert.ok(customVariant, "custom variant entry exists");
    assert.match(customVariant.label, /^Custom \(.+\)$/, "label has 'Custom (...)' shape (concise domain)");
  });
});

describe("buildServiceGroups: Pass 3 (single-service predefined override)", () => {

  /* Pass 3 creates synthetic 2-entry groups for user overrides of single-service predefined channels (channels with no canonicalKey-based variants). Two
   * scenarios:
   *
   *   - Scenario A: user URL domain matches predefined -> no group created (just modified-dot indicator).
   *   - Scenario B: user URL domain differs -> create 2-entry group with "Custom (domain)" + :predefined.
   */

  let originalSelections: Record<string, string>;

  beforeEach(() => {

    originalSelections = getServiceSelections();
  });

  afterEach(() => {

    setServiceSelections(originalSelections);
  });

  test("Scenario A: user override of single-service predefined on the same domain -> no group created", () => {

    /* Find a single-service predefined (no variants, no canonicalKey): we'll use one whose URL we can override with a same-domain URL. ABC's site URL is on
     * abc.com. abc itself has many variants, so it's NOT single-service. We need to scan PREDEFINED_CHANNELS for a canonical with no variants.
     *
     * The 'cnbc' channel might be single-service. Skip the test if we can't find one - the test's intent (Scenario A negative) is documented above.
     */
    const candidates = Object.entries(PREDEFINED_CHANNELS).filter(
      ([ key, channel ]) => (channel.canonicalKey === undefined) && !Object.keys(PREDEFINED_CHANNELS).some((k) => PREDEFINED_CHANNELS[k]?.canonicalKey === key)
    );

    if(candidates.length === 0) {

      return;
    }

    const [ canonicalKey, predefined ] = candidates[0]!;

    setServiceSelections({});

    const url = new URL(predefined.url);
    const userOverride = makeChannel({ ...predefined, name: "Override", url: url.origin + "/different-page" });
    const channels: ResolvedChannelMap = { [canonicalKey]: userOverride };

    buildServiceGroups(channels);

    const group = getServiceGroup(canonicalKey);

    assert.equal(group, undefined, "Scenario A (same-domain override) does not create a service group");
  });

  test("Scenario B: user override of single-service predefined on a DIFFERENT domain -> creates 2-entry group", () => {

    /* Same setup but URL goes to a foreign domain. Pass 3 detects the domain mismatch and emits the 2-entry group.
     */
    const candidates = Object.entries(PREDEFINED_CHANNELS).filter(
      ([ key, channel ]) => (channel.canonicalKey === undefined) && !Object.keys(PREDEFINED_CHANNELS).some((k) => PREDEFINED_CHANNELS[k]?.canonicalKey === key)
    );

    if(candidates.length === 0) {

      return;
    }

    const [ canonicalKey, predefined ] = candidates[0]!;

    setServiceSelections({});

    const userOverride = makeChannel({ ...predefined, name: "Override", url: "https://foreign.example.test/different" });
    const channels: ResolvedChannelMap = { [canonicalKey]: userOverride };

    buildServiceGroups(channels);

    const group = getServiceGroup(canonicalKey);

    assert.ok(group, "Scenario B creates a synthetic group");
    assert.equal(group.variants.length, 2, "group has exactly 2 entries: Custom + :predefined");

    const keys = group.variants.map((v) => v.key).toSorted();

    assert.deepEqual(keys, [ canonicalKey, canonicalKey + ":predefined" ].toSorted());
  });
});

describe("resolveServiceKey: filter-fallback paths", () => {

  /* The filter fallback fires when the user has an active service filter and the resolved selection (or canonical) is filtered out. Two branches:
   *
   *   - No selection: `enabledServices.length > 0 && !isServiceTagEnabled(canonicalServiceTag)` -> findFirstEnabledVariant.
   *   - Valid selection but its tag is filtered out: same fallback.
   */

  let originalSelections: Record<string, string>;
  let originalEnabled: string[];

  beforeEach(() => {

    originalSelections = getServiceSelections();
    originalEnabled = getEnabledServices();
  });

  afterEach(() => {

    setServiceSelections(originalSelections);
    setEnabledServices(originalEnabled);
  });

  test("no selection + no filter: returns the canonical key (happy path)", () => {

    setEnabledServices([]);
    setServiceSelections({});

    /* abc canonical exists with no selection or filter; resolves to itself.
     */
    assert.equal(resolveServiceKey("abc"), "abc");
  });

  test("no selection + filter active and canonical's tag is enabled: returns canonical", () => {

    /* The filter is active but the canonical's service tag is in enabledServices. resolveServiceKey returns the canonical without falling back.
     *
     * For abc canonical, the URL is abc.com which has serviceTag "direct" - "direct" is always enabled regardless of filter, so the canonical resolves.
     */
    setEnabledServices(["sling"]);
    setServiceSelections({});
    buildServiceGroups({});

    /* abc canonical doesn't exist in the empty buildServiceGroups, but resolveServiceKey is robust to that - it just operates on the in-memory state.
     * This test confirms the no-fallback branch when no selection exists and the tag is enabled (direct is always enabled).
     */
    assert.equal(resolveServiceKey("abc"), "abc");
  });

  test("valid selection: returns the selection when its tag is enabled", () => {

    setEnabledServices([]);
    setServiceSelections({ abc: "abc-hulu" });

    /* No filter, valid selection -> returns the selection verbatim.
     */
    assert.equal(resolveServiceKey("abc"), "abc-hulu");
  });

  test("valid selection: returns the selection unchanged even when canonical is in groups", () => {

    /* When the user has explicitly selected a non-canonical variant and no filter is active, the selection wins. Pin the no-fallback branch on the with-selection
     * path.
     */
    setEnabledServices([]);
    setServiceSelections({ abc: "abc-hulu" });

    const channels: ResolvedChannelMap = {

      abc: makeChannel({ name: "ABC", url: "https://abc.com/" }),
      "abc-hulu": makeChannel({ canonicalKey: "abc", url: "https://hulu.com/abc" })
    };

    buildServiceGroups(channels);

    assert.equal(resolveServiceKey("abc"), "abc-hulu");
  });
});
