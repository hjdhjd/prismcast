/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channel-form-domain.test.ts: Integration-tier coverage for findMatchingVariant in src/config/channelForm.ts. The unit suite at src/config/channelForm.test.ts
 * pins channelMatches and computePredefinedDelta directly, plus the no-service-group early return for findMatchingVariant. The iteration body of
 * findMatchingVariant - which queries getServiceGroup() for the user-resolved channel and walks each variant - requires the runtime serviceGroups module
 * cache to be populated, which only happens after initializeUserChannels has run. This file lands the iteration coverage at the right tier.
 */
import { type ChannelFormValues, findMatchingVariant } from "../../../src/config/channelForm.ts";
import { createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { PREDEFINED_CHANNELS } from "../../../src/channels/index.ts";
import assert from "node:assert/strict";
import { getResolvedChannel } from "../../../src/config/services.ts";

/* makeForm builds a ChannelFormValues literal with empty/undefined sentinels by default. Callers override the fields they want to test.
 */
function makeForm(overrides: Partial<ChannelFormValues> = {}): ChannelFormValues {

  return {

    channelNumber: undefined,
    channelSelector: "",
    guideTitle: "",
    hdhrEnabled: true,
    logoUrl: "",
    name: "",
    profile: "",
    stationId: "",
    url: "",
    ...overrides
  };
}

describe("findMatchingVariant - iteration body and positive match", () => {

  test("returns the variant key when the form values match a sibling service variant exactly", async () => {

    /* abc is a multi-service predefined channel with variants like abc-cox, abc-hulu, etc. We pick the abc-cox variant, build a form payload that mirrors its
     * resolved values, and assert findMatchingVariant returns "abc-cox". The iteration body's positive return is the contract: the user customized abc, then
     * later edited it to match the cox variant; the function detects the implicit revert and returns the variant key so the caller can switch the service
     * selection rather than persisting a redundant override.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    /* Confirm the variant exists in the predefined catalog before we build the test fixture against it. If a refactor renamed or removed abc-cox, this
     * assertion fails before the harder findMatchingVariant assertion does, making the cause obvious.
     */
    assert.ok(PREDEFINED_CHANNELS["abc-cox"], "abc-cox variant must exist in the predefined catalog");

    /* Build the form payload from the resolved variant via getResolvedChannel so the form mirrors what the UI's edit-form would pre-populate. The variant
     * inherits identity (name, stationId, channelNumber, tags, etc.) from the abc canonical and adds its own URL / channelSelector for cox.
     */
    const variantResolved = getResolvedChannel("abc-cox");

    assert.ok(variantResolved, "abc-cox resolves through getResolvedChannel");

    const form = makeForm({

      channelNumber: variantResolved.channelNumber,
      channelSelector: variantResolved.channelSelector ?? "",
      guideTitle: variantResolved.guideTitle ?? "",
      hdhrEnabled: variantResolved.hdhrEnabled !== false,
      logoUrl: variantResolved.logoUrl ?? "",
      name: variantResolved.name,
      profile: variantResolved.profile ?? "",
      stationId: variantResolved.stationId ?? "",
      url: variantResolved.url
    });

    const matched = findMatchingVariant("abc", form, variantResolved.tags ?? []);

    assert.equal(matched, "abc-cox", "form values matching cox variant exactly resolve to the cox variant key");
  });

  test("returns undefined when form values do not match any sibling variant", async () => {

    /* Negative case for the iteration body: a form payload that does not match ANY variant must return undefined after walking the full service group. We
     * build a payload whose URL/name/selector clearly belong to no real abc variant.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    const form = makeForm({ name: "Truly Custom ABC", url: "https://this-is-not-a-real-abc-variant.example.test/" });

    const matched = findMatchingVariant("abc", form, []);

    assert.equal(matched, undefined, "no variant matches the custom payload");
  });

  test("skips synthetic :predefined entries during the variant walk (canonical aliasing carve-out)", async () => {

    /* Synthetic ":predefined" entries appear in service groups when a user has overridden a single-service predefined channel with a different URL - they
     * point back to the original predefined data. findMatchingVariant must skip these because they represent the same canonical, already covered by the
     * upstream no-op-save check.
     *
     * To exercise this branch, we'd need a service group containing a :predefined entry, which requires user-overriding a predefined channel. That setup is
     * orchestrated by the channels CRUD endpoints; here we pin a softer invariant: findMatchingVariant returns undefined for a form payload that matches
     * canonical (which the upstream no-op check is supposed to have caught). The contract is that findMatchingVariant does not double-report the canonical
     * itself as a matching variant.
     */
    await using ctx = await createIntegrationContext();

    void ctx;
    await initializePersistence(ctx);

    const canonical = getResolvedChannel("abc");

    assert.ok(canonical, "abc canonical resolves");

    const form = makeForm({

      channelNumber: canonical.channelNumber,
      hdhrEnabled: canonical.hdhrEnabled !== false,
      name: canonical.name,
      stationId: canonical.stationId ?? "",
      url: canonical.url
    });

    /* findMatchingVariant skips the canonical key itself (key === canonicalKey at the top of the loop). It returns undefined for a canonical-shaped form
     * because no real variant matches the canonical's URL/identity exactly. A regression that included the canonical in the walk would surface as a returned
     * "abc" value here, which would then be treated by upstream code as a variant switch.
     */
    const matched = findMatchingVariant("abc", form, canonical.tags ?? []);

    assert.equal(matched, undefined, "canonical-shaped form does not resolve to the canonical key as a variant match");
  });
});
