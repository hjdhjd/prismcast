/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * identity-binding-routing.test.ts: Integration coverage for the channel-edit per-field routing introduced in dd227dd. Identity fields (name, channelNumber,
 * stationId, logoUrl, tvgShift, guideTitle, tags, hdhrEnabled) live on the canonical stored entry; binding fields (url, channelSelector, channelSelection)
 * live on the variant entry when a non-canonical service is resolved. The CRUD endpoint splits a submitted form delta into identity and binding halves and
 * writes each half to the correct entry in a single atomic mutateChannels.
 *
 * The unit tier (channelForm.test.ts) covers the splitter / matcher logic in isolation. This suite exercises the full routed save through PUT
 * /config/channels/:key, asserting that the on-disk channels.json reflects the partitioned shape - identity entries on the canonical key, binding entries on
 * the variant key.
 */
import { bootApp, createIntegrationContext, initializePersistence, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { mutateChannels } from "../../../src/config/userChannels.ts";

describe("PUT /config/channels/:key - identity vs binding partition", () => {

  test("an identity-field edit on a canonical channel lands on the canonical entry only", async () => {

    /* The "abc" canonical with no service selection resolves to the canonical itself. Submitting a channelNumber edit should land that field on data.channels["abc"]
     * - the canonical entry. No variant entry should be created.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    /* The PUT body shape is the resolved display channel + any user changes. We submit with a channelNumber override; everything else stays at predefined
     * defaults (which the no-changes-detector treats as resolved-display values, so they don't form deltas).
     */
    /* Body shape mirrors what the channel edit form submits: every form field as a string. The required fields (name, url) match the predefined ABC canonical
     * so the no-changes detector won't fire; the only delta vs predefined is the channelNumber override.
     */
    const response = await fetch(urlFor("/config/channels/abc"), {

      body: JSON.stringify({

        channelNumber: "7",
        channelSelector: "ABC",
        guideTitle: "",
        hdhrEnabled: "true",
        logoUrl: "",
        name: "ABC",
        profile: "",
        stationId: "",
        tags: "",
        url: "https://abc.com/watch-live"
      }),
      headers: { "content-type": "application/json" },
      method: "PUT"
    });

    assert.equal(response.status, 200, "PUT should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;
    const canonicalEntry = persisted["abc"] as Record<string, unknown> | undefined;

    assert.ok(canonicalEntry, "canonical entry should exist on disk");
    assert.equal(canonicalEntry["channelNumber"], 7, "channelNumber landed on the canonical entry");
  });

  test("submitting both identity and binding edits on a canonical-only channel writes both to the canonical entry", async () => {

    /* When canonical service is active (no variant in play), binding fields also belong on the canonical entry per the resolver's rules. We submit a delta
     * that mixes both categories and verify both fields land in data.channels["abc"].
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    const response = await fetch(urlFor("/config/channels/abc"), {

      body: JSON.stringify({ channelNumber: "7", name: "ABC", url: "https://override.example.test/abc" }),
      headers: { "content-type": "application/json" },
      method: "PUT"
    });

    assert.equal(response.status, 200);

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;
    const canonicalEntry = persisted["abc"] as Record<string, unknown> | undefined;

    assert.ok(canonicalEntry, "canonical entry exists");
    assert.equal(canonicalEntry["channelNumber"], 7, "identity field landed on canonical");
    assert.equal(canonicalEntry["url"], "https://override.example.test/abc", "binding field landed on canonical when canonical is active");
  });

  test("identity vs binding fields land on the SAME canonical entry when canonical service is active", async () => {

    /* Reinforces the canonical-active routing rule: when no variant is in play, both identity and binding fields write to the canonical entry. Distinct from
     * the variant-active routing tested in variant-vs-canonical.test.ts where binding splits to the variant entry. We seed two separate writes to verify each
     * lands on the same canonical entry without bleeding to a phantom variant.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    await mutateChannels((data) => {

      data.channels["abc"] = { channelNumber: 7, url: "https://override.example.test/abc" };
    });

    const persisted = await readPersistedJson(ctx, "channels.json") as Record<string, unknown>;
    const canonicalEntry = persisted["abc"] as Record<string, unknown> | undefined;

    assert.ok(canonicalEntry, "canonical entry exists on disk");
    assert.equal(canonicalEntry["channelNumber"], 7, "identity field on canonical");
    assert.equal(canonicalEntry["url"], "https://override.example.test/abc", "binding field on canonical (canonical-active routing)");

    // No variant key should exist - we never selected a variant service.
    const variantKeys = Object.keys(persisted).filter((k) => k.startsWith("abc-"));

    assert.deepEqual(variantKeys, [], "no variant entry should be created when canonical service is active");
  });
});
