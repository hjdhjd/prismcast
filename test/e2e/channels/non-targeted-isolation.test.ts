/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * non-targeted-isolation.test.ts: Per-entry byte-preservation invariant for channels.json under any mutation. The cross-store-isolation suite pins file-level
 * isolation across stores; this suite pins the within-channels.json analog: a mutation that targets a specific subset of channel keys must leave every other
 * key's on-disk entry byte-identical pre/post. The invariant catches a class of bug invisible to the existing crud/bulk suites - serializer drift that silently
 * re-keys entries, re-orders tag arrays, or rewrites whitespace - because those suites only assert the targeted side of the change.
 *
 * Comparison strategy: parse channels.json, project each channel entry through stringifySorted (the same serializer prepareChannelsForWrite hands to the
 * file-store framework's beforeWrite path before it writes to disk), and compare per-entry strings. This produces the strongest possible byte-identity
 * assertion against parsed output without coupling to
 * full-file bytes - which always change because the targeted entry changes - and survives whitespace/key-order regressions a deepEqual on parsed objects would
 * miss. The on-disk channels.json shape is flat per the prepareChannelsForWrite contract (channel entries top-level alongside schemaVersion / migrationsApplied
 * / serviceSelections / tagRegistry), so the projection iterates only the keys we explicitly named at seed time rather than every top-level key.
 */
import { bootApp, createIntegrationContext, initializePersistence, readPersistedJson } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import { disablePredefinedChannels, mutateChannels } from "../../../src/config/userChannels.ts";
import assert from "node:assert/strict";
import { stringifySorted } from "../../../src/utils/format.ts";

/**
 * Snapshots the on-disk projection of the named channel entries through stringifySorted - the same serializer prepareChannelsForWrite hands to the file-store
 * framework. Two snapshots taken around a mutation can be compared per-key with assert.equal: equal strings prove byte-identity for that entry's projection,
 * inequality surfaces any drift (key reorder, value rewrite, type change). Missing keys are recorded as the literal string "<absent>" so a regression that
 * accidentally deletes an entry distinguishes from one that mutates it.
 */
async function snapshotEntries(ctx: { dataDir: string }, keys: readonly string[]): Promise<Record<string, string>> {

  const parsed = await readPersistedJson({ dataDir: ctx.dataDir, registerCleanup: (): void => undefined }, "channels.json") as Record<string, unknown>;

  return Object.fromEntries(keys.map((key) => [ key, (key in parsed) ? stringifySorted(parsed[key]) : "<absent>" ]));
}

describe("channels.json non-targeted byte-preservation", () => {

  test("bulk auto-number leaves disabled-predefined customizations byte-identical", async () => {

    /* Seed customizations on two channels that will be filtered out by visibility (disabled-predefined) and one that will receive a new number. Auto-number
     * iterates getVisibleChannels - disabled predefineds are excluded - so the disabled entries' on-disk bytes must not change. The serializer hits every
     * non-targeted top-level key on every write, so a regression that re-orders fields or strips whitespace would surface here even when the targeted entry
     * is being written correctly.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    /* Customize abc, ae, and amc with stationId overrides so each has a meaningful on-disk entry. We use stationId because it is identity (canonical-only) and
     * it persists as a distinct delta against the predefined catalog regardless of channelNumber assignment - so auto-number's channelNumber writes don't
     * collapse the entry's other fields away.
     */
    await mutateChannels((data) => {

      data.channels["abc"] = { stationId: "999001" };
      data.channels["ae"] = { stationId: "999002" };
      data.channels["amc"] = { stationId: "999003" };
    });

    // Disable abc and ae via the public wrapper so the in-memory CONFIG cache stays in sync with the persisted disabledPredefined list. mutateConfig() writes
    // to disk but does NOT sync CONFIG.channels.disabledPredefined - that runtime cache is owned by the disablePredefinedChannels / enablePredefinedChannels
    // pair, and isPredefinedChannelDisabled reads from CONFIG. Going through the wrapper is the documented contract for runtime-effective disable.
    await disablePredefinedChannels([ "abc", "ae" ]);

    const before = await snapshotEntries(ctx, [ "abc", "ae" ]);

    const response = await fetch(urlFor("/config/channels/auto-number"), {

      body: JSON.stringify({ start: 100 }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "auto-number should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const after = await snapshotEntries(ctx, [ "abc", "ae" ]);

    assert.equal(after["abc"], before["abc"], "disabled abc entry must be byte-identical pre/post auto-number");
    assert.equal(after["ae"], before["ae"], "disabled ae entry must be byte-identical pre/post auto-number");
  });

  test("POST creating a new user channel leaves existing user channels byte-identical", async () => {

    /* The structurally simplest preservation invariant: adding a new entry to channels.json must not reach into existing entries. A serializer regression that
     * rewrites every entry on every write (e.g., a beforeWrite that re-applies normalization to all entries instead of the new one) would surface here.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await mutateChannels((data) => {

      data.channels["seed-a"] = { name: "Seed A", url: "https://example.test/a" };
      data.channels["seed-b"] = { name: "Seed B", url: "https://example.test/b" };
      data.channels["seed-c"] = { name: "Seed C", url: "https://example.test/c" };
    });

    const before = await snapshotEntries(ctx, [ "seed-a", "seed-b", "seed-c" ]);

    const response = await fetch(urlFor("/config/channels"), {

      body: JSON.stringify({

        channelNumber: "",
        channelSelector: "",
        guideTitle: "",
        hdhrEnabled: "true",
        key: "seed-d",
        logoUrl: "",
        name: "Seed D",
        profile: "",
        stationId: "",
        tags: "",
        url: "https://example.test/d"
      }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "POST should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const after = await snapshotEntries(ctx, [ "seed-a", "seed-b", "seed-c" ]);

    for(const key of [ "seed-a", "seed-b", "seed-c" ] as const) {

      assert.equal(after[key], before[key], key + " must be byte-identical when an unrelated channel is created");
    }
  });

  test("PATCH on one user channel leaves other user channels byte-identical", async () => {

    /* PATCH writes through ChannelDelta on a single key. The non-targeted entries must be untouched; this catches a regression where the inline-edit handler
     * accidentally re-runs normalization across the whole channels record instead of the keyed entry.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await mutateChannels((data) => {

      data.channels["seed-a"] = { channelNumber: 10, name: "Seed A", url: "https://example.test/a" };
      data.channels["seed-b"] = { channelNumber: 20, name: "Seed B", url: "https://example.test/b" };
      data.channels["seed-c"] = { channelNumber: 30, name: "Seed C", url: "https://example.test/c" };
    });

    const before = await snapshotEntries(ctx, [ "seed-a", "seed-b", "seed-c" ]);

    const response = await fetch(urlFor("/config/channels/seed-a"), {

      body: JSON.stringify({ channelNumber: 11 }),
      headers: { "content-type": "application/json" },
      method: "PATCH"
    });

    assert.equal(response.status, 200, "PATCH should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const after = await snapshotEntries(ctx, [ "seed-a", "seed-b", "seed-c" ]);

    assert.notEqual(after["seed-a"], before["seed-a"], "seed-a must change (it was the PATCH target)");
    assert.equal(after["seed-b"], before["seed-b"], "seed-b must be byte-identical when seed-a is PATCHed");
    assert.equal(after["seed-c"], before["seed-c"], "seed-c must be byte-identical when seed-a is PATCHed");
  });

  test("bulk tag assign leaves channels that already carry the tag byte-identical", async () => {

    /* transformChannelTags' no-op short-circuit in userChannels.ts skips channels whose tags are unchanged after the transform. Bulk-add of a tag
     * already present must therefore not rewrite those entries on disk. We seed two channels with the predefined "News" tag and one without, run bulk-add
     * News, and assert: the two existing-News entries are byte-identical, the third is touched.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    await mutateChannels((data) => {

      data.channels["seed-news-1"] = { name: "News One", tags: ["News"], url: "https://example.test/n1" };
      data.channels["seed-news-2"] = { name: "News Two", tags: ["News"], url: "https://example.test/n2" };
      data.channels["seed-no-news"] = { name: "Other", url: "https://example.test/x" };
    });

    const before = await snapshotEntries(ctx, [ "seed-news-1", "seed-news-2", "seed-no-news" ]);

    const response = await fetch(urlFor("/config/channels/bulk-tags"), {

      body: JSON.stringify({ action: "add", tag: "News" }),
      headers: { "content-type": "application/json" },
      method: "POST"
    });

    assert.equal(response.status, 200, "bulk-tags should succeed; body: " + (await response.clone().text()).slice(0, 200));

    const after = await snapshotEntries(ctx, [ "seed-news-1", "seed-news-2", "seed-no-news" ]);

    assert.equal(after["seed-news-1"], before["seed-news-1"], "channel already carrying News must be byte-identical pre/post");
    assert.equal(after["seed-news-2"], before["seed-news-2"], "channel already carrying News must be byte-identical pre/post");
    assert.notEqual(after["seed-no-news"], before["seed-no-news"], "channel that did NOT carry News must change (it was the targeted side)");
  });
});
