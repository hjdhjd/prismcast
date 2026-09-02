/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * spectrum.test.ts: Unit tests for the Spectrum TV provider's watch-address construction and its durable lineup export. The guide is non-virtualized, so the whole
 * lineup arrives in one evaluate round-trip - which is what makes the provider drivable from a page stub handing back a canned guide read, with no browser behind
 * it, the same shape youtubeTv.test.ts uses.
 *
 * What these rows exist to assert is a single-source-of-truth property rather than a behavior: the watch address has exactly one construction site, and all three of
 * its consumers - the strategy's direct navigation, the cached-URL resolver, and the durable-lineup export - produce the identical string for the same channel. A
 * second inlined copy would drift silently, and the failure would surface as a persisted hint that navigates somewhere the tune path does not expect.
 *
 * The lineup fixture makes both of its channels alias-keyed on purpose. The cache files one entry object under a callsign key, a stripped-display-name key, and
 * (for a broadcast affiliate) a network-name key, so a projection that iterated the map's values without reducing to distinct entries would report two channels as
 * five.
 */
import type { ChannelSelectionProfile, DiscoveredChannel, Nullable } from "../../types/index.ts";
import { afterEach, describe, test } from "node:test";
import type { Page } from "puppeteer-core";
import assert from "node:assert/strict";
import { makeProfile } from "../../config/profiles.helpers.ts";
import { spectrumProvider } from "./spectrum.ts";

// One raw guide row as the strategy reads it out of the channel header and its rowheader.
interface GuideRow {

  readonly callsign: string;
  readonly channelNumber: string;
  readonly displayName: string;
  readonly tmsid: string;
}

interface GuidePage {

  // Every URL the strategy navigated to, in order.
  readonly navigations: string[];

  // The Page-shaped stub to hand the provider.
  readonly page: Page;
}

/* Builds a Page-shaped stub carrying the three surfaces the provider touches - the grid-render wait, the evaluate that reads the lineup, and the navigation to a
 * watch URL - plus a record of where it navigated. evaluateWithAbort only ever calls .evaluate() on the context it is handed, so a stub whose evaluate ignores the
 * page function and returns the canned rows is a faithful stand-in for the whole read.
 */
function makeGuidePage(rows: GuideRow[]): GuidePage {

  const navigations: string[] = [];

  const page = {

    evaluate: async (): Promise<GuideRow[]> => {

      await Promise.resolve();

      return rows;
    },
    goto: async (url: string): Promise<null> => {

      navigations.push(url);

      await Promise.resolve();

      return null;
    },
    waitForSelector: async (): Promise<null> => {

      await Promise.resolve();

      return null;
    }
  } as unknown as Page;

  return { navigations, page };
}

// Narrows a neutral profile to what the strategy requires: the Spectrum selection strategy and a non-null channel selector.
function makeSpectrumProfile(channelSelector: string): ChannelSelectionProfile {

  return makeProfile({ channelSelection: { strategy: "spectrumGrid" }, channelSelector }) as ChannelSelectionProfile;
}

/* A guide read carrying one cable channel and one broadcast affiliate. Both alias: ESPN is filed under its callsign "espnhd" and its stripped name "espn", and the
 * affiliate under "wfladt", "nbc (wfla)", and the network name "nbc" - five keys, two entry objects.
 */
const LINEUP: GuideRow[] = [
  { callsign: "ESPNHD", channelNumber: "30", displayName: "ESPN HD", tmsid: "10001" },
  { callsign: "WFLADT", channelNumber: "8", displayName: "NBC (WFLA) HD", tmsid: "12345" }
];

// The address each channel's Gracenote station ID builds, written out here rather than composed, so a row asserting it cannot agree with the production helper by
// sharing its mistake.
const ESPN_WATCH_URL = "https://watch.spectrum.net/livetv?tmsid=10001";
const NBC_WATCH_URL = "https://watch.spectrum.net/livetv?tmsid=12345";

afterEach(() => {

  // The channel cache and the empty-discovery counter are module state, so every row starts from the cleared state a browser restart produces.
  spectrumProvider.strategy.clearCache?.();
});

describe("watch address construction", () => {

  test("the strategy navigates to the address the channel's station ID builds", async () => {

    // Consumer one. The tune path's own navigation is the address in its most consequential use: get this wrong and the tune lands on the wrong channel.
    const guide = makeGuidePage(LINEUP);

    const result = await spectrumProvider.strategy.execute(guide.page, makeSpectrumProfile("ESPN"));

    assert.equal(result.success, true, "a channel present in the guide tunes successfully");
    assert.deepEqual(guide.navigations, [ESPN_WATCH_URL], "the strategy navigated to the address built from the matched channel's station ID");
  });

  test("all three consumers produce the identical address for one channel", async () => {

    /* The single-source-of-truth assertion. The strategy's navigation, the cached-URL resolver, and the durable-lineup export each reach for the same construction site;
     * a second inlined copy would satisfy any one of these rows on its own and drift from the others the moment Spectrum's watch-page shape changed.
     */
    const guide = makeGuidePage(LINEUP);

    await spectrumProvider.strategy.execute(guide.page, makeSpectrumProfile("ESPN"));

    const resolved = await spectrumProvider.strategy.resolveDirectUrl?.("ESPN", guide.page);
    const exported = spectrumProvider.exportDurableLineup?.()?.find((channel) => channel.channelSelector === "ESPN");

    assert.equal(guide.navigations[0], ESPN_WATCH_URL, "the strategy navigation");
    assert.equal(resolved, ESPN_WATCH_URL, "the cached-URL resolver");
    assert.equal(exported?.watchUrl, ESPN_WATCH_URL, "the durable-lineup export");
  });
});

describe("durable lineup export", () => {

  test("reports nothing while the cache is cold", () => {

    // Null is what the discovery-outcome recorder reads as "this provider contributed nothing", which is distinct from a statement that it carries no channels.
    assert.equal(spectrumProvider.exportDurableLineup?.(), null, "a cold cache exports nothing");
  });

  test("exports one row per channel with its watch address, however many keys point at it", async () => {

    /* Five cache keys, two channels. Projecting the map's values without reducing them to distinct entries would persist each channel two or three times, and the
     * duplicates would carry into the channel-form suggestion list and every later boot's fallback.
     */
    const guide = makeGuidePage(LINEUP);

    await spectrumProvider.discoverChannels(guide.page);

    assert.deepEqual(spectrumProvider.exportDurableLineup?.(), [

      { channelSelector: "ESPN", name: "ESPN", watchUrl: ESPN_WATCH_URL },
      { channelSelector: "NBC", name: "NBC (WFLA)", watchUrl: NBC_WATCH_URL }
    ], "each channel is exported once, keyed by the selector a channel record would carry");
  });

  test("carries the affiliate's network selector rather than its callsign", async () => {

    // The persisted lookup matches on channelSelector, and a user's channel record for a local NBC affiliate carries "NBC". A row keyed by the callsign would be
    // invisible to the tune that needs it.
    const guide = makeGuidePage(LINEUP);

    await spectrumProvider.discoverChannels(guide.page);

    const exported: Nullable<{ channelSelector: string }[]> = spectrumProvider.exportDurableLineup?.() ?? null;

    assert.deepEqual(exported?.map((channel) => channel.channelSelector), [ "ESPN", "NBC" ], "the affiliate is keyed by its network name");
  });

  test("mirrors the lineup the discovery walk reported", async () => {

    // The export and the discovery output describe the same cache, so a channel in one has to be in the other - a divergence would mean the store and the
    // suggestion list disagree about what the provider carries.
    const guide = makeGuidePage(LINEUP);

    const discovered: DiscoveredChannel[] = await spectrumProvider.discoverChannels(guide.page);

    assert.deepEqual(spectrumProvider.exportDurableLineup?.()?.map((channel) => channel.name).sort(), discovered.map((channel) => channel.name).sort(),
      "the durable lineup names the same channels the walk reported");
  });
});
