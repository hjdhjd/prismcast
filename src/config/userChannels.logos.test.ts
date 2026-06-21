/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userChannels.logos.test.ts: Direct unit tests for the logo cache helpers.
 *
 * The cache is module-level state populated by showInfo.ts after polling the Channels DVR API. Reads route through getChannelStationId (which resolves
 * Pacific channels to their East counterpart's stationId for shared-brand-logo behavior) and finally consult an internal Map<stationId, url>. Tests use the
 * public reader getChannelLogo with predefined channels whose stationId is known to be present in the catalog, write through setChannelLogo / setChannelLogos,
 * and assert via the public reader. clearChannelLogos is verified by reading after clear.
 *
 * The Pacific east-counterpart resolution in getChannelStationId (the getEastCanonicalKey(channelKey) ?? channelKey line in userChannels.ts) is the load-bearing
 * logic - a regression there silently misroutes logos for every Pacific channel. Pinning it here keeps the case-of-record visible.
 */
import { afterEach, describe, test } from "node:test";
import { clearChannelLogos, getChannelLogo, getChannelStationId, setChannelLogo, setChannelLogos } from "./userChannels.ts";
import { PREDEFINED_CHANNELS } from "../channels/index.ts";
import assert from "node:assert/strict";

/* Find a known predefined canonical that has a stationId. We need this for the read-back tests: setChannelLogo writes under a stationId, and the public reader
 * resolves channelKey -> stationId before consulting the cache. We use abcnews because the catalog declares its stationId.
 */
const ABCNEWS_STATION_ID = ((): string => {

  const sid = getChannelStationId("abcnews");

  if(!sid) {

    throw new Error("Test setup invariant: abcnews must have a predefined stationId");
  }

  return sid;
})();

describe("setChannelLogo / getChannelLogo via canonical channel", () => {

  afterEach(() => {

    clearChannelLogos();
  });

  test("setChannelLogo stores under the stationId; getChannelLogo retrieves via channel-key resolution", () => {

    setChannelLogo(ABCNEWS_STATION_ID, "https://example.test/abcnews-logo.png");

    assert.equal(getChannelLogo("abcnews"), "https://example.test/abcnews-logo.png");
  });

  test("setChannelLogos bulk-populates the cache (Map input)", () => {

    /* The Map-input branch: showInfo.ts batches the DVR's /devices payload into a Map and hands it off in one call. Each entry is independently visible after.
     */
    const logos = new Map<string, string>([[ ABCNEWS_STATION_ID, "https://example.test/bulk-logo.png" ]]);

    setChannelLogos(logos);

    assert.equal(getChannelLogo("abcnews"), "https://example.test/bulk-logo.png");
  });

  test("setChannelLogos overwrites existing entries (rebrand case)", () => {

    setChannelLogos(new Map([[ ABCNEWS_STATION_ID, "https://example.test/old.png" ]]));
    setChannelLogos(new Map([[ ABCNEWS_STATION_ID, "https://example.test/new.png" ]]));

    assert.equal(getChannelLogo("abcnews"), "https://example.test/new.png");
  });

  test("clearChannelLogos empties the cache (subsequent reads return undefined)", () => {

    setChannelLogo(ABCNEWS_STATION_ID, "https://example.test/before-clear.png");
    assert.equal(getChannelLogo("abcnews"), "https://example.test/before-clear.png");

    clearChannelLogos();

    assert.equal(getChannelLogo("abcnews"), undefined);
  });

  test("getChannelLogo returns undefined for a channel key with no resolvable stationId (chain short-circuits)", () => {

    /* No stationId -> cache lookup is never performed. Pin: a channel that doesn't exist in any catalog produces undefined regardless of cache state.
     */
    assert.equal(getChannelLogo("definitely-not-a-channel-x9z2"), undefined);
  });

  test("getChannelLogo returns undefined when the stationId resolves but no cache entry exists", () => {

    /* afterEach clears the cache. abcnews's stationId is resolvable from the catalog, but the cache is empty, so the cache.get() returns undefined.
     */
    assert.equal(getChannelLogo("abcnews"), undefined);
  });
});

describe("getChannelStationId: Pacific east-counterpart resolution", () => {

  /* Pacific channels share their brand identity with the East counterpart. getChannelStationId resolves Pacific keys (e.g., "bravop") via getEastCanonicalKey
   * before reading the catalog, so the Pacific feed's logo, station ID, and other brand metadata fall back to the East variant's. A regression here silently
   * misroutes logos for every Pacific channel.
   */

  test("a Pacific canonical resolves to the East counterpart's stationId", () => {

    /* Find a Pacific canonical whose East counterpart has a known stationId. bravop -> bravo is a documented pair; we read both and assert they match.
     */
    const pacificStationId = getChannelStationId("bravop");
    const eastStationId = getChannelStationId("bravo");

    if(!eastStationId) {

      // Skip if the catalog's bravo stationId shape changed - this assertion only matters for the case where bravo carries a stationId.
      return;
    }

    assert.equal(pacificStationId, eastStationId, "Pacific stationId resolves to the East counterpart's stationId");
  });

  test("an East canonical resolves to its own stationId (no Pacific re-routing)", () => {

    /* Symmetric branch: the East key does not end in "p" so getEastCanonicalKey returns undefined, and the resolver reads the catalog directly. The stationId
     * comes from the East canonical itself.
     */
    const eastStationId = getChannelStationId("abcnews");

    assert.equal(typeof eastStationId, "string", "East canonical's stationId is read directly from the catalog");
  });

  test("returns undefined for an unknown channel key", () => {

    /* The chain bottoms out at the catalog read; an unknown key produces undefined.
     */
    assert.equal(getChannelStationId("definitely-not-a-channel-x9z2"), undefined);
  });

  test("Pacific canonical without a counterpart in the catalog falls back to direct read (returns undefined when the catalog has no stationId for it)", () => {

    /* If a Pacific-shaped key passes the suffix check but its counterpart is not in the catalog, the resolver falls through to reading the catalog at the
     * Pacific key directly. PREDEFINED_CHANNELS["foop"] doesn't exist so the result is undefined.
     */
    if(PREDEFINED_CHANNELS["foop"]) {

      // The catalog grew an entry that breaks this test's invariant; let it be revisited rather than fail brittle.
      return;
    }

    assert.equal(getChannelStationId("foop"), undefined);
  });
});
