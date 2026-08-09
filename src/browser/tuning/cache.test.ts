/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cache.test.ts: Unit tests for the shared channel-cache lifecycle in cache.ts. buildDiscoveredChannelsFromCache is the single source of truth for turning a
 * provider's cached entries into the discovery output its consumers read, so what it has to get right is the same for every provider: alias keys collapse, the
 * order is a locale comparison rather than a code-point one, and whatever the provider's own mapper builds survives the projection untouched.
 *
 * The fixtures below reproduce each provider's entry shape rather than importing the providers, because the projection is generic and the shapes are what vary:
 * a pre-built row, a display name with an optional affiliate, a display name with an optional tier, a display name alone. The conditional-field cases matter
 * because an absent optional field has to stay absent - a key present with an undefined value is a different row on the wire.
 *
 * createProviderChannelCache wraps that projection in the lifecycle every provider shares: the map, the completeness mark gating warm reads, and the clear a
 * browser restart triggers. Two of its guarantees carry real weight for the providers built on it. The gate has to withhold a lineup on every combination short
 * of marked-and-populated, because a partial cache served as the whole lineup is a channel list missing channels. And the map has to survive a clear as the same
 * object, because providers hand out entry references as aliases and update entries in place through them - a replacement map would orphan every one.
 */
import { buildDiscoveredChannelsFromCache, createProviderChannelCache } from "./cache.ts";
import { describe, test } from "node:test";
import type { DiscoveredChannel } from "../../types/index.ts";
import assert from "node:assert/strict";

// A cache entry that already carries its projected row, which is the shape Spectrum, YouTube TV, HBO, and the Comcast Polymer providers hold.
interface ProjectedEntry {

  readonly discovered: DiscoveredChannel;
}

// A cache entry in Hulu's shape: a display name plus an affiliate network carried only by local affiliates.
interface AffiliateEntry {

  readonly affiliate?: string;
  readonly displayName: string;
}

// A cache entry in Sling's shape: a display name plus a tier carried only where the paid/free distinction applies.
interface TieredEntry {

  readonly displayName: string;
  readonly tier?: string;
}

// A cache entry in DirecTV's shape, whose whole projection is derived from the display name.
interface NamedEntry {

  readonly displayName: string;
}

// projectedMapper is the projection the providers that store a pre-built row use.
function projectedMapper(entry: ProjectedEntry): DiscoveredChannel {

  return entry.discovered;
}

// affiliateMapper reproduces Hulu's projection: the network name becomes the selector when the entry has one, and the affiliate field is written only when present.
function affiliateMapper(entry: AffiliateEntry): DiscoveredChannel {

  const result: DiscoveredChannel = { channelSelector: entry.affiliate ?? entry.displayName, name: entry.displayName };

  if(entry.affiliate) {

    result.affiliate = entry.affiliate;
  }

  return result;
}

// tieredMapper reproduces Sling's projection: the display name is always the selector, and the tier field is written only when present.
function tieredMapper(entry: TieredEntry): DiscoveredChannel {

  const result: DiscoveredChannel = { channelSelector: entry.displayName, name: entry.displayName };

  if(entry.tier) {

    result.tier = entry.tier;
  }

  return result;
}

// namedMapper reproduces DirecTV's projection, which builds both fields from the display name.
function namedMapper(entry: NamedEntry): DiscoveredChannel {

  return { channelSelector: entry.displayName, name: entry.displayName };
}

// row builds a pre-projected entry for the providers that store one.
function row(name: string): ProjectedEntry {

  return { discovered: { channelSelector: name, name } };
}

describe("buildDiscoveredChannelsFromCache", () => {

  test("collapses several keys pointing at one entry into a single row", () => {

    // Every provider's tiered matching writes an alias key onto an entry it already found, so a cache routinely holds one object under two or three keys. The
    // projection reports the channel, not the keys, so those have to converge on one row.
    const espn = row("ESPN");
    const cache = new Map<string, ProjectedEntry>([

      [ "espn", espn ],
      [ "espn hd", espn ],
      [ "espn (east)", espn ]
    ]);

    assert.deepEqual(buildDiscoveredChannelsFromCache(cache.values(), projectedMapper), [{ channelSelector: "ESPN", name: "ESPN" }],
      "three keys naming one entry report one channel");
  });

  test("reports every distinct entry when a cache holds no aliases", () => {

    const cache = new Map<string, ProjectedEntry>([

      [ "alpha", row("Alpha") ],
      [ "bravo", row("Bravo") ]
    ]);

    assert.deepEqual(buildDiscoveredChannelsFromCache(cache.values(), projectedMapper).map((channel) => channel.name), [ "Alpha", "Bravo" ],
      "distinct entries each report their own row");
  });

  test("orders the output by locale comparison rather than by code point", () => {

    // Channel names arrive in mixed case, so a code-point sort would file every lowercase name after every uppercase one and produce a guide list no user would
    // recognize as alphabetical. The fixture is chosen so the two orderings disagree.
    const cache = new Map<string, ProjectedEntry>([

      [ "zulu", row("Zulu") ],
      [ "alpha", row("alpha") ],
      [ "bravo", row("Bravo") ]
    ]);

    assert.deepEqual(buildDiscoveredChannelsFromCache(cache.values(), projectedMapper).map((channel) => channel.name), [ "alpha", "Bravo", "Zulu" ],
      "names sort as a reader expects rather than by their leading character's code point");
  });

  test("returns an empty array for a cache with no entries", () => {

    assert.deepEqual(buildDiscoveredChannelsFromCache(new Map<string, ProjectedEntry>().values(), projectedMapper), [],
      "an unpopulated cache projects to nothing rather than failing");
  });

  test("carries the affiliate shape through with the field present only where the entry has one", () => {

    const cache = new Map<string, AffiliateEntry>([

      [ "nbc 5", { affiliate: "NBC", displayName: "NBC 5" } ],
      [ "cnn", { displayName: "CNN" } ]
    ]);

    const [ cnn, nbc ] = buildDiscoveredChannelsFromCache(cache.values(), affiliateMapper);

    assert.deepEqual(nbc, { affiliate: "NBC", channelSelector: "NBC", name: "NBC 5" }, "an affiliate reports its network as the selector and names it");
    assert.deepEqual(cnn, { channelSelector: "CNN", name: "CNN" }, "a plain channel reports its own name as the selector");
    assert.equal(Object.hasOwn(cnn, "affiliate"), false, "a non-affiliate carries no affiliate key at all rather than one holding undefined");
  });

  test("carries the tier shape through with the field present only where the entry has one", () => {

    const cache = new Map<string, TieredEntry>([

      [ "freestream news", { displayName: "Freestream News", tier: "free" } ],
      [ "amc", { displayName: "AMC" } ]
    ]);

    const [ amc, freestream ] = buildDiscoveredChannelsFromCache(cache.values(), tieredMapper);

    assert.deepEqual(freestream, { channelSelector: "Freestream News", name: "Freestream News", tier: "free" }, "a tiered channel reports its tier");
    assert.deepEqual(amc, { channelSelector: "AMC", name: "AMC" }, "an untiered channel reports name and selector alone");
    assert.equal(Object.hasOwn(amc, "tier"), false, "an untiered channel carries no tier key at all rather than one holding undefined");
  });

  test("agrees with display-name deduplication on the DirecTV shape, where an alias key reuses the entry object", () => {

    /* DirecTV keys its cache by the normalized display name and aliases a bare network name onto an affiliate's own entry object. Two entries therefore cannot
     * share a display name - equal names normalize to equal keys, so the second write replaces the first - and an alias is always the same object rather than a
     * copy. Those two facts are what make deduplicating on entry identity and deduplicating on the display-name string report the same rows, which this pins on
     * a cache carrying both an alias and a plain entry.
     */
    const wabc = { displayName: "ABC WABC" };
    const cache = new Map<string, NamedEntry>([

      [ "abc wabc", wabc ],
      [ "abc", wabc ],
      [ "espn", { displayName: "ESPN" } ]
    ]);

    const byReference = buildDiscoveredChannelsFromCache(cache.values(), namedMapper);

    // The same rows computed by deduplicating on the display-name string instead of on entry identity.
    const byDisplayName: DiscoveredChannel[] = [];
    const seen = new Set<string>();

    for(const entry of cache.values()) {

      if(seen.has(entry.displayName)) {

        continue;
      }

      seen.add(entry.displayName);
      byDisplayName.push(namedMapper(entry));
    }

    byDisplayName.sort((a, b) => a.name.localeCompare(b.name));

    assert.deepEqual(byReference, byDisplayName, "both deduplication keys report the same rows for a DirecTV-shaped cache");
    assert.deepEqual(byReference.map((channel) => channel.name), [ "ABC WABC", "ESPN" ], "the aliased affiliate is reported once, alongside the plain entry");
  });
});

describe("createProviderChannelCache", () => {

  test("withholds a lineup until the cache is both marked complete and populated", () => {

    // The gate's whole job is refusing to answer on the three combinations that are not a complete lineup. A cache filled by a partial capture, or marked by a
    // walk that captured nothing, would otherwise be served to the discovery route as though it were the provider's entire channel list.
    const unmarkedEmpty = createProviderChannelCache<ProjectedEntry>(projectedMapper);

    assert.equal(unmarkedEmpty.cached(), null, "an untouched cache reports nothing enumerated");

    const unmarkedFilled = createProviderChannelCache<ProjectedEntry>(projectedMapper);

    unmarkedFilled.map.set("espn", row("ESPN"));

    assert.equal(unmarkedFilled.cached(), null, "a populated but unmarked cache is a partial capture and is withheld");

    const markedEmpty = createProviderChannelCache<ProjectedEntry>(projectedMapper);

    markedEmpty.markComplete();

    assert.equal(markedEmpty.cached(), null, "a mark over an empty map answers nothing rather than an empty lineup");

    const markedFilled = createProviderChannelCache<ProjectedEntry>(projectedMapper);

    markedFilled.map.set("espn", row("ESPN"));
    markedFilled.markComplete();

    assert.deepEqual(markedFilled.cached(), [{ channelSelector: "ESPN", name: "ESPN" }], "marked and populated is the one combination that answers");
  });

  test("clear empties the map and drops the completeness mark together", () => {

    const cache = createProviderChannelCache<ProjectedEntry>(projectedMapper);

    cache.map.set("espn", row("ESPN"));
    cache.markComplete();
    cache.clear();

    assert.equal(cache.map.size, 0, "the map is emptied");
    assert.equal(cache.cached(), null, "the mark is dropped, so refilling the map alone does not resume warm reads");

    cache.map.set("espn", row("ESPN"));

    assert.equal(cache.cached(), null, "a cache refilled after a clear stays withheld until something marks it complete again");
  });

  test("hands out one map object for its whole lifetime, including across a clear", () => {

    /* Providers capture entry references out of the map and file them under additional keys, and a populate pass updates those entries in place expecting every
     * alias to see the change. All of that hangs on there being exactly one map: were clear() to install a fresh one, every captured reference would point at an
     * object unreachable from the cache, and the aliases would silently stop tracking their channel.
     */
    const cache = createProviderChannelCache<ProjectedEntry>(projectedMapper);
    const captured = cache.map;

    cache.map.set("espn", row("ESPN"));
    cache.clear();
    cache.map.set("cnn", row("CNN"));

    assert.equal(cache.map, captured, "the instance exposes the same map object it started with");
    assert.equal(captured.get("cnn")?.discovered.name, "CNN", "a reference captured before the clear still sees writes made after it");
  });

  test("invalidate drops the entry filed under the lowercased selector", () => {

    const cache = createProviderChannelCache<ProjectedEntry>(projectedMapper);

    cache.map.set("espn", row("ESPN"));
    cache.map.set("cnn", row("CNN"));
    cache.invalidate("ESPN");

    assert.equal(cache.map.has("espn"), false, "the selector's key is dropped even though the caller supplied it in a different case");
    assert.equal(cache.map.has("cnn"), true, "every other key is left alone");
  });

  test("discovered projects the current entries regardless of the completeness mark", () => {

    // The paths that have just filled the cache themselves report what they captured without waiting on the mark, which is how a walk that fell short of its
    // completeness threshold still serves the request that triggered it.
    const cache = createProviderChannelCache<ProjectedEntry>(projectedMapper);
    const espn = row("ESPN");

    cache.map.set("espn", espn);
    cache.map.set("espn hd", espn);
    cache.map.set("cnn", row("CNN"));

    assert.deepEqual(cache.discovered().map((channel) => channel.name), [ "CNN", "ESPN" ],
      "an unmarked cache still projects, deduplicated and sorted, through the shared builder");
    assert.equal(cache.cached(), null, "projecting does not imply the lineup is complete");
  });

  test("keeps two instances fully isolated", () => {

    // The Comcast Polymer factory builds one instance per provider off a single platform, so Xfinity filling or clearing its cache must be invisible to Cox.
    const first = createProviderChannelCache<ProjectedEntry>(projectedMapper);
    const second = createProviderChannelCache<ProjectedEntry>(projectedMapper);

    first.map.set("espn", row("ESPN"));
    first.markComplete();

    assert.equal(second.cached(), null, "the second instance holds none of the first's entries or its mark");

    second.map.set("cnn", row("CNN"));
    second.markComplete();
    first.clear();

    assert.deepEqual(second.cached()?.map((channel) => channel.name), ["CNN"], "clearing the first instance leaves the second untouched");
  });
});
