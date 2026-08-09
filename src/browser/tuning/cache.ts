/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cache.ts: Channel-cache data lifecycle shared by the provider tuning strategies. The page and DOM mechanics a strategy drives - installs, scrolling, clicking,
 * guide recovery - live in shared.ts. What lives here is the other half: the map a provider's channel entries sit in, the mark that says its lineup is complete,
 * and the projection that turns those entries into the discovery output its consumers read.
 *
 * The division of labor is deliberate. A cache instance owns lifecycle - holding, gating, emptying, projecting - and never matching policy. How a provider finds
 * an entry (Hulu's fuzzy key resolution, Spectrum's callsign tiers, the prefix-plus-digit affiliate lookups) and how it fills the cache stay in the provider,
 * which is why the raw map is part of the instance's surface rather than hidden behind it.
 */
import type { DiscoveredChannel, Nullable } from "../../types/index.ts";

/**
 * Projects a provider's cached channel entries into the discovery output its consumers read. Entries are deduplicated by reference, because every provider's
 * tiered matching writes alias keys - several cache keys pointing at one entry object - and iterating the map's values without that pass would report one channel
 * as several. A cache holding no aliases still gets a correct result and pays only the Set membership pass. The output is sorted by name.
 * @param entries - The cache's entry values, in whatever order the map holds them.
 * @param toDiscovered - Projects one cached entry onto the discovered-channel shape the provider reports for it.
 * @returns Sorted, deduplicated discovered channels.
 */
export function buildDiscoveredChannelsFromCache<Entry>(entries: Iterable<Entry>, toDiscovered: (entry: Entry) => DiscoveredChannel): DiscoveredChannel[] {

  const channels: DiscoveredChannel[] = [];
  const seen = new Set<Entry>();

  for(const entry of entries) {

    if(seen.has(entry)) {

      continue;
    }

    seen.add(entry);
    channels.push(toDiscovered(entry));
  }

  channels.sort((a, b) => a.name.localeCompare(b.name));

  return channels;
}

/**
 * One provider's channel cache: the entry map, the completeness mark that gates cached reads, and the projection over both.
 */
export interface ProviderChannelCache<Entry> {

  /**
   * Returns the discovery output when the lineup has been marked complete and the map holds entries, and null otherwise. Null is the answer the discovery route
   * reads as "nothing enumerated yet, go load the guide" - a partially-filled cache must never be served as though it were the whole lineup.
   */
  readonly cached: () => Nullable<DiscoveredChannel[]>;

  /**
   * Empties the map in place and drops the completeness mark, which is the state a browser restart leaves behind.
   */
  readonly clear: () => void;

  /**
   * Projects the current entries into discovery output regardless of the completeness mark, for the paths that have just filled the cache themselves.
   */
  readonly discovered: () => DiscoveredChannel[];

  /**
   * Drops the entry filed under the lowercased selector, for a cached URL that failed to produce a working stream. Providers whose write side keys entries by
   * some other normalization wrap the map directly instead, so that a delete resolves keys exactly the way its own writes construct them.
   */
  readonly invalidate: (channelSelector: string) => void;

  /**
   * The entry map itself. Matching, alias writes, frontier scans, and populate loops all need full map access, so the instance hands it over rather than
   * wrapping it: what the instance owns is the lifecycle around the map, not the policy applied to its contents.
   */
  readonly map: Map<string, Entry>;

  /**
   * Marks the lineup complete, which is what lets cached() start answering. Belongs at the point a provider knows it has enumerated everything.
   */
  readonly markComplete: () => void;
}

/**
 * Creates a channel cache for one provider. The map and the completeness mark live in the returned instance's closure, so every provider - and every invocation
 * of a factory that serves several providers off one platform - holds state nothing else can reach or reset.
 *
 * The map is created once and emptied in place for the instance's whole lifetime, never swapped for a fresh one. Providers capture entry references and file them
 * under additional alias keys, and a populate pass that updates an existing entry in place relies on every one of those aliases still pointing at the object it
 * updated. Handing back a fresh map on a clear would orphan all of them.
 * @param toDiscovered - Projects one cached entry onto the discovered-channel shape this provider reports for it.
 * @returns A cache instance holding this provider's entries.
 */
export function createProviderChannelCache<Entry>(toDiscovered: (entry: Entry) => DiscoveredChannel): ProviderChannelCache<Entry> {

  const map = new Map<string, Entry>();

  let complete = false;

  const discovered = (): DiscoveredChannel[] => buildDiscoveredChannelsFromCache(map.values(), toDiscovered);

  return {

    cached: (): Nullable<DiscoveredChannel[]> => (complete && (map.size > 0)) ? discovered() : null,
    clear: (): void => {

      map.clear();

      complete = false;
    },
    discovered,
    invalidate: (channelSelector: string): void => {

      map.delete(channelSelector.toLowerCase());
    },
    map,
    markComplete: (): void => {

      complete = true;
    }
  };
}
