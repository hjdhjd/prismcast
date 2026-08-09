/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cache.ts: Channel-cache data lifecycle shared by the provider tuning strategies. The page and DOM mechanics a strategy drives - installs, scrolling, clicking,
 * guide recovery - live in shared.ts. What lives here is the other half: how a provider's cached channel entries become the discovery output its consumers read.
 */
import type { DiscoveredChannel } from "../../types/index.ts";

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
