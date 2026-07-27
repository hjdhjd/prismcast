/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * healthBridge.ts: Reactive bridge between the health emitter and the channel table patch stream. Listens for every health/auth state change and emits a channel
 * table patch covering the rows whose rendered state the change can affect: the channel whose health was recorded, plus every other channel whose currently-selected
 * service variant resolves to the event's domain (the domain-verification indicator on those rows reflects shared state).
 *
 * Enforces that channel row presentation has a single source of truth - generateChannelRowHtml on the server. Client code never composes channel row HTML,
 * classes, or titles imperatively; every change flows through buildChannelTablePatch...emitChannelUpdate...channelTable.applyPatch.
 */
import { getAuthDomainForChannel, resolveServiceKey } from "../../../config/services.ts";
import { getHealthSnapshot, subscribeToHealth } from "../../../config/health.ts";
import type { ChannelTablePatch } from "./table.ts";
import type { HealthEvent } from "../../../config/health.ts";
import { buildChannelTablePatch } from "./table.ts";
import { emitChannelUpdate } from "../../../streaming/statusEmitter.ts";
import { getAllChannels } from "../../../config/userChannels.ts";
import { getProfiles } from "../../../config/profiles.ts";

/**
 * Resolves the channel keys whose rendered state would change as a consequence of a health event. The event's channelKey is included when present (it is empty
 * for markDomainAuth which has no channel context). Every channel whose currently-selected service variant resolves to the event's domain is also included, since
 * the domain auth indicator on those rows reflects shared state across the service.
 * @param event - The health event.
 * @returns The set of channel keys that should be re-rendered, deduplicated and order-independent.
 */
export function resolveAffectedKeys(event: HealthEvent): string[] {

  const keys = new Set<string>();

  if(event.channelKey) {

    keys.add(event.channelKey);
  }

  if(event.domain) {

    for(const key of Object.keys(getAllChannels())) {

      const variantKey = resolveServiceKey(key);

      if(getAuthDomainForChannel(variantKey) === event.domain) {

        keys.add(key);
      }
    }
  }

  return Array.from(keys);
}

/**
 * Installs the health-to-patch bridge. Subscribes to health events from config/health.ts and, for each one, emits a channel table patch via emitChannelUpdate.
 * Returns the unsubscribe function so callers can tear down the subscription if needed (production never does - the bridge lives for the process lifetime).
 * @returns The unsubscribe function from subscribeToHealth.
 */
export function installHealthBridge(): () => void {

  return subscribeToHealth((event) => {

    const affectedKeys = resolveAffectedKeys(event);

    if(affectedKeys.length === 0) {

      return;
    }

    emitChannelUpdate(buildChannelTablePatch(affectedKeys, getProfiles()));
  });
}

/**
 * Builds a channel table patch covering every row whose visible state is influenced by the current in-memory health/auth state. Used by the SSE snapshot to
 * catch reconnecting clients up to any health changes that may have occurred while their EventSource was disconnected - the patch is bounded by the size of the
 * health snapshot rather than the total channel count, so a fresh install with no recorded state emits an empty patch.
 * @returns A channel table patch covering all rows with current health or domain auth entries.
 */
export function buildSnapshotChannelPatch(): ChannelTablePatch {

  const snapshot = getHealthSnapshot();
  const keys = new Set<string>(Object.keys(snapshot.channels));

  for(const domain of Object.keys(snapshot.domains)) {

    for(const key of Object.keys(getAllChannels())) {

      const variantKey = resolveServiceKey(key);

      if(getAuthDomainForChannel(variantKey) === domain) {

        keys.add(key);
      }
    }
  }

  return buildChannelTablePatch(Array.from(keys), getProfiles());
}
