/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * health.ts: Channel health and provider authentication state persistence for PrismCast.
 */
import { EventEmitter } from "events";
import { LOG } from "../utils/index.js";
import type { Nullable } from "../types/index.js";
import fs from "node:fs";
import { getHealthFilePath } from "./paths.js";

const { promises: fsPromises } = fs;

/* This module tracks two kinds of observed state:
 *
 * 1. Channel health — did the last tune attempt for a specific channel succeed or fail? Each channel's health is independent. Switching a channel's provider resets
 *    its health indicator because the stored providerTag no longer matches.
 *
 * 2. Provider authentication — has the user successfully tuned at least one channel on a given streaming provider? Authentication is proven by success: one successful
 *    tune on any channel from a provider turns the entire provider green. There is no "red" state — providers are either verified (green) or unknown (no entry / TTL
 *    expired). This avoids brittle heuristics for detecting auth failures (domain redirects, login page detection, MVPD picker walls) in favor of a single reliable
 *    signal: did video actually start?
 *
 * State is persisted to health.json in the data directory with a 2-second debounce to avoid excessive writes during rapid tune attempts. Entries older than 7 days
 * are pruned at load time to prevent unbounded growth.
 */

// Types.

type HealthStatus = "failed" | "success";

interface ChannelHealthEntry {

  // The provider tag at the time of recording. Used to detect provider switches — if the current provider tag differs, the entry is stale.
  providerTag: string;

  // Whether the last tune succeeded or failed.
  status: HealthStatus;

  // Unix millisecond timestamp for TTL expiry.
  timestamp: number;
}

interface HealthState {

  channels: Record<string, ChannelHealthEntry>;

  // Provider auth entries are just timestamps. The presence of a non-expired entry means "verified authenticated."
  providers: Record<string, number>;
}

/**
 * Event payload emitted when channel health or provider auth state changes.
 */
export interface HealthEvent {

  channelKey: string;
  providerTag: string;
  status: HealthStatus;
  timestamp: number;
}

/**
 * Snapshot of current health state for SSE initial payload.
 */
export interface HealthSnapshot {

  channels: Record<string, { providerTag: string; status: HealthStatus; timestamp: number }>;
  providers: Record<string, number>;
}

// Health event emitter. Fires on every markChannelSuccess / markChannelFailure call so SSE clients receive real-time indicator updates.
const healthEmitter = new EventEmitter();

healthEmitter.setMaxListeners(100);

// Constants.

// Entries older than 7 days are pruned at load time.
const HEALTH_TTL = 7 * 24 * 60 * 60 * 1000;

// Debounce interval for writes to health.json.
const FLUSH_DELAY = 2000;

// In-memory state.

const channelHealth = new Map<string, ChannelHealthEntry>();

// Provider auth is proven by success only. The presence of a non-expired timestamp means the user has successfully tuned at least one channel on the provider.
const providerAuth = new Map<string, number>();

// Debounce timer for flushHealthState().
let flushTimer: Nullable<ReturnType<typeof setTimeout>> = null;

// Persistence.

/**
 * Loads the health state from health.json into memory. Entries older than HEALTH_TTL are pruned during loading. Called once at startup from app.ts.
 */
export async function loadHealthState(): Promise<void> {

  try {

    const content = await fsPromises.readFile(getHealthFilePath(), "utf-8");
    const state = JSON.parse(content) as HealthState;
    const now = Date.now();

    channelHealth.clear();
    providerAuth.clear();

    // Load channel health entries, pruning stale ones. Runtime guard needed because the file may contain incomplete JSON.
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if(state.channels) {

      for(const [ key, entry ] of Object.entries(state.channels)) {

        if((now - entry.timestamp) < HEALTH_TTL) {

          channelHealth.set(key, entry);
        }
      }
    }

    // Load provider auth entries (timestamps), pruning stale ones. Runtime guard needed because the file may contain incomplete JSON.
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if(state.providers) {

      for(const [ key, timestamp ] of Object.entries(state.providers)) {

        if((now - timestamp) < HEALTH_TTL) {

          providerAuth.set(key, timestamp);
        }
      }
    }

    const channelCount = channelHealth.size;
    const providerCount = providerAuth.size;

    if((channelCount > 0) || (providerCount > 0)) {

      LOG.info("Loaded health state: %d channel(s), %d provider(s).", channelCount, providerCount);
    }
  } catch(error) {

    // File doesn't exist — this is normal on first run.
    if((error as NodeJS.ErrnoException).code === "ENOENT") {

      return;
    }

    LOG.warn("Failed to load health state: %s. Starting with empty health data.", (error instanceof Error) ? error.message : String(error));
  }
}

/**
 * Writes the current in-memory health state to health.json. Debounced — multiple calls within FLUSH_DELAY are coalesced into a single write.
 */
function flushHealthState(): void {

  if(flushTimer) {

    clearTimeout(flushTimer);
  }

  flushTimer = setTimeout(() => {

    flushTimer = null;

    const state: HealthState = {

      channels: Object.fromEntries(channelHealth),
      providers: Object.fromEntries(providerAuth)
    };

    // Sort keys for consistent output.
    const sortedState: HealthState = {

      channels: Object.fromEntries(Object.entries(state.channels).sort(([a], [b]) => a.localeCompare(b))),
      providers: Object.fromEntries(Object.entries(state.providers).sort(([a], [b]) => a.localeCompare(b)))
    };

    fsPromises.writeFile(getHealthFilePath(), JSON.stringify(sortedState, null, 2) + "\n", "utf-8").catch((error: unknown) => {

      LOG.warn("Failed to write health state: %s.", (error instanceof Error) ? error.message : String(error));
    });
  }, FLUSH_DELAY);
}

// Public API.

/**
 * Records a successful tune for a channel. Sets the channel's health to "success" and marks the provider as verified (authenticated). Triggers a debounced flush.
 * @param channelKey - The channel key (canonical key, e.g., "nbc").
 * @param providerTag - The provider tag for the currently selected provider variant.
 */
export function markChannelSuccess(channelKey: string, providerTag: string): void {

  const now = Date.now();

  channelHealth.set(channelKey, { providerTag, status: "success", timestamp: now });
  providerAuth.set(providerTag, now);

  flushHealthState();
  healthEmitter.emit("healthChanged", { channelKey, providerTag, status: "success", timestamp: now } satisfies HealthEvent);
}

/**
 * Records a failed tune for a channel. Sets the channel's health to "failed". Does not affect provider auth — a single channel failure doesn't prove the provider
 * is unauthenticated. Triggers a debounced flush.
 * @param channelKey - The channel key (canonical key, e.g., "nbc").
 * @param providerTag - The provider tag for the currently selected provider variant.
 */
export function markChannelFailure(channelKey: string, providerTag: string): void {

  const now = Date.now();

  channelHealth.set(channelKey, { providerTag, status: "failed", timestamp: now });

  flushHealthState();
  healthEmitter.emit("healthChanged", { channelKey, providerTag, status: "failed", timestamp: now } satisfies HealthEvent);
}

/**
 * Returns the health status and timestamp for a channel. Returns null if no entry exists, the entry is stale (older than 7 days), or the stored providerTag doesn't
 * match the current one (provider was switched).
 * @param channelKey - The channel key (canonical key, e.g., "nbc").
 * @param providerTag - The provider tag for the currently selected provider variant.
 * @returns Object with status and timestamp, or null if unknown.
 */
export function getChannelHealth(channelKey: string, providerTag: string): Nullable<{ status: HealthStatus; timestamp: number }> {

  const entry = channelHealth.get(channelKey);

  if(!entry) {

    return null;
  }

  // Stale entry — older than TTL.
  if((Date.now() - entry.timestamp) >= HEALTH_TTL) {

    return null;
  }

  // Provider was switched — the stored result is for a different provider.
  if(entry.providerTag !== providerTag) {

    return null;
  }

  return { status: entry.status, timestamp: entry.timestamp };
}

/**
 * Returns the timestamp when a provider was last verified as authenticated, or null if unknown. Verification is proven by at least one successful tune within the
 * TTL window. A non-null return means the provider is verified; the value is the Unix millisecond timestamp of the most recent successful tune.
 * @param providerTag - The provider tag to check.
 * @returns Timestamp of last verification, or null if unknown (no entry or stale).
 */
export function getProviderAuth(providerTag: string): Nullable<number> {

  const timestamp = providerAuth.get(providerTag);

  if(timestamp === undefined) {

    return null;
  }

  // Stale entry — older than TTL.
  if((Date.now() - timestamp) >= HEALTH_TTL) {

    return null;
  }

  return timestamp;
}

/**
 * Returns a snapshot of current health state for SSE initial payloads. Stale entries (older than HEALTH_TTL) are excluded.
 * @returns Snapshot with channel health and provider auth maps.
 */
export function getHealthSnapshot(): HealthSnapshot {

  const now = Date.now();
  const channels: Record<string, { providerTag: string; status: HealthStatus; timestamp: number }> = {};
  const providers: Record<string, number> = {};

  for(const [ key, entry ] of channelHealth) {

    if((now - entry.timestamp) < HEALTH_TTL) {

      channels[key] = { providerTag: entry.providerTag, status: entry.status, timestamp: entry.timestamp };
    }
  }

  for(const [ tag, timestamp ] of providerAuth) {

    if((now - timestamp) < HEALTH_TTL) {

      providers[tag] = timestamp;
    }
  }

  return { channels, providers };
}

/**
 * Subscribes a callback to receive health change events. Returns an unsubscribe function. Follows the same pattern as subscribeToStatus in statusEmitter.ts.
 * @param callback - Function to call when channel health or provider auth changes.
 * @returns A function to unsubscribe the callback.
 */
export function subscribeToHealth(callback: (event: HealthEvent) => void): () => void {

  const handler = (event: HealthEvent): void => { callback(event); };

  healthEmitter.on("healthChanged", handler);

  return (): void => {

    healthEmitter.off("healthChanged", handler);
  };
}
