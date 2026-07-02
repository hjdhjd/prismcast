/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * health.ts: Channel health and domain authentication state persistence for PrismCast.
 */
import { EventEmitter } from "node:events";
import { LOG } from "../utils/index.ts";
import type { Nullable } from "../types/index.ts";
import { createFileStore } from "./persistence.ts";
import { getHealthFilePath } from "./paths.ts";

/* This module tracks two kinds of observed state:
 *
 * 1. Channel health - did the last tune attempt for a specific channel succeed or fail? Each channel's health is independent. Switching a channel's domain resets its
 *    health indicator because the stored domain no longer matches.
 *
 * 2. Domain authentication - has the user successfully tuned at least one channel on a given domain? Authentication is proven by success: one successful tune on any
 *    channel from a domain turns the entire domain green. There is no "red" state - domains are either verified (green) or unknown (no entry / TTL expired). This
 *    avoids brittle heuristics for detecting auth failures (domain redirects, login page detection, MVPD picker walls) in favor of a single reliable signal: did video
 *    actually start?
 *
 * State is persisted to health.json in the data directory with a 2-second debounce to avoid excessive writes during rapid tune attempts. Entries older than 7 days
 * are filtered out at load time, and the live maps are kept bounded for the life of the process by pruning expired entries at the write chokepoint (writeHealthState)
 * and on snapshot (getHealthSnapshot); the read paths additionally drop the single stale key they touch.
 */

// Types.

type HealthStatus = "failed" | "success";

interface ChannelHealthEntry {

  // The domain at the time of recording. Used to detect domain changes - if the current domain differs, the entry is stale.
  domain: string;

  // Whether the last tune succeeded or failed.
  status: HealthStatus;

  // Unix millisecond timestamp for TTL expiry.
  timestamp: number;
}

interface HealthState {

  channels: Record<string, ChannelHealthEntry>;

  // Domain auth entries are just timestamps. The presence of a non-expired entry means "verified authenticated."
  domains: Record<string, number>;

  // Audit trail of schema migrations applied to this file. Managed by the file store framework's migration runner.
  migrationsApplied: string[];

  // Schema version. Managed by the file store framework's migration runner. Files predating this field are treated as version 1.
  schemaVersion: number;
}

/**
 * Event payload emitted when channel health or domain auth state changes.
 */
export interface HealthEvent {

  channelKey: string;
  domain: string;
  status: HealthStatus;
  timestamp: number;
}

/**
 * Snapshot of current health state for SSE initial payload.
 */
export interface HealthSnapshot {

  channels: Record<string, { domain: string; status: HealthStatus; timestamp: number }>;
  domains: Record<string, number>;
}

// Health event emitter. Fires on every markChannelSuccess, markChannelFailure, and markDomainAuth call so SSE clients receive real-time indicator updates.
const healthEmitter = new EventEmitter();

// Every /streams/status SSE subscriber registers a listener on this emitter, so we lift the cap from Node's default of 10 to 100 to accommodate many concurrent
// subscribers without triggering a spurious MaxListenersExceededWarning.
healthEmitter.setMaxListeners(100);

// Constants.

// Entries older than 7 days are pruned at load time.
const HEALTH_TTL = 7 * 24 * 60 * 60 * 1000;

// Debounce interval for writes to health.json.
const FLUSH_DELAY = 2000;

// Returns true if the given timestamp is older than HEALTH_TTL.
const isHealthExpired = (timestamp: number): boolean => (Date.now() - timestamp) >= HEALTH_TTL;

// In-memory state.

const channelHealth = new Map<string, ChannelHealthEntry>();

// Domain auth is proven by success only. The presence of a non-expired timestamp means the user has successfully tuned at least one channel on the domain.
const domainAuth = new Map<string, number>();

// Debounce timer for flushHealthState().
let flushTimer: Nullable<ReturnType<typeof setTimeout>> = null;

/* Drops every expired entry from the in-memory maps so the maps stay bounded for the life of the process. Without this the maps would only ever grow - read paths
 * only delete the single stale key they touch, never the untouched remainder, so a channel tuned once and never revisited would linger forever. We run this at the
 * single write chokepoint (writeHealthState) so the on-disk record never carries stale entries, and again from getHealthSnapshot so a long-lived process that stops
 * mutating still sheds its expired state.
 */
const pruneExpiredEntries = (): void => {

  for(const [ key, entry ] of channelHealth) {

    if(isHealthExpired(entry.timestamp)) {

      channelHealth.delete(key);
    }
  }

  for(const [ key, timestamp ] of domainAuth) {

    if(isHealthExpired(timestamp)) {

      domainAuth.delete(key);
    }
  }
};

// Persistence.

/* Current schema version for health.json. No migrations are required today because the parser tolerates absent top-level fields, so legacy files load cleanly, but the
 * framework metadata is still maintained so future migrations can be added trivially.
 */
const CURRENT_HEALTH_SCHEMA_VERSION = 1;

/* Transactional store for health.json. The parser tolerates the absence of either top-level data field so older files (and partial writes from prior versions
 * predating both keys) load cleanly. The beforeWrite hook emits framework metadata alongside the data; data fields are emitted unconditionally since the
 * runtime always populates them on every flush.
 */
const healthStore = createFileStore<HealthState>({

  beforeWrite: (data: HealthState): unknown => {

    const output: Record<string, unknown> = { channels: data.channels, domains: data.domains, schemaVersion: data.schemaVersion };

    if(data.migrationsApplied.length > 0) {

      output["migrationsApplied"] = data.migrationsApplied;
    }

    return output;
  },
  currentSchemaVersion: CURRENT_HEALTH_SCHEMA_VERSION,
  defaultValue: (): HealthState => ({ channels: {}, domains: {}, migrationsApplied: [], schemaVersion: CURRENT_HEALTH_SCHEMA_VERSION }),
  getSchemaVersion: (data: HealthState): number => data.schemaVersion,
  label: "health state",
  parse: (raw: string): HealthState => {

    const parsed = JSON.parse(raw) as Partial<HealthState>;

    let schemaVersion = 1;

    if((typeof parsed.schemaVersion === "number") && Number.isFinite(parsed.schemaVersion) && (parsed.schemaVersion >= 1)) {

      schemaVersion = Math.floor(parsed.schemaVersion);
    }

    const migrationsApplied: string[] = [];

    if(Array.isArray(parsed.migrationsApplied)) {

      for(const entry of parsed.migrationsApplied) {

        if(typeof entry === "string") {

          migrationsApplied.push(entry);
        }
      }
    }

    return {

      channels: parsed.channels ?? {},
      domains: parsed.domains ?? {},
      migrationsApplied,
      schemaVersion
    };
  },
  path: getHealthFilePath,
  setSchemaVersion: (data: HealthState, version: number): void => { data.schemaVersion = version; }
});

/**
 * Loads the health state from health.json into memory. Entries older than HEALTH_TTL are pruned during loading. Called once at startup from app.ts.
 */
export async function loadHealthState(): Promise<void> {

  const result = await healthStore.read();

  channelHealth.clear();
  domainAuth.clear();

  for(const [ key, entry ] of Object.entries(result.data.channels)) {

    if(!isHealthExpired(entry.timestamp)) {

      channelHealth.set(key, entry);
    }
  }

  for(const [ key, timestamp ] of Object.entries(result.data.domains)) {

    if(!isHealthExpired(timestamp)) {

      domainAuth.set(key, timestamp);
    }
  }

  if(result.recoveredFromBackup) {

    LOG.info("Health state was recovered from backup after a corrupt main file.");
  }

  const channelCount = channelHealth.size;
  const domainCount = domainAuth.size;

  if((channelCount > 0) || (domainCount > 0)) {

    LOG.info("Loaded health state for %d channels and %d domains.", channelCount, domainCount);
  }
}

/**
 * Writes the current in-memory health maps to health.json via the transactional file store. Health writes always emit the full state - there is no per-key delta
 * semantic. Shared by the debounced flushHealthState (on the timer) and the immediate flushHealthStateNow (on shutdown), so both go through one write definition.
 * @returns A promise that resolves when the store has committed the write.
 */
async function writeHealthState(): Promise<void> {

  // Shed expired entries before serializing so the on-disk record never carries stale state and the in-memory maps stay bounded across the process lifetime. This is
  // the periodic chokepoint - every mark* mutation schedules a flush through here, so pruning at this point keeps both memory and disk free of expired entries.
  pruneExpiredEntries();

  await healthStore.mutate((state) => {

    state.channels = Object.fromEntries(channelHealth);
    state.domains = Object.fromEntries(domainAuth);
  });
}

/**
 * Writes the current in-memory health state to health.json via the transactional file store. Debounced - multiple calls within FLUSH_DELAY are coalesced into a
 * single write. The store's serialization queue handles the case where a flush fires while a prior mutation is still in flight, so no in-module write-in-progress
 * tracking is needed.
 */
function flushHealthState(): void {

  if(flushTimer) {

    clearTimeout(flushTimer);
  }

  flushTimer = setTimeout(() => {

    flushTimer = null;

    void writeHealthState().catch((error: unknown) => {

      LOG.warn("Failed to write health state: %s.", (error instanceof Error) ? error.message : String(error));
    });
  }, FLUSH_DELAY);
}

/**
 * Flushes the health state to disk immediately, cancelling any pending debounced write and awaiting the on-disk write. Called during graceful shutdown so the final
 * health update is durably written even when the process exits inside the FLUSH_DELAY debounce window. Writing unconditionally (rather than only when a debounce is
 * pending) also drains any in-flight mutation behind the store's serialization queue, guaranteeing the on-disk record matches the final in-memory state.
 * @returns A promise that resolves when the final health-state write has committed.
 */
export async function flushHealthStateNow(): Promise<void> {

  if(flushTimer) {

    clearTimeout(flushTimer);
    flushTimer = null;
  }

  // Best-effort write: a store failure during shutdown is logged but must not propagate, or it would abort the rest of the shutdown teardown (the caller awaits this
  // without its own guard). This mirrors the debounced path's .catch above - both write paths log and continue on error rather than throwing.
  try {

    await writeHealthState();
  } catch(error) {

    LOG.warn("Failed to write health state during shutdown: %s.", (error instanceof Error) ? error.message : String(error));
  }
}

// Public API.

/**
 * Records a successful tune for a channel. Sets the channel's health to "success" and optionally marks the domain as verified (authenticated). Triggers a debounced
 * flush. The markAuth parameter allows callers to skip domain auth marking when a successful tune does not prove paid access (e.g., Sling Freestream channels succeed
 * without a subscription).
 * @param channelKey - The channel key (canonical key, e.g., "nbc").
 * @param domain - The auth domain for the currently selected service variant.
 * @param markAuth - Whether to also mark the domain as authenticated (default: true).
 */
export function markChannelSuccess(channelKey: string, domain: string, markAuth = true): void {

  const now = Date.now();

  channelHealth.set(channelKey, { domain, status: "success", timestamp: now });

  if(markAuth) {

    domainAuth.set(domain, now);
  }

  flushHealthState();
  healthEmitter.emit("healthChanged", { channelKey, domain, status: "success", timestamp: now } satisfies HealthEvent);
}

/**
 * Records a domain as authenticated without a specific channel context. Used when a domain action (e.g., precaching) proves the domain is accessible and logged in,
 * even though no channel was tuned. Triggers a debounced flush and emits a health event with an empty channelKey.
 * @param domain - The domain to mark as authenticated.
 */
export function markDomainAuth(domain: string): void {

  const now = Date.now();

  domainAuth.set(domain, now);

  flushHealthState();
  healthEmitter.emit("healthChanged", { channelKey: "", domain, status: "success", timestamp: now } satisfies HealthEvent);
}

/**
 * Records a failed tune for a channel. Sets the channel's health to "failed". Does not affect domain auth - a single channel failure doesn't prove the domain is
 * unauthenticated. Triggers a debounced flush.
 * @param channelKey - The channel key (canonical key, e.g., "nbc").
 * @param domain - The auth domain for the currently selected service variant.
 */
export function markChannelFailure(channelKey: string, domain: string): void {

  const now = Date.now();

  channelHealth.set(channelKey, { domain, status: "failed", timestamp: now });

  flushHealthState();
  healthEmitter.emit("healthChanged", { channelKey, domain, status: "failed", timestamp: now } satisfies HealthEvent);
}

/**
 * Returns the health status and timestamp for a channel. Returns null if no entry exists, the entry is stale (older than 7 days), or the stored domain doesn't match
 * the current one (domain changed).
 * @param channelKey - The channel key (canonical key, e.g., "nbc").
 * @param domain - The auth domain for the currently selected service variant.
 * @returns Object with status and timestamp, or null if unknown.
 */
export function getChannelHealth(channelKey: string, domain: string): Nullable<{ status: HealthStatus; timestamp: number }> {

  const entry = channelHealth.get(channelKey);

  if(!entry) {

    return null;
  }

  // Stale entry - older than TTL. We delete it here as well as returning null so that a key that is read but never re-marked does not linger in memory; this read path
  // touches exactly one key, so we prune just that key rather than paying for a full-map scan.
  if(isHealthExpired(entry.timestamp)) {

    channelHealth.delete(channelKey);

    return null;
  }

  // Domain was switched - the stored result is for a different domain.
  if(entry.domain !== domain) {

    return null;
  }

  return { status: entry.status, timestamp: entry.timestamp };
}

/**
 * Returns the timestamp when a domain was last verified as authenticated, or null if unknown. Verification is proven by at least one successful tune within the TTL
 * window. A non-null return means the domain is verified; the value is the Unix millisecond timestamp of the most recent successful tune.
 * @param domain - The domain to check.
 * @returns Timestamp of last verification, or null if unknown (no entry or stale).
 */
export function getDomainAuth(domain: string): Nullable<number> {

  const timestamp = domainAuth.get(domain);

  if(timestamp === undefined) {

    return null;
  }

  // Stale entry - older than TTL. We delete it here as well as returning null so that a domain that is read but never re-marked does not linger in memory; this read
  // path touches exactly one key, so we prune just that key rather than paying for a full-map scan.
  if(isHealthExpired(timestamp)) {

    domainAuth.delete(domain);

    return null;
  }

  return timestamp;
}

/**
 * Returns a snapshot of current health state for SSE initial payloads. Stale entries (older than HEALTH_TTL) are excluded.
 * @returns Snapshot with channel health and domain auth maps.
 */
export function getHealthSnapshot(): HealthSnapshot {

  // Snapshots iterate the entire map, so this is the natural read-side chokepoint to shed expired entries. A long-lived process that stops mutating still drops its
  // stale state on the next SSE client connection, which is when snapshots are taken.
  pruneExpiredEntries();

  const channels: Record<string, { domain: string; status: HealthStatus; timestamp: number }> = {};
  const domains: Record<string, number> = {};

  for(const [ key, entry ] of channelHealth) {

    channels[key] = { domain: entry.domain, status: entry.status, timestamp: entry.timestamp };
  }

  for(const [ domainKey, timestamp ] of domainAuth) {

    domains[domainKey] = timestamp;
  }

  return { channels, domains };
}

/**
 * Subscribes a callback to receive health change events. Returns an unsubscribe function. Follows the same pattern as subscribeToStatus in statusEmitter.ts.
 * @param callback - Function to call when channel health or domain auth changes.
 * @returns A function to unsubscribe the callback.
 */
export function subscribeToHealth(callback: (event: HealthEvent) => void): () => void {

  const handler = (event: HealthEvent): void => { callback(event); };

  healthEmitter.on("healthChanged", handler);

  return (): void => {

    healthEmitter.off("healthChanged", handler);
  };
}
