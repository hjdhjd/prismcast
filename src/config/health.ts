/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * health.ts: Channel health and domain authentication state persistence for PrismCast.
 */
import { EventEmitter } from "node:events";
import { LOG } from "../utils/index.ts";
import type { Migration } from "./persistence.ts";
import type { Nullable } from "../types/index.ts";
import { createFileStore } from "./persistence.ts";
import { getHealthFilePath } from "./paths.ts";

/* This module tracks two kinds of observed state:
 *
 * 1. Channel health - did the last tune attempt for a specific channel succeed or fail? Each channel's health is independent. Switching a channel's domain resets its
 *    health indicator because the stored domain no longer matches.
 *
 * 2. Domain authentication - a per-domain tri-state: verified (green), needs-sign-in (red), or unknown (no entry). Verified is proven by success: one successful tune
 *    on any channel from a domain, or a validated channel discovery for the domain's service, turns the entire domain green. Needs-sign-in is proven by inspection: a
 *    discovery that returns zero channels leaves its page open long enough to positively classify a provider authentication wall, and only a confirmed classification
 *    sets the state - a page-shape reading of what is actually on screen, not the redirect-URL or login-page guessing that a heuristic-first design would need. Only
 *    success evidence changes a needs-sign-in entry: fresh verification overwrites it to verified, and a non-empty discovery that cannot prove paid access removes it
 *    back to unknown so the red state never outlives the wall it reported.
 *
 *    TTL semantics: verified entries expire after HEALTH_TTL because verification is aging evidence - a week-old success no longer proves the session is still valid.
 *    Needs-sign-in entries are exempt from the TTL: the entry describes a standing account condition that only new evidence resolves, so letting it lapse silently
 *    would just hide the problem it exists to surface. Boundedness survives the exemption because entries are keyed by the finite provider/domain set and every
 *    needs-sign-in entry is removed on the verified overwrite or the unproven-access deletion.
 *
 *    Downgrade note: an older binary performing a forward-compatible read of a v2 health file treats every domain entry (including needs-sign-in) as a truthy verified
 *    value and renders it green. The state is advisory and self-heals on the next mark or load, so this is accepted rather than defended against.
 *
 * State is persisted to health.json in the data directory with a 2-second debounce to avoid excessive writes during rapid tune attempts. Expired entries are filtered
 * out at load time, and the live maps are kept bounded for the life of the process by pruning expired entries at the write chokepoint (writeHealthState) and on
 * snapshot (getHealthSnapshot); the read paths additionally drop the single stale key they touch.
 */

// Types.

type HealthStatus = "failed" | "success";

/**
 * Authentication status for a domain. "verified" means success evidence exists within the TTL window; "needsLogin" means a confirmed provider auth wall was observed
 * and the user must sign in. The unknown state is represented by the absence of an entry.
 */
export type DomainAuthStatus = "needsLogin" | "verified";

/**
 * A domain's authentication state entry.
 */
export interface DomainAuthEntry {

  // The domain's authentication status.
  status: DomainAuthStatus;

  // Unix millisecond timestamp: when success evidence was recorded (verified) or when the auth wall was detected (needsLogin).
  timestamp: number;
}

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

  // Domain auth entries carry a status and a timestamp. The unknown state is the absence of an entry.
  domains: Record<string, DomainAuthEntry>;

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
  status: HealthStatus | "needsLogin";
  timestamp: number;
}

/**
 * Snapshot of current health state for SSE initial payload.
 */
export interface HealthSnapshot {

  channels: Record<string, { domain: string; status: HealthStatus; timestamp: number }>;
  domains: Record<string, DomainAuthEntry>;
}

// Health event emitter. Fires on every domain auth or channel health mutation so SSE clients receive real-time indicator updates.
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

/* Returns true when a domain auth entry has aged out. Only verified entries expire - verification is aging evidence with a shelf life. Needs-sign-in entries are
 * exempt: they describe a standing account condition that only new evidence (a sign-in followed by success, or an unproven-access discovery) resolves. This predicate
 * is the single expression of that status-aware TTL rule, shared by the load filter, the bulk prune, and the single-key read path.
 */
const isDomainAuthExpired = (entry: DomainAuthEntry): boolean => (entry.status === "verified") && isHealthExpired(entry.timestamp);

/* Converts a persisted domain auth value to the entry shape. Bare numbers are the legacy timestamp form, whose presence meant verified. The v1 to v2 schema migration
 * converts whole files through this function, and the load path applies it per-value as well: an older binary performing a forward-compatible read of a v2 file
 * writes bare numbers back into it without downgrading the schema version, so the migration never re-runs for that file and tolerance has to live at the load
 * boundary too. One converter, two call sites, one definition of the legacy form.
 */
const adoptDomainAuthValue = (value: number | DomainAuthEntry): DomainAuthEntry => (typeof value === "number") ? { status: "verified", timestamp: value } : value;

// In-memory state.

const channelHealth = new Map<string, ChannelHealthEntry>();

// Domain authentication state. The unknown state is the absence of an entry; verified and needs-sign-in are entry-valued per DomainAuthEntry.
const domainAuth = new Map<string, DomainAuthEntry>();

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

  for(const [ key, entry ] of domainAuth) {

    if(isDomainAuthExpired(entry)) {

      domainAuth.delete(key);
    }
  }
};

// Persistence.

/* Current schema version for health.json.
 *
 * Version history:
 *   1 - Original. Domain auth entries are bare Unix millisecond timestamps; the presence of a non-expired value means verified.
 *   2 - Status-bearing domain auth. Each domain value becomes { status, timestamp } so a domain can carry the needs-sign-in state alongside verified.
 */
const CURRENT_HEALTH_SCHEMA_VERSION = 2;

/* Declarative schema migrations. The file store framework runs these in order from the file's stored schemaVersion up to CURRENT_HEALTH_SCHEMA_VERSION, stamping the
 * new version and recording the description in migrationsApplied after each application. Apply functions mutate the data in place.
 */
const healthMigrations: Record<number, Migration<HealthState>> = {

  2: {

    apply: (data: HealthState): void => {

      /* Cast to the pre-migration shape: v1 files store bare-number timestamps that the parser passes through unmodified so this migration can transform them.
       * Values already in the entry shape pass through the converter untouched, which makes the migration idempotent over partially-converted content.
       */
      const domains = data.domains as Record<string, number | DomainAuthEntry>;

      for(const [ domain, value ] of Object.entries(domains)) {

        domains[domain] = adoptDomainAuthValue(value);
      }
    },
    description: "Convert bare-number domain auth timestamps to status-bearing entries"
  }
};

/* Transactional store for health.json. The parser tolerates the absence of either top-level data field so older files (and partial writes from prior versions
 * predating both keys) load cleanly. Legacy bare-number domain values pass through the parser unmodified - the framework runs migrations after parse, so the v1 to v2
 * migration is what transforms them. The beforeWrite hook emits framework metadata alongside the data; data fields are emitted unconditionally since the runtime
 * always populates them on every flush.
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
  migrations: healthMigrations,
  parse: (raw: string): HealthState => {

    // The domains field is typed as a value union here rather than as the current entry shape: v1 files hold bare numbers, and the migration (which runs after
    // parse) is what converts them. Parse must hand those values through unmodified or the migration would have nothing to transform.
    const parsed = JSON.parse(raw) as Partial<Omit<HealthState, "domains">> & { domains?: Record<string, number | DomainAuthEntry> };

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
      domains: (parsed.domains ?? {}) as Record<string, DomainAuthEntry>,
      migrationsApplied,
      schemaVersion
    };
  },
  path: getHealthFilePath,
  recordMigration: (data: HealthState, description: string): void => {

    data.migrationsApplied.push(description);
  },
  setSchemaVersion: (data: HealthState, version: number): void => { data.schemaVersion = version; }
});

/**
 * Loads the health state from health.json into memory. Expired entries are pruned during loading. Called once at startup from app.ts.
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

  for(const [ key, value ] of Object.entries(result.data.domains)) {

    // Adopt each value at the load boundary. Post-migration data is already entry-shaped; the conversion covers bare numbers written by an older binary into a
    // v2-stamped file, which the migration runner never revisits.
    const entry = adoptDomainAuthValue(value);

    if(!isDomainAuthExpired(entry)) {

      domainAuth.set(key, entry);
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

// Write chokepoints. Every domain auth transition flows through exactly one of the three private mutators below (verified, needs-sign-in, removal), so the entry
// shape and the flush-plus-emit sequence are each written in one place.

/* Sets a domain's auth entry to verified, schedules a flush, and emits the change event. The single write chokepoint for the verified transition - both
 * markChannelSuccess's markAuth path (channel-scoped event) and markDomainAuth (domain-scoped event with an empty channelKey) flow through here, so each caller
 * produces exactly one event and the entry it describes is constructed in exactly one place.
 */
function setDomainVerified(channelKey: string, domain: string, timestamp: number): void {

  domainAuth.set(domain, { status: "verified", timestamp });

  flushHealthState();
  healthEmitter.emit("healthChanged", { channelKey, domain, status: "success", timestamp } satisfies HealthEvent);
}

/* Removes a domain's auth entry, returning the domain to the unknown state, then schedules a flush and emits the change event. The single deletion chokepoint. The
 * emitted status is "failed": the event union has no member for unknown because no consumer discriminates on status (the health bridge re-renders affected rows from
 * current truth), and "failed" is the one value that collides with neither the verified transition ("success") nor the needs-sign-in transition ("needsLogin"),
 * keeping the three domain-scoped transitions distinguishable to subscribers.
 */
function removeDomainAuth(domain: string): void {

  domainAuth.delete(domain);

  flushHealthState();
  healthEmitter.emit("healthChanged", { channelKey: "", domain, status: "failed", timestamp: Date.now() } satisfies HealthEvent);
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

  // The verified write chokepoint emits the channel-scoped success event on our behalf, so both paths produce exactly one event per call with the same payload.
  if(markAuth) {

    setDomainVerified(channelKey, domain, now);

    return;
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

  setDomainVerified("", domain, Date.now());
}

/**
 * Records that a domain needs sign-in. Used when a confirmed provider authentication wall is observed during channel discovery. Overwrites any existing entry for
 * the domain, triggers a debounced flush, and emits a health event with an empty channelKey and the "needsLogin" status.
 * @param domain - The domain that needs sign-in.
 */
export function markDomainAuthRequired(domain: string): void {

  const now = Date.now();

  domainAuth.set(domain, { status: "needsLogin", timestamp: now });

  flushHealthState();
  healthEmitter.emit("healthChanged", { channelKey: "", domain, status: "needsLogin", timestamp: now } satisfies HealthEvent);
}

/**
 * Clears a domain's needs-sign-in entry, returning the domain to the unknown state. Used when a non-empty discovery proves the auth wall is gone but the provider's
 * validation cannot prove paid access (e.g., Sling's free-tier lineup appears without a subscription) - the red state must not outlive the wall it reported, but the
 * domain has not earned verified either. A no-op unless the domain's current state is needs-sign-in: verified entries are never deleted by unproven-access evidence.
 * @param domain - The domain whose needs-sign-in entry should be cleared.
 */
export function clearDomainAuthRequirement(domain: string): void {

  if(domainAuth.get(domain)?.status !== "needsLogin") {

    return;
  }

  removeDomainAuth(domain);
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
 * Returns a domain's authentication state entry, or null when the domain is unknown (no entry, or a verified entry older than the TTL). Needs-sign-in entries are
 * exempt from the TTL per the module's status-aware expiry rule.
 * @param domain - The domain to check.
 * @returns The domain's auth entry, or null if unknown.
 */
export function getDomainAuthState(domain: string): Nullable<DomainAuthEntry> {

  const entry = domainAuth.get(domain);

  if(!entry) {

    return null;
  }

  // Stale entry - only verified entries age out. We delete it here as well as returning null so that a domain that is read but never re-marked does not linger in
  // memory; this read path touches exactly one key, so we prune just that key rather than paying for a full-map scan.
  if(isDomainAuthExpired(entry)) {

    domainAuth.delete(domain);

    return null;
  }

  return { status: entry.status, timestamp: entry.timestamp };
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
  const domains: Record<string, DomainAuthEntry> = {};

  for(const [ key, entry ] of channelHealth) {

    channels[key] = { domain: entry.domain, status: entry.status, timestamp: entry.timestamp };
  }

  for(const [ domainKey, entry ] of domainAuth) {

    domains[domainKey] = { status: entry.status, timestamp: entry.timestamp };
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
