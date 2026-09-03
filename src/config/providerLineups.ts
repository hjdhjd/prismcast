/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * providerLineups.ts: Persisted provider channel lineups for PrismCast.
 */
import type { DiscoveredChannel, Nullable } from "../types/index.ts";
import { LOG, formatError } from "../utils/index.ts";
import type { Migration } from "./persistence.ts";
import { createFileStore } from "./persistence.ts";
import { getProviderLineupsFilePath } from "./paths.ts";

/* A provider's channel lineup lives in memory for exactly as long as the browser session that discovered it. When a precache walk comes back empty - a lazy rail
 * that never populated, a guide page broken for the afternoon - the provider is untunable for the life of the process, even though known-good watch URLs existed
 * seconds before the restart. This module gives those lineups a life beyond the session: what a successful discovery found is written to provider-lineups.json,
 * and a later boot whose own discovery has not run (or has failed) reads it back.
 *
 * The posture is trust-nothing, verify-on-use. A persisted row is a hint, never an authority: nothing here is treated as a fresh lineup, nothing skips a
 * verification, and a hint that no longer works costs one failed establishment and a guide-path retry rather than a broken tune. That is the whole trade the
 * feature makes, and it is why the store carries no expiry policy - a stale hint that fails is self-correcting through the eviction below, while an expiry would
 * throw away good hints precisely when a degraded session needs them most.
 *
 * Which fields survive a browser session is a per-provider judgment, so the lineup rows the store holds come from the providers themselves through the optional
 * ProviderModule.exportDurableLineup hook. Providers that tune in-page have no durable watch URL to offer and export nothing beyond identity, which is why the
 * watchUrl field is optional rather than required.
 *
 * Neither write path ever throws. A lineup write is the durable half of a discovery that has already succeeded, so a filesystem failure here must warn and let
 * the discovery stand rather than fail the walk that produced it.
 */

// Types.

/**
 * One channel of a persisted provider lineup. The identity half is derived from DiscoveredChannel rather than re-declared, so the persisted shape cannot drift
 * from the shape discovery produces. The watch URL is the durable tuning half: present for providers whose direct-tune address survives a browser session,
 * absent for the in-page-tuning platforms that have no such address.
 */
export interface PersistedLineupChannel extends Pick<DiscoveredChannel, "channelSelector" | "name"> {

  // The provider's direct watch URL for this channel, when the provider exports one. Absent for providers that tune by interacting with their guide.
  watchUrl?: string;
}

/**
 * The on-disk envelope for provider-lineups.json: one slice per provider slug, plus the schema version the file store's migration runner reads.
 */
interface ProviderLineupsFileData {

  providers: Record<string, PersistedLineupChannel[]>;

  // Schema version. Managed by the file store framework's migration runner.
  schemaVersion: number;
}

// Persistence.

/* Current schema version for provider-lineups.json.
 *
 * Version history:
 *   1 - Original. Each provider slug maps to an array of lineup rows carrying the channel identity and an optional durable watch URL.
 */
const CURRENT_LINEUPS_SCHEMA_VERSION = 1;

/* Declarative schema migrations, keyed by target schema version. The registry is present and empty at version 1: the file store framework arms its migration
 * runner only when currentSchemaVersion, getSchemaVersion, setSchemaVersion, and migrations are all declared, so declaring the whole set now is what makes the
 * version marker on disk mean something. A version marker without a runner behind it is inert.
 */
const providerLineupMigrations: Record<number, Migration<ProviderLineupsFileData>> = {};

// The lineups this process knows about, keyed by provider slug. Hydrated once at boot by loadProviderLineups and updated at each write. Every read path here is
// synchronous against this map, because the consumers are on the tune and page-render paths where an await would buy nothing.
const lineups = new Map<string, PersistedLineupChannel[]>();

/* Transactional store for provider-lineups.json. The parser tolerates a missing providers field so a file written by a partial release, or hand-edited, still
 * loads. There is no load gate of the kind health.json carries: every write here names the one provider slice it replaces rather than serializing whole in-memory
 * state, so a write that lands before the initial load cannot overwrite anything it has not read.
 */
const providerLineupsStore = createFileStore<ProviderLineupsFileData>({

  // The on-disk envelope, stated in one place. Both fields are always populated by the runtime, so both are emitted unconditionally.
  beforeWrite: (data: ProviderLineupsFileData): unknown => ({ providers: data.providers, schemaVersion: data.schemaVersion }),
  currentSchemaVersion: CURRENT_LINEUPS_SCHEMA_VERSION,
  defaultValue: (): ProviderLineupsFileData => ({ providers: {}, schemaVersion: CURRENT_LINEUPS_SCHEMA_VERSION }),
  getSchemaVersion: (data: ProviderLineupsFileData): number => data.schemaVersion,
  label: "provider lineups",
  migrations: providerLineupMigrations,
  parse: (raw: string): ProviderLineupsFileData => {

    const parsed = JSON.parse(raw) as Partial<ProviderLineupsFileData>;

    // A file predating the version field is treated as version 1, matching the framework's convention for every other store.
    let schemaVersion = 1;

    if((typeof parsed.schemaVersion === "number") && Number.isFinite(parsed.schemaVersion) && (parsed.schemaVersion >= 1)) {

      schemaVersion = Math.floor(parsed.schemaVersion);
    }

    return { providers: parsed.providers ?? {}, schemaVersion };
  },
  path: getProviderLineupsFilePath,
  setSchemaVersion: (data: ProviderLineupsFileData, version: number): void => { data.schemaVersion = version; }
});

/**
 * Writes one provider's slice to disk, replacing whatever the file held for that slug. This is the single write chokepoint, and it never rejects: a lineup write
 * is the durable half of a discovery that has already succeeded, so a filesystem failure is warned about and absorbed rather than propagated into the walk or the
 * tune-failure path that produced it.
 * @param slug - The provider slug whose slice to replace.
 * @param channels - The lineup rows to persist for that provider.
 * @returns A promise that resolves once the write has committed or failed.
 */
async function writeProviderSlice(slug: string, channels: PersistedLineupChannel[]): Promise<void> {

  try {

    await providerLineupsStore.mutate((data) => {

      data.providers[slug] = channels;
    });
  } catch(error) {

    LOG.warn("Failed to persist the channel lineup for %s: %s.", slug, formatError(error));
  }
}

/**
 * Loads the persisted provider lineups from provider-lineups.json into memory. Called once at startup from app.ts, before the browser launches, so the first tune
 * of a boot whose precache has not run yet already has hints to work with.
 * @returns A promise that resolves once the in-memory lineups are hydrated.
 */
export async function loadProviderLineups(): Promise<void> {

  const result = await providerLineupsStore.read();

  lineups.clear();

  for(const [ slug, channels ] of Object.entries(result.data.providers)) {

    lineups.set(slug, channels);
  }

  if(lineups.size > 0) {

    LOG.info("Loaded persisted channel lineups for %d provider%s.", lineups.size, (lineups.size === 1) ? "" : "s");
  }
}

/**
 * Records a provider's discovered lineup, replacing that provider's slice wholesale. An empty lineup is a no-op: a walk that found nothing is not evidence that
 * the provider has no channels, and letting it through would erase the very hints a failed discovery most needs.
 * @param slug - The provider slug the lineup belongs to.
 * @param channels - The lineup rows the discovery produced.
 * @returns A promise that resolves once the write has committed or failed. It never rejects.
 */
export async function persistProviderLineup(slug: string, channels: PersistedLineupChannel[]): Promise<void> {

  if(channels.length === 0) {

    return;
  }

  lineups.set(slug, channels);

  await writeProviderSlice(slug, channels);
}

/**
 * Returns the persisted lineup for a provider, or null when nothing has been persisted for that slug. The array is the module's own - callers read it and project
 * from it, never mutate it.
 * @param slug - The provider slug to look up.
 * @returns The persisted lineup rows, or null.
 */
export function getPersistedLineup(slug: string): Nullable<PersistedLineupChannel[]> {

  return lineups.get(slug) ?? null;
}

/**
 * Returns the persisted direct watch URL for a channel, or null when the provider has no persisted lineup, the channel is not in it, or the provider does not
 * export watch URLs at all. Selector matching is case-insensitive, matching the lowercased-key convention every provider's live cache uses.
 * @param slug - The provider slug to look up.
 * @param channelSelector - The channel selector to match.
 * @returns The persisted watch URL, or null.
 */
export function getPersistedWatchUrl(slug: string, channelSelector: string): Nullable<string> {

  const channels = lineups.get(slug);

  if(!channels) {

    return null;
  }

  const target = channelSelector.toLowerCase();

  return channels.find((channel) => channel.channelSelector.toLowerCase() === target)?.watchUrl ?? null;
}

/**
 * Drops the persisted watch URL for a channel that failed to produce a working stream, keeping the identity row so the lineup still describes what the provider
 * carries. Synchronous in memory with a fire-and-forget write behind it: this runs on a tune-failure path, where the next step is a retry that must not wait on a
 * disk write, and the write itself never rejects.
 * @param slug - The provider slug the channel belongs to.
 * @param channelSelector - The channel selector whose watch URL is no longer trusted.
 */
export function evictPersistedWatchUrl(slug: string, channelSelector: string): void {

  const channels = lineups.get(slug);

  if(!channels) {

    return;
  }

  const target = channelSelector.toLowerCase();
  const entry = channels.find((channel) => channel.channelSelector.toLowerCase() === target);

  if(!entry?.watchUrl) {

    return;
  }

  delete entry.watchUrl;

  void writeProviderSlice(slug, channels);
}
