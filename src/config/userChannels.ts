/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userChannels.ts: User channel file management for PrismCast.
 */
import { CHANNEL_BINDING_KEYS, CHANNEL_IDENTITY_KEYS, DELTA_ELIGIBLE_BINDING_KEYS, DELTA_ELIGIBLE_IDENTITY_KEYS } from "../types/index.ts";
import type { Channel, ChannelDelta, ChannelIdentity, ChannelListingEntry, ChannelMap, ChannelSortField, CustomizableField, ResolvedChannel, ResolvedChannelMap,
  SortDirection, StoredChannel, StoredChannelMap } from "../types/index.ts";
import { FileStoreParseError, type Migration, type ValidationIssue, createFileStore } from "./persistence.ts";
import { LOG, containsNonPrintable, sanitizeString } from "../utils/index.ts";
import { PREDEFINED_CHANNELS, PREDEFINED_TAGS } from "../channels/index.ts";
import { buildServiceGroups, getAllServiceTags, getResolvedChannel, isChannelAvailableByService, isServiceVariant,
  resolveServiceKey, setEnabledServices, setServiceSelections } from "./services.ts";
import { CONFIG } from "./index.ts";
import fs from "node:fs";
import { getChannelsFilePath } from "./paths.ts";
import { mutateConfig } from "./userConfig.ts";

const { promises: fsPromises } = fs;

/* PrismCast allows users to define custom channels in channels.json inside the data directory. These user channels are merged with the predefined channels,
 * with user channels taking precedence when there are key conflicts. This allows users to:
 *
 * 1. Add new channels not included in the default set
 * 2. Override predefined channels with custom URLs or profiles
 * 3. Customize channel metadata like display names or station IDs
 *
 * The channels file is separate from the config file to keep channel definitions independent of server settings. Changes made through the web UI take effect
 * immediately for new stream requests.
 */

/* User channels have the same structure as predefined channels. The UserChannel type is equivalent to Channel but defined here for clarity in the context of user
 * configuration.
 */

/**
 * User-defined channel with all channel properties.
 */
export type UserChannel = Channel;

/**
 * Map of channel keys to stored channel data (full definitions or deltas). This is the raw file format for channels.json.
 */
export type UserChannelMap = StoredChannelMap;

/**
 * Tag registry state persisted in channels.json alongside channel data. Tracks user-created tags and user-deleted predefined tags. The runtime tag vocabulary
 * is computed as: (PREDEFINED_TAGS - deletedTags) + tags, sorted alphabetically.
 */
export interface TagRegistry {

  // Predefined tags the user has deleted from their vocabulary. These tags still exist on predefined channel definitions but are hidden from the UI,
  // unassignable, and excluded from ?tag= query validation. Restoring a deleted tag removes it from this list.
  deletedTags: string[];

  // User-created tags added via the tag management editor. These extend the predefined vocabulary with custom organizational categories. Tags are stored
  // with their original casing as entered by the user. Comparisons throughout the system are case-insensitive.
  tags: string[];
}

/**
 * Result of loading user channels from the file.
 */
export interface UserChannelsLoadResult {

  // The loaded user channels (empty object if file doesn't exist or parse error). Already migrated to the current schema version by the file store framework.
  channels: StoredChannelMap;

  // True if the file exists but contains invalid JSON.
  parseError: boolean;

  // Error message if parseError is true.
  parseErrorMessage?: string;

  // True when the main file failed to parse and a usable copy was successfully recovered from the .bak rotation. Mirrors the framework's read-result flag
  // (FileStoreReadResult.recoveredFromBackup) so callers can surface a UI banner or log a recovery event without reaching into the framework's compound result.
  recoveredFromBackup: boolean;

  // Service selections loaded from the file (canonical key -> service variant key).
  serviceSelections: Record<string, string>;

  // Tag registry state (user-created tags and deleted predefined tags).
  tagRegistry: TagRegistry;
}

/* The channels file path is resolved via the centralized paths module (config/paths.ts). The data directory is initialized at startup before channel loading.
 */

/**
 * Returns the path to the user channels file.
 * @returns The absolute path to channels.json inside the data directory.
 */
export function getUserChannelsFilePath(): string {

  return getChannelsFilePath();
}

/* These functions handle reading and writing the channels file. All operations are async and handle errors gracefully.
 */

// Module-level storage for loaded user channels. This is populated at startup and used by getAllChannels(). Entries can be full Channel definitions or
// ChannelDelta overrides for predefined channels.
let loadedUserChannels: StoredChannelMap = {};
let userChannelsParseError = false;
let userChannelsParseErrorMessage: string | undefined;

// Module-level tag registry state. Tracks user-created tags and user-deleted predefined tags. The runtime vocabulary is computed by getActiveTagVocabulary().
let loadedTagRegistry: TagRegistry = { deletedTags: [], tags: [] };

/**
 * Returns whether the user channels file had a parse error.
 * @returns True if the channels file exists but contains invalid JSON.
 */
export function hasChannelsParseError(): boolean {

  return userChannelsParseError;
}

/**
 * Returns the parse error message if the channels file had a parse error.
 * @returns The error message or undefined.
 */
export function getChannelsParseErrorMessage(): string | undefined {

  return userChannelsParseErrorMessage;
}

/* Transactional store for channels.json. The store uses a compound type that carries channel data alongside metadata (serviceSelections, tagRegistry). The parse
 * function extracts all three from the JSON file. The mutateChannels() wrapper normalizes predefined channel deltas before the write and adapts the compound
 * type so callers work directly with StoredChannelMap. The beforeWrite hook injects metadata from module state.
 */

/* Current schema version for channels.json. Migrations are declared in channelsMigrations below; the framework runs them in order from the file's stored
 * version up to this constant, stamps the new version after each, and records the audit trail in migrationsApplied.
 *
 * Version history:
 *   1 - Pre-delta-model user variants. Identity fields stored redundantly on variants and stripped eagerly at load time.
 *   2 - Delta-model user variants. Variants store only fields that differ from their canonical. The v1 -> v2 migration converts legacy-shaped variants by
 *       stamping canonicalKey on entries whose identity fields match the canonical (shape-compatible with a legitimate variant) and skipping entries whose
 *       fields differ (user standalones that happened to share a hyphenated key with a predefined canonical).
 *   3 - Foxone-era service naming. The v2 -> v3 migration renames "foxcom" -> "foxone" in service selections and channel keys, and the special-case
 *       "fox-site" -> "fox-foxone" rename for the FoxOne variant briefly mis-keyed in v1.8.0. Bumping this version also flushes parser-side legacy field
 *       cleanups (provider -> service field, providerSelections -> serviceSelections key) to disk on the same write that lands the foxone rename.
 */
const CURRENT_CHANNELS_SCHEMA_VERSION = 3;

/**
 * Compound data type for the channels file store. Carries channel entries alongside the metadata keys that are stored in the same JSON file. The framework
 * uses schemaVersion and migrationsApplied for migration tracking; everything else is application-owned.
 */
interface ChannelsFileData {

  channels: StoredChannelMap;
  migrationsApplied: string[];
  schemaVersion: number;
  serviceSelections: Record<string, string>;
  tagRegistry: TagRegistry;
}

/**
 * Parses raw channels.json content into the compound data type. Extracts channel entries, service selections, tag registry, and framework metadata
 * (schemaVersion, migrationsApplied) from the top-level JSON object. Also performs structural normalization for legacy field/key naming so consumer code only
 * sees the current shape; the schemaVersion progression governs when the cleanup is persisted to disk via the migration framework.
 * @param raw - The raw JSON string from the file.
 * @returns The parsed compound data.
 */
function parseChannelsFile(raw: string): ChannelsFileData {

  const parsed = JSON.parse(raw) as Record<string, unknown>;
  const channels: StoredChannelMap = {};
  const serviceSelections: Record<string, string> = {};
  const tagRegistry: TagRegistry = { deletedTags: [], tags: [] };
  const migrationsApplied: string[] = [];

  // Files predating the schemaVersion field are treated as version 1. The migration runner in the file store framework upgrades them to the current version.
  let schemaVersion = 1;

  for(const [ key, value ] of Object.entries(parsed)) {

    if(key === "schemaVersion") {

      // Tolerate bad data by falling back to version 1 - any plausibly-legacy value is fine, since migrations are idempotent when no work is needed.
      if((typeof value === "number") && Number.isFinite(value) && (value >= 1)) {

        schemaVersion = Math.floor(value);
      }
    } else if(key === "migrationsApplied") {

      // Audit trail of migrations that have already been applied to this file. Strings only; non-string entries are dropped silently.
      if(Array.isArray(value)) {

        for(const entry of value) {

          if(typeof entry === "string") {

            migrationsApplied.push(entry);
          }
        }
      }
    } else if((key === "serviceSelections") || (key === "providerSelections")) {

      // Copy service selections if it is an object. Accepts the legacy "providerSelections" key for structural compatibility - the v2 -> v3 schema migration
      // ensures the file is rewritten under the current key on next persist.
      if((typeof value === "object") && (value !== null) && !Array.isArray(value)) {

        for(const [ selKey, selValue ] of Object.entries(value)) {

          if(typeof selValue === "string") {

            serviceSelections[selKey] = selValue;
          }
        }
      }
    } else if(key === "tagRegistry") {

      // Extract tag registry with defensive validation. String array fields (tags, deletedTags) drop non-string elements silently.
      if((typeof value === "object") && (value !== null) && !Array.isArray(value)) {

        const rawRegistry = value as Record<string, unknown>;

        if(Array.isArray(rawRegistry["tags"])) {

          tagRegistry.tags = rawRegistry["tags"].filter((t): t is string => typeof t === "string").sort();
        }

        if(Array.isArray(rawRegistry["deletedTags"])) {

          tagRegistry.deletedTags = rawRegistry["deletedTags"].filter((t): t is string => typeof t === "string").sort();
        }
      }
    } else if((typeof value === "object") && (value !== null) && !Array.isArray(value)) {

      // It is a channel definition or delta override.
      channels[key] = value;
    }
  }

  // Structural normalization: rename legacy "provider" field to "service" on channel entries. The old field was used as a display name override for the
  // service selection dropdown; new installs always write "service". The schema migration to v3 stamps the version after this normalization is in memory,
  // ensuring the persist on next write drops the legacy field from disk.
  for(const channel of Object.values(channels)) {

    const legacy = channel as Record<string, unknown>;

    if("provider" in legacy) {

      if(!("service" in legacy)) {

        legacy["service"] = legacy["provider"];
      }

      Reflect.deleteProperty(legacy, "provider");
    }
  }

  return { channels, migrationsApplied, schemaVersion, serviceSelections, tagRegistry };
}

/* Array-valued ChannelDelta fields whose equality requires canonical case-insensitive ordering before JSON.stringify. The set declares which array fields are
 * unordered-set semantics rather than order-significant lists; equality for these fields canonical-sorts both sides so authoring-order or case differences do
 * not defeat the match. Tags are the only such field today, but the property-based framing is the architectural truth: any unordered-set array field belongs in
 * this set. The `satisfies` constraint keeps the list in sync with ChannelDelta's actual keys at compile time - renaming or removing a field forces this tuple
 * to be updated.
 */
const CANONICAL_SORTED_ARRAY_FIELDS = new Set<keyof ChannelDelta>(
  ["tags"] as const satisfies readonly (keyof ChannelDelta)[]
);

/**
 * Normalizes a single stored entry as a delta against a base channel. Delta fields are dropped when they are no-ops: undefined values, nulls against a base
 * that has no such field, and values that match the base exactly. Array-valued fields use JSON.stringify for equality with case-insensitive canonical sorting
 * for the fields listed in CANONICAL_SORTED_ARRAY_FIELDS so authoring-order differences do not defeat the match. Non-delta fields (e.g., canonicalKey) pass
 * through unchanged so the stored entry retains its relationship metadata.
 * @param stored - The raw stored entry.
 * @param base - The base Channel to diff against (predefined definition for overrides, resolved canonical for variants).
 * @returns The normalized entry, or null when every field is a no-op and the entry would carry no information.
 */
function normalizeEntryAgainstBase(stored: StoredChannel, base: Channel): StoredChannel | null {

  const cleaned: Record<string, unknown> = {};
  let hasFields = false;

  for(const [ field, value ] of Object.entries(stored)) {

    // Non-delta fields (like canonicalKey) pass through so the stored entry keeps its relationship metadata - except when the base already declares the same
    // value. For example, a user override of a predefined variant doesn't need to store canonicalKey because the predefined variant entry already declares it;
    // stripping the redundant copy keeps the stored record minimal and lets an empty override collapse to nothing.
    if(!DELTA_ALLOWED_FIELDS.has(field)) {

      if(value === undefined) {

        continue;
      }

      const baseValue = (base as unknown as Record<string, unknown>)[field];

      if(baseValue !== value) {

        cleaned[field] = value;
        hasFields = true;
      }

      continue;
    }

    // Undefined delta values are no-ops.
    if(value === undefined) {

      continue;
    }

    // Null means "clear this field on the base." When the base does not have the field, the clear is a no-op and gets dropped. Otherwise null is preserved so
    // the clear survives.
    if(value === null) {

      if((field as keyof Channel) in base) {

        cleaned[field] = null;
        hasFields = true;
      }

      continue;
    }

    // Values that match the base exactly are redundant. Arrays compare by JSON.stringify; fields in CANONICAL_SORTED_ARRAY_FIELDS are sorted on both sides
    // first so the equality check is order-independent.
    const baseValue = (base as unknown as Record<string, unknown>)[field];

    if(Array.isArray(value)) {

      const canonical = CANONICAL_SORTED_ARRAY_FIELDS.has(field as keyof ChannelDelta);
      const left = canonical ? sortTags(value as string[]) : value;
      const right = canonical ? sortTags((baseValue ?? []) as string[]) : baseValue;

      if(JSON.stringify(left) === JSON.stringify(right)) {

        continue;
      }
    } else if(baseValue === value) {

      continue;
    }

    cleaned[field] = value;
    hasFields = true;
  }

  return hasFields ? (cleaned) : null;
}

/* Every channel entry falls into one of three kinds. The classifier is the single source of truth for this taxonomy; every consumer (resolution, normalization,
 * export) routes through it so the rules live in one place. The variant case splits into "predefined variant" and "user variant" based on which source declared
 * canonicalKey, but both paths go through the same layered overlay at resolution time - so the union collapses them into one discriminant.
 */

/**
 * Classification of a channel entry. Canonical and variant cases carry enough context (predefined entry, stored entry, canonicalKey for variants) that callers
 * do not need to re-read the source maps.
 */
type EntryClassification =
  { kind: "canonical"; predefined: Channel; stored: StoredChannel | undefined } |
  { kind: "variant"; canonicalKey: string; predefined: Channel | undefined; stored: StoredChannel | undefined } |
  { kind: "standalone"; stored: StoredChannel };

/**
 * Classifies a channel entry by consulting both the predefined catalog and the user's stored map. The kind determines how the entry is resolved, normalized, and
 * displayed.
 *
 * Precondition: at least one of the predefined catalog or the stored map must have the key. Callers iterate over the union of key sets and therefore always
 * satisfy this. Violating the precondition throws rather than returning a nullable - the alternative is a defensive undefined check at every call site for a
 * case that never occurs, which is both dead code and a misleading contract.
 * @param key - The channel key.
 * @param stored - The stored entry from the user's channels map (if any).
 * @returns The classification. Never undefined; see the precondition.
 * @throws Error when neither PREDEFINED_CHANNELS nor the caller's stored map contains the key.
 */
function classifyEntry(key: string, stored: StoredChannel | undefined): EntryClassification {

  const predefined = PREDEFINED_CHANNELS[key];
  const userCanonicalKey = stored ? (stored as Channel).canonicalKey : undefined;
  const canonicalKey = userCanonicalKey ?? predefined?.canonicalKey;

  // Variant: either source declares canonicalKey pointing at a different entry.
  if(canonicalKey && (canonicalKey !== key)) {

    return { canonicalKey, kind: "variant", predefined, stored };
  }

  // Canonical: a predefined entry exists (optionally with a user override on top).
  if(predefined) {

    return { kind: "canonical", predefined, stored };
  }

  // Standalone: user-only entry. Stored must be defined here; predefined is falsy (else canonical branch caught) and the neither-side-present state violates
  // the precondition.
  if(stored) {

    return { kind: "standalone", stored };
  }

  throw new Error("classifyEntry called with key '" + key + "' that exists in neither PREDEFINED_CHANNELS nor the stored map.");
}

/**
 * Computes the field-name allowlist for a stored entry's expected shape. Stored entries fall into two structural shapes determined by classification:
 *
 *  - Canonical-shaped (kind = canonical or standalone): allowed fields are DELTA_ELIGIBLE_IDENTITY_KEYS ∪ DELTA_ELIGIBLE_BINDING_KEYS. The full delta surface.
 *  - Variant-shaped (kind = variant): allowed fields are DELTA_ELIGIBLE_BINDING_KEYS plus the structural canonicalKey discriminator. Identity inherits from the
 *    canonical and must not appear on variants per the architectural principle.
 *
 * Single source of truth for the shape rule. Used by filterToDeltaSurface (normalizer pre-strip) and by the customization accessor's per-pass walks. Routes
 * shape determination through classifyEntry so any caller automatically picks up classifier improvements - no parallel logic for "is this a variant?".
 * @param key - The channel key for classification.
 * @param stored - The stored entry for classification.
 * @returns The set of field names allowed for this entry's shape.
 */
function getAllowedFieldsForShape(key: string, stored: StoredChannel): ReadonlySet<string> {

  const isVariant = classifyEntry(key, stored).kind === "variant";

  if(isVariant) {

    return new Set<string>([ ...DELTA_ELIGIBLE_BINDING_KEYS, "canonicalKey" ]);
  }

  return new Set<string>([ ...DELTA_ELIGIBLE_IDENTITY_KEYS, ...DELTA_ELIGIBLE_BINDING_KEYS ]);
}

/**
 * Filters a stored entry to its expected delta surface, stripping any field outside the allowed shape. Returns both the filtered entry and the names of any
 * fields that were stripped, so callers can surface diagnostics.
 *
 * The shape rule is enforced here at storage-write time (called from normalizeChannelDeltas) so legacy data with orphan fields - non-delta-eligible identity
 * (pacificStationId), non-delta-eligible binding (DOM hooks), or identity-on-variants - cleans up automatically on next save. The `stripped` array lets the
 * caller log a warning when cleanup happens, giving operators visibility into the violation.
 * @param key - The channel key for shape classification.
 * @param stored - The stored entry to filter.
 * @returns The filtered entry plus the list of stripped field names.
 */
function filterToDeltaSurface(key: string, stored: StoredChannel): { filtered: StoredChannel; stripped: readonly string[] } {

  const allowed = getAllowedFieldsForShape(key, stored);
  const filtered: Record<string, unknown> = {};
  const stripped: string[] = [];

  for(const [ field, value ] of Object.entries(stored)) {

    if(allowed.has(field)) {

      filtered[field] = value;
    } else {

      stripped.push(field);
    }
  }

  return { filtered, stripped };
}

/**
 * Resolves a variant by layering canonical identity, the predefined variant entry (service binding), and the user delta in order. The canonical contributes
 * ONLY identity (name, stationId, channelNumber, tags, etc.) - its service binding is for the canonical service and would be structurally wrong for any other
 * service variant. The variant's binding is supplied by the predefined variant entry (typically: discovery-driven channelSelector, service-specific URL,
 * profile overrides) and the user's stored delta.
 *
 * Fields propagate through the delta-overlay kernel so null-means-clear and undefined-means-inherit semantics apply uniformly at every layer.
 *
 * Precondition: at least one of predefined or stored must be defined. A variant with no binding source has nothing to resolve; the result would have no URL
 * and no selector, and downstream consumers would fail at the consumption site. classifyEntry only returns a "variant" classification when at least one
 * source carries canonicalKey, so the precondition is satisfied by the only in-tree caller (getMergedChannelMap). Other callers must guarantee the same.
 * @param canonical - The resolved canonical channel. Variant identity inherits from here; canonical binding is intentionally stripped via pickIdentity.
 * @param predefined - The predefined variant entry, if the key has one. Contributes service-binding fields and any per-variant channelNumber override.
 * @param stored - The user's stored delta for this variant, if any. Applied last so user edits win over predefined variant fields.
 * @returns The fully resolved variant.
 */
function resolveVariant(canonical: ResolvedChannel, predefined: Channel | undefined, stored: StoredChannel | undefined): ResolvedChannel {

  // Identity-only base. The url field (required on ResolvedChannel) is supplied by the predefined variant or stored delta below; the cast acknowledges that
  // intermediate state. After the chain completes, the result has url populated when at least one of the binding-bearing layers (predefined or stored) is
  // present - which is the documented invariant for any well-formed variant entry.
  let resolved = pickIdentity(canonical) as ResolvedChannel;

  // Variant overlays apply binding fields only - identity stays from the canonical per the architectural principle that variants are pure tuning data. Both
  // the predefined variant and the user's stored variant entry use the same binding-only overlay.
  if(predefined) {

    resolved = overlayVariantBinding(resolved, predefined);
  }

  if(stored) {

    resolved = overlayVariantBinding(resolved, stored);
  }

  return resolved;
}

/* Dangling-canonical warning dedup. When a variant's canonicalKey points at a missing canonical, getMergedChannelMap preserves the variant as best it can but
 * the user deserves a diagnostic - their data references something that is not there, and silently carrying on would hide a real data-integrity issue. We log
 * once per (variant, canonical) pair to surface the problem without spamming the log on every mutation. When the canonical later reappears (user re-creates
 * it, predefined gets restored), the pair is cleared from the set so a future regression re-warns.
 */
const warnedDanglingVariants = new Set<string>();

/**
 * Composes a stable token identifying a dangling-variant relationship for the warning dedup set.
 * @param variantKey - The variant channel key.
 * @param canonicalKey - The canonical key the variant references.
 * @returns The composite token.
 */
function danglingToken(variantKey: string, canonicalKey: string): string {

  return variantKey + " -> " + canonicalKey;
}

/**
 * Emits a dangling-canonical warning at most once per (variant, canonical) pair for the lifetime of this process. Called from getMergedChannelMap whenever a
 * variant references a canonical that is not present in the resolved map.
 * @param variantKey - The variant key whose canonicalKey points at a missing canonical.
 * @param canonicalKey - The missing canonical key.
 */
function warnDanglingCanonical(variantKey: string, canonicalKey: string): void {

  const token = danglingToken(variantKey, canonicalKey);

  if(warnedDanglingVariants.has(token)) {

    return;
  }

  warnedDanglingVariants.add(token);

  LOG.warn("Channel variant '%s' references missing canonical '%s'. Preserving the variant as-is; restore the canonical or remove the variant to clear this.",
    variantKey, canonicalKey);
}

/**
 * Builds the Pass 1 resolved-canonical map for a given stored map. Iterates every key present in either the predefined catalog or the stored map, resolves
 * non-variant entries (canonicals and standalones), and returns the resulting ChannelMap. Variants are deliberately skipped - they depend on this map and are
 * resolved in Pass 2 by the caller.
 *
 * This is shared by getMergedChannelMap (which uses the map directly as the starting point for its full resolution) and normalizeChannelDeltas (which uses the
 * map as a source of bases for diffing variant deltas).
 * @param stored - The stored channels map.
 * @returns The resolved-canonical map, keyed by channel key.
 */
function buildResolvedCanonicals(stored: StoredChannelMap): ResolvedChannelMap {

  const result: ResolvedChannelMap = {};
  const allKeys = new Set([ ...Object.keys(PREDEFINED_CHANNELS), ...Object.keys(stored) ]);

  for(const key of allKeys) {

    const classification = classifyEntry(key, stored[key]);

    if(classification.kind === "variant") {

      continue;
    }

    if(classification.kind === "canonical") {

      result[key] = classification.stored ? overlayDelta(classification.predefined, classification.stored) : classification.predefined;

      continue;
    }

    // Standalone: defensive copy so downstream mutation cannot leak back into loadedUserChannels. A standalone is structurally equivalent to a CanonicalChannel
    // (identity + binding, no canonicalKey).
    const copy = { ...classification.stored } as ResolvedChannel;

    copy.tags &&= copy.tags.slice();
    result[key] = copy;
  }

  return result;
}

/**
 * Strips null fields from a stored entry. Null is a delta-only convention meaning "clear this field on the base"; without a base to clear against, nulls
 * have no meaning and get dropped. Used by normalizeChannelDeltas for standalone user channels and for dangling-canonical variants that fall back to
 * standalone-style normalization.
 * @param stored - The raw stored entry.
 * @returns The entry with null-valued fields removed.
 */
function stripNulls(stored: StoredChannel): StoredChannel {

  return Object.fromEntries(Object.entries(stored).filter(([ , v ]) => v !== null));
}

/**
 * Normalizes the stored channels map to its minimal delta form. Each entry is classified, diffed against its applicable base, and any no-op fields are
 * stripped. Standalone user channels have no base, so nulls are stripped but everything else is preserved.
 *
 * The flow is uniform across kinds:
 *
 * - Canonical (predefined with user override): base is the predefined channel.
 * - Variant: base is the resolved canonical layered with the predefined variant's service fields (the view the user sees in the form/table, so the delta
 *   records only what differs from that).
 * - Standalone: no base, null values are stripped as a storage convention.
 *
 * Entries whose normalized delta is empty collapse to nothing and get dropped - a redundant override carries no information, and resolution falls through to
 * the default.
 * @param channels - The raw channel entries to normalize.
 * @returns The normalized channel map.
 */
function normalizeChannelDeltas(channels: StoredChannelMap): StoredChannelMap {

  const filtered: StoredChannelMap = {};
  const resolvedCanonicals = buildResolvedCanonicals(channels);

  for(const [ key, stored ] of Object.entries(channels)) {

    // Step 1: enforce the expected stored shape via filterToDeltaSurface. Strips orphan fields (non-delta-eligible identity, non-delta-eligible binding,
    // identity-on-variants) before any diff computation. When fields are stripped, log a warning so operators see when their data is being cleaned. The shape
    // rule is the same regardless of classification kind - canonicalKey-discriminated through filterToDeltaSurface internally.
    const { filtered: shapeFiltered, stripped } = filterToDeltaSurface(key, stored);

    if(stripped.length > 0) {

      const isVariant = classifyEntry(key, stored).kind === "variant";

      LOG.warn("Channel '%s' (%s-shaped) carried fields outside the delta surface: [%s]. Stripped during normalization.",
        key, isVariant ? "variant" : "canonical", stripped.join(", "));
    }

    // Step 2: classification-driven delta minimization against the appropriate base.
    const classification = classifyEntry(key, shapeFiltered);

    if(classification.kind === "canonical") {

      const normalized = normalizeEntryAgainstBase(shapeFiltered, classification.predefined);

      if(normalized) {

        filtered[key] = normalized;
      }

      continue;
    }

    if(classification.kind === "standalone") {

      filtered[key] = stripNulls(shapeFiltered);

      continue;
    }

    // Variant. The normalization base is identity-from-canonical layered with the predefined variant's binding (if any) - the same shape the user sees in the
    // form/table, so the delta records only what differs from that. Critically, this matches the inheritance shape used by resolveVariant: identity from
    // canonical, binding from variant. Without the pickIdentity strip, the canonical service's binding would leak into the normalization base and stored
    // overrides matching the canonical's binding would be incorrectly stripped as no-ops. A dangling canonical falls back to standalone-style normalization so
    // the user does not silently lose data.
    const canonical = resolvedCanonicals[classification.canonicalKey];

    if(!canonical) {

      filtered[key] = stripNulls(shapeFiltered);

      continue;
    }

    const identityBase = pickIdentity(canonical) as ResolvedChannel;
    // Mirror the resolveVariant chain: predefined-variant overlay applies binding fields only. The base computed here is what a stored variant entry should
    // be diffed against - using a category-aware overlay keeps normalization symmetric with resolution.
    const base = classification.predefined ? overlayVariantBinding(identityBase, classification.predefined) : identityBase;
    const normalized = normalizeEntryAgainstBase(shapeFiltered, base);

    if(normalized) {

      filtered[key] = normalized;
    }
  }

  return filtered;
}

/**
 * Prepares channels data for writing to disk. Injects metadata from module state (serviceSelections, tagRegistry) into the serializable output. The metadata
 * is always pulled from module state rather than from the file data, because route handlers may have modified metadata in memory since the last file read.
 * Delta normalization is handled by mutateChannels() before the data reaches this hook.
 * @param data - The compound channels data with already-normalized channels.
 * @returns The serializable output with channels and current metadata.
 */
function prepareChannelsForWrite(data: ChannelsFileData): unknown {

  // Pure function of data. The framework guarantees data is the source of truth at write time - serviceSelections and tagRegistry come from data, never from
  // module state. Module-state caches in services.ts and userChannels.ts are downstream of this writer (hydrated post-mutate from the same data), so reading
  // them here would be a side channel that breaks correctness when the boot persist runs before module state is populated.
  const output: Record<string, unknown> = { schemaVersion: data.schemaVersion, ...data.channels };

  if(data.migrationsApplied.length > 0) {

    output["migrationsApplied"] = data.migrationsApplied;
  }

  if(Object.keys(data.serviceSelections).length > 0) {

    output["serviceSelections"] = data.serviceSelections;
  }

  if((data.tagRegistry.tags.length > 0) || (data.tagRegistry.deletedTags.length > 0)) {

    output["tagRegistry"] = data.tagRegistry;
  }

  return output;
}

/* Declarative schema migrations. The file store framework runs these in order from the file's stored schemaVersion up to CURRENT_CHANNELS_SCHEMA_VERSION,
 * stamping the new version and recording the description in migrationsApplied after each application. Apply functions mutate the data in place. Any new
 * migration is a single entry below; the framework wires the rest.
 */
const channelsMigrations: Record<number, Migration<ChannelsFileData>> = {

  2: {

    apply: (data: ChannelsFileData): void => {

      // Stamp canonicalKey on hyphenated user channel entries whose identity matches a predefined canonical (shape-compatible variants). Conservative
      // classifier - entries whose identity diverges are treated as user standalones and left alone.
      const stampedKeys = collectLegacyVariantStamps(data.channels);

      for(const key of stampedKeys) {

        const channel = data.channels[key];

        if(!channel) {

          continue;
        }

        const hyphenIndex = key.indexOf("-");

        (channel as Channel).canonicalKey = key.substring(0, hyphenIndex);
      }
    },
    description: "Stamp canonicalKey on legacy user channel variant entries"
  },
  3: {

    apply: (data: ChannelsFileData): void => {

      // Rename foxcom -> foxone in service selections, the special-case fox-site -> fox-foxone, and corresponding channel keys. Bumping to v3 also flushes
      // any parser-side legacy field cleanups (provider -> service field, providerSelections -> serviceSelections key) since the migration runner triggers a
      // persist that writes the cleaned shape via prepareChannelsForWrite.
      for(const [ canonicalKey, selectedVariant ] of Object.entries(data.serviceSelections)) {

        if(selectedVariant.endsWith("-foxcom")) {

          data.serviceSelections[canonicalKey] = selectedVariant.slice(0, -6) + "foxone";
        }

        if((canonicalKey === "fox") && (selectedVariant === "fox-site")) {

          data.serviceSelections[canonicalKey] = "fox-foxone";
        }
      }

      for(const [ key, value ] of Object.entries(data.channels)) {

        if(key.endsWith("-foxcom")) {

          data.channels[key.slice(0, -6) + "foxone"] = value;
          Reflect.deleteProperty(data.channels, key);
        }
      }
    },
    description: "Rename foxcom service references to foxone and persist legacy field name cleanup"
  }
};

/**
 * Pre-write integrity validator for the channels store. Detects two classes of suspicious mutations:
 *
 *   1. Identity-field loss: a stored channel's identity field went from set to undefined without canonical fallback. Catches code that
 *      accidentally drops user-authored channel data while normalizing or transforming entries.
 *   2. Metadata wholesale clear: a top-level metadata collection (serviceSelections, tagRegistry.tags, tagRegistry.deletedTags) went from non-empty to empty
 *      in a single mutation. Catches code that accidentally drops the entire collection (the original bug class - if a future writer or fn regression empties
 *      one of these, this guard logs loudly even though the change is structurally allowed).
 *
 * Both checks are log-only - issues surface as warnings without blocking the write. Genuine wholesale clears (operator clears all selections via UI) are the
 * rare false-positive case here; in practice those happen via per-entry mutations rather than a single empty assignment.
 * @param prev - Pre-mutation snapshot of the file data.
 * @param next - Post-mutation, post-normalize file data.
 * @returns Issues found, or empty array.
 */
function validateChannelsIntegrity(prev: ChannelsFileData, next: ChannelsFileData): ValidationIssue[] {

  const issues: ValidationIssue[] = [];

  for(const loss of detectIdentityFieldLoss(prev.channels, next.channels)) {

    issues.push({

      category: "identity-field-loss",
      description: loss + " would be silently cleared without canonical fallback",
      severity: "warning"
    });
  }

  // Metadata wholesale-clear detection. Checks each top-level collection independently. Empty -> empty is a no-op (no issue); non-empty -> non-empty (any
  // size) is a normal mutation; only non-empty -> empty is flagged.
  if((Object.keys(prev.serviceSelections).length > 0) && (Object.keys(next.serviceSelections).length === 0)) {

    issues.push({

      category: "metadata-wholesale-clear",
      description: "serviceSelections went from " + String(Object.keys(prev.serviceSelections).length) + " entries to empty in a single mutation",
      severity: "warning"
    });
  }

  if((prev.tagRegistry.tags.length > 0) && (next.tagRegistry.tags.length === 0)) {

    issues.push({

      category: "metadata-wholesale-clear",
      description: "tagRegistry.tags went from " + String(prev.tagRegistry.tags.length) + " entries to empty in a single mutation",
      severity: "warning"
    });
  }

  if((prev.tagRegistry.deletedTags.length > 0) && (next.tagRegistry.deletedTags.length === 0)) {

    issues.push({

      category: "metadata-wholesale-clear",
      description: "tagRegistry.deletedTags went from " + String(prev.tagRegistry.deletedTags.length) + " entries to empty in a single mutation",
      severity: "warning"
    });
  }

  return issues;
}

// Transactional store instance for channels.json.
const channelsStore = createFileStore<ChannelsFileData>({

  beforeWrite: prepareChannelsForWrite,
  currentSchemaVersion: CURRENT_CHANNELS_SCHEMA_VERSION,
  defaultValue: (): ChannelsFileData => ({

    channels: {},
    migrationsApplied: [],
    schemaVersion: CURRENT_CHANNELS_SCHEMA_VERSION,
    serviceSelections: {},
    tagRegistry: { deletedTags: [], tags: [] }
  }),
  getSchemaVersion: (data: ChannelsFileData): number => data.schemaVersion,
  label: "channels",
  migrations: channelsMigrations,
  parse: parseChannelsFile,
  path: getChannelsFilePath,
  recordMigration: (data: ChannelsFileData, description: string): void => { data.migrationsApplied.push(description); },
  setSchemaVersion: (data: ChannelsFileData, version: number): void => { data.schemaVersion = version; },
  validate: validateChannelsIntegrity
});

/**
 * Reads the current channels from disk without acquiring the serialization lock. Returns the parsed channels with metadata and parse status. Use this for
 * read-only access and startup initialization. For modifications, use mutateChannels() instead.
 * @returns The loaded channels with parse status, service selections, and tag registry.
 */
export async function readChannels(): Promise<UserChannelsLoadResult> {

  const result = await channelsStore.read();

  return {

    channels: result.data.channels,
    parseError: result.parseError,
    parseErrorMessage: result.parseErrorMessage,
    recoveredFromBackup: result.recoveredFromBackup,
    serviceSelections: result.data.serviceSelections,
    tagRegistry: result.data.tagRegistry
  };
}

/**
 * Pre-write integrity guard. Compares the channels map captured immediately after parse against the channels map about to be written, and reports identity
 * fields that have been silently dropped without a canonical fallback.
 *
 * Allowed transitions for any identity field:
 *   - value -> same value  (no change)
 *   - value -> different value (legitimate update)
 *   - value -> null (explicit clear; null is the codebase-wide "clear this field" signal)
 *   - field absent -> any value (new data)
 *
 * Forbidden transition (what this guard catches):
 *   - value -> undefined / missing  WHERE the canonical (for variants and predefined-overrides) does not provide the same value
 *
 * Variant inheritance is honored: stripping a stored field that matches the canonical's value is delta minimization, not data loss, and is allowed. The check
 * is non-blocking: it surfaces suspicious patterns for operator review without aborting legitimate code paths.
 * @param before - The channels map captured immediately after parse, before the caller's mutation ran.
 * @param after - The channels map after the caller's mutation and delta normalization.
 * @returns A list of "key.field" strings naming each detected silent drop. Empty list means no losses.
 */
function detectIdentityFieldLoss(before: StoredChannelMap, after: StoredChannelMap): string[] {

  const losses: string[] = [];

  for(const [ key, beforeEntry ] of Object.entries(before)) {

    const afterEntry = after[key];

    // Whole entry removed - this is intentional deletion (deleteUserChannel etc.), not a silent drop.
    if(!afterEntry) {

      continue;
    }

    // Determine the canonical fallback. For variants the canonicalKey field points at it; for overrides of predefined channels the entry's key is the
    // canonical. For standalones (no predefined match, no canonicalKey) there is no fallback - any drop is a loss.
    const canonicalKey = (afterEntry as Channel).canonicalKey ?? key;
    const canonical = PREDEFINED_CHANNELS[canonicalKey];

    for(const field of CHANNEL_IDENTITY_KEYS) {

      const beforeValue = (beforeEntry as Record<string, unknown>)[field];
      const afterValue = (afterEntry as Record<string, unknown>)[field];

      // Field was not set before - any post state is fine.
      if((beforeValue === undefined) || (beforeValue === null)) {

        continue;
      }

      // Field is still present after (with any value, including null which means explicit clear) - allowed.
      if(afterValue !== undefined) {

        continue;
      }

      // Field went value -> undefined. Check whether the canonical provides the same value, in which case this is variant inheritance / delta minimization
      // and not data loss. Array-valued fields (notably tags) are compared via JSON.stringify since reference equality would always fail.
      const canonicalValue = canonical ? (canonical as unknown as Record<string, unknown>)[field] : undefined;

      if(canonicalValue !== undefined) {

        const sameValue = (Array.isArray(beforeValue) && Array.isArray(canonicalValue)) ?
          (JSON.stringify(beforeValue) === JSON.stringify(canonicalValue)) :
          (beforeValue === canonicalValue);

        if(sameValue) {

          continue;
        }
      }

      losses.push(key + "." + field);
    }
  }

  return losses;
}

/**
 * Serialized read-modify-write operation on channels.json. The mutation function receives the full file data (channels + serviceSelections + tagRegistry +
 * framework metadata) and modifies it in place. The store handles atomicity, serialization, corruption guard, backup, schema migrations, and pre-write
 * integrity validation. Delta normalization is applied to data.channels after the caller's mutation and before the file write so that the normalized result is
 * available for the post-write cache hydration.
 *
 * After a successful write, the in-memory caches that mirror persisted state (loadedUserChannels, loadedTagRegistry, services.ts serviceSelections Map,
 * runtime service groups) are hydrated from the just-written normalized data. This is the single direction caches flow: data is the source of truth, caches
 * are derived. Callers must never poke caches directly and rely on a later save to pick up the change - all mutations go through this function.
 * @param fn - Mutation function. Receives the full file data. Modify in place; return value is ignored.
 * @throws FileStoreParseError if channels.json contains invalid JSON and the .bak rotation is also unparseable.
 */
export async function mutateChannels(fn: (data: ChannelsFileData) => void): Promise<void> {

  // Captured from inside the callback so the post-write cache hydration sees the same normalized data the framework wrote to disk. Captured before the await
  // returns, but the assignments below only run if the store mutation (including atomic file write) completed without error.
  let writtenData: ChannelsFileData | undefined;

  await channelsStore.mutate((data: ChannelsFileData) => {

    fn(data);

    // Normalize channel deltas before the beforeWrite hook serializes the data. This ensures the in-memory cache and the on-disk representation are
    // identical. Other top-level fields (serviceSelections, tagRegistry, framework metadata) are not delta-shaped and pass through unchanged.
    data.channels = normalizeChannelDeltas(data.channels);
    writtenData = data;
  });

  // Side effects after successful write. The store framework guarantees writtenData reflects exactly what was persisted.
  if(writtenData) {

    loadedUserChannels = { ...writtenData.channels };
    loadedTagRegistry = writtenData.tagRegistry;
    setServiceSelections(writtenData.serviceSelections);
  }

  buildServiceGroups(getMergedChannelMap());

  userChannelsParseError = false;
  userChannelsParseErrorMessage = undefined;
}

/**
 * Deletes a user channel by key.
 * @param key - The channel key to delete.
 * @throws FileStoreParseError if the channels file contains invalid JSON.
 * @throws If the file cannot be written.
 */
export async function deleteUserChannel(key: string): Promise<void> {

  await mutateChannels((data) => {

    Reflect.deleteProperty(data.channels, key);
  });

  LOG.info("User channel '%s' deleted.", key);
}

/**
 * Removes all user override entries for a canonical channel - both the canonical-stored entry and the active service variant's stored entry (when one resolves
 * via explicit selection or service-filter fallback). Returns the stored entry that was deleted (canonical takes precedence; falls back to the variant entry
 * when canonical is absent) so callers can compute downstream effects like the playlist reload hint without duplicating the lookup.
 *
 * Callers wrap the call in mutateChannels() and pass the resulting StoredChannelMap. The helper operates on the map directly; it does not own the mutation
 * lifecycle, allowing callers to combine the dual-delete with other operations (e.g., setting serviceSelections) in the same atomic write.
 * @param channels - The stored channels map to mutate in place.
 * @param canonicalKey - The canonical channel key. The active variant is resolved internally via resolveServiceKey.
 * @returns The stored entry that existed pre-deletion (canonical first, variant fallback). Undefined when neither entry existed.
 */
export function clearChannelOverrides(channels: StoredChannelMap, canonicalKey: string): StoredChannel | undefined {

  const resolvedKey = resolveServiceKey(canonicalKey);
  const activeVariantKey = (resolvedKey === canonicalKey) ? undefined : resolvedKey;

  // Canonical takes precedence for the returned entry because the canonical entry is the editing target; the variant entry is secondary cleanup. Falls back
  // to the variant entry only when canonical is absent so callers still see a non-undefined result for variant-only revert cases.
  const stored = channels[canonicalKey] ?? (activeVariantKey ? channels[activeVariantKey] : undefined);

  Reflect.deleteProperty(channels, canonicalKey);

  if(activeVariantKey) {

    Reflect.deleteProperty(channels, activeVariantKey);
  }

  return stored;
}

/**
 * Merges a partial ChannelDelta into a stored entry. Returns a new entry with the existing fields preserved and the delta's fields layered on top. Used by
 * bulk operations that update one or more delta fields across many channels - the helper handles both the "no entry exists yet" case (uses an empty record)
 * and the "entry exists with prior overrides" case (preserves them) without forcing each call site to write the spread + assign manually.
 *
 * The merged entry is returned in StoredChannel shape; callers assign it back to data.channels[key]. The post-mutation normalizer (filterToDeltaSurface +
 * normalizeEntryAgainstBase) handles delta minimization and shape enforcement, so callers can pass arbitrary ChannelDelta keys without pre-validating them.
 * @param stored - The existing stored entry, or undefined when the channel has no prior overrides.
 * @param delta - Fields to assign onto the entry. Null values flow through unchanged for the delta-clear semantic.
 * @returns The merged stored entry.
 */
export function applyChannelDelta(stored: StoredChannel | undefined, delta: ChannelDelta): StoredChannel {

  return { ...stored, ...delta };
}

/**
 * Resets all user channels by deleting the channels file.
 * @throws If the file exists but cannot be deleted.
 */
export async function resetUserChannels(): Promise<void> {

  try {

    await fsPromises.unlink(getChannelsFilePath());

    LOG.info("Channels file deleted, using predefined channels only.");
  } catch(error) {

    // File doesn't exist - already using predefined channels.
    if((error as NodeJS.ErrnoException).code === "ENOENT") {

      LOG.info("Channels file does not exist, already using predefined channels only.");

      return;
    }

    throw error;
  }
}

/* User channels are loaded at server startup and stored in module-level state. This avoids repeated file reads during request handling.
 */

/**
 * Classifies which stored entries are legitimate legacy variants eligible for canonicalKey stamping. A legitimate legacy variant has a hyphenated key whose
 * prefix matches a predefined canonical AND every stored identity field either is absent or matches the canonical's value. Entries with diverging identity
 * are user-created standalones that happen to share the hyphenated key shape (e.g., "abc-kabc" as a local affiliate with its own channel number and station
 * ID) and must be left alone - stamping them would mark them for delta normalization and silently destroy the user's custom identity.
 *
 * Array-valued identity fields (tags) compare via JSON.stringify with case-insensitive canonical sorting so historical write order does not defeat the match.
 * @param channels - The raw stored channel entries.
 * @returns Keys that should receive a canonicalKey stamp. Empty when nothing needs migrating.
 */
function collectLegacyVariantStamps(channels: StoredChannelMap): string[] {

  const stamps: string[] = [];

  for(const [ key, stored ] of Object.entries(channels)) {

    // Entries that already declare canonicalKey are already on the new model - skip.
    if((stored as Channel).canonicalKey) {

      continue;
    }

    const hyphenIndex = key.indexOf("-");

    if(hyphenIndex === -1) {

      continue;
    }

    const prefix = key.substring(0, hyphenIndex);
    const canonical = PREDEFINED_CHANNELS[prefix];


    if(!canonical) {

      continue;
    }

    // Safe classifier: stamp only when every stored identity field is either absent or matches the canonical exactly. Any divergence signals a user-created
    // standalone that happens to share the variant-shaped key; stamping would trigger delta normalization and destroy the user's custom identity.
    let shapeCompatible = true;

    for(const field of CHANNEL_IDENTITY_KEYS) {

      const storedValue = (stored as Record<string, unknown>)[field];

      if(storedValue === undefined) {

        continue;
      }

      const canonicalValue = (canonical as unknown as Record<string, unknown>)[field];

      if(Array.isArray(storedValue) && Array.isArray(canonicalValue)) {

        const left = JSON.stringify(sortTags(storedValue as string[]));
        const right = JSON.stringify(sortTags(canonicalValue as string[]));

        if(left !== right) {

          shapeCompatible = false;

          break;
        }

        continue;
      }

      if(storedValue !== canonicalValue) {

        shapeCompatible = false;

        break;
      }
    }

    if(shapeCompatible) {

      stamps.push(key);
    }
  }

  return stamps;
}

/**
 * Initializes user channels by loading them from the file. This should be called once at server startup. Also builds service groups and loads service selections.
 */
export async function initializeUserChannels(): Promise<void> {

  // Read the file. Schema migrations (canonicalKey stamping, foxcom -> foxone rename, etc.) run automatically inside the file store framework via the
  // declarative channelsMigrations registry; ensureMigrated (called by the release boot coordinator at startup) persists any upgrades to disk before this
  // function runs. The data returned here is always at CURRENT_CHANNELS_SCHEMA_VERSION.
  const result = await readChannels();

  // Populate module-level state from the loaded (and already-migrated) data.
  loadedUserChannels = result.channels;
  loadedTagRegistry = result.tagRegistry;
  userChannelsParseError = result.parseError;
  userChannelsParseErrorMessage = result.parseErrorMessage;

  if(result.recoveredFromBackup) {

    LOG.info("Channels were recovered from backup after a corrupt main file.");
  }

  // Load service selections so prepareChannelsForWrite captures them on subsequent writes.
  setServiceSelections(result.serviceSelections);

  // Inference for setupCompleted: existing users who already have services or channels configured should not see the first-run setup wizard. This is runtime
  // inference (not a schema migration) because it depends on observed state from two stores (channels and config) and never changes the file shape - only
  // sets a flag based on what exists. Lives here at the cross-store boundary where both stores are loaded.
  const configuredServices = CONFIG.channels.enabledServices;

  if(!CONFIG.channels.setupCompleted) {

    const hasServices = configuredServices.length > 0;
    const hasUserChannels = Object.keys(loadedUserChannels).length > 0;

    if(hasServices || hasUserChannels) {

      CONFIG.channels.setupCompleted = true;

      await mutateConfig((config) => {

        config.channels ??= {};
        config.channels.setupCompleted = true;
      });
    }
  }

  // Build the merged channels map and then build service groups.
  const mergedChannels = getMergedChannelMap();

  // buildServiceGroups validates stored service selections against the rebuilt variant structure and reverts any that are stale (cleared from module state).
  // If any were cleaned, persist the same cleanup to disk so the file matches the runtime state and the cleanup survives restarts. mutateChannels operates on
  // the freshly-read on-disk data; explicitly deleting the stale keys from data.serviceSelections is the data-side equivalent of the module-side cleanup
  // buildServiceGroups already performed.
  const staleSelections = buildServiceGroups(mergedChannels);

  if(staleSelections.length > 0) {

    await mutateChannels((data) => {

      for(const key of staleSelections) {

        Reflect.deleteProperty(data.serviceSelections, key);
      }
    });
  }

  // Now that service groups are built, validate the configured service tags. Strip any unrecognized tags and warn.
  if(configuredServices.length > 0) {

    const knownTags = new Set(getAllServiceTags().map((t) => t.tag));
    const configuredSet = new Set(configuredServices);
    const validTags = [...configuredSet.intersection(knownTags)];
    const invalidTags = [...configuredSet.difference(knownTags)];

    if(invalidTags.length > 0) {

      LOG.warn("Ignoring unrecognized service tags in configuration: %s.", invalidTags.join(", "));
    }

    setEnabledServices(validTags);
  } else {

    setEnabledServices(configuredServices);
  }

  // Check for non-printable characters in loaded channel string values. These warnings are informational - loaded data is not modified.
  for(const [ channelKey, stored ] of Object.entries(loadedUserChannels)) {

    for(const [ field, value ] of Object.entries(stored)) {

      if((typeof value === "string") && containsNonPrintable(value)) {

        LOG.warn("User channel '%s' field '%s' contains non-printable characters. Re-save the channel to clean it.", channelKey, field);
      }
    }
  }

  const userCount = Object.keys(loadedUserChannels).length;
  const predefinedCount = Object.keys(PREDEFINED_CHANNELS).length;
  const totalCount = userCount + predefinedCount;

  if(userCount > 0) {

    LOG.info("Loaded %d channels (%d user, %d predefined).", totalCount, userCount, predefinedCount);
  } else {

    LOG.info("Loaded %d channels.", totalCount);
  }
}

// Variant overlay allowlist used by the resolver (overlayVariantBinding). The resolver runs against catalog data merged with user storage, and predefined
// variants legitimately carry DOM-hook binding fields (dismissSelector, scrollSelector, etc.) set by the catalog flattener. So the resolver's allowlist is the
// FULL binding partition, NOT the delta-eligible subset - this is correct domain asymmetry: the resolver accepts catalog-domain data, while storage filtering
// (filterToDeltaSurface) restricts to user-domain data. Identity fields encountered in a variant entry are silently dropped here - canonical's identity always
// wins by construction.
const VARIANT_OVERLAY_ALLOWED_FIELDS = new Set<string>(CHANNEL_BINDING_KEYS);

// The runtime delta surface: every field that may legitimately appear in a stored ChannelDelta override. Derived from the partition arrays in types/channels.ts
// so adding or removing a delta-eligible field in one place propagates everywhere. Typed as Set<string> for ergonomics at .has() call sites that take string-keyed
// values from Object.entries iteration; the contents are statically-known CustomizableField values via the spread above.
const DELTA_ALLOWED_FIELDS = new Set<string>([ ...DELTA_ELIGIBLE_IDENTITY_KEYS, ...DELTA_ELIGIBLE_BINDING_KEYS ]);

/**
 * Extracts the identity-only subset of a channel. Used by resolveVariant and normalizeChannelDeltas to compute the inheritance base for variants: a variant
 * inherits identity from its canonical but must contribute its own service binding (URL, channelSelector, profile, etc.) - the canonical's binding is for the
 * canonical service and is structurally wrong for any other service.
 *
 * The fields copied are exactly those listed in CHANNEL_IDENTITY_KEYS, the single source of truth for the identity partition. Array-valued fields are shallow-
 * copied so downstream mutation cannot leak back into the source.
 * @param channel - The channel to extract identity from. Typed as ResolvedChannel because that is the shape after canonical resolution; CanonicalChannel is
 *   structurally compatible.
 * @returns A new ChannelIdentity object with just the identity fields populated.
 */
export function pickIdentity(channel: ResolvedChannel): ChannelIdentity {

  const identity: ChannelIdentity = {};

  for(const field of CHANNEL_IDENTITY_KEYS) {

    const value = channel[field];

    if(value === undefined) {

      continue;
    }

    (identity as Record<string, unknown>)[field] = value;
  }

  identity.tags &&= identity.tags.slice();

  return identity;
}

/**
 * Overlays a stored channel (full definition or delta) onto a base. Allowlisted delta fields in the stored entry override the base: a null value clears the
 * field from the resolved object, a defined value replaces it, and missing fields inherit from the base. Non-delta fields on the stored entry (e.g.,
 * canonicalKey) pass through so the resolved channel retains its relationship metadata. The returned object is a fresh reference with a defensive copy of any
 * array-valued fields, so callers can mutate it without corrupting the base.
 *
 * This is the single delta-overlay kernel used by both predefined overrides and user variants. The base may be:
 *
 * - A predefined CanonicalChannel for canonical-override resolution.
 * - A ChannelIdentity-only intermediate during variant resolution (URL is supplied by the variant binding overlay).
 * - A fully resolved canonical for variant base computation.
 *
 * Internally typed as ResolvedChannel for the input shape; intermediate variant-resolution states pass through a cast because url is required on
 * ResolvedChannel but is supplied by the next overlay. The contract is: after the full resolveVariant chain, the result has every required field populated.
 * @param base - The base channel to inherit from.
 * @param stored - The stored entry (delta) to overlay.
 * @returns A new ResolvedChannel with the base's fields and the stored entry's overrides applied.
 */
function overlayDelta(base: ResolvedChannel, stored: StoredChannel): ResolvedChannel {

  const resolved: ResolvedChannel = { ...base };

  for(const [ field, value ] of Object.entries(stored)) {

    if(DELTA_ALLOWED_FIELDS.has(field)) {

      if(value === null) {

        // Explicit null means "clear this field" - delete it from the resolved object.
        Reflect.deleteProperty(resolved, field);
      } else if(value !== undefined) {

        // Non-null, non-undefined - override the base value.
        (resolved as unknown as Record<string, unknown>)[field] = value;
      }

      continue;
    }

    // Non-delta fields (e.g., canonicalKey) pass through from the stored entry so the resolved channel carries its relationship metadata. Undefined values are
    // skipped because they have no effect and would only overwrite a defined field on the base with undefined.
    if(value !== undefined) {

      (resolved as unknown as Record<string, unknown>)[field] = value;
    }
  }

  // Defensive copy of reference-type fields to break shared references with the base. The delta overlay above may have replaced tags entirely (if the stored
  // entry included a tags array), but when no delta is present for tags, the spread leaves the base's array reference on the resolved object.
  resolved.tags &&= resolved.tags.slice();

  return resolved;
}

/**
 * Variant-binding-only overlay. Like overlayDelta but applies only fields in CHANNEL_BINDING_KEYS - identity fields on the variant entry are silently dropped
 * during resolution. This enforces the architectural principle that variants are pure tuning data: how to reach the channel via this service. Identity
 * (channelNumber, hdhrEnabled, tags, name, etc.) is a property of the channel as a whole and lives only on the canonical entry.
 *
 * Used by resolveVariant for both predefined-variant and stored-variant overlays. Legacy data, hand-edited files, or future migrations that put identity
 * fields on variant entries are inert at resolution time - canonical's identity always wins. canonicalKey passes through as relationship metadata so the
 * resolved variant can still be identified as a variant.
 *
 * Null/undefined semantics match overlayDelta: null clears the field; undefined inherits.
 * @param base - The base channel (typically canonical identity from pickIdentity).
 * @param stored - The variant entry (predefined or user-stored).
 * @returns A new ResolvedChannel with binding fields applied from the variant.
 */
function overlayVariantBinding(base: ResolvedChannel, stored: StoredChannel): ResolvedChannel {

  const resolved: ResolvedChannel = { ...base };

  for(const [ field, value ] of Object.entries(stored)) {

    // canonicalKey is relationship metadata, not a category-classified field; pass it through so the resolved entry retains its variant identification.
    if(field === "canonicalKey") {

      if(value !== undefined) {

        (resolved as unknown as Record<string, unknown>)[field] = value;
      }

      continue;
    }

    // Variant overlay applies binding fields only. Identity fields encountered here (legacy data, hand edits, future migrations) are silently dropped per
    // the architectural principle - the canonical's identity is the single source of truth for the channel.
    if(!VARIANT_OVERLAY_ALLOWED_FIELDS.has(field)) {

      continue;
    }

    if(value === null) {

      Reflect.deleteProperty(resolved, field);
    } else if(value !== undefined) {

      (resolved as unknown as Record<string, unknown>)[field] = value;
    }
  }

  // Defensive copy of reference-type fields to break shared references with the base.
  resolved.tags &&= resolved.tags.slice();

  return resolved;
}

/**
 * Resolves a stored channel entry into a fully populated ResolvedChannel. Handles the two non-variant cases:
 *
 * 1. Predefined override (key matches a predefined entry): the stored entry is a delta over the predefined canonical definition.
 * 2. Standalone user channel (no predefined equivalent): the stored entry is already a full channel; a defensive copy is returned so downstream mutations
 *    cannot leak into loadedUserChannels.
 *
 * Variants are resolved by getMergedChannelMap via the layered overlay (canonical identity -> predefined variant binding -> user delta). Callers that need a
 * resolved variant should read from channelsRef (via getResolvedChannel) rather than calling this function with a variant key.
 * @param key - The channel key (must not be a variant key).
 * @param stored - The stored channel data.
 * @returns A fully resolved channel with identity and binding both populated.
 */
export function resolveStoredChannel(key: string, stored: StoredChannel): ResolvedChannel {

  const predefined = PREDEFINED_CHANNELS[key];

  if(predefined) {

    return overlayDelta(predefined, stored);
  }

  // Standalone: defensive copy so callers can mutate the result without corrupting loadedUserChannels. A standalone is structurally a CanonicalChannel
  // (identity + binding, no canonicalKey) and therefore satisfies ResolvedChannel.
  const standalone = { ...stored } as ResolvedChannel;

  standalone.tags &&= standalone.tags.slice();

  return standalone;
}

/**
 * Returns the merged channel map (predefined + user) without filtering by enabled status or service variants. Used internally for building service groups.
 * Resolves deltas into fully populated channels.
 *
 * Built from buildResolvedCanonicals (Pass 1: canonicals and standalones) plus a variant pass (Pass 2: layered canonical identity -> predefined variant
 * binding -> user delta). The dangling-canonical path preserves whatever data exists so the user does not silently lose anything when a canonicalKey points
 * at a missing entry.
 * @returns The complete merged channel map.
 */
function getMergedChannelMap(): ResolvedChannelMap {

  const result = buildResolvedCanonicals(loadedUserChannels);
  const allKeys = new Set([ ...Object.keys(PREDEFINED_CHANNELS), ...Object.keys(loadedUserChannels) ]);

  for(const key of allKeys) {

    const stored = loadedUserChannels[key];
    const classification = classifyEntry(key, stored);

    if(classification.kind !== "variant") {

      continue;
    }

    const canonical = result[classification.canonicalKey];

    if(canonical) {

      result[key] = resolveVariant(canonical, classification.predefined, classification.stored);

      // Clear the dangling-warning dedup entry if one was recorded earlier: a previously-missing canonical is now present, so a future regression should
      // re-warn rather than staying silent because we already warned once.
      warnedDanglingVariants.delete(danglingToken(key, classification.canonicalKey));

      continue;
    }

    // Dangling canonical: the variant references a canonical that is not present. Warn once per (variant, canonical) pair, then preserve the variant as best
    // we can so the user does not silently lose data. When a stored entry exists, funnel through resolveStoredChannel - it already overlays onto the
    // predefined definition (if any) or returns a standalone copy. When only the predefined entry exists, use it directly.
    warnDanglingCanonical(key, classification.canonicalKey);

    if(classification.stored) {

      result[key] = resolveStoredChannel(key, classification.stored);
    } else if(classification.predefined) {

      result[key] = classification.predefined;
    }
  }

  return result;
}

/* The getChannelListing() function is the single source of truth for merging predefined channels with user channels. It returns enriched entries with source
 * classification and enabled status. All other channel retrieval functions that need merged data build on top of it.
 */

/**
 * Returns the full channel listing with source classification and enabled status. This is the authoritative merge point for predefined and user channels - all
 * code that needs a merged view of channels should use this function (or getAllChannels() which delegates to it).
 *
 * For each channel key, the source is classified as:
 * - "predefined": exists only in predefined channels
 * - "user": exists only in user channels
 * - "override": exists in both (user channel data takes precedence)
 *
 * The enabled field reflects whether the channel is available for streaming. Predefined-only channels can be disabled via configuration; user and override
 * channels are always enabled.
 *
 * Service variants (non-canonical keys in service groups) are filtered out from this listing - they are accessed via the service selection mechanism instead.
 *
 * Override entries produce a new resolved Channel object (via resolveStoredChannel()), which is a different reference from PREDEFINED_CHANNELS[key]. The service
 * system (services.ts) relies on this reference difference to detect user overrides via isUserOverride(). Predefined-only entries preserve the original reference.
 *
 * The returned channel field is service-resolved: when a non-default service is selected for a channel, the entry's channel reflects the selected variant's URL,
 * channelSelector, stationId, and channelNumber. The entry's key always remains the canonical key.
 * @returns Sorted array of channel listing entries.
 */
export function getChannelListing(): ChannelListingEntry[] {

  const allKeys = new Set([ ...Object.keys(PREDEFINED_CHANNELS), ...Object.keys(loadedUserChannels) ]);
  const listing: ChannelListingEntry[] = [];

  for(const key of allKeys) {

    // Skip service variants - they're accessed via service selection, not as separate channels.
    if(isServiceVariant(key)) {

      continue;
    }

    const isPredefined = key in PREDEFINED_CHANNELS;
    const isUser = key in loadedUserChannels;

    // Determine source classification. User channel data takes precedence on key conflicts.
    let source: "override" | "predefined" | "user";

    if(isPredefined && isUser) {

      source = "override";
    } else if(isUser) {

      source = "user";
    } else {

      source = "predefined";
    }

    // For user entries (including overrides), resolve the stored delta/definition into a full Channel. The resolved object is a new reference, which preserves
    // the isUserOverride() contract in services.ts (reference comparison against PREDEFINED_CHANNELS[key]). Predefined-only entries keep the original reference.
    const userEntry = loadedUserChannels[key];
    const predefinedEntry = PREDEFINED_CHANNELS[key];
    const resolvedBase = isUser && userEntry ? resolveStoredChannel(key, userEntry) : predefinedEntry;

    if(!resolvedBase) {

      // Defensive: the entry exists in at least one source per the allKeys construction, so this branch is unreachable.
      continue;
    }

    // Variants are filtered out at the top of this loop via isServiceVariant, so resolvedBase reaching here is structurally canonical-shaped (a CanonicalChannel
    // from the catalog or a fully-resolved stored entry). Type as ResolvedChannel so the boundary rule in types/channels.ts holds: post-resolution values are
    // typed ResolvedChannel.
    const channel: ResolvedChannel = resolvedBase;

    // When a non-default service is selected, resolve the variant so consumers see the correct URL, channelSelector, stationId, and channelNumber. We skip
    // resolution when the resolved key matches the canonical key - the channel object is already correct and preserving its reference avoids a redundant lookup.
    const resolvedKey = resolveServiceKey(key);
    const resolvedChannel = (resolvedKey !== key) ? getResolvedChannel(resolvedKey) : undefined;

    listing.push({

      availableByService: isChannelAvailableByService(key),
      channel: resolvedChannel ?? channel,
      enabled: !isPredefinedChannelDisabled(key),
      key,
      source
    });
  }

  // Sort alphabetically by key for consistent ordering across all callers.
  listing.sort((a, b) => a.key.localeCompare(b.key));

  return listing;
}

/**
 * Predicate for "visible" channels - entries that are both enabled and available under the current service filter. This is the single source of truth for what
 * "visible" means across bulk operations, the playlist, and the merged channel map. Every site that filters the listing by visibility routes through this
 * predicate so the definition lives in exactly one place.
 * @param entry - The listing entry to test.
 * @returns True when the channel is enabled and passes the service filter.
 */
export function isVisibleChannel(entry: ChannelListingEntry): boolean {

  return entry.enabled && entry.availableByService;
}

/**
 * Returns the subset of getChannelListing() that is enabled and available under the current service filter. Built on top of isVisibleChannel so the visibility
 * predicate lives in one place.
 * @returns Listing entries that pass the visibility predicate.
 */
export function getVisibleChannels(): ChannelListingEntry[] {

  return getChannelListing().filter(isVisibleChannel);
}

/**
 * Returns all available channels (predefined + user), with user channels taking precedence on key conflicts. Disabled predefined channels are excluded. Built on
 * top of getChannelListing() to ensure a single merging code path.
 * @returns The merged channel map with disabled predefined channels filtered out.
 */
export function getAllChannels(): ResolvedChannelMap {

  const result: ResolvedChannelMap = {};

  for(const entry of getVisibleChannels()) {

    result[entry.key] = entry.channel;
  }

  return result;
}

/**
 * Returns a copy of the raw stored user channels map - the unresolved entries as they exist in channels.json. Used by the consistency probe to check
 * cross-store invariants without applying delta resolution. Callers should not rely on the shape for anything beyond key/canonicalKey inspection.
 * @returns Shallow copy of the loaded user channels.
 */
export function getStoredUserChannels(): StoredChannelMap {

  return { ...loadedUserChannels };
}

/**
 * Where a customized field's override lives in storage. "canonical" means the override is on the canonical channel's stored entry (typically identity fields
 * like name, stationId, tags). "variant" means the override is on the active service variant's stored entry (typically binding fields like url,
 * channelSelector). The form's save handler uses this to route changes to the correct entry; the per-field reset uses it to compute the resetValue from the
 * matching predefined source.
 */
export type CustomizationStoredIn = "canonical" | "variant";

/**
 * Per-field customization metadata: where the override is stored and what the field would resolve to if the override were removed (the per-field "reset to
 * default" value).
 */
export interface ChannelCustomizationEntry {

  // The value the field would resolve to after removing the user's override. For canonical-stored fields, this is the predefined canonical's value (or
  // undefined when the predefined entry has no value for the field). For variant-stored fields, this is the predefined variant's value, falling back to the
  // predefined canonical's value when the variant doesn't carry the field (variant-inheritance fallback). Undefined means the field has no predefined value
  // and reset would clear the input.
  resetValue: unknown;

  // Whether the override lives on the canonical or variant stored entry.
  storedIn: CustomizationStoredIn;
}

/**
 * Result of getChannelCustomizations. Carries the per-field provenance map and identifies the active service variant (if a non-canonical service is resolved
 * via explicit selection or service-filter fallback). Callers - the channel edit form's render and save paths - rely on both pieces to display the right
 * "modified from default" indicators and to route saves to the correct entry.
 */
export interface ChannelCustomizations {

  // The variant key the canonical resolves to via the active service selection or service-filter fallback. Undefined when the canonical service is active
  // (no selection, or selection points back at the canonical key, or canonical's service is enabled and no override is in play).
  activeVariantKey: string | undefined;

  // Field-name keyed customization map. Includes only fields that the user has explicitly stored (in either the canonical or the active-variant entry).
  // When the same field appears in both stored entries, the variant entry wins because variant resolution overlays the variant's value last.
  //
  // Map keys are typed as CustomizableField so consumers cannot look up arbitrary strings - typos and unsupported fields are caught at compile time. Pass 2
  // (variant-stored fields) applies the variant binding-only allowlist via getAllowedFieldsForShape so identity fields encountered on a variant entry never
  // surface as customizations.
  customizations: Map<CustomizableField, ChannelCustomizationEntry>;
}

/**
 * Computes the per-field reset value for a customized field. The reset value is what the field would resolve to in the user's effective view if the override
 * were removed - so it must apply the same effective-view rules consumers see (vocabulary-filtered tags, implicit-true hdhrEnabled). Field-specific dispatch
 * routes through the existing single-source helpers (getChannelEffectiveTags, getEffectiveHdhrEnabled) so the rules live in exactly one place.
 *
 * For canonical-stored fields, the reset value reads from the predefined canonical. For variant-stored fields, it reads from the predefined variant first,
 * falling back to the predefined canonical when the variant doesn't carry the field (variant-inheritance fallback). Returns undefined when no predefined
 * source has the field - a reset would clear the input.
 * @param field - The customized field name.
 * @param storedIn - Where the override is stored (canonical or variant).
 * @param predefinedCanonical - The predefined canonical entry (raw catalog read).
 * @param predefinedVariant - The predefined variant entry, if applicable (raw catalog read).
 * @returns The effective reset value, or undefined.
 */
function computeResetValue(field: string, storedIn: CustomizationStoredIn, predefinedCanonical: Channel | undefined, predefinedVariant: Channel | undefined): unknown {

  // Pick the right predefined source for this storage location. Variant-stored fields prefer the predefined variant; fall back to canonical when the variant
  // doesn't carry the field (e.g., a variant that inherits profile from canonical's URL-based detection).
  let source: Channel | undefined;

  if(storedIn === "canonical") {

    source = predefinedCanonical;
  } else if(predefinedVariant && ((predefinedVariant as unknown as Record<string, unknown>)[field] !== undefined)) {

    source = predefinedVariant;
  } else {

    source = predefinedCanonical;
  }

  if(!source) {

    return undefined;
  }

  // Delegate to the effective-view helpers for fields with view semantics. Channel structurally satisfies ResolvedChannel here (CanonicalChannel and
  // VariantChannel both assign), so no cast is needed at the boundary.
  switch(field) {

    case "tags": {

      return getChannelEffectiveTags(source);
    }

    case "hdhrEnabled": {

      return getEffectiveHdhrEnabled(source);
    }

    default: {

      return (source as unknown as Record<string, unknown>)[field];
    }
  }
}

/**
 * Returns the customization provenance for the given canonical channel. The form layer uses this as the single source of truth for which fields are
 * customized (instead of comparing resolved values to predefined values, which produces false positives when a non-canonical service is active and the
 * resolved value legitimately differs from the canonical's predefined value).
 *
 * Resolution: for each field present in the user's canonical-stored entry, mark customized with storedIn="canonical" and resetValue from computeResetValue.
 * For each field present in the active variant's stored entry, mark customized with storedIn="variant" and resetValue from computeResetValue (which handles
 * variant-inheritance fallback). Variant entries override canonical entries on the same field name because variant resolution overlays the variant's value
 * last.
 *
 * Pass 2 (variant-stored fields) restricts the walked field set via getAllowedFieldsForShape so identity fields encountered on a variant entry are silently
 * dropped from the customization map, matching the resolver's behavior. The "canonicalKey" field is similarly excluded from both passes - structural metadata,
 * not user-customizable.
 * @param canonicalKey - The canonical channel key.
 * @returns The customization provenance map and the active variant key (when applicable).
 */
export function getChannelCustomizations(canonicalKey: string): ChannelCustomizations {

  const customizations = new Map<CustomizableField, ChannelCustomizationEntry>();
  const predefinedCanonical = PREDEFINED_CHANNELS[canonicalKey];

  // Determine the active variant via the service resolver. resolveServiceKey returns the canonical key itself when the canonical service is selected (no
  // override is needed) and the variant key otherwise (whether via stored selection or filter fallback).
  const resolvedKey = resolveServiceKey(canonicalKey);
  const activeVariantKey = (resolvedKey === canonicalKey) ? undefined : resolvedKey;

  // Walk a stored entry against its shape's allowed-field set, recording each customized field. The shape rule is computed via getAllowedFieldsForShape so
  // canonical-stored entries see the full delta surface and variant-stored entries see only the variant binding-only subset (identity-on-variants is dropped).
  const recordCustomizations = (storedKey: string, stored: StoredChannel, storedIn: CustomizationStoredIn, predefinedVariant: Channel | undefined): void => {

    const allowed = getAllowedFieldsForShape(storedKey, stored);

    for(const [ field, value ] of Object.entries(stored)) {

      // canonicalKey is structural metadata, not a customization. Other non-allowed fields (e.g., orphan identity on a variant) are silently ignored here -
      // the normalizer's filterToDeltaSurface will strip them on next save with a warning.
      if(!allowed.has(field) || (field === "canonicalKey")) {

        continue;
      }

      if(value === undefined) {

        continue;
      }

      // Cast acknowledges that field is one of the allowed customizable names per the allowlist check above.
      customizations.set(field as CustomizableField, {

        resetValue: computeResetValue(field, storedIn, predefinedCanonical, predefinedVariant),
        storedIn
      });
    }
  };

  // Pass 1: canonical-stored customizations. The canonical entry's allowed shape is the full delta surface (identity + binding delta-eligible).
  const canonicalStored = loadedUserChannels[canonicalKey];

  if(canonicalStored) {

    recordCustomizations(canonicalKey, canonicalStored, "canonical", undefined);
  }

  // Pass 2: variant-stored customizations. The variant entry's allowed shape is binding-only. Variant entries override canonical entries on the same field
  // name (last-overlay-wins matches resolution semantics).
  if(activeVariantKey) {

    const variantStored = loadedUserChannels[activeVariantKey];

    if(variantStored) {

      const predefinedVariant = PREDEFINED_CHANNELS[activeVariantKey];

      recordCustomizations(activeVariantKey, variantStored, "variant", predefinedVariant);
    }
  }

  return { activeVariantKey, customizations };
}

/**
 * Returns the raw stored channel data (without predefined channels). Entries may be full Channel definitions or ChannelDelta overrides.
 * @returns The stored channel map.
 */
export function getUserChannels(): StoredChannelMap {

  return { ...loadedUserChannels };
}

// Tag Registry.

/**
 * Returns the current tag registry state.
 * @returns A copy of the tag registry with user-created tags and deleted predefined tags.
 */
export function getTagRegistry(): TagRegistry {

  return { deletedTags: [...loadedTagRegistry.deletedTags], tags: [...loadedTagRegistry.tags] };
}

/**
 * Persists a new tag registry state. Goes through mutateChannels so the file write, integrity validation, and post-mutate cache hydration all run uniformly -
 * after the call returns, both disk and module-state cache reflect the new value. The caller's input arrays are not mutated; both are sorted non-destructively
 * via sortTags so storage shares one canonical ordering.
 * @param registry - The new tag registry state.
 * @throws FileStoreParseError if channels.json contains invalid JSON and the .bak rotation is also unparseable.
 */
export async function setTagRegistry(registry: TagRegistry): Promise<void> {

  const sorted: TagRegistry = { deletedTags: sortTags(registry.deletedTags), tags: sortTags(registry.tags) };

  await mutateChannels((data) => {

    data.tagRegistry = sorted;
  });
}

/**
 * Returns the active tag vocabulary: predefined tags minus user-deleted tags, plus user-created tags, sorted alphabetically. This is the single source of truth
 * for which tags are visible, assignable, and queryable throughout the system. Tags not in this list are invisible to the UI and rejected by the ?tag= query
 * parameter, even if they exist on channel definitions (vocabulary-as-lens model).
 * @returns Sorted array of active tag strings.
 */
export function getActiveTagVocabulary(): string[] {

  const active = PREDEFINED_TAGS.filter((tag) => !loadedTagRegistry.deletedTags.some((d) => tagsMatch(d, tag)));

  // Merge user tags, deduplicate case-insensitively (in case a user tag matches a non-deleted predefined tag), and sort. When a predefined and user tag
  // collide case-insensitively, the predefined form wins since it appears first in the Map.
  const seen = new Map<string, string>();

  for(const tag of [ ...active, ...loadedTagRegistry.tags ]) {

    const lower = tag.toLowerCase();

    if(!seen.has(lower)) {

      seen.set(lower, tag);
    }
  }

  return [...seen.values()].toSorted((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));
}

/**
 * Returns a channel's effective tags - the intersection of the channel's assigned tags with the active vocabulary. Tags that exist on the channel but are not in
 * the active vocabulary are filtered out, ensuring only assignable and queryable tags are visible in the UI and playlist responses. Output preserves the source
 * order of channel.tags; every write path (sortTags) keeps that source order canonical, so in practice the returned array is sorted.
 * @param channel - The channel to get effective tags for.
 * @returns Effective tag strings in source order, or empty array if the channel has no tags or none are in the active vocabulary.
 */
export function getChannelEffectiveTags(channel: ResolvedChannel): string[] {

  if(!channel.tags || (channel.tags.length === 0)) {

    return [];
  }

  const vocabulary = getActiveTagVocabulary();

  return channel.tags.filter((tag) => vocabulary.some((v) => tagsMatch(v, tag)));
}

/**
 * Returns whether a channel is effectively included in the HDHomeRun lineup. Codifies the sparse-storage convention that absent or true means included, and only
 * an explicit `false` means excluded. This is the single source of truth for the convention - every site that needs to ask "is HDHR enabled for this channel?"
 * routes through this helper rather than reaching for the inline `channel.hdhrEnabled !== false` pattern. Storage-shape inspections that ask a different
 * question ("did the client send literal false?", "did the user override?") stay inline because they are not asking about effective state.
 * @param channel - The channel to evaluate.
 * @returns True when the channel is effectively included in the HDHR lineup.
 */
export function getEffectiveHdhrEnabled(channel: ResolvedChannel): boolean {

  return channel.hdhrEnabled !== false;
}

/**
 * Case-insensitive tag comparison. Tags are freeform strings with preserved casing, but all matching throughout the system is case-insensitive. This function
 * is the single source of truth for that policy - all tag identity checks should use it rather than inline toLowerCase() calls.
 * @param a - The first tag.
 * @param b - The second tag.
 * @returns True if the tags match case-insensitively.
 */
export function tagsMatch(a: string, b: string): boolean {

  return a.toLowerCase() === b.toLowerCase();
}

/**
 * Checks whether a tag is present in the active vocabulary under case-insensitive identity. This is the single source of truth for "does this tag exist?"
 * questions - tag endpoints (create/rename collision, bulk-tags vocabulary check) route through this instead of ad-hoc Set+toLowerCase comparisons so the
 * case-insensitive policy lives in exactly one place (tagsMatch), not in parallel implementations.
 * @param tag - The tag to check.
 * @returns True if the active vocabulary contains a tag matching case-insensitively.
 */
export function isInVocabulary(tag: string): boolean {

  return getActiveTagVocabulary().some((v) => tagsMatch(v, tag));
}

/**
 * Applies a tag transformation across channels and persists the result. This is the single source of truth for batch tag mutations - delete, rename, and bulk
 * toggle all route through this function. The caller provides a filter (which channels to transform) and a transform (how to modify each channel's tags). This
 * function handles loading stored channel data, applying the transform, and saving. Delta normalization in mutateChannels() handles predefined channel
 * delta computation automatically - callers do not need to reason about deltas vs. full definitions.
 * @param filter - Predicate selecting which listing entries to transform. Receives each ChannelListingEntry from getChannelListing().
 * @param transform - Pure function mapping a channel's current resolved tags to its new tags. Receives the channel's current tags array (may be empty, ordering
 *   not guaranteed). Must return the desired tags array (may be empty to clear all tags). The returned array is sorted before storage.
 * @returns Object with the affected channel keys and success status. On parse error, returns an error message and empty affected keys.
 */
export async function transformChannelTags(
  filter: (entry: ChannelListingEntry) => boolean,
  transform: (tags: string[]) => string[]
): Promise<{ affectedKeys: string[]; error?: string }> {

  const affectedKeys: string[] = [];

  try {

    await mutateChannels((data) => {

      for(const entry of getChannelListing()) {

        if(!filter(entry)) {

          continue;
        }

        const currentTags = entry.channel.tags ?? [];

        // Route both the transform result and the "no change" comparison through sortTags so tag storage shares one canonical ordering with the rest of the
        // system. sortTags is non-mutating, which matters here because for predefined-only listing entries, entry.channel is the PREDEFINED_CHANNELS reference
        // directly - a raw .sort() on entry.channel.tags would rearrange the predefined array in process memory.
        const newTags = sortTags(transform(currentTags));

        if(JSON.stringify(newTags) === JSON.stringify(sortTags(currentTags))) {

          continue;
        }

        // Set the new tags on the stored entry. Callers use null uniformly for "clear/empty" - the normalizer in mutateChannels() handles the storage
        // conventions: delta normalization for predefined channels (comparing against raw definitions), null-stripping for user channels.
        const existing = data.channels[entry.key] ?? {};

        (existing as Record<string, unknown>)["tags"] = (newTags.length > 0) ? newTags : null;
        data.channels[entry.key] = existing;
        affectedKeys.push(entry.key);
      }
    });
  } catch(error) {

    if(error instanceof FileStoreParseError) {

      return { affectedKeys: [], error: "Cannot update tags: channels file contains invalid JSON." };
    }

    throw error;
  }

  return { affectedKeys };
}

/* Dangling-canonical warning dedup for getPredefinedChannel. The catalog flattener should make a missing canonical structurally impossible, so observing one
 * indicates real data corruption that an operator needs to know about. We log once per variant key for the lifetime of this process - a tight loop calling
 * getPredefinedChannel on the corrupt entry would otherwise spam the log on every form render.
 */
const warnedMissingCanonicalForPredefined = new Set<string>();

/**
 * Returns the predefined channel definition for a key as a fully-resolved view. For predefined variant entries (which carry only service-specific fields plus
 * canonicalKey), overlays the variant's binding onto its predefined canonical so callers receive the resolved view - canonical identity inherited onto the
 * variant. This matches what the user sees in the form/table and is the correct base for delta-from-predefined computations in crud.ts and channelForm.ts.
 *
 * Always returns ResolvedChannel-shaped data: identity fields (name, tags, hdhrEnabled, etc.) are always available, sourced from the canonical when this key is
 * a variant. CanonicalChannel and VariantChannel both assign structurally to ResolvedChannel since the identity fields are optional in both types.
 *
 * Returns undefined when the key is not predefined OR when the catalog is internally inconsistent (variant references a missing canonical). The latter case is
 * structurally impossible by construction of the flattener, so observing it is a real corruption indicator: the function logs once per offending key and
 * returns undefined rather than fabricating a half-resolved view that would lie about its identity fields.
 * @param key - The channel key to look up.
 * @returns The resolved predefined channel, or undefined if the key is not predefined or the catalog is inconsistent.
 */
export function getPredefinedChannel(key: string): ResolvedChannel | undefined {

  const entry = PREDEFINED_CHANNELS[key];

  if(!entry) {

    return undefined;
  }

  // Canonical entries have full identity already; return as-is. CanonicalChannel assigns to ResolvedChannel structurally (canonicalKey: never is assignable to
  // canonicalKey?: string).
  if(!entry.canonicalKey || (entry.canonicalKey === key)) {

    return entry;
  }

  // Predefined variant: overlay its binding fields onto the canonical so callers see the resolved view with canonical identity. Variant overlays apply
  // binding fields only per the architectural principle - matches resolveVariant's behavior so callers get the same view regardless of which path produced it.
  const canonical = PREDEFINED_CHANNELS[entry.canonicalKey];

  if(!canonical) {

    // Catalog inconsistency. Log once per offending key, then return undefined. Returning a half-resolved view (the variant typed as ResolvedChannel) would
    // lie about which identity fields are reachable and silently hide the corruption from consumers.
    if(!warnedMissingCanonicalForPredefined.has(key)) {

      warnedMissingCanonicalForPredefined.add(key);

      LOG.warn("Catalog inconsistency: predefined variant '%s' references missing canonical '%s'. Returning undefined; the flattener should make this impossible.",
        key, entry.canonicalKey);
    }

    return undefined;
  }

  return overlayVariantBinding(canonical, entry);
}

/**
 * Checks if a channel key exists in the predefined channels.
 * @param key - The channel key to check.
 * @returns True if the channel is predefined.
 */
export function isPredefinedChannel(key: string): boolean {

  return key in PREDEFINED_CHANNELS;
}


/**
 * Returns the East canonical key for a Pacific channel. Pacific canonicals are auto-generated by generatePacificDefinitions with a "p" suffix (e.g., "disneyp"
 * from "disney"). When a Pacific canonical's logo or other brand-level metadata is unavailable, the East counterpart provides a fallback. Returns undefined
 * when the key is not a Pacific canonical (doesn't end with "p" or the base key isn't predefined).
 * @param key - The channel key to check.
 * @returns The East canonical key if this is a Pacific channel, undefined otherwise.
 */
export function getEastCanonicalKey(key: string): string | undefined {

  if(!key.endsWith("p")) {

    return undefined;
  }

  const eastKey = key.slice(0, -1);

  return isPredefinedChannel(eastKey) ? eastKey : undefined;
}

// Logo cache. Logo URLs are fetched from the Channels DVR API and cached by Gracenote station ID. The cache is populated in two tiers by showInfo.ts: first from
// the DVR's /devices endpoint (covers all channels in the M3U playlist), then via TMS station name search for any remaining channels. The cache refreshes every
// 24 hours and updates on individual channel add/edit operations.

// Logo cache keyed by Gracenote station ID. Populated by showInfo.ts via setChannelLogos() and setChannelLogo().
const logoCache = new Map<string, string>();

/**
 * Bulk-populates the logo cache from a stationId-to-URL map. Called by showInfo.ts after fetching logo data from the DVR. Existing entries are overwritten with
 * fresh data to pick up any logo URL changes (e.g., network rebrands).
 * @param logos - Map of Gracenote station ID to logo URL.
 */
export function setChannelLogos(logos: Map<string, string>): void {

  for(const [ stationId, url ] of logos) {

    logoCache.set(stationId, url);
  }
}

/**
 * Sets a single logo cache entry. Called by showInfo.ts after a TMS station name search for a single channel (tier 2 population or channel add/edit).
 * @param stationId - The Gracenote station ID.
 * @param url - The logo URL.
 */
export function setChannelLogo(stationId: string, url: string): void {

  logoCache.set(stationId, url);
}

/**
 * Clears the logo cache. Called on shutdown to release memory.
 */
export function clearChannelLogos(): void {

  logoCache.clear();
}

/**
 * Resolves a channel key to its effective Gracenote station ID. Pacific channels (e.g., "bravop") resolve to their east counterpart's station ID since
 * they share the same brand logo. User channel overrides that change the stationId are reflected. Returns undefined if the channel has no stationId.
 * @param channelKey - The channel key to resolve (e.g., "amc", "bravop").
 * @returns The Gracenote station ID, or undefined.
 */
export function getChannelStationId(channelKey: string): string | undefined {

  // Pacific channels are regional feeds of the same brand as their east counterparts, so we resolve to the east variant's station ID for logo purposes.
  const effectiveKey = getEastCanonicalKey(channelKey) ?? channelKey;

  // User channels take precedence - resolveStoredChannel merges deltas with the predefined base, so overrides that change the stationId are reflected here.
  const userEntry = loadedUserChannels[effectiveKey];

  if(userEntry) {

    return resolveStoredChannel(effectiveKey, userEntry).stationId;
  }

  // PREDEFINED_CHANNELS is a discriminated union; only canonicals carry stationId. The lookup here is by canonical-equivalent key (Pacific resolved to East),
  // so a hit is structurally a canonical. Narrow via the canonicalKey discriminator: a defined canonicalKey marks a VariantChannel, which has no stationId.
  const predefined = PREDEFINED_CHANNELS[effectiveKey];

  if(!predefined || (predefined.canonicalKey !== undefined)) {

    return undefined;
  }

  return predefined.stationId;
}

/**
 * Returns the logo URL for a channel from the cache. Pacific channels (e.g., "bravop") are resolved to their east counterpart automatically. The cache is
 * populated by showInfo.ts from the Channels DVR API - this function only reads from it.
 * @param channelKey - The channel key to look up (e.g., "amc", "bravop"). Pacific keys resolve to their east variant internally.
 * @returns The logo URL if cached, undefined otherwise.
 */
export function getChannelLogo(channelKey: string): string | undefined {

  const stationId = getChannelStationId(channelKey);

  if(!stationId) {

    return undefined;
  }

  return logoCache.get(stationId);
}

/**
 * Checks if a channel key exists in the user channels.
 * @param key - The channel key to check.
 * @returns True if the channel is user-defined.
 */
export function isUserChannel(key: string): boolean {

  return key in loadedUserChannels;
}

/* Users can disable predefined channels to exclude them from the playlist and block streaming. Disabled channels appear grayed out in the UI with an option to
 * re-enable. This is useful for users who don't want certain predefined channels cluttering their channel list.
 */

/**
 * Checks if a predefined channel is disabled. The disabled state is determined solely by the disabledPredefined list in config - the user's explicit visibility
 * intent. Property overrides (HDHR toggle, name change, tag edits) stored as deltas in channels.json are orthogonal and do not affect the enabled/disabled state.
 * @param key - The channel key to check.
 * @returns True if the channel is predefined and disabled.
 */
export function isPredefinedChannelDisabled(key: string): boolean {

  if(!isPredefinedChannel(key)) {

    return false;
  }

  return CONFIG.channels.disabledPredefined.includes(key);
}

/**
 * Returns the list of disabled predefined channel keys.
 * @returns Array of disabled channel keys.
 */
export function getDisabledPredefinedChannels(): string[] {

  return [...CONFIG.channels.disabledPredefined];
}

/**
 * Applies a set operation (add or delete) to the disabledPredefined list in user config. Persists the change to config.json and syncs the runtime CONFIG object
 * so subsequent reads see the updated state immediately. This is the internal implementation shared by disablePredefinedChannels and enablePredefinedChannels so
 * the mutateConfig scaffolding, sort, and CONFIG sync live in exactly one place.
 * @param op - "add" to insert keys into the disabled list, "delete" to remove them.
 * @param keys - The predefined channel keys to apply the operation to.
 */
async function mutateDisabledPredefined(op: "add" | "delete", keys: readonly string[]): Promise<void> {

  if(keys.length === 0) {

    return;
  }

  let updatedList: string[] = [];

  await mutateConfig((config) => {

    config.channels ??= {};
    config.channels.disabledPredefined ??= [];

    const disabledSet = new Set(config.channels.disabledPredefined);

    for(const key of keys) {

      disabledSet[op](key);
    }

    config.channels.disabledPredefined = [...disabledSet].toSorted();
    updatedList = config.channels.disabledPredefined;
  });

  CONFIG.channels.disabledPredefined = updatedList;
}

/**
 * Disables one or more predefined channels by adding their keys to the disabledPredefined list in user config. Idempotent.
 * @param keys - The predefined channel keys to disable. Duplicates within the input array and pre-existing disabled entries are handled idempotently.
 */
export async function disablePredefinedChannels(keys: readonly string[]): Promise<void> {

  await mutateDisabledPredefined("add", keys);
}

/**
 * Enables one or more predefined channels by removing their keys from the disabledPredefined list in user config. Idempotent.
 * @param keys - The predefined channel keys to enable. Entries that aren't in the disabled list are ignored.
 */
export async function enablePredefinedChannels(keys: readonly string[]): Promise<void> {

  await mutateDisabledPredefined("delete", keys);
}

/**
 * Persists a partial update of channel-table display preferences (sort field, sort direction, visible columns) to config.json. Only fields present in `prefs`
 * are written; absent fields are left untouched. The mutate fn copies the new values into data and the runtime CONFIG cache is updated post-write to match.
 * Single-call replacement for the prior set-then-save pattern, consistent with mutateServiceSelections, mutateEnabledServices, and setTagRegistry.
 * @param prefs - Subset of display preferences to update.
 * @throws FileStoreParseError if config.json contains invalid JSON and the .bak rotation is also unparseable.
 */
export async function mutateChannelDisplayPrefs(prefs: {
  channelSortDirection?: SortDirection;
  channelSortField?: ChannelSortField;
  visibleColumns?: readonly string[];
}): Promise<void> {

  const next = {

    channelSortDirection: prefs.channelSortDirection ?? CONFIG.channels.channelSortDirection,
    channelSortField: prefs.channelSortField ?? CONFIG.channels.channelSortField,
    visibleColumns: prefs.visibleColumns ? [...prefs.visibleColumns] : [...CONFIG.channels.visibleColumns]
  };

  await mutateConfig((config) => {

    config.channels ??= {};
    config.channels.channelSortDirection = next.channelSortDirection;
    config.channels.channelSortField = next.channelSortField;
    config.channels.visibleColumns = next.visibleColumns;
  });

  // Hydrate the runtime CONFIG cache from the values just written.
  CONFIG.channels.channelSortDirection = next.channelSortDirection;
  CONFIG.channels.channelSortField = next.channelSortField;
  CONFIG.channels.visibleColumns = next.visibleColumns;
}

/**
 * Marks the first-run Service Setup wizard as completed. Writes the flag to runtime CONFIG and persists to config.json in one call - the operation is a single
 * one-way transition (setupCompleted goes from false/absent to true once, never back), so splitting into set+save would be ceremony without benefit.
 */
export async function markSetupCompleted(): Promise<void> {

  CONFIG.channels.setupCompleted = true;

  await mutateConfig((config) => {

    config.channels ??= {};
    config.channels.setupCompleted = true;
  });
}

/**
 * Returns all predefined channels regardless of disabled state, excluding service variants. Used by the UI to show all predefined channels including disabled ones.
 * Service variants are internal implementation details of channel delivery and are not channels themselves.
 * @returns The predefined channel map with canonical entries only.
 */
export function getPredefinedChannels(): ChannelMap {

  const result: ChannelMap = {};

  for(const [ key, channel ] of Object.entries(PREDEFINED_CHANNELS)) {

    if(isServiceVariant(key)) {

      continue;
    }

    result[key] = channel;
  }

  return result;
}

/* Pacific and East key identification share the same structural logic: iterate canonical predefined keys and test whether each participates in the Pacific
 * timezone naming convention. The helper below centralizes this so the two exported functions differ only in which side of the East/Pacific pair they select.
 */

/**
 * Filters predefined channel keys by their relationship to the Pacific timezone naming convention. For "pacific" mode, returns keys that end in "p" and
 * whose East counterpart (key minus trailing "p") exists. For "east" mode, returns keys that do NOT end in "p" and whose Pacific counterpart (key plus "p")
 * exists. Service variants are excluded - only canonical keys are returned.
 * @param side - Which side of the East/Pacific pair to select.
 * @returns Sorted array of matching canonical predefined channel keys.
 */
function filterPredefinedKeysByTimezone(side: "east" | "pacific"): string[] {

  const keys: string[] = [];

  for(const key of Object.keys(PREDEFINED_CHANNELS)) {

    // Skip service variants - they are internal implementation details, not channels.
    if(isServiceVariant(key)) {

      continue;
    }

    if(side === "pacific") {

      // Pacific: key ends in "p" and the East counterpart exists.
      if(key.endsWith("p") && ((key.slice(0, -1)) in PREDEFINED_CHANNELS)) {

        keys.push(key);
      }
    } else {

      // East: key does NOT end in "p" and the Pacific counterpart exists.
      if(!key.endsWith("p") && ((key + "p") in PREDEFINED_CHANNELS)) {

        keys.push(key);
      }
    }
  }

  return keys.sort();
}

/**
 * Returns the keys of all predefined channels that are Pacific entries (canonical keys only). A key is Pacific if it ends in "p" and the East counterpart
 * (key minus trailing "p") exists in PREDEFINED_CHANNELS.
 * @returns Sorted array of canonical Pacific predefined channel keys.
 */
export function getPacificPredefinedKeys(): string[] {

  return filterPredefinedKeysByTimezone("pacific");
}

/**
 * Returns the keys of all predefined East channels that have Pacific counterparts (canonical keys only). A key qualifies if it does NOT end in "p" and
 * the Pacific counterpart (key plus "p") exists in PREDEFINED_CHANNELS.
 * @returns Sorted array of canonical East predefined channel keys that have Pacific counterparts.
 */
export function getEastWithPacificPredefinedKeys(): string[] {

  return filterPredefinedKeysByTimezone("east");
}

/**
 * Computes enabled/total counts for all three predefined channel scopes (all, east, pacific) against the current disabled set. Both the enabled count and
 * the total are filtered by service availability so that the displayed counts match the visible channel table. When no service filter is active,
 * all channels pass and the counts are unaffected. Used by the server-side HTML renderer and both toggle endpoints to provide consistent counts to the client.
 * @returns An object with `all`, `east`, and `pacific` keys, each containing `{ enabled, total }`.
 */
export function getPredefinedScopeCounts(): { all: { enabled: number; total: number }; east: { enabled: number; total: number };
  pacific: { enabled: number; total: number }; } {

  const allKeys = Object.keys(getPredefinedChannels()).filter((k) => isChannelAvailableByService(k));
  const eastKeys = getEastWithPacificPredefinedKeys().filter((k) => isChannelAvailableByService(k));
  const pacificKeys = getPacificPredefinedKeys().filter((k) => isChannelAvailableByService(k));
  const disabled = new Set(CONFIG.channels.disabledPredefined);

  return {

    all: { enabled: allKeys.filter((k) => !disabled.has(k)).length, total: allKeys.length },
    east: { enabled: eastKeys.filter((k) => !disabled.has(k)).length, total: eastKeys.length },
    pacific: { enabled: pacificKeys.filter((k) => !disabled.has(k)).length, total: pacificKeys.length }
  };
}

/**
 * Checks if a channel is available for streaming. A channel is available if it exists in the merged channel map returned by getAllChannels(), which already
 * excludes disabled predefined channels (unless overridden by a user channel).
 * @param key - The channel key to check.
 * @returns True if the channel can be streamed.
 */
export function isChannelAvailable(key: string): boolean {

  return key in getAllChannels();
}

/* These functions validate channel data before saving.
 */

/**
 * Validates a channel key for format and uniqueness.
 * @param key - The channel key to validate.
 * @param isNew - True if this is a new channel (checks for duplicates among user channels).
 * @returns Error message if invalid, undefined if valid.
 */
export function validateChannelKey(key: string, isNew: boolean): string | undefined {

  // Check for empty key.
  if(!key || (key.trim() === "")) {

    return "Channel key is required.";
  }

  // Check format: lowercase alphanumeric and hyphens only.
  if(!/^[a-z0-9-]+$/.test(key)) {

    return "Channel key must contain only lowercase letters, numbers, and hyphens.";
  }

  // Check length.
  if(key.length > 50) {

    return "Channel key must be 50 characters or less.";
  }

  // Check for duplicates when adding new channel.
  if(isNew && isUserChannel(key)) {

    return "A user channel with this key already exists.";
  }

  return undefined;
}

/**
 * Validates a channel number for range and uniqueness. Returns an error message if invalid, undefined if valid. Empty string means no channel number (valid).
 * This is the single source of truth for channel number validation, shared by the full edit handler and the inline-edit handler.
 * @param value - The channel number as a string (from form input or inline edit). Empty means "no number."
 * @param excludeKey - The channel key being edited, excluded from the duplicate check.
 * @returns Error message if invalid, undefined if valid.
 */
export function validateChannelNumber(value: string, excludeKey: string): string | undefined {

  if(!value) {

    return undefined;
  }

  const num = parseInt(value, 10);

  if(Number.isNaN(num) || (num < 1) || (num > 99999)) {

    return "Channel number must be between 1 and 99999.";
  }

  for(const entry of getChannelListing()) {

    if((entry.channel.channelNumber === num) && (entry.key !== excludeKey)) {

      return "Channel number " + String(num) + " is already used by '" + entry.key + "'.";
    }
  }

  return undefined;
}

/**
 * Validates a channel URL.
 * @param url - The URL to validate.
 * @returns Error message if invalid, undefined if valid.
 */
export function validateChannelUrl(url: string): string | undefined {

  // Check for empty URL.
  if(!url || (url.trim() === "")) {

    return "URL is required.";
  }

  // Check URL format.
  try {

    const parsed = new URL(url);

    // Only allow http and https protocols.
    if((parsed.protocol !== "http:") && (parsed.protocol !== "https:")) {

      return "URL must use http or https protocol.";
    }
  } catch {

    return "Invalid URL format.";
  }

  return undefined;
}

/**
 * Validates a channel name.
 * @param name - The name to validate.
 * @returns Error message if invalid, undefined if valid.
 */
export function validateChannelName(name: string): string | undefined {

  // Check for empty name.
  if(!name || (name.trim() === "")) {

    return "Channel name is required.";
  }

  // Check length.
  if(name.length > 100) {

    return "Channel name must be 100 characters or less.";
  }

  return undefined;
}

/**
 * Validates a profile name.
 * @param profile - The profile name to validate (can be empty for autodetect).
 * @param validProfiles - Array of valid profile names.
 * @returns Error message if invalid, undefined if valid.
 */
export function validateChannelProfile(profile: string | undefined, validProfiles: string[]): string | undefined {

  // Empty profile means autodetect, which is valid.
  if(!profile || (profile.trim() === "")) {

    return undefined;
  }

  // Check if profile exists.
  if(!validProfiles.includes(profile)) {

    return [ "Unknown profile: ", profile, ". Valid profiles: ", validProfiles.join(", "), "." ].join("");
  }

  return undefined;
}

/**
 * Result of validating imported channels.
 */
export interface ChannelsValidationResult {

  // The validated channels if valid. Import always produces full Channel definitions, not deltas.
  channels: ChannelMap;

  // Validation error messages.
  errors: string[];

  // True if validation passed.
  valid: boolean;
}

/**
 * Validates an imported channels object for structure and content.
 * @param data - The raw imported data to validate.
 * @param validProfiles - Array of valid profile names.
 * @returns Validation result with errors if invalid.
 */
export function validateImportedChannels(data: unknown, validProfiles: string[]): ChannelsValidationResult {

  const errors: string[] = [];

  // Check that input is an object.
  if((typeof data !== "object") || (data === null) || Array.isArray(data)) {

    return { channels: {}, errors: ["Invalid format: expected an object with channel definitions."], valid: false };
  }

  const channels: ChannelMap = {};
  const entries = Object.entries(data as Record<string, unknown>);

  for(const [ key, value ] of entries) {

    // Validate key format.
    const keyError = validateChannelKey(key, false);

    if(keyError) {

      errors.push("Channel '" + key + "': " + keyError);

      continue;
    }

    // Check that value is an object.
    if((typeof value !== "object") || (value === null) || Array.isArray(value)) {

      errors.push("Channel '" + key + "': expected an object with channel properties.");

      continue;
    }

    const channelData = value as Record<string, unknown>;

    // Validate required name field. Sanitize after type check to strip non-printable characters from imported data.
    if((typeof channelData["name"] !== "string") || (channelData["name"].trim() === "")) {

      errors.push("Channel '" + key + "': name is required.");

      continue;
    }

    const cleanName = sanitizeString(channelData["name"]);

    const nameError = validateChannelName(cleanName);

    if(nameError) {

      errors.push("Channel '" + key + "': " + nameError);

      continue;
    }

    // Validate required url field. Sanitize after type check.
    if((typeof channelData["url"] !== "string") || (channelData["url"].trim() === "")) {

      errors.push("Channel '" + key + "': url is required.");

      continue;
    }

    const cleanUrl = sanitizeString(channelData["url"]);

    const urlError = validateChannelUrl(cleanUrl);

    if(urlError) {

      errors.push("Channel '" + key + "': " + urlError);

      continue;
    }

    // Validate optional profile field. Sanitize after type check.
    const profile = (typeof channelData["profile"] === "string") ? sanitizeString(channelData["profile"]) : undefined;
    const profileError = validateChannelProfile(profile, validProfiles);

    if(profileError) {

      errors.push("Channel '" + key + "': " + profileError);

      continue;
    }

    // Build validated channel with sanitized values.
    const channel: UserChannel = {

      name: cleanName,
      url: cleanUrl
    };

    if(profile) {

      channel.profile = profile;
    }

    // Include optional fields if present. Sanitize string values to strip non-printable characters.
    if(typeof channelData["stationId"] === "string") {

      channel.stationId = sanitizeString(channelData["stationId"]);
    }

    if(typeof channelData["channelSelector"] === "string") {

      channel.channelSelector = sanitizeString(channelData["channelSelector"]);
    }

    // Validate optional channelNumber field (range and type).
    if(channelData["channelNumber"] !== undefined) {

      const num = Number(channelData["channelNumber"]);

      if(!Number.isInteger(num) || (num < 1) || (num > 99999)) {

        errors.push("Channel '" + key + "': channelNumber must be an integer between 1 and 99999.");

        continue;
      }

      channel.channelNumber = num;
    }

    // Validate optional tvgShift field (must be a finite number). Negative values are valid (e.g., a West Coast feed viewed from the East).
    if(channelData["tvgShift"] !== undefined) {

      const shift = Number(channelData["tvgShift"]);

      if(!Number.isFinite(shift)) {

        errors.push("Channel '" + key + "': tvgShift must be a finite number.");

        continue;
      }

      channel.tvgShift = shift;
    }

    channels[key] = channel;
  }

  // Validate channelNumber uniqueness across all imported channels. We check after building the full map so that all duplicates are reported. channelNumber
  // is identity (canonical-only by architectural principle); the validator above only constructs canonical-shaped entries, but the StoredChannelMap is typed
  // as the union, so we narrow with an in-check before reading.
  const numberToKey = new Map<number, string>();

  for(const [ key, channel ] of Object.entries(channels)) {

    if(!("channelNumber" in channel) || (channel.channelNumber === undefined)) {

      continue;
    }

    const existing = numberToKey.get(channel.channelNumber);

    if(existing) {

      errors.push("Channel '" + key + "': channelNumber " + String(channel.channelNumber) + " is already used by '" + existing + "'.");
    } else {

      numberToKey.set(channel.channelNumber, key);
    }
  }

  return { channels, errors, valid: errors.length === 0 };
}

/**
 * Sorts tags case-insensitively using locale-aware comparison. This is the single source of truth for tag ordering. Every write path (parseTagInput, PATCH
 * handlers, computePredefinedDelta, transformChannelTags, setTagRegistry) routes through this so stored tag arrays share one canonical ordering. The channels
 * normalizer is a READER of that invariant - it uses sortTags on both sides when comparing a delta's tags against a predefined's tags, making the JSON.stringify
 * equality check canonical regardless of how either side was originally populated.
 * @param tags - Any iterable of tag strings (array, Set, generator). Not mutated.
 * @returns A new array with the same elements in canonical case-insensitive order.
 */
export function sortTags(tags: Iterable<string>): string[] {

  return Array.from(tags).toSorted((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));
}

/**
 * Parses a comma-separated tag input string into a normalized tag array. Trims whitespace, drops empty entries, deduplicates case-sensitively via Set (so
 * distinct casings survive as distinct tags), then sorts via sortTags so every caller produces identical normalized arrays.
 * @param raw - The comma-separated tag input from a form field or inline editor. An empty or whitespace-only string returns an empty array.
 * @returns Sorted, case-sensitively-deduplicated array of trimmed tag strings.
 */
export function parseTagInput(raw: string): string[] {

  if(!raw) {

    return [];
  }

  return sortTags(new Set(raw.split(",").map((t) => t.trim()).filter((t) => t.length > 0)));
}

/* Test-only export barrel. The helpers below are intentionally file-private for production code - the resolver, normalizer, and migration classifier are
 * internal building blocks, and exposing them as public API would invite misuse from routes and streaming code that should be going through the higher-level
 * functions above. Unit tests need direct access to exercise each building block in isolation, so they reach in through this named export. Production code
 * must not import from this barrel; its shape is not stable and its presence in the module carries no API guarantee.
 */
export const __internalForTests = {

  buildResolvedCanonicals,
  classifyEntry,
  collectLegacyVariantStamps,
  normalizeChannelDeltas,
  normalizeEntryAgainstBase,
  overlayDelta,
  resolveVariant,
  stripNulls
};
