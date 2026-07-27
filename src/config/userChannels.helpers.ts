/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userChannels.helpers.ts: Test helpers for the channel-resolution suite. Co-located with userChannels.ts, the production module that owns ResolvedChannel
 * construction (via getAllChannels and the merge/normalization pipeline). The factories accept partial overrides and return fully-shaped values typed to the
 * appropriate union member, mirroring the make<Thing> pattern from the test conventions. Excluded from the build emit by the *.helpers.ts pattern in
 * tsconfig.build.json.
 *
 * The helpers fall into the following groups:
 *
 * - makeStoredVariant / makeStoredCanonical / makeChannelDelta build StoredChannel values - the on-disk shape carrying full identity (canonical) or partial
 *   override (variant/delta) data. Used by tests that exercise the merge/resolution pipeline directly.
 *
 * - makeChannel builds a ResolvedChannel - the merged read-model that consumers see after the pipeline runs. Used by tests that operate above the resolution
 *   boundary and only care about the merged shape.
 *
 * - makeChannelsData builds a minimal ChannelsFileData envelope (the compound shape mutateChannels and normalizeChannelDeltas operate on). Tests that need to
 *   assert on serviceSelections or other envelope fields use this; tests that only assert on the channels map use the convenience normalize() wrapper below.
 *
 * - normalize wraps normalizeChannelDeltas with envelope construction so tests focused on the channels map don't have to build the envelope themselves.
 *
 * - getCanonical narrows PREDEFINED_CHANNELS lookups to CanonicalChannel with a runtime guard for misuse.
 */
import type { CanonicalChannel, ChannelDelta, ResolvedChannel, StoredChannel, StoredChannelMap, VariantChannel } from "../types/index.ts";
import type { ChannelsFileData } from "./userChannels.ts";
import { PREDEFINED_CHANNELS } from "../channels/index.ts";
import { __internalForTests } from "./userChannels.ts";

const { normalizeChannelDeltas } = __internalForTests;

/**
 * Builds a stored variant entry - a stored channel that carries a canonicalKey tag. Defaults to a minimal abc-hulu shape so callers only override
 * what they're actually testing.
 * @param overrides - Field overrides; canonicalKey and binding fields are merged onto the defaults.
 * @returns A StoredChannel-typed variant entry.
 */
export function makeStoredVariant(overrides: Partial<VariantChannel> = {}): StoredChannel {

  return {

    canonicalKey: "abc",
    channelSelector: "ABC",
    url: "https://www.hulu.com/live",
    ...overrides
  };
}

/**
 * Builds a ResolvedChannel - the merged read-model produced by getAllChannels() and consumed by routes/UI. Defaults to a minimal name/url shape (the structural
 * minimum required by ResolvedChannel) so callers that only care about identity-level contracts can call makeChannel() with no args. Tests needing other fields
 * (channelNumber, tags, stationId, hdhrEnabled, channelSelection, etc.) override them via the overrides parameter.
 *
 * @param overrides - Field overrides merged onto the defaults.
 * @returns A ResolvedChannel suitable for tests that operate above the resolution pipeline.
 */
export function makeChannel(overrides: Partial<ResolvedChannel> = {}): ResolvedChannel {

  return {

    name: "Test",
    url: "https://example.com",
    ...overrides
  };
}

/**
 * Builds a stored canonical-shape entry with full identity fields. Defaults to a minimal ABC channel; tests override only the fields they care about (typically
 * name, channelNumber, stationId for affiliate cases).
 * @param overrides - Field overrides; identity and binding fields are merged onto the defaults.
 * @returns A StoredChannel-typed canonical entry.
 */
export function makeStoredCanonical(overrides: Partial<CanonicalChannel> = {}): StoredChannel {

  return {

    name: "ABC",
    url: "https://www.abc.com/watch-live",
    ...overrides
  };
}

/**
 * Builds a generic ChannelDelta - the partial-override shape used for canonical edits and predefined-variant overrides. Unlike makeStoredCanonical and
 * makeStoredVariant, this does not include any default identity or binding fields; callers pass exactly the override they need to test.
 * @param overrides - The fields the test wants to set.
 * @returns A StoredChannel-typed delta.
 */
export function makeChannelDelta(overrides: Partial<ChannelDelta>): StoredChannel {

  return { ...overrides };
}

/**
 * Builds a minimal ChannelsFileData envelope around a stored channels map. Used by tests that need to assert on serviceSelections (or any envelope field other
 * than channels). The defaults are the empty-state envelope: schemaVersion=1, no migrations applied, no service selections, empty tag registry. Callers
 * override anything they care about via the second argument; the channels map is required because every test passes a different one. Centralizing the
 * envelope shape here keeps tests focused on what they're customizing.
 * @param channels - The channels map for this test. Defensively shallow-copied so callers can mutate freely.
 * @param overrides - Optional envelope-field overrides (serviceSelections, tagRegistry, schemaVersion, migrationsApplied).
 * @returns A fully-populated ChannelsFileData envelope.
 */
export function makeChannelsData(channels: StoredChannelMap, overrides: Partial<Omit<ChannelsFileData, "channels">> = {}): ChannelsFileData {

  return {

    channels: { ...channels },
    migrationsApplied: [],
    schemaVersion: 1,
    serviceSelections: {},
    tagRegistry: { deletedTags: [], tags: [] },
    ...overrides
  };
}

/**
 * Convenience wrapper: builds a minimal ChannelsFileData envelope, runs the in-place normalizer, and returns just the channels map. Tests focused on channel
 * behavior (the majority) use this; tests that need to assert on serviceSelections build the envelope via makeChannelsData and call normalizeChannelDeltas
 * directly. Both paths share makeChannelsData's defaults so the envelope-construction rule lives in one place.
 * @param channels - The stored channels map to normalize.
 * @returns The normalized channels map (post-heal, post-delta-minimization).
 */
export function normalize(channels: StoredChannelMap): StoredChannelMap {

  const data = makeChannelsData(channels);

  normalizeChannelDeltas(data);

  return data.channels;
}

/**
 * Returns a predefined canonical channel narrowed to CanonicalChannel. PREDEFINED_CHANNELS is typed Channel (CanonicalChannel | VariantChannel) so callers
 * have to narrow at the call site; this helper does it once with a runtime guard so misuse (passing a variant key) surfaces as a thrown error rather than
 * silent type-erasure.
 * @param key - The canonical channel key (no hyphen suffix).
 * @returns The CanonicalChannel for the key.
 * @throws If the key does not exist or resolves to a variant.
 */
export function getCanonical(key: string): CanonicalChannel {

  // PREDEFINED_CHANNELS is typed as ChannelMap (Record<string, Channel>), and noUncheckedIndexedAccess already surfaces a direct index access as
  // Channel | undefined. The cast through an explicit Record<string, VariantChannel | CanonicalChannel | undefined> restates that same optional union with
  // its members spelled out, so the truthiness guard below reads cleanly without depending on the reader knowing the tsconfig flag; the runtime guard still
  // matters because the catalog content evolves and a key may disappear.
  const channel = (PREDEFINED_CHANNELS as Record<string, VariantChannel | CanonicalChannel | undefined>)[key];

  if(!channel) {

    throw new Error("getCanonical: no predefined channel with key '" + key + "'.");
  }

  // canonicalKey is structurally absent on CanonicalChannel (typed as `never`) and required on VariantChannel. Object.hasOwn returns boolean without trying
  // to index into the typed union, so it sidesteps the narrowing that would otherwise make this check appear tautological.
  if(Object.hasOwn(channel, "canonicalKey")) {

    throw new Error("getCanonical: key '" + key + "' resolves to a variant, not a canonical.");
  }

  return channel as CanonicalChannel;
}
