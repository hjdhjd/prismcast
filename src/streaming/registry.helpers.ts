/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * registry.helpers.ts: Test-only helper for constructing StreamRegistryEntry fixtures. Co-located with the registry module. Used across streaming/{clients,
 * lifecycle, registry, hlsSegments}.test.ts and hdhr/discover.test.ts to build entries with sensible defaults that any single test can override.
 *
 * Before this helper, five test files each had their own makeRegistryEntry/makeStreamEntry/makeStream variant. Defaults differed subtly between them
 * (channelName: null vs. "test-channel", hls: createHLSState() vs. {} as cast, id sources, etc.) which made it harder to reason about test-data invariants
 * across files. This helper unifies the defaults; every test that needs an entry imports the same factory and overrides only the fields its scenario cares
 * about.
 *
 * The streamIdStr default uses the production formatter (generateStreamId in setup.ts) so the fixture shape stays in lockstep with what production actually
 * writes. If production changes the format - e.g., a different request-id length or prefix shape - tests pick up the change automatically rather than asserting
 * against a stale "test-stream-NN" placeholder that no real stream ever carries.
 */
import { type StreamRegistryEntry, createHLSState, getNextStreamId } from "./registry.ts";
import { declareKeysOf } from "../testing.helpers.ts";
import { generateStreamId } from "./setup.ts";

/**
 * Compile-time-complete enumeration of every key in StreamRegistryEntry. Pair with assertSameShape in registry.helpers.test.ts to catch drift in either
 * direction:
 *
 * - If StreamRegistryEntry gains a key, declareKeysOf's completeness check fails to compile - the array must be updated.
 * - Once the array is updated, the assertSameShape test fails - the factory must populate the new key.
 *
 * Drift can never silently land. The exported list is consumed only by the parity test (the runtime cost is a single sorted-array comparison once per test
 * run); production code paths use the type directly.
 */
export const STREAM_REGISTRY_ENTRY_KEYS = declareKeysOf<StreamRegistryEntry>()([

  "captureCodec",
  "channelName",
  "clientAddress",
  "ffmpegProcess",
  "hardwareAccelerated",
  "hls",
  "id",
  "info",
  "mpegTsClientCount",
  "nativeBandwidth",
  "nativeProxy",
  "nativeResolution",
  "page",
  "preTuned",
  "profile",
  "rawCaptureStream",
  "segmenter",
  "startTime",
  "stopMonitor",
  "streamIdStr",
  "streamingMode",
  "url"
] as const);

/**
 * Constructs a StreamRegistryEntry with sensible defaults for tests. Defaults are deliberately neutral - empty/null/zero for every nullable field, a fresh
 * HLSState, a fresh id from getNextStreamId, a stable test URL, and "capture" streaming mode. Tests override the subset of fields their scenario cares about
 * via the overrides parameter.
 *
 * The id default comes from getNextStreamId() so registry-keyed assertions stay deterministic relative to the order of registry usage in a test file: every
 * call returns a unique id. Tests that need a specific id should set it explicitly via overrides.id.
 *
 * The streamIdStr default is computed from the resolved channelName/url via the production generateStreamId formatter, so the fixture shape matches what
 * production writes (e.g., "channelName-abc123" or "domain-abc123"). Tests that need a deterministic stream-id-string for log-prefix assertions should set it
 * explicitly via overrides.streamIdStr.
 *
 * @param overrides - Partial fields to override the defaults. Spread into the returned entry.
 * @returns A fully-populated StreamRegistryEntry suitable for use in registry/lifecycle/clients tests.
 */
export function makeRegistryEntry(overrides: Partial<StreamRegistryEntry> = {}): StreamRegistryEntry {

  const id = overrides.id ?? getNextStreamId();
  const channelName = overrides.channelName ?? null;
  const url = overrides.url ?? "https://example.test/stream";
  const streamIdStr = overrides.streamIdStr ?? generateStreamId(channelName ?? undefined, url);

  return {

    captureCodec: null,
    channelName,
    clientAddress: null,
    ffmpegProcess: null,
    hardwareAccelerated: false,
    hls: createHLSState(),
    id,
    info: { lastPlaylistRequest: 0, storeKey: "test-channel" },
    mpegTsClientCount: 0,
    nativeBandwidth: 0,
    nativeProxy: null,
    nativeResolution: null,
    page: null,
    preTuned: false,
    profile: null,
    rawCaptureStream: null,
    segmenter: null,
    startTime: new Date(),
    stopMonitor: null,
    streamIdStr,
    streamingMode: "capture",
    url,
    ...overrides
  };
}
