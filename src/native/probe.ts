/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * probe.ts: HLS manifest probe and media-feed normalizer.
 */
import type { DELTA_ELIGIBLE_BINDING_KEYS, MediaContainer, Nullable, ResolvedChannel } from "../types/index.ts";
import { LOG, chromeFetch, startTimer, stringifySorted } from "../utils/index.ts";
import { inferMediaCodec } from "./codecInference.ts";

/* This module probes an intercepted HLS playlist URL and produces a fully described MediaFeed - the canonical input to the native proxy. The HLS spec defines
 * exactly two playlist kinds, and this module normalizes both to the same shape so downstream code does not branch on which kind arrived:
 *
 * - Master (multivariant) playlists declare variant streams via #EXT-X-STREAM-INF and reference media playlist URLs. We rank the variants by descending bandwidth
 *   and take the first whose body fetches, resolve any separate audio rendition declared via #EXT-X-MEDIA:TYPE=AUDIO for that variant's own audio group, and
 *   classify the chosen body's encryption. A caller that is already relaying a stream supplies the shape of that pipeline, which narrows the same ranked walk to
 *   the candidates the running relay can absorb.
 * - Media playlists declare segments directly via #EXTINF and #EXT-X-TARGETDURATION. The input URL is itself the media feed; we fetch the body once and
 *   classify its encryption.
 *
 * In both cases the output is a MediaFeed carrying the variant URL the proxy will poll, the encryption classification, the AES-128 key URL when applicable, the
 * optional separate-audio rendition URL, and the codec/resolution/bandwidth metadata that the status display reads. Encryption classification logic is identical
 * across both kinds because it operates on the media body, not the master:
 *
 * - No #EXT-X-KEY or METHOD=NONE -> "clear" (no encryption, direct pass-through)
 * - METHOD=AES-128 with accessible key URL -> "aes128" (Node can decrypt with crypto.createDecipheriv)
 * - METHOD=SAMPLE-AES, SAMPLE-AES-CTR, or any other method -> "drm" (requires CDM, not viable)
 *
 * Playlist-kind detection is centralized in classifyHlsPlaylist() so the manifest interceptor and the probe share one source of truth for the master/media
 * decision.
 */

// Timeout for individual manifest/key fetches.
const FETCH_TIMEOUT = 10000;

// How many variants a master-playlist probe tries before giving up. Fallback covers the observed failure shape - a broken top variant with healthy siblings below
// it - while capping the tune-time worst case at three sequential fetch timeouts. The bound matters because the probe runs inside the preroll bridge window:
// capture segments are produced only once the native attempt resolves, so an unbounded crawl could outlast the preroll a client is playing while it waits. The
// cap governs both moments a stream selects a variant, the tune and every token refresh after it, so one ladder-walking policy serves the system.
const MAX_VARIANT_FALLBACK_ATTEMPTS = 3;

// The smallest segment count a tune-time probe will admit as a channel. A window holding a single segment has nothing to advance to, so it describes a fixed
// asset - a session bumper, a slate, a trailer - rather than a stream the relay can follow. Two is the floor at which a window can be observed to move at all.
const MIN_ADMISSIBLE_SEGMENT_COUNT = 2;

/**
 * Encryption classification result from probing an HLS manifest.
 */
export type EncryptionType = "aes128" | "clear" | "drm";

/**
 * Kind of HLS playlist as defined by RFC 8216. A master (multivariant) playlist declares variant streams via #EXT-X-STREAM-INF; a media playlist declares
 * segments directly via #EXTINF and #EXT-X-TARGETDURATION. Bodies that show neither signal classify as "unknown" so consumers can ignore them.
 */
export type HlsPlaylistKind = "master" | "media" | "unknown";

/**
 * The two playlist kinds the observer actually delivers - the recognized subset of HlsPlaylistKind with "unknown" removed, since unknown bodies are never
 * forwarded. Named once here so the observer's union arms, the interceptor's SelectedManifest, and the interception result's selectedKind share a single definition
 * of "a kind that was delivered" rather than re-spelling the literal pair at each site.
 */
export type RecognizedHlsPlaylistKind = Exclude<HlsPlaylistKind, "unknown">;

/**
 * Classifies an HLS playlist body by inspecting its directives. Master detection short-circuits on the first #EXT-X-STREAM-INF tag because that directive only
 * appears in master playlists; media detection accumulates positive signals (#EXTINF, #EXT-X-TARGETDURATION) across the body. Bodies with neither signal are
 * not HLS playlists. This is the single source of truth for the master/media decision - both the manifest interceptor (transport-layer "is this HLS?" gate) and
 * the probe orchestrator (resolution-layer master-vs-media branch) consume it so the classification cannot drift between call sites.
 *
 * @param body - The raw HLS playlist body text.
 * @returns The kind of playlist, or "unknown" if the body is not a recognizable HLS playlist.
 */
export function classifyHlsPlaylist(body: string): HlsPlaylistKind {

  let mediaSignal = false;

  for(const rawLine of body.split("\n")) {

    const line = rawLine.trim();

    if(!line.startsWith("#")) {

      continue;
    }

    if(line.startsWith("#EXT-X-STREAM-INF")) {

      return "master";
    }

    if(line.startsWith("#EXTINF") || line.startsWith("#EXT-X-TARGETDURATION")) {

      mediaSignal = true;
    }
  }

  return mediaSignal ? "media" : "unknown";
}

/**
 * Extracts the value of a URI="..." attribute from a single tag line. This is the one home for the URI attribute pattern in this module: the #EXT-X-KEY key
 * location, #EXT-X-MEDIA rendition URIs, and the master child walk all read a quoted URI the same way, so the pattern lives in exactly one place. Returns null when
 * the line carries no URI attribute.
 *
 * @param line - A single trimmed HLS tag line.
 * @returns The URI attribute value, or null when the line has none.
 */
function uriAttribute(line: string): Nullable<string> {

  return /URI="([^"]+)"/.exec(line)?.[1] ?? null;
}

/**
 * Resolves a single child URI against the master URL, returning null when resolution throws (a malformed relative reference). Keeps extractChildPlaylistUrls total:
 * one bad child reference drops only itself rather than aborting the whole walk.
 *
 * @param uri - The child playlist URI (relative or absolute).
 * @param masterUrl - The master manifest URL for resolving relative references.
 * @returns The resolved absolute URL, or null when resolution throws.
 */
function resolveChildUri(uri: string, masterUrl: string): Nullable<string> {

  try {

    return resolveUrl(uri, masterUrl);
  } catch {

    return null;
  }
}

/**
 * Enumerates every child playlist URI a master manifest declares, resolved to absolute URLs. Two child sources are walked: each #EXT-X-STREAM-INF variant, whose
 * URI is the immediately following non-tag line (the same single-line walk selectVariants uses, so membership sees exactly the children variant selection would),
 * and each #EXT-X-MEDIA rendition of any type that carries a URI attribute (descriptive-only renditions without a URI are skipped). probe is the single home of
 * manifest-format knowledge, so this extraction lives here and feeds the HLS playlist observer's master observations; membership judgment against these children
 * lives in the interceptor's selection policy, not here. Each URI resolves inside a per-entry guard so a single malformed reference skips only that entry and the
 * helper never throws; the returned list is deduplicated.
 *
 * @param masterBody - The master manifest text.
 * @param masterUrl - The master manifest URL for resolving relative child URIs.
 * @returns The deduplicated absolute child playlist URLs.
 */
export function extractChildPlaylistUrls(masterBody: string, masterUrl: string): string[] {

  const lines = masterBody.split("\n");
  const childUrls = new Set<string>();

  for(let i = 0; i < lines.length; i++) {

    const line = lines[i]?.trim() ?? "";

    // A variant stream's URI is the next non-tag line, matching the walk in selectVariants exactly so membership and variant selection agree on the child set.
    if(line.startsWith("#EXT-X-STREAM-INF:")) {

      const variantLine = lines[i + 1]?.trim() ?? "";

      if(variantLine && !variantLine.startsWith("#")) {

        const resolved = resolveChildUri(variantLine, masterUrl);

        if(resolved !== null) {

          childUrls.add(resolved);
        }
      }

      continue;
    }

    // A rendition's URI is a same-line attribute; renditions of every type participate, and descriptive-only entries without a URI are skipped.
    if(line.startsWith("#EXT-X-MEDIA:")) {

      const uri = uriAttribute(line);

      if(uri) {

        const resolved = resolveChildUri(uri, masterUrl);

        if(resolved !== null) {

          childUrls.add(resolved);
        }
      }
    }
  }

  return Array.from(childUrls);
}

/**
 * Reports whether a media playlist is live - one that may still grow. A playlist is live unless it carries an #EXT-X-ENDLIST marker (the producer has declared the
 * playlist complete) or an #EXT-X-PLAYLIST-TYPE:VOD tag (a fixed, fully-available asset); an EVENT-typed playlist counts as live because a live-event playlist only
 * ever appends segments. The scan is line-anchored (trim then startsWith) rather than a substring test so an ENDLIST or VOD token appearing inside a segment URI or
 * comment does not flip the classification.
 *
 * @param mediaBody - The media playlist text.
 * @returns True when the playlist is live (no ENDLIST marker and not VOD-typed).
 */
export function isLiveMediaPlaylist(mediaBody: string): boolean {

  for(const rawLine of mediaBody.split("\n")) {

    const line = rawLine.trim();

    if(line.startsWith("#EXT-X-ENDLIST")) {

      return false;
    }

    if(line.startsWith("#EXT-X-PLAYLIST-TYPE:VOD")) {

      return false;
    }
  }

  return true;
}

/**
 * Counts the segments a media playlist declares. Each segment is introduced by exactly one #EXTINF duration line, so counting those lines counts the window. The
 * scan is line-anchored - trim then startsWith - and tests the colon-suffixed form that a duration line always carries, so an #EXTINF token riding inside a segment
 * URI, a comment, or another tag's quoted attribute value cannot inflate the count.
 *
 * @param mediaBody - The media playlist text.
 * @returns The number of segments the playlist declares.
 */
function countMediaSegments(mediaBody: string): number {

  let segments = 0;

  for(const rawLine of mediaBody.split("\n")) {

    if(rawLine.trim().startsWith("#EXTINF:")) {

      segments++;
    }
  }

  return segments;
}

/**
 * Fully described HLS media feed ready for consumption by the native proxy. Every code path that produces this type (master-playlist resolution and media-only
 * passthrough) emerges with the same shape, so downstream code (proxy creation, token refresh, status display) does not branch on which playlist kind originally
 * arrived.
 */
export interface MediaFeed {

  // URL of the audio rendition playlist when the selected variant's audio group declares one via #EXT-X-MEDIA:TYPE=AUDIO with a URI. Null when the variant names
  // no audio group, meaning its audio is muxed into its own segments, and null when the group it names declares no rendition carrying a URI.
  audioVariantUrl: Nullable<string>;

  // Declared bandwidth of the selected variant in bits per second from the #EXT-X-STREAM-INF BANDWIDTH attribute. Zero when the attribute is absent or unparseable.
  bandwidth: number;

  // URL of the selected variant playlist - the highest-bandwidth variant whose manifest fetch succeeded.
  bestVariantUrl: string;

  // Video codec label (e.g., "H264", "HEVC", "AV1"), or null when the CODECS attribute is absent or the codec is unrecognized.
  codec: Nullable<string>;

  // Container format of the media playlist's segments, determined by whether the body declares an #EXT-X-MAP initialization segment. Null when encryption is
  // "drm", where the caller abandons native streaming before reading any of the media metadata.
  container: Nullable<MediaContainer>;

  // Classified encryption type.
  encryption: EncryptionType;

  // AES-128 key URL if encryption is "aes128". Null otherwise.
  keyUrl: Nullable<string>;

  // Video resolution from the #EXT-X-STREAM-INF RESOLUTION attribute (e.g., "1920x1080"). Null when the attribute is absent.
  resolution: Nullable<string>;
}

/**
 * The construction-fixed envelope of a running consumer: the container its relay reads, the encryption kind it was built to handle, and whether its audio arrives
 * as a separate rendition. Selecting under a shape admits only the candidates that consumer can absorb, which is what lets a token refresh reselect freely while
 * a stream is playing.
 *
 * Encryption is compared as a kind, not as a key location: a key URL that rotates within aes128 is ordinary token churn the relay already follows, while a change
 * of kind describes a pipeline the stream was never built for. "drm" can never be an established kind, since a DRM feed never becomes a running consumer, so a
 * constrained probe declines every drm outcome by the same comparison.
 */
export interface PipelineShape {

  // The container the consumer's relay reads. An "fmp4" source carries a separate initialization segment the relay fetches and re-references; a "ts" source is
  // self-describing.
  container: MediaContainer;

  // The encryption kind the consumer was built to handle.
  encryption: EncryptionType;

  // Whether the consumer's audio arrives as a separate rendition rather than muxed into the video segments.
  separateAudio: boolean;
}

/**
 * Reports whether a pipeline shape admits a candidate feed, comparing the three axes as equals. This is the one home of the compatibility rule: the variant walk
 * consults it to drop a fetched candidate cheaply, the media-only resolver consults it before paying for codec inference, and probeManifest consults it on the
 * fully classified feed to make the constrained probe's guarantee.
 *
 * @param shape - The running consumer's compatibility envelope.
 * @param candidate - The candidate's classification: its container (null when nothing has classified it), its encryption kind, and its audio topology.
 * @returns True when the candidate matches the shape on every axis.
 */
function shapeAdmits(shape: PipelineShape, candidate: { container: Nullable<MediaContainer>; encryption: EncryptionType; separateAudio: boolean }): boolean {

  return (candidate.container === shape.container) && (candidate.encryption === shape.encryption) && (candidate.separateAudio === shape.separateAudio);
}

/**
 * Identity of a probe-cache entry: which channel a classification belongs to, and which channel binding it was derived from. The key is the lookup identity and
 * the stamp is the validity test - a stamp built from different binding values never matches, so a lookup carrying the current binding reads an entry probed
 * under any other binding as absent rather than as a fact about the stream this tune reaches.
 */
export interface ProbeCacheIdentity {

  // The stable channel key the classification is stored under: the registry's store key for a predefined channel, the synthetic per-binding key for an ad-hoc stream.
  key: string;

  // Canonical serialization of the user-editable binding the classification was derived from, as produced by buildProbeCacheStamp.
  stamp: string;
}

/**
 * The binding projection a probe-cache stamp is derived from: one member per field of DELTA_ELIGIBLE_BINDING_KEYS (types/channels.ts), which is the system's
 * single source of truth for which fields determine the stream a tune reaches.
 *
 * The mapping deliberately drops the optionality those fields carry on ResolvedChannel, so every member must be named at the construction site even when its
 * value is undefined. That is what makes the tie to the partition a real one: adding a field to the array turns the construction site into a compile error,
 * where a projection that merely picked the fields would inherit their optionality and let the new field escape the stamp in silence.
 */
export type ProbeCacheBinding = { [Field in typeof DELTA_ELIGIBLE_BINDING_KEYS[number]]: ResolvedChannel[Field] };

/**
 * Builds the validity stamp for a probe-cache entry from the binding a tune resolves under. Serialization runs through stringifySorted, the house canonical
 * serializer, so equal bindings produce byte-equal stamps whatever order their properties were written in...and a member whose value is undefined drops out of
 * JSON entirely, which is the normalization we want: a channel that never carried a selector and one whose selector was cleared describe the same binding and
 * must stamp identically.
 *
 * Every input is a stored configuration value, never the per-tune manifest URL, whose session tokens rotate on every tune and would defeat the cache outright.
 *
 * @param binding - The user-editable binding projection the classification is derived from.
 * @returns The canonical stamp string for that binding.
 */
export function buildProbeCacheStamp(binding: ProbeCacheBinding): string {

  return stringifySorted(binding, 0);
}

// Cache of encryption types keyed by channel key. Each entry is bound by its stamp to the user-editable binding it was derived from - the
// DELTA_ELIGIBLE_BINDING_KEYS projection in types/channels.ts - so a reconfigured channel never reads a prior binding's classification. Entries hold only the
// stable classification (clear/aes128/drm) plus a timestamp for TTL expiration: variant URLs and key URLs contain session-bound auth tokens that expire between
// tunes, so they must never be cached. The DRM skip optimization in setup.ts uses this cache to avoid installing the CDP interceptor for channels known to use DRM.
const probeCache = new Map<string, { encryption: EncryptionType; stamp: string; timestamp: number }>();

// Cache entries older than this are considered stale and re-probed. 24 hours covers the case where a service changes a channel's encryption profile (e.g., free ->
// premium DRM). The DRM short-circuit in probeManifest() still applies within the TTL, so frequently-tuned DRM channels avoid repeated probe overhead.
const PROBE_CACHE_TTL = 24 * 60 * 60 * 1000;

/**
 * Returns the cached encryption type for a channel, or null if the channel has not been probed under this binding or the cache entry has expired. Used by the
 * stream setup path to skip CDP interceptor installation for channels already known to use DRM.
 *
 * @param identity - The probe-cache identity to look up: the channel key that locates the entry, and the binding stamp the entry must match.
 * @returns The cached encryption type, or null if not probed, probed under a different binding, or expired.
 */
export function getCachedEncryption(identity: ProbeCacheIdentity): Nullable<EncryptionType> {

  const entry = probeCache.get(identity.key);

  if(!entry) {

    return null;
  }

  /* The stamp test runs ahead of the age test because a mismatch asks a different question than staleness does. A classification describes the stream one
   * binding reached, and a different binding reaches a different stream, so an entry whose stamp does not match is not old - it is about something else. We
   * delete it on the way out so the slot is free for the classification the current binding produces.
   */
  if(entry.stamp !== identity.stamp) {

    probeCache.delete(identity.key);

    return null;
  }

  if((Date.now() - entry.timestamp) > PROBE_CACHE_TTL) {

    probeCache.delete(identity.key);

    return null;
  }

  return entry.encryption;
}

/**
 * Clears the probe cache for a specific channel. Called when a native stream fails, forcing a fresh probe on the next attempt. The clear is by key alone: a
 * failure invalidates whatever classification the channel holds, whichever binding produced it.
 *
 * @param channelKey - The channel key to clear from the cache.
 */
export function clearProbeCache(channelKey: string): void {

  probeCache.delete(channelKey);
}

/**
 * Probes an HLS playlist URL and returns a fully described MediaFeed. The input may be either a master playlist or a media playlist; classifyHlsPlaylist()
 * decides at runtime and the resolver dispatches accordingly. The probe cache is checked for DRM channels only: an unconstrained probe returns the cached
 * classification immediately, since its caller abandons native streaming without reading any URL, while a probe constrained to a running pipeline declines
 * instead, because no running pipeline is built around DRM. For viable channels (clear or aes128), we always run the full probe because the variant URL and key
 * URL contain auth tokens that expire between browser sessions.
 *
 * A constrained probe carries one further guarantee: every feed it returns matches the supplied pipeline shape on container, encryption kind, and audio
 * topology. A master whose candidates all mismatch resolves to null exactly as one whose candidates all fail to fetch does, because a feed the asking pipeline
 * cannot absorb is not an error - it is a feed for some other pipeline.
 *
 * @param playlistUrl - The HLS playlist URL (master or media; contains auth tokens from the browser's original request).
 * @param identity - The probe-cache identity this stream resolves under: the channel key for lookup, and the binding stamp any entry read or written must carry.
 * @param options - Probe options.
 * @param options.maxVariantAttempts - How many ranked variants a master playlist may fetch before giving up. Callers omit it and take the capped
 *                                     descending-bandwidth walk that is the selection policy for both tune and refresh; a caller naming a smaller number takes
 *                                     a narrower walk.
 * @param options.pipelineShape - The compatibility envelope of a consumer that is already running, supplied by the token-refresh path. Selection is constrained
 *                                to the candidates that shape admits. A tune omits it, having no pipeline yet to be compatible with.
 * @param options.rejectStaticPlaylists - Whether a resolved window of at most one segment is refused. The tune path sets it so a session bumper falls back to
 *                                        capture instead of becoming the stream. The refresh path leaves it off: its probe re-describes a feed the proxy is
 *                                        already relaying, so admitting the channel is not its decision to make.
 * @returns The MediaFeed, or null when the probe fails or resolves a feed the supplied pipeline shape cannot absorb.
 */
export async function probeManifest(playlistUrl: string, identity: ProbeCacheIdentity,
  options: { maxVariantAttempts?: number; pipelineShape?: PipelineShape; rejectStaticPlaylists?: boolean } = {}): Promise<Nullable<MediaFeed>> {

  const { maxVariantAttempts = MAX_VARIANT_FALLBACK_ATTEMPTS, pipelineShape, rejectStaticPlaylists = false } = options;

  // Normalize to a floor of one whole attempt. Array.prototype.slice reads a negative count from the end of the list, so an out-of-range value from a caller
  // would otherwise become a surprising selection rather than a single top-ranked try.
  const variantAttempts = Math.max(1, Math.trunc(maxVariantAttempts));

  // Short-circuit for DRM channels only. The cached DRM classification is stable within the TTL window (services rarely change DRM type), and the caller returns
  // null immediately on DRM without using any URLs. For clear/aes128 channels, we must re-probe to get fresh variant and key URLs with current auth tokens.
  const cached = getCachedEncryption(identity);

  if(cached === "drm") {

    /* A constrained probe declines the sentinel instead of returning it. The entry stays where it is - the classification remains a true fact about this channel
     * for the next tune to read - and this probe simply cannot serve it, since no running pipeline is built around DRM. Handing back the sentinel would give the
     * caller a feed carrying an empty variant URL, which reads as a successful refresh right up until the relay polls nothing.
     */
    if(pipelineShape) {

      LOG.debug("native:probe", "Declining the probe for %s: the cached classification is drm, which no running pipeline can absorb.", identity.key);

      return null;
    }

    LOG.debug("native:probe", "Probe cache hit for %s: drm.", identity.key);

    return { audioVariantUrl: null, bandwidth: 0, bestVariantUrl: "", codec: null, container: null, encryption: "drm", keyUrl: null, resolution: null };
  }

  const elapsed = startTimer();

  try {

    // Fetch the playlist body once and let classifyHlsPlaylist() decide which branch to take. The interceptor has already done a similar classification at the
    // network-observer layer, but we re-classify here because (a) the body can change between the interceptor's read and ours when the master URL serves a
    // live, mutating playlist, and (b) probeManifest() is also invoked directly by the token-refresh path which has no interceptor classification to inherit.
    const body = await fetchManifestText(playlistUrl);

    if(!body) {

      LOG.debug("native:probe", "Failed to fetch playlist for %s.", identity.key);

      return null;
    }

    const kind = classifyHlsPlaylist(body);
    const resolved = (kind === "master") ? await resolveMasterPlaylist(body, playlistUrl, variantAttempts, pipelineShape) :
      (kind === "media") ? await resolveMediaPlaylist(body, playlistUrl, pipelineShape) :
        null;

    if(!resolved) {

      LOG.debug("native:probe", "Could not resolve %s playlist for %s.", kind, identity.key);

      return null;
    }

    /* Tune admission, opted into by the caller. A window holding at most one segment is not a channel: services front their live player with a per-session
     * bumper - a single-segment playlist, often tagged live, whose sequence never moves - and the interceptor latches whichever master reaches the wire first.
     * Relaying that playlist delivers one slate segment and then stalls forever, so the tune declines here and the coordinator falls back to capture, which
     * shows whatever the page itself is playing. Liveness tagging is deliberately not consulted, because a one-segment VOD window is no more consumable than a
     * one-segment live one. This runs ahead of both the encryption classification and the cache write, so a playlist we refuse never contributes a
     * channel-level fact describing itself.
     */
    if(rejectStaticPlaylists) {

      const segments = countMediaSegments(resolved.mediaBody);

      if(segments < MIN_ADMISSIBLE_SEGMENT_COUNT) {

        LOG.debug("native:probe", "Declining native streaming for %s: the playlist window holds %s segment(s), which is a fixed asset rather than a channel.",
          identity.key, segments);

        return null;
      }
    }

    // Classify encryption from the media body. This branch is identical for master-derived and media-only feeds because #EXT-X-KEY tags live on the media
    // playlist regardless of which playlist kind originally arrived.
    const result = await classifyEncryption(resolved, identity.key);

    /* Record the classification as the channel's fact only when the top-ranked variant produced it. The master walk falls back to a lower variant when the one
     * above it fails to fetch, and a fallback-derived classification describes a variant the next tune may not select - while the top variant's own encryption
     * is exactly what stays unknown, since its fetch is what failed. Writing it down would let one variant's encryption stand in for another's for the length
     * of the TTL, including the DRM short-circuit that skips interception outright. The returned MediaFeed is unaffected either way: the caller streams the
     * variant that answered, and only the persisted claim is withheld.
     */
    if(resolved.topRankedVariant) {

      probeCache.set(identity.key, { encryption: result.encryption, stamp: identity.stamp, timestamp: Date.now() });
    }

    /* The constrained probe's guarantee is made here, once, against the finished classification. The walk compares each candidate as it reads the body, which is
     * ahead of the key fetch that can still turn an AES-128 declaration into drm, so this is the only point at which every axis is settled. A mismatch declines
     * the probe, after the classification has been recorded: what this pipeline can absorb has no bearing on what the channel is.
     */
    if(pipelineShape && !shapeAdmits(pipelineShape, { container: result.container, encryption: result.encryption,
      separateAudio: result.audioVariantUrl !== null })) {

      LOG.debug("native:probe", "Declining the resolved feed for %s: it serves %s/%s, which the running pipeline cannot absorb.", identity.key, result.container,
        result.encryption);

      return null;
    }

    LOG.debug("native:probe", "Probe completed for %s in %sms: %s (%s).", identity.key, elapsed(), result.encryption, kind);

    return result;
  } catch(error) {

    LOG.debug("native:probe", "Probe failed for %s: %s.", identity.key, String(error));

    return null;
  }
}

/**
 * Fetches a manifest URL and returns the response text. Returns null on failure.
 *
 * @param url - The manifest URL to fetch.
 * @returns The response text, or null on failure.
 */
async function fetchManifestText(url: string): Promise<Nullable<string>> {

  try {

    const response = await chromeFetch(url, { signal: AbortSignal.timeout(FETCH_TIMEOUT) });

    if(!response.ok) {

      LOG.debug("native:probe", "Manifest fetch returned HTTP %s.", response.status);

      return null;
    }

    return await response.text();
  } catch(error) {

    LOG.debug("native:probe", "Manifest fetch error: %s.", String(error));

    return null;
  }
}

/**
 * Resolved media-feed metadata produced by the master-playlist or media-playlist resolver. This is the single shape that classifyEncryption() consumes - the
 * encryption classifier does not branch on which playlist kind originally arrived, so neither does its input. The resolver is also responsible for filling in
 * codec/resolution/bandwidth from whatever signal its branch can read (master playlist's #EXT-X-STREAM-INF for the master branch, first-segment inference for
 * the media branch).
 */
interface ResolvedMedia {

  // URL of the separate audio rendition playlist when the selected variant's audio group declares one via #EXT-X-MEDIA:TYPE=AUDIO. Always null for media-only
  // feeds because audio renditions are a master-playlist-level concept.
  audioVariantUrl: Nullable<string>;

  // Declared bandwidth in bits per second from the master's BANDWIDTH attribute. Zero for media-only feeds because the playlist itself carries no bandwidth
  // declaration.
  bandwidth: number;

  // Human-readable video codec label (e.g., "H264", "HEVC"), or null when neither the master's CODECS attribute nor first-segment inference produced a label.
  codec: Nullable<string>;

  // The container the resolver already classified, carried so the converge site consumes it rather than scanning the body a second time. Null when nothing
  // classified it, which is every resolution that ran without a pipeline shape to compare against.
  container: Nullable<MediaContainer>;

  // The media playlist body. classifyEncryption() walks this for #EXT-X-KEY tags; for master-derived feeds this is the chosen variant's body, for media-only
  // feeds this is the input playlist itself.
  mediaBody: string;

  // The media playlist URL. The proxy polls this URL on its segment-fetch cycle.
  mediaUrl: string;

  // Video resolution (e.g., "1920x1080") from the master's RESOLUTION attribute. Always null for media-only feeds because TS PMT does not carry resolution and
  // SPS-level inference is out of scope; recovering it would require parsing the SPS NALU inside a video access unit.
  resolution: Nullable<string>;

  // Whether the variant that answered was the first-ranked candidate. False when the master walk fell back past a broken top variant, which is what tells the
  // probe that this feed's encryption describes a variant the next tune may not select. Always true for media-only feeds - a single feed is trivially the top.
  topRankedVariant: boolean;
}

/**
 * Resolves a master playlist into a ResolvedMedia. The declared variants are walked in descending-bandwidth order and the first one that answers usably becomes
 * the feed - so a master whose top variant is broken still yields a stream through a healthy sibling beneath it. The audio rendition resolves from the chosen
 * variant's own audio group, which keeps the feed's audio bound to the video it accompanies.
 *
 * Under a pipeline shape the same ranked walk runs, narrowed to the candidates a running consumer can absorb. Eligibility is decided lazily, as the descent
 * reaches each variant, so the cost of the constraint is proportional to how far the walk actually goes. Only a fetch counts against the attempt budget: a
 * candidate ruled out by pure string work costs nothing and spends nothing.
 *
 * @param masterBody - The master manifest text.
 * @param masterUrl - The master manifest URL for resolving relative variant URLs.
 * @param maxVariantAttempts - How many candidate fetches to spend before giving up.
 * @param pipelineShape - The running consumer's compatibility envelope, or undefined for an unconstrained walk.
 * @returns The resolved media feed metadata, or null when the master yields no candidate this walk can use.
 */
async function resolveMasterPlaylist(masterBody: string, masterUrl: string, maxVariantAttempts: number,
  pipelineShape?: PipelineShape): Promise<Nullable<ResolvedMedia>> {

  const variants = selectVariants(masterBody, masterUrl);

  if(!variants.length) {

    LOG.debug("native:probe", "No variant streams found in master manifest.");

    return null;
  }

  if(maxVariantAttempts < variants.length) {

    LOG.debug("native:probe", "Attempting at most %s of %s advertised variant(s).", maxVariantAttempts, variants.length);
  }

  let attempts = 0;

  for(const variant of variants) {

    if(attempts >= maxVariantAttempts) {

      break;
    }

    /* The audio topology axis is settled before the fetch because it can be: the rendition resolves from the master body already in hand, with no network work
     * at all. A candidate whose topology differs from the running consumer's is therefore dropped without spending an attempt, and the URL this resolution
     * produces is carried to the feed below so the winner never derives it twice. An unconstrained walk resolves nothing here, keeping that work on the one
     * candidate it selects.
     */
    let audioVariantUrl = pipelineShape ? resolveAudioRendition(masterBody, masterUrl, variant.audioGroupId) : null;

    if(pipelineShape && ((audioVariantUrl !== null) !== pipelineShape.separateAudio)) {

      LOG.debug("native:probe", "Skipping the variant at %s bps: its audio topology differs from the running pipeline's.", variant.bandwidth);

      continue;
    }

    attempts++;

    LOG.debug("native:probe", "Attempting variant at %s bps: %s.", variant.bandwidth, variant.url.slice(0, 120));

    /* Fetch this candidate's manifest. The candidate that answers is what classifyEncryption() inspects for #EXT-X-KEY tags and what the proxy polls for segments.
     * The fetches run one after another because the ranking IS the selection policy: a lower variant is tried only once the one above it has actually failed, and
     * a healthy top variant therefore costs exactly one fetch.
     */
    // eslint-disable-next-line no-await-in-loop
    const variantBody = await fetchManifestText(variant.url);

    if(!variantBody) {

      LOG.debug("native:probe", "Failed to fetch variant manifest for %s.", variant.url);

      continue;
    }

    /* The remaining two axes read the body that just arrived: the container tag scan, and the encryption tag scan without the key fetch that settles an aes128
     * declaration. A candidate that mismatches on either is treated exactly as a failed fetch - it is a feed for some other pipeline - so the walk moves on to
     * the next one rather than abandoning the master. The container this scan produces travels with the feed, which is what spares the converge site a second
     * pass over the same body.
     */
    let container: Nullable<MediaContainer> = null;

    if(pipelineShape) {

      container = classifyContainer(variantBody);

      const declaredEncryption = scanEncryptionDeclaration(variantBody).kind;

      if(!shapeAdmits(pipelineShape, { container, encryption: declaredEncryption, separateAudio: audioVariantUrl !== null })) {

        LOG.debug("native:probe", "Skipping the variant at %s bps: it serves %s/%s, which the running pipeline cannot absorb.", variant.bandwidth, container,
          declaredEncryption);

        continue;
      }
    } else {

      audioVariantUrl = resolveAudioRendition(masterBody, masterUrl, variant.audioGroupId);
    }

    if(audioVariantUrl) {

      LOG.debug("native:probe", "Separate audio rendition found: %s.", audioVariantUrl.slice(0, 120));
    }

    return {

      audioVariantUrl,
      bandwidth: variant.bandwidth,
      codec: variant.codec,
      container,
      mediaBody: variantBody,
      mediaUrl: variant.url,
      resolution: variant.resolution,

      // The ranking's own first variant is what the cache write is conditioned on, so a constrained winner reached further down the ladder is measured against
      // the full ranking rather than against the narrowed walk that selected it.
      topRankedVariant: variant === variants[0]
    };
  }

  LOG.debug("native:probe", "None of the %s attempted variant(s) produced a usable feed.", attempts);

  return null;
}

/**
 * Resolves a media playlist into a ResolvedMedia. The input URL is itself the media feed - there is no master to traverse - so the resolver wraps the body and
 * URL verbatim, infers the codec from the first segment via codecInference.ts, and returns. Resolution stays null because TS PMT does not carry resolution and
 * SPS-level inference is out of scope.
 *
 * A media playlist is its own single candidate, so under a pipeline shape this is where it is admitted or declined: its audio is muxed by definition, and its
 * container and encryption kind read from the body already in hand. The comparison runs ahead of the codec inference because that inference fetches a segment,
 * and a feed about to be declined must not cost a network round trip.
 *
 * @param mediaBody - The media playlist text.
 * @param mediaUrl - The media playlist URL (the proxy will poll this).
 * @param pipelineShape - The running consumer's compatibility envelope, or undefined for an unconstrained resolution.
 * @returns The resolved media feed metadata, or null when the shape cannot absorb this feed.
 */
async function resolveMediaPlaylist(mediaBody: string, mediaUrl: string, pipelineShape?: PipelineShape): Promise<Nullable<ResolvedMedia>> {

  let container: Nullable<MediaContainer> = null;

  if(pipelineShape) {

    container = classifyContainer(mediaBody);

    const declaredEncryption = scanEncryptionDeclaration(mediaBody).kind;

    if(!shapeAdmits(pipelineShape, { container, encryption: declaredEncryption, separateAudio: false })) {

      LOG.debug("native:probe", "Declining the media playlist at %s: it serves %s/%s with muxed audio, which the running pipeline cannot absorb.",
        mediaUrl.slice(0, 120), container, declaredEncryption);

      return null;
    }
  }

  // Best-effort codec inference. Returns codec=null on any failure (no segment, fetch error, unrecognized format) so the rest of the pipeline continues unimpaired.
  const inferred = await inferMediaCodec({ baseUrl: mediaUrl, playlistBody: mediaBody });

  return {

    audioVariantUrl: null,
    bandwidth: 0,
    codec: inferred.codec,
    container,
    mediaBody,
    mediaUrl,
    resolution: null,
    topRankedVariant: true
  };
}

/**
 * Metadata for one variant stream declared by a master manifest. Produced by selectVariants() for resolveMasterPlaylist(), which walks the ranked candidates until
 * one of them fetches.
 */
interface VariantSelection {

  // The variant's AUDIO attribute value, naming the audio rendition group that carries this variant's audio. Null when the variant declares no AUDIO attribute,
  // which per RFC 8216 means its audio is muxed into its own segments.
  audioGroupId: Nullable<string>;

  // Declared bandwidth in bits per second from the BANDWIDTH attribute.
  bandwidth: number;

  // Video codec from the CODECS attribute (e.g., "H264", "HEVC", "AV1"), or null when the attribute is absent or the codec is unrecognized.
  codec: Nullable<string>;

  // Video resolution from the RESOLUTION attribute (e.g., "1920x1080"), or null when absent.
  resolution: Nullable<string>;

  // Absolute URL of this candidate's variant playlist.
  url: string;
}

/**
 * Parses every #EXT-X-STREAM-INF entry in a master manifest and returns the declared variants ranked by descending bandwidth. An entry whose URI line is missing,
 * or whose URI cannot be resolved against the master URL, drops out of the list so that one malformed declaration costs only itself.
 *
 * @param masterBody - The master manifest content.
 * @param masterUrl - The master manifest URL for resolving relative variant URLs.
 * @returns The resolvable variants ordered by descending bandwidth, empty when the master declares none.
 */
function selectVariants(masterBody: string, masterUrl: string): VariantSelection[] {

  // Map video codec prefixes from CODECS attribute to human-readable labels. Defined outside the variant iteration loop to avoid per-line allocation.
  const codecPrefixes: Record<string, string> = { "av01": "AV1", "avc1": "H264", "avc3": "H264", "hev1": "HEVC", "hvc1": "HEVC", "vp09": "VP9" };

  const lines = masterBody.split("\n");
  const variants: VariantSelection[] = [];

  for(let i = 0; i < lines.length; i++) {

    const line = lines[i]?.trim() ?? "";

    if(!line.startsWith("#EXT-X-STREAM-INF:")) {

      continue;
    }

    // Parse BANDWIDTH attribute.
    const bandwidth = Number(/BANDWIDTH=(\d+)/.exec(line)?.[1] ?? 0);

    // Parse RESOLUTION attribute (e.g., RESOLUTION=1920x1080).
    const resolution: Nullable<string> = /RESOLUTION=(\d+x\d+)/.exec(line)?.[1] ?? null;

    // Parse the AUDIO attribute, which names the rendition group holding this variant's audio. Its absence means the audio is muxed into the variant's segments.
    const audioGroupId: Nullable<string> = /AUDIO="([^"]+)"/.exec(line)?.[1] ?? null;

    // Parse CODECS attribute and map the video codec prefix to a human-readable label. The CODECS value contains comma-separated codec strings (e.g.,
    // "avc1.640028,mp4a.40.2"). The video codec is identified by its prefix: avc1/avc3 -> H264, hvc1/hev1 -> HEVC, av01 -> AV1, vp09 -> VP9.
    const codecsValue = /CODECS="([^"]+)"/.exec(line)?.[1];
    let codec: Nullable<string> = null;

    if(codecsValue) {

      const prefix = codecsValue.split(",")[0]?.trim().split(".")[0];

      codec = prefix ? (codecPrefixes[prefix] ?? null) : null;
    }

    // The variant URL is on the next line.
    const variantLine = lines[i + 1]?.trim() ?? "";

    if(!variantLine || variantLine.startsWith("#")) {

      continue;
    }

    // Resolve each candidate inside the per-entry guard, the same way the membership walk does. Because every declared variant is resolved here rather than only
    // the one that wins, a naked resolveUrl would let a single malformed URI throw the entire probe away through probeManifest's outer catch.
    const url = resolveChildUri(variantLine, masterUrl);

    if(url === null) {

      continue;
    }

    variants.push({ audioGroupId, bandwidth, codec, resolution, url });
  }

  LOG.debug("native:probe", "Found %s variant(s) with bandwidths: %s.", variants.length, variants.map((variant) => variant.bandwidth).join(", "));

  // Rank by descending bandwidth. Array.prototype.sort is stable, so variants sharing a bandwidth keep their document order - which is what lets a master that
  // advertises no BANDWIDTH at all, and therefore parses entirely as zeroes, resolve through its first declared variant with no special-case handling.
  variants.sort((first, second) => second.bandwidth - first.bandwidth);

  return variants;
}

/**
 * Classifies the encryption type of a media playlist by parsing its #EXT-X-KEY tags. Operates uniformly on master-derived and media-only ResolvedMedia inputs
 * because #EXT-X-KEY tags are a media-playlist-level concept regardless of whether a master playlist sat above the media.
 *
 * @param resolved - The resolved media feed metadata.
 * @param channelKey - The channel key for logging.
 * @returns The MediaFeed with the classified encryption type and (when applicable) the AES-128 key URL.
 */
/**
 * Classifies a media playlist's container format by looking for an #EXT-X-MAP declaration. A playlist that references an initialization segment is fMP4/CMAF;
 * one that does not is MPEG-TS, whose segments carry their own PAT/PMT and need no init. The scan is line-anchored - each line is trimmed and tested with
 * startsWith - so a segment or key URI that happens to contain the literal "#EXT-X-MAP:" inside a query string cannot be mistaken for the tag. This mirrors the
 * discipline classifyHlsPlaylist uses for the master/media decision.
 *
 * @param mediaBody - The media playlist body text.
 * @returns "fmp4" when the body declares an initialization segment, "ts" otherwise.
 */
function classifyContainer(mediaBody: string): MediaContainer {

  for(const rawLine of mediaBody.split("\n")) {

    if(rawLine.trim().startsWith("#EXT-X-MAP:")) {

      return "fmp4";
    }
  }

  return "ts";
}

/**
 * What a media playlist's #EXT-X-KEY tags declare, read from the tags alone. The "aes128" arm carries the key URI the tag named, which is what settles the
 * classification once fetched; the "drm" arm carries the method that produced it along with why, so the two shapes that reach DRM from the tags - a method Node
 * cannot decrypt, and an AES-128 tag naming no key to fetch - stay tellable apart in the field diagnostics.
 */
type EncryptionDeclaration = { keyUri: string; kind: "aes128" } | { kind: "clear" } | { kind: "drm"; method: string; reason: "no-key-uri" | "unsupported-method" };

/**
 * Reads a media playlist's #EXT-X-KEY tags and reports what they declare. This is the tag scan alone - it issues no request, so it costs one pass over a body the
 * caller already holds. A NONE tag does not settle the classification on its own: some manifests interleave a NONE tag with a later, more restrictive key tag
 * (a clear lead-in segment followed by an AES-128 or DRM-protected one), so the scan keeps reading and the first method that is not NONE settles it, favoring the
 * strongest encryption signal present over the order in which the tags happen to appear.
 *
 * Two callers read this, with different appetites: the encryption classifier settles an "aes128" declaration by fetching the key it names, while the constrained
 * variant walk compares the declared kind against a running pipeline's and moves on, paying for no fetch it would only discard.
 *
 * @param mediaBody - The media playlist body text.
 * @returns What the key tags declare.
 */
function scanEncryptionDeclaration(mediaBody: string): EncryptionDeclaration {

  for(const line of mediaBody.split("\n")) {

    const trimmed = line.trim();

    if(!trimmed.startsWith("#EXT-X-KEY:")) {

      continue;
    }

    // Parse METHOD attribute.
    const method = /METHOD=([A-Za-z0-9-]+)/.exec(trimmed)?.[1]?.toUpperCase() ?? "NONE";

    if(method === "NONE") {

      continue;
    }

    if(method === "AES-128") {

      // Parse URI attribute for the key URL. A key that names no location cannot be fetched, so the declaration is no more usable than an unsupported method.
      const uri = uriAttribute(trimmed);

      return (uri === null) ? { kind: "drm", method, reason: "no-key-uri" } : { keyUri: uri, kind: "aes128" };
    }

    // SAMPLE-AES, SAMPLE-AES-CTR, or any other method indicates DRM.
    return { kind: "drm", method, reason: "unsupported-method" };
  }

  return { kind: "clear" };
}

async function classifyEncryption(resolved: ResolvedMedia, channelKey: string): Promise<MediaFeed> {

  const declaration = scanEncryptionDeclaration(resolved.mediaBody);
  let encryption: EncryptionType = declaration.kind;
  let keyUrl: Nullable<string> = null;

  switch(declaration.kind) {

    case "aes128": {

      const rawKeyUrl = resolveUrl(declaration.keyUri, resolved.mediaUrl);

      // Test that the key is accessible and is exactly 16 bytes. This is the classification's only request, and a body that named no key never reaches it.
      const keyAccessible = await testKeyAccessibility(rawKeyUrl);

      if(keyAccessible) {

        keyUrl = rawKeyUrl;
      } else {

        LOG.debug("native:probe", "AES-128 key inaccessible or wrong size for %s.", channelKey);
        encryption = "drm";
      }

      break;
    }

    case "drm": {

      if(declaration.reason === "no-key-uri") {

        LOG.debug("native:probe", "AES-128 key tag has no URI for %s.", channelKey);
      } else {

        LOG.debug("native:probe", "Unsupported encryption method '%s' for %s.", declaration.method, channelKey);
      }

      break;
    }

    default: {

      // A clear body names no key, so there is nothing to fetch and nothing to report.
      break;
    }
  }

  return {

    audioVariantUrl: resolved.audioVariantUrl,
    bandwidth: resolved.bandwidth,
    bestVariantUrl: resolved.mediaUrl,
    codec: resolved.codec,

    // Both playlist-kind branches converge here, so this is the one place the container is decided. A DRM feed carries null because the caller abandons native
    // streaming without reading it, and classifying a body we will never relay would be a fabricated value. A resolver that already classified the body under a
    // pipeline shape hands its result across, so no body is scanned twice.
    container: (encryption === "drm") ? null : (resolved.container ?? classifyContainer(resolved.mediaBody)),
    encryption,
    keyUrl,
    resolution: resolved.resolution
  };
}

/**
 * Tests whether an AES-128 key URL is accessible and returns a 16-byte key.
 *
 * @param keyUrl - The key URL to test.
 * @returns True if the key is accessible and exactly 16 bytes.
 */
async function testKeyAccessibility(keyUrl: string): Promise<boolean> {

  try {

    const response = await chromeFetch(keyUrl, { signal: AbortSignal.timeout(FETCH_TIMEOUT) });

    if(!response.ok) {

      return false;
    }

    const buffer = await response.arrayBuffer();

    LOG.debug("native:probe", "Key fetch returned %s bytes.", buffer.byteLength);

    return buffer.byteLength === 16;
  } catch(error) {

    LOG.debug("native:probe", "Key accessibility test failed: %s.", String(error));

    return false;
  }
}

/**
 * Resolves the audio rendition playlist belonging to the selected variant's audio group. RFC 8216 section 4.3.4.2.1 binds a variant to its renditions through the
 * AUDIO attribute, so the renditions a master declares for other groups say nothing about this variant. Within the named group the first DEFAULT=YES rendition
 * carrying a URI wins, and the first URI-bearing rendition of the group is the fallback when the group names no default. Null covers two outcomes - a variant
 * whose audio is muxed by design, and a declared group in which no rendition carries a URI - and the debug log on the second tells them apart in the field.
 *
 * @param masterBody - The master manifest content.
 * @param masterUrl - The master manifest URL for resolving relative URIs.
 * @param audioGroupId - The selected variant's AUDIO group, or null when the variant declares none.
 * @returns The absolute audio variant URL, or null.
 */
function resolveAudioRendition(masterBody: string, masterUrl: string, audioGroupId: Nullable<string>): Nullable<string> {

  // A variant that names no audio group carries its audio inside its own segments, so no rendition in the master applies to it.
  if(audioGroupId === null) {

    return null;
  }

  let candidateUri: Nullable<string> = null;

  for(const line of masterBody.split("\n")) {

    const trimmed = line.trim();

    if(!trimmed.startsWith("#EXT-X-MEDIA:")) {

      continue;
    }

    // Only match AUDIO renditions.
    if(!trimmed.includes("TYPE=AUDIO")) {

      continue;
    }

    // Match the group by its exact value. A substring test would accept a neighboring group whose id merely contains the selected one.
    if(/GROUP-ID="([^"]+)"/.exec(trimmed)?.[1] !== audioGroupId) {

      continue;
    }

    // Extract the URI attribute. Not all #EXT-X-MEDIA:TYPE=AUDIO tags have a URI - some are descriptive-only when audio is muxed into the video variant. Such a
    // tag is skipped even when it is the group's default, so a descriptive default never ends the walk ahead of a sibling that can actually be played.
    const uri = uriAttribute(trimmed);

    if(!uri) {

      continue;
    }

    // The group's default rendition wins outright; any other is held only until a default turns up.
    if(trimmed.includes("DEFAULT=YES")) {

      return resolveUrl(uri, masterUrl);
    }

    candidateUri ??= uri;
  }

  if(candidateUri === null) {

    LOG.debug("native:probe", "No audio rendition carrying a URI was found in group '%s'.", audioGroupId);

    return null;
  }

  return resolveUrl(candidateUri, masterUrl);
}

/**
 * Resolves a potentially relative URL against a base URL. Handles both absolute and relative URLs. Exported for reuse by the proxy module.
 *
 * @param url - The URL to resolve (may be relative or absolute).
 * @param baseUrl - The base URL for resolving relative references.
 * @returns The resolved absolute URL.
 */
export function resolveUrl(url: string, baseUrl: string): string {

  // If the URL is already absolute, return it directly.
  if(url.startsWith("http://") || url.startsWith("https://")) {

    return url;
  }

  // Use the URL constructor to resolve relative URLs against the base.
  return new URL(url, baseUrl).href;
}
