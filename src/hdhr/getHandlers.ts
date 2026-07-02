/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * getHandlers.ts: Data-driven dispatch table for HDHomeRun UDP Get requests.
 *
 * HDHR-aware clients can issue UDP Get requests for a small fixed catalog of named state keys ("/sys/version", "/tuner0/channel", ...) once they have located a
 * device via Discover. The transport layer parses each request and routes it through this module's resolveGet function; the resolver consults a table of
 * static-system keys (those whose value is a constant or a CONFIG-derived string) and a regex for per-tuner keys (those whose value depends on which slot is
 * being addressed). Unknown keys fall through to an error reply, matching real HDHR firmware behavior.
 *
 * Two layering choices keep this module masterclass-shaped rather than ad-hoc:
 *
 *   1. Pure functions over a GetContext seam. Tests construct a synthetic context with any tuner state and assert reply values without touching CONFIG, the
 *      stream registry, or any other global. The transport layer composes the production context once per request and passes it through.
 *
 *   2. A Record-keyed dispatch table for /sys/* keys, plus a single regex for /tuner&lt;N&gt;/*. Adding a new system key is a single row; adding a new per-tuner key
 *      is one additional case in resolveTunerKey. The dispatch shape stays declarative even as the catalog grows.
 */
import { HDHR_COPYRIGHT, HDHR_HW_MODEL, HDHR_MODEL } from "./identity.ts";
import type { TunerState } from "./tunerState.ts";

// Canonical wire-protocol error string for fall-through cases (unknown system key, unknown per-tuner sub-key). Real HDHomeRun firmware uses this exact wording;
// mirroring it keeps client error displays familiar to operators who have seen it from real devices. Hoisted to a module constant so both fall-through sites
// agree on the spelling - any future tweak (capitalization, punctuation) lands in one place.
const ERROR_UNKNOWN_GETSET = "ERROR: unknown getset variable";

/**
 * Result of resolving a Get key. The discriminated union mirrors the protocol's reply shape: a value reply carries the answered string; an error reply carries
 * the rejection reason. Callers translate the result into a wire packet via buildGetReply/buildErrorReply.
 */
export type GetResult =
  { readonly kind: "value"; readonly value: string } |
  { readonly kind: "error"; readonly error: string };

/**
 * Context required to answer a Get request. Carries the slot-indexed tuner state (so per-tuner keys can look up the addressed slot) and a runtime-version
 * string (so /sys/version reflects the running build). Other identity-shaped values come from hdhr/identity.ts and do not need to flow through the context.
 */
export interface GetContext {

  // PrismCast's package version, used to answer /sys/version.
  readonly runtimeVersion: string;

  // The slot-indexed tuner state from getTunerStates(), used for /tuner&lt;N&gt;/* lookups.
  readonly tuners: readonly TunerState[];
}

/**
 * Dispatch table for /sys/* keys. Each entry is a single-argument function over GetContext returning the answer string. The table is the SSOT for which system
 * keys PrismCast answers; adding a new key is a single row, and the parser, error path, and test surface require no other changes.
 */
const SYSTEM_HANDLERS: Record<string, (ctx: GetContext) => string> = {

  // Copyright string returned when probes call /sys/copyright. Static across runtime; sourced from the identity SSOT.
  "/sys/copyright": (): string => HDHR_COPYRIGHT,

  // Debug surface placeholder. Real HDHRs return verbose multi-line debug strings here; PrismCast returns an empty value so probes succeed without leaking
  // internal state. Operators that want richer debug should consult the standard PrismCast logs instead.
  "/sys/debug": (): string => "",

  // Hardware model identifier. Constant across runtime; sourced from the identity SSOT.
  "/sys/hwmodel": (): string => HDHR_HW_MODEL,

  // Software model identifier (same as hardware model on the HDTC-2US family).
  "/sys/model": (): string => HDHR_MODEL,

  // Running version string, derived from package.json via the runtime context.
  "/sys/version": (ctx: GetContext): string => ctx.runtimeVersion
};

// Regex matching per-tuner keys: "/tuner<N>/<sub-key>". Numeric capture group lets resolveTunerKey index into ctx.tuners; the second capture is the sub-key.
const TUNER_KEY_PATTERN = /^\/tuner(\d+)\/(.+)$/;

/**
 * Resolves a Get key to a reply value or an error. Single entry point the transport layer calls per request. Unknown keys produce an error result matching the
 * "ERROR: unknown getset variable" prose real HDHR firmware uses.
 * @param name - The slash-delimited Get key from the request.
 * @param ctx - The resolution context (tuner state + runtime version).
 * @returns The resolved value or an error.
 */
export function resolveGet(name: string, ctx: GetContext): GetResult {

  // System keys go through the dispatch table first because they cover the most common probes (version, model, copyright).
  const systemHandler = SYSTEM_HANDLERS[name];

  if(systemHandler !== undefined) {

    return { kind: "value", value: systemHandler(ctx) };
  }

  // Per-tuner keys go through the regex extractor. A match means "/tunerN/..." but the slot index may still be out of bounds for the configured tuner count;
  // resolveTunerKey enforces both that bound and the sub-key catalog.
  const tunerMatch = TUNER_KEY_PATTERN.exec(name);

  if(tunerMatch !== null) {

    return resolveTunerKey(Number(tunerMatch[1]), tunerMatch[2] ?? "", ctx);
  }

  // Fall through to the canonical "unknown getset variable" error so unknown system keys produce the same wording real HDHR firmware would.
  return { error: ERROR_UNKNOWN_GETSET, kind: "error" };
}

/**
 * Resolves a per-tuner Get key for a specific slot. Sub-keys fall into two families: static state (filter, lockkey, channelmap, target) returning constants, and
 * dynamic state (channel, status, streaminfo) reading from the addressed TunerState. Unknown sub-keys produce an error matching real HDHR behavior.
 * @param slot - Zero-based tuner slot index.
 * @param subKey - The sub-key portion after the slot prefix.
 * @param ctx - The resolution context.
 * @returns The resolved value or an error.
 */
function resolveTunerKey(slot: number, subKey: string, ctx: GetContext): GetResult {

  const state = ctx.tuners[slot] ?? null;

  if(state === null) {

    // Slot index out of range. Real HDHRs return the same error for an unknown tuner index as for an unknown sub-key.
    return { error: "ERROR: unknown tuner", kind: "error" };
  }

  switch(subKey) {

    case "channel": {

      return { kind: "value", value: formatChannel(state) };
    }

    case "channelmap": {

      // Real HDHRs return the tuning band ("us-bcast", "us-cable"). PrismCast has no RF input, so we return an empty string - clients that filter on this
      // value will route us to the generic IP-tuner code path.
      return { kind: "value", value: "" };
    }

    case "debug": {

      return { kind: "value", value: "" };
    }

    case "filter": {

      // PID filter. Default value matches a fully-open filter (every PID accepted), which is the right wire value for a tuner that has no PSI multiplexing.
      return { kind: "value", value: "0x0000-0x1FFF" };
    }

    case "lockkey": {

      // Lockkey is a write-once exclusive-tuner lock real HDHRs use. PrismCast has no exclusivity model, so we report it as unlocked.
      return { kind: "value", value: "none" };
    }

    case "program": {

      // MPEG-TS program number. We do not surface a useful value because PrismCast remuxes into a single-program MPEG-TS; clients that need it can read the
      // stream itself.
      return { kind: "value", value: "0" };
    }

    case "status": {

      return { kind: "value", value: formatStatus(state) };
    }

    case "streaminfo": {

      return { kind: "value", value: formatStreamInfo(state) };
    }

    case "target": {

      return { kind: "value", value: formatTarget() };
    }

    case "vchannel": {

      return { kind: "value", value: formatVchannel(state) };
    }

    case "vstatus": {

      return { kind: "value", value: formatVstatus(state) };
    }

    default: {

      return { error: ERROR_UNKNOWN_GETSET, kind: "error" };
    }
  }
}

/**
 * Formats /tuner&lt;N&gt;/channel: the currently-tuned channel in HDHR notation. Active slots produce "auto:&lt;number&gt;" matching the HTTP-streaming convention; idle
 * slots produce "none" as real HDHRs do.
 * @param state - The slot's state.
 * @returns The wire-formatted channel string.
 */
function formatChannel(state: TunerState): string {

  if(!state.active || (state.channelNumber === null)) {

    return "none";
  }

  return "auto:" + String(state.channelNumber);
}

/**
 * Formats /tuner&lt;N&gt;/status: a single-line status string. Real HDHRs report something like "ch=auto:5 lock=8vsb ...". PrismCast has no analog tuning, so the
 * relevant fields are channel binding and stream state.
 * @param state - The slot's state.
 * @returns The wire-formatted status string.
 */
function formatStatus(state: TunerState): string {

  if(!state.active) {

    return "ch=none lock=none ss=0 snq=0 seq=0 bps=0 pps=0";
  }

  // We report 100% on the signal metrics because the IP path either delivers bytes or it does not - there is no analog gradation. Bitrate is left at zero
  // because we do not track it at this layer; clients that care can read /stream/* directly.
  return "ch=" + formatChannel(state) + " lock=8vsb ss=100 snq=100 seq=100 bps=0 pps=0";
}

/**
 * Formats /tuner&lt;N&gt;/streaminfo: a brief description of the program PrismCast is delivering. We surface the channel name when available so probe tools have a
 * human-readable identifier.
 * @param state - The slot's state.
 * @returns The wire-formatted streaminfo string.
 */
function formatStreamInfo(state: TunerState): string {

  if(!state.active || !state.channelName) {

    return "none";
  }

  return state.channelName;
}

/**
 * Formats /tuner&lt;N&gt;/target: the destination URL of any RTP forwarding the tuner is doing. PrismCast does not forward (clients fetch over HTTP), so the value
 * is always "none" regardless of slot state. Real HDHRs use this key on legacy RTP-streaming devices; HTTP-streaming devices like the HDTC-2US that PrismCast
 * emulates also return "none" here.
 * @returns The wire-formatted target string.
 */
function formatTarget(): string {

  return "none";
}

/**
 * Formats /tuner&lt;N&gt;/vchannel: the virtual channel number (display number) the tuner is bound to. Real HDHRs differ between physical and virtual channels for
 * over-the-air ATSC; PrismCast has only virtual numbers from the lineup.
 * @param state - The slot's state.
 * @returns The wire-formatted vchannel string.
 */
function formatVchannel(state: TunerState): string {

  if(!state.active || (state.channelNumber === null)) {

    return "none";
  }

  return String(state.channelNumber);
}

/**
 * Formats /tuner&lt;N&gt;/vstatus: a parallel of /tuner&lt;N&gt;/status but reporting the virtual-channel lock state. We mirror /status because PrismCast has only the
 * one channel concept.
 * @param state - The slot's state.
 * @returns The wire-formatted vstatus string.
 */
function formatVstatus(state: TunerState): string {

  if(!state.active) {

    return "vch=none auth=unknown cci=none cgms=none";
  }

  return "vch=" + formatVchannel(state) + " auth=success cci=none cgms=none";
}
