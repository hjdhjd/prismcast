/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * codec.ts: Capture codec selection for PrismCast.
 */
import { CONFIG } from "../config/index.js";
import type { CaptureCodec } from "../types/index.js";
import { getGpuCapabilities } from "../browser/display.js";

// Re-export the CaptureCodec type so existing consumers can import from either module.
export type { CaptureCodec } from "../types/index.js";

/* This module is the single source of truth for capture codec behavior. Every component that needs to know which video codec to use for capture - MIME type
 * selection, preroll generation, status display - calls getEffectiveCaptureCodec() rather than checking GPU capabilities directly. The decision encapsulates three
 * inputs: the user's codec allowlist (CONFIG.streaming.captureCodecs), the GPU's hardware encoding capabilities, and the priority order (prefer higher-quality
 * codecs when available). Codec identity (RECOGNIZED_CODECS and CaptureCodec) lives in types/streaming.ts; this module provides the runtime logic.
 */

// Matroska MIME types keyed by capture codec. Matroska is used over WebM because it supports a broader range of codecs (including HEVC and AV1), allowing codec
// upgrades without changing the container format. FFmpeg's demuxer handles both Matroska and WebM identically. All variants use Opus audio which FFmpeg transcodes
// to AAC.
const CAPTURE_MIME_TYPES: Record<CaptureCodec, string> = {

  h264: "video/x-matroska;codecs=h264,opus",
  hevc: "video/x-matroska;codecs=hvc1.1.6.L93.B0,opus"
};

// Codec priority order. Higher-quality codecs are tried first. Each entry pairs a codec with its GPU capability check. H.264 is the implicit fallback and is not
// listed here since it always works (Chrome's MediaRecorder baseline).
const CODEC_PRIORITY: { capability: "av1HardwareEncoding" | "hevcHardwareEncoding"; codec: CaptureCodec }[] = [

  { capability: "hevcHardwareEncoding", codec: "hevc" }
  // Future: { capability: "av1HardwareEncoding", codec: "av1" }
];

/**
 * Returns the effective capture codec based on the user's allowlist and GPU hardware capabilities. Walks codecs in priority order (highest quality first), returning
 * the first that's both allowed by the user AND supported by the GPU. Falls back to H.264 as the universal baseline.
 *
 * This is the single source of truth for all capture codec decisions in the codebase. Do not check GPU codec capabilities directly - call this function instead.
 * @returns The codec to use for capture.
 */
export function getEffectiveCaptureCodec(): CaptureCodec {

  const allowedCodecs = CONFIG.streaming.captureCodecs;
  const gpuCaps = getGpuCapabilities();

  for(const { capability, codec } of CODEC_PRIORITY) {

    if(allowedCodecs.includes(codec) && gpuCaps?.[capability]) {

      return codec;
    }
  }

  return "h264";
}

/**
 * Returns the Matroska MIME type string for the effective capture codec. Used by the capture pipeline to configure Chrome's MediaRecorder.
 * @returns The MIME type string for the current capture codec.
 */
export function getCaptureMimeType(): string {

  return CAPTURE_MIME_TYPES[getEffectiveCaptureCodec()];
}

/**
 * Returns whether the effective capture codec is hardware-accelerated on the current GPU. Used for status display and stream registry metadata.
 * @returns True if the effective codec has hardware encoding support.
 */
export function isCaptureHardwareAccelerated(): boolean {

  const codec = getEffectiveCaptureCodec();
  const gpuCaps = getGpuCapabilities();

  if(codec === "h264") {

    return gpuCaps?.h264HardwareEncoding === true;
  }

  // All non-H.264 codecs in CODEC_PRIORITY are only selected when their hardware capability is true, so this is guaranteed. The explicit check keeps the function
  // self-contained rather than relying on getEffectiveCaptureCodec's selection logic.
  for(const { capability, codec: priorityCodec } of CODEC_PRIORITY) {

    if(priorityCodec === codec) {

      return gpuCaps?.[capability] === true;
    }
  }

  return false;
}
