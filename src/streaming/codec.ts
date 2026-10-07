/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * codec.ts: Capture codec selection for PrismCast.
 */
import { CAPTURE_BASELINE_CODEC } from "../types/index.ts";
import { CONFIG } from "../config/index.ts";
import type { CaptureCodec } from "../types/index.ts";
import type { GpuCapabilities } from "../browser/display.ts";
import { getGpuCapabilities } from "../browser/display.ts";

// Re-export the CaptureCodec type so existing consumers can import from either module.
export type { CaptureCodec } from "../types/index.ts";

/* This module is the single source of truth for capture codec behavior. The capture's codec decision, getEffectiveCaptureCodec(), combines the user's codec
 * allowlist (CONFIG.streaming.captureCodecs), the GPU's hardware encoding capabilities, and the priority order (prefer higher-quality codecs when available).
 * Whether this GPU can capture a given codec is isCaptureCodecSupported()'s answer. The effective-codec walk and the settings form's codec items read this answer,
 * and the acceleration answer reads the effective codec's flag in CAPTURE_CODEC_CAPABILITIES. Every component that needs the decision or the per-codec answer -
 * MIME type selection, preroll generation, status display, the settings form - asks this module rather than reading the GPU capabilities itself. Codec identity
 * (RECOGNIZED_CODECS, CaptureCodec and CAPTURE_BASELINE_CODEC) lives in types/streaming.ts; this module provides the runtime logic.
 */

// Matroska MIME types keyed by capture codec. Matroska is used over WebM because it supports a broader range of codecs (including HEVC and AV1), allowing codec
// upgrades without changing the container format. FFmpeg's demuxer handles both Matroska and WebM identically. All variants use Opus audio which FFmpeg transcodes
// to AAC.
const CAPTURE_MIME_TYPES: Record<CaptureCodec, string> = {

  h264: "video/x-matroska;codecs=h264,opus",
  hevc: "video/x-matroska;codecs=hvc1.1.6.L93.B0,opus"
};

// Each recognized codec's GPU hardware encoding flag. The table is keyed by the codec union, so the compiler holds every recognized codec, the baseline among them,
// to the flag that answers whether this GPU encodes it in hardware.
const CAPTURE_CODEC_CAPABILITIES: Readonly<Record<CaptureCodec, Exclude<keyof GpuCapabilities, "renderer">>> = {

  h264: "h264HardwareEncoding",
  hevc: "hevcHardwareEncoding"
};

// Codec priority order. Higher-quality codecs are tried first. The baseline needs no GPU capability (Chrome's MediaRecorder encodes it everywhere), so the list's
// type excludes it and it is the walk's fallback rather than an entry.
const CODEC_PRIORITY: readonly Exclude<CaptureCodec, typeof CAPTURE_BASELINE_CODEC>[] = ["hevc"];

/**
 * Returns the effective capture codec based on the user's allowlist and GPU hardware capabilities. Walks codecs in priority order (highest quality first), returning
 * the first that the user allows and this GPU can capture, as isCaptureCodecSupported() answers it. Falls back to CAPTURE_BASELINE_CODEC.
 *
 * This is the single source of truth for the capture's codec decision, and isCaptureCodecSupported() is the one answer to whether this GPU can capture a codec. A
 * caller that needs the decision or the per-codec answer asks this module rather than reading the GPU capabilities itself.
 * @returns The codec to use for capture.
 */
export function getEffectiveCaptureCodec(): CaptureCodec {

  const allowedCodecs = CONFIG.streaming.captureCodecs;

  for(const codec of CODEC_PRIORITY) {

    if(allowedCodecs.includes(codec) && isCaptureCodecSupported(codec)) {

      return codec;
    }
  }

  return CAPTURE_BASELINE_CODEC;
}

/**
 * Answers whether this GPU can capture a codec. The baseline needs no GPU encoding, so it is always supported; any other codec is supported when CODEC_PRIORITY
 * holds it and the GPU reports the hardware encoding flag CAPTURE_CODEC_CAPABILITIES keys to it, and a codec the list does not hold is not. Capabilities not yet
 * detected read as unsupported, because a GPU whose encoders are unknown must never be offered a codec it may not encode. The effective-codec walk and the settings
 * form's codec items read this answer, and the acceleration answer reads the effective codec's flag in CAPTURE_CODEC_CAPABILITIES.
 * @param codec - The codec to test.
 * @returns True when this GPU can capture the codec.
 */
export function isCaptureCodecSupported(codec: CaptureCodec): boolean {

  if(codec === CAPTURE_BASELINE_CODEC) {

    return true;
  }

  // A codec the priority list does not hold is never chosen by the walk, so no GPU flag can make it supported.
  return CODEC_PRIORITY.includes(codec) && (getGpuCapabilities()?.[CAPTURE_CODEC_CAPABILITIES[codec]] === true);
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

  // A codec other than the baseline is chosen only when its flag is set, and the baseline is chosen whether or not its own flag is set, so one read of the effective
  // codec's flag answers every codec.
  return getGpuCapabilities()?.[CAPTURE_CODEC_CAPABILITIES[getEffectiveCaptureCodec()]] === true;
}
