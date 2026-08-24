/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * display.ts: GPU capability caching for PrismCast.
 */
import type { Nullable } from "../types/index.ts";

/* This module provides a simple cache for the GPU capabilities detected during browser initialization: the hardware-encoding availability for AV1, H.264, and HEVC,
 * plus the WebGL renderer string identifying the GPU. Used for diagnostic logging at startup and consumed by codec selection to gate each hardware capture mode
 * when the corresponding hardware encoder is available.
 *
 * The display itself is not cached here, because nothing derives from it: every page renders at the configured quality preset's dimensions, so the capture surface
 * is the same whatever the display can show.
 *
 * This module is intentionally minimal with no imports from other project modules to avoid circular dependencies. The browser module detects capabilities and calls
 * the setter. Other modules call the getter to access the cached value.
 */

/**
 * GPU capabilities detected from the browser at startup via CDP SystemInfo, WebGL renderer identification, and MediaRecorder probing.
 */
export interface GpuCapabilities {

  // Whether Chrome has hardware AV1 encoding available. Requires a hardware AV1 encoder (Intel Arc, Apple M3+, NVIDIA RTX 40+).
  av1HardwareEncoding: boolean;

  // Whether Chrome has hardware H.264 encoding available. Determined by CDP SystemInfo featureStatus.video_encode or the videoEncoding profile array.
  h264HardwareEncoding: boolean;

  // Whether Chrome has hardware HEVC encoding available. Requires both a hardware HEVC encoder and Chrome 136+.
  hevcHardwareEncoding: boolean;

  // The WebGL renderer string identifying the GPU (e.g., "ANGLE (Apple, Apple M1, OpenGL 4.1)" or "Google SwiftShader"). Used for diagnostic logging only.
  renderer: string;
}

// Cached GPU capabilities. Null before browser initialization completes GPU detection.
let gpuCapabilities: Nullable<GpuCapabilities> = null;

/**
 * Sets the GPU capabilities detected from the browser. Called by browser initialization after probing CDP SystemInfo, WebGL, and MediaRecorder.
 * @param capabilities - The detected GPU capabilities.
 */
export function setGpuCapabilities(capabilities: GpuCapabilities): void {

  gpuCapabilities = capabilities;
}

/**
 * Returns the cached GPU capabilities, or null if GPU detection has not yet completed.
 * @returns The GPU capabilities, or null before detection.
 */
export function getGpuCapabilities(): Nullable<GpuCapabilities> {

  return gpuCapabilities;
}
