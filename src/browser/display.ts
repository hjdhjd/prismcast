/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * display.ts: Display dimension detection and caching for PrismCast.
 */
import type { Nullable } from "../types/index.ts";

/* This module provides a simple cache for display and GPU capabilities detected during browser initialization. Three sets of values are cached:
 *
 * 1. Maximum supported viewport: The largest viewport that fits on the display after accounting for browser chrome. Used by the preset system to determine if the
 *    configured preset needs to be degraded.
 *
 * 2. Browser chrome dimensions: The height and width of browser UI elements (title bar, toolbar, borders). Used when resizing the browser window to calculate the
 *    total window size needed for a given viewport size.
 *
 * 3. GPU capabilities: Whether Chrome has hardware GPU acceleration (vs software rendering) and HEVC encoding support. Used for diagnostic logging at startup and
 *    to gate HEVC capture mode when hardware encoding is available.
 *
 * This module is intentionally minimal with no imports from other project modules to avoid circular dependencies. The browser module detects dimensions and calls
 * the setters. Other modules call the getters to access cached values.
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

// Cached maximum supported viewport dimensions. Null before browser initialization completes display detection.
let maxSupportedViewport: Nullable<{ height: number; width: number }> = null;

// Cached browser chrome dimensions (title bar, toolbar, borders). Null before browser initialization completes display detection.
let browserChrome: Nullable<{ height: number; width: number }> = null;

// Cached GPU capabilities. Null before browser initialization completes GPU detection.
let gpuCapabilities: Nullable<GpuCapabilities> = null;

/**
 * Sets the maximum supported viewport dimensions. Called by browser initialization after detecting the display size and accounting for browser chrome.
 * @param width - Maximum viewport width in pixels.
 * @param height - Maximum viewport height in pixels.
 */
export function setMaxSupportedViewport(width: number, height: number): void {

  maxSupportedViewport = { height, width };
}

/**
 * Returns the maximum supported viewport dimensions, or null if display detection has not yet completed. Callers should fall back to the configured preset when null
 * is returned.
 * @returns The maximum supported viewport dimensions, or null before detection.
 */
export function getMaxSupportedViewport(): Nullable<{ height: number; width: number }> {

  return maxSupportedViewport;
}

/**
 * Sets the browser chrome dimensions. Called by browser initialization after measuring the difference between outer and inner window dimensions.
 * @param width - Chrome width in pixels (typically 0 or small for window borders).
 * @param height - Chrome height in pixels (title bar + toolbar).
 */
export function setBrowserChrome(width: number, height: number): void {

  browserChrome = { height, width };
}

/**
 * Returns the cached browser chrome dimensions, or null if display detection has not yet completed.
 * @returns The browser chrome dimensions, or null before detection.
 */
export function getBrowserChrome(): Nullable<{ height: number; width: number }> {

  return browserChrome;
}

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
