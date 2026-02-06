/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * display.ts: Display dimension detection and caching for PrismCast.
 */
import type { Nullable } from "../types/index.js";

/* This module provides a simple cache for display-related dimensions detected during browser initialization. Two sets of values are cached:
 *
 * 1. Maximum supported viewport: The largest viewport that fits on the display after accounting for browser chrome. Used by the preset system to determine if the
 *    configured preset needs to be degraded.
 *
 * 2. Browser chrome dimensions: The height and width of browser UI elements (title bar, toolbar, borders). Used when resizing the browser window to calculate the
 *    total window size needed for a given viewport size.
 *
 * This module is intentionally minimal with no imports from other project modules to avoid circular dependencies. The browser module detects dimensions and calls
 * the setters. Other modules call the getters to access cached values.
 */

// Cached maximum supported viewport dimensions. Null before browser initialization completes display detection.
let maxSupportedViewport: Nullable<{ height: number; width: number }> = null;

// Cached browser chrome dimensions (title bar, toolbar, borders). Null before browser initialization completes display detection.
let browserChrome: Nullable<{ height: number; width: number }> = null;

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
 * Clears the cached display dimensions. Called when browser restarts to force re-detection.
 */
export function clearDisplayCache(): void {

  browserChrome = null;
  maxSupportedViewport = null;
}
