/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * precaching.ts: Provider channel lineup precaching for PrismCast.
 */
import { LOG, formatError, startTimer } from "../utils/index.js";
import { getCurrentBrowser, minimizeBrowserWindow, registerManagedPage, unregisterManagedPage } from "./index.js";
import { CONFIG } from "../config/index.js";
import { getProviderBySlug } from "./channelSelection.js";

/* Precaching discovers channel lineups for selected providers at startup so that even the first tune benefits from cached lineup data. Each provider is precached
 * sequentially — discovery opens a browser page and navigates to a heavy SPA, so running all providers concurrently would stress CPU and GPU on resource-constrained
 * systems. The HTTP server starts immediately; precaching begins in the background after a brief delay.
 *
 * Precaching is triggered from launchBrowser() in browser/index.ts. This covers both initial server startup and browser crash recovery (where all caches are cleared).
 * Each provider has its own try/catch — one failure does not stop the rest. The browser reference is obtained per-provider via getCurrentBrowser() so that a browser
 * crash between providers is handled transparently (the next provider gets the relaunched browser).
 */

// Delay in milliseconds before precaching begins after browser launch. This gives the browser time to settle after initialization.
const PRECACHE_DELAY = 5000;

// Guard flag preventing overlapping precache cycles. Set to true before the cycle starts, cleared in a finally block.
let precacheInProgress = false;

/**
 * Starts the precaching cycle if providers are configured. Called from launchBrowser() after the browser is ready. If no providers are selected, or a precache cycle
 * is already in progress, returns immediately. The actual work is scheduled via setTimeout to avoid blocking browser launch.
 */
export function startPrecaching(): void {

  if(CONFIG.channels.precacheProviders.length === 0) {

    return;
  }

  if(precacheInProgress) {

    LOG.debug("precache", "Precache deferred: already in progress.");

    return;
  }

  // Set the guard before scheduling so that a second call during the delay window (e.g., rapid browser crash + relaunch) sees the flag and defers.
  precacheInProgress = true;

  // Schedule the precache cycle after a brief delay to let the browser settle.
  setTimeout(() => void runPrecacheCycle(), PRECACHE_DELAY);
}

/**
 * Executes the sequential precaching cycle. Discovers channel lineups for each configured provider, clearing the provider's cache first to ensure a complete walk.
 * Providers not in the active provider filter are silently skipped when the filter is non-empty.
 */
async function runPrecacheCycle(): Promise<void> {

  const slugs = CONFIG.channels.precacheProviders;
  const enabledFilter = CONFIG.channels.enabledProviders;
  const hasFilter = enabledFilter.length > 0;
  const cycleElapsed = startTimer();

  let succeeded = 0;

  LOG.info("Starting channel lineup precaching for %d provider%s.", slugs.length, (slugs.length === 1) ? "" : "s");

  try {

    // Providers are precached sequentially — each opens a browser page and navigates to a heavy SPA, so concurrent execution would stress system resources.
    for(const slug of slugs) {

      const provider = getProviderBySlug(slug);

      if(!provider) {

        continue;
      }

      // Skip providers not in the active provider filter. Their stored config is preserved for when the filter changes back.
      if(hasFilter && !enabledFilter.includes(slug)) {

        LOG.debug("precache", "Skipping precache for %s: not in active provider filter.", provider.label);

        continue;
      }

      const providerElapsed = startTimer();

      try {

        // Clear the provider's cache before discovery to ensure a complete walk, even if a tune partially warmed the cache during the startup delay.
        provider.strategy.clearCache?.();

        // eslint-disable-next-line no-await-in-loop
        const browser = await getCurrentBrowser();

        // eslint-disable-next-line no-await-in-loop
        const page = await browser.newPage();

        registerManagedPage(page);

        try {

          // Navigate to the provider's guide URL unless the provider handles its own navigation (e.g., sets up response interception before navigating).
          if(!provider.handlesOwnNavigation) {

            // eslint-disable-next-line no-await-in-loop
            await page.goto(provider.guideUrl, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "networkidle2" });
          }

          // eslint-disable-next-line no-await-in-loop
          const channels = await provider.discoverChannels(page);

          LOG.info("Precached %s: %d channels (%ss).", provider.label, channels.length, (providerElapsed() / 1000).toFixed(1).replace(/\.0$/, ""));

          succeeded++;
        } finally {

          unregisterManagedPage(page);

          try {

            // eslint-disable-next-line no-await-in-loop
            await page.close();
          } catch {

            // Page may already be closed if the browser disconnected during discovery.
          }

          // Re-minimize the browser window. Opening the temporary discovery page may have restored the window on macOS.
          // eslint-disable-next-line no-await-in-loop
          await minimizeBrowserWindow();
        }
      } catch(error) {

        LOG.warn("Failed to precache %s: %s.", provider.label, formatError(error));
      }
    }

    LOG.info("Channel lineup precaching complete: %d of %d provider%s (%ss total).", succeeded, slugs.length, (slugs.length === 1) ? "" : "s",
      (cycleElapsed() / 1000).toFixed(1).replace(/\.0$/, ""));
  } finally {

    precacheInProgress = false;
  }
}
