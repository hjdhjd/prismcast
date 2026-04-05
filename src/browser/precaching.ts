/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * precaching.ts: Service channel lineup precaching for PrismCast.
 */
import { LOG, extractDomain, formatError, startTimer } from "../utils/index.js";
import { getCurrentBrowser, minimizeBrowserWindow, registerManagedPage, unregisterManagedPage } from "./index.js";
import { CONFIG } from "../config/index.js";
import { getProviderBySlug } from "./channelSelection.js";
import { markDomainAuth } from "../config/health.js";

/* Precaching discovers channel lineups for selected services at startup so that even the first tune benefits from cached lineup data. Each service is precached
 * sequentially — discovery opens a browser page and navigates to a heavy SPA, so running all services concurrently would stress CPU and GPU on resource-constrained
 * systems. The HTTP server starts immediately; precaching begins in the background after a brief delay.
 *
 * Precaching is triggered from launchBrowser() in browser/index.ts. This covers both initial server startup and browser crash recovery (where all caches are cleared).
 * Each service has its own try/catch — one failure does not stop the rest. The browser reference is obtained per-service via getCurrentBrowser() so that a browser
 * crash between services is handled transparently (the next service gets the relaunched browser).
 */

// Delay in milliseconds before precaching begins after browser launch. This gives the browser time to settle after initialization.
const PRECACHE_DELAY = 5000;

// Guard flag preventing overlapping precache cycles. Set to true before the cycle starts, cleared in a finally block.
let precacheInProgress = false;

/**
 * Starts the precaching cycle if services are configured. Called from launchBrowser() after the browser is ready. If no services are selected, or a precache cycle
 * is already in progress, returns immediately. The actual work is scheduled via setTimeout to avoid blocking browser launch.
 */
export function startPrecaching(): void {

  if(CONFIG.channels.precacheServices.length === 0) {

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
 * Executes the sequential precaching cycle. Discovers channel lineups for each configured service, clearing the service's cache first to ensure a complete walk.
 * Services not in the active service filter are silently skipped when the filter is non-empty.
 */
async function runPrecacheCycle(): Promise<void> {

  const slugs = CONFIG.channels.precacheServices;
  const enabledFilter = CONFIG.channels.enabledServices;
  const hasFilter = enabledFilter.length > 0;
  const cycleElapsed = startTimer();

  let skipped = 0;
  let succeeded = 0;

  LOG.info("Starting channel lineup precaching for %d service%s.", slugs.length, (slugs.length === 1) ? "" : "s");

  try {

    // Services are precached sequentially — each opens a browser page and navigates to a heavy SPA, so concurrent execution would stress system resources.
    for(const slug of slugs) {

      const provider = getProviderBySlug(slug);

      if(!provider) {

        continue;
      }

      // Skip services not in the active service filter. Their stored config is preserved for when the filter changes back.
      if(hasFilter && !enabledFilter.includes(slug)) {

        LOG.debug("precache", "Skipping precache for %s: not in active service filter.", provider.label);
        skipped++;

        continue;
      }

      const serviceElapsed = startTimer();

      try {

        // Clear the service's cache before discovery to ensure a complete walk, even if a tune partially warmed the cache during the startup delay.
        provider.strategy.clearCache?.();

        // eslint-disable-next-line no-await-in-loop
        const browser = await getCurrentBrowser();

        // eslint-disable-next-line no-await-in-loop
        const page = await browser.newPage();

        // Suppress audio on precache pages. Services like Hulu auto-play a default livestream when their guide loads. Since Chrome's --mute-audio is deliberately
        // disabled (puppeteer-stream needs audio capture for active streams), we intercept play() at the prototype level to mute before any media element can produce
        // audio. evaluateOnNewDocument runs before site JavaScript, so nothing slips through.
        // eslint-disable-next-line no-await-in-loop
        await page.evaluateOnNewDocument((): void => {

          // eslint-disable-next-line @typescript-eslint/unbound-method
          const originalPlay = HTMLMediaElement.prototype.play;

          HTMLMediaElement.prototype.play = async function(this: HTMLMediaElement): Promise<void> {

            this.muted = true;

            return originalPlay.call(this);
          };
        });

        registerManagedPage(page);

        try {

          // Navigate to the service's guide URL unless the provider module handles its own navigation (e.g., sets up response interception before navigating).
          if(!provider.handlesOwnNavigation) {

            // eslint-disable-next-line no-await-in-loop
            await page.goto(provider.guideUrl, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "networkidle2" });
          }

          // eslint-disable-next-line no-await-in-loop
          const channels = await provider.discoverChannels(page);

          LOG.info("Precached %s: %d channels (%ss).", provider.label, channels.length, (serviceElapsed() / 1000).toFixed(1).replace(/\.0$/, ""));

          // A successful discovery with results proves the service is accessible and authenticated. Mark it so the UI shows the green indicator immediately
          // rather than waiting for the first manual tune. When a provider module defines validatePrecache, defer to it — some services (e.g., Sling) return guide data
          // even without authentication, so a non-empty result alone does not prove paid access.
          if((channels.length > 0) && (!provider.validatePrecache || provider.validatePrecache(channels))) {

            markDomainAuth(extractDomain(provider.guideUrl));
          }

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

    const elapsed = (cycleElapsed() / 1000).toFixed(1).replace(/\.0$/, "");
    const skippedSuffix = skipped > 0 ? ", " + String(skipped) + " skipped (filtered)" : "";

    LOG.info("Channel lineup precaching complete: %d service%s cached%s in %ss.", succeeded, (succeeded === 1) ? "" : "s", skippedSuffix, elapsed);
  } finally {

    precacheInProgress = false;
  }
}
