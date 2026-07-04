/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * precaching.ts: Service channel lineup precaching for PrismCast.
 */
import type { DiscoveredChannel, Nullable, ProviderModule } from "../types/index.ts";
import { LOG, extractDomain, formatError, startTimer } from "../utils/index.ts";
import { clearDomainAuthRequirement, getDomainAuthState, markDomainAuth, markDomainAuthRequired } from "../config/health.ts";
import { getCurrentBrowser, isGracefulShutdown, minimizeBrowserWindow, registerManagedPage, unregisterManagedPage } from "./index.ts";
import { getProviderBySlug, getProviderGuideUrls } from "./channelSelection.ts";
import { CONFIG } from "../config/index.ts";
import type { Page } from "puppeteer-core";
import { classifyBlockedPage } from "./blockedPage.ts";
import { isLoginModeActive } from "./login.ts";

/* Precaching discovers channel lineups for selected services at startup so that even the first tune benefits from cached lineup data. Each service is precached
 * sequentially - discovery opens a browser page and navigates to a heavy SPA, so running all services concurrently would stress CPU and GPU on resource-constrained
 * systems. The HTTP server starts immediately; precaching begins in the background after a brief delay.
 *
 * Precaching is triggered from launchBrowser() in browser/index.ts. This covers both initial server startup and browser crash recovery (where all caches are cleared).
 * Each service has its own try/catch - one failure does not stop the rest. The browser reference is obtained per-service via getCurrentBrowser() so that a browser
 * crash between services is handled transparently (the next service gets the relaunched browser).
 *
 * This module also owns the discovery-outcome policy (recordDiscoveryOutcome): the single source of truth for how a completed discovery walk translates into domain
 * auth state, shared by the precache cycle here and the /services/:slug/channels endpoint. The routes layer never calls a health mutator directly - it calls the
 * recorder, which does.
 */

// Delay in milliseconds before precaching begins after browser launch. This gives the browser time to settle after initialization.
const PRECACHE_DELAY = 5000;

// Guard flag preventing overlapping precache cycles. Set to true before the cycle starts, cleared in a finally block.
let precacheInProgress = false;

// Handle for the scheduled precache cycle, tracked so a graceful shutdown can cancel it before it fires. Null when no cycle is pending.
let precacheTimer: Nullable<ReturnType<typeof setTimeout>> = null;

/**
 * Starts the precaching cycle if services are configured. Called from launchBrowser() after the browser is ready. If no services are selected, a shutdown is in
 * progress, or a precache cycle is already in progress, returns immediately. The actual work is scheduled via setTimeout to avoid blocking browser launch.
 */
export function startPrecaching(): void {

  if(CONFIG.channels.precacheServices.length === 0) {

    return;
  }

  // Never schedule a precache during graceful shutdown. launchBrowser() can be reached during teardown; without this guard the scheduled cycle would fire after the
  // browser is closed and relaunch Chrome.
  if(isGracefulShutdown()) {

    return;
  }

  if(precacheInProgress) {

    LOG.debug("precache", "Precache deferred: already in progress.");

    return;
  }

  // Set the guard before scheduling so that a second call during the delay window (e.g., rapid browser crash + relaunch) sees the flag and defers.
  precacheInProgress = true;

  // Schedule the precache cycle after a brief delay to let the browser settle. The handle is tracked so stopPrecaching() can cancel it if shutdown begins during the
  // delay window; the ref is cleared when the timer fires since it is then spent.
  precacheTimer = setTimeout(() => {

    precacheTimer = null;
    void runPrecacheCycle();
  }, PRECACHE_DELAY);
}

/**
 * Cancels a pending precache cycle and clears the in-progress guard. Called during graceful shutdown so a precache scheduled within the startup delay cannot fire
 * after the browser has been closed. Safe to call when no cycle is pending.
 */
export function stopPrecaching(): void {

  if(precacheTimer) {

    clearTimeout(precacheTimer);
    precacheTimer = null;
  }

  precacheInProgress = false;
}

/**
 * Records the domain auth consequences of a completed channel discovery. This is the single source of truth for discovery-outcome policy, consumed by both the
 * precache cycle (via precacheService) and the /services/:slug/channels endpoint - the routes layer never calls a health mutator directly.
 *
 * An empty result classifies the still-open page: a confirmed authentication wall marks the provider's domain needs-sign-in; a consent overlay and the unknown
 * classification change no state (an unexplained empty walk is not evidence of anything). A non-empty result that the provider's validatePrecache accepts (or that
 * needs no validation) marks the domain verified; a non-empty result the validator rejects proves the wall is gone but not that paid access exists, so it clears a
 * standing needs-sign-in entry back to unknown and otherwise changes nothing.
 * @param provider - The provider whose discovery completed.
 * @param channels - The discovered channels (possibly empty).
 * @param page - The still-open discovery page, inspected only when the result is empty.
 * @returns A promise that resolves once any classification and state recording completes.
 */
export async function recordDiscoveryOutcome(provider: ProviderModule, channels: DiscoveredChannel[], page: Page): Promise<void> {

  const domain = extractDomain(provider.guideUrl);

  if(channels.length === 0) {

    const classification = await classifyBlockedPage(page, { indicators: provider.authWallIndicators, requestedUrl: provider.guideUrl });

    switch(classification.kind) {

      case "authWall": {

        markDomainAuthRequired(domain);
        LOG.warn("%s returned no channels because the provider is presenting an authentication wall (%s). Sign in from the channel table's login icon.",
          provider.label, classification.evidence);

        break;
      }

      case "consentOverlay": {

        LOG.warn("%s returned no channels while a consent overlay was present on the page. Open the channel in PrismCast's Chrome from the channel table's " +
          "login icon to dismiss it.", provider.label);

        break;
      }

      case "unknown": {

        LOG.debug("precache", "%s returned no channels and the page did not classify as an auth wall or consent overlay; leaving domain auth unchanged.",
          provider.label);

        break;
      }
    }

    return;
  }

  // A successful discovery with results proves the service is accessible and authenticated. Mark it so the UI shows the green indicator immediately rather than
  // waiting for the first manual tune. When a provider module defines validatePrecache, defer to it - some services (e.g., Sling) return guide data even without
  // authentication, so a non-empty result alone does not prove paid access.
  if(!provider.validatePrecache || provider.validatePrecache(channels)) {

    markDomainAuth(domain);

    return;
  }

  // The validator rejected: the wall is gone (channels came back) but paid access is unproven. Clear a standing needs-sign-in entry back to unknown so the red
  // state never outlives the wall it reported - clearDomainAuthRequirement is a no-op unless the domain is currently flagged, so verified state is never touched.
  clearDomainAuthRequirement(domain);
}

/**
 * Precaches a single service: opens a browser page, navigates to the provider's guide, runs discovery, records the discovery outcome, and cleans the page up. The
 * per-service primitive behind the precache cycle. Errors propagate to the caller, and no service filtering happens here - the cycle loop owns both the filter skip
 * and the per-service error containment.
 * @param provider - The provider to precache.
 * @returns The discovered channels (possibly empty).
 */
export async function precacheService(provider: ProviderModule): Promise<DiscoveredChannel[]> {

  const serviceElapsed = startTimer();

  // Clear the service's cache before discovery to ensure a complete walk, even if a tune partially warmed the cache during the startup delay.
  provider.strategy.clearCache?.();

  const browser = await getCurrentBrowser();
  const page = await browser.newPage();

  // Suppress audio on precache pages. Services like Hulu auto-play a default livestream when their guide loads. Since Chrome's --mute-audio is deliberately
  // disabled (puppeteer-stream needs audio capture for active streams), we intercept play() at the prototype level to mute before any media element can produce
  // audio. evaluateOnNewDocument runs before site JavaScript, so nothing slips through.
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

      await page.goto(provider.guideUrl, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "networkidle2" });
    }

    const channels = await provider.discoverChannels(page);

    LOG.info("Precached %s: %d channels (%ss).", provider.label, channels.length, (serviceElapsed() / 1000).toFixed(1).replace(/\.0$/, ""));

    // Record the domain auth consequences while the page is still open - an empty result classifies the page it walked.
    await recordDiscoveryOutcome(provider, channels, page);

    return channels;
  } finally {

    unregisterManagedPage(page);

    try {

      await page.close();
    } catch {

      // Page may already be closed if the browser disconnected during discovery.
    }

    // Re-minimize the browser window. Opening the temporary discovery page may have restored the window on macOS. Skipped while login mode is active so a
    // discovery finishing mid-login never minimizes the window under the user.
    if(!isLoginModeActive()) {

      await minimizeBrowserWindow();
    }
  }
}

/**
 * Revalidates a domain's authentication after login mode ends: when the domain is currently marked needs-sign-in, re-runs channel discovery for every provider
 * whose guide lives on that domain so fresh success evidence can clear the flag through the discovery-outcome policy. Wired to the login-end observer by app.ts.
 *
 * Never rejects - the observer wiring voids the returned promise, so every failure is logged and absorbed here. Revalidation deliberately ignores both the
 * precacheServices and enabledServices filters: this is clearing-evidence collection for a domain the user just signed in to, not precaching, and the cycle's
 * filter skip is unchanged. While it runs it holds the same single-flight guard the precache cycle holds, so a cycle scheduled mid-revalidation (a browser crash
 * relaunch) defers instead of overlapping.
 * @param url - The login session's URL; its extracted domain selects the providers to revalidate.
 * @returns A promise that resolves when revalidation completes or is skipped. It never rejects.
 */
export async function revalidateDomainAuth(url: string): Promise<void> {

  try {

    const domain = extractDomain(url);

    // Only a standing needs-sign-in entry warrants an automatic discovery - a verified or unknown domain has nothing to clear.
    if(getDomainAuthState(domain)?.status !== "needsLogin") {

      return;
    }

    // The profile-test wizard and sequential multi-provider sign-in flows re-enter login mode immediately after ending it; the final Done fires this observer
    // again with login mode inactive, so deferring here loses nothing.
    if(isLoginModeActive()) {

      LOG.debug("precache", "Skipping post-login revalidation for %s: login mode is active again.", domain);

      return;
    }

    if(isGracefulShutdown()) {

      return;
    }

    // Defer to an in-flight precache cycle rather than overlapping it. Honest limitation: if the flagged provider is not in that cycle's configured service set,
    // the flag persists until the next successful discovery or tune for the domain - an in-flight cycle cannot be retargeted.
    if(precacheInProgress) {

      LOG.info("Deferring the post-login revalidation for %s to the precache cycle already in progress.", domain);

      return;
    }

    // Match every provider whose guide lives on the domain the user just signed in to. The observer's URL may be a DOMAIN_CONFIG loginUrl override rather than a
    // channel URL, but both extract to the same registrable domain the guide URLs use.
    const providers: ProviderModule[] = [];

    for(const [ slug, guideUrl ] of Object.entries(getProviderGuideUrls())) {

      if(extractDomain(guideUrl) === domain) {

        const provider = getProviderBySlug(slug);

        if(provider) {

          providers.push(provider);
        }
      }
    }

    if(providers.length === 0) {

      LOG.debug("precache", "No provider guide matches %s; skipping post-login revalidation.", domain);

      return;
    }

    // Acquire the single-flight guard for the duration of the revalidation, exactly as the cycle does.
    precacheInProgress = true;

    try {

      LOG.info("Re-running channel discovery for %s to verify authentication after sign-in.", domain);

      for(const provider of providers) {

        try {

          // eslint-disable-next-line no-await-in-loop
          await precacheService(provider);
        } catch(error) {

          // A provider still behind its wall commonly times out the guide navigation. Contain the failure per provider and keep going - the flag simply stays set.
          LOG.warn("Post-login revalidation failed for %s: %s.", provider.label, formatError(error));
        }
      }
    } finally {

      precacheInProgress = false;
    }
  } catch(error) {

    LOG.warn("Post-login revalidation for %s failed: %s.", url, formatError(error));
  }
}

/**
 * Executes the sequential precaching cycle. Discovers channel lineups for each configured service, clearing the service's cache first to ensure a complete walk.
 * Services not in the active service filter are silently skipped when the filter is non-empty.
 */
async function runPrecacheCycle(): Promise<void> {

  // Bail if a graceful shutdown began while this cycle was queued. Discovery opens browser pages via getCurrentBrowser(), which would relaunch Chrome after shutdown
  // closed it; this guard makes the cycle a no-op during teardown regardless of how the timer-cancellation race resolves. Reset the in-progress flag since the
  // early return skips the finally block below.
  if(isGracefulShutdown()) {

    precacheInProgress = false;

    return;
  }

  const slugs = CONFIG.channels.precacheServices;
  const enabledFilter = CONFIG.channels.enabledServices;
  const hasFilter = enabledFilter.length > 0;
  const cycleElapsed = startTimer();

  let empty = 0;
  let skipped = 0;
  let succeeded = 0;

  LOG.info("Starting channel lineup precaching for %d service%s.", slugs.length, (slugs.length === 1) ? "" : "s");

  try {

    // Services are precached sequentially - each opens a browser page and navigates to a heavy SPA, so concurrent execution would stress system resources.
    for(const slug of slugs) {

      // Stop opening new discovery pages once a graceful shutdown begins mid-cycle. The entry guard above only covers a cycle that has not started; without this,
      // a cycle already in its loop when shutdown closes the browser would call getCurrentBrowser() below and relaunch Chrome. Break rather than continue so no
      // further service is processed; the in-flight service (if any) finishes and closes its own page via its finally.
      if(isGracefulShutdown()) {

        break;
      }

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

      try {

        // eslint-disable-next-line no-await-in-loop
        const channels = await precacheService(provider);

        // An empty walk cached nothing, so it is counted honestly as empty rather than folded into the success count.
        if(channels.length > 0) {

          succeeded++;
        } else {

          empty++;
        }
      } catch(error) {

        LOG.warn("Failed to precache %s: %s.", provider.label, formatError(error));
      }
    }

    const elapsed = (cycleElapsed() / 1000).toFixed(1).replace(/\.0$/, "");
    const emptySuffix = (empty > 0) ? ", " + String(empty) + " returned no channels" : "";
    const skippedSuffix = (skipped > 0) ? ", " + String(skipped) + " skipped (filtered)" : "";

    LOG.info("Channel lineup precaching complete: %d service%s cached%s%s in %ss.", succeeded, (succeeded === 1) ? "" : "s", emptySuffix, skippedSuffix, elapsed);
  } finally {

    precacheInProgress = false;
  }
}
