/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * providers.ts: Provider channel discovery route for PrismCast.
 */
import type { DiscoveredChannel, ProviderModule } from "../types/index.js";
import type { Express, Request, Response } from "express";
import { getCurrentBrowser, minimizeBrowserWindow, registerManagedPage, unregisterManagedPage } from "../browser/index.js";
import { CONFIG } from "../config/index.js";
import { LOG } from "../utils/index.js";
import type { Page } from "puppeteer-core";
import { getProviderBySlug } from "../browser/channelSelection.js";

/* The providers endpoint exposes channel discovery for each registered provider. A GET request to /providers/:slug/channels creates a temporary browser page,
 * navigates to the provider's guide, runs the provider's discoverChannels implementation, and returns a sorted JSON array of discovered channels. The temporary
 * page is always closed in a finally block to prevent resource leaks. Concurrent requests for the same provider are coalesced — only one discovery walk runs at a
 * time, and subsequent requests piggyback on the in-flight result. A refresh=true request aborts any in-flight discovery and starts fresh.
 */

// Sentinel error used to identify aborted discoveries in the retry loop. Distinguishes abort rejections from genuine discovery failures so the loop only retries
// when the failure was caused by a refresh=true cancellation, not an unrelated error.
class DiscoveryAbortError extends Error {

  constructor() {

    super("Discovery aborted.");
    this.name = "DiscoveryAbortError";
  }
}

// In-flight discovery state. Tracks the running discovery promise and its associated abort controller for each provider slug. When a discovery is in flight,
// subsequent requests await the existing promise instead of spawning redundant browser pages. The abort controller's signal is used to close the page when a
// refresh=true request needs to cancel an in-flight non-refresh discovery.
interface InflightEntry {

  controller: AbortController;
  promise: Promise<DiscoveredChannel[]>;
}

const inflight = new Map<string, InflightEntry>();

/**
 * Logs a discovery failure and sends a 500 error response.
 * @param res - The Express response object.
 * @param label - The provider's display label for log messages.
 * @param error - The error that caused the failure.
 */
function sendDiscoveryError(res: Response, label: string, error: unknown): void {

  const message = (error instanceof Error) ? error.message : String(error);

  LOG.warn("Channel discovery failed for %s: %s.", label, message);
  res.status(500).json({ error: "Channel discovery failed: " + message + "." });
}

/**
 * Runs provider channel discovery in a temporary browser page. Opens a new page, navigates to the provider's guide URL, runs the discovery function, and returns
 * the sorted results. The page is always closed in a finally block. If the abort signal fires (from a refresh=true request), the page is closed mid-discovery,
 * causing Puppeteer operations to throw and the promise to reject with a DiscoveryAbortError.
 * @param provider - The provider module to discover channels for.
 * @param signal - Abort signal for cancellation by refresh requests.
 * @returns Sorted array of discovered channels.
 */
async function runDiscovery(provider: ProviderModule, signal: AbortSignal): Promise<DiscoveredChannel[]> {

  let page: Page | null = null;

  // Close the page when the abort signal fires. This causes any in-progress Puppeteer operations to throw, propagating the cancellation through the discovery
  // function without requiring explicit signal checking in each provider's implementation. The finally block also closes the page unconditionally — the
  // redundant close is idempotent (caught by try/catch).
  const onAbort = (): void => {

    if(page) {

      void page.close().catch(() => {

        // Page may already be closed.
      });
    }
  };

  signal.addEventListener("abort", onAbort, { once: true });

  try {

    const browser = await getCurrentBrowser();

    page = await browser.newPage();
    registerManagedPage(page);

    // Check if we were aborted between entering runDiscovery and creating the page. The onAbort handler would have fired when page was still null, so the page
    // we just opened would never be interrupted. Bail out now and let the finally block close it.
    if(signal.aborted) {

      throw new DiscoveryAbortError();
    }

    // Navigate to the provider's guide URL unless the provider handles its own navigation (e.g., Hulu and Sling set up response interception before navigating).
    // We use networkidle2 rather than load because SPA-based providers (e.g., Hulu) have heavy async initialization that can prevent the load event from firing
    // reliably. Network idle ensures all initial API data has arrived before the discovery function reads the DOM.
    if(!provider.handlesOwnNavigation) {

      await page.goto(provider.guideUrl, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "networkidle2" });
    }

    const channels = await provider.discoverChannels(page);

    // Sort by name for consistent output. Discovery functions sort at cache time, but fresh (uncached) results from the first call may not be sorted yet.
    channels.sort((a, b) => a.name.localeCompare(b.name));

    return channels;
  } catch(error) {

    // Wrap Puppeteer errors caused by page closure during abort into a DiscoveryAbortError so the retry loop can distinguish aborts from genuine failures.
    // signal.aborted is the single source of truth for whether an abort occurred.
    if(signal.aborted) {

      throw new DiscoveryAbortError();
    }

    throw error;
  } finally {

    signal.removeEventListener("abort", onAbort);

    if(page) {

      unregisterManagedPage(page);

      try {

        await page.close();
      } catch {

        // Page may already be closed if the browser disconnected or the abort handler already closed it.
      }

      // Re-minimize the browser window. Opening the temporary discovery page may have restored the window on macOS, and we want it minimized to reduce GPU usage.
      await minimizeBrowserWindow();
    }
  }
}

/**
 * Creates the provider channel discovery endpoint.
 * @param app - The Express application.
 */
export function setupProvidersEndpoint(app: Express): void {

  app.get("/providers/:slug/channels", async (req: Request, res: Response): Promise<void> => {

    const slug = req.params.slug as string;
    const provider = getProviderBySlug(slug);

    if(!provider) {

      res.status(404).json({ error: "Unknown provider: " + slug + "." });

      return;
    }

    // When refresh=true is requested, clear the provider's caches (unified channel cache, row caches, fully-enumerated flags, etc.) so the discovery walk runs
    // against fresh data. This also resets warm tuning state (watch URLs, GUIDs), but the discovery walk repopulates the unified cache before returning — any
    // subsequent tune resolves from the freshly populated cache as normal. If a discovery is already in flight, abort it first — clearing the cache while a
    // discovery is progressively populating it would corrupt its state.
    const refresh = req.query.refresh === "true";

    if(refresh) {

      const existing = inflight.get(slug);

      if(existing) {

        existing.controller.abort();
        inflight.delete(slug);
      }

      provider.strategy.clearCache?.();
    }

    // Check for cached discovery results before creating a browser page. When a prior tune or discovery call has already enumerated the provider's lineup, the
    // cache is warm and we can return immediately without any browser interaction. Skipped when refresh=true since we just cleared the caches above.
    if(!refresh) {

      const cached = provider.getCachedChannels();

      if(cached) {

        res.json(cached);

        return;
      }
    }

    // Coalesce concurrent requests. If a discovery is already in flight for this provider, piggyback on the existing promise instead of spawning a redundant
    // browser page. If the in-flight discovery was aborted (by a refresh=true request that arrived after we checked above), the promise rejects with a
    // DiscoveryAbortError and we retry against whatever new entry replaced it in the map.
    let entry = inflight.get(slug);

    if(!entry) {

      const controller = new AbortController();
      const promise = runDiscovery(provider, controller.signal).finally(() => {

        // Only remove our own entry. A refresh=true request may have already replaced it with a new one.
        if(inflight.get(slug)?.controller === controller) {

          inflight.delete(slug);
        }
      });

      entry = { controller, promise };
      inflight.set(slug, entry);
    }

    // Await the in-flight discovery. If it was aborted by a refresh=true request, a new discovery should now be in the map — retry against that one. The caller
    // doesn't know or care about the abort; they just want channels. Only DiscoveryAbortError triggers a retry; genuine failures are reported immediately.
    for(;;) {

      try {

        // eslint-disable-next-line no-await-in-loop -- Intentional: each iteration awaits a different promise (the replacement after an abort).
        const channels = await entry.promise;

        res.json(channels);

        return;
      } catch(error) {

        // Only retry if this was an abort and a new discovery has replaced the aborted one.
        const retryEntry = inflight.get(slug);

        if((error instanceof DiscoveryAbortError) && retryEntry && (retryEntry !== entry)) {

          entry = retryEntry;

          continue;
        }

        // Genuine failure or no replacement entry after abort.
        sendDiscoveryError(res, provider.label, error);

        return;
      }
    }
  });
}
