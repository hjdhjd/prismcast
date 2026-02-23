/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * providers.ts: Provider channel discovery route for PrismCast.
 */
import type { Express, Request, Response } from "express";
import { getCurrentBrowser, minimizeBrowserWindow, registerManagedPage, unregisterManagedPage } from "../browser/index.js";
import { CONFIG } from "../config/index.js";
import { LOG } from "../utils/index.js";
import { getProviderBySlug } from "../browser/channelSelection.js";

/* The providers endpoint exposes channel discovery for each registered provider. A GET request to /providers/:slug/channels creates a temporary browser page,
 * navigates to the provider's guide, runs the provider's discoverChannels implementation, and returns a sorted JSON array of discovered channels. The temporary
 * page is always closed in a finally block to prevent resource leaks.
 */

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
    // subsequent tune resolves from the freshly populated cache as normal.
    const refresh = req.query.refresh === "true";

    if(refresh) {

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

    let page = null;

    try {

      const browser = await getCurrentBrowser();

      page = await browser.newPage();
      registerManagedPage(page);

      // Navigate to the provider's guide URL unless the provider handles its own navigation (e.g., Hulu and Sling set up response interception before navigating).
      // We use networkidle2 rather than load because SPA-based providers (e.g., Hulu) have heavy async initialization that can prevent the load event from firing
      // reliably. Network idle ensures all initial API data has arrived before the discovery function reads the DOM.
      if(!provider.handlesOwnNavigation) {

        await page.goto(provider.guideUrl, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "networkidle2" });
      }

      const channels = await provider.discoverChannels(page);

      // Sort by name for consistent output. Discovery functions sort at cache time, but fresh (uncached) results from the first call may not be sorted yet.
      channels.sort((a, b) => a.name.localeCompare(b.name));

      res.json(channels);
    } catch(error) {

      const message = (error instanceof Error) ? error.message : String(error);

      LOG.warn("Channel discovery failed for %s: %s.", provider.label, message);

      res.status(500).json({ error: "Channel discovery failed: " + message + "." });
    } finally {

      if(page) {

        unregisterManagedPage(page);

        try {

          await page.close();
        } catch {

          // Page may already be closed if the browser disconnected.
        }

        // Re-minimize the browser window. Opening the temporary discovery page may have restored the window on macOS, and we want it minimized to reduce GPU usage.
        await minimizeBrowserWindow();
      }
    }
  });
}
