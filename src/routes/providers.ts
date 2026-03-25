/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * providers.ts: Provider channel discovery route for PrismCast.
 */
import type { DiscoveredChannel, ProviderModule } from "../types/index.js";
import type { Express, Request, Response } from "express";
import { getChannelListing, getChannelLogo, isPredefinedChannel } from "../config/userChannels.js";
import { getChannelProviderLabel, getProviderGroup, getProviderTagForChannel, getResolvedChannel, isProviderTagEnabled,
  resolveProviderKey } from "../config/providers.js";
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
 * Lineup state for a discovered channel that matches an existing channel in the user's lineup. Attached to each discovered channel by the annotation step
 * so the client can render three-state checkboxes without any matching logic of its own.
 */
interface LineupState {

  // The canonical channel key (e.g., "animal" for Animal Planet). Used by the client to send back to the server for provider switch/remove operations.
  canonicalKey: string;

  // Human-readable label for the currently active provider (e.g., "Hulu", "Spectrum"). Displayed in the browse modal's state label.
  currentProvider: string;

  // Provider tag for the currently active provider (e.g., "hulu", "spectrum"). Compared against the browsed provider's slug to determine checked vs
  // indeterminate state.
  currentTag: string;

  // The canonical channel's display name from the predefined or user-defined definition (e.g., "Disney", "Disney (Pacific)", "A&E"). The client renders
  // this instead of the raw discovery name when present, ensuring the browse modal shows the same names users see in the channels table.
  displayName: string;

  // Whether the channel is currently enabled in the lineup.
  enabled: boolean;

  // Whether the channel has at least one other enabled provider variant besides the browsed provider. Used by the client to determine the visual state
  // when unchecking a "current" channel: indeterminate if alternatives exist (channel persists), empty if not (channel will be disabled).
  hasAlternatives: boolean;

  // Channel logo URL from the DVR logo cache. Used by the client to render logos alongside channel names via channelDisplayHtml.
  logoUrl?: string;

  // The stationId (Gracenote ID) for this canonical channel. Used to disambiguate when two canonicals (East and Pacific) share the same channelSelector
  // for a provider — the discovery entry's stationId is matched against this value to assign the correct canonical.
  stationId?: string;

  // Whether the channel is predefined or user-defined.
  source: string;
}

/**
 * Annotated discovery result that extends DiscoveredChannel with optional lineup state. When a discovered channel matches an existing channel in the user's
 * lineup (by canonical key), the lineup field provides the current provider state. When absent, the channel is new (not in the lineup).
 */
interface AnnotatedChannel extends DiscoveredChannel {

  lineup?: LineupState;
}

/**
 * Annotates discovered channels with lineup state by matching each channel's channelSelector against the provider variants for the browsed provider. This
 * uses the existing provider group system to find which canonical channel each discovered channel corresponds to, avoiding fragile name-to-key normalization
 * (predefined keys are hand-crafted and may not match generateChannelKey output).
 *
 * For each canonical in the listing, we check for a variant matching the browsed provider (via getProviderGroup) and extract its channelSelector via
 * getResolvedChannel. This builds a channelSelector → lineup state map that the discovered channels are matched against.
 *
 * @param channels - The raw discovered channels from the provider.
 * @param providerSlug - The slug of the provider being browsed (e.g., "spectrum", "hulu").
 * @returns Annotated channels with lineup state where applicable.
 */
function annotateWithLineupState(channels: DiscoveredChannel[], providerSlug: string): AnnotatedChannel[] {

  // Build a channelSelector → lineup state mapping for the browsed provider. For each canonical entry in the listing, we find the variant that corresponds
  // to this provider (if one exists) and index by that variant's channelSelector. This ensures matching works even when predefined keys don't match the
  // normalized channel name (e.g., "axstv" vs generateChannelKey("AXS TV") → "axs-tv").
  //
  // When two canonicals share the same channelSelector for a provider (East/Pacific pairs like "Disney Channel"), the map stores an array of states. The
  // annotation step then uses the discovery entry's stationId to disambiguate which canonical to assign.
  const listing = getChannelListing();
  const bySelector = new Map<string, LineupState[]>();

  // Appends a lineup state to the map under the given key. Multiple states can share a key (e.g., East/Pacific pairs both using channelSelector "A&E").
  function indexState(key: string, state: LineupState): void {

    const existing = bySelector.get(key);

    if(existing) {

      existing.push(state);
    } else {

      bySelector.set(key, [state]);
    }
  }

  for(const entry of listing) {

    const canonicalKey = entry.key;
    const resolvedKey = resolveProviderKey(canonicalKey);
    const currentTag = getProviderTagForChannel(resolvedKey);
    const currentProvider = getChannelProviderLabel(entry.channel);
    const displayName = entry.channel.name ?? canonicalKey;
    const source = isPredefinedChannel(canonicalKey) ? "predefined" : "user";

    // Check the provider group for variants. Used both for channelSelector matching and for computing hasAlternatives.
    const group = getProviderGroup(canonicalKey);

    // Determine whether the channel has at least one enabled provider besides the browsed provider. Check the canonical's own tag first, then iterate
    // the group's variants. This uses the existing provider tag and filter infrastructure.
    const canonicalTag = getProviderTagForChannel(canonicalKey);
    let hasAlternatives = (canonicalTag !== providerSlug) && isProviderTagEnabled(canonicalTag);

    if(!hasAlternatives && group) {

      for(const variant of group.variants) {

        const variantTag = getProviderTagForChannel(variant.key);

        if((variantTag !== providerSlug) && isProviderTagEnabled(variantTag)) {

          hasAlternatives = true;

          break;
        }
      }
    }

    const state: LineupState = {

      canonicalKey, currentProvider, currentTag, displayName, enabled: entry.enabled, hasAlternatives,
      logoUrl: getChannelLogo(canonicalKey),
      source, stationId: entry.channel.stationId
    };

    // Find the variant matching the browsed provider and store by its channelSelector.
    if(group) {

      for(const variant of group.variants) {

        if(getProviderTagForChannel(variant.key) === providerSlug) {

          const variantChannel = getResolvedChannel(variant.key);

          if(variantChannel?.channelSelector) {

            indexState(variantChannel.channelSelector, state);
          }

          break;
        }
      }
    }

    // Also match the canonical itself if its provider tag matches (single-provider channels or canonicals that point directly to this provider).
    if((currentTag === providerSlug) && entry.channel.channelSelector) {

      indexState(entry.channel.channelSelector, state);
    }

    // Index by display name when it differs from the channelSelector, so discovered channels whose names don't match any predefined channelSelector can still
    // be annotated via the name fallback. This connects Pacific timezone variants (e.g., discovered "A&E (Pacific)") to their predefined canonical (aep with
    // displayName "A&E (Pacific)") whose channelSelector ("A&E") wouldn't match the discovered name.
    if(entry.channel.channelSelector && (displayName !== entry.channel.channelSelector)) {

      indexState(displayName, state);
    }
  }

  // Annotate each discovered channel by matching its channelSelector against the provider-specific lookup. When the channelSelector doesn't match (e.g.,
  // Xfinity uses callSigns like "ESPND" while predefined variants use display names like "ESPN"), the display name is tried as a fallback. When multiple
  // canonicals share the same key (East/Pacific pairs), the discovery entry's stationId disambiguates which canonical to assign.
  return channels.map((ch) => {

    const states = bySelector.get(ch.channelSelector) ?? bySelector.get(ch.name);

    if(!states || (states.length === 0)) {

      return ch;
    }

    // Single match — no disambiguation needed.
    if(states.length === 1) {

      return { ...ch, lineup: states[0] };
    }

    // Multiple matches — use stationId to pick the right canonical. Fall back to the first match if stationId doesn't disambiguate.
    const match = states.find((s) => s.stationId === ch.stationId) ?? states[0];

    return { ...ch, lineup: match };
  });
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
    const lineup = req.query.lineup === "true";
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

        res.json(lineup ? annotateWithLineupState(cached, slug) : cached);

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

        res.json(lineup ? annotateWithLineupState(channels, slug) : channels);

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
