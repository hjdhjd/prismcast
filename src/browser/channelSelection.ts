/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channelSelection.ts: Channel selection coordinator for multi-channel streaming sites.
 */
import type { ChannelSelectionProfile, ChannelSelectorResult, ChannelStrategyEntry, Nullable, ProviderModule, ResolvedSiteProfile } from "../types/index.js";
import { LOG, delay, evaluateWithAbort, formatError } from "../utils/index.js";
import { getDomainConfig, registerProviderModuleProfile } from "../config/sites.js";
import { CONFIG } from "../config/index.js";
import type { Page } from "puppeteer-core";
import { coxProvider } from "./tuning/cox.js";
import { directvProvider } from "./tuning/directv.js";
import { foxProvider } from "./tuning/fox.js";
import { hboProvider } from "./tuning/hbo.js";
import { huluProvider } from "./tuning/hulu.js";
import { isChannelSelectionProfile } from "../types/index.js";
import { resolveMatchSelector } from "./tuning/shared.js";
import { slingProvider } from "./tuning/sling.js";
import { spectrumProvider } from "./tuning/spectrum.js";
import { thumbnailRowStrategy } from "./tuning/thumbnailRow.js";
import { tileClickStrategy } from "./tuning/tileClick.js";
import { xfinityProvider } from "./tuning/xfinity.js";
import { yttvProvider } from "./tuning/youtubeTv.js";

/* Multi-channel streaming sites (like USA Network) present multiple channels on a single page, with a program guide for each channel. Users must select which
 * channel they want to watch by clicking on a show in the guide. This module coordinates the dispatch to per-provider strategy functions in the tuning/ directory.
 *
 * Each provider tuning file exports a single ProviderModule object that bundles identity metadata (slug, label, guideUrl), the tuning strategy, and a
 * discoverChannels implementation. The coordinator builds its strategy dispatch lookup from provider modules at evaluation time. Generic strategies
 * (thumbnailRow, tileClick) remain bare ChannelStrategyEntry objects - they are site-specific interaction patterns, not provider-level registrations.
 *
 * Shared utilities (scrollAndClick, normalizeChannelName, resolveMatchSelector, logAvailableChannels) live in tuning/shared.ts so that tuning strategy files can
 * import them without creating a circular dependency with this coordinator. This module re-exports them for backward compatibility.
 */

/* Adding a new channel selection provider:
 *
 * 1. Create a new file in tuning/ implementing the strategy function with the ChannelStrategyHandler signature.
 * 2. Export a single ProviderModule object from the file. Set the required fields:
 *    - slug, label, guideUrl: Identity metadata for API endpoints and logging.
 *    - strategyName: The ChannelSelectionStrategy union value that site profiles reference.
 *    - profile, profileName: The SiteProfile definition and its name (e.g., "huluLive"). Registered automatically by the coordinator at import time.
 *    - strategy: A ChannelStrategyEntry with at minimum an execute hook. Also set clearCache, resolveDirectUrl, and invalidateDirectUrl as needed.
 *    - discoverChannels: Reads the provider's guide for all available channels, returning DiscoveredChannel[].
 * 3. Import the provider here and add it to the providerModules array.
 * 4. Add the strategy name to the ChannelSelectionStrategy union type in types/index.ts.
 * 5. Add a DOMAIN_CONFIG entry in config/sites.ts mapping the provider's domain to the profileName.
 *
 * The coordinator handles all cross-cutting concerns (dispatch, cache clearing, direct URL resolution, matchSelector polling) through the ChannelStrategyEntry
 * interface. Strategy files may import scrollAndClick(), normalizeChannelName(), resolveMatchSelector(), and logAvailableChannels() from this module for shared
 * utilities.
 */

// Provider module registry. The primary registry for all provider-level operations. Each entry bundles identity metadata, tuning strategy, and channel discovery.
// Future capabilities become additional methods on ProviderModule - no new registries needed.
const providerModules: readonly ProviderModule[] = [
  coxProvider, directvProvider, foxProvider, hboProvider, huluProvider, slingProvider, spectrumProvider, xfinityProvider, yttvProvider
];

// Strategy dispatch registry. Derived from provider modules (keyed by strategyName) plus generic strategies that are not provider-level registrations.
const strategies: Record<string, ChannelStrategyEntry> = Object.fromEntries([
  ...providerModules.map((p) => [ p.strategyName, p.strategy ]),
  [ "thumbnailRow", thumbnailRowStrategy ],
  [ "tileClick", tileClickStrategy ]
]) as Record<string, ChannelStrategyEntry>;

// Register provider module profiles with the profile resolution system. This happens at module evaluation time so profiles are available before any startup
// code runs. Providers that define a profile and profileName have their profile registered via the setter in sites.ts, avoiding circular dependencies.
for(const provider of providerModules) {

  if(provider.profile && provider.profileName) {

    registerProviderModuleProfile(provider.profileName, provider.profile);
  }
}

/**
 * Returns a direct watch URL for the channel specified in the profile, if one can be resolved. Looks up the strategy entry's resolveDirectUrl hook and calls it
 * with the channelSelector and page. Returns null if the strategy has no resolver, the profile has no channelSelector, or the resolver returns null.
 * @param profile - The resolved site profile.
 * @param page - The Puppeteer page object, passed through to the strategy's resolver for response interception setup or API calls.
 * @returns The direct watch URL or null.
 */
export async function resolveDirectUrl(profile: ResolvedSiteProfile, page: Page): Promise<Nullable<string>> {

  const { channelSelection, channelSelector } = profile;

  if(!channelSelector) {

    return null;
  }


  return await strategies[channelSelection.strategy]?.resolveDirectUrl?.(channelSelector, page) ?? null;
}

/**
 * Invalidates the cached direct watch URL for the channel specified in the profile. Looks up the strategy entry's invalidateDirectUrl hook and calls it with
 * the channelSelector. No-op if the strategy has no invalidator or the profile has no channelSelector.
 * @param profile - The resolved site profile.
 */
export function invalidateDirectUrl(profile: ResolvedSiteProfile): void {

  const { channelSelection, channelSelector } = profile;

  if(!channelSelector) {

    return;
  }


  strategies[channelSelection.strategy]?.invalidateDirectUrl?.(channelSelector);
}

/**
 * Clears all channel selection caches. Called by handleBrowserDisconnect() in browser/index.ts when the browser restarts, since cached state (guide row positions,
 * discovered page URLs, watch URLs) may be stale in a new browser session.
 */
export function clearChannelSelectionCaches(): void {

  for(const entry of Object.values(strategies)) {

    entry.clearCache?.();
  }
}

/**
 * Looks up a provider module by its URL slug. Returns undefined if no provider matches.
 * @param slug - The provider slug (e.g., "yttv", "hulu", "sling").
 * @returns The matching provider module or undefined.
 */
export function getProviderBySlug(slug: string): ProviderModule | undefined {

  return providerModules.find((p) => p.slug === slug);
}

/**
 * Looks up a provider module by its channel-selection strategy name. Used by code paths that have a resolved profile (and therefore the strategy name) but not
 * a slug. Returns undefined when no provider registers the given strategy or when the strategy is one of the generic non-provider strategies (thumbnailRow,
 * tileClick, etc.).
 * @param strategyName - The ChannelSelectionStrategy value (e.g., "foxGrid", "huluLive", "slingLive").
 * @returns The matching provider module or undefined.
 */
export function getProviderByStrategy(strategyName: string): ProviderModule | undefined {

  return providerModules.find((p) => p.strategyName === strategyName);
}

/**
 * Returns all registered provider module slugs. Used for validation in the checkboxList setting for precache providers.
 * @returns Array of provider slugs.
 */
export function getProviderSlugs(): string[] {

  return providerModules.map((p) => p.slug);
}

/**
 * Returns slug, label, domain, and optional icon URL for all registered provider modules. Used by the checkboxList setting for precache labels and by the
 * browse modal for provider picker cards with icons. The domain is extracted from the guide URL. The icon URL is derived from the provider's DOMAIN_CONFIG
 * entry - the single source of truth for provider icon URLs.
 * @returns Array of objects with domain, iconUrl, label, noDirectTuneOptimization, and slug properties.
 */
export function getProviderModuleInfo(): { domain: string; iconUrl?: string; label: string; noDirectTuneOptimization?: boolean; slug: string }[] {

  return providerModules.map((p) => {

    const domain = new URL(p.guideUrl).hostname;
    const domainConfig = getDomainConfig(p.guideUrl);

    return { domain, iconUrl: domainConfig?.iconUrl, label: p.label, noDirectTuneOptimization: p.noDirectTuneOptimization, slug: p.slug };
  });
}

/**
 * Returns a mapping of provider guide URL hostnames to provider slugs for all registered provider modules. Used by the channels panel to embed a client-side
 * lookup table so the browser can fetch provider channel discovery by slug when the user enters a matching URL.
 * @returns Record mapping hostnames to provider slugs.
 */
export function getProviderDomainMap(): Record<string, string> {

  const map: Record<string, string> = {};

  for(const provider of providerModules) {

    map[new URL(provider.guideUrl).hostname] = provider.slug;
  }

  return map;
}

/**
 * Returns a map of provider slugs to their guide URLs. Used client-side to suggest the correct full URL when a user enters a bare or www-variant hostname.
 * @returns Record mapping provider slug to guide URL.
 */
export function getProviderGuideUrls(): Record<string, string> {

  const map: Record<string, string> = {};

  for(const provider of providerModules) {

    map[provider.slug] = provider.guideUrl;
  }

  return map;
}

/**
 * Returns cached discovered channels from all provider modules, grouped by guide URL hostname. Each entry includes the hostname and an array of label/value pairs
 * suitable for datalist population. Only includes providers whose cache is non-null (i.e., discovery or precaching has already run). Used by the channels panel
 * to merge provider-discovered channels into the channel selector datalist alongside predefined channel suggestions.
 * @returns Array of objects with hostname and entries properties.
 */
export function getCachedProviderChannels(): { entries: { label: string; stationId?: string; value: string }[]; hostname: string }[] {

  const results: { entries: { label: string; stationId?: string; value: string }[]; hostname: string }[] = [];

  for(const provider of providerModules) {

    const cached = provider.getCachedChannels();

    if(!cached) {

      continue;
    }

    const hostname = new URL(provider.guideUrl).hostname;
    const entries = cached.map((ch) => ({ label: ch.name, stationId: ch.stationId, value: ch.channelSelector }));

    results.push({ entries, hostname });
  }

  return results;
}

// Re-export shared tuning utilities so external callers that import from channelSelection.ts continue to work. The implementations live in tuning/shared.ts to
// break the circular import between the coordinator (which imports tuning modules) and tuning modules (which need these utilities).
export { logAvailableChannels, normalizeChannelName, resolveMatchSelector, scrollAndClick } from "./tuning/shared.js";

/**
 * Options that callers may supply to selectChannel(). All fields are optional; omitting them preserves the historical no-op call signature.
 */
export interface SelectChannelOptions {

  /**
   * Callback invoked when the resolution layer converts a category selector to a concrete per-user channel identifier. The framework calls this with the resolved
   * call sign so the caller can persist the value to the user's channel store. After persistence, subsequent tunes start with the concrete selector and skip the
   * resolution path entirely.
   *
   * Construction of this callback happens in the streaming setup layer where the active channel key and service tag are in scope. selectChannel deliberately does
   * not know about the user channel store; it only invokes the callback the caller supplies. Errors thrown from the callback do not abort the tune - the resolved
   * selector still flows into the strategy and verifier for the current attempt; only persistence to disk is lost.
   *
   * Null/undefined means "do not persist." The resolution still happens and the resolved selector is used for this tune; it just isn't saved. Useful for ad-hoc
   * tunes (no associated channel record) and for testing.
   */
  persistResolution?: (resolvedSelector: string) => Promise<void>;
}

/**
 * Selects a channel from a multi-channel player UI using the strategy specified in the profile. This is the main entry point for channel selection, called by
 * initializePlayback() after page navigation.
 *
 * The function handles, in order:
 * - No-op short-circuit for single-channel sites (strategy "none" or no channelSelector).
 * - Category resolution: when the active provider declares categorySelectors and the profile's selector matches, resolveCategorySelector() is invoked to convert
 *   the category to a concrete per-user identifier. The resolved value replaces profile.channelSelector for the rest of the call and is persisted via the
 *   options.persistResolution callback if supplied. Resolution failures fall through with the original category selector - the strategy attempts a best-effort
 *   match and the verifier fails open.
 * - Pre-selection scroll phase to force lazy-loaded content into the DOM (when scrollToBottom or scrollSelector+scrollTarget is set).
 * - Polling for channel element readiness before strategy dispatch (when profile.channelSelection.matchSelector is set).
 * - Strategy dispatch based on profile.channelSelection.strategy.
 * @param page - The Puppeteer page object.
 * @param profile - The resolved site profile containing channelSelection config and channelSelector slug.
 * @param options - Optional callbacks for the resolution layer. Currently the only field is persistResolution.
 * @returns Result object with success status and optional failure reason.
 */
export async function selectChannel(page: Page, profile: ResolvedSiteProfile, options: SelectChannelOptions = {}): Promise<ChannelSelectorResult> {

  const { channelSelection } = profile;

  // No channel selection needed if strategy is "none" or no channelSelector is specified.
  if((channelSelection.strategy === "none") || !isChannelSelectionProfile(profile)) {

    return { success: true };
  }

  // The narrowed profile is used for the rest of this function. Resolution may produce a refined profile with a concrete channelSelector replacing the original
  // category value; the strategy receives the refined version. Reassigning the parameter would lose the narrowing TypeScript inferred from isChannelSelectionProfile
  // above, so we keep a separate local instead.
  let activeProfile: ChannelSelectionProfile = profile;

  // Category resolution. When the profile's selector is a category value declared by the active provider, we delegate to the provider's category resolver to
  // convert the category to a concrete per-user identifier (e.g., "FOXD2C" -> "WFLD" for a Chicago-market user). The resolved value replaces the profile's selector
  // for the remainder of this function so the strategy and verifier act on a concrete identifier. Persistence is fire-and-forget on the caller's persistResolution
  // callback - failures there do not abort the tune; the resolved selector still flows through this attempt, only the disk write is lost. When resolution returns
  // a CategoryResolutionFailure, the framework's response is governed by categories.requireResolution: strict providers abort the tune with the resolver-authored
  // reason; permissive providers log the reason and let the strategy attempt a best-effort match while the verifier fails open for the wildcard case.
  //
  // The whole resolution feature is gated by a single optional sub-object on the provider, so one optional-chain access decides whether to engage the layer at
  // all. The type system guarantees that if `categories` is present, every field needed for resolution is also present.
  const provider = getProviderByStrategy(channelSelection.strategy);
  const categoryResolution = provider?.categoryResolution;

  if(categoryResolution?.selectors.includes(activeProfile.channelSelector)) {

    const resolution = await categoryResolution.resolve(activeProfile.channelSelector, page);

    if("callSign" in resolution) {

      LOG.debug("tuning", "Resolved category selector \"%s\" to \"%s\" via %s.", activeProfile.channelSelector, resolution.callSign, provider?.label ?? "unknown");

      activeProfile = { ...activeProfile, channelSelector: resolution.callSign };

      if(options.persistResolution) {

        const resolved = resolution.callSign;

        options.persistResolution(resolved).catch((persistError: unknown) => {

          LOG.debug("tuning", "Failed to persist resolved category selector to \"%s\": %s.", resolved, formatError(persistError));
        });
      }
    } else if(categoryResolution.requireResolution) {

      // Strict provider: a category selector that cannot be resolved is an unrecoverable error. The resolver authored the reason - it has the knowledge to write
      // a complete, domain-shaped sentence including any selector- or provider-specific remediation guidance - so the framework relays it verbatim into the
      // setup-failure path that marks channel health, terminates the pending stream, and surfaces the message to the user.
      return { reason: resolution.reason, success: false };
    } else {

      // Permissive provider: log the resolver-authored reason at debug level and fall through. The strategy will attempt a best-effort match against the original
      // category selector, and the verifier will fail open for the wildcard case if the captured manifest URL cannot be cross-checked against discovered category
      // members.
      LOG.debug("tuning", "%s", resolution.reason);
    }
  }

  const entry = strategies[channelSelection.strategy];


  if(!entry) {

    LOG.warn("Unknown channel selection strategy: %s.", channelSelection.strategy);

    return { reason: "Unknown channel selection strategy.", success: false };
  }

  // Pre-selection scroll phase. Some sites (e.g., Disney+) lazy-load entire page sections - headings, tiles, and images only appear in the DOM after scrolling
  // them into the viewport. Two scroll modes are supported: scrollToBottom scrolls the page to the bottom to force all lazy content into the DOM, and
  // scrollSelector+scrollTarget progressively scrolls until a specific element with matching text content is found and scrolled into view. Both modes gate on a
  // readiness signal before scrolling - scrollToBottom waits for the page to become scrollable (scrollHeight > innerHeight), while scrollSelector waits for the
  // first matching DOM element - since SPAs typically fire the load event before React/framework rendering completes.
  if(channelSelection.scrollToBottom) {

    // Wait for the SPA to render enough content to make the page scrollable. SPAs fire the load event before the framework renders page sections, so scrollHeight
    // equals innerHeight immediately after navigation. We poll until scrollHeight exceeds innerHeight, indicating content has been rendered and there is somewhere
    // to scroll.
    try {

      await page.waitForFunction((): boolean => document.body.scrollHeight > window.innerHeight, { timeout: CONFIG.streaming.videoTimeout });
    } catch {

      LOG.debug("tuning:tileClick", "Page did not become scrollable within %sms (scrollHeight: %s, innerHeight: %s). Proceeding anyway.",
        CONFIG.streaming.videoTimeout, await page.evaluate(() => document.body.scrollHeight), await page.evaluate(() => window.innerHeight));
    }

    // Press End to scroll to the bottom of the page, forcing lazy-loaded sections to render as they enter the viewport.
    await page.keyboard.press("End");

    LOG.debug("tuning:tileClick", "Pressed End to scroll to page bottom (scrollHeight: %s).",
      await page.evaluate(() => document.body.scrollHeight));
  } else if(channelSelection.scrollSelector && channelSelection.scrollTarget) {

    // Targeted scroll: find a specific element matching scrollSelector whose text content equals scrollTarget, then scroll it into view. This is used when only a
    // particular section needs to be visible rather than the entire page. Progressively scrolls in viewport-sized increments, checking after each step whether the
    // target element has appeared - necessary because sites with IntersectionObserver-based lazy loading only add sections to the DOM as they enter the viewport.
    let found = false;

    // Wait for at least one element matching the selector to appear so the SPA has started rendering content.
    try {

      await page.waitForSelector(channelSelection.scrollSelector, { timeout: CONFIG.streaming.videoTimeout });
    } catch {

      LOG.debug("tuning:tileClick", "No \"%s\" elements appeared within %sms. Page may not have rendered.",
        channelSelection.scrollSelector, CONFIG.streaming.videoTimeout);
    }

    // Set the scroll deadline after the readiness gate so the progressive scroll loop gets its own full time budget rather than sharing it with the waitForSelector.
    const scrollDeadline = Date.now() + CONFIG.streaming.videoTimeout;

    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- found is mutated inside the loop body.
    while(!found && (Date.now() < scrollDeadline)) {

      // eslint-disable-next-line no-await-in-loop
      found = await evaluateWithAbort(page, (selector: string, target: string): boolean => {

        for(const el of Array.from(document.querySelectorAll(selector))) {

          if(el.textContent.trim() === target) {

            (el as HTMLElement).scrollIntoView({ behavior: "instant", block: "center" });

            return true;
          }
        }

        return false;
      }, [ channelSelection.scrollSelector, channelSelection.scrollTarget ]);

      if(found) {

        break;
      }

      // Scroll down by one viewport height to trigger the next batch of lazy-loaded sections.
      // eslint-disable-next-line no-await-in-loop
      await page.evaluate(() => { window.scrollBy(0, window.innerHeight); });

      // eslint-disable-next-line no-await-in-loop
      await delay(300);
    }

    if(found) {

      LOG.debug("tuning:tileClick", "Scroll target \"%s\" via \"%s\": found and scrolled into view.",
        channelSelection.scrollTarget, channelSelection.scrollSelector);

      // Brief settle delay for lazy content near the target to finish rendering.
      await delay(500);
    } else {

      // Log what headings exist to help diagnose text mismatches.
      const headings = await evaluateWithAbort(page, (selector: string): string[] => {

        return Array.from(document.querySelectorAll(selector)).map((el) => el.textContent.trim());
      }, [channelSelection.scrollSelector]);

      LOG.debug("tuning:tileClick", "Scroll target \"%s\" via \"%s\": not found after %sms. Found headings: %s.",
        channelSelection.scrollTarget, channelSelection.scrollSelector, CONFIG.streaming.videoTimeout, JSON.stringify(headings));
    }
  }

  // Poll for the channel element to appear and become visible. Only run when matchSelector is explicitly configured - the default fallback in
  // resolveMatchSelector() is for strategy-internal use, and guide-based strategies that don't set matchSelector skip this wait entirely. For <img> elements, we
  // also verify load completion (img.complete + naturalWidth) to prevent race conditions where the element exists with the correct src but hasn't finished
  // rendering, which can cause layout instability and click failures.
  if(channelSelection.matchSelector) {

    const selector = resolveMatchSelector(activeProfile);

    LOG.debug("tuning:tileClick", "Polling for matchSelector: %s (timeout: %sms).", selector, CONFIG.playback.channelSelectorDelay);

    try {

      await page.waitForFunction(
        (sel: string): boolean => {

          const el = document.querySelector(sel);

          if(!el) {

            return false;
          }

          const rect = el.getBoundingClientRect();

          if(!((rect.width > 0) && (rect.height > 0))) {

            return false;
          }

          // For <img> elements, also verify the image has fully loaded.
          if(el instanceof HTMLImageElement) {

            return el.complete && (el.naturalWidth > 0);
          }

          return true;
        },
        { timeout: CONFIG.playback.channelSelectorDelay },
        selector
      );

      LOG.debug("tuning:tileClick", "matchSelector poll succeeded: element found and visible.");
    } catch {

      // Timeout - the element hasn't appeared or loaded yet. Proceed anyway and let the strategy evaluate and report not-found naturally.
      LOG.debug("tuning:tileClick", "matchSelector poll timed out after %sms. Element not found or not visible.", CONFIG.playback.channelSelectorDelay);
    }
  }

  // Dispatch to the appropriate strategy via the registry. The strategy receives activeProfile so it sees the resolved category selector when applicable.
  const result = await entry.execute(page, activeProfile);

  return result;
}
