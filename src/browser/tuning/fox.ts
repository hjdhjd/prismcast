/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * fox.ts: Fox.com guide grid channel selection strategy.
 */
import type { CategoryResolution, ChannelSelectionProfile, ChannelSelectorResult, DiscoveredChannel, Nullable, ProviderModule } from "../../types/index.ts";
import { LOG, evaluateWithAbort, formatError } from "../../utils/index.ts";
import { CONFIG } from "../../config/index.ts";
import type { Page } from "puppeteer-core";
import { logAvailableChannels } from "./shared.ts";

// Raw channel info extracted from each GuideChannelContainer during discovery. The stationCode comes from the button title (e.g., "FOXD2C", "FNC"), the internalCode
// from the first data-content-impression-id prefix (e.g., the local affiliate call sign or internal station identifier), and locked from the presence of a lock-icon
// overlay on the first program thumbnail.
interface FoxChannelInfo {

  internalCode: string;
  locked: boolean;
  stationCode: string;
}

// Cached discovery results. Populated by the first discoverFoxChannels call (discovery endpoint). Fox tuning is stateless (no tuning cache exists), so only
// the discovery endpoint populates this cache. Cleared on browser disconnect via clearFoxCache().
let cachedDiscoveredChannels: Nullable<DiscoveredChannel[]> = null;

// Single source of truth for Fox's category-selector membership. Read by foxProvider.categoryResolution.selectors so the resolution layer in selectChannel() knows which
// selectors to route through resolveFoxCategorySelector, and read by verifyFoxManifest so it applies wildcard semantics on the same set. Adding a new Fox category
// selector means appending one entry here - the provider declaration and the verifier pick it up automatically.
const FOX_CATEGORY_SELECTORS: readonly string[] = ["FOXD2C"];

/**
 * Clears the Fox discovery cache. Called by clearChannelSelectionCaches() in the coordinator when the browser restarts.
 */
function clearFoxCache(): void {

  cachedDiscoveredChannels = null;
}

/**
 * Extracts the channel call sign from a fox.com master HLS manifest URL. The URL shape is
 *
 *     https://<host>/<auth-token>/<prefix>/<callsign>-<region>/index.m3u8?<query>
 *
 * where the second-to-last path segment encodes the call sign and region (e.g., "wfld-ue2", "fnc-ue2", "fbn-ue2"). The call sign is the part before the first
 * dash. Returns null when the URL is not parseable, has fewer than two path segments, or does not match the expected shape - the caller treats null as "shape
 * unrecognized" and fails open rather than rejecting a stream that might still be correct.
 * @param url - The full master manifest URL.
 * @returns The lowercase call sign, or null when not extractable.
 */
function extractCallSignFromManifestUrl(url: string): Nullable<string> {

  let pathname: string;

  try {

    pathname = new URL(url).pathname;
  } catch {

    return null;
  }

  const segments = pathname.split("/").filter((segment) => segment.length > 0);

  if(segments.length < 2) {

    return null;
  }

  // Second-to-last segment is the channel directory; the file name is the last segment.
  const channelSegment = segments[segments.length - 2] ?? "";
  const dashIndex = channelSegment.indexOf("-");

  if(dashIndex <= 0) {

    return null;
  }

  return channelSegment.substring(0, dashIndex).toLowerCase();
}

/**
 * Failsafe verifier called by streaming/setup.ts (setupStream's tune-verification step) after the master manifest URL has been captured for a Fox tune. Confirms
 * the URL belongs to the channel the user asked for. The selector is compared lowercase against the call sign extracted from the URL path - both cable channels
 * (FBN, FNC, FS1, etc.) and local affiliate
 * call signs (WFLD, WPWRDT, etc.) follow the same lowercase convention in Fox's CDN paths. Returns null on match. Returns null when the URL shape is unrecognized
 * (fail-open: better to accept a stream we cannot inspect than to reject a working one when Fox restructures their CDN). Returns a failure reason when the URL
 * decodes cleanly to a different call sign - that is the unmistakable signature of a click that did not switch the player, and we want the stream to fail loudly
 * rather than silently deliver the wrong channel.
 * @param url - The master manifest URL captured by the interceptor.
 * @param channelSelector - The expected channel selector for this tune.
 * @returns Null on match or unrecognizable URL. Failure reason string when the URL belongs to a different channel.
 */
function verifyFoxManifest(url: string, channelSelector: string): Nullable<string> {

  const callSign = extractCallSignFromManifestUrl(url);

  // Fail open on unparseable URLs - we cannot inspect the URL but the strategy and player gates upstream are working hard enough that we trust them when the
  // failsafe cannot speak. Emit a debug line so a CDN-side restructure does not silently disable verification - the next tune attempt (with debug enabled) will
  // surface the unrecognized shape, giving us a forward-warning signal rather than a missing-coverage gap that goes unnoticed until something else breaks.
  if(!callSign) {

    LOG.debug("tuning:fox", "Manifest URL shape unrecognized; verification fail-open. URL: %s.", url);

    return null;
  }

  const expected = channelSelector.toLowerCase();

  // Category selector path. The resolution layer in selectChannel() converts category selectors to concrete call signs before strategy dispatch, so this branch
  // only fires when resolution failed (cache empty AND in-line discovery yielded nothing). With cache populated we accept any call sign discovery has tagged as a
  // member of this category; we deliberately do not allow arbitrary call signs through, so a click that landed on a non-category station still surfaces a clear
  // mismatch. With cache empty we fail open - the resolver tried, the strategy made a best-effort match, and without discovery data we cannot prove the click was
  // wrong. Membership is determined by ch.categorySelector rather than ch.affiliate so the test stays explicit even as more categories are introduced over time.
  if(FOX_CATEGORY_SELECTORS.includes(channelSelector)) {

    if(!cachedDiscoveredChannels) {

      LOG.debug("tuning:fox", "Category selector \"%s\" reached the verifier without discovery cache; failing open. URL call sign: %s.", channelSelector,
        callSign);

      return null;
    }

    const isKnownCategoryMember = cachedDiscoveredChannels.some((ch) => (ch.categorySelector === channelSelector) && (ch.affiliate?.toLowerCase() === callSign));

    if(isKnownCategoryMember) {

      return null;
    }

    return "Manifest URL is for channel \"" + callSign + "\", which is not a known member of the \"" + channelSelector + "\" category in this market.";
  }

  if(callSign === expected) {

    return null;
  }

  return "Manifest URL is for channel \"" + callSign + "\", but \"" + channelSelector + "\" was requested.";
}

/**
 * Resolves one of the provider's declared category selectors (see FOX_CATEGORY_SELECTORS) to a concrete per-user call sign; FOXD2C is the title shared by
 * every Fox-owned local affiliate Fox.com surfaces in the user's market. Resolution consults discovery: if discovery has cached results, we read the first
 * FOXD2C-tagged entry (the entry whose affiliate field is populated, set only for FOXD2C entries during discovery). If the cache is empty, we run discovery
 * in-line on the already-loaded page; the resolver is invoked from selectChannel() after navigation, so the guide grid is rendered or about to render.
 * Returns a CategoryResolutionFailure with a user-facing reason when no FOXD2C affiliate is present, which happens before the grid hydrates or when the user
 * is not authenticated; the resolution layer relays the reason and falls through to a best-effort match with the original category selector.
 *
 * For users in markets with multiple FOXD2C affiliates (e.g., Chicago has both WFLD and WPWRDT - Fox-owned Fox and CW O&O), discovery returns them in DOM order
 * because the in-line walk preserves it and the post-walk alphabetical sort is stable for name ties (every FOXD2C entry has name="FOXD2C"). The first entry is
 * the primary Fox-owned Fox affiliate in nearly all markets. Users who want the secondary station can manually edit the per-channel override after the system
 * persists the resolved selector - the override is the same delta a user-set selector produces.
 * @param selector - The category selector value being resolved (one of the entries in foxProvider.categoryResolution.selectors).
 * @param page - The active Fox.com page. Used to run discovery in-line when the cache is empty.
 * @returns CategoryResolutionSuccess with the resolved call sign, or CategoryResolutionFailure with a user-facing reason when resolution cannot be performed.
 */
async function resolveFoxCategorySelector(selector: string, page: Page): Promise<CategoryResolution> {

  // Defensive: only resolve selectors this provider actually declares as categories. selectChannel only invokes the resolver for categoryResolution.selectors,
  // so reaching this branch indicates a bug - either selectChannel routed wrong, or a caller bypassed selectChannel and invoked the resolver directly with the
  // wrong value. We throw rather than returning a CategoryResolutionFailure because the resolver's `reason` field is contracted as user-facing prose for
  // operational failures; an internal contract violation is a developer-audience event and belongs in the exception channel where it propagates to the existing
  // unexpected-error handling path (handleSetupFailure logs at error level with a stack trace).
  if(!FOX_CATEGORY_SELECTORS.includes(selector)) {

    throw new Error("resolveFoxCategorySelector invoked with non-category selector \"" + selector + "\". This is a contract violation - the resolver may only " +
      "be called with values declared in FOX_CATEGORY_SELECTORS.");
  }

  // Prefer cached discovery (typically populated by precaching at startup or by an earlier discovery call). Fall through to in-line discovery on the loaded page
  // when the cache is empty - the route handler's networkidle2 navigation has already occurred by the time selectChannel runs, so the guide grid is hydrated or
  // very close to it. Wrap in try/catch so unexpected errors (page closed, navigation aborted, Puppeteer evaluate timeout) become an articulated CategoryResolution
  // rather than a thrown exception escaping the resolver. The technical detail goes to the debug log under the "tuning:fox" category for developer diagnosis; the
  // user-facing reason is clean prose with actionable remediation, written for the audience that will see it.
  let channels: DiscoveredChannel[];

  try {

    channels = cachedDiscoveredChannels ?? (await discoverFoxChannels(page));
  } catch(error) {

    LOG.debug("tuning:fox", "discoverFoxChannels threw during category resolution for \"%s\": %s.", selector, formatError(error));

    return { reason: "Fox channel discovery could not complete because the Fox.com page state is unstable. Retry the tune; if the problem persists, restart the " +
      "browser session." };
  }

  if(channels.length === 0) {

    return { reason: "Fox.com returned no channels for this session. Sign in to your TV provider on Fox One in the browser, then re-run channel discovery to " +
      "confirm your lineup is reachable." };
  }

  // Iterate in discovery order and return the first member of the requested category. Discovery preserves DOM order for entries with equal names (the in-line walk
  // is in DOM order and the post-walk sort is stable), so the first match is the primary station in the user's market for the category.
  const member = channels.find((ch) => (ch.categorySelector === selector) && Boolean(ch.affiliate));

  if(!member?.affiliate) {

    return { reason: "Fox.com discovery returned no \"" + selector + "\" category members for this market. The user's account may not include a Fox-owned " +
      "local affiliate, or the guide grid had not finished hydrating when discovery ran." };
  }

  return { callSign: member.affiliate };
}

/**
 * Fox.com grid strategy: finds a channel in the non-virtualized guide grid at fox.com/live/channels by matching the station code (button title attribute) on
 * GuideChannelLogo buttons, with fallback to internal station codes from data-content-impression-id attributes. All ~15 channels are present in the DOM
 * simultaneously once the grid renders dynamically (~4.5s after page load). Clicking the logo button is an SPA state transition - the Bitmovin player at the
 * top of the page switches channels without navigation, destroying and recreating its video element with a new blob src.
 *
 * The selection process:
 * 1. Poll via waitForFunction until the target appears as either a button title (e.g., FOXD2C, FNC) or an impression ID prefix (local affiliate call sign).
 * 2. Scan all channel rows, checking button title first, then impression ID prefix for fallback matching.
 * 3. Case-insensitive match against the channelSelector.
 * 4. On match, call logoButton.click() directly via DOM - coordinate-based clicking is not possible because the GuideProgramHero (sticky, z-40) overlays the
 *    guide grid and intercepts all mouse events at the element's coordinates.
 * @param page - The Puppeteer page object.
 * @param profile - The resolved site profile with a non-null channelSelector (station code like "FOXD2C" or a local affiliate call sign).
 * @returns Result object with success status and optional failure reason.
 */
async function foxGridStrategy(page: Page, profile: ChannelSelectionProfile): Promise<ChannelSelectorResult> {

  const stationCode = profile.channelSelector;

  // Wait for the target to appear in the guide grid. Checks both button title (primary) and data-content-impression-id prefix (fallback for call-sign-based
  // selectors that target a specific local affiliate by call sign).
  try {

    await page.waitForFunction(
      (code: string): boolean => {

        const codeLower = code.toLowerCase();
        const containers = document.querySelectorAll("[data-testid=\"GuideChannelContainer\"]");

        return Array.from(containers).some((c) => {

          const btn = c.querySelector("[data-testid=\"GuideChannelLogo\"] button");

          if((btn?.getAttribute("title") ?? "").toLowerCase() === codeLower) {

            return true;
          }

          // Fallback: check the first data-content-impression-id prefix. The format is "{PREFIX}-program-..." where PREFIX is the internal station code.
          const impressionDiv = c.querySelector("[data-content-impression-id]");
          const impressionId = impressionDiv?.getAttribute("data-content-impression-id") ?? "";
          const prefix = (impressionId.split("-program-")[0] ?? "").toLowerCase();

          return (prefix.length > 0) && (prefix === codeLower);
        });
      },
      { timeout: CONFIG.streaming.videoTimeout },
      stationCode
    );
  } catch {

    // Best-effort diagnostic: collect all available station codes and internal codes from the guide grid.
    try {

      const availableChannels = await evaluateWithAbort(page, (): string[] => {

        const codes = new Set<string>();

        for(const container of Array.from(document.querySelectorAll("[data-testid=\"GuideChannelContainer\"]"))) {

          const btn = container.querySelector("[data-testid=\"GuideChannelLogo\"] button");
          const title = (btn?.getAttribute("title") ?? "").trim();

          if(title.length > 0) {

            codes.add(title);
          }

          const impressionDiv = container.querySelector("[data-content-impression-id]");
          const impressionId = (impressionDiv?.getAttribute("data-content-impression-id") ?? "");
          const prefix = (impressionId.split("-program-")[0] ?? "").trim();

          if((prefix.length > 0) && (prefix !== title)) {

            codes.add(prefix);
          }
        }

        return Array.from(codes).toSorted();
      }, []);

      if(availableChannels.length > 0) {

        logAvailableChannels({

          availableChannels,
          channelName: stationCode,
          guideUrl: "https://www.fox.com/live/channels",
          providerName: "Fox"
        });
      }
    } catch {

      // Diagnostic dump is best-effort.
    }

    return { reason: "Station code " + stationCode + " not found in Fox.com guide grid.", success: false };
  }

  // Wait for Bitmovin to finish booting its initial player session before clicking. The grid-render wait above proves React has mounted the channel logos, but
  // Fox's React component only delegates the channel switch to Bitmovin once the player has a live, decoding session - which we observe externally as <video>
  // having a blob: src AND readyState >= HAVE_CURRENT_DATA (2), i.e. the decoder has produced at least one frame. The combined gate avoids two failure modes:
  // clicking before any src is assigned (player not wired up at all), and clicking during the brief window when Bitmovin is reassigning src as it transitions
  // from a placeholder to the first MSE-backed source. Without this gate, the click registers in React (sidebar caption updates) but the player isn't ready to
  // act on it, and the page sits on its page-default channel for the rest of the stream's lifetime. Best-effort: if the player never reaches this state we still
  // attempt the click, and the user's symptom report will surface the failure.
  try {

    await page.waitForFunction(
      (): boolean => {

        const video = document.querySelector("video");

        return Boolean(video && video.src.startsWith("blob:") && (video.readyState >= 2));
      },
      { timeout: CONFIG.streaming.videoTimeout }
    );
  } catch {

    LOG.debug("tuning:fox", "Bitmovin player did not reach a decoding state within the timeout. Proceeding with click anyway.");
  }

  // Click the channel logo button to tune the player. We use DOM element.click() rather than coordinate-based page.mouse.click() because the GuideProgramHero
  // section (sticky top-[64px], z-40) overlays the guide grid and intercepts all coordinate-based mouse events. DOM .click() dispatches the event directly to the
  // element, bypassing the sticky hero's hit-testing.
  //
  // Matching order: button title first (handles "FOXD2C", "FNC", etc.), then impression ID prefix (handles local affiliate call signs for specific affiliate
  // selection).
  const clicked = await evaluateWithAbort(page, (code: string): boolean => {

    const codeLower = code.toLowerCase();
    const containers = document.querySelectorAll("[data-testid=\"GuideChannelContainer\"]");

    for(const container of Array.from(containers)) {

      const logoButton = container.querySelector<HTMLElement>("[data-testid=\"GuideChannelLogo\"] button");

      if(!logoButton) {

        continue;
      }

      // Primary match: button title (e.g., "FOXD2C", "FNC").
      if((logoButton.getAttribute("title") ?? "").toLowerCase() === codeLower) {

        logoButton.click();

        return true;
      }
    }

    // Fallback pass: match against impression ID prefix for call-sign-based selectors.
    for(const container of Array.from(containers)) {

      const logoButton = container.querySelector<HTMLElement>("[data-testid=\"GuideChannelLogo\"] button");

      if(!logoButton) {

        continue;
      }

      const impressionDiv = container.querySelector("[data-content-impression-id]");
      const impressionId = impressionDiv?.getAttribute("data-content-impression-id") ?? "";
      const prefix = (impressionId.split("-program-")[0] ?? "").toLowerCase();

      if((prefix.length > 0) && (prefix === codeLower)) {

        logoButton.click();

        return true;
      }
    }

    return false;
  }, [stationCode]);

  if(!clicked) {

    return { reason: "Station code " + stationCode + " not found in Fox.com guide grid.", success: false };
  }

  return { success: true };
}

/**
 * Reads all channel info from the Fox guide grid. For each GuideChannelContainer, extracts the display station code (button title), the internal station code
 * (from the first data-content-impression-id prefix), and whether the channel is locked (lock-icon present on the first program thumbnail).
 * @param page - The Puppeteer page object, expected to be on the Fox live channels page with at least one GuideChannelContainer rendered.
 * @returns Array of channel info objects in DOM order.
 */
async function readFoxChannels(page: Page): Promise<FoxChannelInfo[]> {

  return await evaluateWithAbort(page, (): FoxChannelInfo[] => {

    const results: FoxChannelInfo[] = [];

    for(const container of Array.from(document.querySelectorAll("[data-testid=\"GuideChannelContainer\"]"))) {

      const logoButton = container.querySelector("[data-testid=\"GuideChannelLogo\"] button");
      const stationCode = (logoButton?.getAttribute("title") ?? "").trim();

      if(stationCode.length === 0) {

        continue;
      }

      // Extract the internal station code from the first data-content-impression-id. Format: "{PREFIX}-program-..." where PREFIX is the internal code.
      const impressionDiv = container.querySelector("[data-content-impression-id]");
      const impressionId = (impressionDiv?.getAttribute("data-content-impression-id") ?? "");
      const internalCode = (impressionId.split("-program-")[0] ?? "").trim();

      // Locked channels have a lock-icon SVG overlaid on the first program thumbnail. These require TV provider authentication (add-on tier).
      const locked = container.querySelector("[data-testid=\"lock-icon\"]") !== null;

      results.push({ internalCode: internalCode.length > 0 ? internalCode : stationCode, locked, stationCode });
    }

    return results;
  }, []);
}

/**
 * Discovers all channels from the Fox guide grid. Waits for the first grid container to confirm the guide has rendered (the route handler's networkidle2
 * navigation ensures all API data has arrived before this function is called), then reads station codes, internal codes, and lock status from each
 * GuideChannelContainer. For FOXD2C entries (local affiliates), the channelSelector and affiliate are set to the internal call sign to enable precise affiliate
 * selection. For all other channels, the channelSelector and name are the display station code. Locked channels (requiring TV provider authentication) are
 * tagged with tier "addon".
 * @param page - The Puppeteer page object, expected to be on the Fox live channels page.
 * @returns Array of discovered channels with station codes, affiliate tagging, and tier information.
 */
async function discoverFoxChannels(page: Page): Promise<DiscoveredChannel[]> {

  if(cachedDiscoveredChannels) {

    return cachedDiscoveredChannels;
  }

  // Wait for at least one GuideChannelContainer to confirm the guide grid has rendered. The route handler navigates with networkidle2, which ensures all API data
  // has arrived before this function is called - no additional network idle wait is needed here.
  try {

    await page.waitForSelector("[data-testid=\"GuideChannelContainer\"]", { timeout: CONFIG.streaming.videoTimeout });
  } catch {

    return [];
  }

  const foxChannels = await readFoxChannels(page);

  // Do not cache empty results - leave null so subsequent calls retry the full walk. Empty results can indicate no TV provider login.
  if(foxChannels.length === 0) {

    return [];
  }

  cachedDiscoveredChannels = foxChannels.map((ch) => {

    const entry: DiscoveredChannel = { channelSelector: ch.stationCode, name: ch.stationCode };

    // Category-selector membership. When a station's title is one of the values declared in FOX_CATEGORY_SELECTORS, the entry is a member of that category and we
    // tag it with the category name plus the affiliate (the per-market call sign) so the resolver and the verifier can both reason about category membership
    // without needing to know which titles are categories. Use the affiliate as the entry's channelSelector so the discovery output is directly usable as a
    // unique-per-container selector.
    if(FOX_CATEGORY_SELECTORS.includes(ch.stationCode)) {

      entry.affiliate = ch.internalCode;
      entry.categorySelector = ch.stationCode;
      entry.channelSelector = ch.internalCode;
    }

    if(ch.locked) {

      entry.tier = "addon";
    }

    return entry;
  });

  cachedDiscoveredChannels.sort((a, b) => a.name.localeCompare(b.name));

  return cachedDiscoveredChannels;
}

export const foxProvider: ProviderModule = {

  // Category resolution for Fox: "FOXD2C" is a category title shared across every Fox-owned local affiliate Fox.com surfaces in the user's market. Each user gets
  // a per-market resolution (e.g., a Chicago user resolves "FOXD2C" to "WFLD"). The configuration is permissive (requireResolution omitted = false) so a failed
  // resolution falls through to a best-effort strategy match plus wildcard verifier behavior rather than aborting the tune.
  categoryResolution: {

    resolve: resolveFoxCategorySelector,
    selectors: FOX_CATEGORY_SELECTORS
  },

  discoverChannels: discoverFoxChannels,
  getCachedChannels: (): Nullable<DiscoveredChannel[]> => cachedDiscoveredChannels,
  guideUrl: "https://www.fox.com/live/channels",
  label: "Fox",

  // Profile for Fox.com live channel guide grid. The guide page presents all channels in a non-virtualized grid with station codes in the channel logo button
  // titles (e.g., FOXD2C, FNC, FS1). The channelSelector property matches against these station codes. Clicking the channel logo button is an SPA state
  // transition - the player at the top of the page switches channels without navigation. The grid renders dynamically after page load, so the strategy waits
  // for GuideChannelContainer elements before scanning.
  profile: {

    category: "multiChannel",
    channelSelection: { strategy: "foxGrid" },
    description: "Fox.com live channel guide. Set Channel Selector to the station code (e.g., BTN, FOXD2C, FS1).",
    extends: "fullscreenApi",
    summary: "Fox Live (guide grid, needs selector)"
  },
  profileName: "foxLive",
  slug: "foxone",
  strategy: { clearCache: clearFoxCache, execute: foxGridStrategy },
  strategyName: "foxGrid",
  verifyManifestForChannel: verifyFoxManifest
};
