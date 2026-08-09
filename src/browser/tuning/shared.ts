/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * shared.ts: Shared utilities for channel selection tuning strategies.
 */
import type { CDPSession, NewDocumentScriptEvaluation, Page } from "puppeteer-core";
import type { ChannelSelectionProfile, ClickTarget } from "../../types/index.ts";
import { LOG, delay, formatError } from "../../utils/index.ts";
import { CHANNELS } from "../../channels/index.ts";
import { CONFIG } from "../../config/index.ts";

/* These utility functions are used by multiple tuning strategy files (hulu, sling, fox, etc.) and the channel selection coordinator. They live in this shared module
 * to avoid circular imports - the coordinator imports tuning strategy modules, and those modules need these utilities. Placing the utilities in the coordinator would
 * create a circular dependency (coordinator -> tuning -> coordinator).
 */

/* Per-page record of install keys that have already run. A WeakMap keyed by Page lets the entry set be garbage-collected automatically when the page is closed and
 * its reference is released, so there is no manual cleanup and no leak across browser sessions. The value is a Set of string keys so a single page can host several
 * independent one-time installs (e.g., a request-interception listener under one key and an evaluateOnNewDocument injection under another) without collision.
 */
const installedKeysByPage = new WeakMap<Page, Set<string>>();

/**
 * Runs an install action at most once per (page, key) pair. Tuning strategies are re-entered on the same page during recovery re-tunes (tuneToChannel is the single
 * source of truth for both initial setup and recovery), so install steps that are global to a page - registering a `page.on("request")` listener, enabling request
 * interception, or installing an `evaluateOnNewDocument` script - would otherwise accumulate duplicate registrations every time the strategy resolves a direct URL.
 * This helper is the single source of truth for that idempotency: the first call for a given (page, key) awaits and runs `fn`, and every subsequent call for the
 * same pair is a no-op. The key is recorded before awaiting `fn` so concurrent callers on the same page do not both pass the guard.
 * @param page - The Puppeteer page the install targets. Used as the WeakMap key so the per-page record is released when the page is closed.
 * @param key - A stable identifier for the install action (e.g., "channelmap-request-intercept", "fetch-interceptor"). Distinct keys install independently.
 * @param fn - The install action to run on the first call for this (page, key) pair. May be synchronous or return a Promise; it is awaited and its result discarded,
 *   so callers can pass a value-returning call (e.g., page.evaluateOnNewDocument) directly without an extra void-discard wrapper.
 * @returns True if the install ran on this call (first time for the pair), false if it was already installed and was skipped.
 */
export async function installOncePerPage(page: Page, key: string, fn: () => unknown): Promise<boolean> {

  let installedKeys = installedKeysByPage.get(page);

  if(!installedKeys) {

    installedKeys = new Set<string>();
    installedKeysByPage.set(page, installedKeys);
  }

  if(installedKeys.has(key)) {

    return false;
  }

  // Record the key before awaiting the install so that a second call racing on the same page sees the key already present and short-circuits rather than running a
  // duplicate install. The install itself is allowed to fail without un-recording the key - a failed one-time install should not silently retry on every re-tune.
  installedKeys.add(key);

  await fn();

  return true;
}

/* Per-page record of the current evaluateOnNewDocument script identifier for each install key. Lets installOrReplaceOnNewDocument remove the previously-installed
 * script before adding a fresh one, so a re-tune that needs different baked-in arguments runs exactly one interceptor carrying current values rather than an
 * accumulating stack. WeakMap-keyed by Page so the record is released when the page is closed.
 */
const newDocumentScriptIdsByPage = new WeakMap<Page, Map<string, string>>();

/**
 * Installs an evaluateOnNewDocument script under a (page, key) identity, first removing any script previously installed under the same identity. Use this - rather
 * than installOncePerPage - for in-page interceptors whose baked-in arguments can legitimately change between tunes on the same page. The canonical case is Hulu's
 * fetch interceptor, whose UUID/EAB tokens drift from a cold first tune to a warm recovery re-tune: installOncePerPage would freeze the first (cold) arguments and
 * starve the recovery tune, while re-installing without removal would stack duplicate scripts. This helper keeps exactly one live script carrying current arguments,
 * fixing both. installOncePerPage remains the right tool for installs that are genuinely safe to repeat - a request listener that reads live state, or a script
 * whose arguments never change. `install` must perform the page.evaluateOnNewDocument call and return its result so the script
 * identifier can be tracked for the next removal.
 * @param page - The Puppeteer page the script targets. Used as the WeakMap key so the per-page record is released when the page is closed.
 * @param key - A stable identifier for this interceptor (e.g., "fetch-interceptor"). Distinct keys are tracked independently.
 * @param install - Performs the evaluateOnNewDocument install and returns its NewDocumentScriptEvaluation, whose identifier is recorded for the next removal.
 */
export async function installOrReplaceOnNewDocument(page: Page, key: string, install: () => Promise<NewDocumentScriptEvaluation>): Promise<void> {

  let idsForPage = newDocumentScriptIdsByPage.get(page);

  if(!idsForPage) {

    idsForPage = new Map<string, string>();
    newDocumentScriptIdsByPage.set(page, idsForPage);
  }

  // Remove the previously-installed script for this identity, if any, so the page runs exactly one interceptor rather than an accumulating stack. Removal is best-
  // effort: a stale identifier (the page already navigated past it, or the script was never evaluated) is benign and must not abort the fresh install.
  const priorId = idsForPage.get(key);

  if(priorId !== undefined) {

    try {

      await page.removeScriptToEvaluateOnNewDocument(priorId);
    } catch {

      // The prior script is already gone; there is nothing to remove.
    }
  }

  const { identifier } = await install();

  idsForPage.set(key, identifier);
}

/**
 * Clicks pre-computed viewport coordinates using a coordinate-based mouse click, after a 200ms settle delay that lets lazy-loaded content and animations settle
 * before the click is dispatched. The caller is responsible for scrolling the target into view and computing its coordinates. Coordinate-based clicking generates
 * the full pointer event chain (pointerdown -> mousedown -> pointerup -> mouseup -> click), which is more reliable for React/SPA sites than synthetic DOM clicks.
 * @param page - The Puppeteer page object.
 * @param target - The x/y coordinates to click.
 * @returns Always true; the click is dispatched unconditionally and no failure is detectable from page.mouse.click.
 */
export async function scrollAndClick(page: Page, target: ClickTarget): Promise<boolean> {

  // Brief settle delay so any animations or lazy-loaded content from the caller's prior scroll settle before the click is dispatched.
  await delay(200);

  // Click the target coordinates to switch to the channel.
  await page.mouse.click(target.x, target.y);

  return true;
}

/**
 * Normalizes a channel name for case-insensitive, whitespace-tolerant comparison. Trims leading and trailing whitespace, collapses internal whitespace sequences
 * (including non-breaking spaces, tabs, and other Unicode whitespace matched by \s) into a single regular space, and lowercases. This handles data-testid values
 * with trailing spaces, double spaces, or non-breaking space characters that would otherwise cause exact match failures.
 * @param name - The raw channel name to normalize.
 * @returns The normalized channel name.
 */
export function normalizeChannelName(name: string): string {

  return name.trim().replace(/\s+/g, " ").toLowerCase();
}

/**
 * Resolves the CSS selector for finding a channel element. Interpolates the {channel} placeholder in the profile's matchSelector template with the channelSelector
 * value. When matchSelector is not configured, falls back to image URL matching for backward compatibility.
 * @param profile - The resolved profile with channel selection configuration.
 * @returns The CSS selector string.
 */
export function resolveMatchSelector(profile: ChannelSelectionProfile): string {

  const template = profile.channelSelection.matchSelector;

  if(template) {

    return template.replaceAll("{channel}", profile.channelSelector);
  }

  // Default to image URL slug matching for backward compatibility with profiles that don't specify matchSelector.
  return "img[src*=\"" + profile.channelSelector + "\" i]";
}

/**
 * Logs available channel names from a provider's guide grid when channel selection fails. Produces an actionable log message listing channel names that users can
 * use as `channelSelector` values in user-defined channels. When `presetSuffix` is provided, channels already covered by builtin preset definitions are filtered
 * out so users see only channels that require manual configuration. When omitted (small channel sets like Fox or HBO), all channels are logged unfiltered.
 * @param options - Diagnostic dump configuration.
 * @param options.additionalKnownNames - Extra names to exclude from the filtered list (e.g., CHANNEL_ALTERNATES values for YTTV).
 * @param options.availableChannels - Sorted list of channel names discovered in the guide grid.
 * @param options.channelName - The channelSelector value that failed to match, for the log message.
 * @param options.guideUrl - The URL of the provider's guide page, included in the log message so users know what to set as the channel URL.
 * @param options.presetSuffix - Key suffix to filter preset channels (e.g., "-yttv", "-hulu"). Omit for small unfiltered channel sets.
 * @param options.providerName - Human-readable provider name for the log message (e.g., "YouTube TV", "Hulu").
 */
export function logAvailableChannels(options: {
  additionalKnownNames?: string[];
  availableChannels: string[];
  channelName: string;
  guideUrl: string;
  presetSuffix?: string;
  providerName: string;
}): void {

  const { additionalKnownNames, availableChannels, channelName, guideUrl, presetSuffix, providerName } = options;

  if(availableChannels.length === 0) {

    return;
  }

  let filteredChannels: string[];
  let countLabel: string;

  if(presetSuffix) {

    // Collect all channelSelector values from preset channels with this suffix, lowercased for case-insensitive comparison.
    const knownSelectors: string[] = Object.entries(CHANNELS)
      .filter(([key]) => key.endsWith(presetSuffix))
      .map(([ , ch ]) => (ch.channelSelector ?? "").toLowerCase())
      .filter((s) => s.length > 0);

    // Include additional known names (e.g., CHANNEL_ALTERNATES values for YTTV) so those are also filtered out.
    if(additionalKnownNames) {

      for(const name of additionalKnownNames) {

        knownSelectors.push(name.toLowerCase());
      }
    }

    // Filter to channels not matched by any known selector. A channel is "covered" if a preset would find it via exact match (with parenthetical suffix stripped)
    // or prefix+digit match. This mirrors the strategy's own matching tiers so users see only channels that genuinely need manual configuration.
    filteredChannels = availableChannels.filter((name) => {

      const lower = name.toLowerCase();
      const stripped = lower.replace(/ \(.*\)$/, "");

      return !knownSelectors.some((sel) => {

        return (stripped === sel) ||
          (lower.startsWith(sel + " ") && (lower.length > sel.length + 1) && (lower.charCodeAt(sel.length + 1) >= 48) && (lower.charCodeAt(sel.length + 1) <= 57));
      });
    });

    countLabel = "uncovered (" + String(filteredChannels.length) + " of " + String(availableChannels.length) + ")";
  } else {

    // No preset suffix - log all available channels unfiltered. Used for small channel sets (Fox, HBO) where the full list is actionable without filtering.
    filteredChannels = availableChannels;
    countLabel = String(filteredChannels.length);
  }

  if(filteredChannels.length === 0) {

    return;
  }

  LOG.warn("Channel \"%s\" not found in %s guide. Create a user-defined channel with one of the names below as the Channel Selector and %s as the URL. " +
    "Available channels (%s): %s.", channelName, providerName, guideUrl, countLabel, filteredChannels.join(", "));
}

// Number of consecutive empty discoveries before attempting site data recovery via CDP.
const EMPTY_DISCOVERY_RECOVERY_THRESHOLD = 3;

/**
 * Tracks consecutive guide page loads that discover zero channels for one provider, and reports when that streak warrants recovery. Each strategy holds its own
 * instance, so one provider's degradation never reads as another's.
 */
export interface EmptyDiscoveryGuard {

  // Records one guide load that discovered zero channels and warns with the running streak length. Returns true once the streak has reached the recovery
  // threshold, which is the caller's signal to attempt recovery. It keeps returning true for every further consecutive empty load, so a guide that stays
  // degraded is retried rather than being abandoned after a single attempt.
  readonly recordEmpty: () => boolean;

  // Clears the streak. Called on any successful discovery and on browser restart, both of which resolve the degraded state the streak stands for.
  readonly reset: () => void;
}

/**
 * Creates a recovery guard that tracks consecutive guide page loads that discover zero channels for one provider. The count lives in the returned guard's
 * closure rather than in module state, so every strategy that needs one holds an independent instance and no provider's streak can be advanced or cleared by
 * another's.
 * @param providerName - Human-readable provider name for the warning message (e.g., "Spectrum TV", "YouTube TV").
 * @returns A guard exposing recordEmpty and reset.
 */
export function createEmptyDiscoveryGuard(providerName: string): EmptyDiscoveryGuard {

  let consecutiveEmptyDiscoveries = 0;

  const recordEmpty = (): boolean => {

    consecutiveEmptyDiscoveries++;

    LOG.warn("%s guide loaded but no channels were discovered (%s consecutive). The guide may be in a degraded state.", providerName,
      consecutiveEmptyDiscoveries);

    return consecutiveEmptyDiscoveries >= EMPTY_DISCOVERY_RECOVERY_THRESHOLD;
  };

  const reset = (): void => {

    consecutiveEmptyDiscoveries = 0;
  };

  return { recordEmpty, reset };
}

/**
 * Reports whether clearing the given origin's cached site data is safe right now - that is, whether the recovering page is the only live page sitting on that
 * origin. Storage.clearDataForOrigin reaches CacheStorage and service-worker registrations, which the specification scopes to the origin rather than to a page,
 * so no page-scoped variant exists to fall back on: the choice is clearing for every page on the origin or for none. A sibling stream losing its caching layers
 * mid-playback can degrade it or push it into its own recovery, turning one stream's empty guide into a service-wide event, so a shared origin means no clear.
 * An enumeration that fails outright gets the same answer: when the open pages cannot be read, protecting healthy streams outranks maximizing this stream's
 * recovery odds. Both refusals are logged, because a recovery that quietly did less than the surrounding messages imply would be hard to read in the field.
 * @param page - The recovering page. Excluded from the sibling count by object identity rather than by URL, since two pages can legitimately share a URL and
 *   only this one is the page whose caches the recovery is meant to discard.
 * @param options - Naming for the check and its messages.
 * @param options.origin - The origin the clear would target.
 * @param options.providerName - Human-readable provider name for the refusal messages.
 * @returns True when no other live page shares the origin and the clear can proceed.
 */
async function originClearIsSafe(page: Page, options: { origin: string; providerName: string }): Promise<boolean> {

  const { origin, providerName } = options;

  try {

    const originToMatch = new URL(origin).origin;
    const openPages = await page.browser().pages();

    let siblings = 0;

    for(const other of openPages) {

      if(other === page) {

        continue;
      }

      try {

        if(new URL(other.url()).origin === originToMatch) {

          siblings++;
        }
      } catch {

        // A page whose URL does not parse - an about:blank tab, or a target torn down while it was being read - has no origin to share, so it never counts
        // against the clear.
      }
    }

    if(siblings > 0) {

      LOG.warn("Not clearing %s site data - other open pages share the origin (%s). Reloading the guide only.", providerName, siblings);

      return false;
    }

    return true;
  } catch {

    LOG.warn("Not clearing %s site data - the open pages could not be enumerated. Reloading the guide only.", providerName);

    return false;
  }
}

/**
 * Recovers a degraded provider guide by clearing its cached site data via CDP, reloading the guide page, and re-running the provider's own discovery. This
 * targets the failure mode where the guide grid container renders but channel entries are never populated, which stale caching layers produce. Cookies and
 * login session state survive: only the storage types the caller names are cleared, so recovery never forces re-authentication. The clear is skipped entirely
 * when another live page shares the origin, because the storage it discards belongs to the origin rather than to one page; recovery then degrades to the
 * reload, which is page-scoped and leaves the sibling pages alone. Every failure along the way is
 * logged and yields an empty array, leaving the caller's existing empty-guide handling to decide what happens next. The type parameter is the caller's own raw
 * channel shape - the routine counts the discovered entries but never inspects them, so each provider keeps its discovery type end to end.
 * @param page - The Puppeteer page object positioned on the provider's guide.
 * @param options - Provider-specific recovery parameters.
 * @param options.discover - The provider's guide discovery function, re-run on the reloaded page.
 * @param options.origin - Origin whose cached site data is cleared (e.g., "https://tv.youtube.com").
 * @param options.providerName - Human-readable provider name for the log messages (e.g., "Spectrum TV", "YouTube TV").
 * @param options.reloadUrl - Guide URL to reload once the clear succeeds.
 * @param options.storageTypes - Storage.clearDataForOrigin storage types to clear (e.g., "cache_storage,service_workers").
 * @param options.waitSelector - Selector whose appearance confirms the guide grid rendered after the reload.
 * @returns Discovered channels after recovery, or an empty array if recovery failed.
 */
export async function attemptGuideRecovery<T>(page: Page, options: {
  discover: (page: Page) => Promise<T[]>;
  origin: string;
  providerName: string;
  reloadUrl: string;
  storageTypes: string;
  waitSelector: string;
}): Promise<T[]> {

  const { discover, origin, providerName, reloadUrl, storageTypes, waitSelector } = options;

  /* Clear the caching layers the caller named for this origin, but only when this page has the origin to itself - originClearIsSafe carries that judgment and
   * logs its own refusals. The cleared layers repopulate on reload; cookies and login session state are deliberately preserved to avoid forcing
   * re-authentication.
   *
   * The session binding lives outside the try so the finally can release it no matter which way the block exits. Both the creation and the send sit inside the
   * try because a browser that refuses either one is a recoverable condition the caller handles through the empty return, not something to propagate: nothing
   * downstream of the strategy catches it. The release is fire-and-forget - awaiting a detach on a dead connection could hang the failure path, and a rejection
   * here is expected teardown noise rather than an outcome, which the catch above already owns.
   */
  if(await originClearIsSafe(page, { origin, providerName })) {

    LOG.warn("Clearing %s cached site data to recover from empty guide.", providerName);

    let client: CDPSession | undefined;

    try {

      client = await page.createCDPSession();

      await client.send("Storage.clearDataForOrigin", { origin, storageTypes });
    } catch(error) {

      LOG.warn("Failed to clear %s site data: %s.", providerName, formatError(error));

      return [];
    } finally {

      if(client) {

        void client.detach().catch(() => { /* Session may already be detached. */ });
      }
    }
  }

  // Reload the guide page with fresh state. This runs on both paths: a reload is page-scoped, so it is safe even when the clear was skipped, and it is the half
  // of recovery that costs sibling streams nothing.
  try {

    await page.goto(reloadUrl, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "load" });
  } catch(error) {

    LOG.warn("Failed to reload %s guide after clearing site data: %s.", providerName, formatError(error));

    return [];
  }

  // Wait for the guide grid to render after reload.
  try {

    await page.waitForSelector(waitSelector, { timeout: CONFIG.streaming.videoTimeout });
  } catch {

    LOG.warn("%s guide grid did not load after clearing site data.", providerName);

    return [];
  }

  // Re-attempt channel discovery on the reloaded page.
  const channels = await discover(page);

  if(channels.length > 0) {

    LOG.info("%s guide recovery succeeded - discovered %s channels after clearing site data.", providerName, channels.length);
  } else {

    LOG.warn("%s guide still empty after clearing site data.", providerName);
  }

  return channels;
}
