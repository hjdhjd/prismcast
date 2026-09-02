/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * precaching.ts: Service channel lineup precaching for PrismCast.
 */
import type { DiscoveredChannel, Nullable, ProviderModule, ResolvedSiteProfile } from "../types/index.ts";
import { LOG, extractDomain, formatError, startTimer, timeoutSignal } from "../utils/index.ts";
import { clearDomainAuthRequirement, getDomainAuthState, markDomainAuth, markDomainAuthRequired } from "../config/health.ts";
import { createDiscoveryPage, emulateLayoutSurface, getCurrentBrowser, isGracefulShutdown, registerManagedPage, syncWindowVisibility,
  unregisterManagedPage } from "./index.ts";
import { getPersistedLineup, persistProviderLineup } from "../config/providerLineups.ts";
import { getProviderBySlug, getProvidersForDomain } from "./channelSelection.ts";
import type { BlockedPageClassification } from "./blockedPage.ts";
import { CONFIG } from "../config/index.ts";
import type { Page } from "puppeteer-core";
import type { PersistedLineupChannel } from "../config/providerLineups.ts";
import { classifyBlockedPage } from "./blockedPage.ts";
import { getProfileForUrl } from "../config/profiles.ts";
import { isLoginModeActive } from "./login.ts";
import { startOverlayHandling } from "./consent.ts";
import { waitWithSignal } from "homebridge-plugin-utils";

/* Precaching discovers channel lineups for selected services at startup so that even the first tune benefits from cached lineup data. Each service is precached
 * sequentially - discovery opens a browser page in a window of its own and navigates to a heavy SPA, so running all services concurrently would stress CPU and
 * GPU on resource-constrained systems. The HTTP server starts immediately; precaching begins in the background after a brief delay.
 *
 * Precaching is triggered from launchBrowser() in browser/index.ts. This covers both initial server startup and browser crash recovery (where all caches are cleared).
 * Each service has its own try/catch - one failure does not stop the rest. The browser reference is obtained per-service via getCurrentBrowser() so that a browser
 * crash between services is handled transparently (the next service gets the relaunched browser).
 *
 * This module also owns the discovery-outcome policy (recordDiscoveryOutcome): the single source of truth for how a completed discovery walk translates into domain
 * auth state and a persisted channel lineup, shared by the precache cycle here and the /services/:slug/channels endpoint. The routes layer never calls a health
 * mutator or the lineup store directly - it calls the recorder, which does. The page session itself is owned by withProviderGuidePage: the single guarded-page
 * primitive both the precache cycle and that endpoint walk their guides through, and the one place the empty-walk retry policy lives, so no provider carries a
 * retry of its own.
 */

// Delay in milliseconds before precaching begins after browser launch. This gives the browser time to settle after initialization.
const PRECACHE_DELAY = 5000;

/* Delay in milliseconds before the services a cycle could not settle are re-attempted. Five minutes puts the second pass well past the contention a boot creates -
 * the browser launch, the first tunes, the DVR's own channel scan - which is the likeliest reason a provider's lazy content never appeared inside its walk. A
 * service whose walk did not settle - it came back empty, or it was stopped at its budget - gets exactly one such pass: unsettled again on a quiet system, it has
 * a standing problem that another walk will not solve, and a repeating attempt would keep waking the browser for it indefinitely. A service deferred because a
 * login session was on screen is the separate case: its walk never ran, so the pass re-arms itself for it until the session ends rather than spending its one
 * attempt on a window the user is working in.
 */
const PRECACHE_RETRY_DELAY = 300000;

/* The fraction of the saved lineup a walk has to reach before the store is allowed to replace that lineup with it. A non-empty walk returning fewer channels
 * than this fraction of what is already on file is treated as an incomplete read of the guide: a slice is replaced wholesale, so one screenful read off a
 * virtualized guide - twelve rows where the guide carries a hundred and twenty-nine - would otherwise become the whole saved lineup. A quarter sits well below
 * any plausible change a provider makes to its own channel list and well above what that truncation produces, so a count alone tells the two apart without a
 * provider-declared completeness signal.
 *
 * Below the threshold the walk is never accepted automatically, and the reason is what a count can and cannot say: it cannot tell a truncated read from a
 * provider that genuinely shrank, and a second walk agreeing with the first settles nothing either, because a one-screenful truncation reads the same twelve
 * rows every time. So a provider that really did cut its lineup by more than this keeps its extra channel rows until a walk lands at or above the threshold, an
 * on-demand discovery from the channel table refreshes it, or the saved file is corrected by hand. The warn line the guard emits carries both counts and names
 * that outcome.
 */
const SUSPECT_WALK_RATIO = 0.25;

/* The ceiling on a single discovery walk, in milliseconds. Measured walks finish in a few seconds to about seventeen, and an empty walk's reload-and-retry gets a
 * budget of its own, so a minute is far past anything a healthy walk needs: a walk still running at that point is wedged on a page that is not going to answer.
 * The ceiling is a failure to report rather than a delay anyone pays, and it sits deliberately low because the alternative is worse - the stale-page sweep is a
 * safety net for pages nothing owns, not this walk's timer, so a walk without a ceiling of its own has none at all. Failing fast hands the service to the
 * deferred re-attempt, which tries again on a settled system.
 */
const DISCOVERY_WALK_TIMEOUT = 60000;

/**
 * A discovery walk stopped at its budget. The precache cycle and the deferred re-attempt each treat a wedged walk differently from every other discovery failure,
 * so the lapse carries its own type rather than making either of them read a message.
 */
export class DiscoveryWalkTimeoutError extends Error {

  constructor(label: string, timeoutMs: number) {

    super("The channel discovery walk for " + label + " did not finish within " + String(timeoutMs / 1000) + " seconds.");

    this.name = "DiscoveryWalkTimeoutError";
  }
}

// Guard flag preventing overlapping precache cycles. Set to true before the cycle starts, cleared through releasePrecacheGuard in a finally block.
let precacheInProgress = false;

// Handle for the scheduled precache cycle, tracked so a graceful shutdown can cancel it before it fires. Null when no cycle is pending.
let precacheTimer: Nullable<ReturnType<typeof setTimeout>> = null;

/* The pending deferred re-attempt: the services still to re-walk and the timer that will do it. One value rather than two fields, so arming, cancelling, and
 * firing each move the whole thing at once - a slug list with no timer behind it, or a timer whose slugs were cleared, is not a state this can reach.
 */
let deferredRetry: Nullable<{ slugs: string[]; timer: ReturnType<typeof setTimeout> }> = null;

/* Whether a full precache cycle was requested while the single-flight guard was held. The request always comes from a browser relaunch, which cleared every
 * provider cache, so the run holding the guard is walking guides for a browser that no longer exists and its result is worth nothing - dropping the request would
 * leave the new browser with no lineups at all. Whoever releases the guard runs it; releasePrecacheGuard is where that happens, and it is the only place.
 */
let fullCycleRequested = false;

/**
 * Cancels a pending deferred re-attempt and drops its state as one unit. A no-op when nothing is pending.
 */
function clearDeferredRetry(): void {

  if(deferredRetry) {

    clearTimeout(deferredRetry.timer);

    deferredRetry = null;
  }
}

/**
 * Releases the single-flight guard and honors any full-cycle request that arrived while it was held.
 *
 * The order is the whole point. The flag is cleared first, so the reentrant startPrecaching below finds a free guard and schedules. Honoring first would have that
 * call see the guard still held and record the very request being honored, which is how a browser relaunch's cycle would go round forever without ever running.
 * Every path that takes the guard - the cycle, the deferred re-attempt, and the post-login revalidation - releases it here, so the hand-off has one home rather
 * than a copy at each site.
 * @param deps - The injected dependencies, handed to the cycle this may start.
 */
function releasePrecacheGuard(deps: PrecachingDeps): void {

  precacheInProgress = false;

  if(!fullCycleRequested) {

    return;
  }

  fullCycleRequested = false;

  startPrecaching(deps);
}

/* PrecachingDeps is the browser + provider-registry surface the precache cycle composes on: the shared-browser accessors, the discovery-page creator and the
 * page bookkeeping around it, the shutdown gate, the window-visibility sync, the provider lookups, the discovery-phase overlay-poll launcher, and the
 * durable-lineup read and write the discovery-outcome policy performs.
 * It is injected as a default parameter threaded through the module's functions so a test can substitute stubs at the same PrecachingDeps boundary - no loader
 * mock - while production uses the real defaultPrecachingDeps built from the functions this module already imports. startOverlayHandling belongs here for the same
 * reason the browser accessors do: run for real it drives a poll against the page, so a test injects a recording stub to observe the discovery poll's phase and
 * abort timing without a live poll. The lineup store's members belong here for the same reason again: run for real they touch a file, so a test observes the
 * write - and injects a failing one - and states the saved lineup the plausibility guard reads against, both at this boundary. It is kept as an in-module const,
 * NOT a separate *.context.ts adapter: browser/index.ts imports startPrecaching and precaching.ts imports these accessors, so a separate adapter file would sit
 * inside that value-import cycle, whereas the in-module const adds no new import edge.
 * This is the collaborator-injection form of the Clock port (utils/clock.ts).
 */
export interface PrecachingDeps {

  readonly createDiscoveryPage: typeof createDiscoveryPage;
  readonly emulateLayoutSurface: typeof emulateLayoutSurface;
  readonly getCurrentBrowser: typeof getCurrentBrowser;
  readonly getPersistedLineup: typeof getPersistedLineup;
  readonly getProviderBySlug: typeof getProviderBySlug;
  readonly getProvidersForDomain: typeof getProvidersForDomain;
  readonly isGracefulShutdown: typeof isGracefulShutdown;
  readonly persistProviderLineup: typeof persistProviderLineup;
  readonly registerManagedPage: typeof registerManagedPage;
  readonly startOverlayHandling: typeof startOverlayHandling;
  readonly syncWindowVisibility: typeof syncWindowVisibility;
  readonly unregisterManagedPage: typeof unregisterManagedPage;
}

export const defaultPrecachingDeps: PrecachingDeps = {

  createDiscoveryPage,
  emulateLayoutSurface,
  getCurrentBrowser,
  getPersistedLineup,
  getProviderBySlug,
  getProvidersForDomain,
  isGracefulShutdown,
  persistProviderLineup,
  registerManagedPage,
  startOverlayHandling,
  syncWindowVisibility,
  unregisterManagedPage
};

/**
 * Starts the precaching cycle if services are configured. Called from launchBrowser() after the browser is ready. If no services are selected, a shutdown is in
 * progress, or a precache cycle is already in progress, returns immediately. The actual work is scheduled via setTimeout to avoid blocking browser launch.
 * @param deps - The injected browser and provider-registry dependencies; defaults to defaultPrecachingDeps.
 */
export function startPrecaching(deps: PrecachingDeps = defaultPrecachingDeps): void {

  if(CONFIG.channels.precacheServices.length === 0) {

    return;
  }

  // Never schedule a precache during graceful shutdown. launchBrowser() can be reached during teardown; without this guard the scheduled cycle would fire after the
  // browser is closed and relaunch Chrome.
  if(deps.isGracefulShutdown()) {

    return;
  }

  if(precacheInProgress) {

    // Record the request rather than dropping it. This call is a browser relaunch's, and the run currently holding the guard is walking guides for a browser whose
    // caches were just cleared out from under it - so the request is the one worth keeping, and whoever releases the guard runs it.
    fullCycleRequested = true;

    LOG.debug("precache", "Precache deferred: already in progress.");

    return;
  }

  // Set the guard before scheduling so that a second call during the delay window (e.g., rapid browser crash + relaunch) sees the flag and defers.
  precacheInProgress = true;

  // A full cycle supersedes any pending deferred re-attempt: it walks every configured service, the empty ones included, and letting both run would put two passes
  // over the same guides in contention for one browser.
  clearDeferredRetry();

  // Schedule the precache cycle after a brief delay to let the browser settle. The handle is tracked so stopPrecaching() can cancel it if shutdown begins during the
  // delay window; the ref is cleared when the timer fires since it is then spent.
  precacheTimer = setTimeout(() => {

    precacheTimer = null;
    void runPrecacheCycle(deps);
  }, PRECACHE_DELAY);
}

/**
 * Cancels every scheduled precache - the startup cycle and the deferred re-attempt alike - and clears the in-progress guard. Called during graceful shutdown so
 * nothing scheduled can fire after the browser has been closed. Safe to call when nothing is pending.
 *
 * The guard is cleared directly rather than through releasePrecacheGuard, and the hand-off flag with it: a shutdown must not honor a pending full-cycle request,
 * which is exactly what releasing through the hand-off would do.
 */
export function stopPrecaching(): void {

  if(precacheTimer) {

    clearTimeout(precacheTimer);
    precacheTimer = null;
  }

  clearDeferredRetry();

  fullCycleRequested = false;
  precacheInProgress = false;
}

/**
 * Records the consequences of a completed channel discovery: the domain auth state it proves, and the durable lineup it produced. This is the single source of
 * truth for discovery-outcome policy, consumed by both the precache cycle (via precacheService) and the /services/:slug/channels endpoint - the routes layer never
 * calls a health mutator or the lineup store directly. The two halves are bundled deliberately: a completed walk is one event, and what it proves about the domain
 * and what it found on the guide are that event's transient and durable records.
 *
 * An empty result classifies the still-open page: a confirmed authentication wall marks the provider's domain needs-sign-in; a consent overlay and the unknown
 * classification change no state (an unexplained empty walk is not evidence of anything). A non-empty result that the provider's validatePrecache accepts (or that
 * needs no validation) marks the domain verified and persists the lineup, unless it holds too small a fraction of the lineup already on file to be a complete read
 * of the guide, in which case the mark still lands and the saved lineup stands; a non-empty result the validator rejects proves the wall is gone but not that paid
 * access exists, so it clears a standing needs-sign-in entry back to unknown, changes nothing else, and persists nothing - a rejected walk is not a lineup the
 * store can safely replace a slice with.
 * @param provider - The provider whose discovery completed.
 * @param channels - The discovered channels (possibly empty).
 * @param page - The still-open discovery page, inspected only when the result is empty.
 * @param deps - The injected dependencies; the saved-lineup read and the lineup write both run through this port.
 * @param classification - A classification the caller already performed against the page state it wants recorded. When omitted, the still-open page is classified
 *   here, which is what every caller that has not already looked does.
 * @returns A promise that resolves once any classification and state recording completes.
 */
export async function recordDiscoveryOutcome(provider: ProviderModule, channels: DiscoveredChannel[], page: Page, deps: PrecachingDeps,
  classification?: BlockedPageClassification): Promise<void> {

  const domain = extractDomain(provider.guideUrl);

  if(channels.length === 0) {

    const outcome = classification ?? await classifyBlockedPage(page, { indicators: provider.authWallIndicators, requestedUrl: provider.guideUrl });

    switch(outcome.kind) {

      case "authWall": {

        markDomainAuthRequired(domain);
        LOG.warn("%s returned no channels because the provider is presenting an authentication wall (%s). Sign in from the channel table's login icon.",
          provider.label, outcome.evidence);

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

    /* A walk far smaller than the lineup already on file reads as an incomplete pass over the guide rather than as a statement that the provider cut its channel
     * list, so the durable write is withheld and what is on file stands. The order here is the contract: the domain is marked above regardless, because channels
     * came back and that proves access whatever the count says, and the live cache this walk already filled is untouched, so the session still tunes from what
     * the walk did reach. Only the write to the store is given up.
     */
    const saved = deps.getPersistedLineup(provider.slug);

    if((saved !== null) && (channels.length < (saved.length * SUSPECT_WALK_RATIO))) {

      LOG.warn("%s returned %d channels where its saved lineup holds %d, so the walk is treated as incomplete and the saved lineup is kept.", provider.label,
        channels.length, saved.length);

      return;
    }

    /* Persist what the walk found so the lineup outlives this browser session. The provider states its own durable shape through exportDurableLineup - which
     * fields survive a session is provider knowledge, not the recorder's - and a provider with nothing durable to add contributes the channel identities the walk
     * returned.
     *
     * Every condition a write has to pass exists because of the store's replace semantics: a slice is replaced wholesale, so anything short of a trustworthy full
     * statement of the lineup could shrink a fuller slice written earlier, dropping channels out of the cold-listing fallback and taking their durable watch URLs
     * with them. The provider's own validator is the first, and a walk it judges untrustworthy never reaches this block at all; the plausibility guard above is
     * the second, and it withholds a walk too small to be a complete read of the guide. Verify-on-use covers staleness, not a lineup the provider has already
     * rejected or a read that plainly did not finish.
     *
     * The write is fire-and-forget by design: it never throws, and making the discovery endpoint's response wait on a file write would charge the user for a
     * durability guarantee they did not ask for.
     */
    const lineup: PersistedLineupChannel[] = provider.exportDurableLineup?.() ??
      channels.map((channel) => ({ channelSelector: channel.channelSelector, name: channel.name }));

    // The real write absorbs its own failures, so the trailing catch is about the port rather than the store: whatever a caller injects here, this call site can
    // never become a rejection source that takes down the walk it belongs to. The health-state flush guards its own fire-and-forget write the same way.
    void deps.persistProviderLineup(provider.slug, lineup).catch((error: unknown) => {

      LOG.debug("precache", "The channel lineup write for %s did not complete: %s.", provider.label, formatError(error));
    });

    return;
  }

  // The validator rejected: the wall is gone (channels came back) but paid access is unproven. Clear a standing needs-sign-in entry back to unknown so the red
  // state never outlives the wall it reported - clearDomainAuthRequirement is a no-op unless the domain is currently flagged, so verified state is never touched.
  clearDomainAuthRequirement(domain);
}

/**
 * Runs a provider's discovery walk under a deadline, and cancels the walk when that deadline lapses rather than merely giving up on waiting for it.
 *
 * The cancellation is the whole point. A lapse closes the page, which throws whatever Puppeteer operation the walk is sitting on and unwinds the walk itself -
 * the same mechanism the guarded session uses for a caller's abort. A bound that only stopped the wait would leave the walk driving a page the session has moved
 * on from. The lapse is held in a local and handed to the timeout as its abort reason, so the rejection carries that exact error object; every other rejection
 * travels through untouched. Each call gets its own budget, which is what lets an empty walk's retry be bounded exactly like the first attempt.
 * @param provider - The provider whose walk to run.
 * @param page - The guide page the walk runs against, closed if the deadline lapses.
 * @returns The discovered channels (possibly empty).
 * @throws DiscoveryWalkTimeoutError when the walk outlives its budget, and whatever the walk itself rejected with otherwise.
 */
async function walkWithDeadline(provider: ProviderModule, page: Page): Promise<DiscoveredChannel[]> {

  const lapse = new DiscoveryWalkTimeoutError(provider.label, DISCOVERY_WALK_TIMEOUT);
  const deadline = timeoutSignal(DISCOVERY_WALK_TIMEOUT, lapse);

  deadline.signal.addEventListener("abort", () => {

    void page.close().catch(() => { /* Page may already be closed. */ });
  }, { once: true });

  try {

    return await waitWithSignal(provider.discoverChannels(page), deadline.signal);
  } finally {

    deadline.cancel();
  }
}

/**
 * Options for withProviderGuidePage().
 */
interface WithProviderGuidePageOptions {

  /* Runs after the discovery walk completes, with the still-open (and now poll-quiet) page and the discovered channels. Used to record the discovery outcome while
   * the page still holds its evidence.
   *
   * The third argument carries a classification the session already performed and whose page state is the one worth recording - which happens on exactly one path,
   * where an empty walk classified as blocked and the session declined to reload. Every other path leaves it absent, and the recorder classifies the page in front
   * of it, which is what keeps a retried walk's outcome describing the page the retry actually saw.
   */
  readonly afterWalk?: (page: Page, channels: DiscoveredChannel[], classification?: BlockedPageClassification) => Promise<void>;

  // Aborts the walk. When it fires, the page is closed, which throws any in-progress Puppeteer operation and propagates the cancellation through discoverChannels.
  readonly signal?: AbortSignal;
}

/**
 * The outcome of the empty-walk retry.
 */
interface EmptyWalkRetryResult {

  // What the second walk found, or the first walk's empty result when the retry was declined.
  channels: DiscoveredChannel[];

  // The first walk's classification, present only when the retry was declined and that classification is therefore the one describing the page the outcome
  // recorder will act on. Absent when a second walk ran, because the recorder must then classify the reloaded page rather than the one before it.
  classification?: BlockedPageClassification;
}

/**
 * Options for retryAfterEmptyWalk().
 */
interface RetryAfterEmptyWalkOptions {

  // The injected browser and overlay-poll dependencies.
  readonly deps: PrecachingDeps;

  // The still-open page the empty walk ran against.
  readonly page: Page;

  // The resolved site profile for the guide URL, handed to the retry's own overlay poll.
  readonly profile: ResolvedSiteProfile;

  // The provider whose walk came back empty.
  readonly provider: ProviderModule;
}

/**
 * Gives an empty discovery walk one more chance, when the page it left behind says the emptiness is unexplained.
 *
 * The classification comes first and decides everything. A confirmed authentication wall or a standing consent overlay explains the empty result completely, and
 * neither is something a reload can fix - so those skip the retry and hand their classification back, because the evidence that justified it is on the page as it
 * stands and a reload would only put a fresh, undismissed banner in front of the recorder. An unclassifiable page is the case worth retrying: the guide rendered,
 * nothing blocked it, and the lineup simply never populated within the walk's budget, which a reload plausibly cures.
 *
 * The reload is skipped for a provider that navigates inside its own walk, since the retry re-navigates for itself; a reload that throws ends the retry rather than
 * risking a second walk against a page in an unknown state, and the first walk's classification is what gets recorded.
 * @param options - The page, provider, profile, and dependencies. See RetryAfterEmptyWalkOptions.
 * @returns The retry's channels and, when the retry was declined, the classification to record.
 */
async function retryAfterEmptyWalk(options: RetryAfterEmptyWalkOptions): Promise<EmptyWalkRetryResult> {

  const { deps, page, profile, provider } = options;
  const classification = await classifyBlockedPage(page, { indicators: provider.authWallIndicators, requestedUrl: provider.guideUrl });

  if(classification.kind !== "unknown") {

    LOG.debug("precache", "%s returned no channels and the page classified as %s; recording that rather than retrying.", provider.label, classification.kind);

    return { channels: [], classification };
  }

  if(provider.handlesOwnNavigation) {

    LOG.debug("precache", "Retrying the empty discovery walk for %s without a reload: its walk navigates for itself.", provider.label);
  } else {

    LOG.debug("precache", "Reloading the guide and retrying the empty discovery walk for %s.", provider.label);

    try {

      await page.reload({ timeout: CONFIG.streaming.navigationTimeout, waitUntil: "networkidle2" });
    } catch(error) {

      LOG.debug("precache", "The reload before retrying %s failed: %s. Recording the first walk's outcome instead.", provider.label, formatError(error));

      return { channels: [], classification };
    }
  }

  // A fresh controller for the second walk's poll. The first one is already aborted, and the poll's entry check returns immediately on an aborted signal, so
  // reusing it would silently leave the retry unprotected against a banner that reappears after the reload.
  const retryController = new AbortController();

  try {

    void deps.startOverlayHandling(page, profile, { phase: "discovery", signal: retryController.signal });

    return { channels: await walkWithDeadline(provider, page) };
  } finally {

    retryController.abort();
  }
}

/**
 * Opens a guarded browser page for a provider's guide, runs its discovery walk under a consent-overlay poll, and cleans the page up. This is the single owner of the
 * discovery page session shared by the precache cycle and the /services/:slug/channels endpoint: it asks the browser layer's creator for the page and holds
 * everything that happens to it afterwards - managed-page registration, the abort mechanics (close-on-abort plus the pre-navigation early-abort), the audio-mute
 * override, the discovery-phase overlay poll that dismisses cookie banners and per-site modals during the walk, and the close. The overlay poll is aborted the
 * instant the walk completes, so the page is quiet by construction before the afterWalk hook inspects it - a poll still clicking could dismiss the very overlay
 * a classification is about to report.
 *
 * A walk that comes back empty gets one reload-and-retry, on the terms retryAfterEmptyWalk sets out. The hook still runs exactly once, against whichever result
 * stands.
 * @param provider - The provider whose guide to walk.
 * @param options - The optional post-walk hook and abort signal. See WithProviderGuidePageOptions.
 * @param deps - The injected browser and overlay-poll dependencies; defaults to defaultPrecachingDeps.
 * @returns The discovered channels (possibly empty).
 */
export async function withProviderGuidePage(provider: ProviderModule, options: WithProviderGuidePageOptions = {},
  deps: PrecachingDeps = defaultPrecachingDeps): Promise<DiscoveredChannel[]> {

  const { afterWalk, signal } = options;
  const browser = await deps.getCurrentBrowser("page");

  /* The guide page is the active tab of a browser window of its own, opened in the background at the shared window's placement. A guide renders only while its
   * document is visible, and a walk is never captured, so the page gets a window that presents it without disturbing the shared window's own state or the tab
   * the user has selected there. The window closes with the page.
   */
  const page = await deps.createDiscoveryPage(browser);

  // Close the page the moment the caller aborts, so any in-progress Puppeteer operation throws and propagates the cancellation through discoverChannels without each
  // provider having to poll the signal. The helper owns this mechanism because it owns page creation - no caller ever holds the page reference, so close-on-abort
  // must live beside the lifecycle it cancels. The listener is registered the instant the page exists and removed in the finally.
  const onAbort = (): void => {

    void page.close().catch(() => { /* Page may already be closed. */ });
  };

  signal?.addEventListener("abort", onAbort, { once: true });

  // The overlay poll that dismisses cookie banners and per-site modals during the walk. Its controller is aborted the instant the walk completes, so the page is
  // quiet by construction before any classification the afterWalk hook performs - a poll still clicking could dismiss the very overlay a classification is about to
  // report.
  const overlayController = new AbortController();

  try {

    // If the caller aborted between entering this helper and creating the page, the abort listener fired while the page did not yet exist, so it never closed the
    // just-created page. Bail now - the finally closes it - and let the discovery caller map this to its abort sentinel.
    if(signal?.aborted) {

      throw new Error("Discovery aborted before navigation.");
    }

    // Suppress audio on the guide page. Services like Hulu auto-play a default livestream when their guide loads. Since Chrome's --mute-audio is deliberately
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

    /* Hold the page against the stale-page sweep for as long as the walk runs. A discovery page is owned by this walk rather than by any stream the registry
     * records, so without the mark the sweep starts a staleness clock on it at first sight and closes it partway through a long walk. The finally below
     * unregisters the page, which drops the mark, so the sweep stays a safety net for a leaked page while the deadline above is what bounds the walk.
     */
    deps.registerManagedPage(page, { inFlight: true });

    // Declare the layout the walk runs against, before the first navigation so the guide loads once at the surface it will be read on. A page carries no
    // emulation of its own, and every guide strategy was written against the preset's dimensions.
    await deps.emulateLayoutSurface(page);

    // Launch the discovery-phase overlay poll before navigation: a handlesOwnNavigation provider navigates inside discoverChannels, and the tick-error taxonomy lets
    // the poll survive that navigation. The phase's window is the backstop; the abort after the walk is the terminator. The guide page is not a tune, so the phase
    // forbids the embed-gate accept - only cookie rejection and per-site modal dismissal run here.
    const { profile } = getProfileForUrl(provider.guideUrl);

    void deps.startOverlayHandling(page, profile, { phase: "discovery", signal: overlayController.signal });

    // Navigate to the service's guide URL unless the provider module handles its own navigation (e.g., sets up response interception before navigating). We use
    // networkidle2 rather than load because SPA-based services (e.g., Hulu) have heavy async initialization that can prevent the load event from firing reliably.
    if(!provider.handlesOwnNavigation) {

      await page.goto(provider.guideUrl, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "networkidle2" });
    }

    let channels = await walkWithDeadline(provider, page);

    // The walk is complete. Abort the overlay poll so the page is quiet by construction before anything classifies it.
    overlayController.abort();

    let classification: BlockedPageClassification | undefined;

    /* An empty walk gets one more attempt, because the failure it most often represents is transient: a rail or grid whose lazy content never populated inside the
     * walk's budget, on a page that is otherwise fine. The gates are the ones that make a second walk meaningful at all - a closed or cancelled session has nothing
     * to retry against, and a shutdown must not open new work - and the retry's own classification decides whether a reload could help.
     */
    if((channels.length === 0) && !page.isClosed() && !signal?.aborted && !deps.isGracefulShutdown()) {

      ({ channels, classification } = await retryAfterEmptyWalk({ deps, page, profile, provider }));
    }

    // The hook runs once, on whichever result stands - and receives the first walk's classification only when the retry declined to reload, so the page it is
    // handed and the classification it records always describe the same moment.
    await afterWalk?.(page, channels, classification);

    return channels;
  } finally {

    signal?.removeEventListener("abort", onAbort);
    overlayController.abort();
    deps.unregisterManagedPage(page);

    try {

      await page.close();
    } catch {

      // Page may already be closed if the browser disconnected during discovery or the abort handler already closed it.
    }

    // Settle the shared browser window against the policy now that the walk is done. The walk's browser acquisition may have relaunched Chrome, which leaves
    // that window as the launch left it, and a walk runs long enough to span a stream state change; the pass is unconditional because the policy already
    // accounts for a login session or a running capture.
    await deps.syncWindowVisibility();
  }
}

/**
 * Precaches a single service: clears the service's cache, then walks its guide through the shared guarded page session, logging the timing and recording the
 * discovery outcome once the walk completes. The per-service primitive behind the precache cycle. Errors propagate to the caller, and no service filtering happens
 * here - the cycle loop owns both the filter skip and the per-service error containment.
 * @param provider - The provider to precache.
 * @returns The discovered channels (possibly empty).
 */
export async function precacheService(provider: ProviderModule, deps: PrecachingDeps = defaultPrecachingDeps): Promise<DiscoveredChannel[]> {

  const serviceElapsed = startTimer();

  // Clear the service's cache before discovery to ensure a complete walk, even if a tune partially warmed the cache during the startup delay.
  provider.strategy.clearCache?.();

  return withProviderGuidePage(provider, {

    afterWalk: async (page, channels, classification): Promise<void> => {

      LOG.info("Precached %s: %d channels (%ss).", provider.label, channels.length, (serviceElapsed() / 1000).toFixed(1).replace(/\.0$/, ""));

      // Record the outcome while the page is still open - an empty result classifies the page it walked, unless the session already classified it and declined to
      // reload, in which case that verdict travels here rather than being re-derived from a page the reload would have changed.
      await recordDiscoveryOutcome(provider, channels, page, deps, classification);
    }
  }, deps);
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
export async function revalidateDomainAuth(url: string, deps: PrecachingDeps = defaultPrecachingDeps): Promise<void> {

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

    if(deps.isGracefulShutdown()) {

      return;
    }

    // Defer to an in-flight precache cycle rather than overlapping it. Limitation: if the flagged provider is not in that cycle's configured service set,
    // the flag persists until the next successful discovery or tune for the domain - an in-flight cycle cannot be retargeted.
    if(precacheInProgress) {

      LOG.info("Deferring the post-login revalidation for %s to the precache cycle already in progress.", domain);

      return;
    }

    // Match every provider whose guide lives on the domain the user just signed in to. The observer's URL may be a DOMAIN_CONFIG loginUrl override rather than a
    // channel URL, but both extract to the same registrable domain the guide URLs use.
    const providers = deps.getProvidersForDomain(domain);

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
          await precacheService(provider, deps);
        } catch(error) {

          // A provider still behind its wall commonly times out the guide navigation. Contain the failure per provider and keep going - the flag simply stays set.
          LOG.warn("Post-login revalidation failed for %s: %s.", provider.label, formatError(error));
        }
      }
    } finally {

      releasePrecacheGuard(deps);
    }
  } catch(error) {

    LOG.warn("Post-login revalidation for %s failed: %s.", url, formatError(error));
  }
}

/**
 * Schedules the deferred re-attempt for the services a pass could not settle. Does nothing when every service came back with a lineup, and nothing during a
 * shutdown, where scheduling work against the browser is precisely what teardown is closing down. This is the one place a re-attempt is scheduled, so every
 * reason a service is still owed a walk - it ran and found nothing, it was stopped at its budget, or it never ran at all - arrives on the same schedule.
 * @param slugs - The services a pass could not settle: walked without settling, or deferred because a login session was on screen.
 * @param deps - The injected dependencies, handed to the pass this schedules.
 */
function armDeferredRetry(slugs: string[], deps: PrecachingDeps): void {

  if((slugs.length === 0) || deps.isGracefulShutdown()) {

    return;
  }

  LOG.debug("precache", "Scheduling one deferred discovery re-attempt for %d service%s in %d minutes.", slugs.length, (slugs.length === 1) ? "" : "s",
    PRECACHE_RETRY_DELAY / 60000);

  deferredRetry = { slugs, timer: setTimeout(() => void runDeferredRetry(deps), PRECACHE_RETRY_DELAY) };
}

/**
 * Runs the deferred re-attempt for the services a cycle could not settle. Never rejects - it is driven by a timer with nobody to hand a rejection to, so every
 * per-service failure is contained the same way the cycle contains its own.
 *
 * A service whose lineup arrived in the interval - from a later full cycle, or from an on-demand discovery a user triggered - is skipped rather than re-walked,
 * because the walk it would run is the expensive part and the answer is already in hand. A login session that begins before the pass fires stops it where it
 * stands and re-arms everything still owed, so the walks resume once the user is done rather than opening a window over the one they are signing in through.
 * @param deps - The injected browser and provider-registry dependencies.
 * @returns A promise that resolves once the pass completes or is skipped.
 */
async function runDeferredRetry(deps: PrecachingDeps): Promise<void> {

  const pending = deferredRetry;

  // Drop the state before acting on it. This is the only pass there will be, so a handle left standing would tell a later cancellation that something is still
  // scheduled when nothing is.
  deferredRetry = null;

  if(!pending || deps.isGracefulShutdown()) {

    return;
  }

  /* A cycle or a post-login revalidation holding the guard is already walking guides, quite possibly these same ones. This pass exists to try again on a settled
   * system, not to contend with a run in flight - and the run in flight is the better attempt of the two, so this one is dropped wholesale rather than rescheduled.
   */
  if(precacheInProgress) {

    LOG.debug("precache", "Skipping the deferred discovery re-attempt: a precache cycle is already in progress.");

    return;
  }

  precacheInProgress = true;

  let attempted = 0;
  let succeeded = 0;

  // The services this pass leaves unwalked because a login session came up, named rather than counted: they are re-armed below on the same schedule.
  let remaining: string[] = [];

  try {

    for(const [ index, slug ] of pending.slugs.entries()) {

      // Re-checked every iteration, exactly as the cycle's own loop does: a shutdown that begins mid-pass must stop opening discovery pages, or the next
      // getCurrentBrowser relaunches the Chrome that teardown just closed.
      if(deps.isGracefulShutdown()) {

        break;
      }

      const provider = deps.getProviderBySlug(slug);

      if(!provider) {

        continue;
      }

      if(provider.getCachedChannels()) {

        LOG.debug("precache", "Skipping the deferred re-attempt for %s: its lineup was discovered in the meantime.", provider.label);

        continue;
      }

      /* Stop where the login session found us, and take everything from here with us. A walk opens its window at the shared window's placement, which during a
       * login session is the window the user is signing in through. The check reads live state, so the pass that re-arms below runs to the end once the
       * session is over; the slugs already walked are behind us and the one skipped above is settled, so what remains is exactly what is still owed.
       */
      if(isLoginModeActive()) {

        remaining = pending.slugs.slice(index);

        break;
      }

      attempted++;

      try {

        // eslint-disable-next-line no-await-in-loop
        const channels = await precacheService(provider, deps);

        if(channels.length > 0) {

          succeeded++;
        }
      } catch(error) {

        /* This is the one pass a service gets, so a lapse here is reported and left there. Queuing it again would put a wedged walk on an unbounded loop, waking
         * the browser for it every few minutes for the life of the process.
         */
        if(error instanceof DiscoveryWalkTimeoutError) {

          LOG.warn("%s's discovery walk exceeded its %d second budget and was stopped.", provider.label, DISCOVERY_WALK_TIMEOUT / 1000);
        } else {

          LOG.warn("The deferred channel discovery re-attempt failed for %s: %s.", provider.label, formatError(error));
        }
      }
    }

    if(attempted > 0) {

      LOG.info("Deferred channel discovery re-attempt complete: %d of %d service%s now have a lineup.", succeeded, attempted, (attempted === 1) ? "" : "s");
    }

    /* Re-arm what the login session interrupted. This function drops the pending state at entry, so arming here schedules exactly one fresh pass rather than
     * stacking on a handle that is still standing.
     */
    if(remaining.length > 0) {

      LOG.debug("precache", "Deferring %d service%s from the discovery re-attempt: a login session is on screen.", remaining.length,
        (remaining.length === 1) ? "" : "s");

      armDeferredRetry(remaining, deps);
    }
  } finally {

    releasePrecacheGuard(deps);
  }
}

/**
 * Executes the sequential precaching cycle. Discovers channel lineups for each configured service, clearing the service's cache first to ensure a complete walk.
 * Services not in the active service filter are silently skipped when the filter is non-empty. Services the cycle could not settle - they walked and found
 * nothing, their walk ran past its budget, or a login session on screen kept them from walking at all - are handed to the deferred re-attempt, minutes later,
 * once whatever startup contention or user session may have starved them has passed.
 */
async function runPrecacheCycle(deps: PrecachingDeps): Promise<void> {

  /* Bail if a graceful shutdown began while this cycle was queued. Discovery opens browser pages via getCurrentBrowser(), which would relaunch Chrome after shutdown
   * closed it; this guard makes the cycle a no-op during teardown regardless of how the timer-cancellation race resolves. Reset the in-progress flag since the
   * early return skips the finally block below - directly rather than through the hand-off, because honoring a full-cycle request is the one thing a teardown path
   * must not do.
   */
  if(deps.isGracefulShutdown()) {

    precacheInProgress = false;

    return;
  }

  const slugs = CONFIG.channels.precacheServices;
  const enabledFilter = CONFIG.channels.enabledServices;
  const hasFilter = enabledFilter.length > 0;
  const cycleElapsed = startTimer();

  /* The services that walked without settling - they came back with nothing, or the walk ran past its budget and was stopped - named rather than counted, because
   * the deferred re-attempt below needs to know which ones to come back to. Both readings say the same thing about the service: the guide did not answer this
   * time, and a second pass on a settled system is worth trying.
   */
  const unsettledSlugs: string[] = [];

  // The services this cycle stood aside from because a login session was on screen. They travel to the same re-attempt, for the same reason: a walk is still owed.
  const deferredSlugs: string[] = [];

  let skipped = 0;
  let succeeded = 0;

  LOG.info("Starting channel lineup precaching for %d service%s.", slugs.length, (slugs.length === 1) ? "" : "s");

  try {

    // Services are precached sequentially - each opens a browser page and navigates to a heavy SPA, so concurrent execution would stress system resources.
    for(const slug of slugs) {

      // Stop opening new discovery pages once a graceful shutdown begins mid-cycle. The entry guard above only covers a cycle that has not started; without this,
      // a cycle already in its loop when shutdown closes the browser would call getCurrentBrowser() below and relaunch Chrome. Break rather than continue so no
      // further service is processed; the in-flight service (if any) finishes and closes its own page via its finally.
      if(deps.isGracefulShutdown()) {

        break;
      }

      const provider = deps.getProviderBySlug(slug);

      if(!provider) {

        continue;
      }

      // Skip services not in the active service filter. Their stored config is preserved for when the filter changes back.
      if(hasFilter && !enabledFilter.includes(slug)) {

        LOG.debug("precache", "Skipping precache for %s: not in active service filter.", provider.label);
        skipped++;

        continue;
      }

      /* Stand aside while a login session is on screen. A walk opens its window at the shared window's placement, which during a login session is the window
       * the user is signing in through, and a second window over it would take their clicks. The service is collected for the deferred re-attempt below, which
       * is the machinery that already exists for a service this cycle could not settle. The check sits after the filter skip so a filtered-out service is
       * still counted as filtered rather than queued for a walk it would never get.
       */
      if(isLoginModeActive()) {

        LOG.debug("precache", "Deferring the precache for %s: a login session is on screen.", provider.label);
        deferredSlugs.push(slug);

        continue;
      }

      try {

        // eslint-disable-next-line no-await-in-loop
        const channels = await precacheService(provider, deps);

        // An empty walk cached nothing, so it is counted as unsettled rather than folded into the success count.
        if(channels.length > 0) {

          succeeded++;
        } else {

          unsettledSlugs.push(slug);
        }
      } catch(error) {

        /* A walk stopped at its budget is its own outcome rather than a general failure: the ceiling ended a walk that was still going, which says nothing about
         * whether the guide would answer on a settled system, so the service joins the deferred re-attempt exactly as an empty walk does. Every other failure
         * keeps the general warn and is not queued - a provider that threw has a standing problem another walk will not solve.
         */
        if(error instanceof DiscoveryWalkTimeoutError) {

          LOG.warn("%s's discovery walk exceeded its %d second budget and was stopped; the service will be re-attempted.", provider.label,
            DISCOVERY_WALK_TIMEOUT / 1000);
          unsettledSlugs.push(slug);
        } else {

          LOG.warn("Failed to precache %s: %s.", provider.label, formatError(error));
        }
      }
    }

    const elapsed = (cycleElapsed() / 1000).toFixed(1).replace(/\.0$/, "");
    const unsettledSuffix = (unsettledSlugs.length > 0) ? ", " + String(unsettledSlugs.length) + " returned no channels or timed out" : "";
    const deferredSuffix = (deferredSlugs.length > 0) ? ", " + String(deferredSlugs.length) + " deferred for a login session" : "";
    const skippedSuffix = (skipped > 0) ? ", " + String(skipped) + " skipped (filtered)" : "";

    LOG.info("Channel lineup precaching complete: %d service%s cached%s%s%s in %ss.", succeeded, (succeeded === 1) ? "" : "s", unsettledSuffix, deferredSuffix,
      skippedSuffix, elapsed);

    armDeferredRetry([ ...unsettledSlugs, ...deferredSlugs ], deps);
  } finally {

    releasePrecacheGuard(deps);
  }
}
