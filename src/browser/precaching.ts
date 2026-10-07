/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * precaching.ts: Service channel lineup precaching for PrismCast.
 */
import type { ChangeRejection, ConfigChange } from "../config/reactivity.ts";
import type { Config, DiscoveredChannel, Nullable, ProviderModule, ResolvedSiteProfile } from "../types/index.ts";
import { LOG, extractDomain, formatError, startTimer, timeoutSignal } from "../utils/index.ts";
import { clearDomainAuthRequirement, getDomainAuthState, markDomainAuth, markDomainAuthRequired } from "../config/health.ts";
import { createDiscoveryPage, emulateLayoutSurface, getCurrentBrowser, isBrowserConnected, isGracefulShutdown, registerManagedPage, syncWindowVisibility,
  unregisterManagedPage } from "./index.ts";
import { getPersistedLineup, persistProviderLineup } from "../config/providerLineups.ts";
import { getProviderBySlug, getProvidersForDomain } from "./channelSelection.ts";
import { systemClock, waitWithSignal } from "homebridge-plugin-utils";
import type { BlockedPageClassification } from "./blockedPage.ts";
import { CONFIG } from "../config/index.ts";
import type { Clock } from "homebridge-plugin-utils";
import type { Page } from "puppeteer-core";
import type { PersistedLineupChannel } from "../config/providerLineups.ts";
import { classifyBlockedPage } from "./blockedPage.ts";
import { getProfileForUrl } from "../config/profiles.ts";
import { isLoginModeActive } from "./login.ts";
import { isServiceTagEnabled } from "../config/services.ts";
import { registerConfigChangeHandler } from "../config/reactivity.ts";
import { startOverlayHandling } from "./consent.ts";

/* Precaching discovers channel lineups for selected services ahead of any tune, so that even the first tune benefits from cached lineup data. Each service is
 * precached sequentially - discovery opens a browser page in a window of its own and navigates to a heavy SPA, so running all services concurrently would stress
 * CPU and GPU on resource-constrained systems. A cycle runs in the background after a settle delay, never on a request's path.
 *
 * A cycle is requested with a scope. A browser launch requests one over every listed service, because the launched browser starts with every provider cache
 * empty, and a settings save requests one over the services it adds to the list, so the save warms what it selected without clearing any other provider's
 * cache. Precaching never launches the browser: a cycle or a deferred pass that finds no browser running stops, and the next launch requests its own cycle. Each
 * service has its own try/catch - one failure does not stop the rest.
 *
 * This module also owns the discovery-outcome policy (recordDiscoveryOutcome): the single source of truth for how a completed discovery walk translates into domain
 * auth state and a persisted channel lineup, shared by the precache cycle here and the /services/:slug/channels endpoint. The routes layer never calls a health
 * mutator or the lineup store directly - it calls the recorder, which does. The page session itself is owned by withProviderGuidePage: the single guarded-page
 * primitive both the precache cycle and that endpoint walk their guides through, and the one place the empty-walk retry policy lives, so no provider carries a
 * retry of its own.
 *
 * Every timer and deadline this module arms - the settle delay before a cycle, the deferred re-attempt, and each walk's ceiling - runs on the clock its
 * dependencies carry, so one injected clock drives the whole schedule.
 */

// The settle delay in milliseconds before a requested cycle walks its first guide. It gives a browser that just launched time to settle, and every request that
// arrives inside it joins the pending cycle rather than starting another.
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

/* What a precache request asks a cycle to walk: every listed service, or the listed services among a set of slugs. A browser launch asks for every service,
 * because its browser starts with every provider cache empty, and a save asks for the services it adds to the list. A cycle walks only the listed services its
 * scope admits, so a save's cycle clears and walks the services that save added and leaves every other provider's warm cache alone.
 */
type PrecacheScope = { readonly kind: "all" } | { readonly kind: "services"; readonly slugs: ReadonlySet<string> };

/**
 * Merges a new request's scope into the scope already requested, giving the one scope that answers each request, so a request that reaches a pending cycle or a
 * held guard joins what is already requested rather than queueing behind it. A scope over every service absorbs any other, and slug scopes union their slugs.
 * @param current - The scope already requested.
 * @param added - The scope a new request carries.
 * @returns The scope that answers each request.
 */
function mergeScopes(current: PrecacheScope, added: PrecacheScope): PrecacheScope {

  if(current.kind === "all") {

    return current;
  }

  if(added.kind === "all") {

    return added;
  }

  return { kind: "services", slugs: current.slugs.union(added.slugs) };
}

// The single-flight guard. A cycle holds it from the moment it is armed until its run ends, and a deferred re-attempt and a post-login revalidation each hold it
// while they run, so runs never overlap. Every holder releases it through releasePrecacheGuard, and the shutdown paths clear it directly.
let precacheInProgress = false;

/* The scope requested while the guard was held, merged across every such request, or null when none arrived. The run holding the guard walks the scope it was
 * armed with and cannot take on another, so a request is recorded rather than dropped: a launch's means every provider cache was just cleared, and a save's means
 * services were added after that run read the list. Whoever releases the guard hands it on; releasePrecacheGuard is where that happens, and it is the only place.
 */
let requestedScope: Nullable<PrecacheScope> = null;

/* The scheduler's pending work, one value with a slot for each kind: the cycle armed and not yet fired, with the scope it will walk, and the deferred re-attempt,
 * with the services it still owes a walk. The slots are independent because a save's cycle and a deferred re-attempt can be pending at once - a save arms its
 * cycle and leaves a pending re-attempt on its own delay - and updateSchedule, the one writer, ties every timer to the state that holds it.
 *
 * How each lifecycle event moves the slots:
 *
 * - A launch requests every service. It arms the cycle slot, or merges into a pending cycle and re-arms it for a full settle delay, and in each case clears the
 *   re-attempt slot, because that cycle walks everything the re-attempt owes. With the guard held by a run, it is recorded in requestedScope and touches no slot;
 *   the release hands it on, and that arming clears whatever re-attempt the run left behind.
 * - A save requests the services it adds, and only while a browser is connected. It arms the cycle slot, merges into a pending cycle's scope and keeps that
 *   cycle's timer, or is recorded in requestedScope while the guard is held. It never touches the re-attempt slot: a cycle it arms that ends with services it
 *   could not settle merges them into a pending re-attempt, as every cycle does.
 * - A shutdown runs stopPrecaching, which clears every slot through updateSchedule, disposing their timers, drops requestedScope and frees the guard without
 *   honoring it. The request function and armDeferredRetry return during a shutdown, so nothing arms a slot again, and a cycle or pass already running stops at
 *   its next shutdown check.
 * - A browser disconnect changes no slot. A cycle that fires without a browser stops at its entry check, releases the guard through releasePrecacheGuard and arms
 *   no re-attempt. A cycle whose browser goes during its loop breaks at the next service's check, as the shutdown check breaks, so the services it found
 *   unsettled reach armDeferredRetry and the unwalked ones wait for the next launch. A re-attempt that fires without a browser stops and re-arms nothing, its
 *   services waiting for that same launch. A scope recorded meanwhile fires at most one further cycle, which finds no browser and stops.
 * - A cancelled cycle, one that ends without walking, has already cleared its slot when it fired. The shutdown return frees the guard directly and honors
 *   nothing; the browser stop and a scope that admits no listed service release through releasePrecacheGuard, which honors requestedScope. None of them arms a
 *   re-attempt. Only stopPrecaching cancels a pending cycle, because a merge re-arms a cycle rather than cancelling it.
 * - A login session sends the services a cycle stood aside from to armDeferredRetry at the cycle's end, merged into a pending re-attempt or armed as one, and a
 *   re-attempt that meets a session re-arms what remains. A post-login revalidation holds the guard, so a request during it is recorded in requestedScope and a
 *   re-attempt that fires during it re-arms itself on its own delay; the revalidation's release hands the recorded scope on.
 * - A process restart discards module state, and the next launch requests every service.
 */
interface PrecacheSchedule {

  readonly cycle: Nullable<{ readonly scope: PrecacheScope; readonly timer: Disposable }>;
  readonly retry: Nullable<{ readonly slugs: readonly string[]; readonly timer: Disposable }>;
}

// The scheduler's pending work. updateSchedule is its only writer.
let schedule: PrecacheSchedule = { cycle: null, retry: null };

/**
 * Writes the scheduler state, and is the only code that does. It builds the next value from the current one and the change, disposes each timer the current
 * value holds and the next value does not, compared by identity, and assigns the next value. A record replaced by one carrying the same timer keeps that
 * timer armed, as a slug scope merged into a pending cycle does; a record cleared, or replaced by one carrying a new timer, has its timer disposed. So no path
 * can drop a record and leave its timer armed, and none can cancel a timer the state still holds. Disposing a timer that already fired does nothing, so a fire
 * clears its own slot through here as well.
 * @param change - The slots to write; a slot the change leaves out keeps its record.
 */
function updateSchedule(change: Partial<PrecacheSchedule>): void {

  const next: PrecacheSchedule = { ...schedule, ...change };

  // The timers a schedule value holds, one per slot, so the current value's timers are compared against the next value's by identity.
  const timersOf = (value: PrecacheSchedule): (Disposable | undefined)[] => [ value.cycle?.timer, value.retry?.timer ];
  const kept = new Set(timersOf(next));

  for(const timer of timersOf(schedule)) {

    if(timer && !kept.has(timer)) {

      timer[Symbol.dispose]();
    }
  }

  schedule = next;
}

/**
 * Releases the single-flight guard and hands on any scope requested while it was held.
 *
 * The order is the whole point. The guard is freed and the recorded scope taken before the hand-off, so the request below finds a free guard and arms. Handing on
 * first would have that request see the guard still held and record the very scope being handed on, which is how a request would go round forever without ever
 * running. Every path that takes the guard - the cycle, the deferred re-attempt, and the post-login revalidation - releases it here, so the hand-off has one home
 * rather than a copy at each site. The scope goes to the request function rather than to startPrecaching, so it never meets the launch's empty-list gate: the
 * cycle it arms reads the list when it fires.
 * @param deps - The injected dependencies, handed to the cycle this may arm.
 */
function releasePrecacheGuard(deps: PrecachingDeps): void {

  precacheInProgress = false;

  const scope = requestedScope;

  if(!scope) {

    return;
  }

  requestedScope = null;

  requestPrecache(scope, deps);
}

/* PrecachingDeps is the browser + provider-registry surface the precache cycle composes on: the shared-browser accessors, the browser-connected check that keeps
 * the scheduler from ever launching a browser, the discovery-page creator and the page bookkeeping around it, the shutdown gate, the window-visibility sync, the
 * provider lookups, the discovery-phase overlay-poll launcher, and the durable-lineup read and write the discovery-outcome policy performs.
 * It is injected as a default parameter threaded through the module's functions so a test can substitute stubs at the same PrecachingDeps boundary - no loader
 * mock - while production uses the real defaultPrecachingDeps built from the functions this module already imports. startOverlayHandling belongs here for the same
 * reason the browser accessors do: run for real it drives a poll against the page, so a test injects a recording stub to observe the discovery poll's phase and
 * abort timing without a live poll. The lineup store's members belong here for the same reason again: run for real they touch a file, so a test observes the
 * write - and injects a failing one - and states the saved lineup the plausibility guard reads against, both at this boundary. It is kept as an in-module const,
 * NOT a separate *.context.ts adapter: browser/index.ts imports startPrecaching and precaching.ts imports these accessors, so a separate adapter file would sit
 * inside that value-import cycle, whereas the in-module const adds no new import edge.
 * The interface carries the library's Clock itself as one of its members, so the scheduler's time source arrives at the same boundary as its collaborators.
 */
export interface PrecachingDeps {

  // The clock the settle delay before a cycle, the deferred re-attempt, each walk's deadline, and the discovery-phase overlay polls all run on. Production wires
  // the system clock; a test wires a virtual clock and drives the whole schedule from one advance.
  readonly clock: Clock;

  readonly createDiscoveryPage: typeof createDiscoveryPage;
  readonly emulateLayoutSurface: typeof emulateLayoutSurface;
  readonly getCurrentBrowser: typeof getCurrentBrowser;
  readonly getPersistedLineup: typeof getPersistedLineup;
  readonly getProviderBySlug: typeof getProviderBySlug;
  readonly getProvidersForDomain: typeof getProvidersForDomain;
  readonly isBrowserConnected: typeof isBrowserConnected;
  readonly isGracefulShutdown: typeof isGracefulShutdown;
  readonly persistProviderLineup: typeof persistProviderLineup;
  readonly registerManagedPage: typeof registerManagedPage;
  readonly startOverlayHandling: typeof startOverlayHandling;
  readonly syncWindowVisibility: typeof syncWindowVisibility;
  readonly unregisterManagedPage: typeof unregisterManagedPage;
}

export const defaultPrecachingDeps: PrecachingDeps = {

  clock: systemClock,
  createDiscoveryPage,
  emulateLayoutSurface,
  getCurrentBrowser,
  getPersistedLineup,
  getProviderBySlug,
  getProvidersForDomain,
  isBrowserConnected,
  isGracefulShutdown,
  persistProviderLineup,
  registerManagedPage,
  startOverlayHandling,
  syncWindowVisibility,
  unregisterManagedPage
};

/**
 * Arms the cycle slot for a scope on a full settle delay. The callback reads the slot's scope when it fires, never the scope armed here, so every request merged
 * into the slot before the fire reaches the walk, and it clears the slot through updateSchedule before the cycle runs. Arming a scope over every service clears
 * the re-attempt slot in the same write, because that cycle walks every service the re-attempt owes and a second pass over the same guides would contend with it
 * for one browser; a slug scope leaves the re-attempt on its own delay, with its skip for a lineup that arrived meanwhile and its one pass.
 * @param scope - The scope the cycle walks unless a later request merges into it.
 * @param deps - The injected dependencies; the timer arms on their clock and the cycle runs with them.
 */
function armCycle(scope: PrecacheScope, deps: PrecachingDeps): void {

  const timer = deps.clock.schedule(() => {

    const armed = schedule.cycle;

    updateSchedule({ cycle: null });

    // A fire finds its own record in the slot, because every write that drops a cycle record disposes that record's timer first.
    if(!armed) {

      return;
    }

    void runPrecacheCycle(armed.scope, deps);
  }, PRECACHE_DELAY);

  updateSchedule((scope.kind === "all") ? { cycle: { scope, timer }, retry: null } : { cycle: { scope, timer } });
}

/**
 * Lands a precache request, and is the one place one lands. A request carries the scope it wants walked and takes the first of these that applies: during a
 * graceful shutdown it is dropped; with a cycle armed and not yet fired, it merges into that cycle's scope; with the guard held by a run, it merges into
 * requestedScope, which the release hands on; otherwise it takes the guard and arms a cycle.
 *
 * The pending cycle is consulted ahead of the guard, which that cycle holds, because it reads the list and the browser when it fires and so serves the request
 * whole - recording the request instead would run a second cycle after the first. A scope over every service comes from a launch, so merging one into a pending
 * cycle re-arms that cycle for a full settle delay from the merge, the time the new browser needs, and clears the re-attempt slot as arming one does. A slug
 * scope merged into a pending cycle keeps its timer, so a save never postpones a cycle already on its way.
 * @param scope - The services the request wants walked.
 * @param deps - The injected dependencies, handed to the cycle this arms.
 */
function requestPrecache(scope: PrecacheScope, deps: PrecachingDeps): void {

  // Never arm during a graceful shutdown. A launch can be reached during teardown, and a cycle armed then would fire after teardown closed the browser.
  if(deps.isGracefulShutdown()) {

    return;
  }

  const pending = schedule.cycle;

  if(pending) {

    const merged = mergeScopes(pending.scope, scope);

    if(scope.kind === "all") {

      armCycle(merged, deps);
    } else {

      updateSchedule({ cycle: { scope: merged, timer: pending.timer } });
    }

    return;
  }

  if(precacheInProgress) {

    // Record the request rather than dropping it: the run holding the guard walks the scope it was armed with, and whoever releases the guard arms this one.
    requestedScope = requestedScope ? mergeScopes(requestedScope, scope) : scope;

    LOG.debug("precache", "Precache deferred: already in progress.");

    return;
  }

  // Take the guard as the cycle is armed, so every request until the cycle's run ends merges into it or is recorded for its release rather than arming another.
  precacheInProgress = true;

  armCycle(scope, deps);
}

/**
 * Requests a precache cycle over every listed service. This is the browser launch's request, made once its browser is ready, because a launched browser starts
 * with every provider cache empty; a save requests only the services it adds, through applyPrecacheConfigChanges. Returns at once when no service is listed, and
 * otherwise lands the request through requestPrecache, whose cycle waits out the settle delay on the dependencies' clock, so the launch is never blocked.
 * @param deps - The injected browser and provider-registry dependencies; defaults to defaultPrecachingDeps.
 */
export function startPrecaching(deps: PrecachingDeps = defaultPrecachingDeps): void {

  if(CONFIG.channels.precacheServices.length === 0) {

    return;
  }

  requestPrecache({ kind: "all" }, deps);
}

/**
 * Cancels every scheduled precache - the pending cycle and the deferred re-attempt alike - drops a recorded request, and clears the in-progress guard. Called
 * during graceful shutdown so nothing scheduled can fire after the browser has been closed. Safe to call when nothing is pending.
 *
 * The slots are cleared through updateSchedule, which disposes their timers. The guard is cleared directly rather than through releasePrecacheGuard, and the
 * recorded request with it: a shutdown must not hand on a pending request, which is exactly what releasing through the hand-off would do.
 */
export function stopPrecaching(): void {

  updateSchedule({ cycle: null, retry: null });

  requestedScope = null;
  precacheInProgress = false;
}

/**
 * Requests a walk of the services a saved precache list adds. The candidate carries the saved list and CONFIG still holds the running one, because the reconcile
 * commits only after its handlers run, so the services the save adds are the candidate's slugs the running list lacks. They are requested as one scope while a
 * browser is connected, so the save precaches them without clearing any other provider's cache. A save that only removes services needs nothing here, because
 * every reader of the list reads it when it walks, and with no browser running the save requests nothing, because a browser is never launched for a save and
 * the next launch requests every listed service. Requesting cannot fail, so the handler refuses nothing.
 * @param _changes - The change to the precache list; the candidate carries the list, so the handler reads that instead.
 * @param next - The candidate running configuration.
 * @param deps - The injected dependencies; defaults to defaultPrecachingDeps.
 * @returns No rejections.
 */
export async function applyPrecacheConfigChanges(_changes: readonly ConfigChange[], next: Readonly<Config>,
  deps: PrecachingDeps = defaultPrecachingDeps): Promise<readonly ChangeRejection[]> {

  const added = new Set(next.channels.precacheServices).difference(new Set(CONFIG.channels.precacheServices));

  if((added.size > 0) && deps.isBrowserConnected()) {

    requestPrecache({ kind: "services", slugs: added }, deps);
  }

  return [];
}

// Module-load side effect: register the handler once per process, as every config-change handler registers, so it is in place before the first save can reach
// the reconcile.
registerConfigChangeHandler("channels.precacheServices", applyPrecacheConfigChanges);

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
 * @param clock - The clock the deadline arms on.
 * @returns The discovered channels (possibly empty).
 * @throws DiscoveryWalkTimeoutError when the walk outlives its budget, and whatever the walk itself rejected with otherwise.
 */
async function walkWithDeadline(provider: ProviderModule, page: Page, clock: Clock): Promise<DiscoveredChannel[]> {

  const lapse = new DiscoveryWalkTimeoutError(provider.label, DISCOVERY_WALK_TIMEOUT);
  const deadline = timeoutSignal(DISCOVERY_WALK_TIMEOUT, { clock, reason: lapse });

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

    void deps.startOverlayHandling(page, profile, { clock: deps.clock, phase: "discovery", signal: retryController.signal });

    return { channels: await walkWithDeadline(provider, page, deps.clock) };
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

  /* The guide page is the active tab of a browser window of its own, opened in the background at the shared window's placement and presented by the creator for
   * the page's whole life. A guide renders only while Chrome presents its document, and a walk is never captured, so the page gets a window that never disturbs
   * the shared window's own state or the tab the user has selected there, and a presentation that does not depend on that window being shown. The window closes
   * with the page.
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

    void deps.startOverlayHandling(page, profile, { clock: deps.clock, phase: "discovery", signal: overlayController.signal });

    // Navigate to the service's guide URL unless the provider module handles its own navigation (e.g., sets up response interception before navigating). We use
    // networkidle2 rather than load because SPA-based services (e.g., Hulu) have heavy async initialization that can prevent the load event from firing reliably.
    if(!provider.handlesOwnNavigation) {

      await page.goto(provider.guideUrl, { timeout: CONFIG.streaming.navigationTimeout, waitUntil: "networkidle2" });
    }

    let channels = await walkWithDeadline(provider, page, deps.clock);

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

  // Clear the service's cache before discovery to ensure a complete walk, even if a tune partially warmed the cache during the settle delay. Only the walked
  // service's cache is cleared, so a cycle scoped to the services a save added leaves every other provider's warm cache alone.
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
 * filter skip is unchanged. While it runs it holds the same single-flight guard the precache cycle holds, so a cycle requested mid-revalidation, by a launch or
 * a save, is recorded and armed once the revalidation releases the guard, instead of overlapping it.
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

    // Defer to an in-flight precache run rather than overlapping it. Limitation: when the flagged provider is outside what that run walks - a cycle whose list
    // or whose scope leaves it out - the flag persists until the next walk that reaches the provider, the next launch's cycle over every service among them, or a
    // successful tune for the domain, because a run in flight cannot be retargeted.
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
 *
 * A re-attempt already pending takes the new services into its own list and is re-armed for a full delay from the merge, so every service it owes waits the full
 * delay after its last walk rather than inheriting what is left of an earlier one's wait. updateSchedule disposes the timer the merged record replaces.
 * @param slugs - The services a pass could not settle: walked without settling, or deferred because a login session was on screen.
 * @param deps - The injected dependencies, handed to the pass this schedules.
 */
function armDeferredRetry(slugs: readonly string[], deps: PrecachingDeps): void {

  if((slugs.length === 0) || deps.isGracefulShutdown()) {

    return;
  }

  const owed = Array.from(new Set([ ...(schedule.retry?.slugs ?? []), ...slugs ]));

  LOG.debug("precache", "Scheduling one deferred discovery re-attempt for %d service%s in %d minutes.", owed.length, (owed.length === 1) ? "" : "s",
    PRECACHE_RETRY_DELAY / 60000);

  updateSchedule({ retry: { slugs: owed, timer: deps.clock.schedule(() => void runDeferredRetry(deps), PRECACHE_RETRY_DELAY) } });
}

/**
 * Runs the deferred re-attempt for the services a cycle could not settle. Never rejects - it is driven by a timer with nobody to hand a rejection to, so every
 * per-service failure is contained the same way the cycle contains its own.
 *
 * A service whose lineup arrived in the interval - from a later full cycle, or from an on-demand discovery a user triggered - is skipped rather than re-walked,
 * because the walk it would run is the expensive part and the answer is already in hand, and so is a service that has since left the precache list or the
 * active service filter. A login session that begins before the pass fires stops it where it stands and re-arms everything still owed, so the walks resume once
 * the user is done rather than opening a window over the one they are signing in through.
 * @param deps - The injected browser and provider-registry dependencies.
 * @returns A promise that resolves once the pass completes or is skipped.
 */
async function runDeferredRetry(deps: PrecachingDeps): Promise<void> {

  const pending = schedule.retry;

  // Clear the slot before acting on it, through the one writer. This is the only pass there will be, so a record left standing would tell a later cancellation or
  // merge that something is still scheduled when nothing is.
  updateSchedule({ retry: null });

  // A fire finds its own record in the slot, because every write that drops a re-attempt record disposes that record's timer first.
  if(!pending) {

    return;
  }

  if(deps.isGracefulShutdown()) {

    return;
  }

  // Stop rather than walk when no browser is running, as the cycle does. The pass re-arms nothing, because the next launch requests a cycle over every listed
  // service, the ones this pass owes among them.
  if(!deps.isBrowserConnected()) {

    LOG.debug("precache", "Skipping the deferred discovery re-attempt: no browser is running.");

    return;
  }

  /* A cycle or a post-login revalidation holding the guard is already walking guides, quite possibly these same ones. This pass exists to try again on a settled
   * system, not to contend with a run in flight, so it re-arms itself for its services on its own delay, as it does for a login session, rather than dropping
   * them: the run in flight may walk a scope that leaves them out, and when the pass fires again it skips any service whose lineup arrived meanwhile.
   */
  if(precacheInProgress) {

    LOG.debug("precache", "Deferring the discovery re-attempt: a precache run is already in progress.");
    armDeferredRetry(pending.slugs, deps);

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

      // Stop the same way once the browser is gone. No await lies between this check and the browser acquisition precacheService reaches through
      // withProviderGuidePage, so a walk never acquires a browser this check found gone and the pass never launches one; an await inserted between them breaks that.
      if(!deps.isBrowserConnected()) {

        break;
      }

      const provider = deps.getProviderBySlug(slug);

      if(!provider) {

        continue;
      }

      // A service off the list or outside the running filter is owed nothing, so the pass reads the list and the filter at each service's turn, as the cycle does.
      if(!CONFIG.channels.precacheServices.includes(slug)) {

        LOG.debug("precache", "Skipping the deferred re-attempt for %s: it is no longer on the precache list.", provider.label);

        continue;
      }

      if(!isServiceTagEnabled(slug)) {

        LOG.debug("precache", "Skipping the deferred re-attempt for %s: not in active service filter.", provider.label);

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
 * Executes one sequential precache cycle over the listed services its scope admits, clearing each service's cache first to ensure a complete walk. The list is
 * read when the cycle fires, so a service removed during the settle delay is never walked, and again at each service's turn, so one removed while the cycle walks
 * an earlier service is skipped. Services the running service filter excludes are skipped as well. Services the cycle could not settle - they walked and found
 * nothing, their walk ran past its budget, or a login session on screen kept them from walking at all - are handed to the deferred re-attempt, minutes later,
 * once whatever contention or user session may have starved them has passed.
 * @param scope - The services the cycle walks, read from the cycle slot when the cycle fired.
 * @param deps - The injected browser and provider-registry dependencies.
 * @returns A promise that resolves once the cycle completes or stops.
 */
async function runPrecacheCycle(scope: PrecacheScope, deps: PrecachingDeps): Promise<void> {

  /* Bail if a graceful shutdown began while this cycle was pending. Discovery opens browser pages via getCurrentBrowser(), which would relaunch Chrome after
   * shutdown closed it; this guard makes the cycle a no-op during teardown regardless of how the timer-cancellation race resolves. Reset the in-progress flag
   * since the early return skips the finally block below - directly rather than through the hand-off, because handing on a recorded request is the one thing a
   * teardown path must not do.
   */
  if(deps.isGracefulShutdown()) {

    precacheInProgress = false;

    return;
  }

  /* Stop rather than walk when no browser is running. Discovery acquires its browser through getCurrentBrowser(), which would launch Chrome for a cycle, and the
   * next launch requests its own cycle over every listed service. The guard is released through the hand-off, so a request recorded while this cycle was pending
   * still arms, and that cycle stops here in turn when the browser is still gone.
   */
  if(!deps.isBrowserConnected()) {

    LOG.debug("precache", "Skipping a precache cycle: no browser is running.");
    releasePrecacheGuard(deps);

    return;
  }

  const listed = CONFIG.channels.precacheServices;
  const slugs = (scope.kind === "all") ? listed : listed.filter((slug) => scope.slugs.has(slug));

  // A scope that admits no listed service - the list was emptied, or the services a save added were removed again - walks nothing and announces nothing.
  if(slugs.length === 0) {

    releasePrecacheGuard(deps);

    return;
  }

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

      /* Stop the same way once the browser is gone, leaving the services this cycle has not reached to the next launch's cycle. No await lies between this check
       * and the browser acquisition precacheService reaches through withProviderGuidePage, so a walk never acquires a browser this check found gone and the
       * scheduler never launches one; an await inserted between them breaks that guarantee.
       */
      if(!deps.isBrowserConnected()) {

        break;
      }

      const provider = deps.getProviderBySlug(slug);

      if(!provider) {

        continue;
      }

      // Skip a service missing from the running list. The list is read at each turn rather than once when the cycle fired, because a save commits a new list
      // while a cycle can be walking.
      if(!CONFIG.channels.precacheServices.includes(slug)) {

        LOG.debug("precache", "Skipping precache for %s: it left the precache list during the cycle.", provider.label);

        continue;
      }

      // Skip services not in the active service filter. The running filter rather than the persisted list, through the predicate every other filter reader uses,
      // so the cycle skips exactly the services they hide. Their stored config is preserved for when the filter changes back.
      if(!isServiceTagEnabled(slug)) {

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
