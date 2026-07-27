/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * precaching.revalidation.test.ts: Unit tests for the post-login revalidation flow (revalidateDomainAuth), the login-mode minimize guard, and the guarded guide-page
 * session (withProviderGuidePage) in precaching.ts. Running a revalidation or a guide-page walk drives getCurrentBrowser/newPage, which would launch a real Chrome;
 * precaching.ts accepts its browser accessors, provider-registry lookups, and the discovery-phase overlay-poll launcher as an injected PrecachingDeps parameter, so we
 * substitute stubs at that PrecachingDeps injection point and never drive a browser. The injected startOverlayHandling stub records each poll's options, so the
 * guide-page tests observe the discovery phase and its abort timing without a live poll. The health and login modules are real - state assertions go through
 * getDomainAuthState, and login mode is driven through the real startLoginMode/clearLoginState with stub accessors.
 */
import type { Browser, Page } from "puppeteer-core";
import type { DiscoveredChannel, ProviderModule } from "../types/index.ts";
import { LOG, extractDomain } from "../utils/index.ts";
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { clearLoginState, setBrowserAccessors, startLoginMode } from "./login.ts";
import { getDomainAuthState, markDomainAuthRequired } from "../config/health.ts";
import { precacheService, revalidateDomainAuth, startPrecaching, stopPrecaching, withProviderGuidePage } from "./precaching.ts";
import { CONFIG } from "../config/index.ts";
import type { Nullable } from "../types/index.ts";
import type { PrecachingDeps } from "./precaching.ts";
import type { StartOverlayHandlingOptions } from "./consent.ts";
import assert from "node:assert/strict";
import { setImmediate as immediate } from "node:timers/promises";

// Mutable state the deps stubs read, so each test can shape the provider registry and browser behavior without re-registering stubs.
let mockGuideUrls: Record<string, string> = {};
let mockProviders: Record<string, ProviderModule> = {};
let minimizeCalls = 0;
let stubBrowser: Browser;

// The overlay-handling options recorded by the injected startOverlayHandling stub (in call order), and an ordered log of the page operations the guarded session
// performs, so the withProviderGuidePage tests can assert the phase, the abort state, and the mute-before-navigation ordering without a live Chrome.
let overlayHandlingCalls: StartOverlayHandlingOptions[] = [];
let pageEvents: string[] = [];

/* The injected precaching dependencies: the browser accessors and page bookkeeping, the provider-registry lookups, and the discovery-phase overlay-poll launcher,
 * substituted at precaching's PrecachingDeps boundary so revalidation and discovery run against stubs with no real Chrome. Each field reads the mutable module state
 * above at call time, so a test shapes the registry and browser behavior by reassigning those lets. startOverlayHandling stands in for the real poll, recording each
 * call's options (phase and abort signal) into overlayHandlingCalls and logging its launch into pageEvents so the guide-page tests can pin the discovery phase and
 * its abort timing. Typed as the production port so the doubles cannot drift. The health and login modules stay real.
 */
const deps: PrecachingDeps = {

  getCurrentBrowser: async (): Promise<Browser> => stubBrowser,
  getProviderBySlug: (slug: string): ProviderModule | undefined => mockProviders[slug],
  getProvidersForDomain: (domain: string): ProviderModule[] => Object.entries(mockGuideUrls)
    .filter(([ , guideUrl ]) => extractDomain(guideUrl) === domain).flatMap(([slug]) => mockProviders[slug] ?? []),
  isGracefulShutdown: (): boolean => false,
  minimizeBrowserWindow: async (): Promise<void> => {

    minimizeCalls++;
  },
  registerManagedPage: (): void => { /* Stub pages need no bookkeeping. */ },
  startOverlayHandling: async (_page: Page, _profile: unknown, options: StartOverlayHandlingOptions): Promise<void> => {

    pageEvents.push("poll:" + options.phase);
    overlayHandlingCalls.push(options);
  },
  unregisterManagedPage: (): void => { /* Stub pages need no bookkeeping. */ }
};

// Builds a stub Page satisfying the surface the guarded guide-page session touches. The evaluate stub reports "no consent overlay / no containers" so empty
// discoveries classify unknown; the revalidation happy paths return non-empty discoveries and never reach classification. Every page operation pushes to pageEvents
// so the withProviderGuidePage tests can assert the order of the mute injection, the navigation, and the close.
function makeStubPage(): Page {

  return {

    close: async (): Promise<void> => { pageEvents.push("close"); },
    evaluate: async (): Promise<unknown> => false,
    evaluateOnNewDocument: async (): Promise<void> => { pageEvents.push("mute"); },
    goto: async (): Promise<void> => { pageEvents.push("goto"); },
    isClosed: (): boolean => false,
    url: (): string => "https://www.stub-revalidate.test/guide"
  } as unknown as Page;
}

/* Builds a stub ProviderModule for the revalidation flow. handlesOwnNavigation skips page.goto (a stub page has nothing to navigate), and strategy is present so
 * precacheService's optional clearCache call has an object to probe. The double-cast documents that the flow touches this subset, not the full provider surface.
 */
function makeStubProvider(discoverChannels: (page: Page) => Promise<DiscoveredChannel[]>): ProviderModule {

  return {

    discoverChannels,
    guideUrl: "https://www.stub-revalidate.test/guide",
    handlesOwnNavigation: true,
    label: "Stub Revalidate",
    slug: "stub-revalidate",
    strategy: {}
  } as unknown as ProviderModule;
}

// One discovered channel - enough for recordDiscoveryOutcome's non-empty arm to mark the domain verified.
const ONE_CHANNEL = [{ channelSelector: "Stub", name: "Stub" }] as unknown as DiscoveredChannel[];

// Minimal login-page stub for driving the real startLoginMode in the login-mode-active tests, mirroring the login.test.ts stub shape.
function makeLoginPageStub(): Page {

  return {

    close: async (): Promise<void> => { /* Nothing to close on a stub. */ },
    goto: async (): Promise<void> => { /* Nothing to navigate on a stub. */ },
    isClosed: (): boolean => false,
    on: (): void => { /* Close-handler registration is irrelevant here. */ }
  } as unknown as Page;
}

describe("revalidateDomainAuth", () => {

  let originalServices: string[];

  beforeEach(() => {

    originalServices = CONFIG.channels.precacheServices;
    CONFIG.channels.precacheServices = [];

    minimizeCalls = 0;
    mockGuideUrls = { "stub-revalidate": "https://www.stub-revalidate.test/guide" };
    mockProviders = { "stub-revalidate": makeStubProvider(async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL) };
    stubBrowser = { newPage: async (): Promise<Page> => makeStubPage() } as unknown as Browser;

    clearLoginState();

    // Suppress the health flush debounce timer and the precache scheduling timer so nothing fires against a real data directory after the test ends.
    mock.timers.enable({ apis: ["setTimeout"] });
  });

  afterEach(() => {

    stopPrecaching();
    clearLoginState();
    CONFIG.channels.precacheServices = originalServices;
    mock.timers.reset();
  });

  test("is a no-op when the domain is not flagged needs-sign-in", async (t) => {

    /* Traced path: the getDomainAuthState status guard at the top of revalidateDomainAuth. With no entry for the domain, the function must return before the
     * discovery INFO line and before any provider work - a mutation dropping the guard would run a discovery for every login session.
     */
    const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });

    await revalidateDomainAuth("https://www.stub-revalidate.test/somewhere", deps);

    assert.equal(getDomainAuthState("stub-revalidate.test"), null, "no state appears from nowhere");
    assert.equal(info.mock.calls.length, 0, "no discovery is announced for an unflagged domain");
  });

  test("skips with a debug line when login mode is active again (the sequential sign-in flow)", async () => {

    /* Traced path: the isLoginModeActive() guard. The wizard's sequential sign-in flow re-enters login mode immediately after ending it; revalidating mid-wizard
     * would open a discovery page under the user. The final Done fires the observer with login mode inactive, so deferring loses nothing.
     */
    setBrowserAccessors({

      getBrowserInstance: (): Nullable<Browser> => ({ connected: true, newPage: async (): Promise<Page> => makeLoginPageStub() } as unknown as Browser),
      minimizeBrowserWindow: async (): Promise<void> => { /* Not measured here. */ }
    });

    markDomainAuthRequired("stub-revalidate.test");

    await startLoginMode("https://www.stub-revalidate.test/login");
    await revalidateDomainAuth("https://www.stub-revalidate.test/login", deps);

    assert.equal(getDomainAuthState("stub-revalidate.test")?.status, "needsLogin", "the flag stays set while login mode is active");
  });

  test("defers to an in-flight precache cycle at INFO without touching the flag", async (t) => {

    /* Traced path: the precacheInProgress guard. startPrecaching sets the single-flight flag before its delay timer fires (the timer is mocked and never runs), so
     * the revalidation must take the deferral branch - log at INFO and leave the flag for the cycle's own discovery to clear.
     */
    const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });

    CONFIG.channels.precacheServices = ["stub-revalidate"];
    startPrecaching(deps);

    markDomainAuthRequired("stub-revalidate.test");

    await revalidateDomainAuth("https://www.stub-revalidate.test/login", deps);

    assert.equal(getDomainAuthState("stub-revalidate.test")?.status, "needsLogin", "the flag is left for the in-flight cycle");

    const deferralLine = info.mock.calls.find((call) => String(call.arguments[0]).includes("Deferring the post-login revalidation"));

    assert.ok(deferralLine, "the deferral is reported at INFO");
  });

  test("skips quietly when no provider guide matches the domain", async () => {

    // Traced path: the empty-providers early return after registry matching - a flagged domain with no registered provider has nothing to revalidate against.
    mockGuideUrls = {};

    markDomainAuthRequired("orphan-flag.test");

    await revalidateDomainAuth("https://www.orphan-flag.test/login", deps);

    assert.equal(getDomainAuthState("orphan-flag.test")?.status, "needsLogin", "the flag stays; only discovery evidence clears it");
  });

  test("runs discovery for the matching provider and clears the flag to verified on success (the happy path)", async () => {

    /* Traced path: the full flow - flag present, no guards trip, the provider matches by extracted guide domain, precacheService discovers a non-empty lineup, and
     * recordDiscoveryOutcome's non-empty arm marks the domain verified through the criterion-1 chokepoint. This is the needsLogin -> verified round trip the
     * login-end observer exists to produce.
     */
    markDomainAuthRequired("stub-revalidate.test");

    await revalidateDomainAuth("https://www.stub-revalidate.test/login", deps);

    assert.equal(getDomainAuthState("stub-revalidate.test")?.status, "verified", "success evidence overwrites the flag to verified");
  });

  test("holds the single-flight guard while running: a cycle scheduled mid-revalidation defers", async (t) => {

    /* Traced path: the guard acquisition (precacheInProgress = true) happens synchronously before the first await in the revalidation flow, so a startPrecaching
     * call arriving mid-revalidation must hit its own already-in-progress debug branch and schedule nothing. After the revalidation's finally releases the guard,
     * a fresh startPrecaching proceeds normally. This closes the crash-relaunch-cycle-overlap race the guard exists for.
     */
    const debug = t.mock.method(LOG, "debug", () => { /* Captured via the mock. */ });
    const gate = Promise.withResolvers<DiscoveredChannel[]>();

    mockProviders = { "stub-revalidate": makeStubProvider(async (): Promise<DiscoveredChannel[]> => gate.promise) };

    markDomainAuthRequired("stub-revalidate.test");

    const inFlight = revalidateDomainAuth("https://www.stub-revalidate.test/login", deps);

    // The guard is held; a cycle request now must defer.
    CONFIG.channels.precacheServices = ["stub-revalidate"];
    startPrecaching(deps);

    const deferralLine = debug.mock.calls.find((call) => String(call.arguments[1]).includes("already in progress"));

    assert.ok(deferralLine, "the cycle deferred while the revalidation held the guard");

    // Release the discovery; the revalidation completes, clears the flag, and releases the guard.
    gate.resolve(ONE_CHANNEL);
    await inFlight;

    assert.equal(getDomainAuthState("stub-revalidate.test")?.status, "verified", "the gated discovery still cleared the flag");
  });

  test("contains a failing provider (still behind the wall), leaves the flag set, and never rejects", async (t) => {

    /* Traced path: the per-provider try/catch inside the revalidation loop. A provider still behind its wall commonly times out the guide navigation; the failure
     * is contained at WARN, the flag stays for the next evidence source, and the promise resolves (the observer wiring voids it, so a rejection would surface as
     * an unhandled rejection in production).
     */
    const warn = t.mock.method(LOG, "warn", () => { /* Captured via the mock. */ });

    mockProviders = { "stub-revalidate": makeStubProvider(async (): Promise<DiscoveredChannel[]> => {

      throw new Error("Navigation timeout of 30000 ms exceeded");
    }) };

    markDomainAuthRequired("stub-revalidate.test");

    await assert.doesNotReject(() => revalidateDomainAuth("https://www.stub-revalidate.test/login", deps), "revalidateDomainAuth never rejects");

    assert.equal(getDomainAuthState("stub-revalidate.test")?.status, "needsLogin", "the flag stays set when the wall is still up");
    assert.ok(warn.mock.calls.length >= 1, "the per-provider failure is reported at WARN");
  });

  test("returns without running discovery during graceful shutdown", async () => {

    /* Traced path: the isGracefulShutdown() guard in revalidateDomainAuth, reached only after the flagged-domain and login-inactive guards pass. Discovery opens
     * browser pages via getCurrentBrowser(), which would relaunch Chrome after teardown closed it, so the shutdown guard must return before the provider lookup.
     */
    let providerLookups = 0;
    const shutdownDeps: PrecachingDeps = {

      ...deps,
      getProvidersForDomain: (domain: string): ProviderModule[] => {

        providerLookups++;

        return deps.getProvidersForDomain(domain);
      },
      isGracefulShutdown: (): boolean => true
    };

    markDomainAuthRequired("stub-revalidate.test");

    await revalidateDomainAuth("https://www.stub-revalidate.test/login", shutdownDeps);

    assert.equal(providerLookups, 0, "the provider lookup is never reached during shutdown");
    assert.equal(getDomainAuthState("stub-revalidate.test")?.status, "needsLogin", "the flag stays set when discovery is skipped");
  });
});

describe("precacheService - login-mode minimize guard", () => {

  beforeEach(() => {

    minimizeCalls = 0;
    stubBrowser = { newPage: async (): Promise<Page> => makeStubPage() } as unknown as Browser;

    clearLoginState();
    mock.timers.enable({ apis: ["setTimeout"] });
  });

  afterEach(() => {

    clearLoginState();
    mock.timers.reset();
  });

  test("skips the window re-minimize while login mode is active", async () => {

    /* Traced path: the isLoginModeActive() guard in precacheService's finally. A discovery finishing mid-login (a revalidation racing the wizard, or a cycle
     * overlapping a login) must never minimize the window under the user.
     */
    setBrowserAccessors({

      getBrowserInstance: (): Nullable<Browser> => ({ connected: true, newPage: async (): Promise<Page> => makeLoginPageStub() } as unknown as Browser),
      minimizeBrowserWindow: async (): Promise<void> => { /* login.ts's own minimize path is not under test. */ }
    });

    await startLoginMode("https://www.stub-revalidate.test/login");

    await precacheService(makeStubProvider(async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL), deps);

    assert.equal(minimizeCalls, 0, "no minimize while login mode is active");
  });

  test("re-minimizes the window when login mode is inactive", async () => {

    // The complementary arm: with login mode inactive, the pre-existing re-minimize behavior is unchanged.
    await precacheService(makeStubProvider(async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL), deps);

    assert.equal(minimizeCalls, 1, "the discovery page cleanup re-minimizes as before");
  });
});

describe("startPrecaching - graceful-shutdown guard", () => {

  let originalServices: string[];

  beforeEach(() => {

    originalServices = CONFIG.channels.precacheServices;
    stubBrowser = { newPage: async (): Promise<Page> => makeStubPage() } as unknown as Browser;

    // Ensure no prior test left the single-flight guard set, so the positive-control schedule below is not swallowed by the already-in-progress branch.
    stopPrecaching();
    clearLoginState();
    mock.timers.enable({ apis: ["setTimeout"] });
  });

  afterEach(() => {

    stopPrecaching();
    clearLoginState();
    CONFIG.channels.precacheServices = originalServices;
    mock.timers.reset();
  });

  test("schedules no timer during graceful shutdown even with configured precache services", (t) => {

    /* Traced path: the isGracefulShutdown() guard between the empty-services check and the precacheInProgress flag. launchBrowser() can be reached during teardown;
     * without this guard the scheduled cycle would fire after the browser is closed and relaunch Chrome. We spy setTimeout so a queued cycle is directly observable,
     * then prove the guard is the sole gate by scheduling normally the moment it is lifted.
     */
    const scheduled = t.mock.method(globalThis, "setTimeout", (): ReturnType<typeof setTimeout> => 0 as unknown as ReturnType<typeof setTimeout>);
    const shutdownDeps: PrecachingDeps = { ...deps, isGracefulShutdown: (): boolean => true };

    CONFIG.channels.precacheServices = ["stub-revalidate"];

    startPrecaching(shutdownDeps);

    assert.equal(scheduled.mock.calls.length, 0, "no precache cycle is scheduled while shutting down");

    // Lift only the shutdown guard: the identical call now schedules exactly one cycle, proving the guard was the sole gate keeping the timer off the queue.
    startPrecaching(deps);

    assert.equal(scheduled.mock.calls.length, 1, "the same configuration schedules a cycle once shutdown clears");
  });
});

describe("runPrecacheCycle - deps threading through the internal precacheService call", () => {

  let originalServices: string[];

  beforeEach(() => {

    originalServices = CONFIG.channels.precacheServices;

    minimizeCalls = 0;
    mockGuideUrls = { "stub-revalidate": "https://www.stub-revalidate.test/guide" };
    mockProviders = { "stub-revalidate": makeStubProvider(async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL) };
    stubBrowser = { newPage: async (): Promise<Page> => makeStubPage() } as unknown as Browser;

    stopPrecaching();
    clearLoginState();
  });

  afterEach(() => {

    stopPrecaching();
    clearLoginState();
    CONFIG.channels.precacheServices = originalServices;
  });

  test("threads the injected deps through to precacheService rather than falling back to defaultPrecachingDeps", async (t) => {

    /* Traced path: runPrecacheCycle's per-service call site, `precacheService(provider, deps)`. precacheService's own signature defaults its second parameter to
     * defaultPrecachingDeps, so a call site that drops deps silently falls back to the module's real browser accessors instead of the cycle's injected stub - in
     * production this is behavior-neutral (defaultPrecachingDeps IS the real accessors), but it would defeat the PrecachingDeps injection for exactly this test, since a
     * regression here would attempt a real Chrome launch through defaultPrecachingDeps.getCurrentBrowser rather than ever touching the stub deps below. We prove
     * the injected deps reach precacheService by instrumenting getCurrentBrowser - the first collaborator precacheService calls - on a deps copy distinct from the
     * module-level `deps` object the other describe blocks share, so a regression cannot hide behind a call the shared object's own getCurrentBrowser happens to
     * satisfy.
     *
     * We capture the cycle's scheduled setTimeout callback directly via t.mock.method (the same spy technique the graceful-shutdown-guard test above uses) and
     * invoke it ourselves, rather than driving it through mock.timers' virtual clock - the callback fires synchronously either way, and capture-and-invoke needs
     * no per-test enable/reset pair, so it cannot interact with whatever timer state an unrelated earlier test in this file left behind. The single configured
     * service slug means the cycle's loop runs exactly once and terminates on its own; no re-scheduled timer or second pass follows.
     */
    let getCurrentBrowserCalls = 0;
    let scheduledCallback: (() => void) | undefined;

    t.mock.method(globalThis, "setTimeout", (callback: () => void): ReturnType<typeof setTimeout> => {

      scheduledCallback = callback;

      return 0 as unknown as ReturnType<typeof setTimeout>;
    });

    const fakeDeps: PrecachingDeps = {

      ...deps,
      getCurrentBrowser: async (): Promise<Browser> => {

        getCurrentBrowserCalls++;

        return stubBrowser;
      }
    };

    CONFIG.channels.precacheServices = ["stub-revalidate"];

    startPrecaching(fakeDeps);

    assert.ok(scheduledCallback, "startPrecaching schedules the cycle");
    scheduledCallback();

    // Bounded macrotask drain: setImmediate always fires after the entire microtask queue - including continuations queued while draining - has emptied, so two
    // hops give ample margin for precacheService's full await chain (getCurrentBrowser -> newPage -> discoverChannels -> recordDiscoveryOutcome -> page.close ->
    // minimizeBrowserWindow) to settle before we assert.
    await new Promise<void>((resolve) => setImmediate(resolve));
    await new Promise<void>((resolve) => setImmediate(resolve));

    assert.equal(getCurrentBrowserCalls, 1, "the cycle's internal precacheService call received the injected deps rather than defaultPrecachingDeps");
    assert.equal(getDomainAuthState("stub-revalidate.test")?.status, "verified", "the walk ran to completion against the stub browser, not a real launch");
  });
});

describe("precacheService - navigation and cleanup", () => {

  beforeEach(() => {

    minimizeCalls = 0;

    clearLoginState();
    mock.timers.enable({ apis: ["setTimeout"] });
  });

  afterEach(() => {

    clearLoginState();
    mock.timers.reset();
  });

  test("navigates to the guide URL when the provider does not handle its own navigation", async () => {

    /* Traced path: the handlesOwnNavigation branch in precacheService. A provider that does not intercept its own navigation relies on precacheService to drive the
     * page to the guide URL before discovery; dropping the goto would leave discovery running against a blank page.
     */
    const gotoCalls: { options: unknown; url: string }[] = [];
    const page = {

      close: async (): Promise<void> => { /* Nothing to close on a stub. */ },
      evaluate: async (): Promise<unknown> => false,
      evaluateOnNewDocument: async (): Promise<void> => { /* The mute injection is a no-op on a stub. */ },
      goto: async (url: string, options: unknown): Promise<void> => {

        gotoCalls.push({ options, url });
      },
      isClosed: (): boolean => false,
      url: (): string => "https://www.stub-revalidate.test/guide"
    } as unknown as Page;

    stubBrowser = { newPage: async (): Promise<Page> => page } as unknown as Browser;

    const provider = {

      discoverChannels: async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL,
      guideUrl: "https://www.stub-revalidate.test/guide",
      handlesOwnNavigation: false,
      label: "Stub Navigate",
      slug: "stub-navigate",
      strategy: {}
    } as unknown as ProviderModule;

    await precacheService(provider, deps);

    assert.equal(gotoCalls.length, 1, "the navigating provider drives exactly one goto");

    const firstGoto = gotoCalls[0];

    assert.ok(firstGoto, "the navigating provider recorded a goto call");
    assert.equal(firstGoto.url, "https://www.stub-revalidate.test/guide", "the goto targets the provider guide URL");
  });

  test("skips navigation when the provider handles its own navigation", async () => {

    // Complementary arm: a provider that owns its navigation (setting up interception before navigating) must not have precacheService drive a second goto.
    let gotoCalls = 0;
    const page = {

      close: async (): Promise<void> => { /* Nothing to close on a stub. */ },
      evaluate: async (): Promise<unknown> => false,
      evaluateOnNewDocument: async (): Promise<void> => { /* The mute injection is a no-op on a stub. */ },
      goto: async (): Promise<void> => {

        gotoCalls++;
      },
      isClosed: (): boolean => false,
      url: (): string => "https://www.stub-revalidate.test/guide"
    } as unknown as Page;

    stubBrowser = { newPage: async (): Promise<Page> => page } as unknown as Browser;

    await precacheService(makeStubProvider(async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL), deps);

    assert.equal(gotoCalls, 0, "an own-navigation provider triggers no precacheService goto");
  });

  test("resolves when page.close throws after the browser disconnects during discovery", async () => {

    /* Traced path: the try/catch around page.close() in precacheService's finally. If the browser disconnects mid-discovery the page is already gone and close()
     * rejects; swallowing it keeps a per-service teardown failure from turning a successful discovery into a rejected precache.
     */
    const page = {

      close: async (): Promise<void> => {

        throw new Error("Target closed");
      },
      evaluate: async (): Promise<unknown> => false,
      evaluateOnNewDocument: async (): Promise<void> => { /* The mute injection is a no-op on a stub. */ },
      isClosed: (): boolean => false,
      url: (): string => "https://www.stub-revalidate.test/guide"
    } as unknown as Page;

    stubBrowser = { newPage: async (): Promise<Page> => page } as unknown as Browser;

    await assert.doesNotReject(() => precacheService(makeStubProvider(async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL), deps),
      "a close() failure never rejects the precache");
  });
});

describe("withProviderGuidePage", () => {

  beforeEach(() => {

    overlayHandlingCalls = [];
    pageEvents = [];
    minimizeCalls = 0;
    stubBrowser = { newPage: async (): Promise<Page> => makeStubPage() } as unknown as Browser;

    clearLoginState();
    mock.timers.enable({ apis: ["setTimeout"] });
  });

  afterEach(() => {

    clearLoginState();
    mock.timers.reset();
  });

  /* Builds a stub ProviderModule for the guarded guide-page session. handlesOwnNavigation controls whether the helper drives page.goto or the provider is presumed
   * to navigate inside discoverChannels. The double-cast documents that the session touches this subset, not the full provider surface.
   */
  function guideProvider(handlesOwnNavigation: boolean, discoverChannels: (page: Page) => Promise<DiscoveredChannel[]>): ProviderModule {

    return {

      discoverChannels,
      guideUrl: "https://www.stub-guide.test/guide",
      handlesOwnNavigation,
      label: "Stub Guide",
      slug: "stub-guide",
      strategy: {}
    } as unknown as ProviderModule;
  }

  test("installs the mute override and launches the discovery poll before navigation, then hands afterWalk a poll-quiet page", async () => {

    /* Traced path: the helper's happy sequence for a caller-navigated provider. The event log pins the mute-before-navigation and poll-before-navigation ordering,
     * the recorded poll pins the discovery phase, and the abort snapshot taken inside afterWalk pins that the poll is already stopped before any classification runs -
     * a resolved-without-throwing check would prove none of these.
     */
    let signalAbortedInAfterWalk: boolean | null = null;

    const provider = guideProvider(false, async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL);

    const channels = await withProviderGuidePage(provider, {

      afterWalk: async (): Promise<void> => {

        const firstPoll = overlayHandlingCalls[0];

        signalAbortedInAfterWalk = firstPoll ? (firstPoll.signal?.aborted ?? null) : null;
      }
    }, deps);

    assert.deepEqual(channels, ONE_CHANNEL, "the walk's channels are returned");
    assert.equal(overlayHandlingCalls.length, 1, "exactly one overlay poll was launched");

    const firstPoll = overlayHandlingCalls[0];

    assert.ok(firstPoll, "the overlay poll was recorded");
    assert.equal(firstPoll.phase, "discovery", "the guide walk runs under the discovery phase");
    assert.ok(pageEvents.indexOf("mute") < pageEvents.indexOf("goto"), "the mute override installs before navigation");
    assert.ok(pageEvents.indexOf("poll:discovery") < pageEvents.indexOf("goto"), "the discovery poll launches before navigation");
    assert.equal(signalAbortedInAfterWalk, true, "the overlay poll is aborted before afterWalk classifies the page");
  });

  test("launches the discovery poll for a handlesOwnNavigation provider without a caller-driven navigation", async () => {

    // A handlesOwnNavigation provider navigates inside discoverChannels, so the helper drives no goto - but the discovery poll still launches (before that internal
    // navigation) and survives it by the tick-error taxonomy.
    const provider = guideProvider(true, async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL);

    await withProviderGuidePage(provider, {}, deps);

    const firstPoll = overlayHandlingCalls[0];

    assert.ok(firstPoll, "the overlay poll was recorded");
    assert.equal(firstPoll.phase, "discovery", "the walk still runs under the discovery phase");
    assert.ok(pageEvents.includes("poll:discovery"), "the discovery poll launches");
    assert.ok(!pageEvents.includes("goto"), "the helper drives no navigation for a handlesOwnNavigation provider");
  });

  test("throws and closes the page without navigating when the caller has already aborted", async () => {

    // Traced path: the pre-navigation early-abort guard. The listener cannot have closed the page (an already-aborted signal never fires the abort event), so the
    // guard must throw and the finally must close the page - and no mute, poll, or navigation happens.
    const controller = new AbortController();

    controller.abort();

    const provider = guideProvider(false, async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL);

    await assert.rejects(withProviderGuidePage(provider, { signal: controller.signal }, deps), "an early abort rejects so the caller can map it to an abort");

    assert.ok(pageEvents.includes("close"), "the just-created page is closed");
    assert.ok(!pageEvents.includes("goto"), "no navigation happens after an early abort");
    assert.ok(!pageEvents.includes("mute"), "no mute injection happens after an early abort");
    assert.equal(overlayHandlingCalls.length, 0, "no overlay poll is launched after an early abort");
  });

  test("closes the page the instant the caller aborts mid-walk", async () => {

    /* Traced path: the close-on-abort listener the helper owns. The walk pends until a gate resolves; aborting while it pends must close the page immediately, before
     * the walk completes. The pre-abort "not yet closed" check and the post-abort "closed" check pin the close to the abort itself rather than the finally.
     */
    const controller = new AbortController();
    const gate = Promise.withResolvers<DiscoveredChannel[]>();
    const provider = guideProvider(true, async (): Promise<DiscoveredChannel[]> => gate.promise);

    const pending = withProviderGuidePage(provider, { signal: controller.signal }, deps);

    // Let the helper advance to the pending walk.
    await immediate();
    await immediate();

    assert.ok(!pageEvents.includes("close"), "the page is still open mid-walk");

    controller.abort();

    // Let the close-on-abort listener's page.close() run.
    await immediate();

    assert.ok(pageEvents.includes("close"), "the abort closed the page mid-walk");

    // Release the walk so the helper unwinds cleanly.
    gate.resolve(ONE_CHANNEL);

    await pending.catch(() => { /* The unwind path is not under test here. */ });
  });

  test("closes the page when the walk fails", async () => {

    // Traced path: the finally cleanup on a rejected walk. A discoverChannels failure must still close the page and propagate the error.
    const provider = guideProvider(true, async (): Promise<DiscoveredChannel[]> => {

      throw new Error("walk failed");
    });

    await assert.rejects(withProviderGuidePage(provider, {}, deps), /walk failed/);

    assert.ok(pageEvents.includes("close"), "the failed walk still closes the page");
  });
});
