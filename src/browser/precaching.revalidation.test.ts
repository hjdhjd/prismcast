/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * precaching.revalidation.test.ts: Unit tests for the post-login revalidation flow (revalidateDomainAuth), the window sync on discovery-page cleanup, and the
 * guarded guide-page session (withProviderGuidePage) in precaching.ts. Running a revalidation or a guide-page walk drives getCurrentBrowser/newPage, which would
 * launch a real Chrome; precaching.ts accepts its browser accessors, provider-registry lookups, and the discovery-phase overlay-poll launcher as an injected
 * PrecachingDeps parameter, so we substitute stubs at that PrecachingDeps injection point and never drive a browser. The injected startOverlayHandling stub records
 * each poll's options, so the guide-page tests observe the discovery phase and its abort timing without a live poll. The health and login modules are real - state
 * assertions go through getDomainAuthState, and login mode is driven through the real startLoginMode/clearLoginState with stub accessors.
 *
 * The same injection point carries two further surfaces: the lineup write the discovery-outcome recorder performs, observed rather than executed, and the
 * empty-walk retry the guarded session owns, whose rows drive a stub page that records what it was asked to do and a provider whose successive walks are scripted.
 */
import type { Browser, Page } from "puppeteer-core";
import type { DiscoveredChannel, ProviderModule } from "../types/index.ts";
import { LOG, extractDomain } from "../utils/index.ts";
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { clearLoginState, setBrowserAccessors, startLoginMode } from "./login.ts";
import { getDomainAuthState, markDomainAuthRequired } from "../config/health.ts";
import { precacheService, revalidateDomainAuth, startPrecaching, stopPrecaching, withProviderGuidePage } from "./precaching.ts";
import type { BlockedPageClassification } from "./blockedPage.ts";
import { CONFIG } from "../config/index.ts";
import type { Nullable } from "../types/index.ts";
import type { PersistedLineupChannel } from "../config/providerLineups.ts";
import type { PrecachingDeps } from "./precaching.ts";
import type { StartOverlayHandlingOptions } from "./consent.ts";
import type { TestContext } from "node:test";
import assert from "node:assert/strict";
import { setImmediate as immediate } from "node:timers/promises";

// Mutable state the deps stubs read, so each test can shape the provider registry and browser behavior without re-registering stubs.
let mockGuideUrls: Record<string, string> = {};
let mockProviders: Record<string, ProviderModule> = {};
let windowSyncCalls = 0;
let stubBrowser: Browser;

// The overlay-handling options recorded by the injected startOverlayHandling stub (in call order), and an ordered log of the page operations the guarded session
// performs, so the withProviderGuidePage tests can assert the phase, the abort state, and the mute-before-navigation ordering without a live Chrome.
let overlayHandlingCalls: StartOverlayHandlingOptions[] = [];
let pageEvents: string[] = [];

// The options each newPage call received, in call order, so the guarded session's tests can pin how the guide page is created rather than only what is done to it.
let newPageOptions: unknown[] = [];

// The browser each discovery-page creation was handed, in call order. A row reads its length for how many walks opened a page at all, which is what the
// login-mode rows assert on: a deferred service creates none.
let discoveryPageCreations: Browser[] = [];

// The lineup writes the discovery-outcome recorder issues, captured by the injected persistProviderLineup below so the port tests can assert what a completed walk
// hands the store without touching a real file.
const persistedLineups: { channels: PersistedLineupChannel[]; slug: string }[] = [];

/* The injected precaching dependencies: the browser accessors, the discovery-page creator, the page bookkeeping, the layout-surface declaration, the
 * window-visibility sync, the provider-registry lookups, and the discovery-phase overlay-poll launcher, substituted at precaching's PrecachingDeps boundary so
 * revalidation and discovery run against stubs with no real Chrome. Each field reads the mutable module state above at call time, so a test shapes the registry
 * and browser behavior by reassigning those lets. createDiscoveryPage records the browser it was handed and then delegates to that browser's own newPage, so
 * every per-row browser double keeps handing back the page double its row wrote, and what the creator itself puts in the creation options is pinned where the
 * creator lives, in index.test.ts. startOverlayHandling stands in for the real poll, recording each call's options (phase and abort signal) into
 * overlayHandlingCalls and logging its launch into pageEvents so the guide-page tests can pin the discovery phase and its abort timing; emulateLayoutSurface
 * logs itself into the same record and answers with a fixed surface, so the walk's declaration is observable in the page-operation order. Typed as the
 * production port so the doubles cannot drift. The health and login modules stay real.
 */
const deps: PrecachingDeps = {

  createDiscoveryPage: async (browser: Browser): Promise<Page> => {

    discoveryPageCreations.push(browser);

    return browser.newPage();
  },
  emulateLayoutSurface: async (): Promise<{ height: number; width: number }> => {

    pageEvents.push("layout");

    return { height: 1080, width: 1920 };
  },
  getCurrentBrowser: async (): Promise<Browser> => stubBrowser,
  getProviderBySlug: (slug: string): ProviderModule | undefined => mockProviders[slug],
  getProvidersForDomain: (domain: string): ProviderModule[] => Object.entries(mockGuideUrls)
    .filter(([ , guideUrl ]) => extractDomain(guideUrl) === domain).flatMap(([slug]) => mockProviders[slug] ?? []),
  isGracefulShutdown: (): boolean => false,
  persistProviderLineup: async (slug: string, channels: PersistedLineupChannel[]): Promise<void> => {

    persistedLineups.push({ channels, slug });
  },
  registerManagedPage: (): void => { /* Stub pages need no bookkeeping. */ },
  startOverlayHandling: async (_page: Page, _profile: unknown, options: StartOverlayHandlingOptions): Promise<void> => {

    pageEvents.push("poll:" + options.phase);
    overlayHandlingCalls.push(options);
  },
  syncWindowVisibility: async (): Promise<void> => {

    windowSyncCalls++;
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

    windowSyncCalls = 0;
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
      syncWindowVisibility: async (): Promise<void> => { /* Not measured here. */ }
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

describe("precacheService - window sync on discovery-page cleanup", () => {

  beforeEach(() => {

    windowSyncCalls = 0;
    stubBrowser = { newPage: async (): Promise<Page> => makeStubPage() } as unknown as Browser;

    clearLoginState();
    mock.timers.enable({ apis: ["setTimeout"] });
  });

  afterEach(() => {

    clearLoginState();
    mock.timers.reset();
  });

  /* Both login states are exercised at this call site because the call is unconditional: precacheService decides nothing about the window, it asks the policy, and
   * the policy is what accounts for a login session. The login-active arm is the one that proves it - a re-introduced guard would suppress the call there and this
   * test would fail. What the window then ends up as is decideWindowVisibility's login arm, pinned in windowSync.test.ts, not here.
   */
  test("asks for a window sync even while login mode is active", async () => {

    setBrowserAccessors({

      getBrowserInstance: (): Nullable<Browser> => ({ connected: true, newPage: async (): Promise<Page> => makeLoginPageStub() } as unknown as Browser),
      syncWindowVisibility: async (): Promise<void> => { /* login.ts's own sync path is not under test. */ }
    });

    await startLoginMode("https://www.stub-revalidate.test/login");

    await precacheService(makeStubProvider(async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL), deps);

    assert.equal(windowSyncCalls, 1, "the discovery page cleanup syncs the window regardless of login mode");
  });

  test("asks for a window sync when login mode is inactive", async () => {

    // The complementary arm, which the retired login guard already allowed through.
    await precacheService(makeStubProvider(async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL), deps);

    assert.equal(windowSyncCalls, 1, "the discovery page cleanup syncs the window");
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

/* A service that finishes a cycle with nothing gets one more pass, minutes later, once whatever startup contention may have starved it has cleared. The rows here
 * drive the whole schedule on the virtual clock: the cycle fires at its own delay, the re-attempt at the longer one, and cancellation is asserted by advancing past
 * the delay and finding that the walk never happened - not by inspecting a handle.
 *
 * The counter every row reads is precacheService invocations per provider, taken from the cache clear each invocation performs. It counts attempts rather than
 * guide walks, which keeps the assertions about the schedule rather than about the guarded session's own empty-walk retry underneath it.
 */
describe("the deferred discovery re-attempt", () => {

  // How many times precacheService was invoked for each slug, how many discovery walks each provider actually ran, and the channels those walks return. Reset
  // per row.
  let attempts: Record<string, number> = {};
  let walks: Record<string, number> = {};
  let walkResults: Record<string, DiscoveredChannel[]> = {};

  // Which providers report a cached lineup at the moment they are asked, standing in for a lineup that arrived between the cycle and the re-attempt.
  let cachedSlugs = new Set<string>();

  // Every timer the module scheduled but has not had fired or cancelled, keyed by the handle it was given.
  let timers = new Map<number, { callback: () => void; delayMs: number }>();
  let nextHandle = 1;

  let originalServices: string[];

  beforeEach(() => {

    originalServices = CONFIG.channels.precacheServices;
    attempts = {};
    cachedSlugs = new Set();
    discoveryPageCreations = [];
    nextHandle = 1;
    timers = new Map();
    walks = {};
    walkResults = {};
    stubBrowser = { newPage: async (): Promise<Page> => makeStubPage() } as unknown as Browser;

    stopPrecaching();
    clearLoginState();
  });

  afterEach(() => {

    stopPrecaching();
    clearLoginState();
    CONFIG.channels.precacheServices = originalServices;
  });

  /* Captures the module's scheduling instead of running it, which is how this file drives schedules: a captured timer is fired by the row that means to fire it,
   * and a cancelled one is removed the way clearTimeout removes a real one. Capture needs no timer enable/reset pair of its own, so unlike a virtual clock it
   * cannot be disturbed by whatever timer state another suite in this process has installed. node:test restores both methods when the row ends.
   * @param t - The row's test context, which owns the restoration.
   */
  function captureTimers(t: TestContext): void {

    t.mock.method(globalThis, "setTimeout", (callback: () => void, delayMs?: number): unknown => {

      const handle = nextHandle++;

      timers.set(handle, { callback, delayMs: delayMs ?? 0 });

      return handle;
    });

    t.mock.method(globalThis, "clearTimeout", (handle?: unknown): void => {

      timers.delete(handle as number);
    });
  }

  /**
   * Lets every continuation the module queued run to completion, so a row reads settled state rather than a schedule still unwinding. setImmediate is untouched by
   * the capture above, so each hop is a real macrotask boundary after the microtask queue has drained.
   */
  async function drain(): Promise<void> {

    for(let hop = 0; hop < 10; hop++) {

      // eslint-disable-next-line no-await-in-loop
      await immediate();
    }
  }

  /**
   * Fires every captured timer scheduled for the given delay, then drains. A delay nothing was scheduled for fires nothing, which is exactly what a row asserting
   * a cancellation is looking for.
   * @param delayMs - The delay whose timers to fire.
   */
  async function fire(delayMs: number): Promise<void> {

    for(const [ handle, timer ] of Array.from(timers)) {

      if(timer.delayMs !== delayMs) {

        continue;
      }

      timers.delete(handle);
      timer.callback();
    }

    await drain();
  }

  /* Builds a provider whose walks return whatever walkResults holds for its slug and whose cache clear counts the precacheService invocation that performed it.
   * handlesOwnNavigation keeps the stub page free of navigation, and getCachedChannels answers from cachedSlugs so a row can make a lineup appear mid-schedule.
   */
  function deferredProvider(slug: string): ProviderModule {

    return {

      discoverChannels: async (): Promise<DiscoveredChannel[]> => {

        walks[slug] = (walks[slug] ?? 0) + 1;

        return walkResults[slug] ?? [];
      },
      getCachedChannels: (): Nullable<DiscoveredChannel[]> => (cachedSlugs.has(slug) ? ONE_CHANNEL : null),
      guideUrl: "https://www." + slug + ".test/guide",
      handlesOwnNavigation: true,
      label: slug,
      slug,
      strategy: {

        clearCache: (): void => {

          attempts[slug] = (attempts[slug] ?? 0) + 1;
        }
      }
    } as unknown as ProviderModule;
  }

  test("re-attempts only the services the cycle left empty", async (t) => {

    /* The feature in one row: a boot where one provider's lazy content never appeared inside its walk. The service that came back with a lineup is not touched
     * again - re-walking it would cost a heavy SPA load for an answer already in hand - and the empty one gets exactly one more attempt.
     */
    mockProviders = { "deferred-empty": deferredProvider("deferred-empty"), "deferred-full": deferredProvider("deferred-full") };
    walkResults = { "deferred-full": ONE_CHANNEL };
    CONFIG.channels.precacheServices = [ "deferred-empty", "deferred-full" ];

    captureTimers(t);
    startPrecaching(deps);

    await fire(5000);

    assert.deepEqual(attempts, { "deferred-empty": 1, "deferred-full": 1 }, "the cycle attempted both services once");

    // The empty service's lineup shows up on the re-attempt, which is the outcome the delay is betting on.
    walkResults = { "deferred-empty": ONE_CHANNEL, "deferred-full": ONE_CHANNEL };

    await fire(300000);

    assert.deepEqual(attempts, { "deferred-empty": 2, "deferred-full": 1 }, "only the empty service was re-attempted");
  });

  test("skips a service whose lineup arrived in the interval", async (t) => {

    // Five minutes is long enough for a full cycle after a browser relaunch, or for a user to hit the discovery endpoint. Either fills the cache, and the pass has
    // nothing left to do for that service.
    mockProviders = { "deferred-empty": deferredProvider("deferred-empty") };
    CONFIG.channels.precacheServices = ["deferred-empty"];

    captureTimers(t);
    startPrecaching(deps);

    await fire(5000);

    assert.deepEqual(attempts, { "deferred-empty": 1 }, "the cycle attempted the service once");

    cachedSlugs = new Set(["deferred-empty"]);

    await fire(300000);

    assert.deepEqual(attempts, { "deferred-empty": 1 }, "a service that already has a lineup is not walked again");
  });

  test("stopPrecaching cancels the pending pass, and the deferred walk never executes", async (t) => {

    // The shutdown guarantee, asserted by outcome: advance well past the delay and find that nothing ran. A cancellation that only dropped a reference would let
    // the timer fire into a closed browser and relaunch Chrome after teardown.
    mockProviders = { "deferred-empty": deferredProvider("deferred-empty") };
    CONFIG.channels.precacheServices = ["deferred-empty"];

    captureTimers(t);
    startPrecaching(deps);

    await fire(5000);

    stopPrecaching();

    await fire(300000);

    assert.deepEqual(attempts, { "deferred-empty": 1 }, "the cancelled pass never walked");
  });

  test("a fresh cycle supersedes the pending pass", async (t) => {

    // A browser relaunch schedules a full cycle over every configured service, the empty ones included. Letting the deferred pass survive alongside it would put
    // two passes over the same guides in contention for one browser.
    mockProviders = { "deferred-empty": deferredProvider("deferred-empty") };
    CONFIG.channels.precacheServices = ["deferred-empty"];

    captureTimers(t);
    startPrecaching(deps);

    await fire(5000);

    assert.deepEqual(attempts, { "deferred-empty": 1 }, "the first cycle ran");

    walkResults = { "deferred-empty": ONE_CHANNEL };

    captureTimers(t);
    startPrecaching(deps);

    await fire(5000);

    assert.deepEqual(attempts, { "deferred-empty": 2 }, "the fresh cycle ran its own attempt");

    await fire(300000);

    assert.deepEqual(attempts, { "deferred-empty": 2 }, "the superseded pass never fired afterwards");
  });

  test("runs nothing once a graceful shutdown has begun", async (t) => {

    // The per-service check inside the pass, and the one at its entry. Both exist because the pass opens discovery pages, and getCurrentBrowser relaunches the
    // Chrome that teardown just closed.
    let shuttingDown = false;

    const shutdownDeps: PrecachingDeps = { ...deps, isGracefulShutdown: (): boolean => shuttingDown };

    mockProviders = { "deferred-empty": deferredProvider("deferred-empty") };
    CONFIG.channels.precacheServices = ["deferred-empty"];

    captureTimers(t);
    startPrecaching(shutdownDeps);

    await fire(5000);

    assert.deepEqual(attempts, { "deferred-empty": 1 }, "the cycle ran before shutdown began");

    shuttingDown = true;

    await fire(300000);

    assert.deepEqual(attempts, { "deferred-empty": 1 }, "the pass opened no discovery page during teardown");
  });

  test("a full-cycle request that arrives while the guard is held runs once the guard is released", async (t) => {

    /* The dropped-cycle hand-off. A browser crash relaunch calls startPrecaching while a run still holds the guard, and that run is walking guides for a browser
     * whose caches the relaunch just cleared - so its result is worth nothing and the request must not be discarded. The pin is the second cycle actually running,
     * which also proves the release ordering: the reentrant call has to find a free guard, or it would record the very request being honored and schedule nothing.
     */
    const gate = Promise.withResolvers<DiscoveredChannel[]>();

    mockProviders = { "deferred-full": { ...deferredProvider("deferred-full"), discoverChannels: async (): Promise<DiscoveredChannel[]> => gate.promise } };

    CONFIG.channels.precacheServices = ["deferred-full"];

    captureTimers(t);
    startPrecaching(deps);

    await fire(5000);

    assert.deepEqual(attempts, { "deferred-full": 1 }, "the first cycle is in flight");

    // The relaunch's request, arriving while the guard is held.
    startPrecaching(deps);

    // Release the gated walk so the first cycle finishes and its release honors the request.
    mockProviders = { "deferred-full": deferredProvider("deferred-full") };
    walkResults = { "deferred-full": ONE_CHANNEL };

    gate.resolve(ONE_CHANNEL);

    await drain();
    await fire(5000);

    assert.deepEqual(attempts, { "deferred-full": 2 }, "the deferred full cycle ran after the guard was released");
  });

  /* A walk opens a browser window at the shared window's placement, which during a login session is the window the user is signing in through - and a second
   * window over it would take their clicks. So the automatic walks stand aside while a session is on screen and come back for the services afterwards, on the
   * same deferred schedule the rows above drive. The user-initiated browse endpoint is deliberately not gated: the user asked for that window.
   *
   * These rows drive the real login module through startLoginMode and clearLoginState, because the guard production reads is that module's own flag; the timer
   * capture is installed first, so the session's fifteen-minute timeout is captured rather than left running against the process.
   */
  describe("standing aside for a login session", () => {

    /**
     * Starts a real login session against a stub browser, so the cycle and the re-attempt read the flag exactly as production does.
     * @returns A promise that resolves once login mode is active.
     */
    async function startStubLogin(): Promise<void> {

      setBrowserAccessors({

        getBrowserInstance: (): Nullable<Browser> => ({ connected: true, newPage: async (): Promise<Page> => makeLoginPageStub() } as unknown as Browser),
        syncWindowVisibility: async (): Promise<void> => { /* login.ts's own sync path is not under test here. */ }
      });

      await startLoginMode("https://www.deferred-login.test/login");
    }

    test("the cycle defers a service rather than walking it, and the re-attempt walks it once the session ends", async (t) => {

      /* The guard's whole shape in one row. The cycle opens no discovery page and runs no walk while the session is up, says so on its completion line, and
       * hands the service to the same re-attempt an empty walk would have gone to - and the re-attempt, firing after the session ends, does the walk. The
       * completion line is also read for what it must NOT say: a deferred service never walked, so counting it as one that returned no channels would be a
       * different claim about the same slug.
       */
      const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });

      mockProviders = { "deferred-login": deferredProvider("deferred-login") };
      CONFIG.channels.precacheServices = ["deferred-login"];

      captureTimers(t);

      await startStubLogin();

      startPrecaching(deps);

      await fire(5000);

      assert.deepEqual(discoveryPageCreations, [], "the cycle opened no discovery window while the session was on screen");
      assert.deepEqual(walks, {}, "and ran no discovery walk");

      const completionCall = info.mock.calls.find((call) => String(call.arguments[0]).includes("Channel lineup precaching complete"));

      assert.ok(completionCall, "the cycle still reported its completion");

      const completion = completionCall.arguments.map((argument) => String(argument)).join(" ");

      assert.ok(completion.includes("1 deferred for a login session"), "the completion line names what the session deferred");
      assert.ok(!completion.includes("returned no channels"), "a deferred service is not counted as one that walked and found nothing");

      // The session ends and the re-attempt fires: the walk it was owed happens now.
      walkResults = { "deferred-login": ONE_CHANNEL };

      clearLoginState();

      await fire(300000);

      assert.equal(discoveryPageCreations.length, 1, "the re-attempt opened the discovery window it deferred");
      assert.deepEqual(walks, { "deferred-login": 1 }, "and walked the service exactly once");
    });

    test("a re-attempt that meets a login session re-arms every service it still owes, not just the one it stopped on", async (t) => {

      /* The re-arm has to carry the whole remainder. A pass that armed only the slug it collided with would drop every service behind it in the queue, and
       * those services would never be walked at all - so the row queues two, collides on the first, and counts the walks after the session ends.
       */
      mockProviders = { "deferred-first": deferredProvider("deferred-first"), "deferred-second": deferredProvider("deferred-second") };
      CONFIG.channels.precacheServices = [ "deferred-first", "deferred-second" ];

      captureTimers(t);
      startPrecaching(deps);

      await fire(5000);

      // Counted as precacheService invocations, exactly as the rows above count them: an empty walk gets the session's own reload-and-retry, so the walk count
      // for an empty service is two and says nothing about the schedule.
      assert.deepEqual(attempts, { "deferred-first": 1, "deferred-second": 1 }, "the cycle attempted both services, and both came back empty");

      // The session opens inside the re-attempt's delay, so the pass meets it on its first service.
      await startStubLogin();

      discoveryPageCreations = [];
      walks = {};
      walkResults = { "deferred-first": ONE_CHANNEL, "deferred-second": ONE_CHANNEL };

      await fire(300000);

      assert.deepEqual(discoveryPageCreations, [], "the pass opened no discovery window while the session was on screen");
      assert.deepEqual(walks, {}, "and walked nothing");

      clearLoginState();

      await fire(300000);

      assert.equal(discoveryPageCreations.length, 2, "the re-armed pass opened one window per service it still owed");
      assert.deepEqual(walks, { "deferred-first": 1, "deferred-second": 1 }, "both services were walked, not only the one the pass stopped on");
    });

    test("the cycle walks the service normally when no login session is on screen", async (t) => {

      // The guard's other side, so the row above cannot pass by the cycle being broken for every service rather than deferring for this one.
      mockProviders = { "deferred-login": deferredProvider("deferred-login") };
      walkResults = { "deferred-login": ONE_CHANNEL };
      CONFIG.channels.precacheServices = ["deferred-login"];

      captureTimers(t);
      startPrecaching(deps);

      await fire(5000);

      assert.equal(discoveryPageCreations.length, 1, "the cycle opened the discovery window");
      assert.deepEqual(walks, { "deferred-login": 1 }, "and walked the service");
    });
  });
});

describe("runPrecacheCycle - deps threading through the internal precacheService call", () => {

  let originalServices: string[];

  beforeEach(() => {

    originalServices = CONFIG.channels.precacheServices;

    windowSyncCalls = 0;
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
    // the window sync) to settle before we assert.
    await new Promise<void>((resolve) => setImmediate(resolve));
    await new Promise<void>((resolve) => setImmediate(resolve));

    assert.equal(getCurrentBrowserCalls, 1, "the cycle's internal precacheService call received the injected deps rather than defaultPrecachingDeps");
    assert.equal(getDomainAuthState("stub-revalidate.test")?.status, "verified", "the walk ran to completion against the stub browser, not a real launch");
  });
});

describe("precacheService - navigation and cleanup", () => {

  beforeEach(() => {

    windowSyncCalls = 0;

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

/* The lineup write is the durable half of a completed walk, and it reaches the store through the same PrecachingDeps port the browser accessors do. Driving a real
 * walk through precacheService is what makes these rows the port's pin rather than a direct call to the recorder: what is observed is that the walk's own
 * collaborators - not the module's production wiring - are what the write travelled through.
 */
describe("precacheService - the lineup write through the injection port", () => {

  beforeEach(() => {

    persistedLineups.length = 0;
    windowSyncCalls = 0;

    clearLoginState();
    mock.timers.enable({ apis: ["setTimeout"] });
  });

  afterEach(() => {

    clearLoginState();
    mock.timers.reset();
  });

  test("a completed walk hands the provider's durable lineup to the injected write", async () => {

    // The provider states its own durable shape, so what the store receives is the provider's answer rather than a projection the recorder invented for it.
    const durable = [{ channelSelector: "Stub", name: "Stub", watchUrl: "https://www.stub-revalidate.test/watch/stub" }];
    const provider = makeStubProvider(async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL);

    (provider as { exportDurableLineup?: () => PersistedLineupChannel[] }).exportDurableLineup = (): PersistedLineupChannel[] => durable;

    await precacheService(provider, deps);

    assert.deepEqual(persistedLineups, [{ channels: durable, slug: "stub-revalidate" }], "the walk's durable lineup reached the injected write");
  });

  test("a write that rejects leaves the walk's result and its outcome recording untouched", async (t) => {

    /* The containment the feature depends on: the lineup write is fire-and-forget behind a function that absorbs its own failures, and the call site guards the
     * port on top of that, so no implementation a caller injects can turn a successful discovery into a failed one. Without the guard this row would surface as
     * an unhandled rejection rather than a clean pass.
     */
    const rejectingDeps: PrecachingDeps = { ...deps, persistProviderLineup: async (): Promise<void> => Promise.reject(new Error("disk full")) };
    const captured: unknown[] = [];
    const onRejection = (reason: unknown): void => {

      captured.push(reason);
    };

    process.on("unhandledRejection", onRejection);
    t.after(() => process.off("unhandledRejection", onRejection));

    markDomainAuthRequired("stub-revalidate.test");

    const channels = await precacheService(makeStubProvider(async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL), rejectingDeps);

    // Let any rejection the call site failed to guard reach the process handler before the assertions read it.
    await immediate();
    await immediate();

    assert.deepEqual(channels, ONE_CHANNEL, "the walk returns its channels regardless of the write's fate");
    assert.equal(getDomainAuthState("stub-revalidate.test")?.status, "verified", "the outcome recording completed and marked the domain verified");
    assert.deepEqual(captured, [], "the fire-and-forget write never escapes as an unhandled rejection");
  });
});

describe("withProviderGuidePage", () => {

  beforeEach(() => {

    discoveryPageCreations = [];
    newPageOptions = [];
    overlayHandlingCalls = [];
    pageEvents = [];
    windowSyncCalls = 0;

    stubBrowser = {

      newPage: async (options?: unknown): Promise<Page> => {

        newPageOptions.push(options);

        return makeStubPage();
      }
    } as unknown as Browser;

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

  test("takes its page from the discovery-page creator, on the declared layout surface, before it navigates", async () => {

    /* Where the guide page comes from is the pin here. The session asks the browser layer's creator for it exactly once, handing over the browser it acquired,
     * and passes no creation options of its own - the window, the background, and the placement are the creator's to decide, and they are pinned where the
     * creator lives, in index.test.ts. A session that went back to creating the page itself would leave options here rather than the bare undefined its
     * delegation records.
     *
     * The page-operation order carries the second half of the contract. Every guide strategy was written against the preset's dimensions, and a page carries no
     * emulation of its own, so the declaration has to land before the first navigation or the guide lays out once at the window's size and has to be re-laid-out.
     * The prefix is compared exactly rather than by index arithmetic, so a declaration that never happened fails here instead of comparing an index of -1.
     */
    const provider = guideProvider(false, async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL);

    await withProviderGuidePage(provider, {}, deps);

    assert.deepEqual(discoveryPageCreations, [stubBrowser], "the creator was asked exactly once, for the browser the session acquired");
    assert.deepEqual(newPageOptions, [undefined], "the session passes no creation options of its own");
    assert.deepEqual(pageEvents.slice(0, pageEvents.indexOf("goto") + 1), [ "mute", "layout", "poll:discovery", "goto" ],
      "the mute override, the layout declaration, and the overlay poll all precede the first navigation, in that order");
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

/* An empty discovery walk is the failure this arc exists to answer: a rail or grid whose lazy content never populated inside the walk's budget leaves the provider
 * untunable for the life of the process. The session gives it one more attempt, but only when the page it left behind offers no explanation - a confirmed sign-in
 * wall or a standing consent banner explains the emptiness completely, and reloading past that evidence would replace a recordable diagnosis with a fresh,
 * undismissed banner.
 *
 * Every row here observes behavior rather than call counts where it can: what the page was asked to do, how many walks ran, and what the outcome hook was handed.
 */
describe("withProviderGuidePage - the empty-walk retry", () => {

  // The channels each successive walk returns, and the page operations the stub recorded. Reset per test.
  let walkResults: DiscoveredChannel[][] = [];
  let walks = 0;
  let retryEvents: string[] = [];

  // What the outcome hook was handed, one entry per call. A retry that recorded twice, or recorded the wrong walk's result, shows up here.
  let recorded: { channels: DiscoveredChannel[]; classification?: BlockedPageClassification }[] = [];

  beforeEach(() => {

    overlayHandlingCalls = [];
    retryEvents = [];
    recorded = [];
    walkResults = [];
    walks = 0;

    clearLoginState();
    mock.timers.enable({ apis: ["setTimeout"] });
  });

  afterEach(() => {

    clearLoginState();
    mock.timers.reset();
  });

  /* Builds the stub page the retry rows run against. Its evaluate routes on the argument shape, mirroring the precaching.test.ts convention: a string array is the
   * CMP-detect probe, an object carrying maxDepth is the sign-in container collector, and anything else is the embed-gate probe, which reports a located gate by
   * returning a record rather than null. consentPresent flips the CMP probe so a page can be made to classify as a consent overlay; reloadFails makes the reload
   * throw the way a navigation timeout would. Every operation is recorded, which is how the rows below tell "reloaded once" apart from "never reloaded".
   */
  function makeRetryPage(options: { consentPresent?: boolean; reloadFails?: boolean } = {}): Page {

    return {

      $: async (): Promise<unknown> => null,
      close: async (): Promise<void> => { retryEvents.push("close"); },
      evaluate: async (_fn: unknown, arg?: unknown): Promise<unknown> => {

        if(Array.isArray(arg)) {

          return options.consentPresent ?? false;
        }

        return ((typeof arg === "object") && (arg !== null) && ("maxDepth" in arg)) ? [] : null;
      },
      evaluateOnNewDocument: async (): Promise<void> => { retryEvents.push("mute"); },
      goto: async (): Promise<void> => { retryEvents.push("goto"); },
      isClosed: (): boolean => false,
      reload: async (): Promise<void> => {

        retryEvents.push("reload");

        if(options.reloadFails) {

          throw new Error("Navigation timeout of 10000 ms exceeded");
        }
      },
      url: (): string => "https://www.stub-guide.test/guide"
    } as unknown as Page;
  }

  /* Builds a provider whose successive walks hand back walkResults in order, counting each one. authWallIndicators, when set, makes the classifier report an
   * authentication wall off the stub page's own hostname without any DOM to probe.
   */
  function retryProvider(options: { handlesOwnNavigation?: boolean; wallIndicators?: boolean } = {}): ProviderModule {

    return {

      ...(options.wallIndicators === true ? { authWallIndicators: { hosts: ["stub-guide.test"] } } : {}),
      discoverChannels: async (): Promise<DiscoveredChannel[]> => {

        const result = walkResults[walks] ?? [];

        walks++;

        return result;
      },
      guideUrl: "https://www.stub-guide.test/guide",
      handlesOwnNavigation: options.handlesOwnNavigation ?? false,
      label: "Stub Guide",
      slug: "stub-guide",
      strategy: {}
    } as unknown as ProviderModule;
  }

  // The outcome hook every row installs: records what it was handed so the assertions can read the whole call history rather than one flag.
  const afterWalk = async (_page: Page, channels: DiscoveredChannel[], classification?: BlockedPageClassification): Promise<void> => {

    recorded.push({ channels, classification });

    await Promise.resolve();
  };

  test("reloads and walks again when the first walk came back empty and the page explains nothing", async () => {

    /* The incident's shape and the cure: the guide rendered, nothing blocked it, and the lineup simply was not there yet. One reload and one more walk, and the
     * outcome that gets recorded is the second walk's - recorded once, with no classification threaded, so the recorder reads the page the retry actually saw.
     */
    walkResults = [ [], ONE_CHANNEL ];
    stubBrowser = { newPage: async (): Promise<Page> => makeRetryPage() } as unknown as Browser;

    const channels = await withProviderGuidePage(retryProvider(), { afterWalk }, deps);

    assert.equal(walks, 2, "the empty walk was retried exactly once");
    assert.equal(retryEvents.filter((event) => event === "reload").length, 1, "the page was reloaded exactly once before the second walk");
    assert.deepEqual(channels, ONE_CHANNEL, "the retry's result is what the session returns");
    assert.deepEqual(recorded, [{ channels: ONE_CHANNEL, classification: undefined }], "the outcome was recorded once, with the retry's result and no threaded " +
      "classification");
  });

  test("runs a fresh overlay poll for the second walk", async () => {

    // The first walk's poll is aborted before the classification, and an aborted signal makes the poll a silent no-op on entry - so reusing it would leave the
    // retry walking an unprotected page, which is exactly the page a cookie banner reappears on after a reload.
    walkResults = [ [], ONE_CHANNEL ];
    stubBrowser = { newPage: async (): Promise<Page> => makeRetryPage() } as unknown as Browser;

    await withProviderGuidePage(retryProvider(), { afterWalk }, deps);

    assert.equal(overlayHandlingCalls.length, 2, "one poll per walk");
    assert.deepEqual(overlayHandlingCalls.map((call) => call.phase), [ "discovery", "discovery" ], "both polls run under the discovery phase");
    assert.equal(overlayHandlingCalls[1]?.signal?.aborted, true, "the retry's poll is aborted once its walk completes");
  });

  test("records the reloaded page's outcome when both walks come back empty", async () => {

    /* Both-walks-empty is the path where the threaded classification must stay absent: the page the recorder is handed is the reloaded one, so a classification
     * computed before the reload would describe a page that no longer exists.
     */
    walkResults = [ [], [] ];
    stubBrowser = { newPage: async (): Promise<Page> => makeRetryPage() } as unknown as Browser;

    const channels = await withProviderGuidePage(retryProvider(), { afterWalk }, deps);

    assert.equal(walks, 2, "the retry ran");
    assert.deepEqual(channels, [], "an empty retry returns empty");
    assert.deepEqual(recorded, [{ channels: [], classification: undefined }],
      "the outcome was recorded once, with no classification threaded, so the recorder classifies the reloaded page itself");
  });

  test("records the first walk's authentication-wall classification without reloading", async () => {

    // A sign-in wall explains the empty result completely and no reload can clear it. The evidence is on the page as it stands, so the classification travels to
    // the recorder rather than being re-derived after a navigation that would have thrown it away.
    walkResults = [[]];
    stubBrowser = { newPage: async (): Promise<Page> => makeRetryPage() } as unknown as Browser;

    await withProviderGuidePage(retryProvider({ wallIndicators: true }), { afterWalk }, deps);

    assert.equal(walks, 1, "a blocked page is not retried");
    assert.ok(!retryEvents.includes("reload"), "the page was never reloaded");
    assert.equal(recorded.length, 1, "the outcome was recorded once");
    assert.equal(recorded[0]?.classification?.kind, "authWall", "the first walk's classification is what the recorder receives");
  });

  test("records the first walk's consent-overlay classification without reloading", async () => {

    // The other blocked arm. A banner the discovery poll could not clear is the standing obstacle, and reloading would put a fresh, undismissed one in front of
    // the recorder - the diagnosis would survive but the walk that produced it would be wasted.
    walkResults = [[]];
    stubBrowser = { newPage: async (): Promise<Page> => makeRetryPage({ consentPresent: true }) } as unknown as Browser;

    await withProviderGuidePage(retryProvider(), { afterWalk }, deps);

    assert.equal(walks, 1, "a blocked page is not retried");
    assert.ok(!retryEvents.includes("reload"), "the page was never reloaded");
    assert.equal(recorded[0]?.classification?.kind, "consentOverlay", "the first walk's classification is what the recorder receives");
  });

  test("falls back to the first walk's outcome when the reload throws", async () => {

    // Known evidence is never lost to a throwing reload, and a page in an unknown state is never walked again. The failure is absorbed rather than propagated -
    // the caller asked for a discovery, and it has one.
    walkResults = [[]];
    stubBrowser = { newPage: async (): Promise<Page> => makeRetryPage({ reloadFails: true }) } as unknown as Browser;

    const channels = await withProviderGuidePage(retryProvider(), { afterWalk }, deps);

    assert.deepEqual(channels, [], "the session still returns the first walk's result");
    assert.equal(retryEvents.filter((event) => event === "reload").length, 1, "the reload was attempted once");
    assert.equal(walks, 1, "no second walk ran against a page whose reload failed");
    assert.equal(recorded[0]?.classification?.kind, "unknown", "the first walk's already-computed classification is what gets recorded");
  });

  test("skips the reload for a provider that navigates inside its own walk", async () => {

    // Its retry re-navigates for itself, so a reload here would be a second navigation buying nothing - and on a heavy SPA that is seconds of the startup window.
    walkResults = [ [], ONE_CHANNEL ];
    stubBrowser = { newPage: async (): Promise<Page> => makeRetryPage() } as unknown as Browser;

    const channels = await withProviderGuidePage(retryProvider({ handlesOwnNavigation: true }), { afterWalk }, deps);

    assert.equal(walks, 2, "the retry still ran");
    assert.ok(!retryEvents.includes("reload"), "the session drove no reload of its own");
    assert.deepEqual(channels, ONE_CHANNEL, "the retry produced the lineup");
  });

  test("runs no second walk when the caller aborted during the first", async () => {

    // A refresh request that cancelled this walk wants it gone, not retried. The behavior asserted is the walk count itself rather than a spy on the gate.
    const controller = new AbortController();

    walkResults = [ [], ONE_CHANNEL ];
    stubBrowser = { newPage: async (): Promise<Page> => makeRetryPage() } as unknown as Browser;

    const provider = {

      ...retryProvider(),
      discoverChannels: async (): Promise<DiscoveredChannel[]> => {

        walks++;
        controller.abort();

        return [];
      }
    } as unknown as ProviderModule;

    const channels = await withProviderGuidePage(provider, { afterWalk, signal: controller.signal }, deps);

    assert.equal(walks, 1, "the aborted session ran exactly one walk");
    assert.ok(!retryEvents.includes("reload"), "an aborted session never reloads");
    assert.deepEqual(channels, [], "the aborted walk's empty result is what comes back");
  });

  test("runs no second walk once a graceful shutdown has begun", async () => {

    // The retry opens new work against the browser, and the shutdown path closes it. Retrying here would drive the page after teardown started, which is the same
    // relaunch hazard the precache cycle's own per-service check exists to prevent.
    walkResults = [ [], ONE_CHANNEL ];
    stubBrowser = { newPage: async (): Promise<Page> => makeRetryPage() } as unknown as Browser;

    const shutdownDeps: PrecachingDeps = { ...deps, isGracefulShutdown: (): boolean => true };

    const channels = await withProviderGuidePage(retryProvider(), { afterWalk }, shutdownDeps);

    assert.equal(walks, 1, "no retry is attempted during teardown");
    assert.ok(!retryEvents.includes("reload"), "no reload is driven during teardown");
    assert.deepEqual(channels, [], "the first walk's result stands");
  });
});
