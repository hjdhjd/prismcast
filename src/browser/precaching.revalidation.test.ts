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
 *
 * It carries the clock as well. Every timer the scheduler arms - the settle delay before a cycle, the deferred re-attempt, and each walk's deadline - runs on
 * deps.clock, so a row drives the whole schedule by advancing one TestClock and reads what was armed off that clock's ledger. The health store's debounced flush
 * runs on a clock of its own, established in each describe, so it arms nothing on the platform and nothing on the ledger these rows read.
 *
 * The same port carries the browser-connected check, which the rows set to stand a browser up or take it away, and a browser acquisition the rows count, so a row
 * that saves the precache list proves that the walk it requests happens and that no walk ever acquires a browser the check found gone.
 */
import type { Browser, Page } from "puppeteer-core";
import type { Config, DiscoveredChannel, Nullable, ProviderModule } from "../types/index.ts";
import { DiscoveryWalkTimeoutError, applyPrecacheConfigChanges, precacheService, revalidateDomainAuth, startPrecaching, stopPrecaching,
  withProviderGuidePage } from "./precaching.ts";
import { LOG, extractDomain } from "../utils/index.ts";
import { TestClock, settle } from "homebridge-plugin-utils/testing";
import { afterEach, beforeEach, describe, test } from "node:test";
import { clearLoginState, setLoginDeps, startLoginMode } from "./login.ts";
import { getDomainAuthState, markDomainAuthRequired } from "../config/health.ts";
import { getEnabledServices, setEnabledServices } from "../config/services.ts";
import type { BlockedPageClassification } from "./blockedPage.ts";
import { CONFIG } from "../config/index.ts";
import type { Clock } from "homebridge-plugin-utils";
import type { ConfigChange } from "../config/reactivity.ts";
import type { PersistedLineupChannel } from "../config/providerLineups.ts";
import type { PrecachingDeps } from "./precaching.ts";
import type { StartOverlayHandlingOptions } from "./consent.ts";
import assert from "node:assert/strict";
import { setImmediate as immediate } from "node:timers/promises";
import { registerConfigChangeHandler } from "../config/reactivity.ts";
import { useHealthStoreOnClock } from "../config/health.helpers.ts";

// Mutable state the deps stubs read, so each test can shape the provider registry and browser behavior without re-registering stubs.
let mockGuideUrls: Record<string, string> = {};
let mockProviders: Record<string, ProviderModule> = {};
let windowSyncCalls = 0;
let stubBrowser: Browser;

// What the browser-connected check answers, and how many browser acquisitions the walks made. A row that takes the browser away sets the answer back when it ends,
// because every other row expects a browser.
let browserConnected = true;
let browserAcquisitions = 0;

// The overlay-handling options recorded by the injected startOverlayHandling stub (in call order), and an ordered log of the page operations the guarded session
// performs, so the withProviderGuidePage tests can assert the phase, the abort state, and the mute-before-navigation ordering without a live Chrome.
let overlayHandlingCalls: StartOverlayHandlingOptions[] = [];
let pageEvents: string[] = [];

// The options each newPage call received, in call order, so the guarded session's tests can assert how the guide page is created rather than only what is done to it.
let newPageOptions: unknown[] = [];

// The browser each discovery-page creation was handed, in call order. A row reads its length for how many walks opened a page at all, which is what the
// login-mode rows assert on: a deferred service creates none.
let discoveryPageCreations: Browser[] = [];

// The options each managed-page registration received, in call order, so the guarded session's rows can read how the discovery page is registered rather than
// only that it was.
let registrations: { inFlight?: boolean }[] = [];

// The lineup writes the discovery-outcome recorder issues, captured by the injected persistProviderLineup below so the port tests can assert what a completed walk
// hands the store without touching a real file.
const persistedLineups: { channels: PersistedLineupChannel[]; slug: string }[] = [];

/* The injected precaching dependencies: the browser accessors, the discovery-page creator, the page bookkeeping, the layout-surface declaration, the
 * window-visibility sync, the provider-registry lookups, and the discovery-phase overlay-poll launcher, substituted at precaching's PrecachingDeps boundary so
 * revalidation and discovery run against stubs with no real Chrome. Each field reads the mutable module state above at call time, so a test shapes the registry
 * and browser behavior by reassigning those lets. createDiscoveryPage records the browser it was handed and then delegates to that browser's own newPage, so
 * every per-row browser double keeps handing back the page double its row wrote, and what the creator itself puts in the creation options is asserted where the
 * creator lives, in index.test.ts. startOverlayHandling stands in for the real poll, recording each call's options (phase and abort signal) into
 * overlayHandlingCalls and logging its launch into pageEvents so the guide-page tests can assert the discovery phase and its abort timing; emulateLayoutSurface
 * logs itself into the same record and answers with a fixed surface, so the walk's declaration is observable in the page-operation order. getCurrentBrowser
 * counts each acquisition and isBrowserConnected answers what the row set, so a row reads whether a walk reached for a browser at all. Typed as the production
 * port so the doubles cannot drift. The health and login modules stay real.
 *
 * The set is built by a factory rather than written as a literal because every describe rebuilds it around a fresh clock in its beforeEach: the scheduler's
 * timers and each walk's deadline arm on deps.clock, so a row that drives a schedule needs its own clock and a ledger no earlier row has written to.
 */
function makeDeps(rowClock: Clock): PrecachingDeps {

  return {

    clock: rowClock,
    createDiscoveryPage: async (browser: Browser): Promise<Page> => {

      discoveryPageCreations.push(browser);

      return browser.newPage();
    },
    emulateLayoutSurface: async (): Promise<{ height: number; width: number }> => {

      pageEvents.push("layout");

      return { height: 1080, width: 1920 };
    },
    getCurrentBrowser: async (): Promise<Browser> => {

      browserAcquisitions++;

      return stubBrowser;
    },
    getPersistedLineup: (): null => null,
    getProviderBySlug: (slug: string): ProviderModule | undefined => mockProviders[slug],
    getProvidersForDomain: (domain: string): ProviderModule[] => Object.entries(mockGuideUrls)
      .filter(([ , guideUrl ]) => extractDomain(guideUrl) === domain).flatMap(([slug]) => mockProviders[slug] ?? []),
    isBrowserConnected: (): boolean => browserConnected,
    isGracefulShutdown: (): boolean => false,
    persistProviderLineup: async (slug: string, channels: PersistedLineupChannel[]): Promise<void> => {

      persistedLineups.push({ channels, slug });
    },
    registerManagedPage: (_page: Page, options?: { inFlight?: boolean }): void => {

      registrations.push(options ?? {});
    },
    startOverlayHandling: async (_page: Page, _profile: unknown, options: StartOverlayHandlingOptions): Promise<void> => {

      pageEvents.push("poll:" + options.phase);
      overlayHandlingCalls.push(options);
    },
    syncWindowVisibility: async (): Promise<void> => {

      windowSyncCalls++;
    },
    unregisterManagedPage: (): void => { /* Stub pages need no bookkeeping. */ }
  };
}

// The clock every timer the scheduler arms runs on, and the dependency set built around it. Each describe's beforeEach replaces both, so one row's ledger
// never colors the next.
let clock = new TestClock();
let deps: PrecachingDeps = makeDeps(clock);

// The teardown for the health store establishment each describe below holds, assigned by that describe's setup.
let disposeHealthStore: () => Promise<void>;

// Builds a stub Page satisfying the surface the guarded guide-page session touches. The evaluate stub answers false to every probe, and the embed-gate probe reads
// any answer other than null as a located gate, so an empty walk against this page classifies as a consent overlay and the session never retries it. The
// revalidation happy paths return non-empty discoveries and never reach classification. Every page operation pushes to pageEvents so the withProviderGuidePage
// tests can assert the order of the mute injection, the navigation, and the close.
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

// The scheduler's settle delay before a cycle and its deferred re-attempt delay, mirroring PRECACHE_DELAY and PRECACHE_RETRY_DELAY in precaching.ts. The module
// keeps these constants to itself, so the rows here name the same values and go red against a schedule that moves without them.
const PRECACHE_DELAY = 5000;
const PRECACHE_RETRY_DELAY = 300000;

/* The ceiling the session holds a single discovery walk to, mirroring DISCOVERY_WALK_TIMEOUT in precaching.ts. The module keeps that constant to itself, so the
 * rows here name the same value and go red against a budget that moves without them.
 */
const WALK_BUDGET = 60000;

/* The macrotask boundaries a row crosses before it reads settled state, so every continuation the module queued has run rather than a schedule still
 * unwinding. setImmediate is untouched by every timer stand-in this file installs, so each turn is a real macrotask boundary taken after the microtask
 * queue has drained.
 */
const SETTLE_TURNS = 10;

describe("revalidateDomainAuth", () => {

  let originalServices: string[];

  beforeEach(async () => {

    // A fresh clock per row, with the dependency set rebuilt around it, so a schedule this row arms is read on a ledger no earlier row wrote to.
    clock = new TestClock();
    deps = makeDeps(clock);
    originalServices = CONFIG.channels.precacheServices;
    CONFIG.channels.precacheServices = [];

    windowSyncCalls = 0;
    mockGuideUrls = { "stub-revalidate": "https://www.stub-revalidate.test/guide" };
    mockProviders = { "stub-revalidate": makeStubProvider(async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL) };
    stubBrowser = { newPage: async (): Promise<Page> => makeStubPage() } as unknown as Browser;

    clearLoginState();

    // The health store's flush debounce arms on the clock this establishment supplies, so the one global timer these rows would otherwise leave to the platform
    // is virtual and nothing writes to a real data directory after the test ends. The clock is separate from the scheduler's so this arm never colors that ledger.
    disposeHealthStore = await useHealthStoreOnClock(new TestClock());
  });

  afterEach(async () => {

    stopPrecaching();
    clearLoginState();
    CONFIG.channels.precacheServices = originalServices;
    await disposeHealthStore();
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
    setLoginDeps({

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
     * recordDiscoveryOutcome's non-empty arm marks the domain verified through markDomainAuth, the single mutation point. This is the needsLogin -> verified round
     * trip the login-end observer exists to produce.
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

  beforeEach(async () => {

    // A fresh clock per row, with the dependency set rebuilt around it, so a schedule this row arms is read on a ledger no earlier row wrote to.
    clock = new TestClock();
    deps = makeDeps(clock);
    windowSyncCalls = 0;
    stubBrowser = { newPage: async (): Promise<Page> => makeStubPage() } as unknown as Browser;

    clearLoginState();

    // The health store's flush debounce arms on the clock this establishment supplies, so the one global timer these rows would otherwise leave to the platform
    // is virtual and nothing writes to a real data directory after the test ends. The clock is separate from the scheduler's so this arm never colors that ledger.
    disposeHealthStore = await useHealthStoreOnClock(new TestClock());
  });

  afterEach(async () => {

    clearLoginState();
    await disposeHealthStore();
  });

  /* Both login states are exercised at this call site because the call is unconditional: precacheService decides nothing about the window, it asks the policy, and
   * the policy is what accounts for a login session. The login-active arm is the one that proves it - a login-mode guard here would suppress the call there and this
   * test would fail. What the window then ends up as is decideWindowVisibility's login arm, asserted in windowSync.test.ts, not here.
   */
  test("asks for a window sync even while login mode is active", async () => {

    setLoginDeps({

      getBrowserInstance: (): Nullable<Browser> => ({ connected: true, newPage: async (): Promise<Page> => makeLoginPageStub() } as unknown as Browser),
      syncWindowVisibility: async (): Promise<void> => { /* login.ts's own sync path is not under test. */ }
    });

    await startLoginMode("https://www.stub-revalidate.test/login");

    await precacheService(makeStubProvider(async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL), deps);

    assert.equal(windowSyncCalls, 1, "the discovery page cleanup syncs the window regardless of login mode");
  });

  test("asks for a window sync when login mode is inactive", async () => {

    // The complementary arm.
    await precacheService(makeStubProvider(async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL), deps);

    assert.equal(windowSyncCalls, 1, "the discovery page cleanup syncs the window");
  });
});

describe("startPrecaching - graceful-shutdown guard", () => {

  let originalServices: string[];

  beforeEach(async () => {

    // A fresh clock per row, with the dependency set rebuilt around it, so a schedule this row arms is read on a ledger no earlier row wrote to.
    clock = new TestClock();
    deps = makeDeps(clock);
    originalServices = CONFIG.channels.precacheServices;
    stubBrowser = { newPage: async (): Promise<Page> => makeStubPage() } as unknown as Browser;

    // Ensure no prior test left the single-flight guard set, so the positive-control schedule below is not swallowed by the already-in-progress branch.
    stopPrecaching();
    clearLoginState();

    // The health store's flush debounce arms on the clock this establishment supplies, so the one global timer these rows would otherwise leave to the platform
    // is virtual and nothing writes to a real data directory after the test ends. The clock is separate from the scheduler's so this arm never colors that ledger.
    disposeHealthStore = await useHealthStoreOnClock(new TestClock());
  });

  afterEach(async () => {

    stopPrecaching();
    clearLoginState();
    CONFIG.channels.precacheServices = originalServices;
    await disposeHealthStore();
  });

  test("schedules no timer during graceful shutdown even with configured precache services", () => {

    /* Traced path: the isGracefulShutdown() guard requestPrecache checks first, ahead of the pending-cycle merge. A browser launch can be reached during teardown;
     * without this guard the scheduled cycle would fire after the browser is closed and relaunch Chrome. The row's own clock is the instrument: a queued cycle
     * shows up on it directly, and the guard is proven the sole gate by scheduling normally the moment it is lifted.
     */
    const shutdownDeps: PrecachingDeps = { ...deps, isGracefulShutdown: (): boolean => true };

    CONFIG.channels.precacheServices = ["stub-revalidate"];

    startPrecaching(shutdownDeps);

    assert.equal(clock.pending, 0, "no precache cycle is scheduled while shutting down");

    // Lift only the shutdown guard: the identical call now schedules exactly one cycle, proving the guard was the sole gate keeping the timer off the clock.
    startPrecaching(deps);

    assert.equal(clock.pending, 1, "the same configuration schedules a cycle once shutdown clears");
    assert.deepEqual(clock.requested, [PRECACHE_DELAY], "and arms it at the scheduler's own settle delay");
  });
});

/* A service that finishes a cycle with nothing gets one more pass, minutes later, once whatever startup contention may have starved it has cleared. The rows here
 * drive the whole schedule on the virtual clock: the cycle fires at its own delay, the re-attempt at the longer one, and cancellation is asserted by advancing past
 * the delay and finding that the walk never happened - not by inspecting a handle.
 *
 * The counter every row reads is precacheService invocations per provider, taken from the cache clear each invocation performs. It counts attempts rather than
 * guide walks, which keeps the assertions about the schedule rather than about what the guarded session does with each walk. The rows nested last
 * drive the same schedule from a save's request beside a launch's, since a save's cycle and a pending re-attempt share it.
 */
describe("the deferred discovery re-attempt", () => {

  // How many times precacheService was invoked for each slug, how many discovery walks each provider actually ran, and the channels those walks return. Reset
  // per row.
  let attempts: Record<string, number> = {};
  let walks: Record<string, number> = {};
  let walkResults: Record<string, DiscoveredChannel[]> = {};

  // Which providers report a cached lineup at the moment they are asked, standing in for a lineup that arrived between the cycle and the re-attempt.
  let cachedSlugs = new Set<string>();

  let originalServices: string[];

  beforeEach(async () => {

    // A fresh clock per row, with the dependency set rebuilt around it, so a schedule this row arms is read on a ledger no earlier row wrote to.
    clock = new TestClock();
    deps = makeDeps(clock);
    originalServices = CONFIG.channels.precacheServices;
    attempts = {};
    cachedSlugs = new Set();
    discoveryPageCreations = [];
    registrations = [];
    walks = {};
    walkResults = {};
    stubBrowser = { newPage: async (): Promise<Page> => makeStubPage() } as unknown as Browser;

    stopPrecaching();
    clearLoginState();

    // The health store's flush debounce arms on the clock this establishment supplies, so the one global timer these rows would otherwise leave to the platform
    // is virtual and nothing writes to a real data directory after the test ends. The clock is separate from the scheduler's so this arm never colors that ledger.
    disposeHealthStore = await useHealthStoreOnClock(new TestClock());
  });

  afterEach(async () => {

    stopPrecaching();
    clearLoginState();
    CONFIG.channels.precacheServices = originalServices;
    await disposeHealthStore();
  });

  /**
   * Advances the row's clock by the given delay and drains, which fires whatever the scheduler had armed to come due there. The scheduler holds at most one
   * pending cycle and one pending re-attempt, each in its own slot, so a delay neither was armed for fires nothing - exactly what a row asserting a
   * cancellation is looking for.
   * @param delayMs - The delay to advance the clock by.
   */
  async function fire(delayMs: number): Promise<void> {

    clock.advance(delayMs);

    await settle(SETTLE_TURNS);
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

  test("re-attempts only the services the cycle left empty", async () => {

    /* The feature in one row: a boot where one provider's lazy content never appeared inside its walk. The service that came back with a lineup is not touched
     * again - re-walking it would cost a heavy SPA load for an answer already in hand - and the empty one gets exactly one more attempt.
     */
    mockProviders = { "deferred-empty": deferredProvider("deferred-empty"), "deferred-full": deferredProvider("deferred-full") };
    walkResults = { "deferred-full": ONE_CHANNEL };
    CONFIG.channels.precacheServices = [ "deferred-empty", "deferred-full" ];

    startPrecaching(deps);

    await fire(PRECACHE_DELAY);

    assert.deepEqual(attempts, { "deferred-empty": 1, "deferred-full": 1 }, "the cycle attempted both services once");
    assert.equal(clock.pending, 1, "the empty service earned a re-attempt, armed on the injected clock");
    assert.deepEqual(clock.requested, [ PRECACHE_DELAY, WALK_BUDGET, WALK_BUDGET, PRECACHE_RETRY_DELAY ],
      "the cycle's settle delay, one walk deadline per service, then the re-attempt's own longer delay");

    // The empty service's lineup shows up on the re-attempt, which is the outcome the delay is betting on.
    walkResults = { "deferred-empty": ONE_CHANNEL, "deferred-full": ONE_CHANNEL };

    await fire(PRECACHE_RETRY_DELAY);

    assert.deepEqual(attempts, { "deferred-empty": 2, "deferred-full": 1 }, "only the empty service was re-attempted");
  });

  test("skips a service whose lineup arrived in the interval", async () => {

    // Five minutes is long enough for a full cycle after a browser relaunch, or for a user to hit the discovery endpoint. Either fills the cache, and the pass has
    // nothing left to do for that service.
    mockProviders = { "deferred-empty": deferredProvider("deferred-empty") };
    CONFIG.channels.precacheServices = ["deferred-empty"];

    startPrecaching(deps);

    await fire(PRECACHE_DELAY);

    assert.deepEqual(attempts, { "deferred-empty": 1 }, "the cycle attempted the service once");

    cachedSlugs = new Set(["deferred-empty"]);

    await fire(PRECACHE_RETRY_DELAY);

    assert.deepEqual(attempts, { "deferred-empty": 1 }, "a service that already has a lineup is not walked again");
  });

  test("stopPrecaching cancels the pending pass, and the deferred walk never executes", async () => {

    // The shutdown guarantee, asserted by outcome: advance well past the delay and find that nothing ran. A cancellation that only dropped a reference would let
    // the timer fire into a closed browser and relaunch Chrome after teardown.
    mockProviders = { "deferred-empty": deferredProvider("deferred-empty") };
    CONFIG.channels.precacheServices = ["deferred-empty"];

    startPrecaching(deps);

    await fire(PRECACHE_DELAY);

    stopPrecaching();

    assert.equal(clock.pending, 0, "the stop drained the pending pass off the clock");

    await fire(PRECACHE_RETRY_DELAY);

    assert.deepEqual(attempts, { "deferred-empty": 1 }, "the cancelled pass never walked");
  });

  test("a fresh cycle supersedes the pending pass", async () => {

    // A launch requests a cycle over every listed service, the empty ones included. Letting the deferred pass survive alongside it would set that pass against
    // the cycle over the same guides, in contention for one browser.
    mockProviders = { "deferred-empty": deferredProvider("deferred-empty") };
    CONFIG.channels.precacheServices = ["deferred-empty"];

    startPrecaching(deps);

    await fire(PRECACHE_DELAY);

    assert.deepEqual(attempts, { "deferred-empty": 1 }, "the first cycle ran");

    walkResults = { "deferred-empty": ONE_CHANNEL };

    startPrecaching(deps);

    await fire(PRECACHE_DELAY);

    assert.deepEqual(attempts, { "deferred-empty": 2 }, "the fresh cycle ran its own attempt");

    await fire(PRECACHE_RETRY_DELAY);

    assert.deepEqual(attempts, { "deferred-empty": 2 }, "the superseded pass never fired afterwards");
  });

  test("runs nothing once a graceful shutdown has begun", async () => {

    // The per-service check inside the pass, and the one at its entry. Both exist because the pass opens discovery pages, and getCurrentBrowser relaunches the
    // Chrome that teardown just closed.
    let shuttingDown = false;

    const shutdownDeps: PrecachingDeps = { ...deps, isGracefulShutdown: (): boolean => shuttingDown };

    mockProviders = { "deferred-empty": deferredProvider("deferred-empty") };
    CONFIG.channels.precacheServices = ["deferred-empty"];

    startPrecaching(shutdownDeps);

    await fire(PRECACHE_DELAY);

    assert.deepEqual(attempts, { "deferred-empty": 1 }, "the cycle ran before shutdown began");

    shuttingDown = true;

    await fire(PRECACHE_RETRY_DELAY);

    assert.deepEqual(attempts, { "deferred-empty": 1 }, "the pass opened no discovery page during teardown");
  });

  test("a full-cycle request that arrives while the guard is held runs once the guard is released", async () => {

    /* The dropped-cycle hand-off. A launch requests a cycle over every service while a run still holds the guard, and that run is walking guides for a browser
     * whose caches the launch just cleared - so its result is worth nothing and the request must not be discarded. The assertion is the second cycle actually
     * running, which also proves the release ordering: the hand-off has to find a free guard, or it would record the very request being handed on and arm nothing.
     */
    const gate = Promise.withResolvers<DiscoveredChannel[]>();

    mockProviders = { "deferred-full": { ...deferredProvider("deferred-full"), discoverChannels: async (): Promise<DiscoveredChannel[]> => gate.promise } };

    CONFIG.channels.precacheServices = ["deferred-full"];

    startPrecaching(deps);

    await fire(PRECACHE_DELAY);

    assert.deepEqual(attempts, { "deferred-full": 1 }, "the first cycle is in flight");

    // The relaunch's request, arriving while the guard is held.
    startPrecaching(deps);

    // Release the gated walk so the first cycle finishes and its release honors the request.
    mockProviders = { "deferred-full": deferredProvider("deferred-full") };
    walkResults = { "deferred-full": ONE_CHANNEL };

    gate.resolve(ONE_CHANNEL);

    await settle(SETTLE_TURNS);
    await fire(PRECACHE_DELAY);

    assert.deepEqual(attempts, { "deferred-full": 2 }, "the deferred full cycle ran after the guard was released");
  });

  test("a walk stopped at its budget is queued for the re-attempt and counted on the completion line", async (t) => {

    /* A ceiling that ended a walk still in progress says nothing about whether the guide would have answered on a quieter system, so the service goes to the
     * same deferred pass an empty walk goes to. The row drives the lapse through the file's own timer capture: the deadline is scheduled at the budget like any
     * other timer, so firing that delay is what stops the walk.
     */
    const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });
    const warn = t.mock.method(LOG, "warn", () => { /* Captured via the mock. */ });
    const hang = Promise.withResolvers<DiscoveredChannel[]>();

    mockProviders = { "deferred-wedged": { ...deferredProvider("deferred-wedged"), discoverChannels: async (): Promise<DiscoveredChannel[]> => hang.promise } };

    CONFIG.channels.precacheServices = ["deferred-wedged"];

    startPrecaching(deps);

    await fire(PRECACHE_DELAY);

    assert.deepEqual(attempts, { "deferred-wedged": 1 }, "the cycle attempted the service and its walk is still running");

    await fire(WALK_BUDGET);

    const lapseCall = warn.mock.calls.find((call) => String(call.arguments[0]).includes("discovery walk exceeded"));

    assert.ok(lapseCall, "the lapse is reported as its own line rather than as a general precache failure");
    assert.deepEqual(lapseCall.arguments.slice(1), [ "deferred-wedged", WALK_BUDGET / 1000 ], "and names the service and the budget in seconds");

    const completionCall = info.mock.calls.find((call) => String(call.arguments[0]).includes("Channel lineup precaching complete"));

    assert.ok(completionCall, "the cycle still reported its completion");
    assert.ok(completionCall.arguments.map((argument) => String(argument)).join(" ").includes("1 returned no channels or timed out"),
      "the completion line counts the stopped walk among the services the cycle could not settle");

    // The service was queued, so the pass minutes later walks it again - which is the whole reason a lapse is not treated as a general failure.
    mockProviders = { "deferred-wedged": deferredProvider("deferred-wedged") };
    walkResults = { "deferred-wedged": ONE_CHANNEL };

    await fire(PRECACHE_RETRY_DELAY);

    assert.deepEqual(attempts, { "deferred-wedged": 2 }, "the stopped service was re-attempted");

    hang.resolve([]);
  });

  test("a walk stopped at its budget in the deferred pass is reported and not re-armed", async (t) => {

    /* This is the one pass a service gets. Re-arming on a lapse would put a wedged walk on an unbounded loop, waking the browser for it every few minutes for
     * the life of the process, so the pass reports it and stops there.
     */
    const warn = t.mock.method(LOG, "warn", () => { /* Captured via the mock. */ });
    const hang = Promise.withResolvers<DiscoveredChannel[]>();

    mockProviders = { "deferred-wedged": deferredProvider("deferred-wedged") };
    CONFIG.channels.precacheServices = ["deferred-wedged"];

    startPrecaching(deps);

    await fire(PRECACHE_DELAY);

    assert.deepEqual(attempts, { "deferred-wedged": 1 }, "the cycle walked the service and found nothing, so the pass is armed");

    // The pass meets a walk that never answers.
    mockProviders = { "deferred-wedged": { ...deferredProvider("deferred-wedged"), discoverChannels: async (): Promise<DiscoveredChannel[]> => hang.promise } };

    await fire(PRECACHE_RETRY_DELAY);
    await fire(WALK_BUDGET);

    const lapseCall = warn.mock.calls.find((call) => String(call.arguments[0]).includes("discovery walk exceeded"));

    assert.ok(lapseCall, "the pass reports the lapse");
    assert.ok(!String(lapseCall.arguments[0]).includes("re-attempted"), "and does not promise another attempt it will never make");

    // Nothing was re-armed, so advancing another full delay walks nothing at all.
    mockProviders = { "deferred-wedged": deferredProvider("deferred-wedged") };
    walkResults = { "deferred-wedged": ONE_CHANNEL };

    await fire(PRECACHE_RETRY_DELAY);

    assert.deepEqual(attempts, { "deferred-wedged": 2 }, "the lapse ended the schedule rather than restarting it");

    hang.resolve([]);
  });

  test("a walk that throws is reported once and not queued, and the cycle goes on to walk the next service", async (t) => {

    /* A walk that throws has a standing problem another walk will not solve, unlike one that came back empty or ran past its budget, so the cycle reports it
     * and moves on rather than queuing it for the re-attempt. The failing service is listed first, so the row also proves the containment: the service after it
     * is still walked.
     */
    const warn = t.mock.method(LOG, "warn", () => { /* Captured via the mock. */ });

    mockProviders = {

      "deferred-full": deferredProvider("deferred-full"),
      "deferred-throws": { ...deferredProvider("deferred-throws"), discoverChannels: async (): Promise<DiscoveredChannel[]> => {

        throw new Error("The guide failed to render.");
      } }
    };

    walkResults = { "deferred-full": ONE_CHANNEL };
    CONFIG.channels.precacheServices = [ "deferred-throws", "deferred-full" ];

    startPrecaching(deps);

    await fire(PRECACHE_DELAY);

    assert.deepEqual(attempts, { "deferred-full": 1, "deferred-throws": 1 }, "the cycle attempted each service once");
    assert.equal(walks["deferred-full"], 1, "the service after the failing one was walked");

    const naming = warn.mock.calls.filter((call) => call.arguments.map((argument) => String(argument)).includes("deferred-throws"));

    assert.equal(naming.length, 1, "one warning names the failing service");
    assert.ok(String(naming[0]?.arguments[0]).startsWith("Failed to precache"), "and it is the general failure line rather than the lapse line");
    assert.ok(!clock.requested.includes(PRECACHE_RETRY_DELAY), "no re-attempt was armed for the failing service");
    assert.equal(clock.pending, 0, "nothing stays pending once the cycle ends");
  });

  /* A walk opens a browser window at the shared window's placement, which during a login session is the window the user is signing in through - and a second
   * window over it would take their clicks. So the automatic walks stand aside while a session is on screen and come back for the services afterwards, on the
   * same deferred schedule the rows above drive. The user-initiated browse endpoint is deliberately not gated: the user asked for that window.
   *
   * These rows drive the real login module through startLoginMode and clearLoginState, because the guard production reads is that module's own flag. The stub
   * login dependencies supply no clock, so the session's fifteen-minute timeout arms on the system clock rather than on the row's TestClock, which the scheduler
   * and the walk deadlines run on. It never fires within a row: clearLoginState, called in each row and again in the afterEach, disposes it.
   */
  describe("standing aside for a login session", () => {

    let originalEnabled: string[];

    beforeEach(() => {

      // The rows here read the login guard, which sits behind the service filter, so the filter starts empty and only the row that means to exercise it sets one.
      // The cycle reads the running filter, so the rows drive it where production sets it.
      originalEnabled = getEnabledServices();
      setEnabledServices([]);
    });

    afterEach(() => {

      setEnabledServices(originalEnabled);
    });

    /**
     * Starts a real login session against a stub browser, so the cycle and the re-attempt read the flag exactly as production does.
     * @returns A promise that resolves once login mode is active.
     */
    async function startStubLogin(): Promise<void> {

      setLoginDeps({

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

      await startStubLogin();

      startPrecaching(deps);

      await fire(PRECACHE_DELAY);

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

      await fire(PRECACHE_RETRY_DELAY);

      assert.equal(discoveryPageCreations.length, 1, "the re-attempt opened the discovery window it deferred");
      assert.deepEqual(walks, { "deferred-login": 1 }, "and walked the service exactly once");
    });

    test("a re-attempt that meets a login session re-arms every service it still owes, not just the one it stopped on", async () => {

      /* The re-arm has to carry the whole remainder. A pass that armed only the slug it collided with would drop every service behind it in the queue, and
       * those services would never be walked at all - so the row queues two, collides on the first, and counts the walks after the session ends.
       */
      mockProviders = { "deferred-first": deferredProvider("deferred-first"), "deferred-second": deferredProvider("deferred-second") };
      CONFIG.channels.precacheServices = [ "deferred-first", "deferred-second" ];

      startPrecaching(deps);

      await fire(PRECACHE_DELAY);

      // Counted as precacheService invocations, exactly as the rows above count them, because the attempt is the unit the scheduler owes. The stub page classifies
      // an empty walk as a consent overlay, so no empty-walk retry runs here.
      assert.deepEqual(attempts, { "deferred-first": 1, "deferred-second": 1 }, "the cycle attempted both services, and both came back empty");

      // The session opens inside the re-attempt's delay, so the pass meets it on its first service.
      await startStubLogin();

      discoveryPageCreations = [];
      walks = {};
      walkResults = { "deferred-first": ONE_CHANNEL, "deferred-second": ONE_CHANNEL };

      await fire(PRECACHE_RETRY_DELAY);

      assert.deepEqual(discoveryPageCreations, [], "the pass opened no discovery window while the session was on screen");
      assert.deepEqual(walks, {}, "and walked nothing");

      clearLoginState();

      await fire(PRECACHE_RETRY_DELAY);

      assert.equal(discoveryPageCreations.length, 2, "the re-armed pass opened one window per service it still owed");
      assert.deepEqual(walks, { "deferred-first": 1, "deferred-second": 1 }, "both services were walked, not only the one the pass stopped on");
    });

    test("the cycle walks the service normally when no login session is on screen", async () => {

      // The guard's other side, so the row above cannot pass by the cycle being broken for every service rather than deferring for this one.
      mockProviders = { "deferred-login": deferredProvider("deferred-login") };
      walkResults = { "deferred-login": ONE_CHANNEL };
      CONFIG.channels.precacheServices = ["deferred-login"];

      startPrecaching(deps);

      await fire(PRECACHE_DELAY);

      assert.equal(discoveryPageCreations.length, 1, "the cycle opened the discovery window");
      assert.deepEqual(walks, { "deferred-login": 1 }, "and walked the service");
    });

    test("a filtered-out service is counted as filtered rather than deferred, even with a login session on screen", async (t) => {

      /* The filter skip and the login deferral are ordered, and the order is what this row reads. A service outside the active filter is not one the cycle owes
       * a walk to at all, so it must be counted as filtered and left there - deferring it would put a service the user switched off onto a schedule that walks
       * it minutes later. The row runs both branches in one cycle: the filtered service and, behind it, one the session really does defer.
       */
      const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });

      mockProviders = { "login-filtered": deferredProvider("login-filtered"), "login-kept": deferredProvider("login-kept") };
      CONFIG.channels.precacheServices = [ "login-filtered", "login-kept" ];
      setEnabledServices(["login-kept"]);

      await startStubLogin();

      startPrecaching(deps);

      await fire(PRECACHE_DELAY);

      assert.deepEqual(discoveryPageCreations, [], "neither service opened a discovery window");

      const completionCall = info.mock.calls.find((call) => String(call.arguments[0]).includes("Channel lineup precaching complete"));

      assert.ok(completionCall, "the cycle reported its completion");

      const completion = completionCall.arguments.map((argument) => String(argument)).join(" ");

      assert.ok(completion.includes("1 skipped (filtered)"), "the filtered service is counted as filtered");
      assert.ok(completion.includes("1 deferred for a login session"), "and only the service inside the filter is counted as deferred");

      // The re-attempt is the proof the filtered service was never queued: once the session ends, the pass walks the kept service and nothing else.
      walkResults = { "login-filtered": ONE_CHANNEL, "login-kept": ONE_CHANNEL };

      clearLoginState();

      await fire(PRECACHE_RETRY_DELAY);

      assert.deepEqual(walks, { "login-kept": 1 }, "the re-attempt walked only the service the filter allows");
    });

    test("a re-attempt skips a service whose lineup arrived mid-pass and re-arms only what the session interrupted", async (t) => {

      /* A lineup that arrived in the interval and a session that opens partway through both leave a service unwalked, and they are read in that order. A lineup
       * that arrived - from a later cycle, or from a user hitting the discovery endpoint - makes the walk pointless, so that service is skipped with its debug
       * line and is never re-armed; a session stops the pass where it stands and re-arms the remainder. Reading the lineup first only shows when the two meet:
       * the queue here holds a cached service before the session opens and another after it, and the second is the one that would be re-armed by a pass that
       * asked about the session first.
       */
      const debug = t.mock.method(LOG, "debug", () => { /* Captured via the mock. */ });

      mockProviders = { "pass-cached": deferredProvider("pass-cached"), "pass-collides": deferredProvider("pass-collides"),
        "pass-late-cached": deferredProvider("pass-late-cached"), "pass-walks": deferredProvider("pass-walks") };

      CONFIG.channels.precacheServices = [ "pass-cached", "pass-walks", "pass-late-cached", "pass-collides" ];

      startPrecaching(deps);

      await fire(PRECACHE_DELAY);

      assert.deepEqual(attempts, { "pass-cached": 1, "pass-collides": 1, "pass-late-cached": 1, "pass-walks": 1 },
        "the cycle attempted all four and every one came back empty, so all four are owed a second pass");

      /* The interval does its work: two lineups land, and the service between them opens a login session from inside its own walk, so the pass meets the session
       * with one cached service already behind it and one still ahead.
       */
      cachedSlugs = new Set([ "pass-cached", "pass-late-cached" ]);
      walks = {};

      mockProviders = { ...mockProviders, "pass-walks": { ...deferredProvider("pass-walks"),

        discoverChannels: async (): Promise<DiscoveredChannel[]> => {

          walks["pass-walks"] = (walks["pass-walks"] ?? 0) + 1;

          await startStubLogin();

          return ONE_CHANNEL;
        } } };

      await fire(PRECACHE_RETRY_DELAY);

      const skipped = debug.mock.calls.filter((call) => String(call.arguments[1]).includes("its lineup was discovered in the meantime"))
        .map((call) => String(call.arguments[2]));

      assert.deepEqual(skipped, [ "pass-cached", "pass-late-cached" ],
        "both services whose lineups arrived are skipped with their own line, including the one the session was already up for");

      assert.deepEqual(walks, { "pass-walks": 1 }, "only the service that was neither cached nor behind the session was walked");

      // What the session interrupted is re-armed and nothing else is, so clearing the lineups proves the skipped services were never queued: a pass that had
      // asked about the session first would have carried the second cached service along and would walk it here.
      cachedSlugs = new Set();
      walkResults = { "pass-cached": ONE_CHANNEL, "pass-collides": ONE_CHANNEL, "pass-late-cached": ONE_CHANNEL, "pass-walks": ONE_CHANNEL };
      walks = {};

      clearLoginState();

      await fire(PRECACHE_RETRY_DELAY);

      assert.deepEqual(walks, { "pass-collides": 1 }, "the re-armed pass walked exactly the remainder the session stopped it on");
    });

    test("a completion line carrying both an unsettled walk and a login deferral names them in that order", async (t) => {

      /* The line composes its clauses in a fixed order - what the cycle could not settle, then what it stood aside from, then what the filter removed - so a row
       * that produces two of them at once is what holds that order in place. A line that reordered them would still be true and would read as a different
       * sentence every time the mix changed.
       */
      const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });

      let sessionStarted = false;

      mockProviders = { "cycle-deferred": deferredProvider("cycle-deferred"), "cycle-unsettled": { ...deferredProvider("cycle-unsettled"),

        discoverChannels: async (): Promise<DiscoveredChannel[]> => {

          walks["cycle-unsettled"] = (walks["cycle-unsettled"] ?? 0) + 1;

          // The session opens from inside the first walk, so the service behind this one meets it on the next turn of the cycle's loop.
          if(!sessionStarted) {

            sessionStarted = true;

            await startStubLogin();
          }

          return [];
        } } };

      CONFIG.channels.precacheServices = [ "cycle-unsettled", "cycle-deferred" ];

      startPrecaching(deps);

      await fire(PRECACHE_DELAY);

      const completionCall = info.mock.calls.find((call) => String(call.arguments[0]).includes("Channel lineup precaching complete"));

      assert.ok(completionCall, "the cycle reported its completion");

      const completion = completionCall.arguments.map((argument) => String(argument)).join(" ");
      const unsettledAt = completion.indexOf("returned no channels or timed out");
      const deferredAt = completion.indexOf("deferred for a login session");

      assert.notEqual(unsettledAt, -1, "the line names the walk that settled nothing");
      assert.notEqual(deferredAt, -1, "and the service the session deferred");
      assert.ok(unsettledAt < deferredAt, "the unsettled clause comes before the login-deferral clause");
    });
  });

  /* A save that adds services to the precache list requests a cycle scoped to them, and only while a browser is connected, so it warms what it selected without
   * clearing any other provider's cache and never launches a browser. The rows drive the handler the way the reconcile does - CONFIG holds the running list and
   * the candidate the saved one while the handler runs, and the commit then assigns the saved list - and read the schedule off the row's clock: what was armed,
   * what each fire walked, and whether a walk reached for a browser at all.
   */
  describe("a save's request, scoped to the services it adds", () => {

    let originalEnabled: string[];

    beforeEach(() => {

      browserAcquisitions = 0;

      // The deferred pass reads the running filter, so the filter starts empty and only the row that means to exercise it sets one.
      originalEnabled = getEnabledServices();
      setEnabledServices([]);
    });

    afterEach(() => {

      browserConnected = true;
      setEnabledServices(originalEnabled);
    });

    /**
     * Saves the precache list the way the reconcile does: the handler runs while CONFIG still holds the running list and the candidate carries the saved one, and
     * the commit then assigns the saved list as a new array.
     * @param slugs - The saved precache list.
     * @returns A promise that resolves once the handler has run and the list is committed.
     */
    async function saveList(slugs: string[]): Promise<void> {

      const change: ConfigChange = { current: slugs, path: "channels.precacheServices", previous: CONFIG.channels.precacheServices };
      const next: Config = structuredClone(CONFIG);

      next.channels.precacheServices = slugs;

      assert.deepEqual(await applyPrecacheConfigChanges([change], next, deps), [], "the handler refuses nothing");

      CONFIG.channels.precacheServices = [...slugs];
    }

    // How many times the scheduler asked the row's clock for a delay, so a row counts the cycles and re-attempts it armed apart from the walk deadlines.
    const armed = (delayMs: number): number => clock.requested.filter((requested) => requested === delayMs).length;

    /**
     * Counts the cycles a row's LOG.info mock saw start, read off the start line every cycle that walks anything logs.
     * @param calls - The calls the row's LOG.info mock recorded.
     * @returns How many of them were a cycle's start line.
     */
    function startLines(calls: readonly { arguments: readonly unknown[] }[]): number {

      return calls.filter((call) => String(call.arguments[0]).startsWith("Starting channel lineup precaching")).length;
    }

    test("a save that adds a service beside a listed one arms one cycle that clears and walks only the added service", async () => {

      mockProviders = { "save-added": deferredProvider("save-added"), "save-listed": deferredProvider("save-listed") };
      walkResults = { "save-added": ONE_CHANNEL, "save-listed": ONE_CHANNEL };
      CONFIG.channels.precacheServices = ["save-listed"];

      await saveList([ "save-listed", "save-added" ]);

      assert.deepEqual(clock.requested, [PRECACHE_DELAY], "the save armed one cycle at the settle delay");

      await fire(PRECACHE_DELAY);

      assert.deepEqual(attempts, { "save-added": 1 }, "only the added service's cache was cleared");
      assert.deepEqual(walks, { "save-added": 1 }, "and only the added service was walked");
    });

    test("a save that adds a service with no browser running arms nothing and never reaches for a browser", async () => {

      mockProviders = { "save-added": deferredProvider("save-added") };
      walkResults = { "save-added": ONE_CHANNEL };
      CONFIG.channels.precacheServices = [];
      browserConnected = false;

      await saveList(["save-added"]);

      assert.deepEqual(clock.requested, [], "the save asked the clock for nothing");

      await fire(PRECACHE_DELAY);

      assert.equal(browserAcquisitions, 0, "no walk reached for a browser");
      assert.deepEqual(attempts, {}, "and nothing was walked");
    });

    test("with a browser connected, a save that only removes a service arms nothing, and one that replaces a service walks the replacement", async () => {

      mockProviders = { "save-first": deferredProvider("save-first"), "save-replacement": deferredProvider("save-replacement"),
        "save-second": deferredProvider("save-second") };
      walkResults = { "save-first": ONE_CHANNEL, "save-replacement": ONE_CHANNEL, "save-second": ONE_CHANNEL };
      CONFIG.channels.precacheServices = [ "save-first", "save-second" ];

      await saveList(["save-first"]);

      assert.deepEqual(clock.requested, [], "the removal armed nothing");

      await saveList(["save-replacement"]);

      assert.deepEqual(clock.requested, [PRECACHE_DELAY], "the replacement armed one cycle");

      await fire(PRECACHE_DELAY);

      assert.deepEqual(walks, { "save-replacement": 1 }, "the cycle walked the replacement alone");
    });

    test("a save inside a launch cycle's settle delay joins that cycle, so one cycle runs and no second settle delay is armed", async (t) => {

      const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });

      mockProviders = { "save-added": deferredProvider("save-added"), "save-listed": deferredProvider("save-listed") };
      walkResults = { "save-added": ONE_CHANNEL, "save-listed": ONE_CHANNEL };
      CONFIG.channels.precacheServices = ["save-listed"];

      startPrecaching(deps);

      await saveList([ "save-listed", "save-added" ]);
      await fire(PRECACHE_DELAY);

      // The launch's cycle reads the list when it fires, so it walks the service the save added beside the one it was armed for.
      assert.deepEqual(attempts, { "save-added": 1, "save-listed": 1 }, "the launch's cycle walked each listed service once");

      // Past the cycle's release, nothing further is armed or walked.
      await fire(PRECACHE_DELAY);

      assert.equal(armed(PRECACHE_DELAY), 1, "one settle delay was armed in all");
      assert.equal(startLines(info.mock.calls), 1, "one cycle ran");
      assert.deepEqual(attempts, { "save-added": 1, "save-listed": 1 }, "and nothing walked again");
    });

    test("a second launch inside the settle delay re-arms the pending cycle, so one cycle runs a full delay after the second launch", async (t) => {

      const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });

      mockProviders = { "launch-listed": deferredProvider("launch-listed") };
      walkResults = { "launch-listed": ONE_CHANNEL };
      CONFIG.channels.precacheServices = ["launch-listed"];

      startPrecaching(deps);

      await fire(PRECACHE_DELAY / 2);

      startPrecaching(deps);

      assert.deepEqual(clock.requested, [ PRECACHE_DELAY, PRECACHE_DELAY ], "the second launch re-armed the cycle for a full settle delay");

      await fire(PRECACHE_DELAY / 2);

      assert.deepEqual(attempts, {}, "nothing walked at the first launch's deadline");

      await fire(PRECACHE_DELAY / 2);

      assert.deepEqual(attempts, { "launch-listed": 1 }, "the cycle walked a full settle delay after the second launch");

      await fire(PRECACHE_DELAY);

      assert.equal(startLines(info.mock.calls), 1, "one cycle ran");
      assert.deepEqual(attempts, { "launch-listed": 1 }, "and nothing walked again");
    });

    test("a save while a cycle runs yields exactly one further cycle after the release, walking only the added service", async (t) => {

      const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });
      const gate = Promise.withResolvers<DiscoveredChannel[]>();

      mockProviders = { "save-added": deferredProvider("save-added"),
        "save-listed": { ...deferredProvider("save-listed"), discoverChannels: async (): Promise<DiscoveredChannel[]> => gate.promise } };
      walkResults = { "save-added": ONE_CHANNEL };
      CONFIG.channels.precacheServices = ["save-listed"];

      startPrecaching(deps);

      await fire(PRECACHE_DELAY);

      assert.deepEqual(attempts, { "save-listed": 1 }, "the launch's cycle is walking the listed service");

      await saveList([ "save-listed", "save-added" ]);

      gate.resolve(ONE_CHANNEL);

      await settle(SETTLE_TURNS);
      await fire(PRECACHE_DELAY);

      assert.deepEqual(attempts, { "save-added": 1, "save-listed": 1 }, "the further cycle walked only the added service");

      await fire(PRECACHE_DELAY);

      assert.equal(armed(PRECACHE_DELAY), 2, "exactly one further cycle was armed");
      assert.equal(startLines(info.mock.calls), 2, "and it ran once");
    });

    test("a save while a re-attempt is pending walks only the added service and leaves the re-attempt on its own delay", async () => {

      mockProviders = { "retry-arrived": deferredProvider("retry-arrived"), "retry-owed": deferredProvider("retry-owed"),
        "save-added": deferredProvider("save-added") };
      walkResults = { "save-added": ONE_CHANNEL };
      CONFIG.channels.precacheServices = [ "retry-arrived", "retry-owed" ];

      startPrecaching(deps);

      await fire(PRECACHE_DELAY);

      assert.deepEqual(attempts, { "retry-arrived": 1, "retry-owed": 1 }, "the launch's cycle left each service unsettled, so a re-attempt is pending");

      await saveList([ "retry-arrived", "retry-owed", "save-added" ]);
      await fire(PRECACHE_DELAY);

      assert.deepEqual(attempts, { "retry-arrived": 1, "retry-owed": 1, "save-added": 1 }, "the save's cycle walked only the added service");
      assert.equal(armed(PRECACHE_RETRY_DELAY), 1, "and armed no second re-attempt delay");

      // One lineup arrives before the re-attempt's own deadline, a full re-attempt delay after the launch's cycle ended.
      cachedSlugs = new Set(["retry-arrived"]);
      walkResults = { "retry-arrived": ONE_CHANNEL, "retry-owed": ONE_CHANNEL, "save-added": ONE_CHANNEL };

      await fire(PRECACHE_RETRY_DELAY - PRECACHE_DELAY);

      assert.deepEqual(attempts, { "retry-arrived": 1, "retry-owed": 2, "save-added": 1 },
        "the re-attempt skipped the service whose lineup arrived and walked the other once");
    });

    test("a save's cycle that leaves its service unsettled merges it into the pending re-attempt, which waits a full delay from that cycle", async () => {

      mockProviders = { "retry-owed": deferredProvider("retry-owed"), "save-added": deferredProvider("save-added") };
      CONFIG.channels.precacheServices = ["retry-owed"];

      startPrecaching(deps);

      await fire(PRECACHE_DELAY);

      // The launch's cycle left its service unsettled, so its re-attempt is due a full re-attempt delay after it; the save's cycle ends one settle delay later.
      await saveList([ "retry-owed", "save-added" ]);
      await fire(PRECACHE_DELAY);

      assert.deepEqual(attempts, { "retry-owed": 1, "save-added": 1 }, "the save's cycle walked the added service, which came back empty");

      walkResults = { "retry-owed": ONE_CHANNEL, "save-added": ONE_CHANNEL };

      await fire(PRECACHE_RETRY_DELAY - PRECACHE_DELAY);

      assert.deepEqual(attempts, { "retry-owed": 1, "save-added": 1 }, "nothing walked at the first re-attempt's deadline");
      assert.equal(armed(PRECACHE_RETRY_DELAY), 2, "the merge re-armed the re-attempt for a full delay");

      await fire(PRECACHE_DELAY);

      assert.deepEqual(attempts, { "retry-owed": 2, "save-added": 2 }, "one pass a full delay after the save's cycle walked each owed service once");

      await fire(PRECACHE_RETRY_DELAY);

      assert.deepEqual(attempts, { "retry-owed": 2, "save-added": 2 }, "and no further pass followed");
    });

    test("a re-attempt that fires while a save's cycle holds the guard re-arms on its own delay, and its later fire walks its service once", async () => {

      mockProviders = { "retry-owed": deferredProvider("retry-owed"), "save-added": deferredProvider("save-added") };
      walkResults = { "save-added": ONE_CHANNEL };
      CONFIG.channels.precacheServices = ["retry-owed"];

      startPrecaching(deps);

      await fire(PRECACHE_DELAY);

      // The save lands half a settle delay before the re-attempt is due, so its cycle holds the guard when the re-attempt fires.
      await fire(PRECACHE_RETRY_DELAY - (PRECACHE_DELAY / 2));
      await saveList([ "retry-owed", "save-added" ]);
      await fire(PRECACHE_DELAY / 2);

      assert.deepEqual(attempts, { "retry-owed": 1 }, "the re-attempt walked nothing while the save's cycle held the guard");
      assert.equal(armed(PRECACHE_RETRY_DELAY), 2, "and re-armed itself on its own delay");

      await fire(PRECACHE_DELAY / 2);

      assert.deepEqual(attempts, { "retry-owed": 1, "save-added": 1 }, "the save's cycle then walked the added service");

      walkResults = { "retry-owed": ONE_CHANNEL, "save-added": ONE_CHANNEL };

      await fire(PRECACHE_RETRY_DELAY - (PRECACHE_DELAY / 2));

      assert.deepEqual(attempts, { "retry-owed": 2, "save-added": 1 }, "the re-armed re-attempt walked its service once");

      await fire(PRECACHE_RETRY_DELAY);

      assert.deepEqual(attempts, { "retry-owed": 2, "save-added": 1 }, "and no further pass followed");
    });

    test("two saves inside one settle delay, each adding a service, fire one cycle that walks each added service", async () => {

      mockProviders = { "save-first": deferredProvider("save-first"), "save-listed": deferredProvider("save-listed"),
        "save-second": deferredProvider("save-second") };
      walkResults = { "save-first": ONE_CHANNEL, "save-listed": ONE_CHANNEL, "save-second": ONE_CHANNEL };
      CONFIG.channels.precacheServices = ["save-listed"];

      await saveList([ "save-listed", "save-first" ]);
      await fire(PRECACHE_DELAY / 2);
      await saveList([ "save-listed", "save-first", "save-second" ]);
      await fire(PRECACHE_DELAY / 2);

      assert.deepEqual(attempts, { "save-first": 1, "save-second": 1 }, "the one cycle walked each added service and left the listed one alone");
      assert.equal(armed(PRECACHE_DELAY), 1, "the second save joined the first save's cycle on its timer");
    });

    test("a launch inside a save's settle delay re-arms the cycle for a full delay, then walks every listed service and cancels the re-attempt", async () => {

      mockProviders = { "retry-owed": deferredProvider("retry-owed"), "save-added": deferredProvider("save-added") };
      CONFIG.channels.precacheServices = ["retry-owed"];

      startPrecaching(deps);

      await fire(PRECACHE_DELAY);

      // The launch's cycle left its service unsettled, so a re-attempt is pending; the row then counts afresh from the save.
      attempts = {};
      browserAcquisitions = 0;
      walks = {};
      walkResults = { "retry-owed": ONE_CHANNEL, "save-added": ONE_CHANNEL };

      const mark = clock.requested.length;

      await saveList([ "retry-owed", "save-added" ]);
      await fire(PRECACHE_DELAY / 2);

      startPrecaching(deps);

      await fire(PRECACHE_DELAY / 2);

      assert.equal(browserAcquisitions, 0, "nothing reached for a browser at the save's deadline");
      assert.deepEqual(walks, {}, "and nothing was walked");
      assert.deepEqual(clock.requested.slice(mark), [ PRECACHE_DELAY, PRECACHE_DELAY ], "the launch re-armed the cycle for a second settle delay");

      await fire(PRECACHE_DELAY / 2);

      assert.deepEqual(walks, { "retry-owed": 1, "save-added": 1 }, "the cycle walked every listed service once, a full delay after the launch");

      // The launch cancelled the pending re-attempt, so its deadline passes without a walk.
      await fire(PRECACHE_RETRY_DELAY);

      assert.deepEqual(walks, { "retry-owed": 1, "save-added": 1 }, "the cancelled re-attempt never walked");
    });

    test("a service removed while the cycle walks an earlier one is skipped at its turn, and the service after it is walked", async (t) => {

      const debug = t.mock.method(LOG, "debug", () => { /* Captured via the mock. */ });

      mockProviders = {

        "turn-first": { ...deferredProvider("turn-first"), discoverChannels: async (): Promise<DiscoveredChannel[]> => {

          // The save commits while this walk runs, assigning a new array as the reconcile's commit does.
          CONFIG.channels.precacheServices = [ "turn-first", "turn-third" ];

          return ONE_CHANNEL;
        } },
        "turn-second": deferredProvider("turn-second"),
        "turn-third": deferredProvider("turn-third")
      };
      walkResults = { "turn-second": ONE_CHANNEL, "turn-third": ONE_CHANNEL };
      CONFIG.channels.precacheServices = [ "turn-first", "turn-second", "turn-third" ];

      startPrecaching(deps);

      await fire(PRECACHE_DELAY);

      assert.deepEqual(attempts, { "turn-first": 1, "turn-third": 1 }, "the removed service was skipped at its turn and the one after it was walked");

      const skipLine = debug.mock.calls.find((call) => (call.arguments[0] === "precache") &&
        (call.arguments[1] === "Skipping precache for %s: it left the precache list during the cycle."));

      assert.ok(skipLine, "the skip is logged in the precache category");
      assert.equal(skipLine.arguments[2], "turn-second", "and names the removed service");
    });

    test("a browser that goes while the cycle walks stops it at the next service, handing what it left unsettled to the re-attempt", async () => {

      mockProviders = {

        "gone-first": { ...deferredProvider("gone-first"), discoverChannels: async (): Promise<DiscoveredChannel[]> => {

          browserConnected = false;

          return [];
        } },
        "gone-second": deferredProvider("gone-second")
      };
      walkResults = { "gone-second": ONE_CHANNEL };
      CONFIG.channels.precacheServices = [ "gone-first", "gone-second" ];

      startPrecaching(deps);

      await fire(PRECACHE_DELAY);

      assert.deepEqual(attempts, { "gone-first": 1 }, "the cycle stopped before the service after the browser went");
      assert.equal(browserAcquisitions, 1, "only the walk that began with a browser acquired one");
      assert.equal(armed(PRECACHE_RETRY_DELAY), 1, "the unsettled service was handed to the re-attempt");

      await fire(PRECACHE_RETRY_DELAY);

      assert.deepEqual(attempts, { "gone-first": 1 }, "the re-attempt, finding no browser, walked nothing");
      assert.equal(browserAcquisitions, 1, "and reached for no browser");
      assert.equal(clock.pending, 0, "and re-armed nothing");
    });

    test("a deferred pass skips a service removed from the list and one the filter excludes, and stops when no browser is connected", async (t) => {

      const debug = t.mock.method(LOG, "debug", () => { /* Captured via the mock. */ });

      mockProviders = { "pass-filtered": deferredProvider("pass-filtered"), "pass-kept": deferredProvider("pass-kept"),
        "pass-removed": deferredProvider("pass-removed") };
      CONFIG.channels.precacheServices = [ "pass-removed", "pass-filtered", "pass-kept" ];

      startPrecaching(deps);

      await fire(PRECACHE_DELAY);

      assert.deepEqual(attempts, { "pass-filtered": 1, "pass-kept": 1, "pass-removed": 1 }, "the cycle left each service unsettled, so the re-attempt owes each one");

      // In the interval a save removes one service and the filter comes to exclude another. The filter still enables the removed service, so only the list
      // check can skip it.
      CONFIG.channels.precacheServices = [ "pass-filtered", "pass-kept" ];
      setEnabledServices([ "pass-kept", "pass-removed" ]);

      await fire(PRECACHE_RETRY_DELAY);

      assert.deepEqual(attempts, { "pass-filtered": 1, "pass-kept": 2, "pass-removed": 1 }, "the pass walked only the service still listed and enabled");

      const skipLines = debug.mock.calls.filter((call) => (call.arguments[0] === "precache") &&
        String(call.arguments[1]).startsWith("Skipping the deferred re-attempt for %s")).map((call) => [ call.arguments[1], call.arguments[2] ]);

      assert.deepEqual(skipLines, [
        [ "Skipping the deferred re-attempt for %s: it is no longer on the precache list.", "pass-removed" ],
        [ "Skipping the deferred re-attempt for %s: not in active service filter.", "pass-filtered" ]
      ], "each skip is logged in the precache category and names its service");

      // A launch's cycle leaves the kept service unsettled again, and the browser is gone by the time its re-attempt fires.
      startPrecaching(deps);

      await fire(PRECACHE_DELAY);

      assert.deepEqual(attempts, { "pass-filtered": 1, "pass-kept": 3, "pass-removed": 1 }, "the launch's cycle walked the kept service, which came back empty");

      browserConnected = false;
      browserAcquisitions = 0;

      await fire(PRECACHE_RETRY_DELAY);

      assert.equal(browserAcquisitions, 0, "the pass reached for no browser");
      assert.deepEqual(attempts, { "pass-filtered": 1, "pass-kept": 3, "pass-removed": 1 }, "and walked nothing");
      assert.equal(clock.pending, 0, "and re-armed nothing");
    });

    test("a browser that goes while the deferred pass walks stops it at the next service, with no further browser acquired", async () => {

      mockProviders = {

        "pass-gone-first": { ...deferredProvider("pass-gone-first"), discoverChannels: async (): Promise<DiscoveredChannel[]> => {

          walks["pass-gone-first"] = (walks["pass-gone-first"] ?? 0) + 1;

          // The cycle's walk keeps the browser, so the re-attempt owes every service the cycle walked, and the re-attempt's walk is the one that takes the browser away.
          if(walks["pass-gone-first"] === 2) {

            browserConnected = false;
          }

          return [];
        } },
        "pass-gone-second": deferredProvider("pass-gone-second")
      };
      CONFIG.channels.precacheServices = [ "pass-gone-first", "pass-gone-second" ];

      startPrecaching(deps);

      await fire(PRECACHE_DELAY);

      assert.deepEqual(attempts, { "pass-gone-first": 1, "pass-gone-second": 1 }, "the cycle left each service unsettled, so the re-attempt owes each one");

      browserAcquisitions = 0;

      await fire(PRECACHE_RETRY_DELAY);

      assert.deepEqual(attempts, { "pass-gone-first": 2, "pass-gone-second": 1 }, "the pass stopped at the service after the walk that took the browser away");
      assert.equal(browserAcquisitions, 1, "only the walk that began with a browser acquired one");
      assert.equal(clock.pending, 0, "and the pass re-armed nothing");
    });

    test("a list emptied in the settle delay fires without a start line, and a later request arms a cycle", async (t) => {

      const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });

      mockProviders = { "empty-listed": deferredProvider("empty-listed") };
      walkResults = { "empty-listed": ONE_CHANNEL };
      CONFIG.channels.precacheServices = ["empty-listed"];

      startPrecaching(deps);

      CONFIG.channels.precacheServices = [];

      await fire(PRECACHE_DELAY);

      assert.equal(startLines(info.mock.calls), 0, "the cycle that admitted no listed service logged no start line");
      assert.deepEqual(attempts, {}, "and walked nothing");

      // A later request finds the guard free and arms. Its cycle is the positive control: the harness captures the start line a walking cycle logs.
      CONFIG.channels.precacheServices = ["empty-listed"];

      startPrecaching(deps);

      assert.equal(clock.pending, 1, "the later request armed a cycle");

      await fire(PRECACHE_DELAY);

      assert.equal(startLines(info.mock.calls), 1, "the later cycle logged its start line");
      assert.deepEqual(attempts, { "empty-listed": 1 }, "and walked the listed service");
    });

    test("a browser that goes between the request and the fire leaves the cycle unstarted and no browser acquired", async (t) => {

      const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });

      mockProviders = { "gone-listed": deferredProvider("gone-listed") };
      walkResults = { "gone-listed": ONE_CHANNEL };
      CONFIG.channels.precacheServices = ["gone-listed"];

      startPrecaching(deps);

      browserConnected = false;

      await fire(PRECACHE_DELAY);

      assert.equal(browserAcquisitions, 0, "the cycle reached for no browser");
      assert.equal(startLines(info.mock.calls), 0, "and stopped before its start line");
      assert.deepEqual(attempts, {}, "and walked nothing");
      assert.equal(clock.pending, 0, "and armed no re-attempt");
    });

    test("registering a second handler for the precache list throws, because the module registered its own at load", () => {

      assert.throws(() => { registerConfigChangeHandler("channels.precacheServices", applyPrecacheConfigChanges); },
        { message: "A config change handler is already registered for prefix \"channels.precacheServices\"." });
    });

    test("stopPrecaching disposes a save's pending cycle and a pending re-attempt alike, leaving no timer on the clock", async () => {

      mockProviders = { "retry-owed": deferredProvider("retry-owed"), "save-added": deferredProvider("save-added") };
      CONFIG.channels.precacheServices = ["retry-owed"];

      startPrecaching(deps);

      await fire(PRECACHE_DELAY);
      await saveList([ "retry-owed", "save-added" ]);

      assert.equal(clock.pending, 2, "the save's cycle is pending beside the re-attempt");

      stopPrecaching();

      assert.equal(clock.pending, 0, "the stop disposed every pending timer");
    });
  });
});

describe("runPrecacheCycle - deps threading through the internal precacheService call", () => {

  let originalServices: string[];

  beforeEach(async () => {

    // A fresh clock per row, with the dependency set rebuilt around it, so a schedule this row arms is read on a ledger no earlier row wrote to.
    clock = new TestClock();
    deps = makeDeps(clock);
    originalServices = CONFIG.channels.precacheServices;

    windowSyncCalls = 0;
    mockGuideUrls = { "stub-revalidate": "https://www.stub-revalidate.test/guide" };
    mockProviders = { "stub-revalidate": makeStubProvider(async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL) };
    stubBrowser = { newPage: async (): Promise<Page> => makeStubPage() } as unknown as Browser;

    stopPrecaching();
    clearLoginState();

    // The health store's flush debounce arms on the clock this establishment supplies, so the one global timer these rows would otherwise leave to the platform
    // is virtual and nothing writes to a real data directory after the test ends. The clock is separate from the scheduler's so this arm never colors that ledger.
    disposeHealthStore = await useHealthStoreOnClock(new TestClock());
  });

  afterEach(async () => {

    stopPrecaching();
    clearLoginState();
    CONFIG.channels.precacheServices = originalServices;
    await disposeHealthStore();
  });

  test("threads the injected deps through to precacheService rather than falling back to defaultPrecachingDeps", async () => {

    /* Traced path: runPrecacheCycle's per-service call site, `precacheService(provider, deps)`. precacheService's own signature defaults its second parameter to
     * defaultPrecachingDeps, so a call site that drops deps silently falls back to the module's real browser accessors instead of the cycle's injected stub - in
     * production this is behavior-neutral (defaultPrecachingDeps IS the real accessors), but it would defeat the PrecachingDeps injection for exactly this test, since a
     * regression here would attempt a real Chrome launch through defaultPrecachingDeps.getCurrentBrowser rather than ever touching the stub deps below. We prove
     * the injected deps reach precacheService by instrumenting getCurrentBrowser - the first collaborator precacheService calls - on a deps copy distinct from the
     * module-level `deps` object the other describe blocks share, so a regression cannot hide behind a call the shared object's own getCurrentBrowser happens to
     * satisfy.
     *
     * The cycle itself is driven off the row's own clock, which is where the scheduler arms it: advancing to the settle delay runs the callback exactly as the
     * platform would, on a timeline no other row in this file shares. The single configured service slug means the cycle's loop runs exactly once and terminates
     * on its own; no re-scheduled timer or second pass follows.
     */
    let getCurrentBrowserCalls = 0;

    const fakeDeps: PrecachingDeps = {

      ...deps,
      getCurrentBrowser: async (): Promise<Browser> => {

        getCurrentBrowserCalls++;

        return stubBrowser;
      }
    };

    CONFIG.channels.precacheServices = ["stub-revalidate"];

    startPrecaching(fakeDeps);

    assert.equal(clock.pending, 1, "startPrecaching armed the cycle on the injected clock");

    clock.advance(PRECACHE_DELAY);

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

  beforeEach(async () => {

    // A fresh clock per row, with the dependency set rebuilt around it, so a schedule this row arms is read on a ledger no earlier row wrote to.
    clock = new TestClock();
    deps = makeDeps(clock);
    windowSyncCalls = 0;

    clearLoginState();

    // The health store's flush debounce arms on the clock this establishment supplies, so the one global timer these rows would otherwise leave to the platform
    // is virtual and nothing writes to a real data directory after the test ends. The clock is separate from the scheduler's so this arm never colors that ledger.
    disposeHealthStore = await useHealthStoreOnClock(new TestClock());
  });

  afterEach(async () => {

    clearLoginState();
    await disposeHealthStore();
  });

  test("navigates to the guide URL when the provider does not handle its own navigation", async () => {

    /* Traced path: the handlesOwnNavigation branch in withProviderGuidePage, reached through precacheService. A provider that does not intercept its own navigation
     * relies on that helper to drive the page to the guide URL before discovery; dropping the goto would leave discovery running against a blank page.
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

    /* Traced path: the try/catch around page.close() in withProviderGuidePage's finally, reached through precacheService. If the browser disconnects mid-discovery
     * the page is already gone and close() rejects; swallowing it keeps a per-service teardown failure from turning a successful discovery into a rejected precache.
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
 * walk through precacheService is what makes these rows the port's assertion rather than a direct call to the recorder: what is observed is that the walk's own
 * collaborators - not the module's production wiring - are what the write travelled through.
 */
describe("precacheService - the lineup write through the injection port", () => {

  beforeEach(async () => {

    // A fresh clock per row, with the dependency set rebuilt around it, so a schedule this row arms is read on a ledger no earlier row wrote to.
    clock = new TestClock();
    deps = makeDeps(clock);
    persistedLineups.length = 0;
    windowSyncCalls = 0;

    clearLoginState();

    // The health store's flush debounce arms on the clock this establishment supplies, so the one global timer these rows would otherwise leave to the platform
    // is virtual and nothing writes to a real data directory after the test ends. The clock is separate from the scheduler's so this arm never colors that ledger.
    disposeHealthStore = await useHealthStoreOnClock(new TestClock());
  });

  afterEach(async () => {

    clearLoginState();
    await disposeHealthStore();
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

  beforeEach(async () => {

    // A fresh clock per row, with the dependency set rebuilt around it, so a schedule this row arms is read on a ledger no earlier row wrote to.
    clock = new TestClock();
    deps = makeDeps(clock);
    discoveryPageCreations = [];
    newPageOptions = [];
    overlayHandlingCalls = [];
    pageEvents = [];
    registrations = [];
    windowSyncCalls = 0;

    stubBrowser = {

      newPage: async (options?: unknown): Promise<Page> => {

        newPageOptions.push(options);

        return makeStubPage();
      }
    } as unknown as Browser;

    clearLoginState();

    // The health store's flush debounce arms on the clock this establishment supplies, so the one global timer these rows would otherwise leave to the platform
    // is virtual and nothing writes to a real data directory after the test ends. The clock is separate from the scheduler's so this arm never colors that ledger.
    disposeHealthStore = await useHealthStoreOnClock(new TestClock());
  });

  afterEach(async () => {

    clearLoginState();
    await disposeHealthStore();
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

    /* Traced path: the helper's happy sequence for a caller-navigated provider. The event log asserts the mute-before-navigation and poll-before-navigation ordering, the
     * recorded poll asserts the discovery phase, and the abort snapshot taken inside afterWalk asserts that the poll is already stopped before any classification runs -
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
    assert.equal(firstPoll.clock, clock, "the poll runs on the scheduler's own clock rather than on a hard-coded system clock");
    assert.ok(pageEvents.indexOf("mute") < pageEvents.indexOf("goto"), "the mute override installs before navigation");
    assert.ok(pageEvents.indexOf("poll:discovery") < pageEvents.indexOf("goto"), "the discovery poll launches before navigation");
    assert.equal(signalAbortedInAfterWalk, true, "the overlay poll is aborted before afterWalk classifies the page");
  });

  test("takes its page from the discovery-page creator, on the declared layout surface, before it navigates", async () => {

    /* Where the guide page comes from is the assertion here. The session asks the browser layer's creator for it exactly once, handing over the browser it acquired,
     * and passes no creation options of its own - the window, the background, and the placement are the creator's to decide, and they are asserted where the
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
     * the walk completes. The pre-abort "not yet closed" check and the post-abort "closed" check tie the close to the abort itself rather than the finally.
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

  test("registers the guide page as held in flight for the walk's duration", async () => {

    /* The stale-page sweep closes a managed page nothing owns once its grace period elapses, and a discovery page is owned by the walk rather than by any stream
     * the registry records - so without the mark the sweep would be the walk's ceiling instead of a safety net, and a walk longer than the grace period would
     * lose its own page. The finally's unregister drops the mark, so nothing outlives the walk.
     */
    const provider = guideProvider(false, async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL);

    await withProviderGuidePage(provider, {}, deps);

    assert.deepEqual(registrations, [{ inFlight: true }], "the page is registered exactly once, marked as held in flight");
  });

  test("stops a walk that outlives its budget, closing its page before the rejection surfaces", async () => {

    /* The deadline has to cancel the walk, not merely stop waiting on it: a walk left running would keep driving a page the session has moved on from. Closing
     * the page is the cancellation, so the row reads the close the instant the deadline fires - synchronously inside the advance, before the rejection has had a
     * microtask to propagate - and only then reads the typed rejection. The clock's ledger is also what proves the budget is the one the module declares rather
     * than some other timer.
     */
    const hang = Promise.withResolvers<DiscoveredChannel[]>();
    const provider = guideProvider(true, async (): Promise<DiscoveredChannel[]> => hang.promise);
    const pending = withProviderGuidePage(provider, {}, deps);

    await settle(SETTLE_TURNS);

    assert.equal(clock.pending, 1, "the walk armed exactly one deadline on the injected clock");
    assert.deepEqual(clock.requested, [WALK_BUDGET], "at the budget the module declares");
    assert.ok(!pageEvents.includes("close"), "the page stays open while the walk is inside its budget");

    clock.advance(WALK_BUDGET);

    assert.equal(pageEvents.filter((event) => event === "close").length, 1, "the lapse itself closed the page, before the rejection could unwind the session");

    await assert.rejects(pending, DiscoveryWalkTimeoutError, "the lapse surfaces as its own type rather than as a generic failure");

    hang.resolve([]);
  });
});

/* An empty discovery walk is the failure this retry exists to answer: a rail or grid whose lazy content never populated inside the walk's budget leaves the provider
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

  beforeEach(async () => {

    // A fresh clock per row, with the dependency set rebuilt around it, so a schedule this row arms is read on a ledger no earlier row wrote to.
    clock = new TestClock();
    deps = makeDeps(clock);
    overlayHandlingCalls = [];
    retryEvents = [];
    recorded = [];
    walkResults = [];
    walks = 0;

    clearLoginState();

    // The health store's flush debounce arms on the clock this establishment supplies, so the one global timer these rows would otherwise leave to the platform
    // is virtual and nothing writes to a real data directory after the test ends. The clock is separate from the scheduler's so this arm never colors that ledger.
    disposeHealthStore = await useHealthStoreOnClock(new TestClock());
  });

  afterEach(async () => {

    clearLoginState();
    await disposeHealthStore();
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
    assert.deepEqual(overlayHandlingCalls.map((call) => call.clock), [ clock, clock ], "both polls run on the scheduler's own clock");
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

  test("gives the retry a budget of its own rather than the remainder of the first walk's", async () => {

    /* Each walk arms its own deadline, which is what keeps the retry bounded exactly like the first attempt: a retry sharing one budget with the walk before it
     * would inherit whatever that walk had already spent, and on a slow guide it would be cut off before it began. The row reads the deadlines the session armed
     * off the clock's ledger - one per walk, each at the full budget - and then fires the retry's own, which is the deadline that has to be the live one: the
     * first walk's was disposed when that walk settled, so the next deadline the clock holds is the retry's.
     */
    const hang = Promise.withResolvers<DiscoveredChannel[]>();

    stubBrowser = { newPage: async (): Promise<Page> => makeRetryPage() } as unknown as Browser;

    // The first walk answers immediately and empty, which is what earns the retry; the second hangs, so the retry's own deadline is the one that decides.
    const provider = {

      ...retryProvider(),
      discoverChannels: async (): Promise<DiscoveredChannel[]> => {

        walks++;

        return (walks === 1) ? [] : hang.promise;
      }
    } as unknown as ProviderModule;

    const pending = withProviderGuidePage(provider, { afterWalk }, deps);

    await settle(SETTLE_TURNS);

    assert.equal(walks, 2, "the empty first walk earned its retry");
    assert.deepEqual(clock.requested, [ WALK_BUDGET, WALK_BUDGET ], "each walk armed a deadline of its own, both at the full budget");
    assert.equal(clock.pending, 1, "only the retry's deadline is still live - the first walk's was disposed when that walk settled");

    assert.ok(clock.advanceToNext(), "the clock stepped to the retry's deadline");
    assert.ok(retryEvents.includes("close"), "the retry's lapse closes the page");

    await assert.rejects(pending, DiscoveryWalkTimeoutError, "and the retry's lapse is what the session rejects with");

    hang.resolve([]);
  });
});
