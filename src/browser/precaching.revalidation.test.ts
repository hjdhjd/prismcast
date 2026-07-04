/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * precaching.revalidation.test.ts: Unit tests for the post-login revalidation flow (revalidateDomainAuth) and the login-mode minimize guard in precaching.ts.
 * Running a revalidation drives getCurrentBrowser/newPage, which would launch a real Chrome; mock.module + dynamic import is the canonical seam for swapping the
 * browser accessors and the provider registry without driving a browser (the hls.loginMode.test.ts precedent). The seam is isolated in this file so the static-
 * import tests in precaching.test.ts stay unaffected. The health and login modules are real - state assertions go through getDomainAuthState, and login mode is
 * driven through the real startLoginMode/clearLoginState with stub accessors.
 */
import type * as PrecachingModule from "./precaching.ts";
import type { Browser, Page } from "puppeteer-core";
import type { DiscoveredChannel, ProviderModule } from "../types/index.ts";
import { LOG, extractDomain } from "../utils/index.ts";
import { afterEach, before, beforeEach, describe, mock, test } from "node:test";
import { clearLoginState, setBrowserAccessors, startLoginMode } from "./login.ts";
import { getDomainAuthState, markDomainAuthRequired } from "../config/health.ts";
import { CONFIG } from "../config/index.ts";
import type { Nullable } from "../types/index.ts";
import assert from "node:assert/strict";

// Mutable state the module mocks read, so each test can shape the provider registry and browser behavior without re-registering mocks.
let mockGuideUrls: Record<string, string> = {};
let mockProviders: Record<string, ProviderModule> = {};
let minimizeCalls = 0;
let stubBrowser: Browser;

// The functions under test, bound after mock.module registration via dynamic import.
let precacheService: typeof PrecachingModule.precacheService;
let revalidateDomainAuth: typeof PrecachingModule.revalidateDomainAuth;
let startPrecaching: typeof PrecachingModule.startPrecaching;
let stopPrecaching: typeof PrecachingModule.stopPrecaching;

// Builds a stub Page satisfying the surface precacheService touches. The evaluate stub reports "no consent overlay / no containers" so empty discoveries classify
// unknown; the revalidation happy paths return non-empty discoveries and never reach classification.
function makeStubPage(): Page {

  return {

    close: async (): Promise<void> => { /* Nothing to close on a stub. */ },
    evaluate: async (): Promise<unknown> => false,
    evaluateOnNewDocument: async (): Promise<void> => { /* The mute injection is a no-op on a stub. */ },
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

before(async () => {

  /* Unlike the hls.loginMode.test.ts precedent, the mocks enumerate their named exports explicitly instead of spreading the real module: the real browser/index.ts
   * barrel transitively imports precaching.ts itself, so loading it here to spread would cache the REAL precaching module before the mock could take effect, and
   * the dynamic import below would receive the unmocked instance. The enumerated names are exactly the surface precaching.ts consumes from each module.
   *
   * The Node 22 type definitions surface the option as namedExports; the runtime renamed it to exports in a later minor and emits a deprecation warning. We keep
   * namedExports until @types/node catches up - the runtime path is unaffected and the type definition is authoritative for the build.
   */
  const browserUrl = new URL("./index.ts", import.meta.url).href;

  mock.module(browserUrl, {

    namedExports: {

      getCurrentBrowser: async (): Promise<Browser> => stubBrowser,
      isGracefulShutdown: (): boolean => false,
      minimizeBrowserWindow: async (): Promise<void> => {

        minimizeCalls++;
      },
      registerManagedPage: (): void => { /* Stub pages need no bookkeeping. */ },
      unregisterManagedPage: (): void => { /* Stub pages need no bookkeeping. */ }
    }
  });

  const selectionUrl = new URL("./channelSelection.ts", import.meta.url).href;

  mock.module(selectionUrl, {

    namedExports: {

      getProviderBySlug: (slug: string): ProviderModule | undefined => mockProviders[slug],
      getProviderGuideUrls: (): Record<string, string> => mockGuideUrls,
      getProvidersForDomain: (domain: string): ProviderModule[] => Object.entries(mockGuideUrls)
        .filter(([ , guideUrl ]) => extractDomain(guideUrl) === domain).flatMap(([slug]) => mockProviders[slug] ?? [])
    }
  });

  // Dynamic-import precaching.ts now that the mocks are registered, so its captured imports resolve to the overrides above.
  const precachingModule = await import("./precaching.ts");

  precacheService = precachingModule.precacheService;
  revalidateDomainAuth = precachingModule.revalidateDomainAuth;
  startPrecaching = precachingModule.startPrecaching;
  stopPrecaching = precachingModule.stopPrecaching;
});

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

    await revalidateDomainAuth("https://www.stub-revalidate.test/somewhere");

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
    await revalidateDomainAuth("https://www.stub-revalidate.test/login");

    assert.equal(getDomainAuthState("stub-revalidate.test")?.status, "needsLogin", "the flag stays set while login mode is active");
  });

  test("defers to an in-flight precache cycle at INFO without touching the flag", async (t) => {

    /* Traced path: the precacheInProgress guard. startPrecaching sets the single-flight flag before its delay timer fires (the timer is mocked and never runs), so
     * the revalidation must take the deferral branch - log at INFO and leave the flag for the cycle's own discovery to clear.
     */
    const info = t.mock.method(LOG, "info", () => { /* Captured via the mock. */ });

    CONFIG.channels.precacheServices = ["stub-revalidate"];
    startPrecaching();

    markDomainAuthRequired("stub-revalidate.test");

    await revalidateDomainAuth("https://www.stub-revalidate.test/login");

    assert.equal(getDomainAuthState("stub-revalidate.test")?.status, "needsLogin", "the flag is left for the in-flight cycle");

    const deferralLine = info.mock.calls.find((call) => String(call.arguments[0]).includes("Deferring the post-login revalidation"));

    assert.ok(deferralLine, "the deferral is reported at INFO");
  });

  test("skips quietly when no provider guide matches the domain", async () => {

    // Traced path: the empty-providers early return after registry matching - a flagged domain with no registered provider has nothing to revalidate against.
    mockGuideUrls = {};

    markDomainAuthRequired("orphan-flag.test");

    await revalidateDomainAuth("https://www.orphan-flag.test/login");

    assert.equal(getDomainAuthState("orphan-flag.test")?.status, "needsLogin", "the flag stays; only discovery evidence clears it");
  });

  test("runs discovery for the matching provider and clears the flag to verified on success (the happy path)", async () => {

    /* Traced path: the full flow - flag present, no guards trip, the provider matches by extracted guide domain, precacheService discovers a non-empty lineup, and
     * recordDiscoveryOutcome's non-empty arm marks the domain verified through the criterion-1 chokepoint. This is the needsLogin -> verified round trip the
     * login-end observer exists to produce.
     */
    markDomainAuthRequired("stub-revalidate.test");

    await revalidateDomainAuth("https://www.stub-revalidate.test/login");

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

    const inFlight = revalidateDomainAuth("https://www.stub-revalidate.test/login");

    // The guard is held; a cycle request now must defer.
    CONFIG.channels.precacheServices = ["stub-revalidate"];
    startPrecaching();

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

    await assert.doesNotReject(() => revalidateDomainAuth("https://www.stub-revalidate.test/login"), "revalidateDomainAuth never rejects");

    assert.equal(getDomainAuthState("stub-revalidate.test")?.status, "needsLogin", "the flag stays set when the wall is still up");
    assert.ok(warn.mock.calls.length >= 1, "the per-provider failure is reported at WARN");
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

    await precacheService(makeStubProvider(async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL));

    assert.equal(minimizeCalls, 0, "no minimize while login mode is active");
  });

  test("re-minimizes the window when login mode is inactive", async () => {

    // The complementary arm: with login mode inactive, the pre-existing re-minimize behavior is unchanged.
    await precacheService(makeStubProvider(async (): Promise<DiscoveredChannel[]> => ONE_CHANNEL));

    assert.equal(minimizeCalls, 1, "the discovery page cleanup re-minimizes as before");
  });
});
