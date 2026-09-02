/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * services.delegation.test.ts: Unit test pinning that the /services/:slug/channels route delegates its discovery walk to the shared guarded guide-page session
 * (withProviderGuidePage in browser/precaching.ts) rather than owning the page lifecycle itself. The route exposes a ServiceDiscoveryDeps injection point; the test
 * drives the route through the REAL withProviderGuidePage, bound to a stub PrecachingDeps so the helper reaches for a stub browser and a recording overlay poll
 * rather than a live Chrome. The helper's observable effects are asserted - the audio-mute override installs before the guide navigation, the navigation targets the
 * provider's guideUrl, exactly one discovery-phase overlay poll launches, the page is closed by the end, and the response carries the walk's channels sorted by name.
 * Those effects exist only inside withProviderGuidePage, so a route that inlined the page lifecycle instead of delegating would produce none of them - a more
 * reliable way to tell delegation from a mocked call than a spy on the helper's call.
 *
 * A second run pins that the assertions above actually tell delegation from non-delegation apart: the same route wired to a non-delegating guide-page session - one that
 * returns the walk's channels without the real helper's page lifecycle - produces none of the mute/goto/poll/close effects, so the page-lifecycle assertions above
 * genuinely detect delegation rather than passing vacuously. recordDiscoveryOutcome is left real: for the driven provider its non-empty result takes the no-op clear
 * branch, so it records nothing to disk while its own coverage is exercised.
 *
 * A third run pins the refresh path's sequencing through the same injection point with a controllable guide-page session: a refresh clears the provider's caches only
 * after the walk it aborted has settled, the replacement walk it registers is there for any request that arrives behind the abort, and a refresh with nothing in
 * flight still clears before the walk it starts. Those pins drive ordering, so each one owns a private service slug and proves every ordering it relies on with an
 * awaited signal rather than with request order.
 */
import type { AddressInfo, Server } from "node:net";
import type { Browser, Page } from "puppeteer-core";
import type { DiscoveredChannel, ProviderModule } from "../types/index.ts";
import { after, before, beforeEach, describe, test } from "node:test";
import { recordDiscoveryOutcome, withProviderGuidePage } from "../browser/precaching.ts";
import type { PersistedLineupChannel } from "../config/providerLineups.ts";
import type { PrecachingDeps } from "../browser/precaching.ts";
import type { ServiceDiscoveryDeps } from "./services.ts";
import type { StartOverlayHandlingOptions } from "../browser/consent.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWss } from "../testing.helpers.ts";
import express from "express";
import { setupServicesEndpoint } from "./services.ts";

// The slug the injected getProviderBySlug resolves to the stub provider. getCachedChannels returns null so the route skips its warm-cache short-circuit and reaches
// the discovery path, and validatePrecache returns false so the real recordDiscoveryOutcome takes its no-op clear branch (the domain is never flagged) rather than
// writing health state to disk. The double-cast documents that the route and helper touch only this subset of the provider surface.
const DRIVEN_SLUG = "stub-delegation";
const STUB_GUIDE_URL = "https://www.stub-delegation.test/guide";

// The channels the stub discoverChannels returns, deliberately out of name order so the route's own sort (by name) is observable in the response body.
const UNSORTED_CHANNELS = [ { channelSelector: "Bravo", name: "Bravo" }, { channelSelector: "AMC", name: "AMC" } ] as unknown as DiscoveredChannel[];

const stubProvider = {

  discoverChannels: async (): Promise<DiscoveredChannel[]> => UNSORTED_CHANNELS,
  getCachedChannels: (): null => null,
  guideUrl: STUB_GUIDE_URL,
  handlesOwnNavigation: false,
  label: "Stub Delegation",
  slug: DRIVEN_SLUG,
  strategy: {},
  validatePrecache: (): boolean => false
} as unknown as ProviderModule;

// An ordered log of the stub page's operations and the overlay-poll options the injected startOverlayHandling records, so the test can assert the helper's observable
// sequence - the mute-before-navigation ordering, the discovery-phase poll, the guide navigation target, and the page close - without a live Chrome.
const gotoUrls: string[] = [];
const overlayHandlingCalls: StartOverlayHandlingOptions[] = [];
const pageEvents: string[] = [];

// The lineup writes the real discovery-outcome recorder issues while the route drives it. Captured through the injected port rather than performed, so the suite
// exercises the recorder for real without writing a file into the data directory.
const persistedLineups: { channels: PersistedLineupChannel[]; slug: string }[] = [];

let barePort = 0;
let bareServer: Server;
let delegatingPort = 0;
let delegatingServer: Server;
let sequencedPort = 0;
let sequencedServer: Server;

function urlFor(port: number, path: string): string {

  return "http://127.0.0.1:" + String(port) + path;
}

/**
 * Builds a discovery URL on the sequencing server for one pin's private slug.
 * @param slug - The pin's service slug.
 * @param query - The query string to append, including its leading "?".
 * @returns The absolute discovery URL.
 */
function sequencedUrl(slug: string, query = ""): string {

  return urlFor(sequencedPort, "/services/" + slug + "/channels" + query);
}

/* Builds a stub Page satisfying the surface the guarded guide-page session touches. Every operation pushes to pageEvents so the delegation test can assert the order
 * of the mute injection, the navigation, and the close; goto also records its target URL. The double-cast documents that the session touches this subset, not the
 * full Page shape.
 */
function makeStubPage(): Page {

  return {

    close: async (): Promise<void> => { pageEvents.push("close"); },
    evaluate: async (): Promise<unknown> => false,
    evaluateOnNewDocument: async (): Promise<void> => { pageEvents.push("mute"); },
    goto: async (url: string): Promise<void> => {

      pageEvents.push("goto");
      gotoUrls.push(url);
    },
    isClosed: (): boolean => false,
    url: (): string => STUB_GUIDE_URL
  } as unknown as Page;
}

const stubBrowser = { newPage: async (): Promise<Page> => makeStubPage() } as unknown as Browser;

/* The PrecachingDeps the REAL withProviderGuidePage runs against in the delegating deps below: getCurrentBrowser hands back a stub browser whose newPage returns a
 * recording page, createDiscoveryPage delegates straight to that newPage so the real creator's window handling stays out of a route test, startOverlayHandling
 * records each poll's phase and abort signal in place of a live poll, emulateLayoutSurface answers with a fixed surface in place of a device-metrics override,
 * and the managed-page bookkeeping, shutdown probe, window sync, and provider lookups are the remaining members the helper's dependency closure requires. Only
 * the browser accessor, the page creation, the layout declaration, and the overlay poll are exercised; the rest are inert because the discovery success path
 * never revalidates a domain or drives a real window.
 */
const stubPrecachingDeps: PrecachingDeps = {

  createDiscoveryPage: async (browser: Browser): Promise<Page> => browser.newPage(),
  emulateLayoutSurface: async (): Promise<{ height: number; width: number }> => ({ height: 1080, width: 1920 }),
  getCurrentBrowser: async (): Promise<Browser> => stubBrowser,
  getProviderBySlug: (slug: string): ProviderModule | undefined => ((slug === DRIVEN_SLUG) ? stubProvider : undefined),
  getProvidersForDomain: (): ProviderModule[] => [],
  isGracefulShutdown: (): boolean => false,
  persistProviderLineup: async (slug: string, channels: PersistedLineupChannel[]): Promise<void> => {

    persistedLineups.push({ channels, slug });
  },
  registerManagedPage: (): void => { /* Stub pages need no bookkeeping. */ },
  startOverlayHandling: async (_page: Page, _profile: unknown, options: StartOverlayHandlingOptions): Promise<void> => {

    pageEvents.push("poll:" + options.phase);
    overlayHandlingCalls.push(options);
  },
  syncWindowVisibility: async (): Promise<void> => { /* No window to drive on a stub browser. */ },
  unregisterManagedPage: (): void => { /* Stub pages need no bookkeeping. */ }
};

/* The delegating route wiring: getProviderBySlug resolves the stub for the driven slug, recordDiscoveryOutcome is the real policy (its no-op clear branch for this
 * non-empty result), and withProviderGuidePage is the REAL helper bound to the stub PrecachingDeps above. Binding the real helper is what makes the mute/goto/poll/
 * close effects observable while keeping precaching.ts exercised - the route delegates to the genuine guarded session, only its browser and overlay poll are stubbed.
 */
const delegatingDeps: ServiceDiscoveryDeps = {

  getProviderBySlug: (slug: string): ProviderModule | undefined => ((slug === DRIVEN_SLUG) ? stubProvider : undefined),
  precachingDeps: stubPrecachingDeps,
  recordDiscoveryOutcome,
  withProviderGuidePage: (provider, options): Promise<DiscoveredChannel[]> => withProviderGuidePage(provider, options, stubPrecachingDeps)
};

/* The non-delegating control: withProviderGuidePage returns the walk's channels WITHOUT the real helper's page lifecycle - no mute injection, no guide navigation, no
 * discovery poll, no page close. This stands in for a route that inlined a bare page lifecycle instead of delegating to the guarded session. recordDiscoveryOutcome is
 * present but never reached, because this stub never invokes the afterWalk hook the route wires into it.
 */
const bareDeps: ServiceDiscoveryDeps = {

  getProviderBySlug: (slug: string): ProviderModule | undefined => ((slug === DRIVEN_SLUG) ? stubProvider : undefined),
  precachingDeps: stubPrecachingDeps,
  recordDiscoveryOutcome,
  withProviderGuidePage: async (): Promise<DiscoveredChannel[]> => UNSORTED_CHANNELS
};

/* The signals one stub discovery walk exposes to the pin that drives it. started fires when the stub guide-page session is entered, aborted fires when the signal the
 * session received is aborted, and result is the deferred whose settlement the stub returns - the pin settles every invocation explicitly, which is what lets every
 * request it fired drain before the test ends.
 *
 * signal is the abort signal that session received, held so a pin can ask whether the walk was ever cancelled. It answers what the aborted deferred cannot: a signal
 * aborted BEFORE the walk starts never fires a listener the walk registers afterward, so a pin claiming a walk was left alone has to read the signal's own state
 * rather than wait on a barrier that a pre-start cancellation would silently skip.
 */
interface SequencedWalk {

  aborted: PromiseWithResolvers<void>;
  result: PromiseWithResolvers<DiscoveredChannel[]>;
  signal?: AbortSignal;
  started: PromiseWithResolvers<void>;
}

// One refresh-sequencing pin's private service: its stub provider, the ordered log of everything observable about it, and the per-invocation signals its walks and
// cached-check expose.
interface SequencedService {

  // The barrier for the nth cached-check the route performs on this service. A non-refresh request calls getCachedChannels and then runs synchronously into the
  // coalesce block, so awaiting a check proves that request has already joined or created an in-flight entry.
  cacheCheck: (index: number) => PromiseWithResolvers<void>;

  // Claims the next invocation index. Called by the stub guide-page session as it starts a walk, never by a pin.
  claim: () => number;

  // The ordered log of this service's observable effects: each walk's start and settlement, and each cache clear.
  events: string[];

  // The stub provider the injected registry lookup resolves for this service's slug.
  provider: ProviderModule;

  // How many stub walks have started, so a pin can prove a request rode an existing walk instead of spawning its own.
  started: () => number;

  // The signals for the nth walk, created on demand so a pin can hold them before the request that triggers the walk is dispatched.
  walk: (index: number) => SequencedWalk;
}

// The channels every sequenced replacement walk resolves with, distinct from the delegation runs' channels so a pin's response assertion names its own fixture.
const SEQUENCED_CHANNELS = [{ channelSelector: "TNT", name: "TNT" }] as unknown as DiscoveredChannel[];

// The registry the sequencing deps resolve against. Registration is per pin, so the route's module-level in-flight map - shared by every request in this process - is
// partitioned by slug and no pin can inherit another's in-flight residue.
const sequencedServices = new Map<string, SequencedService>();

/**
 * Registers a controllable stub service for a refresh-sequencing pin.
 * @param slug - The pin's private service slug. No other pin may use it.
 * @returns The recorder for that service: its event log, its stub provider, and its per-invocation barriers.
 */
function registerSequencedService(slug: string): SequencedService {

  const cacheChecks: PromiseWithResolvers<void>[] = [];
  const events: string[] = [];
  const walks: SequencedWalk[] = [];
  let checks = 0;
  let starts = 0;

  const cacheCheck = (index: number): PromiseWithResolvers<void> => {

    let record = cacheChecks[index];

    if(!record) {

      // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
      record = Promise.withResolvers<void>();
      cacheChecks[index] = record;
    }

    return record;
  };

  const walk = (index: number): SequencedWalk => {

    let record = walks[index];

    if(!record) {

      // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
      record = { aborted: Promise.withResolvers<void>(), result: Promise.withResolvers<DiscoveredChannel[]>(), started: Promise.withResolvers<void>() };
      walks[index] = record;
    }

    return record;
  };

  // The stub provider surface the route touches: the cached-check that always misses (so every request reaches the discovery path) and a clearCache that records
  // its call in the shared event log, which is what makes the clear's position in the sequence observable.
  const provider = {

    getCachedChannels: (): null => {

      cacheCheck(checks++).resolve();

      return null;
    },
    guideUrl: "https://" + slug + ".test/guide",
    label: slug,
    slug,
    strategy: { clearCache: (): void => { events.push("cleared"); } }
  } as unknown as ProviderModule;

  const service: SequencedService = { cacheCheck, claim: (): number => starts++, events, provider, started: (): number => starts, walk };

  sequencedServices.set(slug, service);

  return service;
}

/* The sequencing route wiring: the registry lookup resolves each pin's stub provider by slug, and the guide-page session is a controllable stand-in that records the
 * walk's start, forwards the abort signal to that walk's barrier, and returns a deferred the pin settles by hand. The settlement is logged from a finally on the
 * deferred, so the event log carries the walk's settlement at the moment the route's own await of it can first observe it - which is what makes "the clear happened
 * after the aborted walk settled" an ordering the log can state rather than an inference. recordDiscoveryOutcome is real but never reached: this session never
 * invokes the afterWalk hook the route wires into it.
 */
const sequencedDeps: ServiceDiscoveryDeps = {

  getProviderBySlug: (slug: string): ProviderModule | undefined => sequencedServices.get(slug)?.provider,
  precachingDeps: stubPrecachingDeps,
  recordDiscoveryOutcome,
  withProviderGuidePage: async (provider, options): Promise<DiscoveredChannel[]> => {

    const service = sequencedServices.get(provider.slug)!;
    const walk = service.walk(service.claim());

    walk.signal = options?.signal;
    options?.signal?.addEventListener("abort", (): void => walk.aborted.resolve(), { once: true });
    service.events.push("walk-started");
    walk.started.resolve();

    return await walk.result.promise.finally((): void => {

      service.events.push("walk-settled");
    });
  }
};

function startServer(deps: ServiceDiscoveryDeps): Promise<{ port: number; server: Server }> {

  const app = express();

  setupServicesEndpoint(app, deps);

  return new Promise((resolve, reject) => {

    const server = app.listen(0, "127.0.0.1", () => {

      const address = server.address() as AddressInfo;

      resolve({ port: address.port, server });
    });

    server.on("error", reject);
  });
}

function closeServer(server: Server): Promise<void> {

  return new Promise((resolve) => {

    server.close(() => {

      resolve();
    });
  });
}

before(async () => {

  const delegating = await startServer(delegatingDeps);
  const bare = await startServer(bareDeps);
  const sequenced = await startServer(sequencedDeps);

  delegatingPort = delegating.port;
  delegatingServer = delegating.server;
  barePort = bare.port;
  bareServer = bare.server;
  sequencedPort = sequenced.port;
  sequencedServer = sequenced.server;
});

after(async () => {

  await closeServer(delegatingServer);
  await closeServer(bareServer);
  await closeServer(sequencedServer);
  await closePuppeteerStreamWss();
});

beforeEach(() => {

  gotoUrls.length = 0;
  overlayHandlingCalls.length = 0;
  pageEvents.length = 0;
});

describe("setupServicesEndpoint - discovery delegates to the real guarded guide-page session", () => {

  test("drives withProviderGuidePage end-to-end: mute before navigation, the discovery poll, the guide goto, the page close, and the sorted response", async () => {

    /* Traced path: the injected getProviderBySlug resolves the stub, the empty cache skips the warm-cache short-circuit, and runDiscovery hands the walk to the real
     * withProviderGuidePage. The checks that would fail against a route that inlined the page lifecycle rather than delegating: the mute override is installed
     * before the guide navigation, exactly one poll launches under the discovery phase, the navigation targets the provider's guideUrl, the page is closed on cleanup,
     * and the response carries the walk's channels sorted by name. None of these effects exist outside withProviderGuidePage.
     */
    const res = await fetch(urlFor(delegatingPort, "/services/" + DRIVEN_SLUG + "/channels"));
    const body = await res.json() as DiscoveredChannel[];

    assert.equal(res.status, 200, "the discovery request succeeds");
    assert.equal(overlayHandlingCalls.length, 1, "exactly one overlay poll was launched by the helper");

    const firstPoll = overlayHandlingCalls[0];

    assert.ok(firstPoll, "the overlay poll was recorded");
    assert.equal(firstPoll.phase, "discovery", "the guide walk runs under the discovery phase");
    assert.ok(pageEvents.includes("mute") && (pageEvents.indexOf("mute") < pageEvents.indexOf("goto")), "the mute override installs before navigation");
    assert.ok(pageEvents.indexOf("poll:discovery") < pageEvents.indexOf("goto"), "the discovery poll launches before navigation");
    assert.deepEqual(gotoUrls, [STUB_GUIDE_URL], "the navigation targets the provider's guideUrl");
    assert.ok(pageEvents.includes("close"), "the helper closes the page on cleanup");
    assert.deepEqual(body.map((c) => c.name), [ "AMC", "Bravo" ], "the response carries the walk's channels, sorted by name");
  });
});

describe("setupServicesEndpoint - the delegation discriminators actually discriminate", () => {

  test("a route wired to a non-delegating guide-page session produces none of the real helper's page-lifecycle effects", async () => {

    // Control: bareDeps.withProviderGuidePage returns the walk's channels without the real helper's page lifecycle. This stands in for a route that inlined a bare
    // page lifecycle instead of delegating. The response still carries the channels sorted (runDiscovery sorts regardless of who walked), so the sorted body alone
    // cannot tell delegation from non-delegation - only the page-lifecycle effects can, which is exactly why the positive test asserts them. Their absence here proves
    // those checks genuinely detect delegation rather than passing vacuously.
    const res = await fetch(urlFor(barePort, "/services/" + DRIVEN_SLUG + "/channels"));
    const body = await res.json() as DiscoveredChannel[];

    assert.equal(res.status, 200, "the discovery request still succeeds against the non-delegating session");
    assert.deepEqual(body.map((c) => c.name), [ "AMC", "Bravo" ], "the response is sorted regardless of who walked, so the body cannot discriminate delegation");
    assert.equal(overlayHandlingCalls.length, 0, "a non-delegating session launches no discovery poll");
    assert.deepEqual(gotoUrls, [], "a non-delegating session performs no guide navigation");
    assert.deepEqual(pageEvents, [], "a non-delegating session touches no page: no mute, no goto, no close");
  });
});

describe("setupServicesEndpoint - a refresh sequences its cache clear behind the walk it aborts", () => {

  test("clears the service's caches only after the aborted walk settles, and answers the refresh from the replacement walk", async () => {

    const slug = "stub-sequencing-settle";
    const service = registerSequencedService(slug);
    const doomed = service.walk(0);
    const replacement = service.walk(1);

    /* Traced path: the first request creates the in-flight entry and its walk; the refresh request aborts that entry and registers a replacement whose promise
     * awaits the doomed walk's settlement before clearing. The barriers are what make this a claim about ordering rather than about timing - the first proves the
     * doomed walk is in flight before the refresh is dispatched, and the second proves the refresh's abort has already run, so everything the refresh does in its
     * own synchronous turn has happened by the time the clear is asserted absent.
     */
    const doomedResponse = fetch(sequencedUrl(slug));

    await doomed.started.promise;

    const refreshResponse = fetch(sequencedUrl(slug, "?refresh=true"));

    await doomed.aborted.promise;

    assert.deepEqual(service.events, ["walk-started"], "the caches are untouched while the aborted walk is still settling");

    // An aborted walk settles by rejecting: the guarded session closes its page out from under the walk and the in-flight Puppeteer call fails. The route maps
    // that rejection to its abort sentinel, which is what sends the first request around its retry loop.
    doomed.result.reject(new Error("Discovery aborted."));

    await replacement.started.promise;

    assert.deepEqual(service.events, [ "walk-started", "walk-settled", "cleared", "walk-started" ],
      "the clear lands after the aborted walk settled and before the replacement walk starts");

    replacement.result.resolve(SEQUENCED_CHANNELS);

    const [ doomedRes, refreshRes ] = await Promise.all([ doomedResponse, refreshResponse ]);
    const [ doomedBody, refreshBody ] = await Promise.all([ doomedRes.json() as Promise<DiscoveredChannel[]>, refreshRes.json() as Promise<DiscoveredChannel[]> ]);

    assert.equal(refreshRes.status, 200, "the refresh request succeeds");
    assert.deepEqual(refreshBody.map((c) => c.name), ["TNT"], "the refresh is answered by the replacement walk it registered");
    assert.deepEqual(doomedBody.map((c) => c.name), ["TNT"], "the aborted request retries onto the replacement rather than surfacing the abort");
  });

  test("a request arriving behind the abort rides the replacement walk rather than starting its own", async () => {

    const slug = "stub-sequencing-piggyback";
    const service = registerSequencedService(slug);
    const doomed = service.walk(0);
    const replacement = service.walk(1);
    const doomedResponse = fetch(sequencedUrl(slug));

    await doomed.started.promise;

    const refreshResponse = fetch(sequencedUrl(slug, "?refresh=true"));

    await doomed.aborted.promise;

    /* The late request is dispatched only after the abort has provably run, so the only in-flight state it can find is what the refresh left behind. Its
     * cached-check barrier proves its handler reached the coalesce block: a non-refresh request runs from that check into the coalesce block with nothing to await
     * in between. Whether it coalesced onto the replacement or retried onto it through the loop, both paths prove a successor existed in the abort's own turn -
     * and the walk count proves it rode that successor instead of starting a walk of its own.
     */
    const lateResponse = fetch(sequencedUrl(slug));

    await service.cacheCheck(1).promise;

    doomed.result.reject(new Error("Discovery aborted."));

    await replacement.started.promise;

    replacement.result.resolve(SEQUENCED_CHANNELS);

    const [ doomedRes, refreshRes, lateRes ] = await Promise.all([ doomedResponse, refreshResponse, lateResponse ]);
    const [ , , lateBody ] = await Promise.all([ doomedRes.json(), refreshRes.json(), lateRes.json() as Promise<DiscoveredChannel[]> ]);

    assert.equal(lateRes.status, 200, "the late request succeeds");
    assert.deepEqual(lateBody.map((c) => c.name), ["TNT"], "the late request is answered by the replacement walk");
    assert.equal(service.started(), 2, "the late request rode the replacement instead of starting a third walk");

    // Riding the replacement means leaving it alone: a piggybacking request takes the walk's result and never cancels the walk it joined, so the replacement's
    // signal staying quiet is part of what this pin claims. The signal's own state is what answers that, because a cancellation landing before the walk starts
    // would never reach a listener the walk registers afterward.
    assert.ok(replacement.signal, "the replacement walk received an abort signal");
    assert.equal(replacement.signal.aborted, false, "the replacement walk was never cancelled by the request that piggybacked on it");
  });

  test("a refresh with nothing in flight clears before the walk it starts", async () => {

    const slug = "stub-sequencing-cold";
    const service = registerSequencedService(slug);
    const walk = service.walk(0);
    const response = fetch(sequencedUrl(slug, "?refresh=true"));

    await walk.started.promise;

    // The claim is the clear's ORDER against the walk, which is what a rework that cleared after starting the walk would break. A clear merely deferred by a
    // microtask while still preceding the walk is invisible to an order assertion and is not claimed here.
    assert.deepEqual(service.events, [ "cleared", "walk-started" ], "the clear precedes the walk it makes room for");

    walk.result.resolve(SEQUENCED_CHANNELS);

    const res = await response;
    const body = await res.json() as DiscoveredChannel[];

    assert.equal(res.status, 200, "the refresh request succeeds");
    assert.deepEqual(body.map((c) => c.name), ["TNT"], "the response carries the fresh walk's channels");
  });
});
