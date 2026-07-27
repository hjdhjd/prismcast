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
 */
import type { AddressInfo, Server } from "node:net";
import type { Browser, Page } from "puppeteer-core";
import type { DiscoveredChannel, ProviderModule } from "../types/index.ts";
import { after, before, beforeEach, describe, test } from "node:test";
import { recordDiscoveryOutcome, withProviderGuidePage } from "../browser/precaching.ts";
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

let barePort = 0;
let bareServer: Server;
let delegatingPort = 0;
let delegatingServer: Server;

function urlFor(port: number, path: string): string {

  return "http://127.0.0.1:" + String(port) + path;
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
 * recording page, startOverlayHandling records each poll's phase and abort signal in place of a live poll, and the managed-page bookkeeping, shutdown probe, window
 * minimize, and provider lookups are the remaining members the helper's dependency closure requires. Only the browser accessor and the overlay poll are exercised;
 * the rest are inert because the discovery success path never revalidates a domain or minimizes a real window.
 */
const stubPrecachingDeps: PrecachingDeps = {

  getCurrentBrowser: async (): Promise<Browser> => stubBrowser,
  getProviderBySlug: (slug: string): ProviderModule | undefined => ((slug === DRIVEN_SLUG) ? stubProvider : undefined),
  getProvidersForDomain: (): ProviderModule[] => [],
  isGracefulShutdown: (): boolean => false,
  minimizeBrowserWindow: async (): Promise<void> => { /* No window to minimize on a stub browser. */ },
  registerManagedPage: (): void => { /* Stub pages need no bookkeeping. */ },
  startOverlayHandling: async (_page: Page, _profile: unknown, options: StartOverlayHandlingOptions): Promise<void> => {

    pageEvents.push("poll:" + options.phase);
    overlayHandlingCalls.push(options);
  },
  unregisterManagedPage: (): void => { /* Stub pages need no bookkeeping. */ }
};

/* The delegating route wiring: getProviderBySlug resolves the stub for the driven slug, recordDiscoveryOutcome is the real policy (its no-op clear branch for this
 * non-empty result), and withProviderGuidePage is the REAL helper bound to the stub PrecachingDeps above. Binding the real helper is what makes the mute/goto/poll/
 * close effects observable while keeping precaching.ts exercised - the route delegates to the genuine guarded session, only its browser and overlay poll are stubbed.
 */
const delegatingDeps: ServiceDiscoveryDeps = {

  getProviderBySlug: (slug: string): ProviderModule | undefined => ((slug === DRIVEN_SLUG) ? stubProvider : undefined),
  recordDiscoveryOutcome,
  withProviderGuidePage: (provider, options): Promise<DiscoveredChannel[]> => withProviderGuidePage(provider, options, stubPrecachingDeps)
};

/* The non-delegating control: withProviderGuidePage returns the walk's channels WITHOUT the real helper's page lifecycle - no mute injection, no guide navigation, no
 * discovery poll, no page close. This stands in for a route that inlined a bare page lifecycle instead of delegating to the guarded session. recordDiscoveryOutcome is
 * present but never reached, because this stub never invokes the afterWalk hook the route wires into it.
 */
const bareDeps: ServiceDiscoveryDeps = {

  getProviderBySlug: (slug: string): ProviderModule | undefined => ((slug === DRIVEN_SLUG) ? stubProvider : undefined),
  recordDiscoveryOutcome,
  withProviderGuidePage: async (): Promise<DiscoveredChannel[]> => UNSORTED_CHANNELS
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

  delegatingPort = delegating.port;
  delegatingServer = delegating.server;
  barePort = bare.port;
  bareServer = bare.server;
});

after(async () => {

  await closeServer(delegatingServer);
  await closeServer(bareServer);
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
