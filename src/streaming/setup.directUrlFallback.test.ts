/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * setup.directUrlFallback.test.ts: Setup-tier tests for the direct-watch-URL failure contract and the guide fallback it drives. Three things are asserted here.
 * createPageWithCapture's catch asks the coordinator whether the failure was evidence against the URL it navigated to, and throws the typed
 * DirectUrlEstablishmentError only when the answer is yes - a page-death failure, which the retention policy keeps the URL for, rethrows raw. The skipDirectUrl
 * option makes the resolution not happen at all, which is what makes a second typed error structurally impossible on the fallback attempt. And setupStream turns
 * the typed error into exactly one more establishment, down the guide path.
 *
 * Everything runs through the CreatePageWithCaptureDeps collaborators the sibling setup.captureLock.test.ts already uses: a stub browser hands back a recording
 * page whose goto rejects with the failure each case wants, and the capture acquisition returns a plain Readable, so no Chrome and no CDP are involved. The direct
 * URL under test comes from the persisted lineup store rather than a live provider cache, which is the cold-boot shape the feature exists for - the store is
 * pointed at a temp data directory so nothing is written outside it.
 */
import type { Browser, Page } from "puppeteer-core";
import type { CaptureMode, ResolvedSiteProfile } from "../types/index.ts";
import { DirectUrlEstablishmentError, createPageWithCapture, setupStream } from "./setup.ts";
import { after, before, beforeEach, describe, test } from "node:test";
import { evictPersistedWatchUrl, persistProviderLineup } from "../config/providerLineups.ts";
import { CONFIG } from "../config/index.ts";
import type { CaptureStream } from "../browser/tabCapture.ts";
import type { CreatePageWithCaptureDeps } from "./setup.ts";
import type { ProbeCacheIdentity } from "../native/probe.ts";
import { Readable } from "node:stream";
import assert from "node:assert/strict";
import { getProviderBySlug } from "../browser/channelSelection.ts";
import { initializeDataDir } from "../config/paths.ts";
import { makeProfile } from "../config/profiles.helpers.ts";
import { mkdtemp } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

// The HBO Max guide URL and the channel the persisted lineup names. HBO is incidental: it is simply a registered provider whose strategy publishes both a
// resolveDirectUrl hook and an invalidateDirectUrl hook, so the coordinator's whole decision path is live rather than partially stubbed.
const GUIDE_URL = "https://play.hbomax.com/channels";
const PERSISTED_WATCH_URL = "https://play.hbomax.com/channel/watch/persisted-hint";

// The failure each stub navigation raises, and the ordered list of URLs the stub page was asked to navigate to. The URL list is what makes the fallback's second
// attempt an observed behavior rather than a spy count: the first attempt navigates to the hint, the second to the guide.
let gotoFailure: Error = new Error("navigation refused");
let gotoUrls: string[] = [];

// The probe-cache identity every case streams under. A stamp no classification was ever stored against means the cache lookup misses and the interception-skip
// decision falls to the option each case sets explicitly.
const PROBE_IDENTITY: ProbeCacheIdentity = { key: "direct-url-fallback-case", stamp: "direct-url-fallback-stamp" };

/**
 * Builds a stub page that records every navigation target and then rejects with the case's failure. The remaining members are the ones createPageWithCapture and
 * its disposer touch on a failing establishment.
 * @returns A stub page.
 */
function makeStubPage(): Page {

  return {

    close: async (): Promise<void> => { /* Nothing to close on a stub. */ },
    evaluate: async (): Promise<unknown> => undefined,
    evaluateOnNewDocument: async (): Promise<void> => { /* The injected video-selector helper needs no real document on a stub. */ },
    goto: async (url: string): Promise<void> => {

      gotoUrls.push(url);

      throw gotoFailure;
    },
    isClosed: (): boolean => false,
    setBypassCSP: async (): Promise<void> => { /* Nothing to bypass on a stub. */ },
    url: (): string => GUIDE_URL
  } as unknown as Page;
}

// The injected browser-boundary collaborators. Capture initialization has to succeed for the establishment to reach navigation at all, so the acquisition hands
// back a plain Readable carrying the two capture controls - the capture session only ever destroys it on the unwind.
const deps: CreatePageWithCaptureDeps = {

  acquireCaptureStream: async (): Promise<CaptureStream> => Object.assign(new Readable({ read: (): void => { /* Nothing is read from the stub capture. */ } }),
    { stop: async (): Promise<void> => undefined, stopped: Promise.resolve() }),
  emulateCaptureSurface: async (): Promise<{ height: number; width: number }> => ({ height: 1080, width: 1920 }),
  getCurrentBrowser: async (): Promise<Browser> => ({ newPage: async (): Promise<Page> => makeStubPage() } as unknown as Browser),
  installActivationHeal: async (): Promise<void> => { /* The activation heal is not what this path measures. */ },
  openSharedWindowTab: async (): Promise<Page> => makeStubPage(),
  reaffirmCaptureSurface: async (): Promise<void> => { /* A failing establishment never reaches the re-affirmation. */ },
  spawnFFmpeg: (): never => { throw new Error("These rows run in native-fMP4 capture mode, where no FFmpeg child is spawned."); },
  startOverlayHandling: async (): Promise<void> => { /* No overlay poll matters on a failing establishment. */ },
  syncWindowVisibility: async (): Promise<void> => { /* Window presentation is not what this path measures. */ }
};

/**
 * Builds the establishment options for one case, naming the HBO strategy so the coordinator's direct-URL resolution and invalidation both dispatch for real.
 * @param overrides - Per-case option overrides, typically skipDirectUrl.
 * @returns The options for createPageWithCapture.
 */
function makeOptions(overrides: { skipDirectUrl?: boolean } = {}): Parameters<typeof createPageWithCapture>[0] {

  const profile: ResolvedSiteProfile = makeProfile({ channelSelection: { strategy: "hboGrid" }, channelSelector: "HBO" });

  return { profile, skipManifestInterception: true, streamId: "direct-url-fallback", url: GUIDE_URL, ...overrides };
}

let originalCaptureMode: CaptureMode;
let originalNavigationRetries: number;

before(async () => {

  originalCaptureMode = CONFIG.streaming.captureMode;
  originalNavigationRetries = CONFIG.streaming.maxNavigationRetries;

  // Native capture keeps FFmpeg resolution out of the path ahead of navigation, and a single navigation attempt keeps the failure immediate rather than spending
  // the retry ladder's backoff sleeps on a stub that will never succeed.
  CONFIG.streaming.captureMode = "native";
  CONFIG.streaming.maxNavigationRetries = 1;

  // Point the persisted lineup store at a temp directory so seeding a hint writes nothing into a real data directory.
  initializeDataDir(await mkdtemp(path.join(os.tmpdir(), "prismcast-direct-url-")));
});

after(() => {

  CONFIG.streaming.captureMode = originalCaptureMode;
  CONFIG.streaming.maxNavigationRetries = originalNavigationRetries;
});

beforeEach(async () => {

  gotoFailure = new Error("navigation refused");
  gotoUrls = [];

  // Reseed the hint each case, because a case that reaches the coordinator's eviction removes it.
  await persistProviderLineup("hbomax", [{ channelSelector: "HBO", name: "HBO", watchUrl: PERSISTED_WATCH_URL }]);
});

describe("createPageWithCapture - direct watch URL failure classification", () => {

  test("navigates to the persisted hint and throws the typed error when the failure is evidence against it", async () => {

    /* The cold-boot path the whole feature turns on: no live provider cache, a hint from an earlier session, and a navigation that fails on the page it reached.
     * The coordinator judges that evidence against the URL, so the hint is evicted and the failure leaves this function typed for the caller to retry.
     */
    await assert.rejects(createPageWithCapture(makeOptions(), deps), (error: unknown) => error instanceof DirectUrlEstablishmentError,
      "a URL-evidence failure on a direct navigation is typed");

    assert.deepEqual(gotoUrls, [PERSISTED_WATCH_URL], "the establishment navigated to the persisted hint rather than the guide");
  });

  test("carries the original failure in both the cause and the message", async () => {

    /* The cause is what a caller that acts on the type would read. The message matters just as much, because the caller that does NOT act on the type - the tab
     * replacement handler - logs the message alone, and a wrapper that swallowed the reason would leave that path with less to go on than an untyped throw gave it.
     */
    const carriesOriginal = (error: unknown): boolean => (error instanceof DirectUrlEstablishmentError) &&
      ((error.cause as Error | undefined)?.message === "navigation refused") && error.message.includes("navigation refused");

    await assert.rejects(createPageWithCapture(makeOptions(), deps), carriesOriginal,
      "the original rejection travels with the typed error, in the cause and in the message");
  });

  test("rethrows raw when the page died under the establishment", async () => {

    /* The retention policy's other arm. A dead frame says nothing about whether the URL still resolves to the right channel, so the hint survives and there is
     * nothing to retry differently - the failure propagates exactly as it arrived.
     */
    gotoFailure = new Error("Attempted to use detached Frame '5D2393C3BF7A9BFEAB6C38D638EA01D8'");

    await assert.rejects(createPageWithCapture(makeOptions(), deps),
      (error: unknown) => !(error instanceof DirectUrlEstablishmentError) && (error as Error).message.includes("detached Frame"),
      "a page-death failure is not evidence against the URL, so it stays untyped");
  });

  test("skipDirectUrl never consults the resolver, navigates to the guide, and cannot produce the typed error", async (t) => {

    /* The branch behind the cannot-loop claim. With the resolution skipped, the strategy's own resolver is never called and no direct URL exists, so the catch's
     * typed arm is unreachable no matter how the establishment fails. The hint is deliberately left in place, which is what makes this assertion non-vacuous: a
     * regression that ignored the resolver's answer instead of skipping the resolution would still navigate to the hint and still type its failure.
     */
    const strategy = getProviderBySlug("hbomax")?.strategy;

    assert.ok(strategy?.resolveDirectUrl, "the HBO strategy publishes a resolveDirectUrl hook");

    const resolve = t.mock.method(strategy as Required<Pick<typeof strategy, "resolveDirectUrl">>, "resolveDirectUrl",
      async (): Promise<null> => Promise.resolve(null));

    await assert.rejects(createPageWithCapture(makeOptions({ skipDirectUrl: true }), deps),
      (error: unknown) => !(error instanceof DirectUrlEstablishmentError) && ((error as Error).message === "navigation refused"),
      "a skipped resolution cannot produce a direct-URL failure");

    assert.equal(resolve.mock.calls.length, 0, "the strategy's resolver is never consulted");
    assert.deepEqual(gotoUrls, [GUIDE_URL], "the establishment navigated to the guide even though a hint was available");
  });
});

describe("setupStream - the guide fallback", () => {

  test("retries once through the guide after a typed direct-URL failure", async () => {

    /* The whole point of typing the error: the first attempt burns the stale hint, and rather than handing the client a failed request, the tune gets the one
     * guide attempt it would have had if the hint had never existed. The navigation list is the assertion - two attempts, the hint then the guide - so removing the
     * fallback leaves a single entry.
     */
    await assert.rejects(setupStream({ channelSelector: "HBO", probeIdentity: PROBE_IDENTITY, url: GUIDE_URL }, (): void => { /* No circuit break here. */ },
      deps), "the fallback's own failure surfaces to the caller");

    assert.deepEqual(gotoUrls, [ PERSISTED_WATCH_URL, GUIDE_URL ], "exactly two establishments ran: the hint, then the guide");
  });

  test("does not retry when the failure was not evidence against the URL", async () => {

    // A page-death failure rethrows raw from the establishment, so the fallback's instanceof gate declines it and the tune fails on its single attempt - the same
    // outcome it had before a fallback existed to decline.
    gotoFailure = new Error("Attempted to use detached Frame '5D2393C3BF7A9BFEAB6C38D638EA01D8'");

    await assert.rejects(setupStream({ channelSelector: "HBO", probeIdentity: PROBE_IDENTITY, url: GUIDE_URL }, (): void => { /* No circuit break here. */ },
      deps), "the untyped failure surfaces to the caller");

    assert.deepEqual(gotoUrls, [PERSISTED_WATCH_URL], "only the first establishment ran");
  });

  test("runs a single establishment when no direct URL was in play at all", async () => {

    // The ordinary guide tune. With no hint to resolve, usedDirectUrl is false, the catch's typed arm is never entered, and the fallback has nothing to decline.
    evictPersistedWatchUrl("hbomax", "HBO");

    await assert.rejects(setupStream({ channelSelector: "HBO", probeIdentity: PROBE_IDENTITY, url: GUIDE_URL }, (): void => { /* No circuit break here. */ },
      deps), "the guide failure surfaces to the caller");

    assert.deepEqual(gotoUrls, [GUIDE_URL], "a tune with no hint makes exactly one attempt");
  });
});
