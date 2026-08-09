/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * proxy.map.test.ts: Unit tests for the native proxy's fMP4/CMAF relay - #EXT-X-MAP parsing, initialization-segment fetching and identity, container-true segment
 * naming, and MAP emission in the playlists the proxy publishes. The factory's deterministic getter surface and the extracted pure helpers live in proxy.test.ts;
 * this file drives the polling loop itself.
 *
 * The loop is drivable here because two ports make it deterministic: NativeProxyOptions.clock virtualizes the polling cadence, and globalThis.fetch is replaced
 * with a URL-routing stub that records every request. Pacing is the one subtlety - a clock whose sleep resolves immediately turns the poll loop into an unbroken
 * microtask chain that starves the macrotask queue, so nothing outside it can ever run. The harness therefore has the ROUTER terminate the loop: the manifest
 * route counts cycles and calls stop() at the target, which is the same technique the consent poll-loop tests use to end a fake-clock loop at tick N.
 *
 * Assertions are made on observable output only - playlists read back from the registry, segment and initialization names and bytes read through the hlsSegments
 * getters, and request counts read from the router. No internal proxy state is inspected.
 */
import { afterEach, describe, test } from "node:test";
import { getNamedInitSegment, getSegment } from "./../streaming/hlsSegments.ts";
import { getStream, registerStream, unregisterStream } from "./../streaming/registry.ts";
import type { Clock } from "../utils/index.ts";
import type { NativeProxyOptions } from "./proxy.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { createNativeProxy } from "./proxy.ts";
import { makeRegistryEntry } from "./../streaming/registry.helpers.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

// The URL the harness always polls for the video variant, and the audio variant for the split-track cases.
const VARIANT_URL = "https://cdn.test/variant.m3u8";
const AUDIO_VARIANT_URL = "https://cdn.test/audio.m3u8";

/* A recording fetch stub. Routes are matched by longest URL prefix so a test can register both a manifest and the segments beneath it; anything unmatched
 * answers 404 so an unintended request surfaces as a missing segment rather than a silent pass. Every request URL is appended to calls, which is what the
 * fetch-count assertions read.
 *
 * Three observations beyond the call list make concurrency and cancellation testable at the network layer rather than by inference. The stub honors the
 * caller's abort signal the way a real fetch does, rejecting an open request and recording which URL was cancelled. It records the PEAK number of requests
 * open at once - a maximum across the whole run, not a gauge, so a window that fills and drains between two reads is still observable afterwards. And
 * requested() hands out a promise that resolves when a matching URL reaches the router, which is what lets a held route release on a later request's arrival
 * instead of on a timer.
 */
interface FetchRouter {

  aborted: string[];
  calls: string[];
  countMatching: (fragment: string) => number;
  peakInFlight: () => number;
  requested: (fragment: string) => Promise<void>;
}

/* A route, optionally marked as one that ignores the caller's cancellation. Rejecting on abort is the default and the contract every other case consumes; a
 * marked route answers whatever it was told to answer even after the caller aborted, which is how a request that completes on the wire in the moment between
 * the abort and the socket closing is reproduced.
 */
type RouteHandler = ((url: string) => Response | Promise<Response>) & { ignoresAbort?: boolean };

function installFetchRouter(routes: Record<string, RouteHandler>): FetchRouter {

  const aborted: string[] = [];
  const calls: string[] = [];
  const waiting: { fragment: string; resolve: () => void }[] = [];

  let openRequests = 0;
  let peak = 0;

  globalThis.fetch = (async (url: string | URL, init?: RequestInit): Promise<Response> => {

    const urlStr = url.toString();

    calls.push(urlStr);

    // Release anything waiting on this URL's arrival. The scan runs backwards because a resolved waiter is spliced out as it fires.
    for(let i = waiting.length - 1; i >= 0; i--) {

      const waiter = waiting[i];

      if(waiter && urlStr.includes(waiter.fragment)) {

        waiting.splice(i, 1);
        waiter.resolve();
      }
    }

    let matched: RouteHandler | null = null;
    let matchedLength = -1;

    for(const prefix of Object.keys(routes)) {

      if(urlStr.startsWith(prefix) && (prefix.length > matchedLength)) {

        matched = routes[prefix] ?? null;
        matchedLength = prefix.length;
      }
    }

    if(!matched) {

      return new Response("not found", { status: 404 });
    }

    const respond = matched;

    openRequests++;
    peak = Math.max(peak, openRequests);

    const { promise: cancelled, reject: cancel } = Promise.withResolvers<never>();
    const signal = respond.ignoresAbort ? null : (init?.signal ?? null);

    const onAbort = (): void => {

      aborted.push(urlStr);
      cancel(new DOMException("The operation was aborted.", "AbortError"));
    };

    // A signal that is already aborted never dispatches its event, so the two cases are handled separately - exactly as a real fetch treats them.
    if(signal?.aborted) {

      onAbort();
    } else {

      signal?.addEventListener("abort", onAbort, { once: true });
    }

    try {

      return await Promise.race([ respond(urlStr), cancelled ]);
    } finally {

      openRequests--;
      signal?.removeEventListener("abort", onAbort);
    }
  }) as typeof globalThis.fetch;

  return {

    aborted,
    calls,
    countMatching: (fragment: string): number => calls.filter((url) => url.includes(fragment)).length,
    peakInFlight: (): number => peak,
    requested: async (fragment: string): Promise<void> => {

      if(calls.some((url) => url.includes(fragment))) {

        return;
      }

      // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
      const { promise, resolve } = Promise.withResolvers<void>();

      waiting.push({ fragment, resolve });

      return promise;
    }
  };
}

/* A clock whose sleeps resolve immediately, so poll cycles advance as fast as the microtask queue allows rather than on real timers.
 */
function makeImmediateClock(): Clock {

  return {

    now: (): number => 0,
    sleep: async (): Promise<void> => { /* Resolve immediately so the cadence never waits on a real timer. */ },
    waitWithTimeout: async <T>(promise: Promise<T>): Promise<T> => promise
  };
}

/* A harness holding one registered stream and the proxy relaying into it. runCycles starts the proxy and resolves once the router has terminated the loop at the
 * requested cycle count, then drains the in-flight cycle's remaining continuations so the final playlist write has landed before anything is read.
 */
interface Harness {

  audioPlaylist: () => string;
  initNames: () => { audio: string | null; video: string | null };
  playlist: () => string;
  proxy: ReturnType<typeof createNativeProxy>;
  segmentNames: () => string[];
  streamId: number;
}

function makeHarness(overrides: Partial<NativeProxyOptions> = {}): Harness {

  const entry = makeRegistryEntry();

  registerStream(entry);

  const proxy = createNativeProxy({

    audioVariantUrl: null,
    channelName: "map-test-channel",
    clock: makeImmediateClock(),

    // The container the proxy reports as its pipeline shape. Nothing in this file reads it - the relay follows whatever bodies the router serves - so the harness
    // names the fMP4 sources these initialization tests are about.
    container: "fmp4",
    encryption: "clear",
    keyUrl: null,
    onError: (): void => { /* Overridden by tests that observe escalation. */ },
    prefetchedKey: null,
    streamId: entry.id,
    streamIdStr: "map-test-stream",
    variantUrl: VARIANT_URL,
    ...overrides
  });

  return {

    audioPlaylist: (): string => getStream(entry.id)?.hls.audioPlaylist ?? "",
    initNames: (): { audio: string | null; video: string | null } => ({

      audio: getStream(entry.id)?.hls.currentInitNames.audio ?? null,
      video: getStream(entry.id)?.hls.currentInitNames.video ?? null
    }),
    playlist: (): string => getStream(entry.id)?.hls.playlist ?? "",
    proxy,
    segmentNames: (): string[] => Array.from(getStream(entry.id)?.hls.segments.keys() ?? []),
    streamId: entry.id
  };
}

/* Drains the microtask queue enough times for an in-flight poll cycle's remaining awaits to settle after the loop has been stopped.
 */
async function drain(): Promise<void> {

  for(let i = 0; i < 50; i++) {

    // The awaits are deliberately sequential - each one yields exactly one microtask checkpoint, which is the unit being counted out here.
    // eslint-disable-next-line no-await-in-loop
    await Promise.resolve();
  }
}

/* Builds a manifest route that terminates the loop at the target cycle. The counter is returned so a test can assert how many poll cycles actually ran.
 */
function makePacedManifestRoute(bodyForCycle: (cycle: number) => string, targetCycles: number, stop: () => void,
  onSettled: () => void): { handler: RouteHandler; cycles: () => number } {

  let cycle = 0;

  return {

    cycles: (): number => cycle,
    handler: (): Response => {

      cycle++;

      const body = bodyForCycle(cycle);

      if(cycle >= targetCycles) {

        stop();
        onSettled();
      }

      return new Response(body, { status: 200 });
    }
  };
}

/* Assembles a media playlist body from its lines, always terminating with a newline the way a served manifest does.
 */
function manifest(lines: string[]): string {

  return lines.join("\n") + "\n";
}

// A minimal fMP4 window: one MAP, three fragments.
function fmp4Manifest(mapUri: string, sequence: number, names: string[]): string {

  const lines = [ "#EXTM3U", "#EXT-X-VERSION:7", "#EXT-X-TARGETDURATION:6", "#EXT-X-MEDIA-SEQUENCE:" + String(sequence), "#EXT-X-MAP:URI=\"" + mapUri + "\"" ];

  for(const name of names) {

    lines.push("#EXTINF:6.000,", name);
  }

  return manifest(lines);
}

/* Runs a proxy against a paced manifest route until the loop self-terminates, then drains. Returns the harness and the router so the test can read observable
 * output and request counts.
 *
 * The run settles on either of the two ways the loop can end: the paced route reaching its target cycle, or the proxy stopping itself when a failure crosses the
 * escalation threshold. Settling on the error path matters for the failure cases - an escalation ends the loop mid-cycle, so the target cycle never arrives, and
 * without this the run would hang until the runner's timeout killed it. A test would then "fail" on a timeout rather than on the assertion it was written to
 * make, which reports nothing about the behavior under test.
 */
async function runRelay(options: { audioRoutes?: Record<string, RouteHandler>; bodyForCycle: (cycle: number) => string; proxyOverrides?: Partial<NativeProxyOptions>;
  routes: Record<string, RouteHandler>; targetCycles: number; }): Promise<{ cycles: number; harness: Harness; router: FetchRouter }> {

  // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
  const { promise: settled, resolve: markSettled } = Promise.withResolvers<void>();

  let stopProxy: () => void = (): void => { /* Assigned once the proxy exists. */ };

  const paced = makePacedManifestRoute(options.bodyForCycle, options.targetCycles, () => stopProxy(), markSettled);

  const router = installFetchRouter({ [VARIANT_URL]: paced.handler, ...options.audioRoutes, ...options.routes });

  // The caller's own error observer still runs first; settling afterwards is additive, and resolving an already-resolved promise is a no-op when both endings
  // happen to coincide.
  const callerOnError = options.proxyOverrides?.onError;

  const harness = makeHarness({

    ...options.proxyOverrides,
    onError: (error: string): void => {

      callerOnError?.(error);
      markSettled();
    }
  });

  stopProxy = harness.proxy.stop;

  harness.proxy.start();

  await settled;
  await drain();

  return { cycles: paced.cycles(), harness, router };
}

// Serves an initialization segment body under a URL, as the CDN would.
function initRoute(body: string): RouteHandler {

  return (): Response => new Response(Buffer.from(body), { status: 200 });
}

// Serves any fragment request with a body derived from its URL, so stored bytes are distinguishable per segment.
const fragmentRoute: RouteHandler = (url: string): Response => new Response(Buffer.from("fragment:" + url), { status: 200 });

/* Builds a route that holds every response it serves open until it is released, so a case can observe what the relay does while a fetch is still in flight.
 * The route also publishes the moment it was requested, which is what a case waits on before making its mid-flight assertions - and what another held route
 * can release on, so every hold ends on a routed event rather than on a timer.
 */
interface HeldRoute {

  handler: RouteHandler;
  release: () => void;
  requested: Promise<void>;
}

function makeHeldRoute(respond: RouteHandler = fragmentRoute, options: { ignoresAbort?: boolean } = {}): HeldRoute {

  // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
  const { promise: requested, resolve: markRequested } = Promise.withResolvers<void>();
  // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
  const { promise: released, resolve: open } = Promise.withResolvers<void>();

  const handler: RouteHandler = async (url: string): Promise<Response> => {

    markRequested();

    await released;

    return respond(url);
  };

  // A hold that ignores cancellation delivers its response even when the release lands after the caller aborted, which is the only way to reproduce a fetch
  // that succeeds at or after the stop rather than one that fails because of it.
  handler.ignoresAbort = options.ignoresAbort;

  return { handler, release: (): void => open(), requested };
}

/* A plain MPEG-TS window with no MAP tag, so every entry in it is fetchable the moment the batch opens - which is what the pipeline cases want when the
 * observation is about the fetch window rather than about initialization handling.
 */
function tsManifest(sequence: number, names: string[]): string {

  const lines = [ "#EXTM3U", "#EXT-X-VERSION:3", "#EXT-X-TARGETDURATION:6", "#EXT-X-MEDIA-SEQUENCE:" + String(sequence) ];

  for(const name of names) {

    lines.push("#EXTINF:6.000,", name);
  }

  return manifest(lines);
}

describe("fMP4 relay: MAP parsing, naming, and playlist emission", () => {

  afterEach(() => {

    unregisterStream(0);
  });

  test("names fragments .m4s, references the init as initialMapUri, and emits version 7 (T2/T6)", async () => {

    /* The end-to-end shape for an fMP4 source. Every stored fragment carries the .m4s extension, the window opens with an EXT-X-MAP pointing at the name the
     * relay minted for the upstream initialization, and the playlist declares version 7 because EXT-X-MAP requires it.
     */
    const { harness, router } = await runRelay({

      bodyForCycle: () => fmp4Manifest("https://cdn.test/init.cmfv", 100, [ "s100.cmfv", "s101.cmfv", "s102.cmfv" ]),
      routes: { "https://cdn.test/init.cmfv": initRoute("VIDEO-INIT-A"), "https://cdn.test/s1": fragmentRoute },
      targetCycles: 2
    });

    assert.deepEqual(harness.segmentNames(), [ "segment0.m4s", "segment1.m4s", "segment2.m4s" ], "fMP4 fragments take the .m4s extension");

    const playlist = harness.playlist();

    assert.match(playlist, /#EXT-X-VERSION:7/, "an fMP4 window declares version 7");
    assert.match(playlist, /#EXT-X-MAP:URI="init-v0\.mp4"/, "the window opens with the relay's init reference");
    assert.equal(harness.initNames().video, "init-v0.mp4", "the video track records its current init name");
    assert.deepEqual(getNamedInitSegment(harness.streamId, "init-v0.mp4"), Buffer.from("VIDEO-INIT-A"), "the upstream init bytes are stored under that name");

    // One MAP applies to every following fragment, so only the window-level reference is emitted - no per-entry MAP on the fragments that inherit it.
    assert.equal((/#EXT-X-MAP/g.exec(playlist) === null) ? 0 : playlist.match(/#EXT-X-MAP/g)?.length, 1, "a single sticky MAP emits exactly one reference");

    assert.equal(router.countMatching("init.cmfv"), 1, "the init is fetched once across both cycles");
  });

  test("derives MEDIA-SEQUENCE from an .m4s filename rather than yielding NaN (T9)", async () => {

    /* The regression this pins: deriving the index by stripping a hardcoded ".ts" leaves "0.m4s", and Number() of that is NaN, which serializes into the
     * playlist as "#EXT-X-MEDIA-SEQUENCE:NaN". The assertion is on the exact integer - a typeof-number or truthiness check would pass against NaN, which is
     * precisely the failure mode.
     */
    const { harness } = await runRelay({

      bodyForCycle: () => fmp4Manifest("https://cdn.test/init.cmfv", 100, [ "s100.cmfv", "s101.cmfv" ]),
      routes: { "https://cdn.test/init.cmfv": initRoute("VIDEO-INIT-A"), "https://cdn.test/s1": fragmentRoute },
      targetCycles: 2
    });

    assert.match(harness.playlist(), /#EXT-X-MEDIA-SEQUENCE:0\n/, "the first window's media sequence is exactly 0");
    assert.doesNotMatch(harness.playlist(), /NaN/, "no NaN reaches the playlist");
  });

  test("fetches the init once per URI across multiple cycles and multiple fragments (T3)", async () => {

    /* Two poll cycles, each carrying fragments under the same MAP. A single-cycle single-fragment reading would pass an implementation that refetches on every
     * poll or every segment, so the count is taken across both cycles with several fragments in each.
     */
    const { harness, router, cycles } = await runRelay({

      bodyForCycle: (cycle) => (cycle === 1) ? fmp4Manifest("https://cdn.test/init.cmfv", 100, [ "s100.cmfv", "s101.cmfv" ]) :
        fmp4Manifest("https://cdn.test/init.cmfv", 102, [ "s102.cmfv", "s103.cmfv" ]),
      routes: { "https://cdn.test/init.cmfv": initRoute("VIDEO-INIT-A"), "https://cdn.test/s1": fragmentRoute },
      targetCycles: 3
    });

    assert.equal(cycles, 3, "the loop ran the expected number of cycles");
    assert.equal(router.countMatching("init.cmfv"), 1, "the unchanged MAP URI is fetched exactly once");
    assert.ok(harness.segmentNames().length >= 4, "fragments from both cycles were stored");
  });

  test("leaves an established init untouched when a poll omits the MAP tag entirely (T22)", async () => {

    /* A document with no MAP does not mean "no initialization" - per the spec the current one stays in force. The relay must therefore make no state change at
     * all on omission: no refetch, no clear, and no flip back to .ts naming.
     */
    const { harness, router } = await runRelay({

      bodyForCycle: (cycle) => (cycle === 1) ? fmp4Manifest("https://cdn.test/init.cmfv", 100, ["s100.cmfv"]) :
        manifest([ "#EXTM3U", "#EXT-X-VERSION:7", "#EXT-X-TARGETDURATION:6", "#EXT-X-MEDIA-SEQUENCE:101", "#EXTINF:6.000,", "s101.cmfv" ]),
      routes: { "https://cdn.test/init.cmfv": initRoute("VIDEO-INIT-A"), "https://cdn.test/s1": fragmentRoute },
      targetCycles: 3
    });

    assert.equal(router.countMatching("init.cmfv"), 1, "the omitted MAP triggers no refetch");
    assert.equal(harness.initNames().video, "init-v0.mp4", "the current init name is unchanged");
    assert.ok(harness.segmentNames().every((name) => name.endsWith(".m4s")), "naming stays fMP4 across the MAP-less poll");
    assert.deepEqual(getNamedInitSegment(harness.streamId, "init-v0.mp4"), Buffer.from("VIDEO-INIT-A"), "the stored init survives");
  });
});

describe("fMP4 relay: init identity and transitions", () => {

  afterEach(() => {

    unregisterStream(0);
  });

  test("mints a new name and emits a transition when the MAP changes to new bytes (T10)", async () => {

    /* A mid-stream initialization change. The new bytes take the next name, the first fragment that depends on them carries a per-entry MAP marking the
     * transition point, and the fragments before it carry none - they inherit the window-level reference.
     */
    const { harness } = await runRelay({

      bodyForCycle: (cycle) => (cycle === 1) ? fmp4Manifest("https://cdn.test/initA.cmfv", 100, [ "s100.cmfv", "s101.cmfv" ]) :
        fmp4Manifest("https://cdn.test/initB.cmfv", 102, [ "s102.cmfv", "s103.cmfv" ]),
      routes: {

        "https://cdn.test/initA.cmfv": initRoute("VIDEO-INIT-A"),
        "https://cdn.test/initB.cmfv": initRoute("VIDEO-INIT-B-DIFFERENT"),
        "https://cdn.test/s1": fragmentRoute
      },
      targetCycles: 3
    });

    assert.equal(harness.initNames().video, "init-v1.mp4", "the changed initialization takes the next name");
    assert.deepEqual(getNamedInitSegment(harness.streamId, "init-v1.mp4"), Buffer.from("VIDEO-INIT-B-DIFFERENT"), "the new bytes are stored");
    assert.deepEqual(getNamedInitSegment(harness.streamId, "init-v0.mp4"), Buffer.from("VIDEO-INIT-A"), "the outgoing init is retained while still referenced");

    const playlist = harness.playlist();
    const lines = playlist.split("\n");
    const transitionIndex = lines.findIndex((line) => line === "#EXT-X-MAP:URI=\"init-v1.mp4\"");

    assert.ok(transitionIndex > 0, "the transition emits a per-entry MAP inside the window");
    assert.match(playlist, /#EXT-X-MAP:URI="init-v0\.mp4"/, "the window still opens with the original init");

    // The transition marker must sit immediately before the first fragment that needs it, not be stamped on every entry.
    assert.equal(playlist.match(/#EXT-X-MAP/g)?.length, 2, "exactly two MAP references - the window opener and the one transition");
  });

  test("reuses the served name when a rotated MAP URL returns identical bytes (T16)", async () => {

    /* Token rotation: the MAP URI changes but the bytes behind it do not. Content identity must recognize that and keep serving the existing name, so no
     * transition is emitted and no second initialization accumulates. The fetch count is asserted as exactly one extra - "at most one" would also be satisfied
     * by an implementation that never fetches and therefore never verifies identity at all.
     */
    const { harness, router } = await runRelay({

      bodyForCycle: (cycle) => (cycle === 1) ? fmp4Manifest("https://cdn.test/init.cmfv?token=one", 100, ["s100.cmfv"]) :
        fmp4Manifest("https://cdn.test/init.cmfv?token=two", 101, ["s101.cmfv"]),
      routes: { "https://cdn.test/init.cmfv": initRoute("VIDEO-INIT-STABLE"), "https://cdn.test/s1": fragmentRoute },
      targetCycles: 3
    });

    assert.equal(router.countMatching("token=one"), 1, "the first token is fetched once");
    assert.equal(router.countMatching("token=two"), 1, "the rotated token is fetched once so its bytes can be compared");
    assert.equal(harness.initNames().video, "init-v0.mp4", "the rotation keeps the existing served name");
    assert.equal(getNamedInitSegment(harness.streamId, "init-v1.mp4"), undefined, "no second initialization is minted for identical bytes");

    const state = getStream(harness.streamId);

    assert.equal(state?.hls.initSegmentBytes, Buffer.from("VIDEO-INIT-STABLE").length, "the byte counter does not drift on reuse");
    assert.equal(harness.playlist().match(/#EXT-X-MAP/g)?.length, 1, "identical bytes emit no transition");
  });

  test("reuses an earlier init when an A/B/A alternation returns to bytes still stored (T20)", async () => {

    /* The ad-pod pattern. Returning to the first initialization must reuse its existing name rather than mint a third, because clients may still hold it - and
     * the byte counter must reflect two stored initializations throughout, neither double-counting the reuse nor drifting.
     */
    const bodies = [

      fmp4Manifest("https://cdn.test/initA.cmfv", 100, ["s100.cmfv"]),
      fmp4Manifest("https://cdn.test/initB.cmfv", 101, ["s101.cmfv"]),
      fmp4Manifest("https://cdn.test/initA2.cmfv", 102, ["s102.cmfv"])
    ];

    const { harness } = await runRelay({

      bodyForCycle: (cycle) => bodies[Math.min(cycle, bodies.length) - 1] ?? "",
      routes: {

        "https://cdn.test/initA.cmfv": initRoute("VIDEO-INIT-A"),
        "https://cdn.test/initA2.cmfv": initRoute("VIDEO-INIT-A"),
        "https://cdn.test/initB.cmfv": initRoute("VIDEO-INIT-B"),
        "https://cdn.test/s1": fragmentRoute
      },
      targetCycles: 4
    });

    assert.equal(harness.initNames().video, "init-v0.mp4", "the third transition returns to the first init's name");
    assert.equal(getNamedInitSegment(harness.streamId, "init-v2.mp4"), undefined, "no third initialization is minted");
    assert.deepEqual(getNamedInitSegment(harness.streamId, "init-v0.mp4"), Buffer.from("VIDEO-INIT-A"), "the first init is still stored");
    assert.deepEqual(getNamedInitSegment(harness.streamId, "init-v1.mp4"), Buffer.from("VIDEO-INIT-B"), "the second init is still stored");

    const state = getStream(harness.streamId);
    const expectedBytes = Buffer.from("VIDEO-INIT-A").length + Buffer.from("VIDEO-INIT-B").length;

    assert.equal(state?.hls.initSegmentBytes, expectedBytes, "the counter equals exactly the two stored initializations");
  });

  test("re-emits the back-to-A transition tag in the rendered playlist when the window holds the whole alternation", async () => {

    /* The annotation half of the A/B/A case. Reusing the first initialization's NAME is not enough on its own: the fragment that returns to it must also carry
     * its own #EXT-X-MAP in the emitted playlist, because the preceding fragment moved the client onto the second initialization. Without that tag a client
     * decodes the third fragment against the wrong codec configuration.
     *
     * The whole alternation sits in one window here (three fragments, well inside the default maxSegments), which is what makes the third entry's tag
     * observable. A transition rule that skipped re-marking an initialization already named at the window level - a plausible-looking optimization, since the
     * window opens on that same initialization - would silently drop exactly this tag while every store-level assertion above stayed green.
     */
    const bodies = [

      fmp4Manifest("https://cdn.test/initA.cmfv", 100, ["s100.cmfv"]),
      fmp4Manifest("https://cdn.test/initB.cmfv", 101, ["s101.cmfv"]),
      fmp4Manifest("https://cdn.test/initA2.cmfv", 102, ["s102.cmfv"])
    ];

    const { harness } = await runRelay({

      bodyForCycle: (cycle) => bodies[Math.min(cycle, bodies.length) - 1] ?? "",
      routes: {

        "https://cdn.test/initA.cmfv": initRoute("VIDEO-INIT-A"),
        "https://cdn.test/initA2.cmfv": initRoute("VIDEO-INIT-A"),
        "https://cdn.test/initB.cmfv": initRoute("VIDEO-INIT-B"),
        "https://cdn.test/s1": fragmentRoute
      },
      targetCycles: 4
    });

    assert.deepEqual(harness.segmentNames(), [ "segment0.m4s", "segment1.m4s", "segment2.m4s" ], "the whole alternation is in one window");

    const lines = harness.playlist().split("\n");

    // The tag governing a fragment is the closest MAP reference above it, so each fragment's tag is read by walking back from its own line.
    const initGoverning = (segment: string): string | null => {

      const segmentIndex = lines.indexOf(segment);

      for(let i = segmentIndex - 1; i >= 0; i--) {

        const match = /^#EXT-X-MAP:URI="([^"]+)"$/.exec(lines[i] ?? "");

        if(match?.[1] !== undefined) {

          return match[1];
        }
      }

      return null;
    };

    assert.equal(initGoverning("segment0.m4s"), "init-v0.mp4", "the first fragment is governed by the window-level reference");
    assert.equal(initGoverning("segment1.m4s"), "init-v1.mp4", "the second fragment moves the client onto the second initialization");
    assert.equal(initGoverning("segment2.m4s"), "init-v0.mp4", "the third fragment carries the back-to-A transition rather than inheriting the second");

    // Three references exactly: the window opener plus one per transition. A stamp-every-entry implementation would emit four.
    assert.equal(harness.playlist().match(/#EXT-X-MAP/g)?.length, 3, "the window opener plus exactly two transitions");
  });
});

describe("fMP4 relay: init fetch failure handling", () => {

  afterEach(() => {

    unregisterStream(0);
  });

  test("attempts the init once per cycle, skips dependent fragments, and retries on the next poll (T4)", async () => {

    /* A failing initialization must not produce fragments that reference it - a stored fragment with an unresolved initialization would be an orphan reference
     * in the playlist. With several dependent fragments in the failing window, exactly one attempt is made for that cycle (the per-cycle pacing local), every
     * dependent fragment is skipped, and the next cycle retries because the upstream URI was deliberately not recorded as adopted.
     */
    let cycle = 0;

    const { harness, router } = await runRelay({

      // Each cycle carries three FRESH dependent fragments under one unchanged MAP, so a per-segment implementation is distinguishable from a per-cycle one by
      // the attempt count alone.
      bodyForCycle: (current) => {

        cycle = current;

        const base = 100 + ((current - 1) * 3);

        return fmp4Manifest("https://cdn.test/init.cmfv", base,
          [ "s" + String(base) + ".cmfv", "s" + String(base + 1) + ".cmfv", "s" + String(base + 2) + ".cmfv" ]);
      },
      routes: {

        // Failure is keyed on the CYCLE, not on the attempt count: every attempt within a failing cycle fails identically. Keying on attempts would let a
        // per-segment implementation stumble into the same total the per-cycle one produces, masking the very difference this test exists to catch.
        "https://cdn.test/init.cmfv": (): Response => ((cycle <= 2) ? new Response("unavailable", { status: 503 }) :
          new Response(Buffer.from("VIDEO-INIT-LATE"), { status: 200 })),
        "https://cdn.test/s1": fragmentRoute
      },

      // The paced route stops the loop on the cycle it targets, so that cycle performs no segment work: four targets give three working cycles - two failing,
      // then the recovery.
      targetCycles: 4
    });

    assert.equal(router.countMatching("init.cmfv"), 3, "exactly one init attempt per cycle, not one per dependent fragment");
    assert.equal(harness.proxy.hasErrored(), false, "three cycles of one attempt each stay under the escalation threshold");
    assert.equal(harness.initNames().video, "init-v0.mp4", "the recovered init is adopted");
    assert.deepEqual(getNamedInitSegment(harness.streamId, "init-v0.mp4"), Buffer.from("VIDEO-INIT-LATE"), "the recovered bytes are stored");

    /* Only the recovered cycle's fragments may be stored. The six fragments from the two failing cycles were skipped because their initialization was never
     * resolved - storing them would have published playlist entries referencing an initialization the relay cannot serve.
     */
    assert.deepEqual(harness.segmentNames(), [ "segment0.m4s", "segment1.m4s", "segment2.m4s" ], "only the recovered cycle's fragments were stored");
  });

  test("escalates through the shared tracker threshold, and not before it (T4 threshold polarity)", async () => {

    /* The init fetch rides the track's existing segment tracker, so its failures escalate through the one threshold the file already has. Both polarities are
     * asserted from a single run: the error callback fires, and the count of attempts at the point of escalation shows the threshold was neither early nor
     * missing. The loop needs no external pacing because crossing the threshold sets the stopped flag itself.
     */
    let escalated = false;
    let attemptsAtEscalation = 0;
    let initAttempts = 0;

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise: settled, resolve: markSettled } = Promise.withResolvers<void>();

    const router = installFetchRouter({

      [VARIANT_URL]: (): Response => new Response(fmp4Manifest("https://cdn.test/init.cmfv", 100, [ "s100.cmfv", "s101.cmfv" ]), { status: 200 }),
      "https://cdn.test/init.cmfv": (): Response => {

        initAttempts++;

        return new Response("unavailable", { status: 503 });
      }
    });

    const harness = makeHarness({

      onError: (): void => {

        escalated = true;
        attemptsAtEscalation = initAttempts;
        markSettled();
      }
    });

    harness.proxy.start();

    await settled;
    await drain();

    assert.equal(escalated, true, "a persistently failing init escalates through the tracker");
    assert.equal(harness.proxy.hasErrored(), true, "the proxy reports the error threshold was reached");
    assert.equal(attemptsAtEscalation, 5, "escalation lands on the fifth consecutive failure, matching MAX_SEGMENT_FAILURES");
    assert.ok(router.countMatching("init.cmfv") >= 5, "the retries actually reached the network");

    harness.proxy.stop();
    unregisterStream(harness.streamId);
  });

  test("does not escalate while other fetches keep succeeding, and keeps retrying the init (T4 mixed window)", async () => {

    /* The mixed window: fragments under the already-adopted initialization keep succeeding while a NEW initialization keeps failing. Their successes reset the
     * shared tracker, so no escalation occurs - correct, because content is still being delivered. The second assertion is what gives this test its teeth: a
     * bare no-escalation check is satisfied identically by an implementation that swallows the failure and stops retrying, so the growing per-cycle attempt
     * count is asserted too.
     */
    let escalated = false;

    const { harness, router } = await runRelay({

      bodyForCycle: (cycle) => (cycle === 1) ? fmp4Manifest("https://cdn.test/initA.cmfv", 100, ["s100.cmfv"]) :
        manifest([

          "#EXTM3U", "#EXT-X-VERSION:7", "#EXT-X-TARGETDURATION:6", "#EXT-X-MEDIA-SEQUENCE:" + String(100 + cycle),
          "#EXTINF:6.000,", "s" + String(100 + cycle) + ".cmfv",
          "#EXT-X-MAP:URI=\"https://cdn.test/initB.cmfv\"",
          "#EXTINF:6.000,", "sB" + String(100 + cycle) + ".cmfv"
        ]),
      proxyOverrides: {

        onError: (): void => {

          escalated = true;
        }
      },
      routes: {

        "https://cdn.test/initA.cmfv": initRoute("VIDEO-INIT-A"),
        "https://cdn.test/initB.cmfv": (): Response => new Response("unavailable", { status: 503 }),
        "https://cdn.test/s": fragmentRoute
      },
      targetCycles: 8
    });

    assert.equal(escalated, false, "successful fragment fetches keep the tracker reset while content flows");
    assert.equal(harness.proxy.hasErrored(), false, "the stream is not torn down while it is still delivering");
    assert.ok(router.countMatching("initB.cmfv") >= 6, "the failing init keeps being retried every cycle rather than being swallowed");
    assert.equal(harness.initNames().video, "init-v0.mp4", "the working initialization remains adopted throughout");
  });
});

describe("fMP4 relay: composite playlists with preroll", () => {

  afterEach(() => {

    unregisterStream(0);
  });

  test("keeps preroll as the window init and redirects the relay's init onto the first real entry (T7)", async () => {

    /* With preroll occupying the head of the window, the window-level MAP must stay the preroll initialization; the relayed content's own initialization is
     * redirected onto the first real entry, where the preroll-to-real DISCONTINUITY has already invalidated the preroll MAP.
     *
     * The window deliberately holds at least two real entries: with only one, the "no other real entry carries a MAP" control is vacuously true and would pass
     * against an implementation that stamps every entry.
     */
    const entry = makeRegistryEntry();

    entry.hls.prerollBaseUrl = "http://host.test";
    entry.hls.prerollCodec = "h264";
    entry.hls.prerollSegmentCount = 3;
    entry.hls.prerollStartTime = new Date();

    registerStream(entry);

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise: settled, resolve: markSettled } = Promise.withResolvers<void>();

    let stopProxy: () => void = (): void => { /* Assigned once the proxy exists. */ };

    const paced = makePacedManifestRoute(() => fmp4Manifest("https://cdn.test/init.cmfv", 100, [ "s100.cmfv", "s101.cmfv" ]), 2, () => stopProxy(), markSettled);

    installFetchRouter({ [VARIANT_URL]: paced.handler, "https://cdn.test/init.cmfv": initRoute("VIDEO-INIT-A"), "https://cdn.test/s1": fragmentRoute });

    const proxy = createNativeProxy({

      audioVariantUrl: null,
      channelName: "composite-channel",
      clock: makeImmediateClock(),
      container: "fmp4",
      encryption: "clear",
      keyUrl: null,
      onError: (): void => { /* Unused. */ },
      prefetchedKey: null,
      prerollCodec: "h264",
      prerollSegmentCount: 3,
      streamId: entry.id,
      streamIdStr: "composite-stream",
      variantUrl: VARIANT_URL
    });

    stopProxy = proxy.stop;

    proxy.start();

    await settled;
    await drain();

    const playlist = getStream(entry.id)?.hls.playlist ?? "";

    assert.match(playlist, /#EXT-X-MAP:URI="http:\/\/host\.test\/preroll\/h264\/init\.mp4"/, "the window opens with the preroll init");
    assert.match(playlist, /#EXT-X-MAP:URI="init-v0\.mp4"/, "the relay's init is redirected into the window");
    assert.equal(playlist.match(/#EXT-X-MAP/g)?.length, 2, "exactly one preroll reference and one redirect - no per-entry stamping");

    // The redirect must land on the first real entry, which is the one carrying the preroll-to-real discontinuity.
    const lines = playlist.split("\n");
    const redirectIndex = lines.indexOf("#EXT-X-MAP:URI=\"init-v0.mp4\"");
    const firstRealIndex = lines.findIndex((line) => line.startsWith("segment"));

    assert.ok((redirectIndex > 0) && (redirectIndex < firstRealIndex), "the redirect precedes the first real fragment");

    proxy.stop();
    unregisterStream(entry.id);
  });

  test("adds no transition for a TS tail under an fMP4 preroll (T15)", async () => {

    /* Today's working case must stay untouched: an MPEG-TS relay under preroll has no initialization of its own, so the composite emits only the preroll
     * reference and no transition at all.
     */
    const entry = makeRegistryEntry();

    entry.hls.prerollBaseUrl = "http://host.test";
    entry.hls.prerollCodec = "h264";
    entry.hls.prerollSegmentCount = 3;
    entry.hls.prerollStartTime = new Date();

    registerStream(entry);

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise: settled, resolve: markSettled } = Promise.withResolvers<void>();

    let stopProxy: () => void = (): void => { /* Assigned once the proxy exists. */ };

    const tsBody = manifest([

      "#EXTM3U", "#EXT-X-VERSION:3", "#EXT-X-TARGETDURATION:6", "#EXT-X-MEDIA-SEQUENCE:100",
      "#EXTINF:6.000,", "s100.ts", "#EXTINF:6.000,", "s101.ts"
    ]);

    const paced = makePacedManifestRoute(() => tsBody, 2, () => stopProxy(), markSettled);

    installFetchRouter({ [VARIANT_URL]: paced.handler, "https://cdn.test/s1": fragmentRoute });

    const proxy = createNativeProxy({

      audioVariantUrl: null,
      channelName: "composite-ts-channel",
      clock: makeImmediateClock(),
      container: "ts",
      encryption: "clear",
      keyUrl: null,
      onError: (): void => { /* Unused. */ },
      prefetchedKey: null,
      prerollCodec: "h264",
      prerollSegmentCount: 3,
      streamId: entry.id,
      streamIdStr: "composite-ts-stream",
      variantUrl: VARIANT_URL
    });

    stopProxy = proxy.stop;

    proxy.start();

    await settled;
    await drain();

    const playlist = getStream(entry.id)?.hls.playlist ?? "";

    assert.equal(playlist.match(/#EXT-X-MAP/g)?.length, 1, "only the preroll reference is emitted for a TS tail");
    assert.match(playlist, /#EXT-X-MAP:URI="http:\/\/host\.test\/preroll\/h264\/init\.mp4"/, "and it is the preroll init");
    assert.ok(Array.from(getStream(entry.id)?.hls.segments.keys() ?? []).every((name) => name.endsWith(".ts")), "TS fragments keep the .ts extension");

    proxy.stop();
    unregisterStream(entry.id);
  });

  test("promotes the relay's init to the window reference once preroll leaves the window (T8)", async () => {

    /* When no preroll entry remains in the window, there is nothing for the relay's initialization to be redirected behind, so it becomes the window-level
     * reference directly. This is driven through the variant path, which is what generates once the composite's preroll window has emptied.
     */
    const { harness } = await runRelay({

      bodyForCycle: () => fmp4Manifest("https://cdn.test/init.cmfv", 100, [ "s100.cmfv", "s101.cmfv" ]),
      routes: { "https://cdn.test/init.cmfv": initRoute("VIDEO-INIT-A"), "https://cdn.test/s1": fragmentRoute },
      targetCycles: 2
    });

    const playlist = harness.playlist();

    assert.match(playlist, /#EXT-X-MAP:URI="init-v0\.mp4"/, "the relay's init is the window reference");
    assert.doesNotMatch(playlist, /preroll/, "no preroll reference remains");
  });
});

describe("fMP4 relay: pruning and lifecycle", () => {

  afterEach(() => {

    unregisterStream(0);
  });

  test("retains an outgoing init while its fragments are in the window and releases it after they rotate (T11/T12)", async () => {

    /* Reference-driven retention. The first initialization must survive as long as a fragment referencing it is still in the playlist window, then be released
     * once the window has rotated past it - and the metadata's own per-fragment record must be pruned alongside, which is observable as the outgoing MAP
     * reference disappearing from the emitted playlist.
     */
    const originalMax = (await import("../config/index.ts")).CONFIG.hls.maxSegments;
    const config = (await import("../config/index.ts")).CONFIG;

    config.hls.maxSegments = 2;

    try {

      const { harness } = await runRelay({

        bodyForCycle: (cycle) => (cycle === 1) ? fmp4Manifest("https://cdn.test/initA.cmfv", 100, ["s100.cmfv"]) :
          fmp4Manifest("https://cdn.test/initB.cmfv", 100 + cycle, ["s" + String(100 + cycle) + ".cmfv"]),
        routes: {

          "https://cdn.test/initA.cmfv": initRoute("VIDEO-INIT-A"),
          "https://cdn.test/initB.cmfv": initRoute("VIDEO-INIT-B-DIFFERENT"),
          "https://cdn.test/s1": fragmentRoute
        },
        targetCycles: 5
      });

      assert.equal(getNamedInitSegment(harness.streamId, "initA.cmfv"), undefined, "upstream URLs are never used as served names");
      assert.equal(getNamedInitSegment(harness.streamId, "init-v0.mp4"), undefined, "the outgoing init is released once nothing references it");
      assert.deepEqual(getNamedInitSegment(harness.streamId, "init-v1.mp4"), Buffer.from("VIDEO-INIT-B-DIFFERENT"), "the live init is retained");

      const state = getStream(harness.streamId);

      assert.equal(state?.hls.initSegmentBytes, Buffer.from("VIDEO-INIT-B-DIFFERENT").length, "the byte counter tracks exactly the live set");

      // The pruned fragment's metadata record went with it, so the playlist carries only the live initialization's reference.
      assert.doesNotMatch(harness.playlist(), /init-v0\.mp4/, "no reference to the released init remains in the playlist");
    } finally {

      config.hls.maxSegments = originalMax;
    }
  });

  test("discards an init that resolves after the proxy has been stopped (T23)", async () => {

    /* The post-stop straggler. The capture fallback stops the proxy and then clears the initialization store; a write landing after that clear would be a
     * permanent orphan, because initialization pruning only ever runs from this poll loop.
     *
     * The test first asserts the fetch was actually dispatched - without that, the no-write assertions would also pass against an implementation where the
     * init path never ran at all.
     */
    const entry = makeRegistryEntry();

    registerStream(entry);

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise: initRequested, resolve: markRequested } = Promise.withResolvers<void>();
    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise: releaseInit, resolve: releaseTheFetch } = Promise.withResolvers<void>();

    let initFetches = 0;

    installFetchRouter({

      [VARIANT_URL]: (): Response => new Response(fmp4Manifest("https://cdn.test/init.cmfv", 100, ["s100.cmfv"]), { status: 200 }),
      "https://cdn.test/init.cmfv": async (): Promise<Response> => {

        initFetches++;
        markRequested();

        // Hold the fetch open so stop() can land while it is still in flight.
        await releaseInit;

        return new Response(Buffer.from("VIDEO-INIT-STRAGGLER"), { status: 200 });
      },
      "https://cdn.test/s1": fragmentRoute
    });

    const proxy = createNativeProxy({

      audioVariantUrl: null,
      channelName: "straggler-channel",
      clock: makeImmediateClock(),
      container: "fmp4",
      encryption: "clear",
      keyUrl: null,
      onError: (): void => { /* Unused. */ },
      prefetchedKey: null,
      streamId: entry.id,
      streamIdStr: "straggler-stream",
      variantUrl: VARIANT_URL
    });

    proxy.start();

    await initRequested;

    assert.equal(initFetches, 1, "the init fetch was dispatched before the stop - the path under test actually ran");

    proxy.stop();
    releaseTheFetch();

    await drain();

    assert.equal(getNamedInitSegment(entry.id, "init-v0.mp4"), undefined, "no store write lands after the stop");
    assert.equal(entry.hls.initSegmentBytes, 0, "the byte counter is untouched");
    assert.equal(entry.hls.currentInitNames.video, null, "the current init name is never set");

    unregisterStream(entry.id);
  });
});

describe("TS relay parity (T-PAR)", () => {

  afterEach(() => {

    unregisterStream(0);
  });

  test("produces byte-identical playlist output for a pure MPEG-TS source", async () => {

    /* The characterization floor. This exact string was captured from the pre-change tree, so any fMP4 handling that leaks into the MPEG-TS path - a stray MAP,
     * an extension flip, a version bump, a changed media-sequence derivation - shows up here as a byte difference rather than as a subtle field regression.
     *
     * The fixture carries a discontinuity, a program-date-time, and a cue-out so the metadata propagation paths are inside the characterization rather than
     * only the trivial ones.
     */
    const expectedPlaylist = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n" +
      "#EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:00.000Z\n#EXTINF:6.000,\nsegment0.ts\n#EXT-X-DISCONTINUITY\n#EXTINF:6.000,\nsegment1.ts\n" +
      "#EXT-X-CUE-OUT:30.000\n#EXTINF:6.000,\nsegment2.ts\n";

    const tsBody = manifest([

      "#EXTM3U",
      "#EXT-X-VERSION:3",
      "#EXT-X-TARGETDURATION:6",
      "#EXT-X-MEDIA-SEQUENCE:100",
      "#EXT-X-PROGRAM-DATE-TIME:2026-01-01T00:00:00.000Z",
      "#EXTINF:6.000,",
      "seg100.ts",
      "#EXT-X-DISCONTINUITY",
      "#EXTINF:6.000,",
      "seg101.ts",
      "#EXT-X-CUE-OUT:30.000",
      "#EXTINF:6.000,",
      "seg102.ts"
    ]);

    const { harness } = await runRelay({

      bodyForCycle: () => tsBody,
      routes: { "https://cdn.test/seg1": fragmentRoute },
      targetCycles: 2
    });

    assert.equal(harness.playlist(), expectedPlaylist, "the MPEG-TS relay's playlist is byte-identical to the pre-change characterization");
    assert.deepEqual(harness.segmentNames(), [ "segment0.ts", "segment1.ts", "segment2.ts" ], "MPEG-TS fragments keep .ts names");
    assert.equal(harness.proxy.getSegmentIndex(), 3, "the segment index advanced exactly three times");
  });
});

describe("fMP4 relay: separate audio renditions", () => {

  afterEach(() => {

    unregisterStream(0);
  });

  test("stores each track's init under its own name and keeps the two independent (T17)", async () => {

    /* A split-track fMP4 source. Each track mints names from its own counter and prefix, so the two never collide, and each track's playlist references its
     * own initialization.
     */
    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise: settled, resolve: markSettled } = Promise.withResolvers<void>();

    let stopProxy: () => void = (): void => { /* Assigned once the proxy exists. */ };
    let videoCycles = 0;

    const videoHandler = (): Response => {

      videoCycles++;

      if(videoCycles >= 3) {

        stopProxy();
        markSettled();
      }

      return new Response(fmp4Manifest("https://cdn.test/vinit.cmfv", 100, ["sv100.cmfv"]), { status: 200 });
    };

    installFetchRouter({

      [AUDIO_VARIANT_URL]: (): Response => new Response(fmp4Manifest("https://cdn.test/ainit.cmfa", 100, ["sa100.cmfa"]), { status: 200 }),
      [VARIANT_URL]: videoHandler,
      "https://cdn.test/ainit.cmfa": initRoute("AUDIO-INIT"),
      "https://cdn.test/sa1": fragmentRoute,
      "https://cdn.test/sv1": fragmentRoute,
      "https://cdn.test/vinit.cmfv": initRoute("VIDEO-INIT")
    });

    const harness = makeHarness({ audioVariantUrl: AUDIO_VARIANT_URL });

    stopProxy = harness.proxy.stop;

    harness.proxy.start();

    await settled;
    await drain();

    assert.equal(harness.initNames().video, "init-v0.mp4", "the video track names from its own prefix and counter");
    assert.equal(harness.initNames().audio, "init-a0.mp4", "the audio track names from its own prefix and counter");
    assert.deepEqual(getNamedInitSegment(harness.streamId, "init-v0.mp4"), Buffer.from("VIDEO-INIT"), "the video init holds video bytes");
    assert.deepEqual(getNamedInitSegment(harness.streamId, "init-a0.mp4"), Buffer.from("AUDIO-INIT"), "the audio init holds audio bytes");

    const state = getStream(harness.streamId);

    assert.ok(Array.from(state?.hls.audioSegments.keys() ?? []).every((name) => name.endsWith(".m4s")), "audio fragments take the .m4s extension");
    assert.match(harness.audioPlaylist(), /#EXT-X-MAP:URI="init-a0\.mp4"/, "the audio playlist references the audio init");
    assert.match(harness.audioPlaylist(), /#EXT-X-VERSION:7/, "the audio playlist declares version 7");
  });
});

describe("segment pipeline: bounded-parallel fetching, in-order commit, and cancellation", () => {

  afterEach(() => {

    unregisterStream(0);
  });

  test("starts later segments' fetches before the first one has answered (P1)", async () => {

    /* The concurrency proof, made at the network layer rather than inferred from timing. The first segment's response is held open until the router has
     * received the THIRD segment's request, so the run can only complete if fetches were started ahead of the commit walk: a relay that downloads one segment
     * at a time never issues the second request while the first is unanswered, and the hold it is waiting on never lifts.
     */
    const firstSegment = makeHeldRoute();
    const thirdSegment = makeHeldRoute();

    // The third segment answers normally - what it contributes is its arrival, which is what releases the first.
    thirdSegment.release();
    void thirdSegment.requested.then((): void => firstSegment.release());

    const { harness, router } = await runRelay({

      bodyForCycle: () => tsManifest(100, [ "s100.ts", "s101.ts", "s102.ts" ]),
      routes: {

        "https://cdn.test/s100.ts": firstSegment.handler,
        "https://cdn.test/s101.ts": fragmentRoute,
        "https://cdn.test/s102.ts": thirdSegment.handler
      },
      targetCycles: 2
    });

    assert.deepEqual(harness.segmentNames(), [ "segment0.ts", "segment1.ts", "segment2.ts" ], "every segment in the window was stored");

    // Three mirrors the production window width, which is module-private.
    assert.equal(router.peakInFlight(), 3, "all three segment requests were open at the same time");
  });

  test("commits in upstream order when the fetches settle out of order (P2)", async () => {

    /* Ordering under out-of-order completion. The sequence-FIRST segment answers LAST, so a relay that stored bytes as they arrived would publish the third
     * segment's bytes as segment0. Identity is checked per segment rather than by count: the fragment route derives every body from its own URL, so the
     * stored bytes name the upstream segment they came from.
     */
    const firstSegment = makeHeldRoute();
    const thirdSegment = makeHeldRoute();

    thirdSegment.release();

    // The first segment is released only once the two behind it have fully settled, which is what makes completion order the reverse of commit order.
    void thirdSegment.requested.then(async (): Promise<void> => {

      await drain();

      firstSegment.release();
    });

    const { harness } = await runRelay({

      bodyForCycle: () => tsManifest(100, [ "s100.ts", "s101.ts", "s102.ts" ]),
      routes: {

        "https://cdn.test/s100.ts": firstSegment.handler,
        "https://cdn.test/s101.ts": fragmentRoute,
        "https://cdn.test/s102.ts": thirdSegment.handler
      },
      targetCycles: 2
    });

    assert.deepEqual(getSegment(harness.streamId, "segment0.ts"), Buffer.from("fragment:https://cdn.test/s100.ts"), "the first name holds the first sequence");
    assert.deepEqual(getSegment(harness.streamId, "segment1.ts"), Buffer.from("fragment:https://cdn.test/s101.ts"), "the second name holds the second");
    assert.deepEqual(getSegment(harness.streamId, "segment2.ts"), Buffer.from("fragment:https://cdn.test/s102.ts"), "the third name holds the third");

    const entries = harness.playlist().split("\n").filter((line) => line.startsWith("segment"));

    assert.deepEqual(entries, [ "segment0.ts", "segment1.ts", "segment2.ts" ], "the published window lists them in broadcast order");
  });

  test("keeps the window at its width and frees a slot only when an item commits (P3)", async () => {

    /* The window discipline from three sides: the fill it reaches, the ceiling it never crosses, and the moment a slot comes back. Ten fetchable segments are
     * offered with the first three held open, so the fourth request can appear only after one of the held items has been released AND has committed. A relay
     * that freed its slot when the bytes arrived rather than when the segment was published would dispatch the fourth request too early.
     */
    const held = [ makeHeldRoute(), makeHeldRoute(), makeHeldRoute() ];
    const names: string[] = [];

    // The names are built by index because each one encodes its own media sequence.
    for(let i = 0; i < 10; i++) {

      names.push("s" + String(100 + i) + ".ts");
    }

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise: settled, resolve: markSettled } = Promise.withResolvers<void>();

    let stopProxy: () => void = (): void => { /* Assigned once the proxy exists. */ };

    const paced = makePacedManifestRoute(() => tsManifest(100, names), 2, () => stopProxy(), markSettled);

    const router = installFetchRouter({

      [VARIANT_URL]: paced.handler,
      "https://cdn.test/s1": fragmentRoute,
      "https://cdn.test/s100.ts": held[0]!.handler,
      "https://cdn.test/s101.ts": held[1]!.handler,
      "https://cdn.test/s102.ts": held[2]!.handler
    });

    const harness = makeHarness();

    stopProxy = harness.proxy.stop;

    harness.proxy.start();

    await Promise.all(held.map((route) => route.requested));
    await drain();

    // Three mirrors the production window width, which is module-private.
    assert.equal(router.peakInFlight(), 3, "the window fills to exactly three concurrent segment fetches");
    assert.equal(router.countMatching("s103.ts"), 0, "the fourth segment waits for a slot rather than joining the window");

    held[0]!.release();

    await drain();

    assert.deepEqual(harness.segmentNames(), ["segment0.ts"], "the released item committed");
    assert.equal(router.countMatching("s103.ts"), 1, "the commit is what freed the slot the fourth segment then took");
    assert.equal(router.peakInFlight(), 3, "and the ceiling held while the window refilled");

    held[1]!.release();
    held[2]!.release();

    await settled;
    await drain();

    assert.equal(harness.segmentNames().length, 10, "the whole window committed once the holds lifted");

    harness.proxy.stop();
    unregisterStream(harness.streamId);
  });

  test("counts a failure when the item commits and clears it on a later success (P4)", async () => {

    /* Accounting rides the commit rather than the fetch. The first cycle ends on a failing segment, so the consecutive-error reading the monitor takes is 1;
     * the second cycle succeeds and clears it. Both readings are taken at a moment when the count is unambiguous - the first while the second cycle's manifest
     * is held open, which is after the first cycle's walk finished and before the second's began.
     */
    const secondManifest = makeHeldRoute(() => new Response(tsManifest(102, [ "s102.ts", "s103.ts" ]), { status: 200 }));

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise: settled, resolve: markSettled } = Promise.withResolvers<void>();

    let cycle = 0;
    let stopProxy: () => void = (): void => { /* Assigned once the proxy exists. */ };

    const manifestHandler = (url: string): Response | Promise<Response> => {

      cycle++;

      switch(cycle) {

        case 1: {

          return new Response(tsManifest(100, [ "s100.ts", "s101.ts" ]), { status: 200 });
        }

        case 2: {

          return secondManifest.handler(url);
        }

        default: {

          stopProxy();
          markSettled();

          return new Response(tsManifest(102, [ "s102.ts", "s103.ts" ]), { status: 200 });
        }
      }
    };

    // s101.ts is deliberately unrouted, so it answers 404 - the mid-batch failure this case turns on.
    const router = installFetchRouter({

      [VARIANT_URL]: manifestHandler,
      "https://cdn.test/s100.ts": fragmentRoute,
      "https://cdn.test/s102.ts": fragmentRoute,
      "https://cdn.test/s103.ts": fragmentRoute
    });

    const harness = makeHarness();

    stopProxy = harness.proxy.stop;

    harness.proxy.start();

    await secondManifest.requested;
    await drain();

    assert.deepEqual(harness.segmentNames(), ["segment0.ts"], "the succeeding segment committed and the failing one did not");
    assert.equal(harness.proxy.getConsecutiveErrors(), 1, "the failing commit counted exactly one consecutive error");

    secondManifest.release();

    await settled;
    await drain();

    assert.equal(harness.proxy.getConsecutiveErrors(), 0, "a later success clears the count");
    assert.equal(harness.proxy.hasErrored(), false, "one failure stays well below the escalation threshold");
    assert.equal(router.countMatching("s101.ts"), 1, "the failing segment was attempted exactly once");

    harness.proxy.stop();
    unregisterStream(harness.streamId);
  });

  test("escalates once mid-window and cancels the fetches already open (P5)", async () => {

    /* Escalation is a single transition, and it takes the window with it. Five consecutive failures cross the threshold while a sixth segment's fetch is
     * already open: the error callback fires exactly once, nothing commits after the escalating item, and the open fetch observes the cancellation - the proxy
     * does not leave connections running past its own stop.
     */
    const heldSixth = makeHeldRoute();
    const names = [ "s100.ts", "s101.ts", "s102.ts", "s103.ts", "s104.ts", "s105.ts", "s106.ts" ];

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise: settled, resolve: markSettled } = Promise.withResolvers<void>();

    let escalations = 0;

    // Only the first and the last segment are routed; the five between them answer 404, which is the consecutive run that crosses the threshold.
    const router = installFetchRouter({

      [VARIANT_URL]: (): Response => new Response(tsManifest(100, names), { status: 200 }),
      "https://cdn.test/s100.ts": fragmentRoute,
      "https://cdn.test/s106.ts": heldSixth.handler
    });

    const harness = makeHarness({

      onError: (): void => {

        escalations++;
        markSettled();
      }
    });

    harness.proxy.start();

    await settled;
    await drain();

    assert.equal(escalations, 1, "the error callback fired exactly once");
    assert.equal(harness.proxy.hasErrored(), true, "the proxy latched the error state the monitor reads");
    assert.equal(harness.proxy.isStopped(), true, "and stopped itself");
    assert.deepEqual(harness.segmentNames(), ["segment0.ts"], "nothing committed after the escalating item");
    assert.ok(router.aborted.includes("https://cdn.test/s106.ts"), "the fetch still open when the threshold was crossed was cancelled");

    heldSixth.release();
    harness.proxy.stop();
    unregisterStream(harness.streamId);
  });

  test("cancels the whole fetch family at stop without counting the cancellations (P6)", async () => {

    /* Teardown reaches every kind of fetch the relay opens. A segment fetch and a decryption key fetch are both open when the proxy stops: both are cancelled,
     * nothing stores afterwards, and neither cancellation reaches the accounting - the consecutive-error reading is zero rather than merely under the
     * threshold, so the termination summary reports fetches that failed rather than fetches the stop itself ended.
     */
    const heldKey = makeHeldRoute(() => new Response(Buffer.alloc(16), { status: 200 }));
    const heldSegment = makeHeldRoute();

    const aesBody = manifest([

      "#EXTM3U", "#EXT-X-VERSION:3", "#EXT-X-TARGETDURATION:6", "#EXT-X-MEDIA-SEQUENCE:100",
      "#EXT-X-KEY:METHOD=AES-128,URI=\"https://cdn.test/key.bin\"",
      "#EXTINF:6.000,", "s100.ts", "#EXTINF:6.000,", "s101.ts"
    ]);

    const router = installFetchRouter({

      [VARIANT_URL]: (): Response => new Response(aesBody, { status: 200 }),
      "https://cdn.test/key.bin": heldKey.handler,
      "https://cdn.test/s100.ts": heldSegment.handler,
      "https://cdn.test/s101.ts": fragmentRoute
    });

    const harness = makeHarness({ encryption: "aes128" });

    harness.proxy.start();

    // The first segment is held at its own fetch; the second gets its bytes and holds at the key fetch, so one of each kind is open when the stop lands.
    await Promise.all([ heldSegment.requested, heldKey.requested ]);
    await drain();

    const pollsBeforeStop = router.countMatching("variant.m3u8");

    harness.proxy.stop();

    await drain();

    assert.ok(router.aborted.includes("https://cdn.test/s100.ts"), "the open segment fetch was cancelled");
    assert.ok(router.aborted.includes("https://cdn.test/key.bin"), "the open key fetch was cancelled");
    assert.deepEqual(harness.segmentNames(), [], "nothing stored after the stop");
    assert.equal(harness.proxy.hasErrored(), false, "a stop is not an error");
    assert.equal(harness.proxy.getConsecutiveErrors(), 0, "cancelled fetches are excluded from the accounting rather than merely under the threshold");
    assert.equal(router.countMatching("variant.m3u8"), pollsBeforeStop, "the poll loop issued no further manifest request");

    heldKey.release();
    heldSegment.release();
    unregisterStream(harness.streamId);
  });

  test("attempts the initialization inline while what precedes it commits and what follows waits (P7)", async () => {

    /* The transition, under a window that runs ahead of the walk. The relay reaches an A-to-B initialization change mid-batch and attempts the new
     * initialization at exactly that position: everything before the transition commits under the initialization already in force while the attempt is open, and
     * nothing after it has even been requested. A design that dispatched the segments beyond the transition early, or adopted the new initialization before
     * the entries preceding it were published, fails one of those two observations. Once the attempt lands, the entries after it are governed by the new
     * initialization in the playlist the client actually reads.
     */
    const heldInitB = makeHeldRoute(initRoute("VIDEO-INIT-B-DIFFERENT"));

    const transitionBody = manifest([

      "#EXTM3U", "#EXT-X-VERSION:7", "#EXT-X-TARGETDURATION:6", "#EXT-X-MEDIA-SEQUENCE:101",
      "#EXT-X-MAP:URI=\"https://cdn.test/initA.cmfv\"",
      "#EXTINF:6.000,", "sA101.cmfv",
      "#EXTINF:6.000,", "sA102.cmfv",
      "#EXT-X-MAP:URI=\"https://cdn.test/initB.cmfv\"",
      "#EXTINF:6.000,", "sB103.cmfv",
      "#EXTINF:6.000,", "sB104.cmfv"
    ]);

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise: settled, resolve: markSettled } = Promise.withResolvers<void>();

    let cycle = 0;
    let stopProxy: () => void = (): void => { /* Assigned once the proxy exists. */ };

    const manifestHandler = (): Response => {

      cycle++;

      if(cycle >= 3) {

        stopProxy();
        markSettled();
      }

      return new Response((cycle === 1) ? fmp4Manifest("https://cdn.test/initA.cmfv", 100, ["sA100.cmfv"]) : transitionBody, { status: 200 });
    };

    const router = installFetchRouter({

      [VARIANT_URL]: manifestHandler,
      "https://cdn.test/initA.cmfv": initRoute("VIDEO-INIT-A"),
      "https://cdn.test/initB.cmfv": heldInitB.handler,
      "https://cdn.test/sA": fragmentRoute,
      "https://cdn.test/sB": fragmentRoute
    });

    const harness = makeHarness();

    stopProxy = harness.proxy.stop;

    harness.proxy.start();

    await heldInitB.requested;
    await drain();

    assert.deepEqual(harness.segmentNames(), [ "segment0.m4s", "segment1.m4s", "segment2.m4s" ],
      "the fragments before the transition committed while the initialization attempt was still open");
    assert.equal(router.countMatching("sB10"), 0, "no fragment beyond the unresolved transition has been requested");

    heldInitB.release();

    await settled;
    await drain();

    assert.equal(router.countMatching("initB.cmfv"), 1, "the transition was attempted exactly once for the cycle");
    assert.deepEqual(harness.segmentNames(), [ "segment0.m4s", "segment1.m4s", "segment2.m4s", "segment3.m4s", "segment4.m4s" ],
      "the fragments after the transition committed once it adopted");

    const lines = harness.playlist().split("\n");

    // The tag governing a fragment is the closest MAP reference above it, so each fragment's tag is read by walking back from its own line.
    const initGoverning = (segment: string): string | null => {

      const segmentIndex = lines.indexOf(segment);

      for(let i = segmentIndex - 1; i >= 0; i--) {

        const match = /^#EXT-X-MAP:URI="([^"]+)"$/.exec(lines[i] ?? "");

        if(match?.[1] !== undefined) {

          return match[1];
        }
      }

      return null;
    };

    assert.equal(initGoverning("segment1.m4s"), "init-v0.mp4", "the fragments before the transition are governed by the original initialization");
    assert.equal(initGoverning("segment2.m4s"), "init-v0.mp4", "including the last one before it");
    assert.equal(initGoverning("segment3.m4s"), "init-v1.mp4", "the first fragment after the transition moves the client onto the new initialization");
    assert.equal(initGoverning("segment4.m4s"), "init-v1.mp4", "and the fragment following it inherits that reference");

    harness.proxy.stop();
    unregisterStream(harness.streamId);
  });

  test("discards a segment whose fetch succeeds at or after the stop (P8)", async () => {

    /* The straggler that succeeds rather than fails. Cancellation normally surfaces as a rejected fetch, but a request already on the wire can complete
     * normally in the moment between the abort and the socket closing, and the decrypt path turns a cancelled key fetch into a resolved null on the same
     * frame. Reading only the shape of the settlement would therefore let bytes into a stream whose state is being torn down; what stands between them and
     * the store is the relay's reading of the cancellation state itself at settlement. The route here ignores the cancellation and answers with real segment
     * data after the stop, which is the settlement no rejection-based reading can catch.
     */
    const lateSegment = makeHeldRoute(fragmentRoute, { ignoresAbort: true });

    const router = installFetchRouter({

      [VARIANT_URL]: (): Response => new Response(tsManifest(100, ["s100.ts"]), { status: 200 }),
      "https://cdn.test/s100.ts": lateSegment.handler
    });

    const harness = makeHarness();

    harness.proxy.start();

    await lateSegment.requested;
    await drain();

    assert.deepEqual(harness.segmentNames(), [], "nothing has been stored while the fetch is still open");

    harness.proxy.stop();

    lateSegment.release();

    await drain();

    assert.deepEqual(harness.segmentNames(), [], "the segment that resolved after the stop was discarded rather than stored");
    assert.equal(harness.proxy.getConsecutiveErrors(), 0, "and it counted as neither a success nor a failure");
    assert.equal(harness.proxy.hasErrored(), false, "a stop is not an error");
    assert.equal(router.countMatching("s100.ts"), 1, "the fetch under test did reach the network, so the discard is of real bytes");

    unregisterStream(harness.streamId);
  });
});
