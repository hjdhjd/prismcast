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
import { getStream, registerStream, unregisterStream } from "./../streaming/registry.ts";
import type { Clock } from "../utils/index.ts";
import type { NativeProxyOptions } from "./proxy.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { createNativeProxy } from "./proxy.ts";
import { getNamedInitSegment } from "./../streaming/hlsSegments.ts";
import { makeRegistryEntry } from "./../streaming/registry.helpers.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

// The URL the harness always polls for the video variant, and the audio variant for the split-track cases.
const VARIANT_URL = "https://cdn.test/variant.m3u8";
const AUDIO_VARIANT_URL = "https://cdn.test/audio.m3u8";

/* A recording fetch stub. Routes are matched by longest URL prefix so a test can register both a manifest and the segments beneath it; anything unmatched
 * answers 404 so an unintended request surfaces as a missing segment rather than a silent pass. Every request URL is appended to calls, which is what the
 * fetch-count assertions read.
 */
interface FetchRouter {

  calls: string[];
  countMatching: (fragment: string) => number;
}

type RouteHandler = (url: string) => Response | Promise<Response>;

function installFetchRouter(routes: Record<string, RouteHandler>): FetchRouter {

  const calls: string[] = [];

  globalThis.fetch = (async (url: string | URL): Promise<Response> => {

    const urlStr = url.toString();

    calls.push(urlStr);

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

    return matched(urlStr);
  }) as typeof globalThis.fetch;

  return {

    calls,
    countMatching: (fragment: string): number => calls.filter((url) => url.includes(fragment)).length
  };
}

/* A clock whose sleeps resolve immediately, so poll cycles advance as fast as the microtask queue allows rather than on real timers.
 */
function makeImmediateClock(): Clock {

  return {

    now: (): number => 0,
    raceWithTimeout: async <T>(promise: Promise<T>): Promise<T> => promise,
    sleep: async (): Promise<void> => { /* Resolve immediately so the cadence never waits on a real timer. */ }
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
