/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * manifestInterceptor.test.ts: Unit tests for the installManifestInterceptor and awaitMatchingManifest state machines - the main consumer-facing surface of
 * manifestInterceptor.ts. These exercise the orchestration on top of the HLS playlist observer: finalize-direct-vs-guide semantics, the settle-delay wait, the
 * channel-selection epoch and membership/liveness override, the epoch-free timeout, dispose paths, and TC39 using-syntax integration. The pure
 * selectInterceptedManifest helper that backs finalize()'s resolution is tested in the companion file manifestInterceptor.selection.test.ts.
 *
 * How the observer is substituted. The orchestrators sit one layer above hlsPlaylistObserver.observeHlsPlaylists(), which itself rides on the tab network
 * observer + CDP; driving the real lower layers would require a Puppeteer browser. Both orchestrators accept the observer factory as an injected parameter
 * (default observeHlsPlaylists), so we pass a controlled factory that captures the onPlaylist callback and lets tests synthesize playlist observations on
 * demand. The factory is typed as observeHlsPlaylists' own contract, so the double cannot drift from the real signature.
 */
import type { HlsPlaylistObserver, HlsPlaylistObserverOptions, ObservedHlsPlaylist } from "./hlsPlaylistObserver.ts";
import type { ManifestInterceptionResult, ManifestInterceptorHandle } from "./manifestInterceptor.ts";
import { awaitMatchingManifest, installManifestInterceptor } from "./manifestInterceptor.ts";
import { beforeEach, describe, mock, test } from "node:test";
import type { Nullable } from "../types/index.ts";
import type { Page } from "puppeteer-core";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

// The interception window every install() helper passes. The timeout parameter is required now that no production caller relies on a shared default; stating it in
// the test is strictly clearer, and the timeout-path tests tick just past this value.
const TEST_INTERCEPTION_WINDOW = 15000;

/* An ObservedHlsPlaylist minus its wire sequence, plus an optional explicit sequence override. fire() assigns the next incremental ordinal when sequence is
 * omitted; a case that needs to place a delivery out of arrival order (skew cases) passes the ordinal explicitly. childUrls defaults to an empty list on the
 * master arm so cases that do not exercise membership stay terse.
 */
type ObservedHlsPlaylistDraft = { readonly childUrls?: readonly string[]; readonly kind: "master"; readonly sequence?: number; readonly url: string } |
  { readonly kind: "media"; readonly live: boolean; readonly sequence?: number; readonly url: string };

/* ControlledObserver is the test-side handle returned by the injected observe factory. It captures the onPlaylist callback at construction so tests can call
 * fire() to synthesize a playlist observation, assigns wire-arrival sequences the same way the real observer does (incrementing at each observation, starting at
 * 1), exposes currentSequence() so mark plumbing reads the same fence the production observer offers, and exposes a read-only disposed flag so tests can assert
 * the disposal lifecycle. dispose() and Symbol.dispose are the same function reference to match the real observer's contract.
 */
interface ControlledObserver extends HlsPlaylistObserver {

  // Read-only view of the observer's disposed state. Backed by a closure variable inside the factory so the property is observable without permitting outside
  // mutation - tests assert on it but cannot flip it from the outside.
  readonly disposed: boolean;

  // Fires a synthetic playlist observation through the captured onPlaylist callback. No-op when the observer has already been disposed.
  readonly fire: (draft: ObservedHlsPlaylistDraft) => void;
}

/* createControlledObserver constructs a fresh test-side observer. The disposed state and the wire-sequence counter live in closure variables so they cannot be
 * mutated from outside the factory; the public surface exposes disposed as a getter and currentSequence as the counter reader. dispose() and Symbol.dispose are
 * the same function reference, satisfying the production observer's identity contract. options is the real HlsPlaylistObserverOptions so fire() delivers a
 * genuine ObservedHlsPlaylist.
 */
function createControlledObserver(options: HlsPlaylistObserverOptions): ControlledObserver {

  let disposed = false;
  let sequenceCounter = 0;

  const dispose = (): void => {

    disposed = true;
  };

  return {

    currentSequence: (): number => sequenceCounter,
    [Symbol.dispose]: dispose,
    dispose,
    get disposed(): boolean {

      return disposed;
    },
    fire: (draft: ObservedHlsPlaylistDraft): void => {

      if(disposed) {

        return;
      }

      // Assign the next incremental ordinal unless the case supplied one; an explicit ordinal above the counter advances it so currentSequence keeps tracking the
      // highest wire arrival, while an explicit low ordinal (a late, out-of-order delivery) leaves the counter alone.
      const sequence = draft.sequence ?? (sequenceCounter + 1);

      if(sequence > sequenceCounter) {

        sequenceCounter = sequence;
      }

      const playlist: ObservedHlsPlaylist = (draft.kind === "master") ?
        { childUrls: draft.childUrls ?? [], kind: "master", sequence, url: draft.url } :
        { kind: "media", live: draft.live, sequence, url: draft.url };

      options.onPlaylist(playlist);
    }
  };
}

// Per-test mock state. mockInstallShouldFail makes the next call to the observe factory return null (simulating the underlying tab observer failing to install);
// mockPendingObserver receives the ControlledObserver created on each successful call so tests can drive it. Declared as separate `let` bindings rather than
// fields on a container object so per-field mutability is explicit at the declaration site.
let mockInstallShouldFail = false;
let mockPendingObserver: ControlledObserver | null = null;

/* The injected observe factory: returns null when mockInstallShouldFail is set, otherwise constructs a ControlledObserver, parks it in mockPendingObserver so the
 * test can drive it, and returns it as the observer handle. Typed as observeHlsPlaylists' own contract so any drift in the real signature surfaces here at compile
 * time - the fidelity a module mock could not enforce.
 */
const mockObserveFactory = async (_page: Page, options: HlsPlaylistObserverOptions): Promise<Nullable<HlsPlaylistObserver>> => {

  if(mockInstallShouldFail) {

    return null;
  }

  const observer = createControlledObserver(options);

  mockPendingObserver = observer;

  return observer;
};

// fakePage is a Page stub that satisfies the orchestrators' parameter type. The orchestrators never call methods on the page directly - they pass it through to
// the observe factory, which in our double ignores it. A minimal cast suffices.
const fakePage = {} as unknown as Page;

// Install/await helpers that inject the controlled observe factory (and the fake page and explicit window), so individual tests read as behavior rather than
// wiring. Every test drives its observer through mockPendingObserver after calling these.
function install(timeout: number = TEST_INTERCEPTION_WINDOW): Promise<Nullable<ManifestInterceptorHandle>> {

  return installManifestInterceptor(fakePage, timeout, mockObserveFactory);
}

function awaitMatch(predicate: (url: string) => boolean, timeout: number): Promise<Nullable<string>> {

  return awaitMatchingManifest(fakePage, predicate, timeout, mockObserveFactory);
}

beforeEach(() => {

  // Reset shared mock state so a prior test's setup cannot leak into the next test's setup. Each test that needs install failure sets mockInstallShouldFail
  // explicitly; mockPendingObserver is overwritten on each successful install.
  mockInstallShouldFail = false;
  mockPendingObserver = null;
});

describe("installManifestInterceptor", () => {

  test("returns null when the underlying HLS observer fails to install", async () => {

    // Boundary: the orchestrator is a strict layer on top of observeHlsPlaylists. If installation fails, the orchestrator propagates the failure rather than
    // returning a handle whose promise would never resolve.
    mockInstallShouldFail = true;

    const interceptor = await install();

    assert.equal(interceptor, null, "install failure surfaces as null");
  });

  test("finalize(true) with a master URL already captured resolves immediately with that URL", async () => {

    // The direct-tune fast path: when the navigated URL itself selects the channel and the player has already loaded the master manifest, finalize(true) must
    // settle the promise without waiting the FINALIZE_SETTLE_DELAY. The first-master-URL-wins selection rule is what this test pins.
    const interceptor = await install();

    assert.ok(interceptor, "interceptor installed");

    const observer = mockPendingObserver;

    assert.ok(observer, "controlled observer captured");

    observer.fire({ kind: "master", url: "https://cdn.test/first-master.m3u8" });

    interceptor.finalize(true);

    const result = await interceptor.promise;

    assert.ok(result, "promise resolved with a result");
    assert.equal(result.manifestUrl, "https://cdn.test/first-master.m3u8", "first master URL selected");
    assert.equal(result.selectedKind, "master", "kind reported as master");
    assert.equal(observer.disposed, true, "observer disposed on settle");
  });

  test("finalize(false) waits the settle delay and resolves with the latest URL (guide-tune semantics)", async () => {

    // Guide tune with no epoch declared: the channel-switch click may trigger a fresh manifest fetch that arrives milliseconds after the click handler returns.
    // finalize(false) waits the settle delay before resolving, and with no epoch the latest master wins over the page's default-channel manifest.
    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const interceptor = await install();

      assert.ok(interceptor, "interceptor installed");

      const observer = mockPendingObserver;

      assert.ok(observer, "controlled observer captured");

      observer.fire({ kind: "master", url: "https://cdn.test/first-master.m3u8" });

      interceptor.finalize(false);

      // The settle delay is 1500ms; a newer manifest arrives 500ms into the wait.
      mock.timers.tick(500);
      observer.fire({ kind: "master", url: "https://cdn.test/latest-master.m3u8" });

      // Advance past the remaining settle window.
      mock.timers.tick(1100);

      const result = await interceptor.promise;

      assert.ok(result, "promise resolved with a result");
      assert.equal(result.manifestUrl, "https://cdn.test/latest-master.m3u8", "latest master URL wins on guide tune with no epoch");
      assert.equal(result.selectedKind, "master");
    } finally {

      mock.timers.reset();
    }
  });

  test("a master wins over a later live media on a guide tune when no epoch was declared", async () => {

    // With no channel-selection epoch stamped, a live media arriving after a master does not override it - the override requires a declared epoch. This locks the
    // no-epoch branch of the rule that selectInterceptedManifest applies at resolution time.
    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const interceptor = await install();

      assert.ok(interceptor, "interceptor installed");

      const observer = mockPendingObserver;

      assert.ok(observer, "controlled observer captured");

      observer.fire({ kind: "master", url: "https://cdn.test/master.m3u8" });
      observer.fire({ kind: "media", live: true, url: "https://cdn.test/late-media.m3u8" });

      interceptor.finalize(false);

      mock.timers.tick(1600);

      const result = await interceptor.promise;

      assert.ok(result, "promise resolved with a result");
      assert.equal(result.manifestUrl, "https://cdn.test/master.m3u8", "master wins over later media with no epoch declared");
      assert.equal(result.selectedKind, "master");
    } finally {

      mock.timers.reset();
    }
  });

  test("falls back to media URL when no master ever arrives (media-only sites)", async () => {

    // Media-only direct tune: a site whose player loads only a media playlist (e.g., Angelcam from issue #34) must still resolve. The media URL is selected
    // because no master is available; the settle delay applies because the direct-tune fast path only short-circuits when a master is already captured.
    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const interceptor = await install();

      assert.ok(interceptor, "interceptor installed");

      const observer = mockPendingObserver;

      assert.ok(observer, "controlled observer captured");

      observer.fire({ kind: "media", live: true, url: "https://cdn.test/only-media.m3u8" });

      interceptor.finalize(true);

      mock.timers.tick(1600);

      const result = await interceptor.promise;

      assert.ok(result, "promise resolved with a result");
      assert.equal(result.manifestUrl, "https://cdn.test/only-media.m3u8", "media URL selected when no master arrived");
      assert.equal(result.selectedKind, "media");
    } finally {

      mock.timers.reset();
    }
  });

  test("timeout safety net resolves with the latest captured URL when finalize is never called", async () => {

    // Defensive contract: if a caller forgets to invoke finalize, the timeout fires at the caller-supplied window (15000ms here) and the promise resolves with
    // whatever was captured. This prevents an interceptor from hanging the calling code if the lifecycle hand-off goes wrong upstream.
    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const interceptor = await install();

      assert.ok(interceptor, "interceptor installed");

      const observer = mockPendingObserver;

      assert.ok(observer, "controlled observer captured");

      observer.fire({ kind: "master", url: "https://cdn.test/captured.m3u8" });

      // Do NOT call finalize. Advance past the supplied 15000ms window.
      mock.timers.tick(15100);

      const result = await interceptor.promise;

      assert.ok(result, "timeout still resolves with the captured URL");
      assert.equal(result.manifestUrl, "https://cdn.test/captured.m3u8", "latest URL selected on timeout (mirrors guide-tune semantics)");
      assert.equal(result.selectedKind, "master");
    } finally {

      mock.timers.reset();
    }
  });

  test("timeout with no captured manifest resolves null and disposes the observer", async () => {

    // Negative path: no manifest captured, no finalize, timeout fires. The promise must resolve null (rather than hang) and the observer must be disposed so
    // resources are reclaimed.
    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const interceptor = await install();

      assert.ok(interceptor, "interceptor installed");

      const observer = mockPendingObserver;

      assert.ok(observer, "controlled observer captured");

      mock.timers.tick(15100);

      const result = await interceptor.promise;

      assert.equal(result, null, "timeout with no captures resolves null");
      assert.equal(observer.disposed, true, "observer disposed even on the empty-timeout path");
    } finally {

      mock.timers.reset();
    }
  });

  test("dispose() before finalize resolves the promise with null and tears down the observer", async () => {

    // Cancellation path: a caller may abandon the interception before finalize fires (e.g., the upstream tune step threw). dispose() must settle the promise so
    // awaiters do not hang and the underlying observer must be released.
    const interceptor = await install();

    assert.ok(interceptor, "interceptor installed");

    const observer = mockPendingObserver;

    assert.ok(observer, "controlled observer captured");

    observer.fire({ kind: "master", url: "https://cdn.test/captured.m3u8" });

    interceptor.dispose();

    const result = await interceptor.promise;

    assert.equal(result, null, "dispose resolves the promise with null even when captures exist");
    assert.equal(observer.disposed, true, "underlying observer disposed");
  });

  test("dispose() after finalize is a safe no-op (idempotent lifecycle)", async () => {

    // Boundary: the cleanup paths in PrismCast can invoke dispose from multiple code paths. After finalize has already settled the promise, dispose must not
    // throw, must not re-resolve, and must not re-dispose the observer.
    const interceptor = await install();

    assert.ok(interceptor, "interceptor installed");

    const observer = mockPendingObserver;

    assert.ok(observer, "controlled observer captured");

    observer.fire({ kind: "master", url: "https://cdn.test/captured.m3u8" });

    interceptor.finalize(true);

    const result = await interceptor.promise;

    assert.ok(result, "finalize settled the promise");
    assert.equal(observer.disposed, true, "observer disposed by finalize");

    // Second dispose must be a no-op.
    assert.doesNotThrow(() => {

      interceptor.dispose();
    });
  });

  test("[Symbol.dispose] is the same function reference as dispose() on the handle", async () => {

    // Identity contract for TC39 ERM: callers can use either explicit dispose() or "using" and get identical behavior because Symbol.dispose IS dispose - not
    // just an alias by convention. Matches the same expectation on TabNetworkObserver and HlsPlaylistObserver.
    const interceptor = await install();

    assert.ok(interceptor, "interceptor installed");
    assert.equal(typeof interceptor[Symbol.dispose], "function", "Symbol.dispose hook present");
    assert.equal(interceptor[Symbol.dispose], interceptor.dispose, "Symbol.dispose is the same function reference as dispose");

    interceptor.dispose();
  });

  test("the using keyword triggers disposal at scope exit (normal path)", async () => {

    // End-to-end TC39 ERM contract: at scope exit, V8/Node invokes Symbol.dispose, which calls dispose(), which resolves the pending promise with null and
    // tears down the observer. We capture the promise outside the using scope so it can be awaited after disposal.
    let capturedPromise!: Promise<Nullable<ManifestInterceptionResult>>;
    let capturedObserver!: ControlledObserver;

    {

      using interceptor = await install();

      assert.ok(interceptor, "interceptor installed inside the using scope");

      capturedPromise = interceptor.promise;
      capturedObserver = mockPendingObserver!;
    }

    // Scope exit fires Symbol.dispose. The observable side effects are: (1) the promise resolves to null, (2) the underlying observer is disposed.
    const result = await capturedPromise;

    assert.equal(result, null, "using-scope exit resolves the promise with null");
    assert.equal(capturedObserver.disposed, true, "using-scope exit disposes the underlying observer");
  });

  test("the using keyword triggers disposal even when the scope exits via thrown exception", async () => {

    // Exception-safety contract: TC39 ERM guarantees disposal on the throw path, which is why Symbol.dispose is used here instead of an explicit dispose()
    // call inside a finally block - without that guarantee, the finally-block approach would be required instead.
    let capturedPromise!: Promise<Nullable<ManifestInterceptionResult>>;
    let capturedObserver!: ControlledObserver;

    await assert.rejects(async () => {

      using interceptor = await install();

      assert.ok(interceptor, "interceptor installed inside the using scope");

      capturedPromise = interceptor.promise;
      capturedObserver = mockPendingObserver!;

      throw new Error("simulated failure inside the using scope");
    }, /simulated failure/);

    const result = await capturedPromise;

    assert.equal(result, null, "throw-path scope exit resolves the promise with null");
    assert.equal(capturedObserver.disposed, true, "throw-path scope exit disposes the underlying observer");
  });
});

/* Epoch, membership, and liveness orchestration. These drive the full state machine through the injected observer and mock.timers, exercising the guide-tune
 * three-signal rule end-to-end: the mark plumbing, wire-ordered slot updates, and the epoch-free timeout. Each scenario asserts selectedKind on the resolved
 * result so the kind the verifier gate depends on is pinned, not inferred.
 */
describe("installManifestInterceptor epoch and membership orchestration", () => {

  const PAGE_MASTER = "https://guide.test/default-chan/master.m3u8";
  const PAGE_MASTER_CHILDREN = ["https://guide.test/default-chan/720p.m3u8"];
  const CLICKED_MEDIA = "https://cdn.test/clicked-chan/chunklist.m3u8";

  test("the bug end-to-end: a page-load master, a mark, then a post-mark live foreign media resolves the media", async () => {

    // The flagship fix. The page-load default channel's master is captured first, the channel-selection epoch is stamped, then the click's live non-member media
    // arrives after the mark. finalize(false) plus the settle tick resolves the media - the clicked channel's playlist - rather than the stale master. This same
    // observation sequence resolves the master under a categorical master-preference and the media under the three-signal epoch rule; the opposite resolution from
    // one sequence is the behavioral pin, since no executable red-before compiles against the widened signature.
    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const interceptor = await install();

      assert.ok(interceptor, "interceptor installed");

      const observer = mockPendingObserver;

      assert.ok(observer, "controlled observer captured");

      observer.fire({ childUrls: PAGE_MASTER_CHILDREN, kind: "master", url: PAGE_MASTER });
      interceptor.markChannelSelectionStart();
      observer.fire({ kind: "media", live: true, url: CLICKED_MEDIA });

      interceptor.finalize(false);
      mock.timers.tick(1600);

      const result = await interceptor.promise;

      assert.ok(result, "promise resolved with a result");
      assert.equal(result.manifestUrl, CLICKED_MEDIA, "the post-epoch live non-member media overrides the pre-epoch page-load master");
      assert.equal(result.selectedKind, "media");
    } finally {

      mock.timers.reset();
    }
  });

  test("a healthy master-based tune resolves the post-mark master categorically despite a later foreign live media", async () => {

    // Master-based guide site: the mark is stamped, the click's fresh master arrives after it, and a member media plus a foreign live media follow. The post-mark
    // master answers categorically - rule 2 fires before the media override is even considered.
    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const interceptor = await install();

      assert.ok(interceptor, "interceptor installed");

      const observer = mockPendingObserver;

      assert.ok(observer, "controlled observer captured");

      const freshMaster = "https://cdn.test/clicked-chan/master.m3u8";
      const memberMedia = "https://cdn.test/clicked-chan/720p.m3u8";

      interceptor.markChannelSelectionStart();
      observer.fire({ childUrls: [memberMedia], kind: "master", url: freshMaster });
      observer.fire({ kind: "media", live: true, url: memberMedia });
      observer.fire({ kind: "media", live: true, url: "https://cdn.test/other-chan/chunklist.m3u8" });

      interceptor.finalize(false);
      mock.timers.tick(1600);

      const result = await interceptor.promise;

      assert.ok(result, "promise resolved with a result");
      assert.equal(result.manifestUrl, freshMaster, "the post-mark master answers the click categorically");
      assert.equal(result.selectedKind, "master");
    } finally {

      mock.timers.reset();
    }
  });

  test("a pre-mark master with a post-mark MEMBER media only resolves the master (membership corroborates)", async () => {

    // The media fired after the mark is genuinely one of the master's declared children, so it corroborates the master rather than overriding it. The master's
    // childUrls literally contain the fired media URL, so a broken membership match would resolve the media and fail this case.
    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const interceptor = await install();

      assert.ok(interceptor, "interceptor installed");

      const observer = mockPendingObserver;

      assert.ok(observer, "controlled observer captured");

      const memberMedia = "https://cdn.test/default-chan/720p.m3u8";

      observer.fire({ childUrls: [memberMedia], kind: "master", url: PAGE_MASTER });
      interceptor.markChannelSelectionStart();
      observer.fire({ kind: "media", live: true, url: memberMedia });

      interceptor.finalize(false);
      mock.timers.tick(1600);

      const result = await interceptor.promise;

      assert.ok(result, "promise resolved with a result");
      assert.equal(result.manifestUrl, PAGE_MASTER, "a member media corroborates the master and does not override it");
      assert.equal(result.selectedKind, "master");
    } finally {

      mock.timers.reset();
    }
  });

  test("a pre-mark-sequenced master delivered after a post-mark media is still treated as pre-epoch (delivery-order skew)", async () => {

    // Delivery-order skew: after the mark and after the post-mark media, a stale master with a pre-mark sequence is delivered last. Because the record fences on
    // wire ordinal, not delivery order, the late master stays pre-epoch and the media override wins. A delivery-order fence would resolve the last-delivered
    // master instead.
    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const interceptor = await install();

      assert.ok(interceptor, "interceptor installed");

      const observer = mockPendingObserver;

      assert.ok(observer, "controlled observer captured");

      observer.fire({ childUrls: PAGE_MASTER_CHILDREN, kind: "master", sequence: 1, url: PAGE_MASTER });
      interceptor.markChannelSelectionStart();
      observer.fire({ kind: "media", live: true, sequence: 2, url: CLICKED_MEDIA });

      // A stale master delivered last but carrying a pre-mark wire ordinal. It must not become the "latest" master or read post-epoch.
      observer.fire({ childUrls: PAGE_MASTER_CHILDREN, kind: "master", sequence: 1, url: "https://guide.test/default-chan/stale-master.m3u8" });

      interceptor.finalize(false);
      mock.timers.tick(1600);

      const result = await interceptor.promise;

      assert.ok(result, "promise resolved with a result");
      assert.equal(result.manifestUrl, CLICKED_MEDIA, "a pre-mark-sequenced master delivered late does not flip the resolution to the master");
      assert.equal(result.selectedKind, "media");
    } finally {

      mock.timers.reset();
    }
  });

  test("the first slot holds the lower-sequenced master when a lower ordinal is delivered after a higher one", async () => {

    // Wire-order first-slot guard on a direct tune: a master with ordinal 2 is delivered first, then one with ordinal 1. The first slot must correct to the
    // lower ordinal (the earlier wire arrival), so finalize(true) resolves the ordinal-1 master.
    const interceptor = await install();

    assert.ok(interceptor, "interceptor installed");

    const observer = mockPendingObserver;

    assert.ok(observer, "controlled observer captured");

    observer.fire({ kind: "master", sequence: 2, url: "https://cdn.test/master-high.m3u8" });
    observer.fire({ kind: "master", sequence: 1, url: "https://cdn.test/master-low.m3u8" });

    interceptor.finalize(true);

    const result = await interceptor.promise;

    assert.ok(result, "promise resolved with a result");
    assert.equal(result.manifestUrl, "https://cdn.test/master-low.m3u8", "the first slot holds the lower-sequenced (earlier wire arrival) master");
    assert.equal(result.selectedKind, "master");
  });

  test("the latest slot is not displaced by a late-delivered lower-sequenced master", async () => {

    // Wire-order latest-slot guard: a master with ordinal 2 is delivered, then one with ordinal 1. The latest slot must keep the higher ordinal. Marking before
    // any observation makes the ordinal-2 master post-epoch, so it wins categorically - a proof the latest slot still holds it.
    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const interceptor = await install();

      assert.ok(interceptor, "interceptor installed");

      const observer = mockPendingObserver;

      assert.ok(observer, "controlled observer captured");

      interceptor.markChannelSelectionStart();
      observer.fire({ kind: "master", sequence: 2, url: "https://cdn.test/master-high.m3u8" });
      observer.fire({ kind: "master", sequence: 1, url: "https://cdn.test/master-low.m3u8" });

      interceptor.finalize(false);
      mock.timers.tick(1600);

      const result = await interceptor.promise;

      assert.ok(result, "promise resolved with a result");
      assert.equal(result.manifestUrl, "https://cdn.test/master-high.m3u8", "the late lower-sequenced master did not displace the higher-sequenced latest fact");
      assert.equal(result.selectedKind, "master");
    } finally {

      mock.timers.reset();
    }
  });

  test("re-marking moves the epoch past a second master so a later foreign media wins (latest stamp wins)", async () => {

    // A master arrives, the epoch is stamped, a second master arrives, then the epoch is re-stamped past it, then a post-mark live foreign media arrives. Under
    // latest-stamp-wins the second master is pre-epoch and the media wins; a set-once-mark bug would leave the second master post-epoch and resolve it instead,
    // so the outcomes diverge and this pins the re-mark semantics.
    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const interceptor = await install();

      assert.ok(interceptor, "interceptor installed");

      const observer = mockPendingObserver;

      assert.ok(observer, "controlled observer captured");

      observer.fire({ childUrls: PAGE_MASTER_CHILDREN, kind: "master", url: "https://guide.test/default-chan/master-1.m3u8" });
      interceptor.markChannelSelectionStart();
      observer.fire({ childUrls: PAGE_MASTER_CHILDREN, kind: "master", url: "https://guide.test/default-chan/master-2.m3u8" });
      interceptor.markChannelSelectionStart();
      observer.fire({ kind: "media", live: true, url: CLICKED_MEDIA });

      interceptor.finalize(false);
      mock.timers.tick(1600);

      const result = await interceptor.promise;

      assert.ok(result, "promise resolved with a result");
      assert.equal(result.manifestUrl, CLICKED_MEDIA, "the re-mark pushed the second master pre-epoch, so the later foreign media wins");
      assert.equal(result.selectedKind, "media");
    } finally {

      mock.timers.reset();
    }
  });

  test("the epoch-free timeout resolves the master on a record where finalize would have resolved the media", async () => {

    // The defensive timeout resolves epoch-free, so on a diverging record - a pre-epoch master plus a post-epoch live non-member media - it resolves the MASTER,
    // whereas finalize(false) with the epoch present would resolve the media. The two resolutions differ, which is what makes this discriminate the epoch-free
    // path (a direct tune the timeout may be guarding must never reach the epoch rule).
    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const interceptor = await install();

      assert.ok(interceptor, "interceptor installed");

      const observer = mockPendingObserver;

      assert.ok(observer, "controlled observer captured");

      observer.fire({ childUrls: PAGE_MASTER_CHILDREN, kind: "master", url: PAGE_MASTER });
      interceptor.markChannelSelectionStart();
      observer.fire({ kind: "media", live: true, url: CLICKED_MEDIA });

      // Do NOT finalize. Advance past the interception window so the epoch-free timeout resolves the record.
      mock.timers.tick(15100);

      const result = await interceptor.promise;

      assert.ok(result, "promise resolved with a result");
      assert.equal(result.manifestUrl, PAGE_MASTER, "the epoch-free timeout resolves the master, unlike the epoch-present finalize path");
      assert.equal(result.selectedKind, "master");
    } finally {

      mock.timers.reset();
    }
  });
});

describe("awaitMatchingManifest", () => {

  test("returns null when the underlying HLS observer fails to install", async () => {

    // Boundary: same propagation contract as installManifestInterceptor - install failure surfaces as null.
    mockInstallShouldFail = true;

    const result = await awaitMatch(() => true, 100);

    assert.equal(result, null, "install failure surfaces as null");
  });

  test("resolves with the first master URL whose predicate returns true", async () => {

    // The predicate is consulted only for master playlists. We feed two masters; only the second matches the predicate, and the function must resolve with
    // that URL.
    const interceptor = awaitMatch((url) => url.includes("target"), 1000);

    // Yield to the microtask queue so the observer is installed before we drive observations.
    await Promise.resolve();

    const observer = mockPendingObserver;

    assert.ok(observer, "controlled observer captured");

    observer.fire({ kind: "master", url: "https://cdn.test/wrong.m3u8" });
    observer.fire({ kind: "master", url: "https://cdn.test/target-channel.m3u8" });

    const result = await interceptor;

    assert.equal(result, "https://cdn.test/target-channel.m3u8", "predicate match wins");
    assert.equal(observer.disposed, true, "observer disposed on match");
  });

  test("ignores media playlists - predicate is not consulted for non-master kinds", async () => {

    // Tune verification is a multi-channel concept that only applies to master playlists. The predicate must never see a media URL even if the URL would
    // textually match - this is the kind-filter contract. We use a predicate that returns true for any URL containing "match" and fire a media URL containing
    // exactly that token; if the orchestrator leaked the media observation through to the predicate, the predicate would match and the function would resolve
    // with the wrong URL. After the media fire, we feed a master URL whose path also matches so the function resolves quickly rather than waiting for the
    // timeout, and assert (a) the predicate was called exactly once - for the master - and (b) the returned URL is the master URL, not the media URL.
    let predicateCalls = 0;
    const recordedUrls: string[] = [];

    const interceptor = awaitMatch((url) => {

      predicateCalls++;
      recordedUrls.push(url);

      return url.includes("match");
    }, 1000);

    await Promise.resolve();

    const observer = mockPendingObserver;

    assert.ok(observer, "controlled observer captured");

    // Fire a media URL whose path WOULD satisfy the predicate if the kind filter were missing. The orchestrator must drop this without consulting the predicate.
    observer.fire({ kind: "media", live: true, url: "https://cdn.test/media-match.m3u8" });

    // Fire a master URL that also satisfies the predicate. This is the URL the function should resolve with.
    observer.fire({ kind: "master", url: "https://cdn.test/master-match.m3u8" });

    const result = await interceptor;

    assert.equal(predicateCalls, 1, "predicate consulted exactly once - the kind filter dropped the media observation before any predicate evaluation");
    assert.deepEqual(recordedUrls, ["https://cdn.test/master-match.m3u8"], "predicate saw only the master URL even though the media URL would have matched");
    assert.equal(result, "https://cdn.test/master-match.m3u8", "function resolves with the master URL, not the media URL that textually matched the predicate");
  });

  test("resolves null when the timeout elapses without a predicate match", async () => {

    // Negative path: the predicate keeps returning false, the timer elapses, the function resolves null. The observer must be disposed on the timeout path.
    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const interceptor = awaitMatch(() => false, 200);

      await Promise.resolve();

      const observer = mockPendingObserver;

      assert.ok(observer, "controlled observer captured");

      // Feed a master that the predicate rejects.
      observer.fire({ kind: "master", url: "https://cdn.test/no-match.m3u8" });

      // Advance past the timeout.
      mock.timers.tick(250);

      const result = await interceptor;

      assert.equal(result, null, "timeout without match resolves null");
      assert.equal(observer.disposed, true, "observer disposed on timeout");
    } finally {

      mock.timers.reset();
    }
  });
});
