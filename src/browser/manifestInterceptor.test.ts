/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * manifestInterceptor.test.ts: Unit tests for the installManifestInterceptor and awaitMatchingManifest state machines - the main consumer-facing surface of
 * manifestInterceptor.ts. These exercise the orchestration on top of the HLS playlist observer: finalize-direct-vs-guide semantics, the settle-delay wait,
 * master-priority arbitration, timeout safety nets, dispose paths, and TC39 using-syntax integration. The pure selectInterceptedManifest helper that backs
 * finalize()'s resolution is tested in the companion file manifestInterceptor.selection.test.ts.
 *
 * Why mock.module + dynamic import. The orchestrators sit one layer above hlsPlaylistObserver.observeHlsPlaylists(), which itself sits above the tab network
 * observer + CDP. Driving the real lower layers would require a Puppeteer browser. mock.module is the canonical seam for swapping the dependency without
 * touching production code; we substitute a controlled observer that captures the onPlaylist callback so the test can synthesize playlist observations on demand.
 * A static import of manifestInterceptor.ts at the top of the file would bind the real observeHlsPlaylists before the mock could register, so the import is
 * deferred to a dynamic import inside before().
 */
import type * as ManifestInterceptorModule from "./manifestInterceptor.ts";
import { before, beforeEach, describe, mock, test } from "node:test";
import type { HlsPlaylistKind } from "../native/probe.ts";
import type { Page } from "puppeteer-core";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

/* ControlledObserver is the test-side handle returned by the mocked observeHlsPlaylists. It captures the onPlaylist callback at construction so tests can call
 * fire() to synthesize a playlist observation, and exposes a read-only disposed flag so tests can assert the disposal lifecycle. dispose() and Symbol.dispose
 * are the same function reference to match the real observer's contract.
 */
interface ControlledObserver {

  // Read-only view of the observer's disposed state. Backed by a closure variable inside the factory so the property is observable without permitting outside
  // mutation - tests assert on it but cannot flip it from the outside.
  readonly disposed: boolean;

  readonly dispose: () => void;

  // Fires a synthetic playlist observation through the captured onPlaylist callback. No-op when the observer has already been disposed.
  readonly fire: (kind: HlsPlaylistKind, url: string) => void;

  readonly [Symbol.dispose]: () => void;
}

/* MockObserveOptions mirrors the shape of the real observeHlsPlaylists option bag at the surface our mock cares about. Promoted to module scope rather than
 * declared inside before() so it can be referenced anywhere in the file and is grep-discoverable alongside the other types.
 */
interface MockObserveOptions {

  onPlaylist: (playlist: { kind: HlsPlaylistKind; url: string }) => void;
}

/* createControlledObserver constructs a fresh test-side observer. The disposed state lives in a closure variable so it cannot be mutated from outside the
 * factory; the public surface exposes it as a getter. dispose() and Symbol.dispose are the same function reference, satisfying the production observer's
 * identity contract.
 */
function createControlledObserver(options: MockObserveOptions): ControlledObserver {

  let disposed = false;

  const dispose = (): void => {

    disposed = true;
  };

  return {

    [Symbol.dispose]: dispose,
    dispose,
    get disposed(): boolean {

      return disposed;
    },
    fire: (kind: HlsPlaylistKind, url: string): void => {

      if(!disposed) {

        options.onPlaylist({ kind, url });
      }
    }
  };
}

// Per-test mock state. installShouldFail makes the next call to observeHlsPlaylists return null (simulating the underlying tab observer failing to install);
// pendingObserver receives the ControlledObserver created on each successful call so tests can drive it. Declared as separate `let` bindings rather than fields
// on a container object so per-field mutability is explicit at the declaration site.
let mockInstallShouldFail = false;
let mockPendingObserver: ControlledObserver | null = null;

let installManifestInterceptor: typeof ManifestInterceptorModule.installManifestInterceptor;
let awaitMatchingManifest: typeof ManifestInterceptorModule.awaitMatchingManifest;

before(async () => {

  const moduleUrl = new URL("./hlsPlaylistObserver.ts", import.meta.url).href;

  // The mocked observeHlsPlaylists factory: returns null when mockInstallShouldFail is set, otherwise constructs a ControlledObserver, parks it in
  // mockPendingObserver so the test can drive it, and returns it as the observer handle.
  const observeHlsPlaylists = async (_page: Page, options: MockObserveOptions): Promise<ControlledObserver | null> => {

    if(mockInstallShouldFail) {

      return null;
    }

    const observer = createControlledObserver(options);

    mockPendingObserver = observer;

    return observer;
  };

  // The Node 22 type definitions surface the option as namedExports; the runtime renamed it to exports in a later minor and emits a deprecation warning. We
  // keep namedExports until @types/node catches up - the runtime path is unaffected and the type definition is authoritative for the build. Same precedent as
  // streaming/hls.loginMode.test.ts and config/persistence.integrity.test.ts.
  mock.module(moduleUrl, {

    namedExports: {

      observeHlsPlaylists
    }
  });

  // After the mock is in place, dynamic-import manifestInterceptor.ts so its captured observeHlsPlaylists binding points at the mock. A static import at the
  // top of this file would resolve before the mock was registered.
  const mod = await import("./manifestInterceptor.ts");

  installManifestInterceptor = mod.installManifestInterceptor;
  awaitMatchingManifest = mod.awaitMatchingManifest;
});

beforeEach(() => {

  // Reset shared mock state so a prior test's setup cannot leak into the next test's setup. Each test that needs install failure sets mockInstallShouldFail
  // explicitly; mockPendingObserver is overwritten on each successful install.
  mockInstallShouldFail = false;
  mockPendingObserver = null;
});

// fakePage is a Page stub that satisfies the orchestrators' parameter type. The orchestrators never call methods on the page directly - they pass it through to
// observeHlsPlaylists, which in our mock ignores it. A minimal cast suffices.
const fakePage = {} as unknown as Page;

describe("installManifestInterceptor", () => {

  test("returns null when the underlying HLS observer fails to install", async () => {

    // Boundary: the orchestrator is a strict layer on top of observeHlsPlaylists. If installation fails, the orchestrator propagates the failure rather than
    // returning a handle whose promise would never resolve.
    mockInstallShouldFail = true;

    const interceptor = await installManifestInterceptor(fakePage);

    assert.equal(interceptor, null, "install failure surfaces as null");
  });

  test("finalize(true) with a master URL already captured resolves immediately with that URL", async () => {

    // The direct-tune fast path: when the navigated URL itself selects the channel and the player has already loaded the master manifest, finalize(true) must
    // settle the promise without waiting the FINALIZE_SETTLE_DELAY. The first-master-URL-wins selection rule is what this test pins.
    const interceptor = await installManifestInterceptor(fakePage);

    assert.ok(interceptor, "interceptor installed");

    const observer = mockPendingObserver;

    assert.ok(observer, "controlled observer captured");

    observer.fire("master", "https://cdn.test/first-master.m3u8");

    interceptor.finalize(true);

    const result = await interceptor.promise;

    assert.ok(result, "promise resolved with a result");
    assert.equal(result.masterManifestUrl, "https://cdn.test/first-master.m3u8", "first master URL selected");
    assert.equal(observer.disposed, true, "observer disposed on settle");
  });

  test("finalize(false) waits the settle delay and resolves with the latest URL (guide-tune semantics)", async () => {

    // Guide tune: the channel-switch click may trigger a fresh manifest fetch that arrives milliseconds after the click handler returns. finalize(false) waits
    // the settle delay before resolving so a late-arriving newer manifest wins over the page's default-channel manifest.
    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const interceptor = await installManifestInterceptor(fakePage);

      assert.ok(interceptor, "interceptor installed");

      const observer = mockPendingObserver;

      assert.ok(observer, "controlled observer captured");

      observer.fire("master", "https://cdn.test/first-master.m3u8");

      interceptor.finalize(false);

      // The settle delay is 1500ms; a newer manifest arrives 500ms into the wait.
      mock.timers.tick(500);
      observer.fire("master", "https://cdn.test/latest-master.m3u8");

      // Advance past the remaining settle window.
      mock.timers.tick(1100);

      const result = await interceptor.promise;

      assert.ok(result, "promise resolved with a result");
      assert.equal(result.masterManifestUrl, "https://cdn.test/latest-master.m3u8", "latest master URL wins on guide tune");
    } finally {

      mock.timers.reset();
    }
  });

  test("master URL outranks media URL on a guide tune (master priority across kinds)", async () => {

    // Master priority: even if a media playlist arrives later than a master, the master wins because it carries richer metadata. This locks the rule that
    // selectInterceptedManifest applies at resolution time.
    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const interceptor = await installManifestInterceptor(fakePage);

      assert.ok(interceptor, "interceptor installed");

      const observer = mockPendingObserver;

      assert.ok(observer, "controlled observer captured");

      observer.fire("master", "https://cdn.test/master.m3u8");
      observer.fire("media", "https://cdn.test/late-media.m3u8");

      interceptor.finalize(false);

      mock.timers.tick(1600);

      const result = await interceptor.promise;

      assert.ok(result, "promise resolved with a result");
      assert.equal(result.masterManifestUrl, "https://cdn.test/master.m3u8", "master wins over later-arriving media");
    } finally {

      mock.timers.reset();
    }
  });

  test("falls back to media URL when no master ever arrives (media-only sites)", async () => {

    // Media-only direct tune: a site whose player loads only a media playlist (e.g., Angelcam from issue #34) must still resolve. The media URL is selected
    // because no master is available; the settle delay applies because the direct-tune fast path only short-circuits when a master is already captured.
    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const interceptor = await installManifestInterceptor(fakePage);

      assert.ok(interceptor, "interceptor installed");

      const observer = mockPendingObserver;

      assert.ok(observer, "controlled observer captured");

      observer.fire("media", "https://cdn.test/only-media.m3u8");

      interceptor.finalize(true);

      mock.timers.tick(1600);

      const result = await interceptor.promise;

      assert.ok(result, "promise resolved with a result");
      assert.equal(result.masterManifestUrl, "https://cdn.test/only-media.m3u8", "media URL selected when no master arrived");
    } finally {

      mock.timers.reset();
    }
  });

  test("timeout safety net resolves with the latest captured URL when finalize is never called", async () => {

    // Defensive contract: if a caller forgets to invoke finalize, the timeout fires at INTERCEPTION_TIMEOUT (default 15000ms) and the promise resolves with
    // whatever was captured. This prevents an interceptor from hanging the calling code if the lifecycle hand-off goes wrong upstream.
    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const interceptor = await installManifestInterceptor(fakePage);

      assert.ok(interceptor, "interceptor installed");

      const observer = mockPendingObserver;

      assert.ok(observer, "controlled observer captured");

      observer.fire("master", "https://cdn.test/captured.m3u8");

      // Do NOT call finalize. Advance past the default INTERCEPTION_TIMEOUT.
      mock.timers.tick(15100);

      const result = await interceptor.promise;

      assert.ok(result, "timeout still resolves with the captured URL");
      assert.equal(result.masterManifestUrl, "https://cdn.test/captured.m3u8", "latest URL selected on timeout (mirrors guide-tune semantics)");
    } finally {

      mock.timers.reset();
    }
  });

  test("timeout with no captured manifest resolves null and disposes the observer", async () => {

    // Negative path: no manifest captured, no finalize, timeout fires. The promise must resolve null (rather than hang) and the observer must be disposed so
    // resources are reclaimed.
    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const interceptor = await installManifestInterceptor(fakePage);

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
    const interceptor = await installManifestInterceptor(fakePage);

    assert.ok(interceptor, "interceptor installed");

    const observer = mockPendingObserver;

    assert.ok(observer, "controlled observer captured");

    observer.fire("master", "https://cdn.test/captured.m3u8");

    interceptor.dispose();

    const result = await interceptor.promise;

    assert.equal(result, null, "dispose resolves the promise with null even when captures exist");
    assert.equal(observer.disposed, true, "underlying observer disposed");
  });

  test("dispose() after finalize is a safe no-op (idempotent lifecycle)", async () => {

    // Boundary: the cleanup paths in PrismCast can invoke dispose from multiple code paths. After finalize has already settled the promise, dispose must not
    // throw, must not re-resolve, and must not re-dispose the observer.
    const interceptor = await installManifestInterceptor(fakePage);

    assert.ok(interceptor, "interceptor installed");

    const observer = mockPendingObserver;

    assert.ok(observer, "controlled observer captured");

    observer.fire("master", "https://cdn.test/captured.m3u8");

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
    const interceptor = await installManifestInterceptor(fakePage);

    assert.ok(interceptor, "interceptor installed");
    assert.equal(typeof interceptor[Symbol.dispose], "function", "Symbol.dispose hook present");
    assert.equal(interceptor[Symbol.dispose], interceptor.dispose, "Symbol.dispose is the same function reference as dispose");

    interceptor.dispose();
  });

  test("the using keyword triggers disposal at scope exit (normal path)", async () => {

    // End-to-end TC39 ERM contract: at scope exit, V8/Node invokes Symbol.dispose, which calls dispose(), which resolves the pending promise with null and
    // tears down the observer. We capture the promise outside the using scope so it can be awaited after disposal.
    let capturedPromise!: Promise<ManifestInterceptorModule.ManifestInterceptionResult | null>;
    let capturedObserver!: ControlledObserver;

    {

      using interceptor = await installManifestInterceptor(fakePage);

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

    // Exception-safety contract: TC39 ERM guarantees disposal on the throw path. This is the load-bearing reason to use Symbol.dispose at all - otherwise an
    // explicit dispose() call inside a finally block would suffice.
    let capturedPromise!: Promise<ManifestInterceptorModule.ManifestInterceptionResult | null>;
    let capturedObserver!: ControlledObserver;

    await assert.rejects(async () => {

      using interceptor = await installManifestInterceptor(fakePage);

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

describe("awaitMatchingManifest", () => {

  test("returns null when the underlying HLS observer fails to install", async () => {

    // Boundary: same propagation contract as installManifestInterceptor - install failure surfaces as null.
    mockInstallShouldFail = true;

    const result = await awaitMatchingManifest(fakePage, () => true, 100);

    assert.equal(result, null, "install failure surfaces as null");
  });

  test("resolves with the first master URL whose predicate returns true", async () => {

    // The predicate is consulted only for master playlists. We feed two masters; only the second matches the predicate, and the function must resolve with
    // that URL.
    const interceptor = awaitMatchingManifest(fakePage, (url) => url.includes("target"), 1000);

    // Yield to the microtask queue so the observer is installed before we drive observations.
    await Promise.resolve();

    const observer = mockPendingObserver;

    assert.ok(observer, "controlled observer captured");

    observer.fire("master", "https://cdn.test/wrong.m3u8");
    observer.fire("master", "https://cdn.test/target-channel.m3u8");

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

    const interceptor = awaitMatchingManifest(fakePage, (url) => {

      predicateCalls++;
      recordedUrls.push(url);

      return url.includes("match");
    }, 1000);

    await Promise.resolve();

    const observer = mockPendingObserver;

    assert.ok(observer, "controlled observer captured");

    // Fire a media URL whose path WOULD satisfy the predicate if the kind filter were missing. The orchestrator must drop this without consulting the predicate.
    observer.fire("media", "https://cdn.test/media-match.m3u8");

    // Fire a master URL that also satisfies the predicate. This is the URL the function should resolve with.
    observer.fire("master", "https://cdn.test/master-match.m3u8");

    const result = await interceptor;

    assert.equal(predicateCalls, 1, "predicate consulted exactly once - the kind filter dropped the media observation before any predicate evaluation");
    assert.deepEqual(recordedUrls, ["https://cdn.test/master-match.m3u8"], "predicate saw only the master URL even though the media URL would have matched");
    assert.equal(result, "https://cdn.test/master-match.m3u8", "function resolves with the master URL, not the media URL that textually matched the predicate");
  });

  test("resolves null when the timeout elapses without a predicate match", async () => {

    // Negative path: the predicate keeps returning false, the timer elapses, the function resolves null. The observer must be disposed on the timeout path.
    mock.timers.enable({ apis: ["setTimeout"] });

    try {

      const interceptor = awaitMatchingManifest(fakePage, () => false, 200);

      await Promise.resolve();

      const observer = mockPendingObserver;

      assert.ok(observer, "controlled observer captured");

      // Feed a master that the predicate rejects.
      observer.fire("master", "https://cdn.test/no-match.m3u8");

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
