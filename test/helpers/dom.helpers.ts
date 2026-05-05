/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * dom.helpers.ts: Foundation for the DOM-runtime test tier under test/e2e/dom-runtime/. Where the integration tier exercises production routes end-to-end at
 * the HTTP boundary, the DOM-runtime tier exercises the EMITTED CLIENT-SIDE JAVASCRIPT - the strings that PrismCast ships to the browser as inline <script>
 * blocks. Unit tests pin "the generator emits a string containing X"; this tier pins "when that emitted X runs in a DOM, it does Y."
 *
 * Architecture:
 *
 *   1. Boot a per-test integration context (temp data dir + LIFO cleanup).
 *   2. Boot the production-shaped Express app on an ephemeral port (bootApp). This is the same listener the real client would talk to - fetch calls from
 *      the synthetic DOM hit real production routes, persisting state to the temp data dir.
 *   3. Fetch the served HTML from the bootApp at the requested path.
 *   4. Construct a happy-dom Window with disableJavaScriptEvaluation: true. The Window's URL is pointed at the bootApp listener so relative-URL fetch calls
 *      from the emitted scripts resolve against the production server.
 *   5. Load the served HTML into the Window. With script evaluation disabled, the parser creates <script> elements but does not execute them; the harness
 *      extracts the inline bodies into an array indexed in document order.
 *   6. Tests opt in to which scripts to run via runScripts(predicate) or evaluate(code). This is deliberate: the emitted scripts have side-effecting init
 *      code (status.ts opens an EventSource, channels.ts kicks off DOM scans) and EventSource is one happy-dom v20 web standard not implemented. Default-
 *      executing every script would crash before the test body runs. Tests pick the surface they want to exercise.
 *
 * Why happy-dom and not jsdom: shared.ts's DOM usage is narrow and standard (querySelector, classList, addEventListener, getBoundingClientRect, localStorage,
 * fetch). happy-dom resolves these ~3-5x faster than jsdom and has no Custom Elements or Shadow DOM dependencies that would require jsdom's wider coverage.
 * The one happy-dom gap (EventSource) is sidestepped by selective script execution rather than papered over with a stub - papering over would mask real
 * regressions in status.ts. When the next session covers status.ts, the harness can opt-in to a polyfilled EventSource at that point.
 *
 * Disposal: composed from createIntegrationContext. The Window is registered as a cleanup hook so it closes BEFORE the bootApp listener tears down - happy-dom
 * has open async tasks (microtasks, timers) that need to settle before the surrounding context completes. The disposal protocol drains LIFO via the structural
 * Symbol.asyncDispose contract of the inner integration context.
 */
import type { BootedApp, DisposableIntegrationContext } from "./integration.helpers.ts";
import { bootApp, createIntegrationContext, initializePersistence } from "./integration.helpers.ts";
import type { Document } from "happy-dom";
import { Window } from "happy-dom";

/**
 * Inline <script> block extracted from the served page HTML. Carries the source text plus the document-order index, which is the stable identifier the script
 * filter uses to opt scripts in or out of execution. external-src scripts are not represented here - the harness ignores them on the assumption that PrismCast
 * inlines all client-side JavaScript (if that ever changes, the harness gains a separate field for src-attribute scripts).
 */
export interface ExtractedScript {

  /** The textContent of the <script> tag - the JavaScript body that would execute in a real browser. */
  readonly content: string;

  /** Position of the script in the served HTML, in document order (0-based). Stable identifier across runs because the server's render order is deterministic. */
  readonly index: number;
}

/**
 * Options for createDomTestContext. All fields optional; defaults reflect the most common case (load the landing page, do not auto-execute scripts because the
 * shared utilities script - the most common subject of DOM-runtime tests - is initialized via runScripts at the test site).
 */
export interface DomTestContextOptions {

  /** Path on the bootApp to load. Defaults to "/" - the landing page. */
  readonly path?: string;

  /** When true, skip initializePersistence. Use only for tests that exercise routes which don't read user state (rare). Default false. */
  readonly skipPersistence?: boolean;
}

/**
 * Per-test context for DOM-runtime tests. Composes the integration context with a synthetic DOM, the bootApp listener that the synthetic DOM talks to, and the
 * extracted inline scripts the page HTML contained. Tests interact with the Window directly (window.document, window.localStorage, etc.) and run scripts via
 * runScripts/evaluate.
 *
 * The Window and Document fields are typed via happy-dom's exports. happy-dom's types diverge from lib.dom in places (its Window does not extend the global
 * Window interface), so tests that need a specific lib.dom type for a return value cast at the assertion site rather than fighting a structural type lift.
 */
export interface DomTestContext {

  /** The bootApp Express application. Use to mount additional middleware in tests that need it. */
  readonly app: BootedApp["app"];

  /** Absolute path to the per-test temp data directory. Production paths resolver points here for the duration of the binding scope. */
  readonly dataDir: string;

  /** The synthetic DOM Document. Tests query/mutate the DOM through this surface. */
  readonly document: Document;

  /** The full HTML response from the bootApp at the requested path. Held so tests can re-extract scripts or assert on raw HTML if needed. */
  readonly html: string;

  /** Bound port number of the bootApp listener (ephemeral, OS-assigned). */
  readonly port: number;

  /** Inline <script> bodies extracted from the served HTML, in document order. Tests pick which to run via runScripts(). */
  readonly scripts: readonly ExtractedScript[];

  /** Composes a full URL against the bootApp listener for the given path. Convenience over manual port string concatenation. */
  readonly urlFor: (path: string) => string;

  /** The synthetic DOM Window. Carries window.fetch (resolves relative URLs against the bootApp), window.localStorage, etc. */
  readonly window: Window;

  /**
   * Evaluates JavaScript in the Window's global scope. Use to bootstrap test fixtures (e.g., setting window.somePreset before running a script that depends on
   * it) or to inspect post-execution state via expressions like ctx.evaluate("typeof window.channelTable").
   * @param code - JavaScript source text to evaluate.
   * @returns The completion value of the expression, as returned by happy-dom's eval implementation.
   */
  evaluate(code: string): unknown;

  /**
   * Evaluates JavaScript and round-trips the result through JSON.stringify (in the happy-dom sandbox) and JSON.parse (in the test process). Use this whenever
   * the expression returns an object or array - happy-dom's VM sandbox uses its own Array/Object prototypes, which fail Node's deepStrictEqual reference checks
   * even when the structure matches. JSON round-tripping returns plain Node objects, so deepEqual works as expected.
   *
   * Limitations: anything JSON.stringify drops (functions, undefined, symbols, circular references) is lost in the round-trip. For those cases, evaluate
   * primitive properties one at a time via evaluate(), or assemble an inspection-friendly object inside the eval expression.
   *
   * @param code - JavaScript expression whose result is JSON-serializable. The harness wraps this in JSON.stringify before evaluating.
   * @returns The deserialized value as `unknown`. Callers narrow at the assertion site, mirroring readPersistedJson's contract.
   */
  evaluateJson(code: string): unknown;

  /**
   * Drains happy-dom's pending microtasks and timers via window.happyDOM.waitUntilComplete(). Use after triggering any operation that returns a Promise from
   * inside the sandbox (async controllers, fetch handlers, deferred renders) and before asserting on the post-resolution state. Without this drain, the
   * sandbox's microtask queue is still pending when the test process advances to its next synchronous line, and assertions read pre-resolution values.
   */
  flushAsync(): Promise<void>;

  /**
   * Registers a cleanup function that runs at disposal. Cleanups drain LIFO so resources acquired later tear down before their dependencies. Pass-through to
   * the underlying integration context's registry.
   * @param fn - Cleanup callback. May be sync or async; both are awaited.
   */
  registerCleanup(fn: () => Promise<void> | void): void;

  /**
   * Executes the extracted inline scripts whose entry satisfies the predicate, in document order. Each script's body runs in the Window's global scope so any
   * window.* assignments persist across scripts and become visible to subsequent test assertions.
   *
   * Returns the indices of the scripts that ran, so tests can assert "exactly the shared-utilities script ran, nothing else" without re-deriving the index list.
   *
   * @param predicate - Selector for which scripts to run. Receives the extracted entry and its document-order index. Common patterns:
   *   - `(s) => s.content.includes("window.channelTable")` runs the shared-utilities script.
   *   - `(s) => !s.content.includes("EventSource")` runs everything except status.ts.
   *   - `() => true` runs every inline script (only safe if the test does not transitively depend on EventSource).
   * @returns Array of the indices of executed scripts.
   */
  runScripts(predicate: (script: ExtractedScript, index: number) => boolean): number[];
}

/** Disposable variant. Returned by createDomTestContext; bind via `await using` for structural cleanup. */
export type DisposableDomTestContext = DomTestContext & AsyncDisposable;

/**
 * Provisions a fresh DOM-runtime context: integration context + bootApp + happy-dom Window loaded with the served page HTML. Disposal closes the Window first,
 * then the listener, then removes the temp dir, draining the inner integration context's cleanup queue in LIFO order.
 *
 * Why we don't auto-execute the served scripts: the emitted scripts have side-effecting init code (status.ts opens EventSource on load; happy-dom does not
 * implement EventSource). Executing every script by default would crash the harness before the test body could run. Tests opt in to which scripts to run via
 * runScripts(), which is the right scope for behavioral tests anyway - they should pin the runtime behavior of the script under test, not collateral effects
 * from other scripts loading alongside it. A future status.ts session can layer in an EventSource polyfill at the harness level if desired.
 *
 * @param options - Optional path / persistence-bypass overrides. See DomTestContextOptions.
 * @returns A disposable DOM-runtime context. Bind via `await using ctx = await createDomTestContext()`.
 */
export async function createDomTestContext(options?: DomTestContextOptions): Promise<DisposableDomTestContext> {

  const ctx: DisposableIntegrationContext = await createIntegrationContext();

  if(!options?.skipPersistence) {

    await initializePersistence(ctx);
  }

  const booted = await bootApp(ctx);
  const path = options?.path ?? "/";

  // Fetch the page HTML directly. We do this through Node's native fetch (not happy-dom's window.fetch) so the response handling is straightforward and we
  // capture the text before constructing the Window. The Window's URL is pointed at the bootApp so subsequent in-page fetch calls (made by emitted client
  // scripts) resolve relative paths against the same listener.
  const response = await fetch(booted.urlFor(path));

  if(!response.ok) {

    throw new Error("createDomTestContext: bootApp returned " + String(response.status) + " for " + path + "; cannot proceed without page HTML.");
  }

  const html = await response.text();

  // Construct the happy-dom Window with script evaluation disabled. The parser will still create <script> elements when we set the HTML, but their bodies
  // do not execute automatically - leaving the test in control of which scripts run. The url field is what window.location.origin resolves to and what
  // window.fetch uses to resolve relative URLs.
  const window = new Window({

    settings: {

      disableJavaScriptEvaluation: true
    },
    url: booted.urlFor("/")
  });

  // Register Window close as a cleanup hook BEFORE we trigger any DOM operations. happy-dom's Window has internal async tasks (microtasks, timers) that
  // need to drain on close; happyDOM.close() handles that and also signals the test runner that this Window's vm sandbox can be reaped. Registered here so
  // it fires before bootApp's listener teardown (LIFO).
  ctx.registerCleanup(async () => {

    await window.happyDOM.close();
  });

  // Load the served HTML. We use document.write() because it accepts a full HTML document (DOCTYPE + html + head + body), parses it, and replaces the
  // current document tree in one call. With disableJavaScriptEvaluation: true, scripts are parsed but not executed, leaving us free to extract their bodies.
  window.document.write(html);

  // Extract inline <script> bodies in document order. We index sequentially regardless of whether a script has src= - the unindexed src-only scripts are
  // dropped, which is correct for PrismCast (every emitted script is inline). If a future change introduces external scripts the harness would gain a
  // separate field for them.
  const scripts: ExtractedScript[] = [];
  const scriptElements = window.document.querySelectorAll("script");
  let extractIndex = 0;

  for(const el of scriptElements) {

    // Skip src= scripts - they would not be inline, and PrismCast does not emit them today. If/when the page introduces a CDN-loaded script the harness
    // gains a parallel array; for now we silently filter so the indices match what tests expect.
    if(el.getAttribute("src")) {

      continue;
    }

    scripts.push({ content: el.textContent, index: extractIndex++ });
  }

  return {

    app: booted.app,
    dataDir: ctx.dataDir,
    document: window.document,
    evaluate(code): unknown {

      return window.eval(code);
    },
    evaluateJson(code: string): unknown {

      // JSON.stringify inside the sandbox produces a primitive string that crosses the vm boundary cleanly. Parse on this side to land a plain Node value.
      const serialized = window.eval("JSON.stringify(" + code + ")") as string | undefined;

      // happy-dom returns undefined when the expression itself evaluates to undefined; mirror JSON.parse(undefined) by returning undefined to the caller rather
      // than throwing. Tests that need to disambiguate "undefined" from "null" can call evaluate() directly.
      if(serialized === undefined) {

        return undefined;
      }

      return JSON.parse(serialized);
    },
    async flushAsync(): Promise<void> {

      await window.happyDOM.waitUntilComplete();
    },
    html,
    port: booted.port,
    registerCleanup(fn): void {

      ctx.registerCleanup(fn);
    },
    runScripts(predicate): number[] {

      const ran: number[] = [];

      for(const script of scripts) {

        if(predicate(script, script.index)) {

          window.eval(script.content);
          ran.push(script.index);
        }
      }

      return ran;
    },
    scripts,
    urlFor: booted.urlFor,
    window,

    async [Symbol.asyncDispose](): Promise<void> {

      // Delegate to the inner integration context's disposer. The Window close cleanup is already registered there (LIFO), so it fires first, then bootApp's
      // listener.close, then temp dir removal.
      await ctx[Symbol.asyncDispose]();
    }
  };
}
