/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * tabNetworkObserver.ts: Tab-wide CDP Network observation across all renderer processes (OOPIF-aware).
 */
import type { CDPSession, Page } from "puppeteer-core";
import { LOG } from "../utils/index.ts";
import type { Nullable } from "../types/index.ts";

/* This module exposes one transport primitive: observeTabResponses(). Its job is to deliver every HTTP response observed in a tab to a single callback, regardless
 * of which renderer process serves them. A tab in modern Chrome is not one renderer - Site Isolation places cross-site iframes in separate processes (OOPIFs), and
 * each process emits Network events through its own CDP session. Observing only the top-frame session, as a naive page.createCDPSession() does, silently misses
 * every cross-origin embed's network activity. This observer fixes that at the architectural level so no consumer ever has to think about it again.
 *
 * Mechanism. The observer opens a CDP session against the page target and sends Target.setAutoAttach with flatten=true. With flatten enabled, all child target
 * sessions multiplex onto the parent connection; the parent can resolve each child as a CDPSession via the parent's connection().session() lookup. When a child
 * attaches, the parent session fires Target.attachedToTarget; the observer responds by (a) enabling Network on the child session, (b) installing the same
 * response listener, and (c) recursively calling setAutoAttach on the child so its own descendants are caught. Symmetrically, Target.detachedFromTarget removes
 * the child from tracking. Target.attachedToTarget retroactively fires for currently-attached children when setAutoAttach is enabled on an existing tree, so
 * the observer does not miss frames that attached before installation - provided the listener is registered before setAutoAttach is sent. Reversing that order
 * means the retroactive attach events are missed, and attachToSession() depends on this ordering.
 *
 * Target type filtering. By default the observer attaches to every target except the browser-level target, because any of them (pages, iframes including OOPIFs,
 * workers, service workers, shared workers) can emit a network response a consumer might care about. The default policy is exposed as a parameterized predicate
 * so a consumer that needs a stricter filter (e.g., only iframes, or exclude service workers) can override without re-implementing the rest of the observer.
 *
 * Disposal. The handle implements both dispose() (project convention) and Symbol.dispose (TC39 explicit resource management) so callers can write either
 * "observer.dispose()" or "using observer = await observeTabResponses(...)" and get identical, repeat-safe teardown. Disposal removes listeners on every tracked
 * session, sends Network.disable best-effort, and detaches best-effort. Errors during teardown are swallowed because sessions may already be gone (page closed,
 * target detached) and that is not actionable.
 *
 * This module knows nothing about HLS. It is a generic tab-network observation primitive; the HLS-specific filtering and classification live in
 * hlsPlaylistObserver.ts which consumes this observer. Any future feature that needs tab-wide network visibility (license-server traffic analysis, ad-pixel
 * detection, etc.) is one consumer of this same primitive.
 */

/**
 * A network response observed anywhere in the tab's target tree. Carries the response URL, the headers as delivered by CDP, and the sessionId of the originating
 * target. The sessionId lets a consumer route follow-up CDP commands (e.g., Network.getResponseBody) to the correct session if it ever needs to; the default
 * consumer in hlsPlaylistObserver does not, since it fetches manifest bodies directly via Node fetch to avoid Chrome's response-body cache eviction.
 */
export interface ObservedResponse {

  // CDP-supplied response headers, lowercased keys per CDP convention. Empty object when CDP omits the headers field.
  readonly headers: Readonly<Record<string, string>>;

  // CDP session ID of the target that observed this response. The empty string for the root page session, per CDP convention. Useful for routing follow-up
  // commands to the originating session.
  readonly sessionId: string;

  // Absolute URL of the response.
  readonly url: string;
}

/**
 * Information about a target the observer encountered. Passed to the consumer-supplied targetFilter predicate so consumers can decide which target types to
 * observe. Modeled as a record rather than puppeteer's full TargetInfo so the predicate API is stable across puppeteer versions.
 */
export interface ObservedTargetInfo {

  // CDP target type ("page", "iframe", "service_worker", "worker", "shared_worker", "browser", etc.).
  readonly type: string;
}

/**
 * Options accepted by observeTabResponses(). The onResponse callback receives every response observed across every target accepted by targetFilter; consumers
 * apply their own URL or content-type filtering on top.
 */
export interface TabNetworkObserverOptions {

  // Invoked once per observed response. Synchronous; throw-safe callers should wrap their own logic.
  readonly onResponse: (response: ObservedResponse) => void;

  // Optional predicate deciding which target types to observe. Returns true to attach to the target, false to skip it. Defaults to "all types except browser"
  // because pages, iframes (including OOPIFs), workers, and service workers can all emit network responses a consumer might care about; the browser-level target
  // emits none. Callers needing stricter filtering (e.g., iframes only) override this rather than re-implementing the rest of the observer.
  readonly targetFilter?: (info: ObservedTargetInfo) => boolean;
}

/**
 * Handle returned by observeTabResponses(). Implements both the project's dispose() convention and TC39 Symbol.dispose so callers may use either an explicit
 * dispose() call or the "using" keyword for scope-bound cleanup. Disposal is safe to call more than once.
 */
export interface TabNetworkObserver extends Disposable {

  // Releases all tracked CDP sessions, removes listeners, and disables Network on each. Safe to call multiple times; subsequent calls are no-ops.
  readonly dispose: () => void;

  // TC39 explicit resource management hook. Aliases dispose() so "using observer = ..." produces deterministic teardown at scope exit, including on thrown errors.
  readonly [Symbol.dispose]: () => void;
}

// CDP Target events we listen to. Declared as standalone interfaces modeling the structural payloads CDP delivers so the handlers can be typed without leaning on
// puppeteer's internal Protocol types.
interface TargetAttachedParams {

  readonly sessionId: string;
  readonly targetInfo: { readonly targetId: string; readonly type: string };
  readonly waitingForDebugger: boolean;
}

interface TargetDetachedParams {

  readonly sessionId: string;
  readonly targetId?: string;
}

interface NetworkResponseReceivedParams {

  readonly response: { readonly headers?: Record<string, string>; readonly url: string };
}

// Default target filter: attach to every target except the browser-level one. The browser target emits no Network events relevant to a tab consumer, and
// attaching to it pollutes the session set. Defining the default at module scope makes it shareable across calls and trivially testable.
const defaultTargetFilter = (info: ObservedTargetInfo): boolean => info.type !== "browser";

/**
 * Installs a tab-wide network observer on the given page. The returned handle delivers every response observed across every target accepted by the targetFilter
 * predicate (top page, iframes including OOPIFs, workers, service workers by default) to the supplied callback. Disposal is the caller's responsibility; the
 * returned handle supports both an explicit dispose() call and the "using" keyword via Symbol.dispose.
 *
 * Returns null when the page is already closed, the root CDP session could not be created, or the puppeteer Connection backing the session is not available
 * (e.g., the session detached during construction).
 *
 * @param page - The puppeteer page to observe.
 * @param options - Observer options including the response callback and optional target-type predicate.
 * @returns The observer handle, or null if installation failed.
 */
export async function observeTabResponses(page: Page, options: TabNetworkObserverOptions): Promise<Nullable<TabNetworkObserver>> {

  if(page.isClosed()) {

    return null;
  }

  let rootSession: CDPSession;

  try {

    rootSession = await page.createCDPSession();
  } catch(error) {

    LOG.debug("browser:tabObserver", "Failed to create root CDP session: %s.", String(error));

    return null;
  }

  // Connection lookup. With flatten=true, child sessions multiplex onto the parent connection and are reachable by sessionId via connection.session(). Puppeteer
  // types CDPSession.connection() as `Connection | undefined`; the undefined case indicates the session has been detached or is otherwise unusable. We bail out
  // cleanly rather than risking a TypeError on first child attach.
  const connection = rootSession.connection();

  if(!connection) {

    LOG.debug("browser:tabObserver", "Root CDP session has no connection - aborting observer install.");

    try {

      await rootSession.detach();
    } catch(_error) {

      // Cleanup best-effort.
    }

    return null;
  }

  const targetFilter = options.targetFilter ?? defaultTargetFilter;

  // Track every session we attach to so disposal can tear them all down deterministically. The root session is included so it is cleaned up alongside children
  // by one code path. A Set is used because the same session is never registered twice (sessionIds are unique per connection) but membership tests are needed
  // during detach handling.
  const sessions = new Set<CDPSession>([rootSession]);
  let disposed = false;

  // Installs Network domain + response listener + recursive auto-attach on a freshly-attached session. Called for the root session synchronously below and for
  // every child target as it attaches. Order matters: the Target.attachedToTarget listener has to be registered before setAutoAttach is sent, because
  // setAutoAttach retroactively fires attachedToTarget for currently-attached children and we cannot miss those events. The Network.responseReceived and
  // Target.detachedFromTarget listeners are insensitive to ordering relative to setAutoAttach; only the attach listener carries the retroactive-fire constraint.
  const attachToSession = async (session: CDPSession, sessionId: string): Promise<void> => {

    if(disposed) {

      return;
    }

    sessions.add(session);

    // Listen for responses on this session. The handler captures sessionId by closure so the consumer can identify the originating target.
    session.on("Network.responseReceived", (params: NetworkResponseReceivedParams): void => {

      if(disposed) {

        return;
      }

      options.onResponse({ headers: params.response.headers ?? {}, sessionId, url: params.response.url });
    });

    // Propagate the attach policy down this subtree. Without this, grandchildren of the root (iframes inside iframes, workers spawned by an OOPIF) would attach
    // but their Target.attachedToTarget events would fire on the intermediate session, not the root - and we would not be listening there. Recursive setAutoAttach
    // ensures every descendant session reaches our handler.
    session.on("Target.attachedToTarget", (params: TargetAttachedParams): void => { void onChildAttached(params); });
    session.on("Target.detachedFromTarget", (params: TargetDetachedParams): void => { onChildDetached(params); });

    try {

      await session.send("Target.setAutoAttach", { autoAttach: true, flatten: true, waitForDebuggerOnStart: false });
      await session.send("Network.enable");

      LOG.debug("browser:tabObserver", "Attached to session %s.", sessionId || "<root>");
    } catch(error) {

      // Session may have detached between attach and our init - benign during page navigations and shutdown. Logging at debug rather than warn because the
      // observer's job is to be resilient to lifecycle churn, not to alert on every churn event.
      LOG.debug("browser:tabObserver", "Setup failed for session %s: %s.", sessionId || "<root>", String(error));
    }
  };

  // Handler for Target.attachedToTarget. Applies the target-type predicate first, then resolves the freshly-attached sessionId to a CDPSession via the parent
  // connection, and forwards to attachToSession. Filtering before the session lookup means a rejected target type never pays for the connection.session() call.
  // The filter is the consumer's policy lever; the default skips only the browser-level target.
  const onChildAttached = async (params: TargetAttachedParams): Promise<void> => {

    if(disposed) {

      return;
    }

    if(!targetFilter({ type: params.targetInfo.type })) {

      return;
    }

    const childSession = connection.session(params.sessionId);

    if(!childSession) {

      // Race: the child detached between the attach event and our lookup. Not actionable.
      return;
    }

    await attachToSession(childSession, params.sessionId);
  };

  // Handler for Target.detachedFromTarget. Removes the detached session from tracking and clears its listeners. We do not call detach() on the session here -
  // Chrome has already torn down the underlying target, and an explicit detach would race with the framework's internal cleanup. removeAllListeners is sufficient
  // because we no longer want events from this session and the session object will be garbage-collected once the framework releases it.
  const onChildDetached = (params: TargetDetachedParams): void => {

    if(disposed) {

      return;
    }

    const childSession = connection.session(params.sessionId);

    if(!childSession) {

      return;
    }

    sessions.delete(childSession);
    childSession.removeAllListeners("Network.responseReceived");
    childSession.removeAllListeners("Target.attachedToTarget");
    childSession.removeAllListeners("Target.detachedFromTarget");

    LOG.debug("browser:tabObserver", "Detached from session %s.", params.sessionId);
  };

  // Install on the root session. The empty sessionId is the CDP convention for "root" - flattened child sessions get real sessionIds. Doing this synchronously
  // before returning the handle means by the time the caller awaits the returned promise, all currently-attached children have already fired their retroactive
  // Target.attachedToTarget events and been onboarded.
  await attachToSession(rootSession, "");

  const dispose = (): void => {

    if(disposed) {

      return;
    }

    disposed = true;

    for(const session of sessions) {

      try {

        session.removeAllListeners("Network.responseReceived");
        session.removeAllListeners("Target.attachedToTarget");
        session.removeAllListeners("Target.detachedFromTarget");
        void session.send("Network.disable").catch(() => { /* Session may already be detached. */ });
        void session.detach().catch(() => { /* Session may already be detached. */ });
      } catch(_error) {

        // Disposal is best-effort; a failed session teardown does not block teardown of the others. The Set iteration continues regardless.
      }
    }

    sessions.clear();

    LOG.debug("browser:tabObserver", "Tab network observer disposed.");
  };

  return { dispose, [Symbol.dispose]: dispose };
}
