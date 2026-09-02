/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * windowSync.ts: The shared browser window's presentation policy and its serialized executor.
 *
 * Chrome's tab capture consumes the compositor's output for the shared browser window, so that output has to be composed for a window the desktop is actually
 * presenting. A minimized window composes differently, and capture reads the difference as corrupted frames: stale bands, offset video, cropped lower thirds. The
 * window therefore stays visible for as long as any capture stream is alive, and returns to minimized once none are - which is also what login mode needs, for the
 * unrelated reason that a human has to interact with the window.
 *
 * Which tab that window is showing matters for the same reason. Chrome composes the capture of a SELECTED tab from the window's fitted presentation rather than from
 * the emulated surface, so a capture tab left in front records a clipped view of itself; a blank tab in front leaves every capture composing the surface it was
 * emulated at. That makes presentation two decisions rather than one - how the window is shown, and what it is showing - taken from the same pair of inputs.
 *
 * Each decision is a pure function over those two inputs, and every caller reaches them through one serialized executor. That shape is deliberate: window
 * presentation is a single shared resource driven from at least eight places (startup, establishment, teardown, recovery, precaching, login, restart, native
 * upgrade), and gating each of those call sites individually is what let a stale decision land after a fresh one. Here a decision cannot be older than the command
 * that carries it, because both inputs are read once inside the serialized loop, immediately before the commands that act on them. The asymmetric latencies of the
 * two window primitives - the minimize path waits for the window manager to settle, the un-minimize path does not - stop mattering for the same reason.
 *
 * There is no timer and no polling. Triggers are events: a stream registers, a stream unregisters, a mode flips, login starts or ends. Convergence comes from the
 * drain loop, which keeps passing until no request is outstanding.
 *
 * The module is dependency-free by design, in the shape captureLock.ts established: it imports no browser state and constructs nothing, taking its whole world
 * through WindowSyncDeps. browser/index.ts owns the supervisor and therefore builds the one production instance.
 */
import { LOG, formatError } from "../utils/index.ts";
import type { Nullable } from "../types/index.ts";
import type { Page } from "puppeteer-core";

// Types.

/**
 * The two presentation states this application drives the shared browser window between.
 */
export type WindowVisibility = "minimized" | "normal";

/**
 * The collaborators the executor composes on. Every member is injected so the executor can be driven with fakes at its own boundary, with no browser in the process.
 */
export interface WindowSyncDeps {

  /* Brings the owned blank utility tab to the front of the window, creating or re-creating it when the reference it keeps is gone. Ownership of that tab lives
   * with whoever supplies these dependencies; the executor only asks for it to be in front.
   */
  readonly ensureForegroundBlank: () => Promise<void>;

  // Reports whether any stream is currently in capture mode. Read fresh on every pass.
  readonly hasActiveCaptureStreams: () => boolean;

  // Reports whether a user is authenticating in the browser window. Read fresh on every pass.
  readonly isLoginModeActive: () => boolean;

  // Reports whether the process is tearing down. A pass that sees this abandons the loop without issuing a command, since window presentation is meaningless for a
  // browser that is closing and the command would race the teardown.
  readonly isShuttingDown: () => boolean;

  // Puts the window into its minimized state.
  readonly minimize: (page: Page) => Promise<void>;

  /* Produces the page whose CDP session carries this pass's window command, preferring the one the caller supplied. The resolution carries its own cleanup: dispose
   * is null for a page that was borrowed from the browser, and closes-and-releases a page the resolver had to create, so the loop releases exactly what the
   * resolution created and nothing else. A null resolution means there is no window to act on and the pass is skipped.
   */
  readonly resolvePage: (preferred: Nullable<Page>) => Promise<Nullable<{ dispose: Nullable<() => Promise<void>>; page: Page }>>;

  // Restores the window to its normal, visible state.
  readonly unminimize: (page: Page) => Promise<void>;
}

// Functions.

/**
 * Decides how the shared browser window should be presented. This is the single owner of that policy: either reason to be on screen wins, and the window is
 * minimized only when neither holds.
 * @param options - The two live inputs the decision reads.
 * @param options.captureActive - Whether any capture stream is active. Capture reads the compositor's output for this window, which is only composed correctly
 * while the window is presented.
 * @param options.loginActive - Whether a user is authenticating in the window and needs to see and click it.
 * @returns The presentation state the window should be in.
 */
export function decideWindowVisibility(options: { captureActive: boolean; loginActive: boolean }): WindowVisibility {

  return (options.captureActive || options.loginActive) ? "normal" : "minimized";
}

/**
 * Decides whether the window should be showing its blank utility tab. Chrome composes the capture of a selected tab from the window's fitted presentation rather
 * than from the emulated surface, so a capture tab that is left selected records a clipped view of itself...keeping a blank tab in front is what leaves every
 * capture composing the surface it was emulated at. Login wins over that, because a user authenticating needs to see and click the page they are working in, and
 * an idle window needs nothing: with no capture running there is no composition to protect.
 * @param options - The two live inputs the decision reads.
 * @param options.captureActive - Whether any capture stream is active.
 * @param options.loginActive - Whether a user is authenticating in the window.
 * @returns True when the blank tab belongs in front.
 */
export function decideForegroundBlank(options: { captureActive: boolean; loginActive: boolean }): boolean {

  return options.captureActive && !options.loginActive;
}

/**
 * Builds the serialized executor that keeps the window's presentation in agreement with the policy. The returned function is what every trigger site calls; it
 * takes an optional page to prefer for the CDP session and resolves once a pass that started at or after the call has completed.
 *
 * Requests coalesce rather than queue. A call marks work outstanding and then waits for a settled boundary at which nothing is outstanding, sharing whichever run
 * is live rather than starting one of its own - so a waiter never resolves ahead of the effect it asked for, and a storm of triggers costs one CDP command per
 * genuine state change rather than one per trigger. The one case that resolves a waiter without a command is a pass that failed, which is deliberate: window
 * presentation is cosmetic enough that no caller should fail or spin over it.
 * @param deps - The injected collaborators.
 * @returns The trigger function, which is safe to call concurrently and from any layer.
 */
export function createWindowVisibilitySync(deps: WindowSyncDeps): (page?: Page) => Promise<void> {

  // Whether at least one request is outstanding. The drain loop clears it before each pass, so a trigger arriving mid-pass queues another pass rather than being
  // absorbed by the one already running.
  let pending = false;

  // The page the most recent page-carrying caller asked to use, consumed by the next pass. Callers with no page in hand leave it as it stands.
  let preferredPage: Nullable<Page> = null;

  // The live run, or null when no pass is in flight. Doubles as the coalescing gate and as the promise every waiter shares.
  let run: Nullable<Promise<void>> = null;

  /**
   * Runs passes until no request is outstanding. Each pass re-reads the policy inputs and resolves its own page, so nothing carries over from the pass before it.
   */
  const drain = async (): Promise<void> => {

    /* eslint-disable no-await-in-loop -- serialization is the whole purpose of this loop: each pass must complete its CDP command before the next pass reads state,
     * because a parallel pass would be exactly the stale-decision race the executor exists to remove.
     */
    while(pending) {

      // Cleared before the pass rather than after it, so a trigger that lands while this pass is in flight is honored by another pass instead of being lost to
      // the one already reading state.
      pending = false;

      // Checked inside the loop, not once on entry: a coalesced pass can begin after teardown started, and issuing a window command into a closing browser buys
      // nothing.
      if(deps.isShuttingDown()) {

        return;
      }

      const requested = preferredPage;

      preferredPage = null;

      let resolution: Nullable<{ dispose: Nullable<() => Promise<void>>; page: Page }> = null;

      try {

        // Resolved fresh on every pass. Tab replacement closes the page a previous pass used, and the CDP layer swallows a command issued into a closed page, so
        // a carried-over resolution would let a pass silently do nothing.
        resolution = await deps.resolvePage(requested);

        if(!resolution) {

          continue;
        }

        /* Both inputs are read here, immediately before the commands that act on them, and both presentation decisions are taken from those same two locals.
         * Serialization does the rest: with one pass in flight at a time, no staler decision can be waiting behind this one to land after it, and the foreground
         * step below cannot act on a different reading of the world than the visibility step it follows.
         */
        const captureActive = deps.hasActiveCaptureStreams();
        const loginActive = deps.isLoginModeActive();
        const visibility = decideWindowVisibility({ captureActive, loginActive });

        if(visibility === "normal") {

          await deps.unminimize(resolution.page);
        } else {

          await deps.minimize(resolution.page);
        }

        // The blank tab is fronted after the window is presented, not before: fronting a tab in a minimized window settles nothing, and the window command is the
        // one every pass owes its caller. A restore is awaited to its confirmation (or to its ceiling), so a caller that goes on to acquire capture does so
        // against a window Chrome has reported restored; a window already on screen costs a single state read.
        if(decideForegroundBlank({ captureActive, loginActive })) {

          await deps.ensureForegroundBlank();
        }

        /* A page that died under the command took the command with it - the CDP layer swallows anything issued into a detaching target, and a terminating stream
         * closes its page fire-and-forget, which can leave that page listed as open for a moment longer. Queueing one more pass re-resolves a live page. This
         * converges rather than spinning, because a closed page is never resolved again.
         */
        if(resolution.page.isClosed()) {

          pending = true;
        }
      } catch(error) {

        /* A failed pass is not worth retrying on its own account: the next state transition triggers another one, and window presentation is cosmetic enough that
         * a caller should not fail over it. Clearing the outstanding flag on the way out is what makes that true for the trigger below as well, since it waits
         * for a settled boundary with nothing outstanding: a request that arrived during this pass is dropped with the pass rather than driving another command
         * straight back at a browser that has just refused one, and a steady arrival of triggers through failing passes cannot hold a caller in that retry.
         */
        pending = false;

        LOG.debug("browser:lifecycle", "Browser window visibility sync failed: %s.", formatError(error));

        break;
      } finally {

        // Release exactly what the resolution created. A borrowed page carries no dispose and must survive the pass.
        if(resolution?.dispose) {

          await resolution.dispose();
        }
      }
    }

    /* eslint-enable no-await-in-loop */
  };

  /**
   * Hands back the run in flight, starting one when none is. Kept separate from the trigger below so the slot is read and filled in one place.
   * @returns The live run.
   */
  const currentRun = (): Promise<void> => {

    run ??= drain().finally(() => {

      run = null;
    });

    return run;
  };

  return async (page?: Page): Promise<void> => {

    if(page) {

      preferredPage = page;
    }

    pending = true;

    /* Wait across settle boundaries rather than attaching to whichever run happens to be live. A run that has already taken its last look at the outstanding flag
     * is still assigned for the microtask it takes to clear itself, and a request landing in that window would ride it and resolve with nothing having acted on
     * it. Re-reading the flag after each settled run closes that window: this returns only at a boundary where no request is outstanding, which is the guarantee
     * every awaiting caller is entitled to - createPageWithCapture awaits this before acquiring capture, and a false resolution there hands the compositor a
     * window that was never brought on screen.
     *
     * The first pass is unconditional, because the line above just marked a request. Requests still coalesce: concurrent callers share whatever run is live and
     * cost one command per genuine state change, not one per caller.
     */
    do {

      // eslint-disable-next-line no-await-in-loop -- waiting on successive runs is the point: each pass has to settle before this caller can read the flag again.
      await currentRun();

    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- the drain loop clears both of these from its own closure; TS cannot track that.
    } while(pending || run);
  };
}
