/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * pageStaleness.ts: The stale-page decision core - the pure judgment behind managed page cleanup.
 */

/* This module is the pure decision core for stale page cleanup. The Chrome adapter in browser/index.ts owns the page-tracking collections and all of the Chrome
 * I/O (listing the browser's pages, closing them); this module answers, from an explicit snapshot with `now` passed in by the caller, which pages start a
 * staleness clock, which pages close, which tracking entries are forgotten, and which in-flight marks have converged - the same shape as the relaunch
 * governor (launchGovernor.ts), so it is deterministically unit-testable with literal timestamps and no timer mocking.
 *
 * Page identity crosses this boundary as managed ids rather than as Puppeteer objects, which is what keeps the module free of Chrome types...the caller resolves
 * the ids in the returned actions back to pages when it applies them.
 *
 * The judgment, in one place:
 *
 *   - A page PrismCast did not create carries no managed id and is never tracked, never closed. Pages opened by the user for debugging and pages a streaming site
 *     opened for itself (OAuth popups and the like) are left alone.
 *   - A page an active stream owns is not a staleness candidate, and a clock already running on one is dropped: ownership arrived, so the page is in use.
 *   - A page an operation still holds in flight is exempt on the same terms. A stream's page reaches the registry only once its setup completes, and a discovery
 *     page reaches the registry at no point at all, so without the exemption a slow tune or a running walk would have its own page closed out from under it.
 *   - An unowned managed page starts a clock on first sight and closes only once the grace period has elapsed on a later call. The delay absorbs the brief
 *     windows where a page is legitimately untracked during stream initialization or teardown.
 *   - A tracking entry whose page is gone from the browser is forgotten, so the caller's map does not accumulate ids for pages closed by other means.
 *   - At least one page always survives, because Chrome exits when its last page closes.
 */

/**
 * Everything the staleness judgment reads, captured at one instant by the caller. Passing the clock reading in rather than reading it here is what makes the
 * decision reproducible from literal values.
 */
export interface StalePageSnapshot {

  // Managed ids of pages that active streams own, as recorded in the stream registry.
  readonly activePageIds: ReadonlySet<string>;

  // How long a page must be continuously unowned before it may be closed, in milliseconds.
  readonly gracePeriodMs: number;

  // Managed ids of pages an operation owns for its duration, while nothing recorded in the stream registry speaks for that ownership.
  readonly inFlightPageIds: ReadonlySet<string>;

  // The caller's current timestamp in milliseconds, measured against the staleness clocks.
  readonly now: number;

  // The browser's pages in the order the browser reports them, each entry the page's managed id or undefined for a page PrismCast did not create.
  readonly pageIds: readonly (string | undefined)[];

  // Managed id to the timestamp at which the page was first observed unowned - the running staleness clocks.
  readonly staleFirstSeen: ReadonlyMap<string, number>;
}

/**
 * The judgment, expressed as work for the caller to apply against its own collections and against Chrome. Every list is a set of managed ids; the caller resolves
 * them back to pages where it needs the page itself.
 */
export interface StalePageActions {

  // Ids whose in-flight mark has done its job and may be dropped.
  readonly clearInFlightIds: readonly string[];

  // Ids of pages to close, in the order the browser reported them and already trimmed to the budget that preserves a surviving page.
  readonly closeIds: readonly string[];

  // Ids whose staleness clocks are to be forgotten, either because the page became owned or because it is gone from the browser.
  readonly forgetTrackedIds: readonly string[];

  // Ids of pages seen unowned for the first time, whose staleness clocks the caller starts at `now`.
  readonly startTrackingIds: readonly string[];
}

/**
 * Decides what stale page cleanup should do this pass. Pure: it reads only the snapshot and mutates nothing, so the caller owns every collection the actions
 * describe.
 * @param snapshot - The page, ownership, and clock state captured at one instant.
 * @returns The clocks to start, the marks to clear, the entries to forget, and the pages to close.
 */
export function evaluateStalePages(snapshot: StalePageSnapshot): StalePageActions {

  const clearInFlightIds: string[] = [];
  const closeCandidateIds: string[] = [];
  const currentManagedIds = new Set<string>();
  const forgetTrackedIds: string[] = [];
  const startTrackingIds: string[] = [];

  // The managed ids the browser currently reports, which the dead-entry sweep below measures the tracked clocks against.
  for(const pageId of snapshot.pageIds) {

    if(pageId !== undefined) {

      currentManagedIds.add(pageId);
    }
  }

  // Walk the pages in the browser's own order, which is the order any closes will follow.
  for(const pageId of snapshot.pageIds) {

    // A page PrismCast did not create is not ours to judge.
    if(pageId === undefined) {

      continue;
    }

    /* An owned page - by an active stream, or by an operation still holding it in flight - is not a candidate, and any clock running on it is forgotten. Both
     * memberships answer the same question from different ends, which is why they are one condition: the registry says a stream's ownership has landed, the
     * in-flight mark says an operation has the page right now, whether or not the registry will ever record it.
     */
    if(snapshot.activePageIds.has(pageId) || snapshot.inFlightPageIds.has(pageId)) {

      if(snapshot.staleFirstSeen.has(pageId)) {

        forgetTrackedIds.push(pageId);
      }

      continue;
    }

    const firstSeen = snapshot.staleFirstSeen.get(pageId);

    // First sight of an unowned page starts its clock. Nothing closes on the pass that starts a clock; the grace period is measured from here.
    if(firstSeen === undefined) {

      startTrackingIds.push(pageId);

      continue;
    }

    // The clock is running but the grace period has not elapsed, so the page keeps its reprieve.
    if((snapshot.now - firstSeen) < snapshot.gracePeriodMs) {

      continue;
    }

    closeCandidateIds.push(pageId);
  }

  // Clocks for pages that are gone from the browser are forgotten - those pages were closed by other means, and their entries would otherwise accumulate.
  for(const trackedId of snapshot.staleFirstSeen.keys()) {

    if(!currentManagedIds.has(trackedId)) {

      forgetTrackedIds.push(trackedId);
    }
  }

  /* An in-flight mark whose id now appears among the active pages has done its job: registry ownership has arrived and carries the exemption from here on.
   *
   * A mark whose id is absent from the page list is deliberately kept rather than pruned. Every path by which such a page really vanishes already drops the mark
   * at its source - the owning operation's unregister, or the session-end clear - so an absence here means the page list simply did not report the page on this
   * pass, and pruning on that would strip the exemption from an operation still running and re-expose the mid-flight close this exemption exists to prevent.
   */
  for(const inFlightId of snapshot.inFlightPageIds) {

    if(snapshot.activePageIds.has(inFlightId)) {

      clearInFlightIds.push(inFlightId);
    }
  }

  /* How many pages may close while leaving one behind to keep Chrome alive. The arithmetic counts only the browser's pages and the active ones because that is
   * all the floor needs: the walk above already excludes both active and in-flight ids from the candidates, so an in-flight page can never be closed no matter
   * what this budget says, and the pages left standing always number at least one. Subtracting in-flight pages here as well would buy no additional safety and
   * would throttle legitimate cleanup by however many operations happen to be holding one.
   */
  const maxToClose = Math.max(0, snapshot.pageIds.length - 1 - snapshot.activePageIds.size);

  return { clearInFlightIds, closeIds: closeCandidateIds.slice(0, maxToClose), forgetTrackedIds, startTrackingIds };
}
