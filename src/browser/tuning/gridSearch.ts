/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * gridSearch.ts: Bounded binary search over a virtualized guide grid, shared by the provider tuning strategies. The page and DOM mechanics a strategy drives -
 * installs, scrolling, clicking, guide recovery - live in shared.ts, and the map a provider's channel entries sit in lives in cache.ts. What lives here is the
 * third piece: the loop that walks a guide too tall to render all at once, narrowing a row range until the row it wants is among the handful the virtualizer
 * has put in the DOM.
 *
 * The division of labor is deliberate. This module owns the search choreography and nothing else - the bounds, the midpoint, the iteration cap, the direction
 * decision. Every provider-specific answer arrives as a callback: what a probe does to bring a row range into the DOM and what it reads back there, which of
 * those rows can be trusted to anchor a direction decision, what to try when the target sorts inside the rendered window without matching a row, and what a
 * hit's payload is. That split is what lets one loop serve guides agreeing on almost nothing else - different scroll hosts, different row identity, different
 * caching side effects, different notions of what counts as a match.
 */
import type { Nullable } from "../../types/index.ts";

/**
 * What one probe of the grid found: the rows the virtualizer rendered at the probed position, and the target's payload if the probe recognized it among them.
 *
 * Rows arrive in the guide's own visual order. That is the contract the search rests on and the only one a probe can satisfy, because the search reads the
 * first and last of them as the alphabetical extremes of the rendered window and an in-range-miss hook reasoning about position needs the order the guide
 * actually renders. A guide whose DOM order is its visual order hands its rows back untouched; one whose virtualizer recycles elements out of visual order
 * restores that order inside its own probe. Null or empty rows report that the probe read nothing, which is a transient virtualizer state rather than an
 * answer about where the target sits.
 */
export interface GridProbeResult<TRow, TFound> {

  /**
   * The payload for a target the probe recognized among the rows it read, or null when it did not. Anything non-null ends the search, so a probe that resolved
   * only part of what a hit needs - a matched name whose click coordinates did not resolve, say - reports null and lets the search carry on.
   */
  found: Nullable<TFound>;

  // The rows rendered at the probed position, in guide order, or null when the probe found nothing rendered.
  rows: Nullable<TRow[]>;
}

/**
 * How a search ended: the target was found, an in-range-miss hook resolved the caller's whole request on its own, or the range was exhausted without either.
 *
 * The resolved arm exists only for callers that pass a hook returning one. TResult defaults to never for the callers that do not, and the conditional collapses
 * that arm out of the union entirely rather than leaving them a case they would have to handle and could never reach. The comparison is written over one-element
 * tuples so that it tests TResult as a whole instead of distributing across it.
 */
export type GridSearchOutcome<TFound, TResult> =
  { found: TFound; kind: "found" } |
  { kind: "notFound" } |
  ([TResult] extends [never] ? never : { kind: "resolved"; result: TResult });

/**
 * Binary-searches a virtualized guide grid for a channel by name, probing the midpoint of a shrinking row range until a probe recognizes the target or the
 * range runs out. A virtualized guide renders only a small window of its rows, so each step scrolls that window somewhere new and reads what landed there:
 * a hit ends the search, and a miss narrows the range by comparing the target against the window's alphabetical extremes.
 *
 * The search reads nothing off a row itself and touches no provider state. Rows are compared through nameOf, are never sorted or reordered, and reach the
 * in-range-miss hook as the very array the probe returned, because a hook that reasons about a row's position among its neighbors depends on both.
 * @param options - The search bounds and the provider's policy callbacks.
 * @param options.isDirectionAnchor - Reports whether a row's name can be trusted to sort where the row appears, for guides that render rows whose displayed
 *   name is not their sort key. Omit when every row anchors.
 * @param options.maxIterations - Hard cap on probes. Sized per guide against the row count it converges over, and a bound on pathological virtualizer behavior
 *   rather than the normal exit.
 * @param options.nameOf - Reads the name a row is compared by.
 * @param options.onInRangeMiss - Called with the probe's rows when the target sorts inside the rendered window but no row matched it, for guides where that
 *   case is recoverable. Returning a resolved value ends the search as resolved, a found value ends it as found, and null ends it as not found. Omit when the
 *   case simply means the channel is absent.
 * @param options.probe - Brings the rows around a row index into the DOM and reads them back, in guide order, along with the target's payload if it is there.
 * @param options.targetName - The normalized channel name being searched for.
 * @param options.totalRows - How many rows the guide holds, which is the initial upper bound.
 * @returns The outcome of the search.
 */
export async function searchVirtualizedGrid<TRow, TFound, TResult = never>(options: {
  isDirectionAnchor?: (row: TRow) => boolean;
  maxIterations: number;
  nameOf: (row: TRow) => string;
  onInRangeMiss?: (rows: TRow[]) => Promise<Nullable<{ found?: TFound; resolved?: TResult }>>;
  probe: (rowIndex: number) => Promise<GridProbeResult<TRow, TFound>>;
  targetName: string;
  totalRows: number;
}): Promise<GridSearchOutcome<TFound, TResult>> {

  const { isDirectionAnchor, maxIterations, nameOf, onInRangeMiss, probe, targetName, totalRows } = options;

  let low = 0;
  let high = totalRows - 1;

  for(let iteration = 0; iteration < maxIterations; iteration++) {

    if(low > high) {

      break;
    }

    const mid = Math.floor((low + high) / 2);

    // eslint-disable-next-line no-await-in-loop
    const { found, rows } = await probe(mid);

    /* A probe that rendered nothing carries no evidence about where the target sits, so the range is left exactly where it was and the next iteration probes
     * the same midpoint again. Emptiness is settled before the hit is read, which is what makes a probe reporting both - however it managed it - resolve as
     * the empty read it is rather than as a hit on rows nobody saw.
     */
    if(!rows || (rows.length === 0)) {

      continue;
    }

    if(found !== null) {

      return { found, kind: "found" };
    }

    // Anchors are the rows whose names the caller vouches for as sorting where they appear. Filtering builds its own array, and the rows the probe returned are
    // never reordered, so what the miss hook receives below is untouched.
    const anchors = isDirectionAnchor ? rows.filter(isDirectionAnchor) : rows;
    const firstAnchor = anchors[0];
    const lastAnchor = anchors.at(-1);

    // A window holding no anchors says nothing about direction - every row it rendered was one the caller excluded from the comparison. Move down and hope the
    // next window reads better. One check covers both ends, since an array with a first element has a last one.
    if((firstAnchor === undefined) || (lastAnchor === undefined)) {

      low = mid + 1;

      continue;
    }

    // Direction comes from the window's alphabetical extremes, read in the order the probe returned them.
    if(targetName.localeCompare(nameOf(firstAnchor)) < 0) {

      // The target sorts before everything anchored here, so its row is above this window.
      high = mid - 1;

      continue;
    }

    if(targetName.localeCompare(nameOf(lastAnchor)) > 0) {

      // The target sorts after everything anchored here, so its row is below this window.
      low = mid + 1;

      continue;
    }

    // The target belongs inside this window but no row matched it. What that means is the caller's to say: a guide that labels a row with something other than
    // the name it sorts under has a channel to recover here, and a guide that does not simply has no such channel.
    if(onInRangeMiss) {

      // eslint-disable-next-line no-await-in-loop
      const miss = await onInRangeMiss(rows);

      if(miss?.resolved !== undefined) {

        /* The resolved arm is part of the outcome union exactly when TResult is not never, and TResult is not never exactly when the caller passed a hook that
         * returns one - which is the branch this return stands in. The conditional type cannot carry those two facts to each other, so the assertion states
         * what the call signature has already guaranteed.
         */
        return { kind: "resolved", result: miss.resolved } as GridSearchOutcome<TFound, TResult>;
      }

      if(miss?.found !== undefined) {

        return { found: miss.found, kind: "found" };
      }
    }

    break;
  }

  return { kind: "notFound" };
}
