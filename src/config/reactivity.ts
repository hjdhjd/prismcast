/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * reactivity.ts: Config-change reactivity primitive for PrismCast.
 *
 * Subsystems that need to react to configuration changes - opening or closing a network socket, rebinding a server, invalidating a cached resource - register a
 * handler against a path prefix and receive the subset of the diff that matches. Subsystems that simply read CONFIG live at use time need not register; the
 * primitive defaults their paths to "deferred", which preserves the legacy "restart on save" behavior for everything that has not opted in to live application.
 *
 * The primitive owns two responsibilities:
 *
 *   1. Computing a diff between two CONFIG snapshots (computeConfigDiff). Pure function. Walks both objects, emits one ConfigChange per leaf-value difference,
 *      treating arrays as opaque leaves so handlers can react to array replacement without per-element noise.
 *
 *   2. Dispatching that diff to registered handlers (applyConfigChanges). Each change is routed to the longest-matching registered prefix; changes whose path
 *      is not matched by any handler are reported as deferred. Handlers are awaited; their per-path outcomes are merged into the final ApplyResult.
 *
 * The primitive intentionally does not touch the in-memory CONFIG object. The caller (typically the settings save handler) is responsible for committing the new
 * configuration to CONFIG before invoking applyConfigChanges, so handlers that re-read CONFIG see the post-commit state. Keeping commit and dispatch separate
 * lets callers control ordering and lets test code dispatch synthetic diffs against any handler set without mutating real config state.
 *
 * Multiple handlers per prefix are disallowed at registration time and throw immediately, so duplicate-wiring bugs surface during boot rather than as silent
 * misrouted dispatches at runtime.
 */
import { LOG, formatError } from "../utils/index.ts";
import type { Nullable } from "../types/index.ts";

/**
 * A single config field that changed between two snapshots. Path is the dot-separated location (e.g., "hdhr.port"). previous and current are the leaf values
 * before and after the change; either may be undefined if the field was added or removed.
 */
export interface ConfigChange {

  // The new value (after the change).
  readonly current: unknown;

  // The dot-separated path to the changed field.
  readonly path: string;

  // The old value (before the change).
  readonly previous: unknown;
}

/**
 * Per-change outcome reported by a handler. "applied" means the change is fully live (in-memory CONFIG plus any side effects); "deferred" means the value is in
 * CONFIG but full effect requires a restart; "rejected" means the handler refused the change (e.g., a dependency is unavailable). The reason field on the non-
 * applied variants is surfaced to operators in the settings-save response and the server log.
 */
export type ChangeOutcome =
  { readonly kind: "applied"; readonly path: string } |
  { readonly kind: "deferred"; readonly path: string; readonly reason: string } |
  { readonly kind: "rejected"; readonly path: string; readonly reason: string };

/**
 * Handler signature. Receives the subset of the diff that matched the handler's registered prefix and returns one outcome per change. Handlers may report
 * outcomes for paths they did not receive; the primitive ignores those entries and emits a debug-level warning. Handlers that omit a path are conservatively
 * treated as having deferred that change.
 *
 * Concurrency contract: applyConfigChanges dispatches handlers across distinct prefixes in parallel via Promise.allSettled, so a thrown handler does not
 * short-circuit the rest of the apply - the throwing bucket's changes are converted to rejected outcomes with the formatted error message as the reason, and
 * every other bucket's outcomes flow through unaffected. Within a single prefix, all changes that matched it arrive in one invocation as a sorted batch, so
 * the handler can sequence its internal work however it wants. Across prefixes, handlers run concurrently and must not share mutable state with one another.
 * Subsystems whose live-apply work touches state owned by another subsystem need to coordinate through that subsystem's public API, not through shared globals.
 */
export type ConfigChangeHandler = (changes: readonly ConfigChange[]) => Promise<readonly ChangeOutcome[]>;

/**
 * Aggregate result of applyConfigChanges. The three buckets are disjoint and together cover every ConfigChange in the input diff.
 */
export interface ApplyResult {

  // Changes whose handler reported "applied".
  readonly applied: readonly ConfigChange[];

  // Changes whose handler reported "deferred" or for which no handler was registered. The reason explains the deferral.
  readonly deferred: readonly { readonly change: ConfigChange; readonly reason: string }[];

  // Changes whose handler reported "rejected". The reason explains the refusal.
  readonly rejected: readonly { readonly change: ConfigChange; readonly reason: string }[];
}

// Registry of (prefix -> handler) entries. Lookups walk the entries by descending prefix length to honor longest-prefix-match semantics.
const handlers = new Map<string, ConfigChangeHandler>();

// Default deferral reason when no handler is registered for a path. Surfaced to operators and useful in tests that want to assert this specific code path.
export const NO_HANDLER_REASON = "no live-apply handler registered for this field";

// Default deferral reason when a handler did not report an outcome for one of its input changes. A handler bug, but a safe conservative default.
export const MISSING_OUTCOME_REASON = "handler did not report an outcome for this field";

/**
 * Registers a handler that will receive any ConfigChange whose path starts with the given prefix. Throws if a handler is already registered for the prefix to
 * surface duplicate wiring at boot time. Prefixes are matched as plain string prefixes against the dot-separated path - by convention they end with a "."
 * ("hdhr." matches "hdhr.enabled" and "hdhr.port" but not "hdhrFoo").
 * @param prefix - Path prefix (e.g., "hdhr.").
 * @param handler - The handler to invoke for matching changes.
 */
export function registerConfigChangeHandler(prefix: string, handler: ConfigChangeHandler): void {

  if(handlers.has(prefix)) {

    throw new Error("A config change handler is already registered for prefix \"" + prefix + "\".");
  }

  handlers.set(prefix, handler);
}

/**
 * Clears the config-change handler registry. Primarily a testing hook - tests register handlers per case, run the dispatch, and reset between cases to keep
 * isolation strong. Production code never calls this; handlers are registered once at module load by each subsystem and remain for the process lifetime.
 */
export function resetConfigChangeHandlers(): void {

  handlers.clear();
}

/**
 * Computes the leaf-value differences between two configuration snapshots. Both inputs are walked recursively; arrays are treated as opaque leaves so an array
 * replacement appears as a single change rather than per-element noise. The returned changes are sorted alphabetically by path for deterministic dispatch order.
 * @param previous - The snapshot before the change.
 * @param current - The snapshot after the change.
 * @returns Array of ConfigChange entries; empty if the snapshots are equivalent.
 */
export function computeConfigDiff(previous: object, current: object): readonly ConfigChange[] {

  const changes: ConfigChange[] = [];

  collectDiff("", previous, current, changes);
  changes.sort((a, b) => a.path.localeCompare(b.path));

  return changes;
}

/**
 * Dispatches a diff to registered handlers and returns the aggregate outcome. Changes are grouped by longest-matching prefix, handlers are awaited in parallel,
 * and per-change outcomes are folded back into the input order. Changes with no matching prefix are returned as deferred with NO_HANDLER_REASON. Handlers that
 * omit a path from their outcome list have that path conservatively recorded as deferred with MISSING_OUTCOME_REASON.
 * @param diff - The diff to dispatch.
 * @returns Aggregate result partitioning every input change into applied, deferred, or rejected.
 */
export async function applyConfigChanges(diff: readonly ConfigChange[]): Promise<ApplyResult> {

  if(diff.length === 0) {

    return { applied: [], deferred: [], rejected: [] };
  }

  // Bucket each change by the longest registered prefix that matches its path. Paths with no matching prefix go into the unhandled bucket.
  const byPrefix = new Map<string, ConfigChange[]>();
  const unhandled: ConfigChange[] = [];

  for(const change of diff) {

    const prefix = findLongestPrefix(change.path);

    if(prefix === null) {

      unhandled.push(change);

      continue;
    }

    const bucket = byPrefix.get(prefix) ?? [];

    bucket.push(change);
    byPrefix.set(prefix, bucket);
  }

  // Dispatch each bucket to its handler in parallel via Promise.allSettled so a thrown handler does not short-circuit the rest of the apply. Materializing
  // the buckets up front lets us pair each settled result with its source bucket by index - allSettled preserves array length, so settled[i] aligns with
  // buckets[i] for the lifetime of this dispatch. A handler that throws is treated as having rejected every change in its bucket: the formatted error message
  // becomes the rejection reason for each change, so the dispatcher's guarantee that every input change gets an outcome holds even when handlers fail.
  const buckets = Array.from(byPrefix.entries());
  const settled = await Promise.allSettled(buckets.map(async ([ prefix, changes ]) => {

    const handler = handlers.get(prefix);

    // The handler must exist - findLongestPrefix returns a prefix only when handlers.has(prefix) is true - but TypeScript widens map.get to T | undefined.
    if(!handler) {

      return [] as readonly ChangeOutcome[];
    }

    return handler(changes);
  }));

  const dispatched = settled.map((result, index) => {

    // allSettled preserves array length, so buckets[index] is always defined; the explicit guard satisfies TypeScript without leaning on a non-null assertion.
    const bucket = buckets[index];

    if(!bucket) {

      return { changes: [] as readonly ConfigChange[], outcomes: [] as readonly ChangeOutcome[] };
    }

    const [ , changes ] = bucket;

    if(result.status === "fulfilled") {

      return { changes, outcomes: result.value };
    }

    // Handler threw - synthesize a rejected outcome so every change in the bucket still gets an outcome.
    const reason = formatError(result.reason);
    const outcomes: readonly ChangeOutcome[] = changes.map((c) => ({ kind: "rejected", path: c.path, reason }));

    return { changes, outcomes };
  });

  // Build the aggregate result. Iterate the input diff in order so callers see deterministic output even when handlers ran in parallel.
  const applied: ConfigChange[] = [];
  const deferred: { change: ConfigChange; reason: string }[] = [];
  const rejected: { change: ConfigChange; reason: string }[] = [];

  // Index outcomes by path for O(1) lookup during the merge below. A handler is only authoritative for the changes it was actually given, so we accept an
  // outcome only when its path was in that handler's input batch. Ignoring foreign-path outcomes upholds the documented contract and prevents one handler
  // from overriding another handler's classification of a shared path via last-write-wins into this map. Ignored entries are surfaced at debug level so
  // a misbehaving handler is diagnosable without polluting the operator-facing log.
  const outcomeByPath = new Map<string, ChangeOutcome>();

  for(const { changes, outcomes } of dispatched) {

    const inputPaths = new Set(changes.map((change) => change.path));

    for(const outcome of outcomes) {

      if(!inputPaths.has(outcome.path)) {

        LOG.debug("config:reactivity", "Ignoring a config-change outcome for a path the handler was not given: %s.", outcome.path);

        continue;
      }

      outcomeByPath.set(outcome.path, outcome);
    }
  }

  // Anything in the unhandled bucket defers with NO_HANDLER_REASON.
  const unhandledPaths = new Set(unhandled.map((c) => c.path));

  for(const change of diff) {

    if(unhandledPaths.has(change.path)) {

      deferred.push({ change, reason: NO_HANDLER_REASON });

      continue;
    }

    const outcome = outcomeByPath.get(change.path);

    if(!outcome) {

      deferred.push({ change, reason: MISSING_OUTCOME_REASON });

      continue;
    }

    switch(outcome.kind) {

      case "applied": {

        applied.push(change);

        break;
      }

      case "deferred": {

        deferred.push({ change, reason: outcome.reason });

        break;
      }

      case "rejected": {

        rejected.push({ change, reason: outcome.reason });

        break;
      }
    }
  }

  return { applied, deferred, rejected };
}

/**
 * Returns the longest registered prefix that the given path starts with, or null if no prefix matches. Used by applyConfigChanges to route changes to handlers.
 * @param path - The dot-separated config path.
 * @returns The matching prefix or null.
 */
function findLongestPrefix(path: string): Nullable<string> {

  let best: Nullable<string> = null;

  for(const prefix of handlers.keys()) {

    if(path.startsWith(prefix) && ((best === null) || (prefix.length > best.length))) {

      best = prefix;
    }
  }

  return best;
}

/**
 * Recursive walker that populates the changes array with leaf-value differences between two values rooted at the given path prefix. Plain objects recurse; any
 * other value (including arrays, dates, nulls, primitives) is compared as a leaf via deep equality.
 * @param prefix - Current path prefix.
 * @param previous - Previous value at this path.
 * @param current - Current value at this path.
 * @param changes - Accumulator the walker pushes into.
 */
function collectDiff(prefix: string, previous: unknown, current: unknown, changes: ConfigChange[]): void {

  // If either side is not a plain object, treat this position as a leaf and emit a change when the values differ.
  if(!isPlainObject(previous) || !isPlainObject(current)) {

    if(!deepEqual(previous, current)) {

      changes.push({ current, path: prefix, previous });
    }

    return;
  }

  // Both sides are plain objects: recurse into the union of their keys so additions and removals are captured along with mutations.
  const keys = new Set(Object.keys(previous)).union(new Set(Object.keys(current)));

  for(const key of Array.from(keys).sort()) {

    const childPath = prefix ? (prefix + "." + key) : key;

    collectDiff(childPath, previous[key], current[key], changes);
  }
}

/**
 * Predicate: is the value a plain object (not array, not null, not a class instance like Date)? The reactivity primitive treats any non-plain value as a leaf,
 * which is the right behavior for Config - it is JSON-shaped and contains no class instances or arrays-of-objects whose elements should diff independently.
 * @param value - The candidate.
 * @returns True if value is a plain object literal.
 */
function isPlainObject(value: unknown): value is Record<string, unknown> {

  if((value === null) || (typeof value !== "object") || Array.isArray(value)) {

    return false;
  }

  // Object.getPrototypeOf is typed as returning any in the standard lib. Cast through unknown so the comparisons below are type-safe.
  const proto = Object.getPrototypeOf(value) as unknown;

  return (proto === null) || (proto === Object.prototype);
}

/**
 * Deep equality check for leaf values. Stringification via JSON.stringify normalizes nested arrays and objects and handles undefined-vs-missing correctly when
 * the values are wrapped in a single-element array. Sufficient for Config which is JSON-shaped throughout.
 * @param a - First value.
 * @param b - Second value.
 * @returns True if the values are deeply equal.
 */
function deepEqual(a: unknown, b: unknown): boolean {

  // Wrapping in an array side-steps the JSON.stringify(undefined) === undefined corner case so the comparison handles undefined leaves correctly.
  return JSON.stringify([a]) === JSON.stringify([b]);
}
