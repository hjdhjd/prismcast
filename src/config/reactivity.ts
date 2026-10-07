/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * reactivity.ts: Config-change reactivity primitive for PrismCast.
 *
 * Every leaf of the configuration carries a reactivity class - live, next-stream, or restart - that says how a saved value reaches the running process. The
 * caller, the reconcile in config/index.ts, diffs the running configuration against the candidate it just validated and reconciles that gap through this
 * module: it partitions the gap by class, holds the restart-class changes out of the running configuration, hands the live and next-stream changes to the
 * handlers registered for their path prefixes together with the candidate running configuration, and commits exactly the changes the handlers realized. Only
 * after that commit does the candidate become the loaded snapshot. A process write's commit in the same module dispatches the leaves it wrote to their
 * handlers the same way.
 *
 * The primitive owns the responsibilities below, and none of them touches a configuration object:
 *
 *   1. Computing a diff between two configuration snapshots (computeConfigDiff). Pure function. Walks both objects, emits one ConfigChange per leaf-value
 *      difference, treating arrays as opaque leaves so handlers can react to array replacement without per-element noise.
 *
 *   2. Partitioning a diff by reactivity class (partitionConfigChanges). Pure function. The classifier is injected, so the policy of which leaf carries which
 *      class stays with the configuration layer, and a test can partition synthetic paths.
 *
 *   3. Dispatching the live and next-stream changes to the registered handlers (applyConfigChanges). Each change is routed to the longest-matching registered
 *      prefix, and each handler receives its changes with the candidate running configuration. A handler realizes the candidate and vetoes what it cannot
 *      realize, returning a rejection with a reason for each such change and leaving that change's side effect at its previous state. A change no handler
 *      refused is realized, a change no handler is registered for among them, because its readers take it up at their next use.
 *
 * Multiple handlers per prefix are disallowed at registration time and throw immediately, so duplicate-wiring bugs surface during boot rather than as silent
 * misrouted dispatches at runtime.
 */
import type { Config, Nullable, ReactivityClass } from "../types/index.ts";
import { LOG, assertNever, formatError, isPlainObject } from "../utils/index.ts";

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
 * A handler's refusal of one change it was given. The reconcile commits nothing a handler refused, so the running configuration keeps its value and the change
 * stays in the gap every later save retries.
 */
export interface ChangeRejection {

  // The dot-separated path of the change the handler refused.
  readonly path: string;

  // Why the handler refused it, in a complete sentence the save response and the log carry to the operator.
  readonly reason: string;
}

/**
 * Handler signature. Receives the live and next-stream changes that matched the handler's registered prefix, together with the candidate running configuration:
 * the running configuration with every live and next-stream change of the dispatch applied, the gap's for a save's reconcile and the written leaves for a
 * process write's commit. CONFIG still holds the previous values while handlers run, so a handler reads the state it is asked to realize from the candidate. It
 * drives its subsystem to that state and returns a rejection for each change it could not realize; returning nothing accepts every change it was given. A
 * rejection for a path the handler was not given is ignored with a debug-level line.
 *
 * Concurrency contract: applyConfigChanges dispatches handlers across distinct prefixes in parallel via Promise.allSettled, so a thrown handler does not
 * short-circuit the rest of the dispatch - each change in the throwing bucket is rejected with a reason naming the prefix and carrying the formatted error, and
 * every other bucket flows through unaffected. Within a single prefix, all changes that matched it arrive in one invocation as a batch in the partition's order,
 * so the handler can sequence its internal work however it wants. Across prefixes, handlers run concurrently and must not share mutable state with one another.
 * A handler never writes the configuration, because the save's reconcile or the process write's commit it runs inside holds the store's queue, and a write it
 * awaited would wait on that queue and never settle.
 */
export type ConfigChangeHandler = (changes: readonly ConfigChange[], next: Readonly<Config>) => Promise<readonly ChangeRejection[]>;

/**
 * Resolves the reactivity class of a configuration path. The configuration layer passes getReactivityClass; a test passes its own over synthetic paths.
 */
export type ReactivityClassifier = (path: string) => ReactivityClass;

/**
 * A diff partitioned by reactivity class. The lists are disjoint, together cover every change of the diff, and each keeps the diff's order.
 */
export interface ConfigChangePartition {

  // Changes whose class holds them out of the running configuration: restart-class, persisted and never committed, pending while the loaded value
  // differs from the running one.
  readonly held: readonly ConfigChange[];

  // Changes whose class is live: committed once a handler realizes them, in effect at their readers' next use.
  readonly live: readonly ConfigChange[];

  // Changes whose class is next-stream: committed once a handler realizes them, read by streams that start after the commit.
  readonly nextStream: readonly ConfigChange[];
}

/**
 * The outcome of dispatching a partition's live and next-stream changes. Together the lists cover every change dispatched.
 */
export interface DispatchResult {

  // The live and next-stream changes no handler refused, in the partition's order; the caller commits exactly these.
  readonly realized: readonly ConfigChange[];

  // The changes a handler refused, each with its reason; the caller commits none of them.
  readonly rejected: readonly { readonly change: ConfigChange; readonly reason: string }[];
}

/**
 * The outcome a save reports. The reconcile composes it from the gap it realized and from the delta between the previous loaded snapshot and the file this save
 * wrote: everything realized is reported, while the restart a save schedules and the refusals it reports are the ones its own changes earned.
 */
export interface ApplyResult {

  // Live changes the reconcile realized.
  readonly applied: readonly ConfigChange[];

  // This save's restart-class changes whose loaded value differs from the running one, in effect once the restart reads the file.
  readonly deferred: readonly ConfigChange[];

  // Next-stream changes the reconcile realized.
  readonly nextStream: readonly ConfigChange[];

  // This save's changes a handler refused, each with its reason.
  readonly rejected: readonly { readonly change: ConfigChange; readonly reason: string }[];
}

// Registry of (prefix -> handler) entries. A lookup scans every registered prefix and keeps the longest one the path starts with, which gives
// longest-prefix-match routing.
const handlers = new Map<string, ConfigChangeHandler>();

/**
 * Registers a handler that will receive any ConfigChange whose path starts with the given prefix. Throws if a handler is already registered for the prefix to
 * surface duplicate wiring at boot time. Prefixes are matched as plain string prefixes against the dot-separated path, so a prefix ending in "." covers a
 * subtree ("hdhr." matches "hdhr.enabled" and "hdhr.port" but not "hdhrFoo"), and a full leaf path registers that one setting ("logging.maxSize").
 * @param prefix - Path prefix (e.g., "hdhr.") or full leaf path (e.g., "logging.maxSize").
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
 * Partitions a diff by the reactivity class the classifier gives each change's path. Pure: the classifier is the only policy, and the input order survives
 * within each list.
 * @param diff - The changes to partition.
 * @param classify - Resolves a path's reactivity class.
 * @returns The changes held for a restart, the live changes, and the next-stream changes.
 */
export function partitionConfigChanges(diff: readonly ConfigChange[], classify: ReactivityClassifier): ConfigChangePartition {

  const held: ConfigChange[] = [];
  const live: ConfigChange[] = [];
  const nextStream: ConfigChange[] = [];

  for(const change of diff) {

    const reactivity = classify(change.path);

    switch(reactivity) {

      case "live": {

        live.push(change);

        break;
      }

      case "next-stream": {

        nextStream.push(change);

        break;
      }

      case "restart": {

        held.push(change);

        break;
      }

      default: {

        assertNever(reactivity);
      }
    }
  }

  return { held, live, nextStream };
}

/**
 * Dispatches a partition's live and next-stream changes to the registered handlers and reports which of them were realized. Changes are grouped by
 * longest-matching prefix, each handler is called once with its group and the candidate running configuration, and handlers run in parallel. A change no
 * handler refused is realized, whether or not a handler is registered for its path; a refused change is rejected with the handler's reason. Held changes never
 * reach a handler.
 * @param partition - The partitioned changes. Only its live and next-stream changes are dispatched.
 * @param next - The candidate running configuration the handlers realize.
 * @returns The realized and the rejected changes, each list in the partition's order.
 */
export async function applyConfigChanges(partition: ConfigChangePartition, next: Readonly<Config>): Promise<DispatchResult> {

  const dispatched = [ ...partition.live, ...partition.nextStream ];

  if(dispatched.length === 0) {

    return { realized: [], rejected: [] };
  }

  // Bucket each change by the longest registered prefix that matches its path. A change no prefix matches has no handler that could refuse it, so it stays out
  // of every bucket and is realized below.
  const byPrefix = new Map<string, ConfigChange[]>();

  for(const change of dispatched) {

    const prefix = findLongestPrefix(change.path);

    if(prefix === null) {

      continue;
    }

    const bucket = byPrefix.get(prefix) ?? [];

    bucket.push(change);
    byPrefix.set(prefix, bucket);
  }

  // Dispatch each bucket to its handler in parallel via Promise.allSettled so a thrown handler does not short-circuit the rest of the dispatch. Materializing
  // the buckets up front lets us pair each settled result with its source bucket by index - allSettled preserves array length, so settled[i] aligns with
  // buckets[i] for the lifetime of this dispatch.
  const buckets = Array.from(byPrefix.entries());
  const settled = await Promise.allSettled(buckets.map(async ([ prefix, changes ]) => {

    const handler = handlers.get(prefix);

    // The handler must exist - findLongestPrefix returns a prefix only when handlers.has(prefix) is true - but TypeScript widens map.get to T | undefined.
    if(!handler) {

      return [] as readonly ChangeRejection[];
    }

    return handler(changes, next);
  }));

  // Index each refusal by path. A handler is authoritative only for the changes it was given, so a rejection for any other path is ignored rather than allowed
  // to refuse a change another handler realized. Ignored entries are surfaced at debug level so a misbehaving handler is diagnosable without polluting the
  // operator-facing log.
  const reasons = new Map<string, string>();

  for(const [ index, result ] of settled.entries()) {

    // allSettled preserves array length, so buckets[index] is always defined; the explicit guard satisfies TypeScript without leaning on a non-null assertion.
    const bucket = buckets[index];

    if(!bucket) {

      continue;
    }

    const [ prefix, changes ] = bucket;

    // A handler that throws refuses every change in its bucket, so a failed handler never reads as a realized change and the caller commits none of them.
    if(result.status === "rejected") {

      const reason = "The configuration handler for \"" + prefix + "\" failed: " + formatError(result.reason) + ".";

      for(const change of changes) {

        reasons.set(change.path, reason);
      }

      continue;
    }

    const givenPaths = new Set(changes.map((change) => change.path));

    for(const rejection of result.value) {

      if(!givenPaths.has(rejection.path)) {

        LOG.debug("config:reactivity", "Ignoring a config-change rejection for a path the handler was not given: %s.", rejection.path);

        continue;
      }

      reasons.set(rejection.path, rejection.reason);
    }
  }

  // Fold the refusals back over the dispatched changes so the realized and rejected lists keep the partition's order even though handlers ran in parallel.
  const realized: ConfigChange[] = [];
  const rejected: { change: ConfigChange; reason: string }[] = [];

  for(const change of dispatched) {

    const reason = reasons.get(change.path);

    if(reason === undefined) {

      realized.push(change);

      continue;
    }

    rejected.push({ change, reason });
  }

  return { realized, rejected };
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

  // If either side is not a plain object, treat this position as a leaf and emit a change when the values differ. Treating every non-plain value as a leaf is
  // the right behavior for Config - it is JSON-shaped and contains no class instances or arrays-of-objects whose elements should diff independently.
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
 * Deep equality check for leaf values. Stringification via JSON.stringify normalizes nested arrays and objects, which is sufficient for Config because it is
 * JSON-shaped throughout. Each value is wrapped in a single-element array before it is stringified, and that wrap treats an absent or undefined leaf and a
 * null leaf as equal, because both serialize as [null]: a leaf that moves between absent and null reports no change.
 * @param a - First value.
 * @param b - Second value.
 * @returns True if the values are deeply equal.
 */
function deepEqual(a: unknown, b: unknown): boolean {

  return JSON.stringify([a]) === JSON.stringify([b]);
}
