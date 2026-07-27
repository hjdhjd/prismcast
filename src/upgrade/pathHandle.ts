/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * pathHandle.ts: A narrow value type that wraps an absolute filesystem path with platform-aware structural query primitives. This module is the single source of
 * truth for path semantics in the upgrade-detection layer - separator selection, case-folding rules, segment splitting, and prefix containment all live here. The
 * detection strategies receive a PathHandle (not a raw string) and reason about install layout exclusively through its narrow API. By construction a strategy
 * cannot write `currentFile.includes("/node_modules/")` and accidentally hard-code POSIX semantics into a cross-platform code path, because PathHandle has no
 * `.includes` method...the only way to test for a "node_modules/prismcast" run in the path is `hasSegmentChain("node_modules", "prismcast")`, which dispatches to
 * the right separator and case-folding rule automatically.
 *
 * The factory accepts an explicit platform override so cross-platform tests can produce Windows-flavored handles on a Linux CI host. Internally, the factory
 * dispatches every path operation to `path.win32` or `path.posix` rather than the ambient `path` module, so behavior is deterministic regardless of where the
 * host process runs. macOS is treated as case-sensitive, matching Node's `path.posix` semantics; APFS volumes with case-sensitive flags work correctly, and
 * macOS users on default case-insensitive volumes are unaffected because `import.meta.url` and any subprocess output agree on canonical casing in the install
 * layouts PrismCast targets.
 *
 * The API is deliberately small. `raw` is the lone escape hatch, used for handoff to subprocess cwd (the npm-local resolver's packageDir) and for display. Every
 * other operation is a structural query or platform-aware composition, so strategy authors never need to reason about separators or case-folding rules.
 */
import path from "node:path";

/**
 * A platform-aware handle on an absolute filesystem path. Strategies reason about install layout exclusively through this type; the underlying separator,
 * case-folding rules, and segment topology are encapsulated. Adding a new structural query is an additive edit to this interface; the factory below is the only
 * implementation site.
 */
export interface PathHandle {

  // The original OS-native absolute path string, preserved verbatim. The only legitimate consumers are subprocess cwd handoff (npm install needs an OS-native
  // path) and human-facing display. Strategies never compare or transform this directly...they call the structural methods below.
  readonly raw: string;

  // Reports whether the path contains a consecutive run of named segments. Case-folded on Windows, case-sensitive on POSIX. An empty chain returns false; the
  // vacuous-truth alternative would invite accidental matches when a rest-parameter expansion produces an empty list.
  hasSegmentChain(...segments: readonly string[]): boolean;

  // Reports whether the path is a strict descendant of the supplied prefix. The prefix is interpreted with the same platform semantics as the handle. A
  // non-absolute or empty prefix returns false defensively...relative containment is undefined for absolute filesystem paths, and an empty prefix would
  // trivially match everything.
  isUnder(prefix: string): boolean;

  // Joins additional segments onto the handle's raw path using the platform-native separator and returns the resulting raw path string. The standard composition
  // primitive for callers that need to construct a child path (e.g., the npm-local resolver building the manifest path under the discovered project root).
  join(...segments: readonly string[]): string;

  // Returns a new PathHandle anchored at the path slice ending just before the first occurrence of the supplied segment chain. Returns null when the chain does
  // not appear, when it appears at position zero (no parent above it), or when the would-be parent is not a meaningful absolute path (e.g., a bare Windows drive
  // letter, the POSIX root with no segments above the chain). The new handle inherits this handle's platform, so further queries follow the same semantics.
  parentBefore(...segments: readonly string[]): PathHandle | null;
}

/**
 * Options accepted by createPathHandle. The platform override lets tests produce Windows-flavored handles on a Linux host.
 */
export interface PathHandleOptions {

  // The platform whose path conventions govern this handle. Defaults to process.platform, which is the right answer in production. Tests set this explicitly to
  // exercise cross-platform behavior deterministically without needing a Windows CI runner.
  readonly platform?: NodeJS.Platform;
}

/**
 * Constructs a PathHandle from an absolute path string. The factory captures the platform once and dispatches every subsequent query through `path.win32` or
 * `path.posix` accordingly, so behavior is independent of where the host process runs. The handle is logically immutable...returned fields are typed readonly
 * and the closed-over state is never mutated.
 *
 * @param raw - The OS-native absolute path string. Empty input produces a degenerate handle whose queries return false/null; a relative input is not rejected
 * (hasSegmentChain still matches its segments), so callers must supply an absolute path.
 * @param options - Optional overrides; the platform override lets tests exercise cross-platform behavior deterministically.
 * @returns A platform-aware PathHandle.
 */
export function createPathHandle(raw: string, options: PathHandleOptions = {}): PathHandle {

  const platform = options.platform ?? process.platform;
  const isWindows = (platform === "win32");
  const pathLib = isWindows ? path.win32 : path.posix;

  // Case-folding rule. Windows is the only platform where the OS treats the filesystem as case-insensitive for path comparisons. POSIX (including macOS at the
  // Node API layer) is case-sensitive. Folding both sides of every comparison consistently is sufficient; we do not need to canonicalize the raw path itself.
  const fold = (segment: string): string => isWindows ? segment.toLowerCase() : segment;

  // Pre-computed segment lists. rawSegments preserves the original casing for path reconstruction (parentBefore needs to splice the original spelling back
  // together so the resulting handle's raw path is byte-faithful to the input). normalized is the folded form used for every comparison.
  const rawSegments = raw.split(pathLib.sep);
  const normalized = rawSegments.map(fold);

  // Locates the first index at which a normalized segment chain matches the path. Returns -1 when there is no match, and also when the chain is empty...we
  // explicitly refuse the vacuous-truth case so an empty rest-parameter expansion never produces a surprising hit.
  const findChain = (chain: readonly string[]): number => {

    if(chain.length === 0) {

      return -1;
    }

    const target = chain.map(fold);
    const lastStart = normalized.length - target.length;

    for(let i = 0; i <= lastStart; i++) {

      if(target.every((segment, j) => normalized[i + j] === segment)) {

        return i;
      }
    }

    return -1;
  };

  return {

    hasSegmentChain(...segments: readonly string[]): boolean {

      return findChain(segments) !== -1;
    },

    isUnder(prefix: string): boolean {

      // Defensive guard. An empty or relative prefix is not anchorable in the install-detection sense...relative containment is undefined for absolute filesystem
      // paths, and an empty prefix would trivially match every path, which is never useful here.
      if(!prefix || !pathLib.isAbsolute(prefix)) {

        return false;
      }

      // We compare segment lists rather than calling pathLib.relative because Node's relative() has implementation-defined case-handling on Windows that varies
      // by version and is undocumented at the API surface. A hand-rolled split-and-compare gives us one explicit rule that we own and can test. Empty segments
      // (which arise from leading separators on POSIX and the UNC double-leading on Windows) are filtered out symmetrically so the comparison is well-defined
      // for every absolute layout we care about.
      const prefixSegments = pathLib.normalize(prefix).split(pathLib.sep).map(fold).filter((segment) => segment.length > 0);
      const haystack = normalized.filter((segment) => segment.length > 0);

      if((prefixSegments.length === 0) || (prefixSegments.length >= haystack.length)) {

        return false;
      }

      for(let i = 0; i < prefixSegments.length; i++) {

        if(haystack[i] !== prefixSegments[i]) {

          return false;
        }
      }

      return true;
    },

    join(...segments: readonly string[]): string {

      return pathLib.join(raw, ...segments);
    },

    parentBefore(...segments: readonly string[]): PathHandle | null {

      const index = findChain(segments);

      // index <= 0 means no match, or a match at position zero with no parent above it. Either way there is no meaningful parent path to return.
      if(index <= 0) {

        return null;
      }

      const parentRaw = rawSegments.slice(0, index).join(pathLib.sep);

      // The reconstructed parent must be a meaningful absolute path. A bare Windows drive letter ("C:") or the POSIX root with no segments above the chain
      // ("") fails this check and we return null...these are degenerate install layouts (prismcast at the filesystem root) that have no usable project root.
      if(!pathLib.isAbsolute(parentRaw)) {

        return null;
      }

      return createPathHandle(parentRaw, { platform });
    },

    raw
  };
}
