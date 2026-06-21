/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * detection.ts: Installation method detection for PrismCast upgrade.
 *
 * The architecture is ports-and-adapters with a strategy registry whose entries are the single source of truth for everything PrismCast knows about its
 * installation methods - including the InstallMethod type, which is derived from the registry rather than declared in parallel with it.
 *
 *   - DetectionContext (this file) - the runtime-input port; what every strategy needs to make its decision
 *   - ResolvableFields (this file) - the narrow set of InstallInfo fields a strategy's resolver may compute (today: packageDir)
 *   - InstallStrategy<M> (this file) - a discriminated union over `upgradeable`; non-upgradeable variants carry manualUpgradeMessage
 *   - INSTALL_STRATEGIES (this file) - the registry; the SSOT for what install methods PrismCast recognizes, in priority order
 *   - RegisteredMethod, InstallMethod (derived) - the literal-union types produced from the registry; cannot drift from the strategies
 *   - InstallInfo (this file) - the dispatcher's output; mirrors the strategy union so non-upgradeable infos carry the message
 *   - detectInstallMethod (this file) - the dispatcher; a fold over the registry that builds the right InstallInfo variant
 *   - PathHandle (pathHandle.ts) - the platform-aware value type wrapping the running module's filesystem location; the SSOT for path semantics (separator
 *     selection, case-folding, segment topology). Strategies query it through a narrow structural API and never see a raw path string, which makes the
 *     forward-slash-vs-backslash class of cross-platform bugs structurally inexpressible at the strategy layer.
 *   - createDefaultDetectionContext (detection.context) - the adapter; the only place that reads import.meta.url, runs subprocesses, or touches the FS
 *
 * Adding a new install method is appending one record to INSTALL_STRATEGIES. The InstallMethod type updates automatically. The dispatcher and the adapter never
 * change. Tests construct DetectionContext literals inline; production calls detectInstallMethod() with no arguments and the default-argument wires through to
 * createDefaultDetectionContext.
 *
 * Why the discriminated union on `upgradeable`. Boolean flags that gate the presence of other fields are a smell - they let callers ask for fields that are not
 * meaningful in the current state, and they push the responsibility for "is this field present?" onto every consumer's runtime check. Splitting the strategy
 * (and its corresponding InstallInfo) into upgradeable vs non-upgradeable variants lets TypeScript enforce that manualUpgradeMessage exists exactly when
 * upgradeable is false. Consumers narrow on `if(!info.upgradeable)` and read the message field with full type safety; the structural impossibility of an
 * upgradeable strategy carrying manual instructions, or a non-upgradeable one omitting them, is enforced at the type level rather than by convention.
 */
import type { PathHandle } from "./pathHandle.ts";
import { createDefaultDetectionContext } from "./detection.context.ts";

/**
 * The runtime context detection consumes. Each field is one narrow capability the decision logic depends on - the path of the running module, whether we are
 * inside a Docker container, the ability to run a shell command and read its trimmed stdout, the ability to check that a path exists. Decision logic is a pure
 * function of this shape; the default adapter wires it from real I/O, tests pass a context literal.
 */
export interface DetectionContext {

  // The platform-aware handle on the running module's filesystem location, constructed at the boundary from import.meta.url. Used by the path-pattern strategies
  // (homebrew, npm-*), which query it through PathHandle's structural API rather than reasoning about separators directly. The PathHandle type is the SSOT for
  // path semantics in the detection layer...see pathHandle.ts.
  readonly currentFile: PathHandle;

  // Filesystem existence check. Used by the npm-local strategy to verify the project root has a package.json.
  readonly fileExists: (path: string) => boolean;

  // Whether the process is running inside a container. Used by the docker strategy.
  readonly isContainer: boolean;

  // Subprocess runner. Returns trimmed stdout on success, null on failure (timeout, non-zero exit, command-not-found, etc.). Used by the npm-global strategy
  // to resolve the npm global prefix via `npm prefix -g`.
  readonly runCommand: (cmd: string) => string | null;
}

/**
 * The narrow set of InstallInfo fields a strategy's resolver may compute from the runtime context. Today there's only one - npm-local's packageDir, derived
 * from the consumer's project root - but the dedicated type guarantees the resolver cannot accidentally override the strategy's constant fields. Adding a new
 * resolvable field is an additive edit here; strategies opt in by declaring the field on their resolve return.
 */
export interface ResolvableFields {

  // For npm-local: the absolute path of the consumer's project directory (the directory above node_modules), present when that directory has a package.json.
  // The upgrade runner uses this as the cwd for `npm install`.
  readonly packageDir?: string;
}

/**
 * Common fields shared by every install strategy. Generic in the literal id type so each strategy declaration carries its own narrow id (e.g.,
 * InstallStrategyBase<"docker">), which the registry then harvests into the RegisteredMethod literal union.
 *
 * Strategies own no state and perform no I/O outside what the context exposes.
 */
interface InstallStrategyBase<M extends string = string> {

  // Human-readable label for the install method, used by the upgrade command's status output. Co-located with the rest of the per-method facts so adding a new
  // install method is a single record.
  readonly displayName: string;

  // The install method this strategy detects. Becomes the InstallInfo.method field directly; the dispatcher does not let resolvers override it.
  readonly id: M;

  // Predicate that returns true when this strategy matches the runtime context. Pure function of DetectionContext; no side effects beyond what runCommand and
  // fileExists do, and those are themselves contracts of the context.
  readonly matches: (ctx: DetectionContext) => boolean;

  // Optional resolver for context-derived fields. Called only when matches() returns true. Returns a ResolvableFields object whose entries are layered into
  // the final InstallInfo before the strategy's constants are overlaid - the dispatcher orders the spread so constants always win, making this hook
  // structurally incapable of overriding method, displayName, or upgradeCommand.
  readonly resolve?: (ctx: DetectionContext) => ResolvableFields;

  // The shell command the user (or the tool) would run to upgrade. For upgradeable strategies the runner executes it; for non-upgradeable strategies the
  // command is shown as part of the manual instructions.
  readonly upgradeCommand: string;
}

/**
 * Strategy variant for install methods the tool can upgrade in place. The runner invokes upgradeCommand directly; no manual-instruction strings are needed.
 */
export interface UpgradeableInstallStrategy<M extends string = string> extends InstallStrategyBase<M> {

  readonly upgradeable: true;
}

/**
 * Strategy variant for install methods the tool cannot upgrade in place. Carries manualUpgradeMessage - the user-facing prose lines that explain why the
 * automated path is unavailable and what the user should do instead. The dispatcher copies this onto the resulting InstallInfo so the consumer (commands.ts)
 * never has to switch on the install method to choose its messaging.
 */
export interface NonUpgradeableInstallStrategy<M extends string = string> extends InstallStrategyBase<M> {

  // The prose lines printed before the indented upgradeCommand. Strategy-specific (docker says "Docker containers cannot be upgraded in-place" first; the
  // unknown sentinel says "Unable to detect installation method"). Owning this on the strategy keeps install-method-specific knowledge in one place.
  readonly manualUpgradeMessage: readonly string[];

  readonly upgradeable: false;
}

/**
 * Discriminated union of install-method strategies. Discriminator is `upgradeable`. Consumers narrow on it to access manualUpgradeMessage with type safety;
 * structurally impossible to declare an upgradeable strategy that carries manual instructions or a non-upgradeable strategy that omits them.
 */
export type InstallStrategy<M extends string = string> = UpgradeableInstallStrategy<M> | NonUpgradeableInstallStrategy<M>;

/**
 * Docker strategy. Matched via the container-environment probe rather than the path - inside a container, path-based detection is irrelevant because the path
 * looks like any other npm install but the upgrade flow is fundamentally different (recreate the container, not run a package manager). Carries the manual
 * upgrade message the consumer prints before the indented command.
 */
const DOCKER_STRATEGY = {

  displayName: "Docker",
  id: "docker",
  manualUpgradeMessage: [

    "Docker containers cannot be upgraded in-place.",
    "To upgrade, pull the latest image and recreate the container:"
  ],
  matches: (ctx: DetectionContext): boolean => ctx.isContainer,
  upgradeCommand: "docker pull ghcr.io/hjdhjd/prismcast:latest && docker compose up -d",
  upgradeable: false
} as const satisfies InstallStrategy<"docker">;

/**
 * Homebrew strategy. Matched via the "Cellar" + "prismcast" segment chain. Homebrew formula installs always live under <prefix>/Cellar/<formula>/<version>/, so
 * the pair is a reliable discriminator on both Intel (/usr/local/Cellar) and Apple Silicon (/opt/homebrew/Cellar) layouts. The chain is deliberately the pair
 * rather than just "Cellar" so an npm-on-Homebrew install at /opt/homebrew/lib/node_modules/prismcast/ does not false-positive...that path contains "homebrew"
 * but not "Cellar/prismcast".
 */
const HOMEBREW_STRATEGY = {

  displayName: "Homebrew",
  id: "homebrew",
  matches: (ctx: DetectionContext): boolean => ctx.currentFile.hasSegmentChain("Cellar", "prismcast"),
  upgradeCommand: "brew update && brew upgrade prismcast",
  upgradeable: true
} as const satisfies InstallStrategy<"homebrew">;

/**
 * npm global strategy. Matched in two stages: a cheap path prefilter (the running file must contain a "node_modules" segment), then the authoritative subprocess
 * call to `npm prefix -g` followed by a structural is-under check against the reported prefix. The prefilter avoids spawning npm for paths that obviously cannot
 * be an npm install (e.g., a development checkout, a Homebrew Cellar path), keeping the matches predicate inexpensive in the common no-match case. Both checks
 * flow through PathHandle so the predicate is platform-agnostic by construction.
 */
const NPM_GLOBAL_STRATEGY = {

  displayName: "npm (global)",
  id: "npm-global",
  matches: (ctx: DetectionContext): boolean => {

    if(!ctx.currentFile.hasSegmentChain("node_modules")) {

      return false;
    }

    const globalPrefix = ctx.runCommand("npm prefix -g");

    if(!globalPrefix) {

      return false;
    }

    return ctx.currentFile.isUnder(globalPrefix);
  },
  upgradeCommand: "npm install -g prismcast@latest",
  upgradeable: true
} as const satisfies InstallStrategy<"npm-global">;

/**
 * npm local strategy. Matched via the "node_modules" + "prismcast" segment chain; the resolver walks the same chain via parentBefore to recover the consumer's
 * project root (the directory above node_modules) and confirms the manifest is actually there via fileExists. Some unusual layouts have an orphaned node_modules
 * without a manifest, and the upgrade runner needs the right cwd for `npm install`, so we only surface packageDir when we can prove the manifest exists.
 */
const NPM_LOCAL_STRATEGY = {

  displayName: "npm (local)",
  id: "npm-local",
  matches: (ctx: DetectionContext): boolean => ctx.currentFile.hasSegmentChain("node_modules", "prismcast"),
  resolve: (ctx: DetectionContext): ResolvableFields => {

    const projectRoot = ctx.currentFile.parentBefore("node_modules", "prismcast");

    if(!projectRoot) {

      return {};
    }

    return ctx.fileExists(projectRoot.join("package.json")) ? { packageDir: projectRoot.raw } : {};
  },
  upgradeCommand: "npm install prismcast@latest",
  upgradeable: true
} as const satisfies InstallStrategy<"npm-local">;

/* The strategy registry as a narrow tuple. Declared `as const` so each entry retains its literal id type, which the RegisteredMethod type derivation below
 * harvests. This name is module-private because consumers don't need the narrow tuple type - they iterate via INSTALL_STRATEGIES (the public, wide-typed alias)
 * which carries the InstallStrategy<RegisteredMethod> shape they want.
 */
const STRATEGY_TUPLE = [

  DOCKER_STRATEGY,
  HOMEBREW_STRATEGY,
  NPM_GLOBAL_STRATEGY,
  NPM_LOCAL_STRATEGY
] as const;

/**
 * The literal-union type of install method ids that strategies actually register for. Derived from the strategy tuple rather than declared in parallel - adding
 * a strategy automatically widens this type, removing one is impossible without removing the strategy. There is no way for the type and the registry to drift.
 */
export type RegisteredMethod = typeof STRATEGY_TUPLE[number]["id"];

/**
 * The install-method strategy registry. Same runtime data as STRATEGY_TUPLE; the public name carries the wide InstallStrategy<RegisteredMethod> type so the
 * dispatcher (and any future consumer that iterates) sees the union uniformly. Order is priority order: docker first (container probe is unambiguous), homebrew
 * (path marker is unambiguous), npm-global (requires successful npm prefix lookup), npm-local (broader path marker). Adding a new install method is appending an
 * entry to STRATEGY_TUPLE.
 */
export const INSTALL_STRATEGIES: readonly InstallStrategy<RegisteredMethod>[] = STRATEGY_TUPLE;

/**
 * The full install-method discriminator. Adds the unknown sentinel that the dispatcher returns when no strategy in the registry matches; everything else is
 * registered. Consumers that need exhaustive coverage over the full method set get it via this type.
 */
export type InstallMethod = RegisteredMethod | "unknown";

/**
 * Common fields shared by every InstallInfo variant. Mirrors InstallStrategyBase but with `method: InstallMethod` instead of `id: M`, and extends ResolvableFields
 * so packageDir is optionally present on either variant.
 */
interface InstallInfoBase extends ResolvableFields {

  // Human-readable label for the install method, copied from the matching strategy's record. Consumers display this directly. The unknown sentinel carries
  // "Unknown".
  readonly displayName: string;

  // The discriminator identifying which install method was detected.
  readonly method: InstallMethod;

  // The shell command the user (or the tool) would run to upgrade. Always populated, even for non-upgradeable methods, so callers can show manual instructions.
  readonly upgradeCommand: string;
}

/**
 * InstallInfo variant for upgradeable install methods. The upgrade runner can invoke upgradeCommand directly.
 */
export interface UpgradeableInstallInfo extends InstallInfoBase {

  readonly upgradeable: true;
}

/**
 * InstallInfo variant for non-upgradeable install methods. Carries manualUpgradeMessage - the prose lines printed before the indented upgradeCommand. The
 * structural guarantee that manualUpgradeMessage exists exactly when upgradeable is false means consumers (commands.ts) can narrow on `!info.upgradeable` and
 * read the message with type safety, rather than switching on info.method to pick the right messaging.
 */
export interface NonUpgradeableInstallInfo extends InstallInfoBase {

  // The prose lines the consumer prints before the indented upgradeCommand. Copied from the matching strategy's manualUpgradeMessage; UNKNOWN_INSTALL declares
  // its own.
  readonly manualUpgradeMessage: readonly string[];

  readonly upgradeable: false;
}

/**
 * Information about the detected installation method and how to upgrade. Discriminated union over `upgradeable`; consumers narrow on it to choose between
 * "run the command" and "print manual instructions" without ever switching on info.method.
 */
export type InstallInfo = UpgradeableInstallInfo | NonUpgradeableInstallInfo;

/**
 * The fallback InstallInfo returned when no strategy in the registry matches. Development builds, npx invocations, and unrecognized layouts land here. The
 * upgrade command points at the canonical npm-global install path because that is the most likely successful manual upgrade, but upgradeable is false so the
 * tool prints instructions rather than running the command.
 */
export const UNKNOWN_INSTALL: NonUpgradeableInstallInfo = {

  displayName: "Unknown",
  manualUpgradeMessage: ["Unable to detect installation method. Please upgrade manually:"],
  method: "unknown",
  upgradeCommand: "npm install -g prismcast@latest",
  upgradeable: false
};

/**
 * Detects the installation method. Walks INSTALL_STRATEGIES in priority order; on the first match, builds an InstallInfo by composing the strategy's optional
 * resolver output with the strategy's constant fields. The resolver's return type is restricted to ResolvableFields, so it cannot override method, displayName,
 * or upgradeCommand; the spread order also places constants after the resolved fields as belt-and-suspenders. The strategy's `upgradeable`
 * discriminator selects which InstallInfo variant the dispatcher emits, carrying manualUpgradeMessage through for the non-upgradeable case.
 *
 * Pure function of DetectionContext - all I/O happens inside the context's runCommand and fileExists callbacks, which the default adapter wires to real system
 * calls and tests stub inline. Falls back to UNKNOWN_INSTALL when no strategy matches.
 *
 * @param ctx - The runtime context. Defaults to createDefaultDetectionContext() which builds one from real I/O.
 * @returns Installation information.
 */
export function detectInstallMethod(ctx: DetectionContext = createDefaultDetectionContext()): InstallInfo {

  for(const strategy of INSTALL_STRATEGIES) {

    if(!strategy.matches(ctx)) {

      continue;
    }

    const base = {

      ...strategy.resolve?.(ctx),
      displayName: strategy.displayName,
      method: strategy.id,
      upgradeCommand: strategy.upgradeCommand
    };

    if(strategy.upgradeable) {

      return { ...base, upgradeable: true };
    }

    return { ...base, manualUpgradeMessage: strategy.manualUpgradeMessage, upgradeable: false };
  }

  return UNKNOWN_INSTALL;
}
