/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * integration.helpers.ts: Foundation for the integration / e2e test tier under test/. Every integration suite composes the primitives here - per-test temp
 * data directory, cleanup registry, persisted-JSON read/write helpers, optional full-subsystem boot. The pattern is deliberately minimal and opinionated:
 * tests describe what they need by what they call, not by inheriting boot behavior they may not want.
 *
 * The disposal model uses the ES2024 explicit-resource-management protocol (Symbol.asyncDispose / `await using`). Tests acquire a context and the language
 * guarantees its disposer runs exactly once at scope exit, regardless of whether the body resolved or threw. When both the body and the disposer throw, the
 * language wraps them in a native SuppressedError so neither failure is lost. We never need a finally block, never need to rethrow, and the call site reads
 * top-to-bottom like sync code:
 *
 *   test("...", async () => {
 *
 *     await using ctx = await createIntegrationContext();
 *
 *     await mutateChannels((data) => { data.channels["test"] = { name: "T", url: "https://example.test/" }; });
 *     // assertions...
 *   });
 *
 * Two design rules govern this module:
 *
 *   1. Disposal is structural, not by convention. The context exposes [Symbol.asyncDispose], so tests that bind it via `await using` cannot forget to dispose
 *      and cannot dispose more than once. Resource leaks - the #1 cause of flaky integration suites - are ruled out by the language, not by code review.
 *
 *   2. Boot is composable, not monolithic. A test that exercises only the file-store framework should not pay the cost of loading channels, profiles, and
 *      health. createIntegrationContext gives you the bare temp dir; helpers like initializePersistence layer on top when wanted. Suites pick the right level
 *      for the surface they're testing.
 *
 * Production module-level singletons (file stores, in-memory caches) survive across tests in the same Node process. The temp data directory plus a per-test
 * call to initializeDataDir (which createIntegrationContext does at construction time) ensures stores read from and write to a fresh tree without needing to
 * reset module state - the deferred path resolvers built into the file-store framework pick up the new directory automatically.
 */
import { getDataDir, initializeDataDir } from "../../src/config/paths.ts";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import type { AddressInfo } from "node:net";
import type { Express } from "express";
import type { Server } from "node:http";
import { delay } from "../../src/utils/delay.ts";
import { ensureAllMigrated } from "../../src/config/persistence.ts";
import express from "express";
import { formatError } from "../../src/utils/errors.ts";
import { initializeUserChannels } from "../../src/config/userChannels.ts";
import { initializeUserProfiles } from "../../src/config/userProfiles.ts";
import { loadHealthState } from "../../src/config/health.ts";
import os from "node:os";
import path from "node:path";
import { setupRoutes } from "../../src/routes/index.ts";

/**
 * Per-test execution context. Tests interact with the integration tier exclusively through this surface so the harness owns isolation and cleanup. Direct
 * filesystem access against ctx.dataDir is permitted and expected; the harness only wraps the operations that have meaningful failure modes (json shape
 * narrowing, missing-file handling).
 *
 * The interface is shared between the body of a test (which uses dataDir / registerCleanup) and the disposal protocol (which the language calls). Tests
 * should never call [Symbol.asyncDispose] directly - the `await using` declaration handles that.
 */
export interface IntegrationContext {

  /**
   * Absolute path to the per-test temporary data directory. Production initializers point at this via initializeDataDir() (called by createIntegrationContext);
   * ad-hoc fs operations resolve relative paths against it. Always exists for the duration of the binding scope.
   */
  readonly dataDir: string;

  /**
   * Registers a cleanup function that runs at disposal regardless of whether the test body resolved or threw. Cleanups drain in LIFO order so resources
   * acquired later (e.g., a server built on top of a temp dir) tear down before their dependencies.
   *
   * Cleanup functions need not be safe to call more than once, because the disposal protocol guarantees [Symbol.asyncDispose] is called exactly once - the
   * language structurally prevents re-entry. A cleanup that throws does not stop subsequent cleanups from running; all errors are accumulated and surfaced
   * via AggregateError when more than one fires, or directly when only one does.
   *
   * @param fn - Cleanup callback. May be sync or async; both are awaited.
   */
  registerCleanup(fn: () => Promise<void> | void): void;
}

/**
 * Disposable variant returned by createIntegrationContext. Equivalent to IntegrationContext at the type level except for the [Symbol.asyncDispose] member,
 * which the language uses when the context is bound via `await using`. Exported so test bodies that need to thread the context through helpers can declare the
 * parameter type explicitly when they want to advertise that the helper is allowed to register cleanup hooks.
 */
export type DisposableIntegrationContext = IntegrationContext & AsyncDisposable;

/**
 * Provisions a fresh integration context: creates a unique temp directory under os.tmpdir() with a prismcast-prefix and points production module path
 * resolvers at it via initializeDataDir(). Bind with `await using` so the context's disposer drains every registered cleanup hook (LIFO) and removes the temp
 * directory at scope exit.
 *
 * The disposer accumulates every cleanup-side failure into an array and surfaces them as: nothing thrown when the array is empty, the underlying error
 * directly when the array has exactly one entry (so test output points at the real failure), or AggregateError when multiple cleanups failed (so no failure
 * is silently lost). Test-body failures propagate through the language's normal throw semantics - if the body throws and the disposer also throws, the
 * language wraps them in SuppressedError; we do nothing special.
 *
 * Because initializeDataDir is a module-level mutator on the production paths module, two contexts cannot meaningfully coexist - the second context's
 * initializeDataDir overwrites the first's resolution. Tests that need parallel scenarios should run them across separate test cases (each with its own
 * context) rather than nest contexts within a single case.
 *
 * @returns A disposable integration context. Dispose via `await using ctx = await createIntegrationContext()`.
 */
export async function createIntegrationContext(): Promise<DisposableIntegrationContext> {

  const dataDir = await mkdtemp(path.join(os.tmpdir(), "prismcast-integ-"));
  const cleanups: (() => Promise<void> | void)[] = [];

  // Point production resolvers at the temp dir before any test code runs. This is structurally necessary for safety - a production module that imports the
  // path resolver and writes to its file (every config module does) would otherwise corrupt the user's real ~/.prismcast directory.
  initializeDataDir(dataDir);

  return {

    dataDir,

    registerCleanup(fn): void {

      cleanups.push(fn);
    },

    async [Symbol.asyncDispose](): Promise<void> {

      // Drain registered cleanups in LIFO order. Sequential by design: cleanups may depend on prior cleanups completing (LIFO ordering reflects acquisition
      // dependencies), and parallelizing would race those dependencies. We accumulate every failure into an array rather than stopping at the first one so a
      // single bad cleanup does not strand the rest.
      const errors: unknown[] = [];

      while(cleanups.length > 0) {

        const cleanup = cleanups.pop();

        try {

          // eslint-disable-next-line no-await-in-loop -- LIFO sequential cleanup is the contract; parallelizing would break dependent cleanups.
          await cleanup?.();
        } catch(err) {

          errors.push(err);
        }
      }

      // Remove the temp dir. We treat its failure the same as a cleanup failure - accumulate, surface alongside any other errors below.
      try {

        await rm(dataDir, { force: true, recursive: true });
      } catch(err) {

        errors.push(err);
      }

      if(errors.length === 0) {

        return;
      }

      // Single error: surface it directly so the test runner's output points at the actual cause rather than wrapping it in an AggregateError of one. Wrap a
      // non-Error value in an Error to satisfy the only-throw-error contract; this preserves the stringified value as the wrapped message.
      if(errors.length === 1) {

        const sole = errors[0];

        throw (sole instanceof Error) ? sole : new Error("createIntegrationContext: cleanup threw a non-Error value: " + formatError(sole));
      }

      throw new AggregateError(errors, "createIntegrationContext: " + String(errors.length) + " cleanup operations failed during disposal");
    }
  };
}

/**
 * Resolves a path inside the context's data directory. Convenience wrapper around path.join with the dataDir as the root segment; saves call sites from
 * importing path and threading dataDir explicitly.
 * @param ctx - The integration context.
 * @param segments - Path segments under the data directory.
 * @returns The absolute path.
 */
export function pathInDataDir(ctx: IntegrationContext, ...segments: string[]): string {

  return path.join(ctx.dataDir, ...segments);
}

/**
 * Reads and JSON-parses a file inside the context's data directory. Returns the parsed value as `unknown` - callers must narrow at the assertion site, where
 * the test author knows the shape they expect. Returning a typed value via `as T` would be a lie because the on-disk shape is whatever production code wrote,
 * not whatever the call site declares.
 *
 * Use for asserting on-disk state that production code wrote during the test. For seeding initial state (where the caller fully controls the shape), use
 * writePersistedJson.
 *
 * @param ctx - The integration context.
 * @param filename - File name under the data directory (e.g., "channels.json"). Subdirectories are permitted via path separators.
 * @returns The parsed JSON content as unknown. Caller narrows.
 * @throws The underlying read or parse error if the file is missing or malformed.
 */
export async function readPersistedJson(ctx: IntegrationContext, filename: string): Promise<unknown> {

  const raw = await readFile(pathInDataDir(ctx, filename), "utf8");

  return JSON.parse(raw);
}

/**
 * Writes a value as JSON into a file under the context's data directory, creating any missing parent directories. Use for seeding initial state that the test
 * body will then load through production code paths.
 * @param ctx - The integration context.
 * @param filename - File name under the data directory.
 * @param data - The value to serialize. JSON.stringify must succeed against it; helper does not pre-validate.
 */
export async function writePersistedJson(ctx: IntegrationContext, filename: string, data: unknown): Promise<void> {

  const target = pathInDataDir(ctx, filename);

  await mkdir(path.dirname(target), { recursive: true });
  await writeFile(target, JSON.stringify(data, null, 2), "utf8");
}

/**
 * Boots the persistence subsystem against the context's data directory: runs the user-profiles, channels, and health loaders in the same order app.ts uses at
 * startup. Use this in suites that exercise multiple stores together (e.g., cross-store consistency, settings preservation). Suites that test a single store
 * directly should not call this - they create their own FileStore via createFileStore and avoid the cost of unrelated loads.
 *
 * Production loaders also register their own intervals or background work in some cases; this helper does not call those code paths because the integration
 * harness has no use for them. If a future suite needs interval-driven behavior, add a focused boot helper for that scenario rather than expanding this one.
 *
 * @param ctx - The integration context. The data directory is already pointed at ctx.dataDir by createIntegrationContext - this helper trusts that guarantee
 *   and does not re-call initializeDataDir.
 */
export async function initializePersistence(ctx: IntegrationContext): Promise<void> {

  // Guard assertion: the production resolver must already point at ctx.dataDir. This is the guarantee createIntegrationContext establishes; any code path
  // that re-pointed initializeDataDir between context creation and this call would otherwise cause initializePersistence to load against the wrong tree
  // (silent corruption). The check turns that failure mode from silent into loud.
  if(getDataDir() !== ctx.dataDir) {

    throw new Error("initializePersistence: production data-dir resolver returns " + getDataDir() + " but ctx.dataDir is " + ctx.dataDir +
      ". Call createIntegrationContext() and use it via `await using` without re-pointing the data dir between then and this call.");
  }

  // Run migrations across every registered store and persist the upgrades to disk - mirrors the release boot coordinator's call sequence in app.ts. Tests
  // that seed v1/v2 fixtures rely on this to land the migration on disk before the load functions hydrate module state from the post-migration content.
  await ensureAllMigrated();

  await initializeUserProfiles();
  await initializeUserChannels();
  await loadHealthState();
}

/**
 * The shape returned by bootApp. Carries the app + ephemeral port + a urlFor convenience for composing test request URLs against the listener.
 */
export interface BootedApp {

  /** The Express application after setupRoutes has been called. Useful for tests that want to mount additional helpers or inspect handlers. */
  app: Express;

  /** Bound port number (ephemeral - assigned by the OS at listen time). */
  port: number;

  /**
   * The Node HTTP server backing the listener. Exposed for tests that need the server itself rather than the Express app: the WebSocket upgrade path binds to
   * the server's "upgrade" event, which the app never sees.
   */
  server: Server;

  /** Composes a full URL against the listener for the given path (must start with "/"). Convenience over manual string concatenation. */
  urlFor: (path: string) => string;
}

/**
 * Boots a minimal Express app on an ephemeral port and registers the production routes (setupRoutes) against it. Designed for HTTP-level integration tests
 * that exercise route handlers end-to-end without the full app.ts startup ceremony (signal handlers, intervals, browser launch). The app listens on an
 * OS-assigned port (port 0) so multiple suites can run concurrently without port collisions.
 *
 * Cleanup is automatic via ctx.registerCleanup: the listener is closed at context disposal in LIFO order, before the temp dir is removed. The "trust proxy"
 * Express setting is enabled because the playlist endpoint and others read req.protocol via X-Forwarded-Proto in production behind reverse proxies; tests
 * should see the same resolution path.
 *
 * @param ctx - The integration context. Persistence must already be initialized (initializePersistence) before calling bootApp because the route handlers
 *   read CONFIG and the in-memory channel state at handler-invocation time.
 * @returns A BootedApp carrying the app, the bound port, the HTTP server behind the listener, and a URL composer.
 */
export async function bootApp(ctx: IntegrationContext): Promise<BootedApp> {

  const app = express();

  // Trust proxy enables X-Forwarded-* awareness used by playlist's resolveBaseUrl. Mirror app.ts's setting so tests behave identically.
  app.set("trust proxy", true);

  // Body parsing middleware. Production buildApp installs both - the urlencoded parser handles HTML form posts (settings page) and the JSON parser handles
  // API client posts (every config endpoint). Without these, req.body is undefined and route handlers throw on body access.
  app.use(express.urlencoded({ extended: true }));
  app.use(express.json());

  setupRoutes(app);

  // Listen on an ephemeral port. The OS picks an unused one, returned via server.address(). We capture the resolved port for the urlFor composer.
  const server: Server = await new Promise((resolve, reject) => {

    const s = app.listen(0, "127.0.0.1", () => { resolve(s); });

    s.on("error", reject);
  });

  // The listener is bound to a TCP host:port (never a Unix socket), and this line runs only after the listen callback above already resolved, so
  // server.address() is guaranteed to return an AddressInfo here rather than the string or null shapes the type also allows.
  const address = server.address() as AddressInfo;
  const port = address.port;

  // Register the listener close as a cleanup hook. server.close() is async; we promisify it so the disposer awaits it before moving on. The hook fires LIFO,
  // so any test-registered cleanup added AFTER bootApp drains before this one - usually the right order (test-side state cleanup before the listener teardown).
  ctx.registerCleanup(async () => {

    await new Promise<void>((resolve) => { server.close(() => { resolve(); }); });
  });

  return {

    app,
    port,
    server,
    urlFor: (subPath: string): string => "http://127.0.0.1:" + String(port) + subPath
  };
}

/**
 * Wait long enough for the health module's debounced flush timer to fire and the file-store write that follows it to settle on disk. Health writes are
 * fire-and-forget through markChannelSuccess / markChannelFailure / markDomainAuth, which schedule the actual disk update on a 2-second debounce (`FLUSH_DELAY`
 * in `src/config/health.ts`). Tests that need to verify on-disk health state - or, contrapositively, that need to confirm an unrelated mutation did NOT
 * accidentally write to health.json - must wait past that debounce plus a small headroom for the framework's atomic write to land.
 *
 * Shared by the health-state suite and the cross-store-isolation suite (and any future suite that asserts on health-vs-other-store interactions) so they wait
 * on the same primitive without re-deriving the timing constant. The wait is bounded so a regression that stalls the flush surfaces as a test timeout, not as
 * a silent hang. Health-side timing changes (e.g., bumping `FLUSH_DELAY`) tighten or loosen this single constant rather than rippling through every caller.
 */
export async function waitForHealthFlush(): Promise<void> {

  // Health module's debounce is 2000ms (FLUSH_DELAY in src/config/health.ts). 2500ms gives the debounce plus the file-store write a comfortable margin.
  await delay(2500);
}

/**
 * Boots a minimal Express app on an ephemeral port for use as an upstream stub - typically a fake HLS origin, a fake DVR, or any other external HTTP service the
 * production code under test would normally reach across the network. The caller's `configure` callback installs whatever routes the stub needs; the harness
 * provides only the bind, the urlFor composer, and the LIFO cleanup hook.
 *
 * Deliberately minimal. bootApp installs production middleware (trust proxy, body parsers) because production routes assume those. bootStubServer installs
 * NOTHING - the stub is an opaque counterparty whose behavior is entirely the test's responsibility. If a stub needs body parsing, request logging, or any
 * other middleware, the test installs it in `configure` so the dependency is visible at the test site rather than buried in the harness. This is the same
 * cohesion principle bootApp follows for production: middleware lives where the responsibility lives.
 *
 * @param ctx - The integration context. The listener is closed via the context's cleanup hook at scope exit.
 * @param configure - Callback that receives the bare Express app. Install routes, middleware, or any other handlers here.
 * @returns The app, the bound ephemeral port, the HTTP server behind the listener, and a urlFor composer that builds full URLs against it.
 */
export async function bootStubServer(ctx: IntegrationContext, configure: (app: Express) => void): Promise<BootedApp> {

  const app = express();

  configure(app);

  // Listen on an ephemeral port. The stub binds to 127.0.0.1 only - it has no business serving traffic from elsewhere on the host.
  const server: Server = await new Promise((resolve, reject) => {

    const s = app.listen(0, "127.0.0.1", () => { resolve(s); });

    s.on("error", reject);
  });

  // The listener is bound to a TCP host:port (never a Unix socket), and this line runs only after the listen callback above already resolved, so
  // server.address() is guaranteed to return an AddressInfo here rather than the string or null shapes the type also allows.
  const address = server.address() as AddressInfo;
  const port = address.port;

  ctx.registerCleanup(async () => {

    await new Promise<void>((resolve) => { server.close(() => { resolve(); }); });
  });

  return {

    app,
    port,
    server,
    urlFor: (subPath: string): string => "http://127.0.0.1:" + String(port) + subPath
  };
}
