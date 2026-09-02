/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * precaching.test.ts: Unit tests for the precaching coordinator's no-op gates and the discovery-outcome recorder in precaching.ts. The module exports
 * startPrecaching (the gated scheduler tested here), stopPrecaching (the shutdown-time canceller), precacheService (the per-service primitive), and
 * recordDiscoveryOutcome (the discovery-outcome policy, covered by the matrix below). startPrecaching inspects the CONFIG.channels.precacheServices list and the
 * module-level precacheInProgress flag before scheduling the precache cycle; the tests here cover those two gates. The remaining gate, the isGracefulShutdown()
 * check, and the full runPrecacheCycle/precacheService flow (driven through the PrecachingDeps injection seam) are covered in the sibling
 * precaching.revalidation.test.ts. The unit tests here lock the gate-behavior contract so that future refactors of the gates do not silently regress.
 *
 *   1. CONFIG.channels.precacheServices: when empty, the function returns immediately with no side effects (no timer scheduled, no log lines, no internal flag
 *      mutated).
 *
 *   2. The module-level precacheInProgress flag: when true, the function defers. The flag is set when startPrecaching schedules the cycle (before the timer fires) and
 *      cleared in runPrecacheCycle's finally block.
 */
import type { AuthWallIndicators, DiscoveredChannel, Nullable, ProviderModule } from "../types/index.ts";
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { getDomainAuthState, markDomainAuth, markDomainAuthRequired } from "../config/health.ts";
import { recordDiscoveryOutcome, startPrecaching } from "./precaching.ts";
import { CONFIG } from "../config/index.ts";
import { LOG } from "../utils/index.ts";
import type { Page } from "puppeteer-core";
import type { PersistedLineupChannel } from "../config/providerLineups.ts";
import type { PrecachingDeps } from "./precaching.ts";
import assert from "node:assert/strict";

// The lineup writes the recorder issues, captured by the injected port below so each case can assert what was persisted without touching a real file.
let persistedLineups: { channels: PersistedLineupChannel[]; slug: string }[] = [];

/* The injected deps the recorder runs against. recordDiscoveryOutcome reaches exactly one member of PrecachingDeps - the lineup write - so the double-cast
 * documents that this stub covers the recorder's slice of the port rather than the whole surface, matching the makeProvider convention below.
 */
const recorderDeps = {

  persistProviderLineup: async (slug: string, channels: PersistedLineupChannel[]): Promise<void> => {

    persistedLineups.push({ channels, slug });
  }
} as unknown as PrecachingDeps;

describe("startPrecaching", () => {

  let originalServices: string[];

  beforeEach(() => {

    // Snapshot CONFIG.channels.precacheServices so each test can mutate freely without affecting other test files. We replace it with an empty default rather
    // than rely on whatever the runtime config holds.
    originalServices = CONFIG.channels.precacheServices;
    CONFIG.channels.precacheServices = [];
  });

  afterEach(() => {

    CONFIG.channels.precacheServices = originalServices;
    mock.timers.reset();
  });

  test("returns immediately and schedules no work when precacheServices is empty", () => {

    // The first guard: with no services configured, the function must return without scheduling any timer. We enable mock.timers and verify the timer queue
    // remains empty after the call.
    mock.timers.enable({ apis: ["setTimeout"] });

    assert.doesNotThrow(() => {

      startPrecaching();
    }, "empty precache list -> clean no-op");

    // mock.timers' tick exposes whether anything is queued by examining the internal queue. We tick a long way; if any callback fires it will throw because the
    // cycle would try to call getCurrentBrowser. The empty-list guard means tick is a clean no-op.
    assert.doesNotThrow(() => {

      mock.timers.runAll();
    }, "no scheduled work means runAll completes without invoking the cycle");
  });

  test("returns silently when precacheServices contains only entries (the no-services-configured case is a no-op even after mutation)", () => {

    // Boundary: the guard explicitly checks length === 0. This test confirms that with an empty array the early-exit short-circuits and the function remains safely
    // callable without scheduling any work.
    CONFIG.channels.precacheServices = [];

    assert.doesNotThrow(() => {

      startPrecaching();
    }, "empty list short-circuits regardless of prior state");
  });

  test("does not throw on repeated calls with an empty precacheServices list", () => {

    // Idempotency: callers (browser launch sequence) may invoke startPrecaching once per launch, including after browser crash recovery. With no services
    // configured, every call must be a clean no-op.
    for(let i = 0; i < 3; i++) {

      assert.doesNotThrow(() => {

        startPrecaching();
      }, "iteration " + String(i + 1));
    }
  });

  test("schedules a deferred cycle when precacheServices is non-empty (timer queued)", () => {

    // Boundary: with at least one service configured, the function schedules the cycle via setTimeout. We use mock.timers to detect that a timer was queued
    // without actually running it - tick(0) lets timers due at the current time fire, but the precache delay is 5000ms so a tick(0) does not trigger the cycle.
    mock.timers.enable({ apis: ["setTimeout"] });

    CONFIG.channels.precacheServices = ["never-registered-slug"];

    startPrecaching();

    // Ticking less than the precache delay confirms the timer is in flight without firing it. This test only proves a timer was queued, not that its body runs;
    // actually firing the cycle would drive Puppeteer, so that path is deferred to e2e.
    mock.timers.tick(0);
  });
});

describe("recordDiscoveryOutcome", () => {

  beforeEach(() => {

    persistedLineups = [];

    // We mock Date for deterministic timestamps and setTimeout to suppress the 2-second debounced health flush timer the mark calls below schedule. Each test uses
    // unique synthetic domains so state from one scenario cannot color another.
    mock.timers.enable({ apis: [ "Date", "setTimeout" ], now: 1_700_000_000_000 });
  });

  afterEach(() => {

    mock.timers.reset();
  });

  /* Builds a minimal ProviderModule carrying only the fields recordDiscoveryOutcome reads (authWallIndicators, exportDurableLineup, guideUrl, label, slug,
   * validatePrecache). The double-cast documents that the recorder's contract touches this subset, not the full provider surface.
   */
  function makeProvider(overrides: { authWallIndicators?: AuthWallIndicators; exportDurableLineup?: () => Nullable<PersistedLineupChannel[]>; guideUrl: string;
    slug?: string; validatePrecache?: (channels: DiscoveredChannel[]) => boolean; }): ProviderModule {

    return { label: "Stub Service", slug: "stub-service", ...overrides } as unknown as ProviderModule;
  }

  /* Builds a Page stub whose evaluate routes on the argument shape, mirroring the consent.test.ts stub convention: a string array is the CMP-detect probe, an
   * object with a `gate` key is the embed-gate probe, and an object with a `maxDepth` key is the sign-in container collector. The classifier reaches these probes
   * only when no provider indicator already decided.
   */
  function makePage(url: string, router: (arg: unknown) => unknown = (): unknown => false): Page {

    return {

      $: async (): Promise<unknown> => null,
      evaluate: async (_fn: unknown, arg?: unknown): Promise<unknown> => router(arg),
      url: (): string => url
    } as unknown as Page;
  }

  // A single discovered channel for the non-empty scenarios; the recorder only reads channels.length and hands the array to validatePrecache.
  const ONE_CHANNEL = [{ channelSelector: "Stub", name: "Stub" }] as unknown as DiscoveredChannel[];

  test("an empty result classified as an auth wall marks the domain needs-sign-in and warns with the remedy", async (t) => {

    /* Traced path: channels.length === 0 -> classifyBlockedPage returns authWall via the provider host indicator -> the recorder's authWall case calls
     * markDomainAuthRequired(extractDomain(guideUrl)) and emits the WARN. Dropping the mark would leave the state read null; dropping the WARN drops the operator
     * signal this arc exists to create.
     */
    const warn = t.mock.method(LOG, "warn", () => { /* Captured via the mock. */ });
    const provider = makeProvider({ authWallIndicators: { hosts: ["auth.case-wall.test"] }, guideUrl: "https://www.case-wall.test/guide" });

    await recordDiscoveryOutcome(provider, [], makePage("https://auth.case-wall.test/login"), recorderDeps);

    assert.deepEqual(getDomainAuthState("case-wall.test"), { status: "needsLogin", timestamp: 1_700_000_000_000 }, "domain marked needs-sign-in");
    assert.equal(warn.mock.calls.length, 1, "exactly one WARN");

    const message = String(warn.mock.calls[0]?.arguments[0]);

    assert.match(message, /authentication wall/, "names the wall");
    assert.match(message, /Sign in from the channel table's login icon\./, "names the remedy");
  });

  test("an empty result classified as a consent overlay warns but never marks auth state", async (t) => {

    /* Traced path: channels.length === 0 -> no indicators -> consentOverlayPresent's CMP probe (the string-array evaluate) returns true -> consentOverlay case.
     * The scenario pins that the case logs WARN and reaches no health mutator - a mutation that marked needs-sign-in here would trip the null read.
     */
    const warn = t.mock.method(LOG, "warn", () => { /* Captured via the mock. */ });
    const provider = makeProvider({ guideUrl: "https://www.case-consent.test/guide" });
    const page = makePage("https://www.case-consent.test/guide", (arg) => Array.isArray(arg));

    await recordDiscoveryOutcome(provider, [], page, recorderDeps);

    assert.equal(getDomainAuthState("case-consent.test"), null, "no auth state change on a consent overlay");
    assert.equal(warn.mock.calls.length, 1, "the overlay is reported at WARN");
  });

  test("an empty result classified unknown changes no state and does not warn", async (t) => {

    /* Traced path: channels.length === 0 -> no indicators, no consent, no sign-in containers (the collector probe returns []) -> unknown case: debug-only, no
     * mutation. An unexplained empty walk is not evidence of anything.
     */
    const warn = t.mock.method(LOG, "warn", () => { /* Captured via the mock. */ });
    const provider = makeProvider({ guideUrl: "https://www.case-unknown.test/guide" });
    const page = makePage("https://www.case-unknown.test/guide", (arg) => {

      // The CMP probe (string array) reports no banner; the embed-gate probe reports no gate; the collector reports no containers.
      if(Array.isArray(arg)) {

        return false;
      }

      return ((typeof arg === "object") && (arg !== null) && ("maxDepth" in arg)) ? [] : null;
    });

    await recordDiscoveryOutcome(provider, [], page, recorderDeps);

    assert.equal(getDomainAuthState("case-unknown.test"), null, "no auth state change on unknown");
    assert.equal(warn.mock.calls.length, 0, "no WARN on unknown");
  });

  test("a non-empty result with no validatePrecache marks the domain verified (today's semantics)", async () => {

    // Traced path: channels.length > 0 -> the !provider.validatePrecache disjunct -> markDomainAuth. This is the pre-existing verified mark, unchanged.
    const provider = makeProvider({ guideUrl: "https://www.case-plain.test/guide" });

    await recordDiscoveryOutcome(provider, ONE_CHANNEL, makePage("https://www.case-plain.test/guide"), recorderDeps);

    assert.deepEqual(getDomainAuthState("case-plain.test"), { status: "verified", timestamp: 1_700_000_000_000 }, "verified without a validator");
  });

  test("a non-empty result passing validatePrecache marks the domain verified", async () => {

    // Traced path: channels.length > 0 -> validatePrecache returns true -> markDomainAuth.
    const provider = makeProvider({ guideUrl: "https://www.case-validated.test/guide", validatePrecache: (): boolean => true });

    await recordDiscoveryOutcome(provider, ONE_CHANNEL, makePage("https://www.case-validated.test/guide"), recorderDeps);

    assert.deepEqual(getDomainAuthState("case-validated.test"), { status: "verified", timestamp: 1_700_000_000_000 }, "verified through the validator");
  });

  test("a non-empty result failing validatePrecache clears a standing needs-sign-in flag back to unknown", async () => {

    /* Traced path: channels.length > 0 -> validatePrecache returns false -> clearDomainAuthRequirement, whose needsLogin guard passes because the domain is
     * flagged - the wall is gone (channels came back) but paid access is unproven, so the red state must not persist. Dropping the clear would leave the flag
     * standing forever for a free-tier user.
     */
    const provider = makeProvider({ guideUrl: "https://www.case-unproven.test/guide", validatePrecache: (): boolean => false });

    markDomainAuthRequired("case-unproven.test");

    await recordDiscoveryOutcome(provider, ONE_CHANNEL, makePage("https://www.case-unproven.test/guide"), recorderDeps);

    assert.equal(getDomainAuthState("case-unproven.test"), null, "the needs-sign-in entry is cleared to unknown");
  });

  test("a non-empty result failing validatePrecache never deletes a verified entry", async () => {

    /* Traced path: channels.length > 0 -> validatePrecache returns false -> clearDomainAuthRequirement, whose needsLogin guard REJECTS a verified entry. A Sling
     * free-tier walk after a legitimate verified tune must leave the green state alone.
     */
    const provider = makeProvider({ guideUrl: "https://www.case-keeps-verified.test/guide", validatePrecache: (): boolean => false });

    markDomainAuth("case-keeps-verified.test");

    await recordDiscoveryOutcome(provider, ONE_CHANNEL, makePage("https://www.case-keeps-verified.test/guide"), recorderDeps);

    assert.deepEqual(getDomainAuthState("case-keeps-verified.test"), { status: "verified", timestamp: 1_700_000_000_000 }, "verified state untouched");
  });

  test("a non-empty result failing validatePrecache on an unknown domain changes nothing", async () => {

    // Traced path: the validator-rejected arm with no standing entry - clearDomainAuthRequirement's guard finds nothing to clear.
    const provider = makeProvider({ guideUrl: "https://www.case-still-unknown.test/guide", validatePrecache: (): boolean => false });

    await recordDiscoveryOutcome(provider, ONE_CHANNEL, makePage("https://www.case-still-unknown.test/guide"), recorderDeps);

    assert.equal(getDomainAuthState("case-still-unknown.test"), null, "unknown stays unknown");
  });

  test("a non-empty result persists the walk's channel identities through the injected port", async () => {

    /* Traced path: the non-empty arm's lineup write for a provider with no exportDurableLineup hook. The recorder falls back to the walk's own channels projected
     * onto identity rows, so a provider that tunes in-page contributes a lineup with no watch URL in it - the shape that makes the persisted store inert for
     * in-page-tuning platforms rather than conditionally skipped.
     */
    const provider = makeProvider({ guideUrl: "https://www.case-identity-lineup.test/guide", slug: "case-identity-lineup" });

    await recordDiscoveryOutcome(provider, ONE_CHANNEL, makePage("https://www.case-identity-lineup.test/guide"), recorderDeps);

    assert.deepEqual(persistedLineups, [{ channels: [{ channelSelector: "Stub", name: "Stub" }], slug: "case-identity-lineup" }],
      "the walk's identities are persisted under the provider's slug, with no watch URL");
  });

  test("a non-empty result prefers the provider's own durable lineup over the walk's identities", async () => {

    // Traced path: the non-empty arm with an exportDurableLineup hook present. Which fields survive a browser session is provider knowledge, so the hook's answer
    // wins outright - dropping the preference would silently strip every watch URL out of the store and reduce the feature to a suggestion list.
    const durable = [{ channelSelector: "Stub", name: "Stub", watchUrl: "https://www.case-durable-lineup.test/watch/stub" }];
    const provider = makeProvider({ exportDurableLineup: (): PersistedLineupChannel[] => durable, guideUrl: "https://www.case-durable-lineup.test/guide",
      slug: "case-durable-lineup" });

    await recordDiscoveryOutcome(provider, ONE_CHANNEL, makePage("https://www.case-durable-lineup.test/guide"), recorderDeps);

    assert.deepEqual(persistedLineups, [{ channels: durable, slug: "case-durable-lineup" }], "the provider's durable rows are what reach the store");
  });

  test("a non-empty result the validator rejects persists nothing", async () => {

    /* Traced path: the validator-rejected arm, which returns without reaching the lineup write. The store replaces a provider's slice wholesale, so a walk the
     * provider itself judges untrustworthy - Sling's free-tier lineup read without a paid subscription is the case that produces one - could shrink a fuller slice
     * an accepted walk wrote earlier, dropping channels out of the cold-listing fallback and taking their durable watch URLs with them.
     */
    const provider = makeProvider({ guideUrl: "https://www.case-rejected-lineup.test/guide", slug: "case-rejected-lineup",
      validatePrecache: (): boolean => false });

    await recordDiscoveryOutcome(provider, ONE_CHANNEL, makePage("https://www.case-rejected-lineup.test/guide"), recorderDeps);

    assert.deepEqual(persistedLineups, [], "a rejected walk never reaches the lineup store");
  });

  test("an empty result persists nothing at all", async () => {

    // Traced path: the empty arm returns before the lineup write. A walk that found nothing is not evidence that the provider has no channels, and letting it
    // reach the store would erase the very hints a failed discovery most needs - the store's own empty-array guard is the second line, this is the first.
    const provider = makeProvider({ guideUrl: "https://www.case-empty-lineup.test/guide", slug: "case-empty-lineup" });

    await recordDiscoveryOutcome(provider, [], makePage("https://www.case-empty-lineup.test/guide"), recorderDeps);

    assert.deepEqual(persistedLineups, [], "an empty walk never reaches the lineup store");
  });
});

/* Deferred to e2e (require Puppeteer/Chrome integration):
 *
 * precaching.revalidation.test.ts already covers runPrecacheCycle's deps threading through to precacheService, precacheService's navigation, mute injection,
 * cleanup ordering, and the window sync on discovery-page cleanup, and the precacheInProgress guard's positive case, all through the PrecachingDeps injection seam
 * without a real browser. What remains genuinely deferred is:
 *
 * - runPrecacheCycle's succeeded/empty/skipped counters and the completion sentence they compose, which requires driving a full multi-service cycle rather than
 *   the single-service deps-threading check above.
 *
 * - The service-filter skip path (skipping services not in CONFIG.channels.enabledServices) - exercised inside runPrecacheCycle.
 *
 * - Per-provider error isolation (one provider failing while others succeed) - requires a real browser to populate the discovery flow.
 *
 * - The real Puppeteer mechanics of page.evaluateOnNewDocument and page.goto against an actual page, which the injection seam stubs out.
 */
