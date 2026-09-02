/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * shared.test.ts: Unit tests for the pure install helpers in shared.ts - installOncePerPage and installOrReplaceOnNewDocument. installOncePerPage is the single
 * source of truth for "run this page-global install exactly once per (page, key) pair". installOrReplaceOnNewDocument is its complement for evaluateOnNewDocument
 * scripts whose baked-in arguments drift across re-tunes (e.g. Hulu's UUID/EAB tokens): it removes the prior script before installing a fresh one, so exactly one
 * live interceptor carrying current arguments runs - fixing both the stale-arguments bug and the duplicate-script accumulation. Both are used by the tuning
 * strategies (comcastPolymer, hulu, directv). The bookkeeping is fully pure - it only uses the Puppeteer Page as a map key (and, for the replace helper, calls
 * page.removeScriptToEvaluateOnNewDocument) - so both are unit-testable with a plain object stub standing in for the Page reference.
 *
 * normalizeChannelName, resolveMatchSelector, and logAvailableChannels are pure functions the tuning strategies and the channel-selection coordinator rely on for
 * name matching, selector resolution, and the "channels you must configure manually" diagnostic; they are asserted directly below. The remaining export,
 * scrollAndClick, drives Puppeteer through page.mouse and page.evaluate and is deferred to the e2e tier; the locate-style helpers that call it live in the
 * individual provider modules.
 *
 * createEmptyDiscoveryGuard and attemptGuideRecovery are the shared guide-recovery chassis that the virtualized-guide providers (Spectrum, YouTube TV) drive: the
 * guard counts a provider's consecutive zero-channel guide loads and reports when that streak warrants recovery, and the recovery routine clears the provider's
 * cached site data via CDP, reloads the guide, and re-runs the provider's own discovery. Both are exercised here against page and CDP-session stubs, which asserts
 * the choreography, the counter semantics, and every failure-path return. What no test tier can reach is a genuinely degraded provider guide, so whether clearing
 * site data actually revives one remains a field observation rather than something asserted here.
 */
import type { Browser, CDPSession, NewDocumentScriptEvaluation, Page } from "puppeteer-core";
import { FakeCdpSession, assertNoUnhandledRejections, firstOf, nthOf } from "../../testing.helpers.ts";
import { afterEach, beforeEach, describe, test } from "node:test";
import { attemptGuideRecovery, createEmptyDiscoveryGuard, installOncePerPage, installOrReplaceOnNewDocument, logAvailableChannels, normalizeChannelName,
  resolveMatchSelector } from "./shared.ts";
import { CHANNELS } from "../../channels/index.ts";
import type { ChannelSelectionProfile } from "../../types/index.ts";
import type { LogEntry } from "../../utils/logEmitter.ts";
import assert from "node:assert/strict";
import { makeProfile } from "../../config/profiles.helpers.ts";
import { subscribeToLogs } from "../../utils/logEmitter.ts";

/* makePage returns a minimal object that stands in for a Puppeteer Page. installOncePerPage only uses the reference as a WeakMap key and never reads any property,
 * so an empty object cast through unknown is a faithful stub. Each call produces a distinct identity, which is exactly what the per-page isolation tests need.
 */
function makePage(): Page {

  return {} as unknown as Page;
}

describe("installOncePerPage", () => {

  test("runs the install and returns true on the first call for a (page, key) pair", async () => {

    const page = makePage();
    let runs = 0;

    const installed = await installOncePerPage(page, "interceptor", () => { runs++; });

    assert.equal(installed, true, "first install for the pair returns true");
    assert.equal(runs, 1, "the install function ran exactly once");
  });

  test("skips the install and returns false on a repeat call for the same (page, key) pair", async () => {

    const page = makePage();
    let runs = 0;

    const first = await installOncePerPage(page, "interceptor", () => { runs++; });
    const second = await installOncePerPage(page, "interceptor", () => { runs++; });
    const third = await installOncePerPage(page, "interceptor", () => { runs++; });

    assert.equal(first, true, "first call installs");
    assert.equal(second, false, "second call for the same pair is a no-op");
    assert.equal(third, false, "third call for the same pair is a no-op");
    assert.equal(runs, 1, "the install function ran only on the first call");
  });

  test("installs independently for distinct keys on the same page", async () => {

    // Two different install actions on one page - e.g., a request-interception listener and an evaluateOnNewDocument script - must each run once. The key
    // namespaces the bookkeeping so distinct installs never collide on a shared page.
    const page = makePage();
    const runs = { a: 0, b: 0 };

    const firstA = await installOncePerPage(page, "key-a", () => { runs.a++; });
    const firstB = await installOncePerPage(page, "key-b", () => { runs.b++; });
    const repeatA = await installOncePerPage(page, "key-a", () => { runs.a++; });

    assert.equal(firstA, true, "key-a installs on its first call");
    assert.equal(firstB, true, "key-b installs on its first call despite sharing the page");
    assert.equal(repeatA, false, "key-a does not reinstall");
    assert.deepEqual(runs, { a: 1, b: 1 }, "each key ran its install exactly once");
  });

  test("installs independently for the same key on distinct pages", async () => {

    // Each stream gets its own page, so the same logical install (same key) must run once per page. The WeakMap keys on the page identity, so two pages are
    // tracked separately even when they share the key string.
    const pageOne = makePage();
    const pageTwo = makePage();
    let runs = 0;

    const firstOne = await installOncePerPage(pageOne, "interceptor", () => { runs++; });
    const firstTwo = await installOncePerPage(pageTwo, "interceptor", () => { runs++; });
    const repeatOne = await installOncePerPage(pageOne, "interceptor", () => { runs++; });

    assert.equal(firstOne, true, "first page installs");
    assert.equal(firstTwo, true, "second page installs independently for the same key");
    assert.equal(repeatOne, false, "the first page does not reinstall");
    assert.equal(runs, 2, "the install ran once per distinct page");
  });

  test("awaits an asynchronous install before returning", async () => {

    // The install action may be async (page.setRequestInterception, page.evaluateOnNewDocument both return promises). The helper must await it so the caller can
    // rely on the install having completed once the returned promise resolves.
    const page = makePage();
    let completed = false;

    const installed = await installOncePerPage(page, "async-install", async () => {

      await Promise.resolve();

      completed = true;
    });

    assert.equal(installed, true, "the async install reports as run");
    assert.equal(completed, true, "the helper awaited the asynchronous install to completion");
  });

  test("records the key before the install runs so a failed install does not silently retry", async () => {

    // The key is recorded before fn is awaited. A one-time install that throws must not re-arm itself: a subsequent call for the same pair stays a no-op rather
    // than re-running a side-effecting install (which on a real page would stack a duplicate listener). The thrown error still propagates on the first call.
    const page = makePage();
    let attempts = 0;

    const failing = (): void => {

      attempts++;

      throw new Error("install failed");
    };

    await assert.rejects(installOncePerPage(page, "fragile", failing), /install failed/, "the first call propagates the install error");

    const retry = await installOncePerPage(page, "fragile", failing);

    assert.equal(retry, false, "the failed install is recorded and does not retry on a later call for the same pair");
    assert.equal(attempts, 1, "the install function ran only on the first (failing) call");
  });
});

describe("installOrReplaceOnNewDocument", () => {

  // makeReplacePage stands in for a Page that records the script identifiers passed to removeScriptToEvaluateOnNewDocument, so a test can assert that each re-install
  // removes exactly the previously-installed script.
  function makeReplacePage(): { page: Page; removed: string[] } {

    const removed: string[] = [];
    const page = {

      removeScriptToEvaluateOnNewDocument: async (id: string): Promise<void> => { removed.push(id); }
    } as unknown as Page;

    return { page, removed };
  }

  // makeInstall returns an install thunk that hands out sequential identifiers, mimicking page.evaluateOnNewDocument's NewDocumentScriptEvaluation result.
  function makeInstall(): { ids: string[]; install: () => Promise<NewDocumentScriptEvaluation> } {

    const ids: string[] = [];
    let n = 0;

    const install = async (): Promise<NewDocumentScriptEvaluation> => {

      const identifier = "script-" + String(n++);

      ids.push(identifier);

      return { identifier };
    };

    return { ids, install };
  }

  test("installs without removing anything on the first call for a (page, key) pair", async () => {

    const { page, removed } = makeReplacePage();
    const { ids, install } = makeInstall();

    await installOrReplaceOnNewDocument(page, "fetch-interceptor", install);

    assert.deepEqual(removed, [], "nothing is removed on the first install");
    assert.deepEqual(ids, ["script-0"], "the install ran exactly once");
  });

  test("removes the prior script before installing a fresh one on each subsequent call (never stacks)", async () => {

    // This asserts the critical Hulu guarantee: re-tuning the same page with drifting arguments must run exactly one interceptor carrying current values. Each call
    // removes the script installed by the previous call, then installs anew. A regression that dropped the removal would let stale interceptor scripts stack and
    // run competing window.fetch patches frozen at old argument values.
    const { page, removed } = makeReplacePage();
    const { install } = makeInstall();

    await installOrReplaceOnNewDocument(page, "fetch-interceptor", install);
    await installOrReplaceOnNewDocument(page, "fetch-interceptor", install);
    await installOrReplaceOnNewDocument(page, "fetch-interceptor", install);

    assert.deepEqual(removed, [ "script-0", "script-1" ], "each re-install removes exactly the previous script, never accumulating a stack");
  });

  test("tracks distinct keys independently, removing only the prior script for the key being re-installed", async () => {

    const { page, removed } = makeReplacePage();
    const { install } = makeInstall();

    await installOrReplaceOnNewDocument(page, "key-a", install);
    await installOrReplaceOnNewDocument(page, "key-b", install);
    await installOrReplaceOnNewDocument(page, "key-a", install);

    assert.deepEqual(removed, ["script-0"], "re-installing key-a removes only key-a's prior script, not key-b's");
  });

  test("swallows a removal failure and still installs the fresh script", async () => {

    // A stale identifier (the page already navigated past the script) makes removal reject; that must not abort the fresh install.
    const removed: string[] = [];
    const page = {

      removeScriptToEvaluateOnNewDocument: async (id: string): Promise<void> => {

        removed.push(id);

        throw new Error("script already gone");
      }
    } as unknown as Page;
    const { ids, install } = makeInstall();

    await installOrReplaceOnNewDocument(page, "k", install);

    await assert.doesNotReject(installOrReplaceOnNewDocument(page, "k", install), "a removal failure does not abort the fresh install");

    assert.deepEqual(ids, [ "script-0", "script-1" ], "the fresh script installed despite the prior removal failing");
  });
});

describe("normalizeChannelName", () => {

  test("lowercases and trims surrounding whitespace", () => {

    assert.equal(normalizeChannelName("  ESPN  "), "espn");
  });

  test("collapses internal whitespace runs - spaces, tabs, and non-breaking spaces - into a single space", () => {

    // Guide data-testid values arrive with double spaces, tabs, or U+00A0 non-breaking spaces that would defeat an exact-string match. The normalizer folds every
    // \s-matched run (which includes \t and U+00A0) to one ASCII space so a padded or non-breaking-spaced name matches its plain-spaced canonical form.
    assert.equal(normalizeChannelName("Cartoon   Network"), "cartoon network");
    assert.equal(normalizeChannelName("Cartoon\tNetwork"), "cartoon network");
    assert.equal(normalizeChannelName("Cartoon\u00A0Network"), "cartoon network");
  });

  test("normalizing an already-normalized name returns it unchanged", () => {

    // Callers on both sides of a match (the predefined channelSelector and the discovered name) normalize independently, so the function must be a fixed point:
    // running it twice yields the same string it produced once, or matching would depend on how many times each side happened to be normalized.
    const once = normalizeChannelName("  A&E  (East) ");

    assert.equal(normalizeChannelName(once), once);
  });

  test("returns an empty string for whitespace-only input", () => {

    assert.equal(normalizeChannelName("   \t   "), "");
  });
});

describe("resolveMatchSelector", () => {

  test("interpolates every {channel} placeholder in the matchSelector template with the channelSelector", () => {

    // A template may reference {channel} more than once (e.g. a compound attribute selector matching both a data attribute and an alt text). replaceAll must
    // substitute all occurrences, not just the first, or the second selector clause would search for the literal placeholder and never match.
    const profile = makeProfile({ channelSelection: { matchSelector: "[data-ch=\"{channel}\"], img[alt*=\"{channel}\"]", strategy: "none" },
      channelSelector: "ESPN" }) as ChannelSelectionProfile;

    assert.equal(resolveMatchSelector(profile), "[data-ch=\"ESPN\"], img[alt*=\"ESPN\"]");
  });

  test("falls back to case-insensitive image-src slug matching when no matchSelector template is configured", () => {

    // Profiles that predate matchSelector leave it unset; the resolver then defaults to the img[src*="<selector>" i] form so the thumbnailRow and tileClick
    // strategies still locate the channel logo by URL slug. The trailing " i" flag keeps the match case-insensitive against mixed-case CDN paths.
    const profile = makeProfile({ channelSelection: { strategy: "none" }, channelSelector: "espn" }) as ChannelSelectionProfile;

    assert.equal(resolveMatchSelector(profile), "img[src*=\"espn\" i]");
  });
});

describe("logAvailableChannels", () => {

  // logAvailableChannels calls the module-singleton LOG, whose formatted output is broadcast to the SSE emitter that subscribeToLogs taps. Asserting on the
  // formatted message asserts what the operator sees (which channels are reported, and the covered/uncovered count) rather than a pre-format implementation detail.
  // The subscription is installed per test and reset so one test's output cannot leak into another's assertions.
  let captured: LogEntry[];

  let unsubscribe: () => void;

  beforeEach(() => {

    captured = [];
    unsubscribe = subscribeToLogs((entry) => { captured.push(entry); });
  });

  afterEach(() => {

    unsubscribe();
  });

  test("logs every available channel unfiltered when no preset suffix is supplied", () => {

    // Small channel sets (Fox, HBO) pass no presetSuffix, so the full discovered list is actionable and reported verbatim with a plain count.
    logAvailableChannels({ availableChannels: [ "Alpha Channel", "Beta Channel" ], channelName: "Gamma", guideUrl: "https://guide.example",
      providerName: "Test Provider" });

    const warning = captured.find((line) => (line.level === "warn") && line.message.includes("Test Provider"));

    assert.ok(warning, "a warning is emitted when channels are available");
    assert.match(warning.message, /Alpha Channel/, "the unfiltered list includes the first channel");
    assert.match(warning.message, /Beta Channel/, "the unfiltered list includes the second channel");
    assert.match(warning.message, /\(2\)/, "the count label reports the total number of channels");
    assert.match(warning.message, /https:\/\/guide\.example/, "the guide URL is included so users know what to set as the channel URL");
  });

  test("excludes channels already covered by a preset definition when a preset suffix is supplied", () => {

    // The covered set is driven from the CHANNELS single source of truth: any discovered channel whose name matches a preset channelSelector for this suffix is
    // "covered" and filtered out of the diagnostic, so users see only channels that genuinely need manual configuration. We pick a real preset selector as the
    // covered case and pair it with a fabricated name no preset matches; a regression in the coverage filter changes the reported count.
    const suffix = "-yttv";
    const knownSelectors = Object.entries(CHANNELS).filter(([key]) => key.endsWith(suffix)).map(([ , channel ]) => channel.channelSelector)
      .filter((selector): selector is string => (typeof selector === "string") && (selector.length > 0));

    assert.ok(knownSelectors.length > 0, "the CHANNELS single source of truth still defines preset channels for the " + suffix + " suffix");

    const covered = knownSelectors[0] ?? "";

    logAvailableChannels({ availableChannels: [ covered, "Nonexistent Diagnostic Channel" ], channelName: "Whatever", guideUrl: "https://guide.example",
      presetSuffix: suffix, providerName: "YouTube TV" });

    const warning = captured.find((line) => (line.level === "warn") && line.message.includes("YouTube TV"));

    assert.ok(warning, "a warning is emitted because at least one channel is uncovered");
    assert.match(warning.message, /Nonexistent Diagnostic Channel/, "the uncovered fabricated channel is reported");
    assert.match(warning.message, /uncovered \(1 of 2\)/, "the count label reports one uncovered channel of two available");
  });

  test("treats additionalKnownNames as covered so they are excluded from the diagnostic", () => {

    // YouTube TV passes CHANNEL_ALTERNATES values via additionalKnownNames; a discovered channel whose name matches one of those must be filtered out even though
    // it is not itself a preset channelSelector for the suffix.
    const extra = "Some Alternate Name";

    logAvailableChannels({ additionalKnownNames: [extra], availableChannels: [ extra, "Truly Unlisted Channel" ], channelName: "Whatever",
      guideUrl: "https://guide.example", presetSuffix: "-yttv", providerName: "Alt Provider" });

    const warning = captured.find((line) => (line.level === "warn") && line.message.includes("Alt Provider"));

    assert.ok(warning, "a warning is emitted for the remaining uncovered channel");
    assert.doesNotMatch(warning.message, /Some Alternate Name/, "the additionalKnownNames entry is excluded from the reported list");
    assert.match(warning.message, /Truly Unlisted Channel/, "the genuinely-uncovered channel is still reported");
  });

  test("emits nothing when there are no available channels to report", () => {

    logAvailableChannels({ availableChannels: [], channelName: "Whatever", guideUrl: "https://guide.example", providerName: "Silent Provider" });

    assert.equal(captured.filter((line) => line.message.includes("Silent Provider")).length, 0, "no warning is emitted for an empty channel list");
  });

  test("emits nothing when every available channel is already covered by a preset", () => {

    // When the coverage filter removes every channel, there is nothing actionable to tell the user, so the diagnostic stays silent rather than logging an empty list.
    const suffix = "-yttv";
    const knownSelectors = Object.entries(CHANNELS).filter(([key]) => key.endsWith(suffix)).map(([ , channel ]) => channel.channelSelector)
      .filter((selector): selector is string => (typeof selector === "string") && (selector.length > 0));

    assert.ok(knownSelectors.length > 0, "the CHANNELS single source of truth still defines preset channels for the " + suffix + " suffix");

    logAvailableChannels({ availableChannels: [knownSelectors[0] ?? ""], channelName: "Whatever", guideUrl: "https://guide.example",
      presetSuffix: suffix, providerName: "Covered Provider" });

    assert.equal(captured.filter((line) => line.message.includes("Covered Provider")).length, 0, "a fully-covered channel list produces no diagnostic");
  });
});

describe("createEmptyDiscoveryGuard", () => {

  // The guard's warning is emitted through the module-singleton LOG, so the streak length it reports is read back off the log emitter the same way the
  // logAvailableChannels tests read theirs. The subscription is installed per test and torn down after so one test's output cannot leak into another's counts.
  let captured: LogEntry[];

  let unsubscribe: () => void;

  beforeEach(() => {

    captured = [];
    unsubscribe = subscribeToLogs((entry) => { captured.push(entry); });
  });

  afterEach(() => {

    unsubscribe();
  });

  test("withholds the recovery signal until the streak reaches the threshold", () => {

    // Recovery discards the provider's caches and reloads the page, so a single empty discovery - which an unusually slow guide render can produce on its own -
    // must not trigger it. The guard reports true only once the streak is long enough that a slow render stops being a plausible explanation.
    const guard = createEmptyDiscoveryGuard("Test Provider");

    assert.equal(guard.recordEmpty(), false, "the first empty discovery does not call for recovery");
    assert.equal(guard.recordEmpty(), false, "the second empty discovery does not call for recovery");
    assert.equal(guard.recordEmpty(), true, "the third consecutive empty discovery calls for recovery");
  });

  test("keeps calling for recovery on every consecutive empty discovery past the threshold", () => {

    // The signal is "at or past the threshold", never "exactly at it". A guide that stays degraded has to be retried on each subsequent empty load rather than
    // getting a single attempt and then being left alone forever, which is what an equality check against the threshold would produce.
    const guard = createEmptyDiscoveryGuard("Test Provider");

    guard.recordEmpty();
    guard.recordEmpty();
    guard.recordEmpty();

    assert.equal(guard.recordEmpty(), true, "the fourth consecutive empty discovery still calls for recovery");
    assert.equal(guard.recordEmpty(), true, "the fifth consecutive empty discovery still calls for recovery");
  });

  test("restarts the streak on reset so the next recovery needs a fresh run of empty discoveries", () => {

    // A successful discovery and a browser restart both resolve the degraded state the streak stands for, so the count has to start over. Without that, a
    // provider that fails occasionally would accumulate its way into recovering on every single empty load.
    const guard = createEmptyDiscoveryGuard("Test Provider");

    guard.recordEmpty();
    guard.recordEmpty();
    guard.reset();

    assert.equal(guard.recordEmpty(), false, "the first empty discovery after a reset starts a new streak");
    assert.equal(guard.recordEmpty(), false, "the second empty discovery after a reset is still below the threshold");
    assert.equal(guard.recordEmpty(), true, "the threshold is reached again only after a full fresh streak");
  });

  test("counts each provider independently", () => {

    // Every strategy holds its own guard. Spectrum sliding into a degraded guide must not push YouTube TV toward a recovery it has no reason to run, which a
    // single shared counter would do.
    const spectrum = createEmptyDiscoveryGuard("Spectrum TV");
    const youtubeTv = createEmptyDiscoveryGuard("YouTube TV");

    spectrum.recordEmpty();
    spectrum.recordEmpty();

    assert.equal(youtubeTv.recordEmpty(), false, "the second guard starts its own streak rather than inheriting the first guard's count");
    assert.equal(spectrum.recordEmpty(), true, "the first guard reaches the threshold on its own third empty discovery");
  });

  test("warns with the provider name and the running streak length on every empty discovery", () => {

    // This warning is the operator's only view of a guide sliding into a degraded state, so it has to name the provider and say how deep the streak already is.
    const guard = createEmptyDiscoveryGuard("Test Provider");

    guard.recordEmpty();
    guard.recordEmpty();

    const warnings = captured.filter((line) => (line.level === "warn") && line.message.includes("Test Provider"));

    assert.equal(warnings.length, 2, "one warning is emitted per empty discovery");
    assert.match(firstOf(warnings, "warning").message, /\(1 consecutive\)/, "the first warning reports a streak of one");
    assert.match(nthOf(warnings, 1, "warning").message, /\(2 consecutive\)/, "the second warning reports a streak of two");
  });
});

describe("attemptGuideRecovery", () => {

  // Stand-in for a provider's own raw channel shape. The routine counts the discovered entries but never reads inside them, so any object shape serves.
  const twoChannels = [ { name: "Alpha" }, { name: "Beta" } ];

  /* RejectingSendSession is the canonical CDP session fake with a Storage.clearDataForOrigin that the browser refuses. Subclassing rather than hand-rolling a
   * session keeps detachCalls and the sent record behaving exactly as every other CDP test sees them, which is what the release assertions read.
   */
  class RejectingSendSession extends FakeCdpSession {

    public override async send(): Promise<unknown> {

      await Promise.resolve();

      throw new Error("clear rejected");
    }
  }

  /* RejectingDetachSession is the canonical fake with a release that fails, which is what a page torn down mid-recovery produces. It still counts the attempt
   * through the base implementation before rejecting, so a test can assert both that the release was tried and that its failure went nowhere.
   */
  class RejectingDetachSession extends FakeCdpSession {

    public override async detach(): Promise<void> {

      await super.detach();

      throw new Error("detach rejected");
    }
  }

  // The clear-failure warning is emitted through the module-singleton LOG, so the tests that count it read it back off the log emitter.
  let captured: LogEntry[];

  let unsubscribe: () => void;

  beforeEach(() => {

    captured = [];
    unsubscribe = subscribeToLogs((entry) => { captured.push(entry); });
  });

  afterEach(() => {

    unsubscribe();
  });

  /* makeRecoveryPage returns a Page-shaped stub carrying the surfaces the recovery routine touches, plus a record of what it did with them: the CDP session it
   * was handed, the URLs it navigated to, and the selectors it waited on. Any stage can be made to reject so a test can fail exactly one step of the
   * choreography and assert the routine's response to it.
   */
  interface RecoveryPageOptions {

    // Makes page.createCDPSession reject, standing in for a browser that will not open an auxiliary session.
    readonly createFails?: boolean;

    // Makes page.goto reject, standing in for a guide reload that never lands.
    readonly gotoFails?: boolean;

    // Makes browser.pages() reject, standing in for a browser that cannot report what it has open.
    readonly pagesFails?: boolean;

    // Makes page.waitForSelector reject, standing in for a grid that never renders within the timeout.
    readonly selectorFails?: boolean;

    // The CDP session page.createCDPSession hands out. Defaults to a plain canonical fake; tests that need a failing command supply a subclass.
    readonly session?: FakeCdpSession;

    // Other pages the browser reports as open alongside the recovering page. Defaults to none, so the default page is alone on its origin and the clear runs.
    readonly siblingPages?: readonly Page[];
  }

  interface RecoveryPage {

    // Every URL the routine navigated to, in order.
    readonly navigations: string[];

    // The Page-shaped stub to hand the routine.
    readonly page: Page;

    // Every selector the routine waited on, in order.
    readonly selectors: string[];

    // The CDP session the stub hands out, carrying the sent record and the detach counter.
    readonly session: FakeCdpSession;
  }

  function makeRecoveryPage(options: RecoveryPageOptions = {}): RecoveryPage {

    const navigations: string[] = [];
    const selectors: string[] = [];
    const session = options.session ?? new FakeCdpSession(null);

    // What browser.pages() reports. It is filled in after the page stub exists so the recovering page can be its own first entry, which is what the default
    // arrangement needs: a browser holding only the page being recovered, where the sibling gate finds nothing and the clear proceeds.
    const openPages: Page[] = [];

    const page = {

      browser: (): Browser => ({

        pages: async (): Promise<Page[]> => {

          await Promise.resolve();

          if(options.pagesFails) {

            throw new Error("pages enumeration rejected");
          }

          return openPages;
        }
      } as unknown as Browser),
      createCDPSession: async (): Promise<CDPSession> => {

        await Promise.resolve();

        if(options.createFails) {

          throw new Error("session creation rejected");
        }

        return session as unknown as CDPSession;
      },
      goto: async (url: string): Promise<null> => {

        navigations.push(url);

        await Promise.resolve();

        if(options.gotoFails) {

          throw new Error("navigation rejected");
        }

        return null;
      },
      // The recovering page sits on the origin under test. It has to, for the sibling gate's identity exclusion to mean anything: a gate that excluded pages by
      // URL instead would find this page matching and refuse every clear.
      url: (): string => "https://guide.example/guide",
      waitForSelector: async (selector: string): Promise<null> => {

        selectors.push(selector);

        await Promise.resolve();

        if(options.selectorFails) {

          throw new Error("selector wait timed out");
        }

        return null;
      }
    } as unknown as Page;

    openPages.push(page, ...(options.siblingPages ?? []));

    return { navigations, page, selectors, session };
  }

  // makeOpenPage returns a bare page stub that reports the given URL, standing in for some other tab the browser happens to have open.
  function makeOpenPage(url: string): Page {

    return { url: (): string => url } as unknown as Page;
  }

  // makeDiscover returns a discovery stub that records the pages it was called with, so a test can prove re-discovery ran against the page that was reloaded
  // rather than inferring it from the return value alone.
  function makeDiscover(result: { name: string }[]): { calls: Page[]; discover: (page: Page) => Promise<{ name: string }[]> } {

    const calls: Page[] = [];

    const discover = async (page: Page): Promise<{ name: string }[]> => {

      calls.push(page);

      return result;
    };

    return { calls, discover };
  }

  // runRecovery drives the routine with one fixed set of provider options so each test reads as the single thing it is asserting rather than repeating the
  // options object six times.
  async function runRecovery(page: Page, discover: (page: Page) => Promise<{ name: string }[]>): Promise<{ name: string }[]> {

    return await attemptGuideRecovery(page, {

      discover,
      origin: "https://guide.example",
      providerName: "Test Provider",
      reloadUrl: "https://guide.example/guide",
      storageTypes: "cache_storage",
      waitSelector: "li.channel-row"
    });
  }

  test("clears the caller's origin, reloads the guide, waits for the grid, and returns the re-discovered channels", async () => {

    // The full choreography in order. The CDP command is asserted with the caller's own origin and storage types because those are the provider policy the
    // chassis exists to carry: a chassis that cleared a hardcoded origin would silently clear the wrong site for every provider but the first.
    const { navigations, page, selectors, session } = makeRecoveryPage();
    const { calls, discover } = makeDiscover(twoChannels);

    const channels = await runRecovery(page, discover);

    assert.deepEqual(channels, twoChannels, "the routine returns whatever re-discovery found");
    assert.deepEqual(session.sent, [{ method: "Storage.clearDataForOrigin", params: { origin: "https://guide.example", storageTypes: "cache_storage" } }],
      "exactly one clear is issued, carrying the caller's origin and storage types");
    assert.deepEqual(navigations, ["https://guide.example/guide"], "the guide is reloaded once, at the caller's reload URL");
    assert.deepEqual(selectors, ["li.channel-row"], "the routine waits for the caller's grid selector");
    assert.deepEqual(calls, [page], "re-discovery runs against the reloaded page");

    const outcome = captured.find((line) => line.message.includes("guide recovery succeeded"));

    assert.ok(outcome, "a successful recovery reports itself");
    assert.equal(outcome.level, "info", "success is reported at info, not as a warning");
    assert.match(outcome.message, /Test Provider guide recovery succeeded - discovered 2 channels after clearing site data\./,
      "the success message names the provider and the channel count it recovered");
  });

  test("returns empty and never navigates when the CDP session cannot be created", async () => {

    // A createCDPSession failure is caught, not propagated. Nothing downstream of the strategy has a catch for it, so letting it escape would turn a recoverable
    // degraded guide into an unhandled rejection.
    const { navigations, page } = makeRecoveryPage({ createFails: true });
    const { calls, discover } = makeDiscover(twoChannels);

    const channels = await runRecovery(page, discover);

    assert.deepEqual(channels, [], "a session that cannot be created ends recovery with an empty result");
    assert.deepEqual(navigations, [], "the guide is not reloaded when the clear never happened");
    assert.deepEqual(calls, [], "re-discovery does not run");
  });

  test("returns empty and never navigates when the clear command is refused", async () => {

    const { navigations, page } = makeRecoveryPage({ session: new RejectingSendSession(null) });
    const { calls, discover } = makeDiscover(twoChannels);

    const channels = await runRecovery(page, discover);

    assert.deepEqual(channels, [], "a refused clear ends recovery with an empty result");
    assert.deepEqual(navigations, [], "the guide is not reloaded when the clear failed");
    assert.deepEqual(calls, [], "re-discovery does not run");
  });

  test("returns empty when the guide reload fails", async () => {

    // The clear succeeded here, so the failure lands squarely on the navigation and the routine stops before waiting for a grid that was never re-requested.
    const { page, selectors } = makeRecoveryPage({ gotoFails: true });
    const { calls, discover } = makeDiscover(twoChannels);

    const channels = await runRecovery(page, discover);

    assert.deepEqual(channels, [], "a failed reload ends recovery with an empty result");
    assert.deepEqual(selectors, [], "the routine does not wait for the grid on a page it could not reload");
    assert.deepEqual(calls, [], "re-discovery does not run");
  });

  test("returns empty when the guide grid never renders after the reload", async () => {

    const { navigations, page } = makeRecoveryPage({ selectorFails: true });
    const { calls, discover } = makeDiscover(twoChannels);

    const channels = await runRecovery(page, discover);

    assert.deepEqual(channels, [], "a grid that never renders ends recovery with an empty result");
    assert.deepEqual(navigations, ["https://guide.example/guide"], "the reload still happened before the wait gave up");
    assert.deepEqual(calls, [], "re-discovery does not run against a guide that never rendered");
  });

  test("returns empty when re-discovery finds nothing on the reloaded guide", async () => {

    // The clear and reload can both succeed and still leave an empty guide. That is a completed recovery attempt that failed, not an error, so it returns the
    // empty result and leaves the caller's own empty-guide handling to decide what happens next.
    const { page } = makeRecoveryPage();
    const { calls, discover } = makeDiscover([]);

    const channels = await runRecovery(page, discover);

    assert.deepEqual(channels, [], "recovery that revives nothing returns an empty result");
    assert.equal(calls.length, 1, "re-discovery ran exactly once");

    const outcome = captured.find((line) => line.message.includes("still empty after clearing site data"));

    assert.ok(outcome, "a recovery that revived nothing says so");
    assert.equal(outcome.level, "warn", "an unrevived guide is reported as a warning");
    assert.match(outcome.message, /Test Provider guide still empty after clearing site data\./, "the message names the provider");
  });

  test("releases the CDP session when the clear command is refused", async () => {

    // The failure path is the one that has to hold: a refused clear returns early, and without a release keyed off the block's exit rather than its happy path,
    // every failed recovery on a page that outlives it strands one more attached session.
    const session = new RejectingSendSession(null);
    const { page } = makeRecoveryPage({ session });
    const { discover } = makeDiscover(twoChannels);

    const channels = await runRecovery(page, discover);

    assert.deepEqual(channels, [], "the refused clear still ends recovery with an empty result");
    assert.equal(session.detachCalls, 1, "the session is released even though the clear failed");
  });

  test("releases the CDP session exactly once when the clear succeeds", async () => {

    const { navigations, page, session } = makeRecoveryPage();
    const { discover } = makeDiscover(twoChannels);

    await runRecovery(page, discover);

    assert.equal(session.detachCalls, 1, "the session is released once on the success path");
    assert.deepEqual(navigations, ["https://guide.example/guide"], "releasing the session does not cost the reload that follows it");
  });

  test("attempts no release when the session was never created, and reports the clear failure once", async () => {

    // There is nothing to detach when creation itself failed, so the guarded release has to stay quiet rather than reaching through an empty binding. The single
    // warning asserts the other half of that path: a creation failure is caught and reported as a clear failure exactly once, not propagated and not double-logged.
    const session = new FakeCdpSession(null);
    const { page } = makeRecoveryPage({ createFails: true, session });
    const { discover } = makeDiscover(twoChannels);

    const channels = await runRecovery(page, discover);

    const failures = captured.filter((line) => (line.level === "warn") && line.message.includes("Failed to clear Test Provider site data"));

    assert.deepEqual(channels, [], "a creation failure returns an empty result rather than escaping the routine");
    assert.equal(session.detachCalls, 0, "no release is attempted for a session that was never created");
    assert.equal(failures.length, 1, "the clear failure is reported exactly once");
  });

  test("swallows a release that itself fails", async () => {

    // The release is dispatched and not awaited, so a page that went away mid-recovery makes it reject with nobody waiting. That rejection has to be absorbed at
    // the dispatch site: it must neither escape as an unhandled rejection nor change what the routine returns.
    const restore = assertNoUnhandledRejections();
    const session = new RejectingDetachSession(null);
    const { page } = makeRecoveryPage({ session });
    const { discover } = makeDiscover(twoChannels);

    try {

      const channels = await runRecovery(page, discover);

      assert.equal(session.detachCalls, 1, "the release was attempted");
      assert.deepEqual(channels, twoChannels, "a failing release neither throws nor changes the recovered channels");
    } finally {

      restore();
    }
  });

  test("skips the clear and reloads only when another live page shares the origin", async () => {

    // Storage.clearDataForOrigin discards storage the whole origin shares, so clearing here would pull a sibling stream's caching layers out from under it
    // mid-playback. One stream's degraded guide must not become every co-tuned stream's problem, so recovery gives up the clear and keeps the reload.
    const { navigations, page, selectors, session } = makeRecoveryPage({ siblingPages: [makeOpenPage("https://guide.example/watch/espn")] });
    const { calls, discover } = makeDiscover(twoChannels);

    const channels = await runRecovery(page, discover);

    assert.deepEqual(session.sent, [], "no clear is issued while a sibling shares the origin");
    assert.deepEqual(navigations, ["https://guide.example/guide"], "the reload still runs, since it is page-scoped and costs the sibling nothing");
    assert.deepEqual(selectors, ["li.channel-row"], "the routine still waits for the grid after the reload");
    assert.deepEqual(calls, [page], "re-discovery still runs");
    assert.deepEqual(channels, twoChannels, "reload-only recovery still returns whatever it found");

    const refusal = captured.find((line) => (line.level === "warn") && line.message.includes("other open pages share the origin"));

    assert.ok(refusal, "the skip is logged rather than being silent");
    assert.match(refusal.message, /Test Provider/, "the refusal names the provider");
    assert.match(refusal.message, /\(1\)/, "the refusal reports how many pages share the origin");

    assert.equal(captured.filter((line) => line.message.includes("Clearing Test Provider cached site data")).length, 0,
      "the clear is not announced on a path that does not clear");
  });

  test("counts a sibling by identity, not by URL, so a second page on the same URL still blocks the clear", async () => {

    // Two pages can legitimately sit on the same URL - two streams tuned to the same guide. Only the page being recovered is exempt from the count, and it is
    // exempt because it is that object, not because its URL matches.
    const { page, session } = makeRecoveryPage({ siblingPages: [makeOpenPage("https://guide.example/guide")] });
    const { discover } = makeDiscover(twoChannels);

    await runRecovery(page, discover);

    assert.deepEqual(session.sent, [], "a URL-identical sibling is still a sibling and still blocks the clear");
  });

  test("clears when the recovering page is the only page on its origin", async () => {

    // The mirror of the case above: the recovering page matches the origin too, and excluding it by identity is what lets a lone page still get its clear. A
    // gate that matched on URL would refuse here and no provider would ever recover.
    const { page, session } = makeRecoveryPage();
    const { discover } = makeDiscover(twoChannels);

    await runRecovery(page, discover);

    assert.equal(session.sent.length, 1, "the clear proceeds when nothing else shares the origin");
    assert.equal(captured.filter((line) => line.message.includes("Not clearing")).length, 0, "no refusal is logged on the path that clears");
    assert.equal(captured.filter((line) => line.message.includes("Clearing Test Provider cached site data to recover from empty guide.")).length, 1,
      "the clear is announced exactly once, on the path that performs it");
  });

  test("ignores open pages on other origins and pages whose URL does not parse", async () => {

    // The browser normally holds unrelated tabs. Only pages actually sharing the origin can be hurt by the clear, and a page whose URL cannot be read has no
    // origin to share, so neither kind may block recovery.
    const siblingPages = [ makeOpenPage("https://elsewhere.example/live"), makeOpenPage("about:blank"), makeOpenPage("") ];
    const { page, session } = makeRecoveryPage({ siblingPages });
    const { discover } = makeDiscover(twoChannels);

    await runRecovery(page, discover);

    assert.equal(session.sent.length, 1, "unrelated and unreadable pages do not block the clear");
  });

  test("skips the clear and reloads only when the open pages cannot be enumerated", async () => {

    // With no way to tell whether a sibling is out there, protecting healthy streams outranks maximizing this stream's recovery odds. The dedicated message
    // says so without a count, since there is no count to report.
    const { navigations, page, session } = makeRecoveryPage({ pagesFails: true });
    const { calls, discover } = makeDiscover(twoChannels);

    const channels = await runRecovery(page, discover);

    assert.deepEqual(session.sent, [], "no clear is issued when the page list could not be read");
    assert.deepEqual(navigations, ["https://guide.example/guide"], "the reload still runs");
    assert.deepEqual(calls, [page], "re-discovery still runs");
    assert.deepEqual(channels, twoChannels, "reload-only recovery still returns whatever it found");

    const refusal = captured.find((line) => (line.level === "warn") && line.message.includes("could not be enumerated"));

    assert.ok(refusal, "the skip is logged with the dedicated no-count message");
    assert.match(refusal.message, /Test Provider/, "the refusal names the provider");
  });
});
