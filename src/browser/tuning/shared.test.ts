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
 * name matching, selector resolution, and the "channels you must configure manually" diagnostic; they are pinned directly below. The remaining exports
 * (scrollAndClick and the locate/click helpers) drive Puppeteer through page.mouse and page.evaluate and are deferred to the e2e tier.
 */
import type { NewDocumentScriptEvaluation, Page } from "puppeteer-core";
import { afterEach, beforeEach, describe, test } from "node:test";
import { installOncePerPage, installOrReplaceOnNewDocument, logAvailableChannels, normalizeChannelName, resolveMatchSelector } from "./shared.ts";
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

    // This is the core of the Hulu fix: re-tuning the same page with drifting arguments must run exactly one interceptor carrying current values. Each call removes
    // the script installed by the previous call, then installs anew. A regression that dropped the removal would let stale interceptor scripts stack and run
    // competing window.fetch patches frozen at old argument values.
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

  test("is idempotent - normalizing an already-normalized name returns it unchanged", () => {

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
  // formatted message pins the operator-visible invariant (which channels are reported, and the covered/uncovered count) rather than a pre-format implementation
  // detail. The subscription is installed per test and reset so one test's output cannot leak into another's assertions.
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
