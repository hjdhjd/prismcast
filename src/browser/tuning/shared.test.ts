/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * shared.test.ts: Unit tests for the pure install helpers in shared.ts - installOncePerPage and installOrReplaceOnNewDocument. installOncePerPage is the single
 * source of truth for "run this page-global install exactly once per (page, key) pair". installOrReplaceOnNewDocument is its complement for evaluateOnNewDocument
 * scripts whose baked-in arguments drift across re-tunes (e.g. Hulu's UUID/EAB tokens): it removes the prior script before installing a fresh one, so exactly one
 * live interceptor carrying current arguments runs - fixing both the stale-arguments bug and the duplicate-script accumulation. Both are used by the tuning
 * strategies (comcastPolymer, hulu, directv). The bookkeeping is fully pure - it only uses the Puppeteer Page as a map key (and, for the replace helper, calls
 * page.removeScriptToEvaluateOnNewDocument) - so both are unit-testable with a plain object stub standing in for the Page reference.
 *
 * The other exports in shared.ts (scrollAndClick, locate/click helpers) drive Puppeteer through page.mouse and page.evaluate and are deferred to the e2e tier.
 * normalizeChannelName, resolveMatchSelector, and logAvailableChannels are exercised through their consuming providers and the channel-selection coordinator tests.
 */
import type { NewDocumentScriptEvaluation, Page } from "puppeteer-core";
import { describe, test } from "node:test";
import { installOncePerPage, installOrReplaceOnNewDocument } from "./shared.ts";
import assert from "node:assert/strict";

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
