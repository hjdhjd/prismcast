/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * windowSync.test.ts: Unit tests for the browser window's visibility policy and its serialized executor in windowSync.ts. The module is dependency-free by design -
 * it takes its whole world through WindowSyncDeps - so the whole executor runs here against fakes with no browser, no CDP, and no timers.
 *
 * Two surfaces are asserted. decideWindowVisibility is a pure function and every arm of it is covered directly. The executor is covered through its factory: each test
 * builds an instance with recording fakes, and the primitives are held open with deferred promises where the order of "the command was issued" against "the caller's
 * promise resolved" is the thing under assertion - a microtask coincidence would otherwise let a broken drain look correct.
 */
import { createWindowVisibilitySync, decideWindowVisibility } from "./windowSync.ts";
import { describe, test } from "node:test";
import type { Nullable } from "../types/index.ts";
import type { Page } from "puppeteer-core";
import type { WindowSyncDeps } from "./windowSync.ts";
import assert from "node:assert/strict";

/**
 * Builds a deferred gate a test uses to hold a pass open until it chooses to release it. Forcing the order this way is what makes the drain assertions real: without
 * a held primitive, "the caller resolved after the command completed" is satisfied by a microtask coincidence rather than by the drain.
 * @returns The gate's promise and the call that releases it.
 */
function makeGate(): { promise: Promise<void>; release: () => void } {

  // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
  const { promise, resolve } = Promise.withResolvers<void>();

  return { promise, release: (): void => { resolve(); } };
}

// A page double carrying only what the executor reads: whether it is still open. Tests flip closed mid-pass to exercise the re-queue.
interface PageDouble {

  closed: boolean;
  isClosed: () => boolean;
  name: string;
}

/**
 * Builds a page double with a name the assertions can read back.
 * @param name - The label identifying this page in recorded operations.
 * @param closed - Whether the page starts out closed.
 * @returns The double, usable anywhere the executor expects a Page.
 */
function makePage(name: string, closed = false): PageDouble {

  const page: PageDouble = {

    closed,
    isClosed: (): boolean => page.closed,
    name
  };

  return page;
}

// What a test wants to observe and steer: the ordered operation log, the dispose count, and the mutable inputs the loop re-reads on every pass.
interface Harness {

  captureActive: boolean;
  disposals: string[];
  loginActive: boolean;
  ops: string[];
  resolveCalls: number;
  shuttingDown: boolean;
}

/**
 * Builds a harness plus the deps that read from it. Every dep records into the shared operation log, and the mutable flags are read at call time so a test can flip
 * an input between passes exactly as production state does.
 * @param options - The resolver behavior and any starting input values.
 * @param options.captureActive - Whether the capture predicate starts out true.
 * @param options.loginActive - Whether the login predicate starts out true.
 * @param options.minimize - Extra behavior to run after the minimize command is recorded, used to hold a pass open.
 * @param options.resolve - The page resolver, receiving the preferred page and the harness so it can record and steer.
 * @param options.unminimize - Extra behavior to run after the unminimize command is recorded, used to hold a pass open.
 * @returns The harness and the deps built over it.
 */
function makeHarness(options: {
  captureActive?: boolean;
  loginActive?: boolean;
  minimize?: (page: Page, harness: Harness) => Promise<void>;
  resolve?: (preferred: Nullable<Page>, harness: Harness) => Nullable<{ dispose: Nullable<() => Promise<void>>; page: Page }>;
  unminimize?: (page: Page, harness: Harness) => Promise<void>;
} = {}): { deps: WindowSyncDeps; harness: Harness } {

  const harness: Harness = {

    captureActive: options.captureActive ?? false,
    disposals: [],
    loginActive: options.loginActive ?? false,
    ops: [],
    resolveCalls: 0,
    shuttingDown: false
  };

  const defaultPage = makePage("default");

  const deps: WindowSyncDeps = {

    hasActiveCaptureStreams: (): boolean => harness.captureActive,
    isLoginModeActive: (): boolean => harness.loginActive,
    isShuttingDown: (): boolean => harness.shuttingDown,
    minimize: async (page: Page): Promise<void> => {

      harness.ops.push("minimize:" + (page as unknown as PageDouble).name);

      if(options.minimize) {

        await options.minimize(page, harness);
      }
    },
    resolvePage: async (preferred: Nullable<Page>): Promise<Nullable<{ dispose: Nullable<() => Promise<void>>; page: Page }>> => {

      harness.resolveCalls++;

      if(options.resolve) {

        return options.resolve(preferred, harness);
      }

      return { dispose: null, page: (preferred ?? defaultPage) as unknown as Page };
    },
    unminimize: async (page: Page): Promise<void> => {

      harness.ops.push("unminimize:" + (page as unknown as PageDouble).name);

      if(options.unminimize) {

        await options.unminimize(page, harness);
      }
    }
  };

  return { deps, harness };
}

/**
 * Asserts the postcondition every awaiting caller is entitled to: by the time its promise resolved, a pass had issued a command, and that command agrees with the
 * decision for the inputs as they stand at resolution. A caller that resolved off a run which was already settling would fail this, because nothing would have
 * acted on its request. Checking the agreement rather than a bare call count is what makes it a statement about the caller's own request.
 * @param harness - The harness whose op log and live inputs are read.
 * @param context - A label naming the caller, so a failure says which one resolved unhonored.
 */
function assertResolvedHonored(harness: Harness, context: string): void {

  const expected = decideWindowVisibility({ captureActive: harness.captureActive, loginActive: harness.loginActive });
  const issued = harness.ops.at(-1);

  assert.ok(issued, context + ": a pass issued a command before the caller resolved");
  assert.ok(issued.startsWith((expected === "normal") ? "unminimize" : "minimize"),
    context + ": the command issued last agrees with the decision for the inputs at resolution, not an older one");
}

describe("decideWindowVisibility", () => {

  test("capture and login both active yields normal", () => {

    assert.equal(decideWindowVisibility({ captureActive: true, loginActive: true }), "normal", "either reason alone suffices, so both certainly do");
  });

  test("capture alone yields normal", () => {

    // Tab capture consumes the compositor's output for this window, which is only composed for capture to read while the window is presented.
    assert.equal(decideWindowVisibility({ captureActive: true, loginActive: false }), "normal", "a capture stream holds the window on screen");
  });

  test("login alone yields normal", () => {

    assert.equal(decideWindowVisibility({ captureActive: false, loginActive: true }), "normal", "a user authenticating holds the window on screen");
  });

  test("neither yields minimized", () => {

    /* This arm composes with the registry predicate's native-only case to give the native-streaming contract: native streams are relayed in Node and never read the
     * compositor, so hasActiveCaptureStreams reads false for them and the window stays minimized. A naive "any stream present" check would take this arm to normal
     * and hold the window on screen for streams that have no use for it.
     */
    assert.equal(decideWindowVisibility({ captureActive: false, loginActive: false }), "minimized", "with no reason to be on screen the window minimizes");
  });
});

describe("createWindowVisibilitySync - dispatch", () => {

  test("issues minimize and never unminimize when neither input holds", async () => {

    const { deps, harness } = makeHarness();
    const sync = createWindowVisibilitySync(deps);

    await sync();

    assert.deepEqual(harness.ops, ["minimize:default"], "exactly the minimize command, and only it");
    assertResolvedHonored(harness, "the minimize caller");
  });

  test("issues unminimize and never minimize while capture is active", async () => {

    // The negative half is the point: an implementation that fired both primitives would satisfy a bare "did it unminimize" assertion.
    const { deps, harness } = makeHarness({ captureActive: true });
    const sync = createWindowVisibilitySync(deps);

    await sync();

    assert.deepEqual(harness.ops, ["unminimize:default"], "exactly the unminimize command, and only it");
    assertResolvedHonored(harness, "the capture-arm caller");
  });

  test("issues unminimize and never minimize while login is active", async () => {

    const { deps, harness } = makeHarness({ loginActive: true });
    const sync = createWindowVisibilitySync(deps);

    await sync();

    assert.deepEqual(harness.ops, ["unminimize:default"], "exactly the unminimize command, and only it");
    assertResolvedHonored(harness, "the login-arm caller");
  });

  test("prefers the caller's page when it is open", async () => {

    const preferred = makePage("caller");
    const { deps, harness } = makeHarness();
    const sync = createWindowVisibilitySync(deps);

    await sync(preferred as unknown as Page);

    assert.deepEqual(harness.ops, ["minimize:caller"], "the command targeted the page the caller handed over");
    assertResolvedHonored(harness, "the page-carrying caller");
  });
});

describe("createWindowVisibilitySync - drain and coalescing", () => {

  test("a call during a live run shares that run and resolves only after the drain empties", async () => {

    /* The assertion: the second caller must not resolve on the first pass. We hold the first pass open, let the second call arrive while it is genuinely in flight, and
     * check that the extra pass ran before either promise settled. A run that resolved at the end of whichever pass it happened to be in would settle the second
     * caller with only one command issued.
     */
    const issued = makeGate();
    const release = makeGate();
    const settled: string[] = [];
    let firstPass = true;

    const { deps, harness } = makeHarness({

      minimize: async (): Promise<void> => {

        if(!firstPass) {

          return;
        }

        firstPass = false;

        issued.release();

        await release.promise;
      }
    });

    const sync = createWindowVisibilitySync(deps);
    const a = sync().then(() => { settled.push("a"); });

    // Wait until the first pass is genuinely in flight before the second caller arrives, so the coalescing is forced rather than a matter of microtask timing.
    await issued.promise;

    const b = sync().then(() => { settled.push("b"); });

    assert.deepEqual(settled, [], "nothing settles while the first pass is held open");
    assert.equal(harness.ops.length, 1, "only the held pass has issued a command so far");

    release.release();

    await Promise.all([ a, b ]);

    assert.equal(harness.ops.length, 2, "the coalesced request produced a second pass rather than being absorbed");
    assert.deepEqual(settled.toSorted(), [ "a", "b" ], "both callers resolved off the same run");
    assertResolvedHonored(harness, "both coalesced callers");
  });

  test("the caller's promise does not resolve before the command it asked for is issued", async () => {

    // The drain guarantee, forced rather than observed: the primitive is held open, so a run that resolved early would be visible as a settlement recorded while
    // the command was still in flight.
    const held = makeGate();
    const issued = makeGate();
    const timeline: string[] = [];

    const { deps } = makeHarness({

      minimize: async (): Promise<void> => {

        timeline.push("issued");
        issued.release();

        await held.promise;

        timeline.push("completed");
      }
    });

    const sync = createWindowVisibilitySync(deps);
    const pending = sync().then(() => { timeline.push("resolved"); });

    await issued.promise;

    assert.deepEqual(timeline, ["issued"], "the command is in flight and nothing has resolved");

    held.release();

    await pending;

    assert.deepEqual(timeline, [ "issued", "completed", "resolved" ], "the caller resolved only after the command completed");
  });

  test("a trigger fired while a run is settling still gets its own pass before resolving", async () => {

    /* The trigger waits for a settled boundary with nothing outstanding rather than attaching to whichever run is live, so a request arriving late in a run's
     * life is honored by a pass of its own rather than piggybacking on the tail of one that has already decided.
     *
     * Honest strength: this asserts the intended behavior at the point in a run's life a test can reach deterministically - after the held command is released,
     * before the pass resumes. The window the loop shape actually exists to close is narrower still (between the drain's final flag check and the run clearing),
     * and it sits between microtasks that no injected collaborator can be scheduled into, so this test passes under the earlier attach-to-one-run shape as well.
     * It documents the contract; the source comment carries the reasoning a test cannot reach.
     */
    const gate = makeGate();
    const issued = makeGate();

    const { deps, harness } = makeHarness({

      minimize: async (): Promise<void> => {

        issued.release();

        await gate.promise;
      }
    });

    const sync = createWindowVisibilitySync(deps);
    const first = sync();

    await issued.promise;

    // Release the held pass and trigger again in the same turn, before yielding, so the second request lands while the first run is on its way to settling.
    gate.release();

    const second = sync();

    await Promise.all([ first, second ]);

    assert.equal(harness.ops.length, 2, "the late request produced a pass of its own");
    assertResolvedHonored(harness, "the caller that arrived while the run was settling");
  });

  test("re-reads both inputs on every pass, so a mid-run flip changes the second command", async () => {

    // The whole reason the inputs are read inside the loop: a decision taken before a pass cannot be stale by the time its command lands.
    const gate = makeGate();
    const issued = makeGate();

    const { deps, harness } = makeHarness({

      captureActive: true,
      unminimize: async (): Promise<void> => {

        issued.release();

        await gate.promise;
      }
    });

    const sync = createWindowVisibilitySync(deps);
    const first = sync();

    // The flip lands only once the first pass has decided and issued, so the second pass is genuinely reading a changed input rather than sharing the first's.
    await issued.promise;

    harness.captureActive = false;

    const second = sync();

    gate.release();

    await Promise.all([ first, second ]);

    assert.deepEqual(harness.ops, [ "unminimize:default", "minimize:default" ], "the second pass read the flipped input and issued the other command");

    // Both callers waited past the flip, so the command each of them ended up with is the one the inputs called for at the moment they resolved.
    assertResolvedHonored(harness, "the caller that spanned the input flip");
  });
});

describe("createWindowVisibilitySync - page resolution", () => {

  test("resolves a fresh page on every pass rather than reusing the previous one", async () => {

    /* Tab replacement closes the page an earlier pass used. Re-resolving per pass is what keeps a command from being issued into a dead target, where the CDP
     * layer would swallow it and the pass would silently do nothing.
     */
    const pages = [ makePage("first"), makePage("second") ];
    let index = 0;

    const { deps, harness } = makeHarness({

      resolve: (): Nullable<{ dispose: Nullable<() => Promise<void>>; page: Page }> => {

        const page = pages[Math.min(index++, pages.length - 1)];

        return { dispose: null, page: page as unknown as Page };
      }
    });

    const sync = createWindowVisibilitySync(deps);

    await sync();
    await sync();

    assert.equal(harness.resolveCalls, 2, "each pass asked the resolver again");
    assert.deepEqual(harness.ops, [ "minimize:first", "minimize:second" ], "the second pass used the page it resolved, not the first pass's page");
    assertResolvedHonored(harness, "the second sequential caller");
  });

  test("skips the pass when no page resolves", async () => {

    const { deps, harness } = makeHarness({ resolve: (): null => null });
    const sync = createWindowVisibilitySync(deps);

    await sync();

    assert.deepEqual(harness.ops, [], "with no window to act on, no command is issued");
  });

  test("queues exactly one more pass when the page dies under the command", async () => {

    /* A terminating stream closes its page fire-and-forget, so a pass can resolve a page that detaches while the command is in flight - and the CDP layer swallows
     * a command issued into a detaching target. The re-queue re-resolves a live page. It converges because a closed page is never resolved again, which the
     * resolver here models by handing back a fresh page after the first.
     */
    const dying = makePage("dying");
    const healthy = makePage("healthy");
    let first = true;

    const { deps, harness } = makeHarness({

      minimize: async (page: Page): Promise<void> => {

        const target = page as unknown as PageDouble;

        // Only the dying page dies. A probe that closed every page it touched would never converge, which is itself the property the re-queue has to have.
        if(target.name === "dying") {

          target.closed = true;
        }
      },
      resolve: (): Nullable<{ dispose: Nullable<() => Promise<void>>; page: Page }> => {

        const page = first ? dying : healthy;

        first = false;

        return { dispose: null, page: page as unknown as Page };
      }
    });

    const sync = createWindowVisibilitySync(deps);

    await sync();

    assert.deepEqual(harness.ops, [ "minimize:dying", "minimize:healthy" ], "one extra pass ran, on a page that was still alive");

    // The re-queued pass has to land before the caller resolves, or the caller would return having had its command swallowed by a dying page.
    assertResolvedHonored(harness, "the caller whose page died mid-command");
  });

  test("invokes the resolution's dispose when one is provided, and not otherwise", async () => {

    const temporary = makePage("temporary");
    const borrowed = makePage("borrowed");
    let first = true;

    const { deps, harness } = makeHarness({

      resolve: (): Nullable<{ dispose: Nullable<() => Promise<void>>; page: Page }> => {

        if(first) {

          first = false;

          return {

            dispose: async (): Promise<void> => { harness.disposals.push("temporary"); },
            page: temporary as unknown as Page
          };
        }

        return { dispose: null, page: borrowed as unknown as Page };
      }
    });

    const sync = createWindowVisibilitySync(deps);

    await sync();
    await sync();

    assert.deepEqual(harness.disposals, ["temporary"], "only the resolution that created a page is released");
  });
});

describe("createWindowVisibilitySync - shutdown and errors", () => {

  test("issues nothing once shutdown has begun", async () => {

    const { deps, harness } = makeHarness();

    harness.shuttingDown = true;

    const sync = createWindowVisibilitySync(deps);

    await sync();

    assert.equal(harness.resolveCalls, 0, "the pass abandons before it even resolves a page");
    assert.deepEqual(harness.ops, [], "no command is issued into a closing browser");
  });

  test("a coalesced pass that begins after shutdown started issues nothing", async () => {

    // This is what proves the shutdown check lives inside the loop rather than at the entry: the first pass runs normally, the flag flips mid-run, and the pass
    // the second caller queued must find it.
    const gate = makeGate();

    const { deps, harness } = makeHarness({

      minimize: async (): Promise<void> => {

        await gate.promise;
      }
    });

    const sync = createWindowVisibilitySync(deps);
    const first = sync();
    const second = sync();

    harness.shuttingDown = true;
    gate.release();

    await Promise.all([ first, second ]);

    assert.deepEqual(harness.ops, ["minimize:default"], "only the pass that was already in flight issued a command");
  });

  test("an iteration error ends the loop without rejecting the waiters", async () => {

    // Window presentation is cosmetic enough that a caller must not fail over it, and the next state transition triggers another pass anyway.
    const { deps, harness } = makeHarness({

      minimize: async (): Promise<void> => { throw new Error("synthetic CDP failure"); }
    });

    const sync = createWindowVisibilitySync(deps);

    await assert.doesNotReject(() => sync(), "a failed pass resolves its waiters rather than rejecting them");

    assert.deepEqual(harness.ops, ["minimize:default"], "the failing command was attempted once and the loop stopped");
  });

  test("a persistently failing pass settles its callers instead of driving the trigger round", async () => {

    /* The trigger waits for a settled boundary with no request outstanding, so a pass that fails while a second request is armed would send another command
     * straight back at a browser that has just refused one. The drain clears the flag as it exits on an error, which is what stops that: the failing command is
     * attempted once and both callers settle. Verified against a build with the clearing removed - the count reads 2 there, and a steady arrival of triggers
     * through failing passes would keep it climbing.
     */
    let attempts = 0;

    const issued = makeGate();

    const { deps } = makeHarness({

      minimize: async (): Promise<void> => {

        attempts++;

        issued.release();

        throw new Error("synthetic CDP failure");
      }
    });

    const sync = createWindowVisibilitySync(deps);
    const first = sync();
    const second = sync();

    await Promise.all([ first, second, issued.promise ]);

    assert.equal(attempts, 1, "the failing command is attempted once, not retried against a browser that just refused it");
  });

  test("a later call starts a fresh run after an earlier one failed", async () => {

    let failNext = true;

    const { deps, harness } = makeHarness({

      minimize: async (): Promise<void> => {

        if(failNext) {

          failNext = false;

          throw new Error("synthetic CDP failure");
        }
      }
    });

    const sync = createWindowVisibilitySync(deps);

    await sync();
    await sync();

    assert.equal(harness.ops.length, 2, "the run slot was released, so the next trigger drove another pass");
  });
});
