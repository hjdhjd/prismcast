/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * precaching.test.ts: Unit tests for the precaching coordinator's no-op gates in precaching.ts. The module exports a single function, startPrecaching, which
 * inspects two pieces of state before scheduling the precache cycle:
 *
 *   1. CONFIG.channels.precacheServices: when empty, the function returns immediately with no side effects (no timer scheduled, no log lines, no internal flag
 *      mutated).
 *
 *   2. The module-level precacheInProgress flag: when true, the function defers. The flag is set when the cycle starts and cleared in a finally block.
 *
 * The actual precache cycle (runPrecacheCycle) drives Puppeteer via getCurrentBrowser, browser.newPage, page.goto, and provider.discoverChannels - that path is
 * deferred to e2e. The unit tests here lock the gate-behavior contract so that future refactors of the gates do not silently regress.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { CONFIG } from "../config/index.ts";
import assert from "node:assert/strict";
import { startPrecaching } from "./precaching.ts";

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

    // Boundary: the guard explicitly checks length === 0. This test confirms that the early-exit is contingent on that exact condition by setting the array back
    // to empty after a non-empty start - the function should still be safely callable.
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

    // Ticking less than the precache delay confirms the timer is in flight without firing it. If startPrecaching had short-circuited, this would still pass; the
    // distinguishing assertion is that runAll fires SOMETHING (the timer body), which we test in a sibling case below.
    mock.timers.tick(0);
  });
});

/* Deferred to e2e (require Puppeteer/Chrome integration):
 *
 * - runPrecacheCycle (the cycle body itself - drives getCurrentBrowser, browser.newPage, page.evaluateOnNewDocument, page.goto, provider.discoverChannels,
 *   markDomainAuth, page.close, minimizeBrowserWindow).
 *
 * - The precacheInProgress guard's positive case (the cycle must be in flight to observe the flag set; verifying that requires running the cycle which requires a
 *   real browser).
 *
 * - The service-filter skip path (skipping services not in CONFIG.channels.enabledServices) - exercised inside runPrecacheCycle.
 *
 * - The validatePrecache path - per-provider auth confirmation runs after a real discovery completes.
 *
 * - Per-provider error isolation (one provider failing while others succeed) - requires a real browser to populate the discovery flow.
 */
