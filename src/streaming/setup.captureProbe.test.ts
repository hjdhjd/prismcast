/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * setup.captureProbe.test.ts: Setup-tier tests for the capture probe the browser launch gate runs: verifyCaptureSystem and the attempt it repeats. The attempt
 * opens its page and declares the page's layout surface inside its own error handling, so an attempt whose emulation rejects is reported the way any failed
 * capture is: the page it opened is released and the gate goes on to its next attempt. The rows drive the gate against a stub browser whose pages reject the
 * emulation, on a virtual clock that carries the wait between attempts, so no Chrome is involved and no real time passes.
 */
import type { Browser, Page } from "puppeteer-core";
import { TestClock, drainClock } from "homebridge-plugin-utils/testing";
import { describe, test } from "node:test";
import { CaptureLaunchError } from "../browser/index.ts";
import { LOG } from "../utils/index.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { verifyCaptureSystem } from "./setup.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

describe("verifyCaptureSystem", () => {

  test("retries an attempt whose layout emulation rejects, closes each page, and fails the gate with the emulation's error after its last attempt", async (t) => {

    /* Every page the gate opens rejects its surface declaration. The emulation runs inside the attempt's try, so each rejection comes back as a failed attempt
     * rather than escaping the gate's loop: the gate waits and tries again until its attempts are spent, then throws the launch-gate error naming what the last
     * attempt saw. Every page an attempt opened is closed on the way.
     */
    const clock = new TestClock();
    const warn = t.mock.method(LOG, "warn", () => { /* Captured via the mock. */ });
    const pages: { closes: number }[] = [];

    const browser = {

      newPage: async (): Promise<Page> => {

        const record = { closes: 0 };

        pages.push(record);

        return {

          close: async (): Promise<void> => { record.closes++; },
          isClosed: (): boolean => record.closes > 0,
          setViewport: async (): Promise<void> => { throw new Error("The viewport could not be declared."); }
        } as unknown as Page;
      }
    } as unknown as Browser;

    const verifying = verifyCaptureSystem(browser, clock);

    // Observed at once, so the rejection that lands while the clock is drained is never an unhandled one; the assertion below reads the same promise.
    void verifying.catch(() => { /* Read by the assertion below. */ });

    const steps = await drainClock(clock);

    await assert.rejects(verifying, (error: unknown): boolean => (error instanceof CaptureLaunchError) &&
      error.message.startsWith("Capture system verification failed after 3 attempts: The viewport could not be declared"), "the gate fails with its own error type");

    assert.equal(steps, 2, "the gate waited between its attempts on the injected clock");
    assert.equal(pages.length, 3, "each attempt opened a page of its own");
    assert.deepEqual(pages.map((page) => page.closes), [ 1, 1, 1 ], "and every page was closed once");
    assert.equal(warn.mock.callCount(), 2, "each attempt before the last reported its failure and the retry");
  });
});
