/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * setup.retire.test.ts: Unit tests for retireRawStream, the destroy-and-confirm core every capture teardown that precedes a page close runs through. Two
 * outcomes are asserted because both matter to a caller: the ordinary one, where the capture extension confirms the recording stopped and the page may be closed
 * safely, and the lapse, where the confirmation never arrives and the function warns and returns rather than holding the page open for a browser that has
 * stopped answering. The stub capture is a real PassThrough carrying the two capture controls, so the destroy under test is a genuine one.
 */
import { afterEach, beforeEach, describe, test } from "node:test";
import type { CaptureStream } from "../browser/tabCapture.ts";
import type { LogEntry } from "../utils/logEmitter.ts";
import { PassThrough } from "node:stream";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { makeFakeClock } from "../utils/clock.helpers.ts";
import { retireRawStream } from "./setup.ts";
import { subscribeToLogs } from "../utils/logEmitter.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

/**
 * Builds a stub capture over a real PassThrough, with the completion signal the row wants.
 * @param stopped - The completion signal the stub reports.
 * @returns The stub capture.
 */
function makeStubCapture(stopped: Promise<void>): CaptureStream {

  return Object.assign(new PassThrough(), { stop: async (): Promise<void> => undefined, stopped });
}

describe("retireRawStream", () => {

  let captured: LogEntry[] = [];
  let unsubscribe = (): void => { /* Replaced per test by the real unsubscribe. */ };

  beforeEach(() => {

    captured = [];
    unsubscribe = subscribeToLogs((entry) => { captured.push(entry); });
  });

  afterEach(() => {

    unsubscribe();
  });

  test("destroys the capture and returns once the extension confirms the recording stopped", async () => {

    // The destroy is what sends the stop request, and the confirmation is what makes the page safe to close afterwards. Neither is optional, and a confirmation
    // that arrives normally is not an event worth logging.
    const { clock } = makeFakeClock();
    const stream = makeStubCapture(Promise.resolve());

    await retireRawStream(stream, clock);

    assert.equal(stream.destroyed, true, "the capture was destroyed, which is what sends the stop request");
    assert.deepEqual(captured.filter((entry) => entry.level === "warn"), [], "an ordinary retire says nothing");
  });

  test("warns and returns when the confirmation never arrives, rather than holding the page open", async () => {

    /* A browser that is already gone will never close the socket the confirmation arrives on, so waiting for it would strand every teardown behind a dead
     * process. The clock's bound is what ends the wait, and the function's own contract is that it never throws - a caller mid-teardown has nothing to do with
     * a rejection here.
     */
    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const { promise: never } = Promise.withResolvers<void>();
    const { clock } = makeFakeClock({

      waitWithTimeout: async <T>(_promise: Promise<T>, timeoutMs: number, timeoutError?: Error): Promise<T> => {

        throw timeoutError ?? new Error("timed out after " + String(timeoutMs) + "ms.");
      }
    });
    const stream = makeStubCapture(never);

    await assert.doesNotReject(() => retireRawStream(stream, clock), "a lapsed confirmation is not a failure the caller has to handle");

    assert.equal(stream.destroyed, true, "the capture was still destroyed");

    const warnings = captured.filter((entry) => entry.level === "warn");

    assert.equal(warnings.length, 1, "exactly one warning");
    assert.match(warnings[0]?.message ?? "", /did not confirm the recording stopped within 3000ms/, "the warning names the bound that ended the wait");
    assert.match(warnings[0]?.message ?? "", /closing the page regardless/, "and says what happens next");
  });
});
