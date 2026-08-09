/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * captureLock.test.ts: Unit tests for the task-scoped capture lock. Every "held" or "released" claim is observed structurally through a QUEUED FURTHER run() whose
 * task callback has (or has not) been invoked, never through a bare comment. The lock is driven with a fake Clock and task stubs backed by Promise.withResolvers so
 * turn grants, give-ups, wedges, and deadlines resolve deterministically without real time. A fresh createCaptureLock is built per test so no tail leaks between cases.
 */
import { CaptureAbandonedError, CaptureDeadlineError, CaptureTurnTimeoutError, createCaptureLock } from "./captureLock.ts";
import { assertNoUnhandledRejections, expectAt } from "../testing.helpers.ts";
import { describe, test } from "node:test";
import type { CaptureRunOptions } from "./captureLock.ts";
import type { Clock } from "../utils/clock.ts";
import assert from "node:assert/strict";
import { makeFakeClock } from "../utils/clock.helpers.ts";

// A standard set of per-call options: a 10s task deadline and turn-wait, matching the stream call site's navigationTimeout default. Returned fresh each call so no
// onWedge callback is shared across tests.
function runOpts(): CaptureRunOptions {

  return { deadlineMessage: "deadline", deadlineMs: 10000, turnWaitMs: 10000 };
}

// Drains the microtask queue so chained promise settlements (turn grant, forwarding, markSettled) resolve before an assertion reads them. The lock schedules no real
// timers under the fake clock, so a bounded microtask flush is sufficient.
async function flushMicro(): Promise<void> {

  for(let i = 0; i < 30; i++) {

    // eslint-disable-next-line no-await-in-loop -- Sequential microtask yields are the point; parallelizing would defeat the drain.
    await Promise.resolve();
  }
}

// Yields one macrotask so any unhandledRejection event has been emitted before assertNoUnhandledRejections's cleanup reads its capture buffer.
async function flushMacro(): Promise<void> {

  await new Promise<void>((resolve) => {

    setImmediate(resolve);
  });
}

// A fake clock whose waitWithTimeout forwards every promise unchanged and whose sleep is driven manually: each sleep records its requested duration and a resolver, so
// a test controls exactly when (and whether) the wedge fires. This is the shape the wedge tests need - the wedge sleep stays pending until the test releases it.
function makeManualClock(): { clock: Clock; sleeps: { ms: number; resolve: () => void }[] } {

  const sleeps: { ms: number; resolve: () => void }[] = [];

  const clock: Clock = {

    now: (): number => 0,
    sleep: (ms: number): Promise<void> => {

      // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
      const { promise, resolve } = Promise.withResolvers<void>();

      sleeps.push({ ms, resolve });

      return promise;
    },
    waitWithTimeout: async <T>(promise: Promise<T>): Promise<T> => promise
  };

  return { clock, sleeps };
}

// A fake clock whose waitWithTimeout forwards the turn-wait but fires the deadline once - for the first task only, the one the test abandons - so its successor still
// gets its turn and completes normally. Sleep keeps the makeFakeClock default (resolves immediately, recording durations), which the retire-ordering test asserts on.
function makeDeadlineFiringClock(): ReturnType<typeof makeFakeClock> {

  let deadlineFired = false;

  return makeFakeClock({

    waitWithTimeout: async <T>(promise: Promise<T>, _timeoutMs: number, timeoutError?: Error): Promise<T> => {

      if((timeoutError instanceof CaptureDeadlineError) && !deadlineFired) {

        deadlineFired = true;

        throw timeoutError;
      }

      return promise;
    }
  });
}

describe("createCaptureLock", () => {

  test("serializes tasks: a later task does not begin until the earlier task settles", async () => {

    const { clock } = makeFakeClock();
    const lock = createCaptureLock({ clock, wedgeFloorMs: 30000, wedgeMarginMs: 5000 });
    const aWork = Promise.withResolvers<string>();
    const calls: string[] = [];
    const pA = lock.run(async (): Promise<string> => {

      calls.push("A");

      return aWork.promise;
    }, runOpts());
    const pB = lock.run(async (): Promise<string> => {

      calls.push("B");

      return "B";
    }, runOpts());

    await flushMicro();

    assert.deepEqual(calls, ["A"], "B has not begun while A holds the turn");

    aWork.resolve("A");

    await flushMicro();

    assert.deepEqual(calls, [ "A", "B" ], "B begins only after A settles");
    assert.equal(await pA, "A");
    assert.equal(await pB, "B");
  });

  test("give-up forwards to the predecessor and never advances the chain past an unsettled task", async () => {

    const restore = assertNoUnhandledRejections();

    // The turn-wait times out only while this flag is set, which the test flips synchronously between run() calls: run() reads the flag when it invokes
    // waitWithTimeout, before its first await, so the reads are deterministic.
    let turnWaitTimesOut = false;
    const { clock } = makeFakeClock({

      waitWithTimeout: async <T>(promise: Promise<T>, _timeoutMs: number, timeoutError?: Error): Promise<T> => {

        if((timeoutError instanceof CaptureTurnTimeoutError) && turnWaitTimesOut) {

          throw timeoutError;
        }

        return promise;
      }
    });
    const lock = createCaptureLock({ clock, wedgeFloorMs: 30000, wedgeMarginMs: 5000 });
    const aWork = Promise.withResolvers<string>();
    const calls: string[] = [];
    const pA = lock.run(async (): Promise<string> => {

      calls.push("A");

      return aWork.promise;
    }, runOpts());

    turnWaitTimesOut = true;

    let bRan = false;
    const pB = lock.run(async (): Promise<string> => {

      bRan = true;

      return "B";
    }, runOpts());

    turnWaitTimesOut = false;

    const pC = lock.run(async (): Promise<string> => {

      calls.push("C");

      return "C";
    }, runOpts());

    await assert.rejects(pB, (error: unknown) => error instanceof CaptureTurnTimeoutError);
    await flushMicro();

    assert.equal(bRan, false, "the give-up waiter's task never ran");
    assert.deepEqual(calls, ["A"], "C is still blocked behind the unsettled A - the give-up did not advance the chain");

    aWork.resolve("A");

    await flushMicro();

    assert.deepEqual(calls, [ "A", "C" ], "C runs only after the running task actually settles");
    assert.equal(await pA, "A");
    assert.equal(await pC, "C");

    await flushMacro();

    restore();
  });

  test("the wedge fires exactly once at the derived bound and never releases the turn early", async () => {

    const { clock, sleeps } = makeManualClock();
    const lock = createCaptureLock({ clock, wedgeFloorMs: 30000, wedgeMarginMs: 5000 });
    const aWork = Promise.withResolvers<string>();
    const calls: string[] = [];
    let wedgeCount = 0;
    const pA = lock.run(async (): Promise<string> => {

      calls.push("A");

      return aWork.promise;
    }, { ...runOpts(), onWedge: (): void => {

      wedgeCount++;
    } });

    await flushMicro();

    assert.deepEqual(calls, ["A"], "A holds the turn");
    assert.equal(sleeps.length, 1, "the wedge sleep is armed at turn grant");

    const wedgeSleep = await expectAt(() => sleeps[0]);

    assert.equal(wedgeSleep.ms, 30000, "the wedge bound derives to the floor for a 10s deadline");
    assert.equal(wedgeCount, 0, "the wedge has not fired while the task is under the bound");

    let bRan = false;
    const pB = lock.run(async (): Promise<string> => {

      bRan = true;

      return "B";
    }, runOpts());

    await flushMicro();

    assert.equal(bRan, false, "B is blocked while A holds the turn");

    wedgeSleep.resolve();

    await flushMicro();

    assert.equal(wedgeCount, 1, "the wedge fired exactly once at the bound");
    assert.equal(bRan, false, "the wedge did not release the turn: B is still blocked");

    aWork.resolve("A");

    await flushMicro();

    assert.equal(bRan, true, "B runs only after A truly settles");
    assert.equal(wedgeCount, 1, "the wedge did not fire a second time");
    assert.equal(await pA, "A");
    assert.equal(await pB, "B");
  });

  test("the wedge does not fire when the task settles before the bound", async () => {

    const { clock, sleeps } = makeManualClock();
    const lock = createCaptureLock({ clock, wedgeFloorMs: 30000, wedgeMarginMs: 5000 });
    let wedged = false;
    const p = lock.run(async (): Promise<string> => "done", { ...runOpts(), onWedge: (): void => {

      wedged = true;
    } });

    await flushMicro();

    assert.equal(sleeps.length, 1, "the wedge sleep is armed");

    const armedSleep = await expectAt(() => sleeps[0]);

    // The task already settled, so firing the armed wedge is a no-op.
    armedSleep.resolve();

    await flushMicro();

    assert.equal(wedged, false, "the wedge does not fire for a task that settled before the bound");
    assert.equal(await p, "done");
  });

  test("the wedge never fires for a task still waiting for its turn", async () => {

    const { clock, sleeps } = makeManualClock();
    const lock = createCaptureLock({ clock, wedgeFloorMs: 30000, wedgeMarginMs: 5000 });
    const aWork = Promise.withResolvers<string>();
    const pA = lock.run(async (): Promise<string> => aWork.promise, runOpts());
    let bWedged = false;
    const pB = lock.run(async (): Promise<string> => "B", { ...runOpts(), onWedge: (): void => {

      bWedged = true;
    } });

    await flushMicro();

    assert.equal(sleeps.length, 1, "only the turn-holder armed a wedge; the waiter armed nothing");

    const holderSleep = await expectAt(() => sleeps[0]);

    // Even releasing the only armed wedge (A's) cannot fire B's wedge, because B is still in the turn-wait phase.
    holderSleep.resolve();

    await flushMicro();

    assert.equal(bWedged, false, "a task waiting for its turn never fires its wedge");

    aWork.resolve("A");

    await flushMicro();

    assert.equal(await pA, "A");
    assert.equal(await pB, "B");
  });

  test("the wedge bound derives as max(floor, deadline + margin) and is always later than the caller deadline", async () => {

    // A small deadline derives to the floor, which is strictly later than the 10s caller deadline (30000 > 10000).
    {

      const { clock, sleeps } = makeManualClock();
      const lock = createCaptureLock({ clock, wedgeFloorMs: 30000, wedgeMarginMs: 5000 });
      const work = Promise.withResolvers<string>();
      const p = lock.run(async (): Promise<string> => work.promise, { deadlineMessage: "d", deadlineMs: 10000, onWedge: (): void => { /* Unused here. */ },
        turnWaitMs: 10000 });

      await flushMicro();

      assert.equal((await expectAt(() => sleeps[0])).ms, 30000, "a 10s deadline derives the wedge to the 30s floor, strictly later than the caller deadline");

      work.resolve("x");

      await p;
    }

    // A large deadline derives to deadline + margin, which is strictly later than the 40s caller deadline (45000 > 40000).
    {

      const { clock, sleeps } = makeManualClock();
      const lock = createCaptureLock({ clock, wedgeFloorMs: 30000, wedgeMarginMs: 5000 });
      const work = Promise.withResolvers<string>();
      const p = lock.run(async (): Promise<string> => work.promise, { deadlineMessage: "d", deadlineMs: 40000, onWedge: (): void => { /* Unused here. */ },
        turnWaitMs: 40000 });

      await flushMicro();

      assert.equal((await expectAt(() => sleeps[0])).ms, 45000, "a 40s deadline derives the wedge to deadline + margin, strictly later than the caller deadline");

      work.resolve("x");

      await p;
    }
  });

  test("the deadline applies to the task phase only, aborts the signal, and holds the turn until the task truly settles", async () => {

    const { clock } = makeDeadlineFiringClock();
    const lock = createCaptureLock({ clock, wedgeFloorMs: 30000, wedgeMarginMs: 5000 });
    const aWork = Promise.withResolvers<string>();
    let capturedSignal: AbortSignal | null = null;
    const pA = lock.run(async (signal: AbortSignal): Promise<string> => {

      capturedSignal = signal;

      return aWork.promise;
    }, { deadlineMessage: "Stream initialization timed out.", deadlineMs: 10000, turnWaitMs: 10000 });

    await assert.rejects(pA, (error: unknown) => (error instanceof CaptureDeadlineError) && (error.message === "Stream initialization timed out."));

    const abortedSignal = await expectAt(() => capturedSignal ?? undefined);

    assert.equal(abortedSignal.aborted, true, "the task's signal was aborted when the deadline fired");

    let bRan = false;
    const pB = lock.run(async (): Promise<string> => {

      bRan = true;

      return "B";
    }, runOpts());

    await flushMicro();

    assert.equal(bRan, false, "the turn is held past the deadline until the abandoned task settles");

    aWork.resolve("late");

    await flushMicro();

    assert.equal(bRan, true, "the successor runs once the abandoned task truly settles");
    assert.equal(await pB, "B");
  });

  test("an orphaned late success is retired inside the turn before the successor begins", async () => {

    const restore = assertNoUnhandledRejections();
    const { clock, sleeps } = makeDeadlineFiringClock();
    const lock = createCaptureLock({ clock, wedgeFloorMs: 30000, wedgeMarginMs: 5000 });
    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const gotStream = Promise.withResolvers<void>();
    const stubStream = { destroy(): void {

      this.destroyed = true;
    }, destroyed: false };
    let settleElapsed = false;
    const pA = lock.run(async (signal: AbortSignal): Promise<typeof stubStream> => {

      await gotStream.promise;

      // Mirror the real stream task's abandonment branch: on a fired deadline, retire the resource this task produced, then reject so no path uses it.
      if(signal.aborted) {

        stubStream.destroy();

        await clock.sleep(500);

        settleElapsed = true;

        throw new CaptureAbandonedError();
      }

      return stubStream;
    }, { deadlineMessage: "Stream initialization timed out.", deadlineMs: 10000, turnWaitMs: 10000 });

    await assert.rejects(pA, (error: unknown) => error instanceof CaptureDeadlineError);

    let stateAtBStart: { destroyed: boolean; settleElapsed: boolean } | null = null;
    const pB = lock.run(async (): Promise<string> => {

      stateAtBStart = { destroyed: stubStream.destroyed, settleElapsed };

      return "B";
    }, runOpts());

    await flushMicro();

    assert.equal(stateAtBStart, null, "the successor has not begun while the orphan is being retired");

    // The operation resolves late, after the caller abandoned it.
    gotStream.resolve();

    await flushMicro();

    const observedAtBStart = await expectAt(() => stateAtBStart ?? undefined);

    assert.equal(observedAtBStart.destroyed, true, "the orphaned stream was destroyed before the turn released");
    assert.equal(observedAtBStart.settleElapsed, true, "the STOP_RECORDING settle elapsed before the turn released");
    assert.ok(sleeps.includes(500), "the retire settle delay was scheduled");
    assert.equal(await pB, "B");

    await flushMacro();

    restore();
  });

  test("a task rejection releases the turn without an unhandled rejection, in-time and late-after-abandonment", async () => {

    const restore = assertNoUnhandledRejections();

    // Variant 1: an in-time rejection reaches the caller and releases the turn.
    {

      const { clock } = makeFakeClock();
      const lock = createCaptureLock({ clock, wedgeFloorMs: 30000, wedgeMarginMs: 5000 });
      const aWork = Promise.withResolvers<string>();
      const pA = lock.run(async (): Promise<string> => aWork.promise, runOpts());
      let bRan = false;
      const pB = lock.run(async (): Promise<string> => {

        bRan = true;

        return "B";
      }, runOpts());

      await flushMicro();

      assert.equal(bRan, false, "B is blocked while A holds the turn");

      const boom = new Error("in-time boom");

      aWork.reject(boom);

      await assert.rejects(pA, (error: unknown) => error === boom);
      await flushMicro();

      assert.equal(bRan, true, "the successor runs after the in-time rejection releases the turn");
      assert.equal(await pB, "B");
    }

    // Variant 2: a late rejection, after the caller abandoned the task at the deadline, still releases the turn.
    {

      const { clock } = makeDeadlineFiringClock();
      const lock = createCaptureLock({ clock, wedgeFloorMs: 30000, wedgeMarginMs: 5000 });
      const aWork = Promise.withResolvers<string>();
      const pA = lock.run(async (): Promise<string> => aWork.promise, { deadlineMessage: "Stream initialization timed out.", deadlineMs: 10000, turnWaitMs: 10000 });

      await assert.rejects(pA, (error: unknown) => error instanceof CaptureDeadlineError);

      let bRan = false;
      const pB = lock.run(async (): Promise<string> => {

        bRan = true;

        return "B";
      }, runOpts());

      await flushMicro();

      assert.equal(bRan, false, "the turn is still held while the abandoned task is pending");

      aWork.reject(new Error("late boom"));

      await flushMicro();

      assert.equal(bRan, true, "the late rejection released the turn");
      assert.equal(await pB, "B");
    }

    await flushMacro();

    restore();
  });

  test("the error classes carry their exact, compatibility-critical messages", () => {

    assert.equal(new CaptureTurnTimeoutError().message, "Capture queue wait timed out.");
    assert.equal(new CaptureDeadlineError("Stream initialization timed out.").message, "Stream initialization timed out.");
    assert.equal(new CaptureAbandonedError().message, "Capture stream retired after the caller abandoned its turn.");
  });
});
