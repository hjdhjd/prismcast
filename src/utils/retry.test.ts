/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * retry.test.ts: Unit tests for retryOperation. The function consumes a Clock (see clock.ts) for sleeps between attempts and for the per-attempt timeout race;
 * tests pass a fake clock built by makeFakeClock (clock.helpers.ts) that resolves sleeps instantly and forwards (or selectively rejects) the timeout race. No
 * real-time delays, no mock.timers - the fake-clock literal is the entire test substrate, deterministic and budget-free. The pure maxRetryDuration estimator is
 * tested directly against the same default constants retryOperation reads, so the worst-case closed form stays tied to the loop that produces the sleeps.
 */
import { describe, mock, test } from "node:test";
import { maxRetryDuration, retryOperation } from "./retry.ts";
import assert from "node:assert/strict";
import { makeFakeClock } from "./clock.helpers.ts";

describe("retryOperation", () => {

  test("returns the operation's value on first-attempt success without scheduling any backoff", async () => {

    const { clock, sleeps } = makeFakeClock();
    let attempts = 0;

    const result = await retryOperation({

      clock,
      description: "happy-path",
      maxAttempts: 3,
      operation: async () => {

        attempts++;

        return "ok";
      },
      timeoutMs: 1_000
    });

    assert.equal(result, "ok", "successful op returns its value");
    assert.equal(attempts, 1, "operation invoked exactly once");
    assert.equal(sleeps.length, 0, "no backoff scheduled when the first attempt succeeds");
  });

  test("returns the value after N failures by exercising the backoff between attempts", async () => {

    const { clock, sleeps } = makeFakeClock();
    let attempts = 0;

    const result = await retryOperation({

      clock,
      description: "succeeds-on-third",
      maxAttempts: 5,
      operation: async () => {

        attempts++;

        if(attempts < 3) {

          throw new Error("transient failure " + String(attempts));
        }

        return "succeeded";
      },
      timeoutMs: 1_000
    });

    assert.equal(result, "succeeded", "third attempt succeeded");
    assert.equal(attempts, 3, "operation invoked three times");

    // Backoff fires between attempts 1->2 and 2->3, but NOT after the successful third attempt - the loop short-circuits via the `return` before reaching the
    // sleep block. The recorded sleeps verify both the count and that the schedule lives entirely between failed attempts.
    assert.equal(sleeps.length, 2, "backoff scheduled twice (between three attempts)");
  });

  test("throws the last error after exhausting maxAttempts when operation never succeeds", async () => {

    const { clock, sleeps } = makeFakeClock();
    let attempts = 0;

    await assert.rejects(

      () => retryOperation({

        clock,
        description: "always-fails",
        maxAttempts: 3,
        operation: async () => {

          attempts++;

          throw new Error("attempt " + String(attempts) + " failed");
        },
        timeoutMs: 1_000
      }),
      /attempt 3 failed/,
      "throws the most recent error after exhaustion"
    );

    assert.equal(attempts, 3, "operation tried exactly maxAttempts times");

    // Backoff fires between failed attempts but NOT after the final failure (the loop guard `attempt < maxAttempts` excludes it). Three attempts, two sleeps.
    assert.equal(sleeps.length, 2, "no sleep after the final failed attempt");
  });

  test("schedules exponential backoff with jitter capped by maxBackoffDelay", async () => {

    // The backoff formula is min(1000 * 2^(attempt-1), maxBackoffDelay) + random(0, backoffJitter). With maxBackoffDelay=100 and backoffJitter=0, the first two
    // sleeps must equal exactly 1000 then 2000 capped to 100, i.e. 100 each (because 1000 already exceeds the cap on attempt 1 too: 1000 > 100). With
    // maxBackoffDelay=5000 (above the natural growth), we see 1000 then 2000.
    const cap = makeFakeClock();

    await assert.rejects(

      () => retryOperation({

        backoffJitter: 0,
        clock: cap.clock,
        description: "capped",
        maxAttempts: 3,
        maxBackoffDelay: 100,
        operation: async () => {

          throw new Error("fail");
        },
        timeoutMs: 1_000
      }),
      /fail/
    );

    assert.deepEqual(cap.sleeps, [ 100, 100 ], "both sleeps clamped to maxBackoffDelay because 1000ms already exceeds the cap");

    const grow = makeFakeClock();

    await assert.rejects(

      () => retryOperation({

        backoffJitter: 0,
        clock: grow.clock,
        description: "growing",
        maxAttempts: 3,
        maxBackoffDelay: 5_000,
        operation: async () => {

          throw new Error("fail");
        },
        timeoutMs: 1_000
      }),
      /fail/
    );

    assert.deepEqual(grow.sleeps, [ 1_000, 2_000 ], "exponential growth follows 1000 * 2^(attempt-1) when below the cap");
  });

  test("throws immediately on a session-closed error without consuming a retry budget", async () => {

    const { clock, sleeps } = makeFakeClock();
    let attempts = 0;

    await assert.rejects(

      () => retryOperation({

        clock,
        description: "session-closed",
        maxAttempts: 5,
        operation: async () => {

          attempts++;

          throw new Error("Target closed");
        },
        timeoutMs: 1_000
      }),
      /Target closed/,
      "session-closed errors propagate immediately"
    );

    assert.equal(attempts, 1, "no retries attempted after session closed");
    assert.equal(sleeps.length, 0, "session-closed short-circuits before any backoff is scheduled");
  });

  test("aborts before the first attempt when shouldAbort returns true upfront", async () => {

    const { clock } = makeFakeClock();
    const operation = mock.fn(async (): Promise<string> => "should-not-run");

    await assert.rejects(

      () => retryOperation({

        clock,
        description: "pre-abort",
        maxAttempts: 3,
        operation,
        shouldAbort: () => true,
        timeoutMs: 1_000
      }),
      /Operation aborted/,
      "abort throws the documented sentinel"
    );

    assert.equal(operation.mock.callCount(), 0, "operation never invoked when abort is true at the gate");
  });

  test("aborts mid-retry when shouldAbort flips during backoff", async () => {

    const { clock, sleeps } = makeFakeClock();
    let attempts = 0;
    let aborted = false;

    await assert.rejects(

      () => retryOperation({

        clock,
        description: "mid-abort",
        maxAttempts: 5,
        operation: async () => {

          attempts++;

          // Flip the abort flag after the first failure - the loop will see it on the next iteration's gate check.
          if(attempts === 1) {

            aborted = true;
          }

          throw new Error("attempt " + String(attempts));
        },
        shouldAbort: () => aborted,
        timeoutMs: 1_000
      }),
      /Operation aborted/,
      "abort short-circuits the retry loop"
    );

    assert.equal(attempts, 1, "second attempt was skipped because abort fired");

    // The first attempt failed and the loop scheduled a backoff sleep before the next iteration's abort-gate fired. The sleep ran (instantly, via the fake
    // clock), but the abort caught us before attempt 2 began.
    assert.equal(sleeps.length, 1, "one backoff sleep ran before the abort gate fired");
  });

  test("returns undefined when earlySuccessCheck signals success after a timeout", async () => {

    // The earlySuccessCheck path covers cases where an operation legitimately finished but its caller timed out waiting for some signal (e.g., page loaded but
    // networkidle2 never resolved). Here the operation throws a timeout-shaped error directly (the fake clock's waitWithTimeout forwards unchanged, so the
    // error must come from the operation itself for the formatError check to see "timed out"). The operation is typed Promise<string> so the inferred return is
    // string | undefined rather than the void-expression-flagged never | undefined.
    const { clock } = makeFakeClock();

    const result: string | undefined = await retryOperation({

      clock,
      description: "early-success",
      earlySuccessCheck: async () => true,
      maxAttempts: 3,
      operation: async (): Promise<string> => {

        throw new Error("Operation timed out after 1000ms.");
      },
      timeoutMs: 1_000
    });

    assert.equal(result, undefined, "early-success path returns undefined (no value to surface)");
  });

  test("ignores earlySuccessCheck failures and continues retrying", async () => {

    // Negative test: when earlySuccessCheck itself throws, the function must NOT bubble that error out. It should fall through to the normal retry path.
    const { clock, sleeps } = makeFakeClock();
    let attempts = 0;

    await assert.rejects(

      () => retryOperation({

        clock,
        description: "early-success-throws",
        earlySuccessCheck: async () => {

          throw new Error("check failed");
        },
        maxAttempts: 2,
        operation: async () => {

          attempts++;

          throw new Error("Operation timed out after 1000ms.");
        },
        timeoutMs: 1_000
      }),
      /timed out/,
      "outer rejection surfaces the operation error, not the early-check error"
    );

    assert.equal(attempts, 2, "retry continued normally after the early-check throw");
    assert.equal(sleeps.length, 1, "one backoff between the two attempts");
  });

  test("propagates a timeout error from the clock's waitWithTimeout when the operation hangs", async () => {

    // Locks the timeout-race contract: when the per-attempt race fires before the operation resolves, the function treats it as a normal failure and proceeds to
    // the next attempt. The fake clock's waitWithTimeout throws synchronously (without awaiting the inner promise) so we can deterministically simulate the
    // timer winning the race. With maxAttempts=2 and no earlySuccessCheck, both attempts time out and the loop throws the last error.
    const handle = makeFakeClock({

      waitWithTimeout: async (_promise, timeoutMs, timeoutError) => {

        throw timeoutError ?? new Error("Operation timed out after " + String(timeoutMs) + "ms.");
      }
    });

    let attempts = 0;

    await assert.rejects(

      () => retryOperation({

        clock: handle.clock,
        description: "hangs",
        maxAttempts: 2,
        operation: async () => {

          attempts++;

          // Operation never resolves - real production code would hang here, but the fake clock pre-empts it with the timeout reject.
          return new Promise<string>(() => { /* never resolves */ });
        },
        timeoutMs: 1_000
      }),
      /timed out after 1000ms/,
      "the timeout error from waitWithTimeout is the error the loop ultimately throws"
    );

    assert.equal(attempts, 2, "both attempts started even though both timed out");
    assert.equal(handle.sleeps.length, 1, "backoff between the two timed-out attempts");
  });

  test("respects a maxAttempts of 1 (no retries, single shot)", async () => {

    const { clock, sleeps } = makeFakeClock();
    let attempts = 0;

    await assert.rejects(

      () => retryOperation({

        clock,
        description: "single-shot",
        maxAttempts: 1,
        operation: async () => {

          attempts++;

          throw new Error("nope");
        },
        timeoutMs: 1_000
      }),
      /nope/,
      "single attempt, single throw"
    );

    assert.equal(attempts, 1, "exactly one attempt");
    assert.equal(sleeps.length, 0, "no backoff with maxAttempts=1");
  });

  test("a maxAttempts of 0 throws the initial null lastError without invoking the operation", async () => {

    // Boundary: the loop guard `attempt <= maxAttempts` excludes the body when maxAttempts is 0. The function reaches the trailing `throw lastError` with
    // lastError still at its initial null. This is locked behavior - callers must not pass 0, but if they do, the function fails predictably.
    const { clock } = makeFakeClock();
    let attempts = 0;
    let captured: unknown = "sentinel";

    try {

      await retryOperation({

        clock,
        description: "zero-attempts",
        maxAttempts: 0,
        operation: async () => {

          attempts++;

          return "should-not-run";
        },
        timeoutMs: 1_000
      });
    } catch(err) {

      captured = err;
    }

    assert.equal(attempts, 0, "operation never invoked with maxAttempts=0");
    assert.equal(captured, null, "the rejection value is the literal initial null");
  });

  test("default-arg wires through to realClock when no clock is supplied", async () => {

    // Locks the default-argument behavior so a future refactor that breaks the optional doesn't pass unnoticed. The operation succeeds on the first attempt, so
    // realClock's sleep() is never called - the test exercises the wiring without depending on any real-time path.
    let attempts = 0;

    const result = await retryOperation({

      description: "default-clock",
      maxAttempts: 3,
      operation: async () => {

        attempts++;

        return "wired";
      },
      timeoutMs: 1_000
    });

    assert.equal(result, "wired");
    assert.equal(attempts, 1);
  });
});

describe("maxRetryDuration", () => {

  test("sums every attempt's timeout plus one ceilinged backoff gap per retry using the shared defaults", () => {

    // The closed form is maxAttempts * timeoutMs + (maxAttempts - 1) * (maxBackoffDelay + backoffJitter). With retry.ts's defaults - a 3000ms backoff cap and a
    // 1000ms jitter ceiling, the same constants the loop's destructuring reads - three attempts of 10000ms with two gaps of 4000ms gives 38000ms.
    assert.equal(maxRetryDuration({ maxAttempts: 3, timeoutMs: 10000 }), (3 * 10000) + (2 * (3000 + 1000)));
    assert.equal(maxRetryDuration({ maxAttempts: 3, timeoutMs: 10000 }), 38000);
  });

  test("honors explicit backoff overrides in place of the defaults", () => {

    // Four attempts of 5000ms with three gaps, each capped at 2000ms plus 500ms of jitter, gives 20000 + 7500 = 27500ms.
    assert.equal(maxRetryDuration({ backoffJitter: 500, maxAttempts: 4, maxBackoffDelay: 2000, timeoutMs: 5000 }), (4 * 5000) + (3 * (2000 + 500)));
  });

  test("adds no backoff for a single attempt", () => {

    // With one attempt there are zero gaps, so the estimate is just the one per-attempt timeout.
    assert.equal(maxRetryDuration({ maxAttempts: 1, timeoutMs: 8000 }), 8000);
  });

  test("clamps the gap count at zero for an out-of-contract attempt count below one", () => {

    // maxAttempts of 0 yields no attempts and no gaps, mirroring retryOperation's own zero-attempt boundary rather than producing a negative term.
    assert.equal(maxRetryDuration({ maxAttempts: 0, timeoutMs: 8000 }), 0);
  });
});
